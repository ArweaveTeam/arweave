%%% A wrapper library for gun.
-module(ar_http).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, req/1]).

-ifdef(AR_TEST).
-export([block_peer_connections/0, unblock_peer_connections/0]).
-endif.

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Fixed connection pool. Each peer's pool grows toward connections_per_peer as
%% requests arrive (get_connection) and shrinks one connection per tick while the
%% peer is idle (evaluate_pools). There is no sizing beyond the configured ceiling:
%% for a store/peer-bound workload more connections don't raise throughput and can
%% overload the peer, so connections_per_peer is a fixed cap rather than a
%% scheduler decision.
-define(POOL_EVAL_INTERVAL_MS, 10_000).
%% Per-peer count of requests currently inside req/2 (throttle -> get_connection ->
%% gun request/await). evaluate_pools only shrinks a peer whose count is 0, so a
%% connection carrying a long-running stream - which can span several 10s ticks
%% while req_by_peer for that tick reads 0 - is never shut down mid-request.
-define(HTTP_INFLIGHT_TABLE, ar_http_inflight).

-record(state, {
                pid_by_peer = #{},
                status_by_pid = #{},
                %% Per-peer request count since the last evaluate_pools tick; used only
                %% to tell active peers from idle ones (idle peers get a connection
                %% shrunk).
                reqs_by_peer = #{}
               }).

%%% ==================================================================
%%% Public interface.
%%% ==================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


-ifdef(AR_TEST).
block_peer_connections() ->
    ets:insert(?MODULE, {block_peer_connections}),
    ok.

unblock_peer_connections() ->
    ets:delete(?MODULE, block_peer_connections),
    ok.

req(Args) ->
    case ar_shutdown_manager:state() of
        running ->
            req2(Args);
        shutdown ->
            {error, shutdown}
    end.

req2(#{ peer := {_, _} } = Args) ->
    req(Args, false);
req2(#{ peer := Peer } = Args) ->
    Port = arweave_config:get([port]),
    case Port == element(5, Peer) of
        true ->
            %% Do not block requests to self.
            req(Args, false);
        false ->
            case ets:lookup(?MODULE, block_peer_connections) of
                [{_}] ->
                    case lists:keyfind(<<"x-p2p-port">>, 1, maps:get(headers, Args, [])) of
                        {_, _} ->
                            {error, blocked};
                        _ ->
                            %% Do not block requests made from the test processes.
                            req(Args, false)
                    end;
                _ ->
                    req(Args, false)
            end
    end.
-else.
req(Args) ->
    req(Args, false).
-endif.

req(Args, ReestablishedConnection) ->
    %% Drop stale gun_* messages from the calling process's mailbox before
    %% issuing a new request. Each gun stream can leave straggler messages
    %% (e.g. {gun_error, _, _, {badstate, "The stream cannot be found."}})
    %% after gun:await returns - the connection died asynchronously after
    %% we moved past that stream. Without draining, those accumulate in
    %% callers' mailboxes; gun:await's selective receive then scans the
    %% entire mailbox each iteration looking for its own ref, slowing
    %% scanners over time.
    %%
    %% ar_http is the only production Gun client. Callers that also own Gun
    %% messages can pass `drain_gun => false'. Only drain on the top-level
    %% call, not on the recursive retry that needs to keep messages from
    %% the just-issued in-flight request.
    case {ReestablishedConnection, maps:get(drain_gun, Args, true)} of
        {false, true} -> drain_stale_gun_messages();
        _ -> ok
    end,
    StartTime = erlang:monotonic_time(),
    #{ peer := Peer, path := Path, method := Method } = Args,

    %% This call blocks until timeout, or until we think it's a good time to
    %% call the endpoint.
    arweave_throttling:throttle(Peer, Path),

    %% Count this request as in-flight for the whole call so evaluate_pools never
    %% shrinks a connection out from under an active (possibly long) stream.
    ets:update_counter(?HTTP_INFLIGHT_TABLE, Peer, 1, {Peer, 0}),
    Response = try
        case catch gen_server:call(?MODULE, {get_connection, Args}, 15000) of
                   {ok, PID} ->
                       case request(PID, Args) of
                           {error, Error} ->
                            case {ReestablishedConnection,
                                    should_retry_closed_connection(Error)} of
                                   {false, true} ->
                                       req(Args, true);
                                   {_, true} ->
                                       {error, client_error};
                                   {_, false} ->
                                       {error, Error}
                               end;
                           {ok, {{_Status, _}, Headers, _, _Start, _End}} = Reply ->
                               arweave_throttling:update_quota(Peer, Path, Headers),
                               Reply
                       end;
                   {'EXIT', _} -> {error, client_error};
                   Error -> Error
        end
    after
        ets:update_counter(?HTTP_INFLIGHT_TABLE, Peer, -1, {Peer, 0})
    end,
    EndTime = erlang:monotonic_time(),
    %% Only log the metric for the top-level call to req/2 - not the recursive call
    %% that happens when the connection is reestablished.
    case ReestablishedConnection of
        true ->
            ok;
        false ->
            %% NOTE: the erlang prometheus client looks at the metric name to determine units.
            %%       If it sees <name>_duration_<unit> it assumes the observed value is in
            %%       native units and it converts it to <unit> .To query native units, use:
            %%       erlant:monotonic_time() without any arguments.
            %%       See: https://github.com/deadtrickster/prometheus.erl/blob/6dd56bf321e99688108bb976283a80e4d82b3d30/src/prometheus_time.erl#L2-L84
            arweave_metrics:histogram_observe(ar_http_request_duration_seconds, [
                                                                            method_to_list(Method),
                                                                            ar_http_iface_server:label_http_path(list_to_binary(Path)),
                                                                            arweave_metrics:get_status_class(Response)
                                                                           ], EndTime - StartTime)
    end,
    Response.

%%% ==================================================================
%%% gen_server callbacks.
%%% ==================================================================

init([]) ->
    erlang:send_after(?POOL_EVAL_INTERVAL_MS, self(), evaluate_pools),
    {ok, #state{}}.

handle_call({get_connection, Args}, From,
        #state{ pid_by_peer = PIDByPeer, status_by_pid = StatusByPID,
                reqs_by_peer = ReqsByPeer } = State0) ->
    Peer = maps:get(peer, Args),
    %% Count the request so evaluate_pools can distinguish active from idle peers.
    State = State0#state{ reqs_by_peer =
            arweave_util:increment_map_value(Peer, ReqsByPeer) },
    PIDs = maps:get(Peer, PIDByPeer, []),
    %% Grow toward the fixed ceiling connections_per_peer; round-robin once full.
    Target = connections_per_peer(),
    case length(PIDs) < Target of
        true ->
            %% Grow this peer's pool: open a new connection and route this request
            %% to it (queued until it connects). This fills the pool over the first
            %% N requests to the peer. With connections_per_peer = 1 (the default)
            %% this is exactly the original single-connection behaviour.
            {ok, PID} = open_connection(Args),
            MonitorRef = monitor(process, PID),
            PIDByPeer2 = maps:put(Peer, [PID | PIDs], PIDByPeer),
            StatusByPID2 = maps:put(PID, {{connecting, [{From, Args}]}, MonitorRef, Peer},
                                    StatusByPID),
            {noreply, State#state{ pid_by_peer = PIDByPeer2, status_by_pid = StatusByPID2 }};
        false ->
            %% Pool full: round-robin across the peer's connections (rotate the list).
            {PID, Rotated} = rotate(PIDs),
            State2 = State#state{ pid_by_peer = maps:put(Peer, Rotated, PIDByPeer) },
            case maps:get(PID, StatusByPID) of
                {{connecting, PendingRequests}, MonitorRef, Peer} ->
                    StatusByPID2 = maps:put(PID, {{connecting,
                                                   [{From, Args} | PendingRequests]}, MonitorRef, Peer}, StatusByPID),
                    {noreply, State2#state{ status_by_pid = StatusByPID2 }};
                {connected, _MonitorRef, Peer} ->
                    {reply, {ok, PID}, State2}
            end
    end;

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({gun_up, PID, _Protocol}, #state{ status_by_pid = StatusByPID } = State) ->
    case maps:get(PID, StatusByPID, not_found) of
        not_found ->
            %% A connection timeout should have occurred.
            {noreply, State};
        {{connecting, PendingRequests}, MonitorRef, Peer} ->
            [gen_server:reply(ReplyTo, {ok, PID}) || {ReplyTo, _} <- PendingRequests],
            StatusByPID2 = maps:put(PID, {connected, MonitorRef, Peer}, StatusByPID),
            arweave_metrics:gauge_inc(outbound_connections),
            ar_peers:connected_peer(Peer),
            {noreply, State#state{ status_by_pid = StatusByPID2 }};
        {connected, _MonitorRef, Peer} ->
            ?LOG_WARNING([{event, gun_up_pid_already_exists},
                          {peer, arweave_util:format_peer(Peer)}]),
            ar_peers:connected_peer(Peer),
            {noreply, State}
    end;

handle_info({gun_error, PID, Reason},
            #state{ pid_by_peer = PIDByPeer, status_by_pid = StatusByPID } = State) ->
    case maps:get(PID, StatusByPID, not_found) of
        not_found ->
            ?LOG_WARNING([{even, gun_connection_error_with_unknown_pid}]),
            {noreply, State};
        {Status, _MonitorRef, Peer} ->
            PIDByPeer2 = remove_pid(Peer, PID, PIDByPeer),
            StatusByPID2 = maps:remove(PID, StatusByPID),
            Reason2 =
                case Reason of
                    timeout ->
                        connect_timeout;
                    {Type, _} ->
                        Type;
                    _ ->
                        Reason
                end,
            case Status of
                {connecting, PendingRequests} ->
                    reply_error(PendingRequests, Reason2);
                connected ->
                    arweave_metrics:gauge_dec(outbound_connections),
                    ok
            end,
            disconnected_if_last(Peer, PIDByPeer2),
            gun:shutdown(PID),
            ?LOG_DEBUG([{event, connection_error}, {reason, io_lib:format("~p", [Reason])}]),
            {noreply, State#state{ status_by_pid = StatusByPID2, pid_by_peer = PIDByPeer2 }}
    end;

%% missing pattern from gun 2.2+
handle_info({gun_down, Pid, Protocol, Reason, Streams}, State) ->
    handle_info({gun_down, Pid, Protocol, Reason, [], Streams}, State);

handle_info({gun_down, PID, Protocol, Reason, _KilledStreams, _UnprocessedStreams},
            #state{ pid_by_peer = PIDByPeer, status_by_pid = StatusByPID } = State) ->
    case maps:get(PID, StatusByPID, not_found) of
        not_found ->
            ?LOG_WARNING([{even, gun_connection_down_with_unknown_pid},
                          {protocol, Protocol}]),
            {noreply, State};
        {Status, _MonitorRef, Peer} ->
            PIDByPeer2 = remove_pid(Peer, PID, PIDByPeer),
            StatusByPID2 = maps:remove(PID, StatusByPID),
            Reason2 =
                case Reason of
                    {Type, _} ->
                        Type;
                    _ ->
                        Reason
                end,
            case Status of
                {connecting, PendingRequests} ->
                    reply_error(PendingRequests, Reason2);
                _ ->
                    arweave_metrics:gauge_dec(outbound_connections),
                    ok
            end,
            disconnected_if_last(Peer, PIDByPeer2),
            {noreply, State#state{ status_by_pid = StatusByPID2, pid_by_peer = PIDByPeer2 }}
    end;

handle_info({'DOWN', _Ref, process, PID, Reason},
            #state{ pid_by_peer = PIDByPeer, status_by_pid = StatusByPID } = State) ->
    case maps:get(PID, StatusByPID, not_found) of
        not_found ->
            {noreply, State};
        {Status, _MonitorRef, Peer} ->
            PIDByPeer2 = remove_pid(Peer, PID, PIDByPeer),
            StatusByPID2 = maps:remove(PID, StatusByPID),
            case Status of
                {connecting, PendingRequests} ->
                    reply_error(PendingRequests, Reason);
                _ ->
                    arweave_metrics:gauge_dec(outbound_connections),
                    ok
            end,
            disconnected_if_last(Peer, PIDByPeer2),
            {noreply, State#state{ status_by_pid = StatusByPID2, pid_by_peer = PIDByPeer2 }}
    end;

handle_info(evaluate_pools, #state{ pid_by_peer = PIDByPeer,
        reqs_by_peer = ReqsByPeer } = State) ->
    Max = connections_per_peer(),
    %% Shrink one connection per tick from any peer that is idle (no requests this
    %% tick) or above the ceiling (e.g. connections_per_peer lowered at runtime).
    %% Active peers keep the pool get_connection grew up to the ceiling. gun:shutdown
    %% routes through the existing 'DOWN' handler so cleanup stays in one place.
    maps:foreach(fun(Peer, PIDs) ->
            Idle = maps:get(Peer, ReqsByPeer, 0) == 0,
            %% Never shrink a peer with a request in flight - the connection we'd
            %% shut down (lists:last) may be carrying a live stream.
            NoInFlight = ets:lookup_element(?HTTP_INFLIGHT_TABLE, Peer, 2, 0) == 0,
            ShouldShrink = length(PIDs) > 1 andalso NoInFlight
                    andalso (Idle orelse length(PIDs) > Max),
            case ShouldShrink of
                true -> catch gun:shutdown(lists:last(PIDs));
                false -> ok
            end
        end, PIDByPeer),
    erlang:send_after(?POOL_EVAL_INTERVAL_MS, self(), evaluate_pools),
    {noreply, State#state{ reqs_by_peer = #{} }};

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
    {noreply, State}.

terminate(Reason, #state{ status_by_pid = StatusByPID }) ->
    maps:map(fun(PID, _Status) -> gun:shutdown(PID) end, StatusByPID),
    ?LOG_INFO([{event, http_client_terminating}, {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%% ==================================================================
%%% Private functions.
%%% ==================================================================

open_connection(#{ peer := Peer } = Args) ->
    {IPOrHost, Port} = get_ip_port(Peer),
    ConnectTimeout = maps:get(connect_timeout, Args,
                              maps:get(timeout, Args, ?HTTP_REQUEST_CONNECT_TIMEOUT)),
    ClosingTimeout = arweave_config:get(
                       [network, client, http, closing_timeout]),
    HTTPKeepalive = arweave_config:get(
                      [network, client, http, keepalive]),
    TCPDelaySend = arweave_config:get(
        [network, client, socket, delay_send]),
    TCPKeepalive = arweave_config:get(
        [network, client, socket, keepalive]),
    TCPLinger = arweave_config:get(
        [network, client, socket, linger]),
    TCPLingerTimeout = arweave_config:get(
        [network, client, socket, linger_timeout]),
    TCPNodelay = arweave_config:get(
        [network, client, socket, nodelay]),
    TCPSendTimeoutClose = arweave_config:get(
        [network, client, socket, send_timeout_close]),
    TCPSendTimeout = arweave_config:get(
        [network, client, socket, send_timeout]),
    GunOpts = #{
                retry => 0,
                connect_timeout => ConnectTimeout,
                http_opts => #{
                               closing_timeout => ClosingTimeout,
                               keepalive => HTTPKeepalive
                              },
                tcp_opts => [
                             {delay_send, TCPDelaySend},
                             {keepalive, TCPKeepalive},
                             {linger, {TCPLinger, TCPLingerTimeout}},
                             {nodelay, TCPNodelay},
                             {send_timeout_close, TCPSendTimeoutClose},
                             {send_timeout, TCPSendTimeout}
                            ]
               },
    gun:open(IPOrHost, Port, GunOpts).

get_ip_port({_, _} = Peer) ->
    Peer;
get_ip_port(Peer) ->
    {erlang:delete_element(size(Peer), Peer), erlang:element(size(Peer), Peer)}.

%% @doc Parallel HTTP connections to maintain per peer (>= 1). Read fresh each
%% call so a runtime change takes effect as connections are (re)opened. A single
%% peer isn't limited to one TCP flow / one gun process — see the connection-pool
%% plan. Defaults to 1 (original single-connection behaviour) if unset.
connections_per_peer() ->
    case catch arweave_config:get([network, client, http, connections_per_peer]) of
        N when is_integer(N), N >= 1 -> N;
        _ -> 1
    end.

%% @doc Round-robin: return the head connection and the list rotated by one, so
%% successive get_connection calls for a peer spread across its pool.
rotate([PID | Rest]) ->
    {PID, Rest ++ [PID]}.

%% @doc Remove a dead connection PID from a peer's pool, dropping the peer key
%% entirely once its last connection is gone.
remove_pid(Peer, PID, PIDByPeer) ->
    case maps:get(Peer, PIDByPeer, []) of
        [] ->
            PIDByPeer;
        PIDs ->
            case lists:delete(PID, PIDs) of
                [] -> maps:remove(Peer, PIDByPeer);
                Rest -> maps:put(Peer, Rest, PIDByPeer)
            end
    end.

%% @doc Mark the peer disconnected only when its last connection is gone. With a
%% multi-connection pool, one connection dying must not mark a peer that still has
%% healthy connections as disconnected. remove_pid/3 drops the peer key when its
%% last PID is removed, so an absent key means no connections remain.
disconnected_if_last(Peer, PIDByPeer) ->
    case maps:is_key(Peer, PIDByPeer) of
        false -> ar_peers:disconnected_peer(Peer);
        true -> ok
    end.

reply_error([], _Reason) ->
    ok;
reply_error([PendingRequest | PendingRequests], Reason) ->
    ReplyTo = element(1, PendingRequest),
    Args = element(2, PendingRequest),
    Method = maps:get(method, Args),
    Path = maps:get(path, Args),
    record_response_status(Method, Path, {error, Reason}),
    gen_server:reply(ReplyTo, {error, Reason}),
    reply_error(PendingRequests, Reason).

record_response_status(Method, Path, Response) ->
    arweave_metrics:counter_inc(gun_requests_total, 
                                [method_to_list(Method),
                                 ar_http_iface_server:label_http_path(list_to_binary(Path)),
                                 arweave_metrics:get_status_class(Response)]).

method_to_list(get) ->
    "GET";
method_to_list(post) ->
    "POST";
method_to_list(put) ->
    "PUT";
method_to_list(head) ->
    "HEAD";
method_to_list(delete) ->
    "DELETE";
method_to_list(connect) ->
    "CONNECT";
method_to_list(options) ->
    "OPTIONS";
method_to_list(trace) ->
    "TRACE";
method_to_list(patch) ->
    "PATCH";
method_to_list(_) ->
    "unknown".

request(PID, Args) ->
    Timeout = maps:get(timeout, Args, ?HTTP_REQUEST_SEND_TIMEOUT),
    Ref = request2(PID, Args),
    ResponseArgs = #{ pid => PID
                    , stream_ref => Ref
                    , timeout => Timeout
                      %% Default to ?MAX_BODY_SIZE, matching the server-side default in
                      %% ar_http_iface_middleware:read_complete_body/2. The client-side
                      %% and server-side limits are matched purely for the sake of
                      %% implementation simplicity.
                    , limit => maps:get(limit, Args, ?MAX_BODY_SIZE)
                    , counter => 0
                    , acc => []
                    , start => os:system_time(microsecond)
                    , is_peer_request => maps:get(is_peer_request, Args, true)
                    },
    Response = await_response(maps:merge(Args, ResponseArgs)),
    Method = maps:get(method, Args),
    Path = maps:get(path, Args),
    record_response_status(Method, Path, Response),
    Response.

request2(PID, #{ path := Path } = Args) ->
    Headers =
        case maps:get(is_peer_request, Args, true) of
            true ->
                merge_headers(?DEFAULT_REQUEST_HEADERS, maps:get(headers, Args, []));
            _ ->
                maps:get(headers, Args, [])
        end,
    Method = case maps:get(method, Args) of get -> "GET"; post -> "POST" end,
    gun:request(PID, Method, Path, Headers, maps:get(body, Args, <<>>)).

merge_headers(HeadersA, HeadersB) ->
    lists:ukeymerge(1, lists:keysort(1, HeadersB), lists:keysort(1, HeadersA)).

await_response( #{ pid := PID, stream_ref := Ref, timeout := Timeout
                 , start := Start, limit := Limit, counter := Counter
                 , acc := Acc, method := Method, path := Path } = Args) ->
    case gun:await(PID, Ref, Timeout) of
        {response, fin, Status, Headers} ->
            End = os:system_time(microsecond),
            upload_metric(Args),
            {ok, {{integer_to_binary(Status), <<>>}, Headers, <<>>, Start, End}};

        {response, nofin, Status, Headers} ->
            await_response(Args#{ status => Status, headers => Headers });

        {data, nofin, Data} ->
            case Limit of
                infinity ->
                    await_response(Args#{ acc := [Acc | Data] });
                Limit ->
                    Counter2 = size(Data) + Counter,
                    case Limit >= Counter2 of
                        true ->
                            await_response(Args#{ counter := Counter2, acc := [Acc | Data] });
                        false ->
                            log(err, http_fetched_too_much_data, Args,
                                <<"Fetched too much data">>),
                            {error, too_much_data}
                    end
            end;

        {data, fin, Data} ->
            End = os:system_time(microsecond),
            FinData = iolist_to_binary([Acc | Data]),
            download_metric(FinData, Args),
            upload_metric(Args),
            ResponseCode = gen_code_rest(maps:get(status, Args)),
            ResponseHeaders = maps:get(headers, Args),
            Response = {ResponseCode, ResponseHeaders, FinData, Start, End},
            {ok, Response};

        {error, timeout} = Response ->
            record_response_status(Method, Path, Response),
            gun:cancel(PID, Ref),
            log(warn, gun_await_process_down, Args, Response),
            Response;

        {error, Reason} = Response when is_tuple(Reason) ->
            record_response_status(Method, Path, Response),
            gun:cancel(PID, Ref),
            log(warn, gun_await_process_down, Args, Reason),
            Response;

        Response ->
            record_response_status(Method, Path, Response),
            gun:cancel(PID, Ref),
            log(warn, gun_await_unknown, Args, Response),
            Response
    end.

log(Type, Event, #{method := Method, peer := Peer, path := Path}, Reason) ->
    case arweave_config:get([features, http_logging]) of
        true when Type == warn ->
            ?LOG_WARNING([
                          {event, Event},
                          {http_method, Method},
                          {peer, arweave_util:format_peer(Peer)},
                          {path, Path},
                          {reason, Reason}
                         ]);
        true when Type == err ->
            ?LOG_ERROR([
                        {event, Event},
                        {http_method, Method},
                        {peer, arweave_util:format_peer(Peer)},
                        {path, Path},
                        {reason, Reason}
                       ]);
        _ ->
            ok
    end.

download_metric(Data, #{path := Path}) ->
    arweave_metrics:counter_inc(
      http_client_downloaded_bytes_total,
      [ar_http_iface_server:label_http_path(list_to_binary(Path))],
      byte_size(Data)
     ).

upload_metric(#{method := post, path := Path, body := Body}) ->
    arweave_metrics:counter_inc(
      http_client_uploaded_bytes_total,
      [ar_http_iface_server:label_http_path(list_to_binary(Path))],
      byte_size(Body)
     );

upload_metric(_) ->
    ok.

%% Non-blocking drain of any gun_* messages currently sitting in the caller's
%% mailbox. Late messages can still arrive after this returns; worker processes
%% that sit idle after ar_http:req may still need local handle_info ignore
%% clauses for those stragglers.
drain_stale_gun_messages() ->
    receive
        {gun_error, _, _, _} -> drain_stale_gun_messages();
        {gun_error, _, _} -> drain_stale_gun_messages();
        {gun_response, _, _, _, _, _} -> drain_stale_gun_messages();
        {gun_data, _, _, _, _} -> drain_stale_gun_messages();
        {gun_trailers, _, _, _} -> drain_stale_gun_messages();
        {gun_inform, _, _, _, _} -> drain_stale_gun_messages();
        {gun_push, _, _, _, _, _, _} -> drain_stale_gun_messages();
        {gun_down, _, _, _, _} -> drain_stale_gun_messages();
        {gun_down, _, _, _, _, _} -> drain_stale_gun_messages();
        {gun_up, _, _} -> drain_stale_gun_messages()
    after 0 -> ok
    end.

%% @doc True iff the failure reason means the gun connection or stream is
%% gone, so retrying on a freshly reopened connection can succeed; false for
%% application-level outcomes (`timeout', `too_much_data', HTTP status codes).
%% Matches on reason shape rather than enumerating gun's varying nested reasons.
should_retry_closed_connection({stream_error, _}) ->
    true;
should_retry_closed_connection({connection_error, _}) ->
    true;
should_retry_closed_connection({down, _}) ->
    true;
should_retry_closed_connection({shutdown, _}) ->
    true;
should_retry_closed_connection(noproc) ->
    true;
should_retry_closed_connection(closed) ->
    true;
should_retry_closed_connection(closing) ->
    true;
should_retry_closed_connection(_) ->
    false.

gen_code_rest(200) ->
    {<<"200">>, <<"OK">>};
gen_code_rest(201) ->
    {<<"201">>, <<"Created">>};
gen_code_rest(202) ->
    {<<"202">>, <<"Accepted">>};
gen_code_rest(208) ->
    {<<"208">>, <<"Transaction already processed">>};
gen_code_rest(400) ->
    {<<"400">>, <<"Bad Request">>};
gen_code_rest(419) ->
    {<<"419">>, <<"419 Missing Chunk">>};
gen_code_rest(421) ->
    {<<"421">>, <<"Misdirected Request">>};
gen_code_rest(429) ->
    {<<"429">>, <<"Too Many Requests">>};
gen_code_rest(N) ->
    {integer_to_binary(N), <<>>}.

%%%===================================================================
%%% Tests.
%%%===================================================================

configured_local_peer_calls_throttler_test_() ->
    ar_test_util:with_mocked([
                              {arweave_throttling, throttle, fun(Peer, Path) ->
                                                                     throw({throttle_called, Peer, Path})
                                                             end}
                             ], fun configured_local_peer_calls_throttler/0).

configured_local_peer_calls_throttler() ->
    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    ok = arweave_config:start(),
    ConfigSnapshot = arweave_config:snapshot(),
    Peer = {127, 0, 0, 1, arweave_config:get([port])},
    Path = "/info",
    try
        ok = arweave_config:set([peers, local], [Peer]),
        ?assertThrow({throttle_called, Peer, Path}, req(#{
                                                          peer => Peer,
                                                          path => Path,
                                                          method => get
                                                         }, false))
    after
        ok = arweave_config:restore(ConfigSnapshot),
        AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
        lists:foreach(fun application:stop/1, AppsNow -- AppsBefore)
    end.

-ifdef(AR_TEST).

%% Round-robin over a peer's connection pool: head is selected, list rotates, and
%% a full cycle returns to the start.
rotate_test() ->
    ?assertEqual({a, [b, c, a]}, rotate([a, b, c])),
    ?assertEqual({a, [a]}, rotate([a])),
    {P1, R1} = rotate([a, b, c]),
    {P2, R2} = rotate(R1),
    {P3, R3} = rotate(R2),
    ?assertEqual([a, b, c], [P1, P2, P3]),
    ?assertEqual([a, b, c], R3).

%% A dead connection is removed from its peer's pool; the peer key is dropped only
%% when its last connection is gone; unknown pid/peer is a no-op.
remove_pid_test() ->
    M = #{ peer1 => [a, b, c], peer2 => [x] },
    ?assertEqual(#{ peer1 => [a, c], peer2 => [x] }, remove_pid(peer1, b, M)),
    ?assertEqual(#{ peer1 => [a, b, c] }, remove_pid(peer2, x, M)),
    ?assertEqual(M, remove_pid(peer1, z, M)),
    ?assertEqual(M, remove_pid(peer3, a, M)).

%% Review point 2: one connection dying must not mark a peer disconnected while
%% other connections remain; only the loss of the last connection disconnects it.
disconnected_if_last_test() ->
    meck:new(ar_peers, [passthrough]),
    meck:expect(ar_peers, disconnected_peer, fun(_) -> ok end),
    disconnected_if_last(peer1, #{ peer1 => [pidA] }),
    ?assertEqual(0, meck:num_calls(ar_peers, disconnected_peer, [peer1])),
    disconnected_if_last(peer1, #{}),
    ?assertEqual(1, meck:num_calls(ar_peers, disconnected_peer, [peer1])),
    meck:unload(ar_peers).

%% Review point 1: evaluate_pools must not shut down a connection while the peer
%% has a request in flight (a long stream can span several idle 10s ticks).
evaluate_pools_inflight_test() ->
    catch ets:new(?HTTP_INFLIGHT_TABLE, [named_table, public, set]),
    meck:new(gun, [passthrough]),
    meck:expect(gun, shutdown, fun(_) -> ok end),
    State = #state{ pid_by_peer = #{ peer1 => [pidA, pidB] }, reqs_by_peer = #{} },
    %% Idle this tick but a request in flight -> must NOT shrink.
    ets:insert(?HTTP_INFLIGHT_TABLE, {peer1, 1}),
    handle_info(evaluate_pools, State),
    ?assertEqual(0, meck:num_calls(gun, shutdown, ['_'])),
    %% Nothing in flight -> the idle peer shrinks by one connection.
    ets:insert(?HTTP_INFLIGHT_TABLE, {peer1, 0}),
    handle_info(evaluate_pools, State),
    ?assertEqual(1, meck:num_calls(gun, shutdown, ['_'])),
    meck:unload(gun).

%% The restartable worker must accept the inflight table already owned by its
%% long-lived supervisor instead of trying to replace it during init.
init_reuses_supervisor_owned_inflight_table_test() ->
    catch ets:new(?HTTP_INFLIGHT_TABLE, [named_table, public, set]),
    Owner = ets:info(?HTTP_INFLIGHT_TABLE, owner),
    ?assertMatch({ok, #state{}}, init([])),
    ?assertEqual(Owner, ets:info(?HTTP_INFLIGHT_TABLE, owner)).

-endif.
