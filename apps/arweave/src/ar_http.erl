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
%% scheduler decision. Requests to peers go over HTTP/2 unless it is disabled or
%% the peer refused it.
-define(POOL_EVAL_INTERVAL_MS, 10_000).
%% Wait six hours before retrying HTTP/2 after a peer refuses it.
-define(HTTP1_FALLBACK_MS, (6 * 60 * 60 * 1000)).
%% Per-peer count of requests currently inside req/2 (throttle -> get_connection ->
%% gun request/await). evaluate_pools only shrinks a peer whose count is 0, so a
%% connection carrying a long-running stream - which can span several 10s ticks
%% while req_by_peer for that tick reads 0 - is never shut down mid-request.
-define(HTTP_INFLIGHT_TABLE, ar_http_inflight).

%% A connection in a peer's pool.
-record(connection, {
    peer,
    %% The protocol asked for (default: gun's choice), then the one gun_up
    %% reports, http or http2.
    protocol,
    %% {connecting, PendingRequests} until gun_up, then connected.
    status
}).

%% A peer's connection pool.
-record(pool, {
    %% The pool's connections, rotated for round-robin.
    pids = [],
    %% Request count since the last evaluate_pools tick; used only
    %% to tell active peers from idle ones (idle peers get a
    %% connection shrunk).
    requests = 0,
    %% After the peer refused HTTP/2, the monotonic time in
    %% milliseconds until which it is reached over HTTP/1.1.
    http1_until = undefined
}).

-record(state, {
    %% PID => #connection{}.
    connections = #{},
    %% Peer => #pool{}.
    pools = #{}
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
            {ok, PID, Protocol} ->
                case request(PID, Args) of
                    {error, Error} ->
                        case is_http2_refusal(Protocol, Error)
                                andalso maps:get(is_peer_request, Args, true) of
                            true ->
                                catch gen_server:call(?MODULE,
                                    {http2_refused, Peer, PID, Error}, 15000);
                            false ->
                                ok
                        end,
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
        #state{ connections = Connections } = State) ->
    Peer = maps:get(peer, Args),
    #pool{ pids = PoolPIDs, requests = Requests } = Pool = pool(Peer, State),
    {Protocol, PIDs} =
        case maps:get(is_peer_request, Args, true) of
            true ->
                Now = erlang:monotonic_time(millisecond),
                PeerProtocol = protocol(Pool, Now),
                %% A peer's protocol changes when it refuses HTTP/2, when that
                %% fallback ends, or when the protocol option changes. A refused
                %% response leaves its connection open, and round-robin would
                %% keep sending requests to it, so close the pool's
                %% connections of the old protocol.
                Kept = close_stale_connections(PeerProtocol, PoolPIDs,
                    Connections),
                {PeerProtocol, Kept};
            false ->
                %% Webhooks, blacklist sources and mining pools are not
                %% Arweave nodes: leave the protocol to gun, which speaks
                %% HTTP/1.1 unless TLS negotiates HTTP/2.
                {default, PoolPIDs}
        end,
    %% Count the request so evaluate_pools can distinguish active from idle peers.
    Pool2 = Pool#pool{ pids = PIDs, requests = Requests + 1 },
    %% Grow toward the fixed ceiling connections_per_peer; round-robin once full.
    Target = connections_per_peer(),
    case length(PIDs) < Target of
        true ->
            %% Grow this peer's pool: open a new connection and route this request
            %% to it (queued until it connects). This fills the pool over the first
            %% N requests to the peer. With connections_per_peer = 1 (the default)
            %% this is exactly the original single-connection behaviour.
            {ok, PID} = open_connection(Args, Protocol),
            monitor(process, PID),
            Connection = #connection{ peer = Peer, protocol = Protocol,
                    status = {connecting, [{From, Args}]} },
            State2 = put_pool(Peer, Pool2#pool{ pids = [PID | PIDs] }, State),
            {noreply, State2#state{
                    connections = maps:put(PID, Connection, Connections) }};
        false ->
            %% Pool full: round-robin across the peer's connections (rotate the list).
            {PID, Rotated} = rotate(PIDs),
            State2 = put_pool(Peer, Pool2#pool{ pids = Rotated }, State),
            case maps:get(PID, Connections) of
                #connection{ status = {connecting, Pending} } = Connection ->
                    Connection2 = Connection#connection{
                            status = {connecting, [{From, Args} | Pending]} },
                    Connections2 = maps:put(PID, Connection2, Connections),
                    {noreply, State2#state{ connections = Connections2 }};
                #connection{status = connected, protocol = PIDProtocol} ->
                    {reply, {ok, PID, PIDProtocol}, State2}
            end
    end;

handle_call({http2_refused, Peer, PID, Reason}, _From, State) ->
    %% The caller knows the negotiated protocol even if gun_down already
    %% removed PID. Exclude it before retrying, without waiting for gun_down.
    #pool{pids = PIDs} = Pool = pool(Peer, State),
    State2 = put_pool(Peer, Pool#pool{pids = lists:delete(PID, PIDs)}, State),
    gun:shutdown(PID),
    {reply, ok, fall_back_to_http1(Peer, Reason, State2)};

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({gun_up, PID, Protocol},
        #state{ connections = Connections } = State) ->
    case maps:get(PID, Connections, not_found) of
        not_found ->
            %% A connection timeout should have occurred.
            {noreply, State};
        #connection{ status = {connecting, PendingRequests}, peer = Peer }
                = Connection ->
            [gen_server:reply(ReplyTo, {ok, PID, Protocol})
                || {ReplyTo, _} <- PendingRequests],
            Connection2 = Connection#connection{ status = connected,
                    protocol = Protocol },
            arweave_metrics:gauge_inc(outbound_connections, [Protocol], 1),
            ar_peers:connected_peer(Peer),
            {noreply, State#state{
                    connections = maps:put(PID, Connection2, Connections) }};
        #connection{ status = connected, peer = Peer } ->
            ?LOG_WARNING([{event, gun_up_pid_already_exists},
                          {peer, arweave_util:format_peer(Peer)}]),
            ar_peers:connected_peer(Peer),
            {noreply, State}
    end;

handle_info({gun_error, PID, Reason}, State) ->
    Reason2 =
        case Reason of
            timeout ->
                connect_timeout;
            {Type, _} ->
                Type;
            _ ->
                Reason
        end,
    case remove_connection(PID, Reason2, State) of
        not_found ->
            ?LOG_WARNING([{even, gun_connection_error_with_unknown_pid}]),
            {noreply, State};
        {_Connection, State2} ->
            gun:shutdown(PID),
            ?LOG_DEBUG([{event, connection_error}, {reason, io_lib:format("~p", [Reason])}]),
            {noreply, State2}
    end;

%% missing pattern from gun 2.2+
handle_info({gun_down, Pid, Protocol, Reason, Streams}, State) ->
    handle_info({gun_down, Pid, Protocol, Reason, [], Streams}, State);

handle_info({gun_down, PID, Protocol, Reason, _KilledStreams, _UnprocessedStreams},
            State) ->
    Reason2 =
        case Reason of
            {Type, _} ->
                Type;
            _ ->
                Reason
        end,
    case remove_connection(PID, Reason2, State) of
        not_found ->
            ?LOG_WARNING([{even, gun_connection_down_with_unknown_pid},
                          {protocol, Protocol}]),
            {noreply, State};
        {#connection{ peer = Peer }, State2} ->
            %% Preserve fallback when Gun reports the cause to its owner
            %% before a request observes the failure.
            case is_http2_refusal(Protocol, Reason) of
                true -> {noreply, fall_back_to_http1(Peer, Reason, State2)};
                false -> {noreply, State2}
            end
    end;

handle_info({'DOWN', _Ref, process, PID, Reason}, State) ->
    case remove_connection(PID, Reason, State) of
        not_found ->
            {noreply, State};
        {_Connection, State2} ->
            {noreply, State2}
    end;

handle_info(evaluate_pools, #state{ pools = Pools } = State) ->
    Max = connections_per_peer(),
    %% Shrink one connection per tick from any peer that is idle (no requests this
    %% tick) or above the ceiling (e.g. connections_per_peer lowered at runtime).
    %% Active peers keep the pool get_connection grew up to the ceiling. gun:shutdown
    %% routes through the existing 'DOWN' handler so cleanup stays in one place.
    maps:foreach(fun(Peer, #pool{ pids = PIDs, requests = Requests }) ->
            Idle = Requests == 0,
            %% Never shrink a peer with a request in flight - the connection we'd
            %% shut down (lists:last) may be carrying a live stream.
            NoInFlight = ets:lookup_element(?HTTP_INFLIGHT_TABLE, Peer, 2, 0) == 0,
            ShouldShrink = length(PIDs) > 1 andalso NoInFlight
                    andalso (Idle orelse length(PIDs) > Max),
            case ShouldShrink of
                true -> catch gun:shutdown(lists:last(PIDs));
                false -> ok
            end
        end, Pools),
    erlang:send_after(?POOL_EVAL_INTERVAL_MS, self(), evaluate_pools),
    Now = erlang:monotonic_time(millisecond),
    Pools2 = maps:filtermap(
        fun(_Peer, Pool) -> reset_or_delete_pool(Pool, Now) end, Pools),
    {noreply, State#state{ pools = Pools2 }};

handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
    {noreply, State}.

terminate(Reason, #state{ connections = Connections }) ->
    maps:map(fun(PID, _Connection) -> gun:shutdown(PID) end, Connections),
    ?LOG_INFO([{event, http_client_terminating}, {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%% ==================================================================
%%% Private functions.
%%% ==================================================================

open_connection(#{ peer := Peer } = Args, Protocol) ->
    {IPOrHost, Port} = get_ip_port(Peer),
    ConnectTimeout = maps:get(connect_timeout, Args,
                              maps:get(timeout, Args, ?HTTP_REQUEST_CONNECT_TIMEOUT)),
    ClosingTimeout = arweave_config:get(
                       [network, client, http, closing_timeout]),
    HTTPKeepalive = arweave_config:get(
                      [network, client, http, keepalive]),
    TCPDelaySend = arweave_config:get(
                     [network, client, tcp, delay_send]),
    TCPKeepalive = arweave_config:get(
                     [network, client, tcp, keepalive]),
    TCPLinger = arweave_config:get(
                  [network, client, tcp, linger]),
    TCPLingerTimeout = arweave_config:get(
                         [network, client, tcp, linger_timeout]),
    TCPNodelay = arweave_config:get(
                   [network, client, tcp, nodelay]),
    TCPSendTimeoutClose = arweave_config:get(
                            [network, client, tcp, send_timeout_close]),
    TCPSendTimeout = arweave_config:get(
                       [network, client, tcp, send_timeout]),
    GunOpts = #{
                retry => 0,
                connect_timeout => ConnectTimeout,
                http_opts => #{
                               closing_timeout => ClosingTimeout,
                               keepalive => HTTPKeepalive
                              },
                http2_opts => #{
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
    %% gun speaks a single protocol over TCP (HTTP/1.1 unless told
    %% otherwise) and lets the server choose over TLS, which it uses for port
    %% 443. A peer request asks for the peer's protocol; any other request
    %% (default) keeps gun's choice.
    GunOpts2 =
        case Protocol of
            default -> GunOpts;
            _ -> GunOpts#{ protocols => [Protocol] }
        end,
    gun:open(IPOrHost, Port, GunOpts2).

get_ip_port({_, _} = Peer) ->
    Peer;
get_ip_port(Peer) ->
    {erlang:delete_element(size(Peer), Peer), erlang:element(size(Peer), Peer)}.

%% @doc Parallel HTTP connections to maintain per peer (>= 1). Read fresh each
%% call so a runtime change takes effect as connections are (re)opened. A single
%% peer isn't limited to one TCP flow / one gun process — see the connection-pool
%% plan. The option's type also accepts 0 and infinity, which mean one.
connections_per_peer() ->
    case arweave_config:get([network, client, http, connections_per_peer]) of
        N when is_integer(N), N >= 1 -> N;
        _ -> 1
    end.

%% @doc Return the protocol for a new connection in a peer's pool: the
%% configured one, or HTTP/1.1 if the peer refused HTTP/2 within
%% ?HTTP1_FALLBACK_MS.
protocol(#pool{ http1_until = Until }, Now) ->
    case arweave_config:get([network, client, http, protocol]) of
        http2 when is_integer(Until), Until > Now -> http;
        Protocol -> Protocol
    end.

%% @doc Recognize explicit HTTP/2 refusals in Gun's owner and request errors.
is_http2_refusal(http2, Reason) ->
    do_is_http2_refusal(Reason);
is_http2_refusal(_Protocol, _Reason) ->
    false.

do_is_http2_refusal({Wrapper, Reason}) when Wrapper == error;
        Wrapper == connection_error; Wrapper == stream_error; Wrapper == closed ->
    do_is_http2_refusal(Reason);
do_is_http2_refusal({Type, Code, _Description}) when Type == connection_error;
        Type == stream_error; Type == goaway ->
    do_is_http2_refusal({Code, undefined});
do_is_http2_refusal({Code, _Description}) when Code == protocol_error;
        Code == http_1_1_required ->
    true;
do_is_http2_refusal(_Reason) ->
    false.

fall_back_to_http1(Peer, Reason, State) ->
    Until = erlang:monotonic_time(millisecond) + ?HTTP1_FALLBACK_MS,
    ?LOG_DEBUG([{event, peer_refused_http2},
        {peer, arweave_util:format_peer(Peer)}, {reason, Reason},
        {http1_until, Until}]),
    Pool = pool(Peer, State),
    put_pool(Peer, Pool#pool{ http1_until = Until }, State).

%% @doc Close connections that do not match the peer's selected protocol.
close_stale_connections(Protocol, PIDs, Connections) ->
    SpeaksProtocol =
        fun(PID) ->
            #connection{ protocol = PIDProtocol } = maps:get(PID, Connections),
            PIDProtocol == Protocol
        end,
    {Kept, Others} = lists:partition(SpeaksProtocol, PIDs),
    lists:foreach(fun gun:shutdown/1, Others),
    Kept.

pool(Peer, #state{ pools = Pools }) ->
    maps:get(Peer, Pools, #pool{}).

put_pool(Peer, Pool, #state{ pools = Pools } = State) ->
    State#state{ pools = maps:put(Peer, Pool, Pools) }.

%% @doc Reset a pool's request count for the next evaluate_pools tick, or
%% delete the pool when it has no connections and no HTTP/1.1 fallback in
%% force.
reset_or_delete_pool(#pool{ pids = [], http1_until = Until }, Now)
        when not is_integer(Until); Until =< Now ->
    false;
reset_or_delete_pool(Pool, _Now) ->
    {true, Pool#pool{ requests = 0 }}.

%% @doc Round-robin: return the head connection and the list rotated by one, so
%% successive get_connection calls for a peer spread across its pool.
rotate([PID | Rest]) ->
    {PID, Rest ++ [PID]}.

%% @doc Take a connection that went down out of the state and its peer's pool,
%% failing the requests still waiting for it to connect.
remove_connection(PID, Reason, #state{ connections = Connections } = State) ->
    case maps:take(PID, Connections) of
        error ->
            not_found;
        {#connection{ peer = Peer, protocol = Protocol,
                status = Status } = Connection, Connections2} ->
            case Status of
                {connecting, PendingRequests} ->
                    reply_error(PendingRequests, Reason);
                connected ->
                    arweave_metrics:gauge_dec(outbound_connections,
                        [Protocol], 1)
            end,
            #pool{ pids = PIDs } = Pool = pool(Peer, State),
            State2 = put_pool(Peer, Pool#pool{ pids = lists:delete(PID, PIDs) },
                    State#state{ connections = Connections2 }),
            disconnected_if_last(Peer, State2),
            {Connection, State2}
    end.

%% @doc Mark the peer disconnected only when its last connection is gone. With a
%% multi-connection pool, one connection dying must not mark a peer that still has
%% healthy connections as disconnected.
disconnected_if_last(Peer, State) ->
    case pool(Peer, State) of
        #pool{ pids = [] } -> ar_peers:disconnected_peer(Peer);
        _ -> ok
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
    Headers = lowercase_names(maps:get(headers, Args, [])),
    Headers2 =
        case maps:get(is_peer_request, Args, true) of
            true ->
                merge_headers(?DEFAULT_REQUEST_HEADERS, Headers);
            _ ->
                Headers
        end,
    Method = case maps:get(method, Args) of get -> "GET"; post -> "POST" end,
    gun:request(PID, Method, Path, Headers2, maps:get(body, Args, <<>>)).

%% @doc Lowercase header names, which HTTP/2 requires.
lowercase_names(Headers) ->
    [{string:lowercase(Name), Value} || {Name, Value} <- Headers].

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

        {data, IsFin, Data} ->
            Counter2 = Counter + byte_size(Data),
            case Limit /= infinity andalso Counter2 > Limit of
                true ->
                    log(err, http_fetched_too_much_data, Args,
                        <<"Fetched too much data">>),
                    gun:cancel(PID, Ref),
                    {error, too_much_data};
                false when IsFin == nofin ->
                    await_response(Args#{ counter := Counter2,
                            acc := [Acc | Data] });
                false ->
                    End = os:system_time(microsecond),
                    FinData = iolist_to_binary([Acc | Data]),
                    download_metric(FinData, Args),
                    upload_metric(Args),
                    ResponseCode = gen_code_rest(maps:get(status, Args)),
                    ResponseHeaders = maps:get(headers, Args),
                    {ok, {ResponseCode, ResponseHeaders, FinData, Start, End}}
            end;

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
should_retry_closed_connection({stream_error, _, _}) ->
    true;
should_retry_closed_connection({connection_error, _}) ->
    true;
should_retry_closed_connection({connection_error, _, _}) ->
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

%% On gun_up, a connection's protocol becomes the one the message reports,
%% replacing the one it asked for.
gun_up_records_protocol_test() ->
    meck:new(ar_peers, [passthrough]),
    meck:expect(ar_peers, connected_peer, fun(_) -> ok end),
    Connection = #connection{ peer = {127, 0, 0, 1, 1984},
        protocol = default, status = {connecting, []} },
    State = #state{ connections = #{ pid1 => Connection } },
    {noreply, State2} = handle_info({gun_up, pid1, http2}, State),
    ?assertMatch(#connection{ protocol = http2, status = connected },
        maps:get(pid1, State2#state.connections)),
    meck:unload(ar_peers).

%% A connection that goes down leaves its peer's pool, and the peer is marked
%% disconnected only when its last connection is gone.
remove_connection_test() ->
    meck:new(ar_peers, [passthrough]),
    meck:expect(ar_peers, disconnected_peer, fun(_) -> ok end),
    Connected = #connection{ protocol = http, status = connected },
    State = #state{
        connections = #{
            a => Connected#connection{ peer = peer1 },
            b => Connected#connection{ peer = peer1 },
            x => Connected#connection{ peer = peer2 } },
        pools = #{
            peer1 => #pool{ pids = [a, b] },
            peer2 => #pool{ pids = [x] } } },
    {_, State2} = remove_connection(a, closed, State),
    ?assertEqual([b], (pool(peer1, State2))#pool.pids),
    ?assertNot(maps:is_key(a, State2#state.connections)),
    ?assertEqual(0, meck:num_calls(ar_peers, disconnected_peer, '_')),
    {_, State3} = remove_connection(x, closed, State2),
    ?assertEqual([], (pool(peer2, State3))#pool.pids),
    ?assertEqual(1, meck:num_calls(ar_peers, disconnected_peer, [peer2])),
    ?assertEqual(not_found, remove_connection(z, closed, State3)),
    meck:unload(ar_peers).

%% Review point 1: evaluate_pools must not shut down a connection while the peer
%% has a request in flight (a long stream can span several idle 10s ticks).
evaluate_pools_inflight_test() ->
    catch ets:new(?HTTP_INFLIGHT_TABLE, [named_table, public, set]),
    meck:new(gun, [passthrough]),
    meck:expect(gun, shutdown, fun(_) -> ok end),
    State = #state{ pools = #{ peer1 => #pool{ pids = [pidA, pidB] } } },
    %% Idle this tick but a request in flight -> must NOT shrink.
    ets:insert(?HTTP_INFLIGHT_TABLE, {peer1, 1}),
    handle_info(evaluate_pools, State),
    ?assertEqual(0, meck:num_calls(gun, shutdown, ['_'])),
    %% Nothing in flight -> the idle peer shrinks by one connection.
    ets:insert(?HTTP_INFLIGHT_TABLE, {peer1, 0}),
    handle_info(evaluate_pools, State),
    ?assertEqual(1, meck:num_calls(gun, shutdown, ['_'])),
    meck:unload(gun).

%% A pool's request count starts over each tick; a pool with no connections is
%% deleted unless its HTTP/1.1 fallback is still in force.
reset_or_delete_pool_test() ->
    ?assertEqual({true, #pool{ pids = [pid1], requests = 0 }},
        reset_or_delete_pool(#pool{ pids = [pid1], requests = 5 }, 0)),
    ?assertEqual(false, reset_or_delete_pool(#pool{}, 0)),
    ?assertEqual(false, reset_or_delete_pool(#pool{ http1_until = 0 }, 0)),
    ?assertMatch({true, _}, reset_or_delete_pool(#pool{ http1_until = 1 }, 0)).

%% A peer that refused HTTP/2 is reached over HTTP/1.1 until its fallback ends
%% (monotonic time is usually negative).
protocol_test() ->
    Pool = #pool{ http1_until = -2_000 },
    ?assertEqual(http, protocol(Pool, -2_001)),
    ?assertEqual(http2, protocol(Pool, -2_000)),
    ?assertEqual(http2, protocol(#pool{}, -5_000)).

%% @doc Only explicit refusals on HTTP/2 connections trigger fallback.
is_http2_refusal_test() ->
    ?assert(is_http2_refusal(http2, {error, {connection_error, protocol_error,
        'Invalid connection preface received. (RFC7540 3.5)'}})),
    ?assert(is_http2_refusal(http2, {stream_error, {stream_error, protocol_error,
        'Header names must be lowercase. (RFC7540 8.1.2)'}})),
    ?assert(is_http2_refusal(http2,
        {connection_error, {protocol_error, description}})),
    ?assert(is_http2_refusal(http2, {stream_error,
        {stream_error, http_1_1_required, description}})),
    ?assert(is_http2_refusal(http2, {stream_error,
        {goaway, http_1_1_required, description}})),
    ?assertNot(is_http2_refusal(http2,
        {error, {connection_error, enhance_your_calm,
            'Frame rate larger than configuration allows.'}})),
    ?assertNot(is_http2_refusal(http2, {down, noproc})),
    ?assertNot(is_http2_refusal(http2, {stream_error,
        {goaway, enhance_your_calm, description}})),
    ?assertNot(is_http2_refusal(http2, {stream_error, closed})),
    ?assertNot(is_http2_refusal(http2, timeout)),
    ?assertNot(is_http2_refusal(http,
        {connection_error, {protocol_error, description}})),
    ?assertNot(is_http2_refusal(http, {stream_error,
        {goaway, http_1_1_required, description}})).

%% @doc Refusal excludes the failed PID and sets a six-hour fallback.
http2_refused_test() ->
    Peer = {127, 0, 0, 1, 1984},
    Connection = #connection{peer = Peer, protocol = http2, status = connected},
    State = #state{connections = #{self() => Connection},
        pools = #{Peer => #pool{pids = [self()]}}},
    Error = {connection_error, {protocol_error, description}},
    Before = erlang:monotonic_time(millisecond),
    {reply, ok, State2} = handle_call(
        {http2_refused, Peer, self(), Error}, from, State),
    Now = erlang:monotonic_time(millisecond),
    Pool = pool(Peer, State2),
    Until = Pool#pool.http1_until,
    ?assertEqual([], Pool#pool.pids),
    ?assert(Until >= Before + timer:hours(6)),
    ?assert(Until =< Now + timer:hours(6)),
    ?assertEqual(http, protocol(Pool, Until - 1)),
    ?assertEqual(http2, protocol(Pool, Until)),
    receive {'$gen_cast', shutdown} -> ok after 0 -> error(no_shutdown) end.

%% @doc Refusal still records fallback if gun_down already removed the connection.
http2_refused_after_down_test() ->
    Peer = {127, 0, 0, 1, 1984},
    {reply, ok, State} = handle_call(
        {http2_refused, Peer, self(), {protocol_error, description}},
        from, #state{}),
    ?assertEqual(http, protocol(pool(Peer, State),
        erlang:monotonic_time(millisecond))),
    receive {'$gen_cast', shutdown} -> ok after 0 -> error(no_shutdown) end.

%% The restartable worker must accept the inflight table already owned by its
%% long-lived supervisor instead of trying to replace it during init.
init_reuses_supervisor_owned_inflight_table_test() ->
    catch ets:new(?HTTP_INFLIGHT_TABLE, [named_table, public, set]),
    Owner = ets:info(?HTTP_INFLIGHT_TABLE, owner),
    ?assertMatch({ok, #state{}}, init([])),
    ?assertEqual(Owner, ets:info(?HTTP_INFLIGHT_TABLE, owner)).

-endif.
