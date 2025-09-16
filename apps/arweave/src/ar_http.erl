%%% A wrapper library for gun.
-module(ar_http).

-behaviour(gen_server).

-export([start_link/0, req/1]).

-ifdef(AR_TEST).
-export([block_peer_connections/0, unblock_peer_connections/0]).
-endif.

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	pid_by_peer = #{},
	status_by_pid = #{}
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
	case arweave_config:get(port) == element(5, Peer) of
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
	StartTime = erlang:monotonic_time(),
	#{ peer := Peer, path := Path, method := Method } = Args,
	ok = ar_rate_limiter:throttle(Peer, Path),
	Response = case catch gen_server:call(?MODULE, {get_connection, Args}, 15000) of
		{ok, PID} ->
			case request(PID, Args) of
				{error, Error} when Error == {shutdown, normal}; Error == noproc ->
					case ReestablishedConnection of
						true ->
							{error, client_error};
						false ->
							req(Args, true)
					end;
				Reply ->
					Reply
			end;
		{'EXIT', _} -> {error, client_error};
		Error -> Error
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
			prometheus_histogram:observe(ar_http_request_duration_seconds, [
					method_to_list(Method),
					ar_http_iface_server:label_http_path(list_to_binary(Path)),
					ar_metrics:get_status_class(Response)
				], EndTime - StartTime)
	end,
	Response.

%%% ==================================================================
%%% gen_server callbacks.
%%% ==================================================================

init([]) ->
	{ok, #state{}}.

handle_call({get_connection, Args}, From,
		#state{ pid_by_peer = PIDByPeer, status_by_pid = StatusByPID } = State) ->
	Peer = maps:get(peer, Args),
	case maps:get(Peer, PIDByPeer, not_found) of
		not_found ->
			{ok, PID} = open_connection(Args),
			MonitorRef = monitor(process, PID),
			PIDByPeer2 = maps:put(Peer, PID, PIDByPeer),
			StatusByPID2 = maps:put(PID, {{connecting, [{From, Args}]}, MonitorRef, Peer},
					StatusByPID),
			{noreply, State#state{ pid_by_peer = PIDByPeer2, status_by_pid = StatusByPID2 }};
		PID ->
			case maps:get(PID, StatusByPID) of
				{{connecting, PendingRequests}, MonitorRef, Peer} ->
					StatusByPID2 = maps:put(PID, {{connecting,
							[{From, Args} | PendingRequests]}, MonitorRef, Peer}, StatusByPID),
					{noreply, State#state{ status_by_pid = StatusByPID2 }};
				{connected, _MonitorRef, Peer} ->
					{reply, {ok, PID}, State}
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
			prometheus_gauge:inc(outbound_connections),
			ar_peers:connected_peer(Peer),
			{noreply, State#state{ status_by_pid = StatusByPID2 }};
		{connected, _MonitorRef, Peer} ->
			?LOG_WARNING([{event, gun_up_pid_already_exists},
					{peer, ar_util:format_peer(Peer)}]),
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
			PIDByPeer2 = maps:remove(Peer, PIDByPeer),
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
					prometheus_gauge:dec(outbound_connections),
					ok
			end,
			ar_peers:disconnected_peer(Peer),
			gun:shutdown(PID),
			?LOG_DEBUG([{event, connection_error}, {reason, io_lib:format("~p", [Reason])}]),
			{noreply, State#state{ status_by_pid = StatusByPID2, pid_by_peer = PIDByPeer2 }}
	end;

% missing pattern from gun 2.2+
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
			PIDByPeer2 = maps:remove(Peer, PIDByPeer),
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
					prometheus_gauge:dec(outbound_connections),
					ok
			end,
			ar_peers:disconnected_peer(Peer),
			{noreply, State#state{ status_by_pid = StatusByPID2, pid_by_peer = PIDByPeer2 }}
	end;

handle_info({'DOWN', _Ref, process, PID, Reason},
		#state{ pid_by_peer = PIDByPeer, status_by_pid = StatusByPID } = State) ->
	case maps:get(PID, StatusByPID, not_found) of
		not_found ->
			{noreply, State};
		{Status, _MonitorRef, Peer} ->
			PIDByPeer2 = maps:remove(Peer, PIDByPeer),
			StatusByPID2 = maps:remove(PID, StatusByPID),
			case Status of
				{connecting, PendingRequests} ->
					reply_error(PendingRequests, Reason);
				_ ->
					prometheus_gauge:dec(outbound_connections),
					ok
			end,
			ar_peers:disconnected_peer(Peer),
			{noreply, State#state{ status_by_pid = StatusByPID2, pid_by_peer = PIDByPeer2 }}
	end;

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
	GunOpts = #{
		retry => 0,
		connect_timeout => ConnectTimeout,
		http_opts => #{
			closing_timeout => arweave_config:get('http_client.http.closing_timeout'),
			keepalive => arweave_config:get('http_client.http.keepalive')
		},
		tcp_opts => [
			{delay_send, arweave_config:get('http_client.tcp.delay_send')},
			{keepalive, arweave_config:get('http_client.tcp.keepalive')},
			{linger, {
			   		arweave_config:get('http_client.tcp.linger'),
					arweave_config:get('http_client.tcp.linger_timeout')
				}
			},
			{nodelay, arweave_config:get('http_client.tcp.nodelay')},
			{send_timeout_close, arweave_config:get('http_client.tcp.send_timeout_close')},
			{send_timeout, arweave_config:get('http_client.tcp.send_timeout')}
		]
	},
	gun:open(IPOrHost, Port, GunOpts).

get_ip_port({_, _} = Peer) ->
	Peer;
get_ip_port(Peer) ->
	{erlang:delete_element(size(Peer), Peer), erlang:element(size(Peer), Peer)}.

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
	prometheus_counter:inc(gun_requests_total, [method_to_list(Method),
			ar_http_iface_server:label_http_path(list_to_binary(Path)),
			ar_metrics:get_status_class(Response)]).

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
			, limit => maps:get(limit, Args, infinity)
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
	case lists:member(http_logging, arweave_config:get(enable)) of
		true when Type == warn ->
			?LOG_WARNING([
				{event, Event},
				{http_method, Method},
				{peer, ar_util:format_peer(Peer)},
				{path, Path},
				{reason, Reason}
			]);
		true when Type == err ->
			?LOG_ERROR([
				{event, Event},
				{http_method, Method},
				{peer, ar_util:format_peer(Peer)},
				{path, Path},
				{reason, Reason}
			]);
		_ ->
			ok
	end.

download_metric(Data, #{path := Path}) ->
	prometheus_counter:inc(
		http_client_downloaded_bytes_total,
		[ar_http_iface_server:label_http_path(list_to_binary(Path))],
		byte_size(Data)
	).

upload_metric(#{method := post, path := Path, body := Body}) ->
	prometheus_counter:inc(
		http_client_uploaded_bytes_total,
		[ar_http_iface_server:label_http_path(list_to_binary(Path))],
		byte_size(Body)
	);
upload_metric(_) ->
	ok.

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
