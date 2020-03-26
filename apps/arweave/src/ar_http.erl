%%% A wrapper library for gun.

-module(ar_http).

-export([req/1]).

-include("ar.hrl").

%%% ==================================================================
%%% API
%%% ==================================================================

req(#{peer := Peer} = Opts) ->
	{IpOrHost, Port} = get_ip_port(Peer),
	{ok, Pid} = gun:open(IpOrHost, Port),
	case gun:await_up(Pid, maps:get(connect_timeout, Opts, maps:get(timeout, Opts, ?HTTP_REQUEST_CONNECT_TIMEOUT))) of
		{ok, _} ->
			Timer = inet:start_timer(maps:get(timeout, Opts, ?HTTP_REQUEST_SEND_TIMEOUT)),
			RespOpts = #{
				pid => Pid,
				stream_ref => make_request(Pid, Opts),
				timer => Timer,
				limit => maps:get(limit, Opts, infinity),
				counter => 0,
				acc => [],
				start => os:system_time(microsecond),
				is_peer_request => maps:get(is_peer_request, Opts, true)
			},
			Resp = get_reponse(maps:merge(Opts, RespOpts)),
			gun:close(Pid),
			inet:stop_timer(Timer),
			Resp;
		{error, timeout} ->
			Resp = {error, connect_timeout},
			gun:close(Pid),
			log(warn, http_connect_timeout, Opts, Resp),
			Resp;
		{error, Reason} = Resp when is_tuple(Reason) ->
			gun:close(Pid),
			log(warn, gun_await_up_process_down, Opts, Reason),
			Resp;
		Unknown ->
			gun:close(Pid),
			log(warn, gun_await_up_unknown, Opts, Unknown),
			Unknown
	end.

%%% ==================================================================
%%% Internal functions
%%% ==================================================================

make_request(Pid, #{method := post, path := P} = Opts) ->
	Headers = case maps:get(is_peer_request, Opts, true) of
		true ->
			merge_headers(?DEFAULT_REQUEST_HEADERS, maps:get(headers, Opts, []));
		_ ->
			maps:get(headers, Opts, [])
	end,
	gun:post(Pid, P, Headers, maps:get(body, Opts, <<>>));
make_request(Pid, #{method := get, path := P} = Opts) ->
	gun:get(Pid, P, merge_headers(?DEFAULT_REQUEST_HEADERS, maps:get(headers, Opts, []))).

get_reponse(#{pid := Pid, stream_ref := SR, timer := T, start := S, limit := L, counter := C, acc := Acc} = Opts) ->
	case gun:await(Pid, SR, inet:timeout(T)) of
		{response, fin, Status, Headers} ->
			End = os:system_time(microsecond),
			store_data_time(Opts, <<>>, End - S),
			upload_metric(Opts),
			{ok, {{integer_to_binary(Status), <<>>}, Headers, <<>>, S, End}};
		{response, nofin, Status, Headers} ->
			get_reponse(Opts#{status => Status, headers => Headers});
		{data, nofin, Data} ->
			case L of
				infinity ->
					get_reponse(Opts#{acc := [Acc | Data]});
				L ->
					NewCounter = size(Data) + C,
					case L >= NewCounter of
						true ->
							get_reponse(Opts#{counter := NewCounter, acc := [Acc | Data]});
						false ->
							log(err, http_fetched_too_much_data, Opts, <<"Fetched too much data">>),
							{error, too_much_data}
					end
			end;
		{data, fin, Data} ->
			End = os:system_time(microsecond),
			FinData = iolist_to_binary([Acc | Data]),
			download_metric(FinData, Opts),
			upload_metric(Opts),
			store_data_time(Opts, FinData, End - S),
			{ok, {gen_code_rest(maps:get(status, Opts)), maps:get(headers, Opts), FinData, S, End}};
		{error, timeout} = Resp ->
			log(warn, gun_await_process_down, Opts, Resp),
			Resp;
		{error, Reason} = Resp when is_tuple(Reason) ->
			log(warn, gun_await_process_down, Opts, Reason),
			Resp;
		Unknown ->
			log(warn, gun_await_unknown, Opts, Unknown),
			Unknown
	end.

log(Type, Event, #{method := Method, peer := Peer, path := Path}, Reason) ->
	ar:Type([{event, Event}, {http_method, Method}, {peer, ar_util:format_peer(Peer)}, {path, Path}, {reason, Reason}]).

gen_code_rest(200) ->
	{<<"200">>, <<"OK">>};
gen_code_rest(201) ->
	{<<"201">>, <<"Created">>};
gen_code_rest(202) ->
	{<<"202">>, <<"Accepted">>};
gen_code_rest(400) ->
	{<<"400">>, <<"Bad Request">>};
gen_code_rest(421) ->
	{<<"421">>, <<"Misdirected Request">>};
gen_code_rest(429) ->
	{<<"429">>, <<"Too Many Requests">>};
gen_code_rest(N) ->
	{integer_to_binary(N), <<>>}.

upload_metric(#{method := post, path := Path, body := Body}) ->
	prometheus_counter:inc(
		http_client_uploaded_bytes_total,
		[ar_metrics:label_http_path(list_to_binary(Path))],
		byte_size(Body)
	);
upload_metric(_) ->
	ok.

download_metric(Data, #{path := Path}) ->
	prometheus_counter:inc(
		http_client_downloaded_bytes_total,
		[ar_metrics:label_http_path(list_to_binary(Path))],
		byte_size(Data)
	).

store_data_time(#{ is_peer_request := true, peer:= Peer }, Data, MicroSecs) ->
	P =
		case ar_meta_db:get({peer, Peer}) of
			not_found -> #performance{};
			X -> X
		end,
	ar_meta_db:put({peer, Peer},
		P#performance {
			transfers = P#performance.transfers + 1,
			time = P#performance.time + MicroSecs,
			bytes = P#performance.bytes + size(Data)
		}
	);
store_data_time(_, _, _) ->
	ok.

merge_headers(HeadersA, HeadersB) ->
	lists:ukeymerge(1, lists:keysort(1, HeadersB), lists:keysort(1, HeadersA)).

get_ip_port({_, _} = Peer) ->
	Peer;
get_ip_port(Peer) ->
	{erlang:delete_element(size(Peer), Peer), erlang:element(size(Peer), Peer)}.
