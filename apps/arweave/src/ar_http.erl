%%% A wrapper library for gun.

-module(ar_http).

-export([req/1, gun_total_metric/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%% ==================================================================
%%% API
%%% ==================================================================

req(#{peer := Peer} = Opts) ->
	{IpOrHost, Port} = get_ip_port(Peer),
	{ok, Pid} = gun:open(IpOrHost, Port, #{ http_opts => #{ keepalive => infinity } }),
	ConnectTimeout =
		maps:get(connect_timeout, Opts, maps:get(timeout, Opts, ?HTTP_REQUEST_CONNECT_TIMEOUT)),
	case gun:await_up(Pid, ConnectTimeout) of
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
			gun_total_metric(Opts#{ response => Resp }),
			gun:close(Pid),
			inet:stop_timer(Timer),
			Resp;
		{error, timeout} ->
			Resp = {error, connect_timeout},
			gun:close(Pid),
			gun_total_metric(Opts#{ response => Resp }),
			log(warn, http_connect_timeout, Opts, Resp),
			Resp;
		{error, Reason} = Resp when is_tuple(Reason) ->
			gun:close(Pid),
			gun_total_metric(Opts#{ response => erlang:element(1, Reason) }),
			log(warn, gun_await_up_process_down, Opts, Reason),
			Resp;
		Unknown ->
			gun:close(Pid),
			gun_total_metric(Opts#{ response => Unknown }),
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

get_reponse(Opts) ->
	#{
		pid := Pid,
		stream_ref := SR,
		timer := T,
		start := S,
		limit := L,
		counter := C,
		acc := Acc
	} = Opts,
	case gun:await(Pid, SR, inet:timeout(T)) of
		{response, fin, Status, Headers} ->
			End = os:system_time(microsecond),
			update_peer_performance(Opts, <<>>, End - S),
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
							log(
								err,
								http_fetched_too_much_data,
								Opts,
								<<"Fetched too much data">>
							),
							{error, too_much_data}
					end
			end;
		{data, fin, Data} ->
			End = os:system_time(microsecond),
			FinData = iolist_to_binary([Acc | Data]),
			download_metric(FinData, Opts),
			upload_metric(Opts),
			update_peer_performance(Opts, FinData, End - S),
			{ok, {gen_code_rest(maps:get(status, Opts)), maps:get(headers, Opts), FinData, S, End}};
		{error, timeout} = Resp ->
			gun_total_metric(Opts#{ response => Resp }),
			log(warn, gun_await_process_down, Opts, Resp),
			Resp;
		{error, Reason} = Resp when is_tuple(Reason) ->
			gun_total_metric(Opts#{ response => erlang:element(1, Reason) }),
			log(warn, gun_await_process_down, Opts, Reason),
			Resp;
		Unknown ->
			gun_total_metric(Opts#{ response => Unknown }),
			log(warn, gun_await_unknown, Opts, Unknown),
			Unknown
	end.

log(Type, Event, #{method := Method, peer := Peer, path := Path}, Reason) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(http_logging, Config#config.enable) of
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

gun_total_metric(#{method := M, path := P, response := Resp}) ->
	prometheus_counter:inc(
		gun_requests_total,
		[method_to_list(M), ar_metrics:label_http_path(list_to_binary(P)), ar_metrics:get_status_class(Resp)]
	).

update_peer_performance(#{ is_peer_request := false }, _, _) ->
	ok;
update_peer_performance(#{ peer := Peer }, Data, MicroSecs) ->
	ar_meta_db:update_peer_performance(Peer, MicroSecs, size(Data)).

merge_headers(HeadersA, HeadersB) ->
	lists:ukeymerge(1, lists:keysort(1, HeadersB), lists:keysort(1, HeadersA)).

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

get_ip_port({_, _} = Peer) ->
	Peer;
get_ip_port(Peer) ->
	{erlang:delete_element(size(Peer), Peer), erlang:element(size(Peer), Peer)}.
