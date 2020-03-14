%%% A wrapper library for gun.

-module(ar_http).

-export([req/1]).

-include("ar.hrl").

%% -------------------------------------------------------------------
%% @doc
%% Wrapper of gun:post/4,  gun:get/3
%% @end
%% -------------------------------------------------------------------
req(#{peer := Peer} = Opts) ->
	{IP, Port} = {erlang:delete_element(size(Peer), Peer), erlang:element(size(Peer), Peer)},
	{ok, Pid} = gun:open(IP, Port, #{connect_timeout => maps:get(connect_timeout, Opts, infinity)}),
	StreamRef = gen_ref(Pid, Opts),
	RespOpts = #{
		pid => Pid,
		stream_ref => StreamRef,
		timeout => maps:get(timeout, Opts, ?HTTP_REQUEST_SEND_TIMEOUT),
		limit => maps:get(limit, Opts, infinity),
		counter => 0,
		acc => <<>>,
		start => os:system_time(microsecond)
	},
	Resp = gen_resp(maps:merge(Opts, RespOpts)),
	ok = gun:close(Pid),
	Resp.

%%% ==================================================================
%%% Internal functions
%%% ==================================================================

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Generate reference based on method
%% @end
%% -------------------------------------------------------------------
gen_ref(Pid, #{method := post, path := P} = Opts) ->
	gun:post(Pid, P, maps:get(headers, Opts, []), maps:get(body, Opts, <<>>));
gen_ref(Pid, #{method := get, path := P} = Opts) ->
	gun:get(Pid, P, maps:get(headers, Opts, [])).

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Generate response data
%% @end
%% -------------------------------------------------------------------
gen_resp(#{pid := Pid, stream_ref := SR, timeout := T, start := S} = Opts) ->
	MRef = erlang:monitor(process, Pid),
	receive
		{gun_response, Pid, SR, fin, Status, Headers} ->
			End = os:system_time(microsecond),
			_ = store_data_time(maps:get(peer, Opts), S, End),
			{ok, {{integer_to_binary(Status), <<>>}, Headers, <<>>, S, End}};
		{gun_response, Pid, SR, nofin, Status, Headers} ->
			case recv_chunks(Opts#{mref => MRef}) of
				chunk_limit ->
					err(http_fetched_too_much_data, ?MODULE, "gen_resp/1", ?LINE, <<"Fetched too much data">>),
					{error, too_much_data};
				Data ->
					End = os:system_time(microsecond),
					_ = store_data_time(maps:get(peer, Opts), size(Data), End),
					{ok, {gen_code_rest(Status), Headers, Data, S, End}}
			end;
		{'DOWN', MRef, process, Pid, Reason} ->
			err(http_response_data_process_down, ?MODULE, "gen_resp/1", ?LINE, Reason),
			exit(Reason)
	after T ->
		err(http_response_timeout, ?MODULE, "gen_resp/1", ?LINE, timeout),
		exit(timeout)
	end.

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Receive data by chunks
%% @end
%% -------------------------------------------------------------------
recv_chunks(#{pid := Pid, mref := MRef, stream_ref := SR, limit := L, counter := C, acc := Acc} = Opts) ->
	receive
		{gun_data, Pid, SR, nofin, Data} ->
			case L of
				infinity ->
					recv_chunks(Opts#{acc := <<Acc/binary, Data/binary>>});
				L ->
					NewCounter = size(Data) + C,
					case L >= NewCounter of
						true ->
							recv_chunks(Opts#{counter := NewCounter, acc := <<Acc/binary, Data/binary>>});
						false ->
							chunk_limit
					end
			end;
		{gun_data, Pid, SR, fin, Data} ->
			FinData = <<Acc/binary, Data/binary>>,
			_ = download_metric(FinData, Opts),
			_ = upload_metric(Opts),
			FinData;
		{'DOWN', MRef, process, Pid, Reason} ->
			err(http_receive_data_process_down, ?MODULE, "recv_chunks/1", ?LINE, Reason),
			exit(Reason)
	end.

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Logging error
%% @end
%% -------------------------------------------------------------------
err(Event, Module, FunInfo, Line, Reason) ->
	ar:err([{event, Event}, {module, Module}, {function, FunInfo}, {line, Line}, {reason, Reason}]).

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Generate code response
%% @end
%% -------------------------------------------------------------------
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

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Metric for upload data
%% @end
%% -------------------------------------------------------------------
upload_metric(#{method := post, path := Path, body := Body}) ->
	prometheus_counter:inc(
		http_client_uploaded_bytes_total,
		[ar_metrics:label_http_path(list_to_binary(Path))],
		byte_size(Body)
	);
upload_metric(_) ->
	ok.

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Metric for download data
%% @end
%% -------------------------------------------------------------------
download_metric(Data, #{path := Path}) ->
	prometheus_counter:inc(
		http_client_downloaded_bytes_total,
		[ar_metrics:label_http_path(list_to_binary(Path))],
		byte_size(Data)
	).

%% -------------------------------------------------------------------
%% @private
%% @doc
%% Update the database with new timing data
%% @end
%% -------------------------------------------------------------------
store_data_time(Peer, Bytes, MicroSecs) ->
	P =
		case ar_meta_db:get({peer, Peer}) of
			not_found -> #performance{};
			X -> X
		end,
	ar_meta_db:put({peer, Peer},
		P#performance {
			transfers = P#performance.transfers + 1,
			time = P#performance.time + MicroSecs,
			bytes = P#performance.bytes + Bytes
		}
	).
