-module(ar_httpc).
-export([request/1, request/3, request/4, request/5, request/6, get_performance/1, update_timer/1]).
-export([reset_peer/1]).
-include("ar.hrl").

%%% A wrapper library for httpc.
%%% Performs HTTP calls and stores the peer and time-per-byte
%%% in the meta db.

%% @doc Perform a HTTP call with the httpc library, store the time required.
request(Peer) ->
	request(<<"GET">>, Peer, "/", [], <<>>).

request(Method, Peer, Path) ->
	request(Method, Peer, Path, []).

request(Method, Peer, Path, Headers) ->
	request(Method, Peer, Path, Headers, <<>>).

request(Method, Peer, Path, Headers, Body) ->
	request(Method, Peer, Path, Headers, Body, default_timeout).

request(Method, Peer, Path, Headers, Body, Timeout) ->
	Host = "http://" ++ ar_util:format_peer(Peer),
	{ok, Client} = fusco:start(
		Host,
		[{connect_timeout, connect_timeout(Timeout)}]
	),
	Result = fusco:request(
		Client,
		list_to_binary(Path),
		Method,
		merge_headers(?DEFAULT_REQUEST_HEADERS, Headers),
		Body,
		1,
		request_timeout(Timeout)
	),
	case Body of
		[] ->
			noop;
		_ ->
			prometheus_counter:inc(
				http_client_uploaded_bytes_total,
				[ar_metrics:label_http_path(list_to_binary(Path))],
				byte_size(Body)
			)
	end,
	ok = fusco:disconnect(Client),
	case Result of
		{ok, {{_, _}, _, ResponseBody, Start, End}} ->
			prometheus_counter:inc(
				http_client_downloaded_bytes_total,
				[ar_metrics:label_http_path(list_to_binary(Path))],
				byte_size(ResponseBody)
			),
			case Body of
				[] -> store_data_time(Peer, 0, End-Start);
				_ -> store_data_time(Peer, byte_size(Body), End-Start)
			end;
		_ -> ok
		end,
	Result.

connect_timeout(default_timeout) ->
	?HTTP_REQUEST_CONNECT_TIMEOUT;
connect_timeout(Timeout) ->
	Timeout.

request_timeout(default_timeout) ->
	?HTTP_REQUEST_SEND_TIMEOUT;
request_timeout(Timeout) ->
	Timeout.

%% @doc Merges proplists with headers. For duplicates, HeadersB has precedence.
merge_headers(HeadersA, HeadersB) ->
	lists:ukeymerge(
		1,
		lists:keysort(1, HeadersB),
		lists:keysort(1, HeadersA)
	).

%% @doc Update the database with new timing data.
store_data_time(Peer = {_, _, _, _, _}, Bytes, MicroSecs) ->
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

%% @doc Return the performance object for a node.
get_performance(Peer = {_, _, _, _, _}) ->
	case ar_meta_db:get({peer, Peer}) of
		not_found -> #performance{};
		P -> P
	end.

%% @doc Reset the performance data for a given peer.
reset_peer(Peer = {_, _, _, _, _}) ->
	ar_meta_db:put({peer, Peer}, #performance{}).

%% @doc Update the "last on list" timestamp of a given peer
update_timer(Peer = {_, _, _, _, _}) ->
	case ar_meta_db:get({peer, Peer}) of
		not_found -> #performance{};
		P ->
			ar_meta_db:put({peer, Peer},
				P#performance {
					transfers = P#performance.transfers,
					time = P#performance.time ,
					bytes = P#performance.bytes,
					timestamp = os:system_time(seconds)
				}
			)
	end.
