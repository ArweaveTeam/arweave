-module(ar_httpc).
-export([request/1, request/4, request/5, get_performance/1, update_timer/1]).
-export([reset_peer/1]).
-include("ar.hrl").

%%% A wrapper library for httpc.
%%% Performs HTTP calls and stores the peer and time-per-byte
%%% in the meta db.

%% @doc Perform a HTTP call with the httpc library, store the time required.
request(Peer) -> 
	%ar:report([{ar_httpc_request,Peer}]),
	Host="http://" ++ ar_util:format_peer(Peer),
	{ok, Client} = fusco:start(Host, [{connect_timeout, ?CONNECT_TIMEOUT}]),
	{ok, Request} = fusco:request(Client, <<"/">> , <<"GET">>, [], [], 1, ?NET_TIMEOUT),
	ok = fusco:disconnect(Client),
	Request.
request(Method, Peer, Path, Body) ->
	%ar:report([{ar_httpc_request,Peer},{method,Method}, {path,Path}]),
	Host="http://" ++ ar_util:format_peer(Peer),
	{ok, Client} = fusco:start(Host, [{connect_timeout, ?CONNECT_TIMEOUT}]),
	Result = fusco:request(Client, list_to_binary(Path), Method, [], Body, 1, ?NET_TIMEOUT),
	ok = fusco:disconnect(Client),
	case Result of
		{ok, {{_, _}, _, _, Start, End}} ->
			[_|RawIP] = string:split(Host, "//"),
			[IP|_Port] = string:split(RawIP, ":"),
			case Body of
				[] -> store_data_time(ar_util:parse_peer(IP), 0, End-Start);
				_ -> store_data_time(ar_util:parse_peer(IP), byte_size(Body), End-Start)
			end;
		_ -> ok
		end,
	Result.
request(Method, Peer, Path, Body, Timeout) ->
	%ar:report([{ar_httpc_request,Peer},{method,Method}, {path,Path}]),
	Host="http://" ++ ar_util:format_peer(Peer),
	{ok, Client} = fusco:start(Host, [{connect_timeout, ?CONNECT_TIMEOUT}]),
	Result = fusco:request(Client, list_to_binary(Path), Method, ?DEFAULT_REQUEST_HEADERS, Body, 1, Timeout),
	ok = fusco:disconnect(Client),
	case Result of
		{ok, {{_, _}, _, _, Start, End}} ->
			[_|RawIP] = string:split(Host, "//"),
			[IP|_Port] = string:split(RawIP, ":"),
			case Body of
				[] -> store_data_time(ar_util:parse_peer(IP), 0, End-Start);
				_ -> store_data_time(ar_util:parse_peer(IP), byte_size(Body), End-Start)
			end;
		_ -> ok
		end,
	Result.

%% @doc Update the database with new timing data.
store_data_time(IP, Bytes, MicroSecs) ->
	P =
		case ar_meta_db:get({peer, IP}) of
			not_found -> #performance{};
			X -> X
		end,
	ar_meta_db:put({peer, IP},
		P#performance {
			transfers = P#performance.transfers + 1,
			time = P#performance.time + MicroSecs,
			bytes = P#performance.bytes + Bytes
		}
	).

%% @doc Return the performance object for a node.
get_performance(IP) ->
	case ar_meta_db:get({peer, IP}) of
		not_found -> #performance{};
		P -> P
	end.

%% @doc Reset the performance data for a given peer.
reset_peer(IP) ->
	ar_meta_db:put({peer, IP}, #performance{}).

%% @doc Update the "last on list" timestamp of a given peer
update_timer(IP) ->
	case ar_meta_db:get({peer, IP}) of
		not_found -> #performance{};
		P -> 
			ar_meta_db:put({peer, IP},
				P#performance {
					transfers = P#performance.transfers,
					time = P#performance.time ,
					bytes = P#performance.bytes,
					timestamp = os:system_time(seconds)
				}
			)
	end.
