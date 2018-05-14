-module(ar_httpc).
-export([request/1, request/4, get_performance/1, update_timer/1]).
-include("ar.hrl").

%%% A wrapper library for httpc.
%%% Performs HTTP calls and stores the peer and time-per-byte
%%% in the meta db.

%% Perform a HTTP call with the httpc library, store the time required.
request(URL) -> httpc:request(get, {URL, []}, [], []).
request(Method, Request, HTTPOpts, Opts) ->
	{MicroSecs, Res} =
		timer:tc(fun() -> httpc:request(Method, Request, HTTPOpts, Opts) end),
	store_data_time(Request, calculate_size(Request), MicroSecs),
	Res.

%% Return a number of bytes (after headers) of the size of a HTTP request.
calculate_size({_URL, _Headers}) -> 0;
calculate_size({_URL, _Headers, _ContentType, Body}) when is_list(Body) ->
	byte_size(unicode:characters_to_binary(Body));
calculate_size({_URL, _Headers, _ContentType, Body}) when is_binary(Body) ->
	byte_size(Body).

%% Update the database with new timing data.
store_data_time(Request, Bytes, MicroSecs) ->
	P =
		case ar_meta_db:get({peer, IP = get_ip(Request)}) of
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

%% Return the performance object for a node.
get_performance(IP) ->
	case ar_meta_db:get({peer, IP}) of
		not_found -> #performance{};
		P -> P
	end.

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

%% Extract an IP address from a httpc request() term.
get_ip({URL, _}) -> get_ip(URL);
get_ip({URL, _, _, _}) -> get_ip(URL);
get_ip("https://" ++ URL) -> get_ip(URL);
get_ip("http://" ++ URL) -> get_ip(URL);
get_ip(URL) -> ar_util:parse_peer(hd(string:tokens(URL, "/"))).