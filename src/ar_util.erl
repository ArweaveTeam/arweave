-module(ar_util).
-export([pick_random/1, pick_random/2]).
-export([hexify/1, dehexify/1]).
-export([parse_peer/1, parse_port/1, format_peer/1, unique/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% General misc. utility functions. Most of these should probably
%%% have been in the Erlang standard library...

%% @doc Pick a list of random elements from a given list.
pick_random(_, 0) -> [];
pick_random([], _) -> [];
pick_random(List, N) ->
	Elem = pick_random(List),
	[Elem|pick_random(List -- [Elem], N - 1)].

%% @doc Select a random element from a list.
pick_random(Xs) ->
	lists:nth(rand:uniform(length(Xs)), Xs).

%% @doc Convert a binary to a list of hex characters.
hexify(Bin) ->
	lists:flatten(
		[
			io_lib:format("~2.16.0B", [X])
		||
    		X <- binary_to_list(Bin)
		]
	).

%% @doc Turn a list of hex characters into a binary.
dehexify(Bin) when is_binary(Bin) ->
	dehexify(binary_to_list(Bin));
dehexify(S) ->
	dehexify(S, []).
dehexify([], Acc) ->
	list_to_binary(lists:reverse(Acc));
dehexify([X,Y|T], Acc) ->
	{ok, [V], []} = io_lib:fread("~16u", [X,Y]),
	dehexify(T, [V | Acc]).

%% @doc Parse a string representing a remote host into our internal format.
parse_peer(BitStr) when is_bitstring(BitStr) ->
	parse_peer(bitstring_to_list(BitStr));
parse_peer(Str) when is_list(Str) ->
	case io_lib:fread("~d.~d.~d.~d", Str) of
		{ok, [A, B, C, D], PortStr} ->
			{A, B, C, D, parse_port(PortStr)};
		{error, _} ->
			{127, 0, 0, 1, parse_port(Str)}
	end;
parse_peer({IP, Port}) ->
	{A, B, C, D} = parse_peer(IP),
	{A, B, C, D, parse_port(Port)}.

%% @doc Parses a port string into an integer.
parse_port(Int) when is_integer(Int) -> Int;
parse_port("") -> ?DEFAULT_HTTP_IFACE_PORT;
parse_port(PortStr) ->
	{ok, [Port], ""} = io_lib:fread(":~d", PortStr),
	Port.

%% @doc Take a remote host ID in various formats, return a HTTP-friendly string.
format_peer({A, B, C, D}) ->
	format_peer({A, B, C, D, ?DEFAULT_HTTP_IFACE_PORT});
format_peer({A, B, C, D, Port}) ->
	lists:flatten(io_lib:format("~w.~w.~w.~w:~w", [A, B, C, D, Port]));
format_peer(Host) when is_list(Host) ->
	format_peer({Host, ?DEFAULT_HTTP_IFACE_PORT});
format_peer({Host, Port}) ->
	lists:flatten(io_lib:format("~s:~w", [Host, Port])).

%% @doc Takes a list and return the unique values in it.
unique(Xs) -> unique([], Xs).
unique(Res, []) -> lists:reverse(Res);
unique(Res, [X|Xs]) ->
	case lists:member(X, Res) of
		false -> unique([X|Res], Xs);
		true -> unique(Res, Xs)
	end.

%% @doc Test that unique functions correctly.
basic_unique_test() ->
	[a, b, c] = unique([a, a, b, b, b, c, c]).

%% @doc Ensure that hosts are formatted as lists correctly.
basic_peer_format_test() ->
	"127.0.0.1:9001" = format_peer({127,0,0,1,9001}).

%% @doc Test that values can be hexed and dehexed.
round_trip_hexify_test() ->
	Bytes = dehexify(hexify(Bytes = crypto:strong_rand_bytes(32))).

%% @doc Ensure that pick_random's are actually in the starting list.
pick_random_test() ->
	List = [a, b, c, d, e],
	true = lists:member(pick_random(List), List).
