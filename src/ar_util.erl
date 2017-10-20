-module(ar_util).
-export([pick_random/1, pick_random/2]).
-export([hexify/1, dehexify/1]).

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
