-module(ar_util).
-export([pick_random/1, pick_random/2]).

%%% General misc. utility functions. Most of these should probably
%%% have been in the Erlang standard library...

%% Pick a list of random elements from a given list.
pick_random(_, 0) -> [];
pick_random([], _) -> [];
pick_random(List, N) ->
	Elem = pick_random(List),
	[Elem|pick_random(List -- [Elem], N - 1)].

%% Select a random element from a list.
pick_random(Xs) ->
	lists:nth(rand:uniform(length(Xs)), Xs).
