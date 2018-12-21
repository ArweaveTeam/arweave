%%%
%%% @doc The module defines Arweave hard forks' heights.
%%%

-module(ar_fork).

-export([height_1_7/0, height_1_8/0]).

height_1_7() ->
	235200. % Targeting 2019-07-08 UTC

height_1_8() ->
	300000.
