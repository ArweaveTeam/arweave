%%%
%%% @doc The module defines Arweave hard forks' heights.
%%%

-module(ar_fork).

-export([height_1_7/0, height_1_8/0, height_1_9/0, height_2_0/0]).

-include("ar.hrl").

-ifdef(DEBUG).
height_1_7() ->
	0.
-else.
height_1_7() ->
	235200. % Targeting 2019-07-08 UTC
-endif.

-ifdef(DEBUG).
height_1_8() ->
	0.
-else.
height_1_8() ->
	269510. % Targeting 2019-08-29 UTC
-endif.

-ifdef(DEBUG).
height_1_9() ->
	0.
-else.
height_1_9() ->
	315700. % Targeting 2019-11-04 UTC
-endif.

-ifdef(DEBUG).
height_2_0() ->
	0.
-else.
height_2_0() ->
	422250. % Targeting 2020-04-09 10:00 UTC
-endif.
