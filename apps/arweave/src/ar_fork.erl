%%%
%%% @doc The module defines Arweave hard forks' heights.
%%%

-module(ar_fork).

-export([height_1_6/0, height_1_7/0, height_1_8/0, height_1_9/0, height_2_0/0, height_2_2/0,
		height_2_3/0, height_2_4/0, height_2_5/0, height_2_6/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-ifdef(FORKS_RESET).
height_1_6() ->
	0.
-else.
height_1_6() ->
	95000.
-endif.

-ifdef(FORKS_RESET).
height_1_7() ->
	0.
-else.
height_1_7() ->
	235200. % Targeting 2019-07-08 UTC
-endif.

-ifdef(FORKS_RESET).
height_1_8() ->
	0.
-else.
height_1_8() ->
	269510. % Targeting 2019-08-29 UTC
-endif.

-ifdef(FORKS_RESET).
height_1_9() ->
	0.
-else.
height_1_9() ->
	315700. % Targeting 2019-11-04 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_0() ->
	0.
-else.
height_2_0() ->
	422250. % Targeting 2020-04-09 10:00 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_2() ->
	0.
-else.
height_2_2() ->
	552180. % Targeting 2020-10-21 13:00 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_3() ->
	0.
-else.
height_2_3() ->
	591140. % Targeting 2020-12-21 11:00 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_4() ->
	0.
-else.
height_2_4() ->
	633720. % Targeting 2021-02-24 11:50 UTC
-endif.

-ifdef(FORKS_RESET).
height_2_5() ->
	0.
-else.
height_2_5() ->
	812970.
-endif.

-ifdef(FORKS_RESET).
height_2_6() ->
	0.
-else.
height_2_6() ->
	1072170.
-endif.
