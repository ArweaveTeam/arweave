%%%
%%% @doc The module defines Arweave hard forks' heights.
%%%

-module(ar_fork).

-export([height_1_6/0, height_1_7/0, height_1_8/0, height_1_9/0, height_2_0/0, height_2_2/0,
		height_2_3/0, height_2_4/0, height_2_5/0, height_2_6/0, height_2_6_8/0,
		height_2_7/0, height_2_7_1/0, height_2_7_2/0,
		height_2_8/0]).

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

-ifdef(FORK_2_6_HEIGHT).
height_2_6() ->
	?FORK_2_6_HEIGHT.
-else.
	-ifdef(FORKS_RESET).
		height_2_6() ->
			0.
	-else.
		height_2_6() ->
			1132210. % Targeting 2023-03-06 14:00 UTC
	-endif.
-endif.

-ifdef(FORK_2_6_8_HEIGHT).
height_2_6_8() ->
	?FORK_2_6_8_HEIGHT.
-else.
	-ifdef(FORKS_RESET).
		height_2_6_8() ->
			0.
	-else.
		height_2_6_8() ->
			1189560. % Targeting 2023-05-30 16:00 UTC
	-endif.
-endif.

-ifdef(FORK_2_7_HEIGHT).
height_2_7() ->
	?FORK_2_7_HEIGHT.
-else.
	-ifdef(FORKS_RESET).
		height_2_7() ->
			0.
	-else.
		height_2_7() ->
			1275480. % Targeting 2023-10-04 14:00 UTC
	-endif.
-endif.

-ifdef(FORK_2_7_1_HEIGHT).
height_2_7_1() ->
	?FORK_2_7_1_HEIGHT.
-else.
	-ifdef(FORKS_RESET).
		height_2_7_1() ->
			0.
	-else.
		height_2_7_1() ->
			1316410. % Targeting 2023-12-05 14:00 UTC
	-endif.
-endif.

-ifdef(FORK_2_7_2_HEIGHT).
height_2_7_2() ->
	?FORK_2_7_2_HEIGHT.
-else.
	-ifdef(FORKS_RESET).
		height_2_7_2() ->
			0.
	-else.
		height_2_7_2() ->
			1391330. % Targeting 2024-03-26 14:00 UTC
	-endif.
-endif.

-ifdef(FORK_2_8_HEIGHT).
height_2_8() ->
	?FORK_2_8_HEIGHT.
-else.
	-ifdef(FORKS_RESET).
		height_2_8() ->
			0.
	-else.
		height_2_8() ->
			infinity.
	-endif.
-endif.
