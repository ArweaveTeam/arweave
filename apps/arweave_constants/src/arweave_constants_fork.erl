%%%
%%% @doc The module defines Arweave hard forks' heights.
%%%

-module(arweave_constants_fork).

-export([
    height_1_6/0,
    height_1_7/0,
    height_1_8/0,
    height_1_9/0,
    height_2_0/0,
    height_2_2/0,
    height_2_3/0,
    height_2_4/0,
    height_2_5/0,
    height_2_6/0,
    height_2_6_8/0,
    height_2_7/0,
    height_2_7_1/0,
    height_2_7_2/0,
    height_2_8/0,
    height_2_9/0,
    height_2_9_6/0
]).

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
    % Targeting 2019-07-08 UTC
    235200.
-endif.

-ifdef(FORKS_RESET).
height_1_8() ->
    0.
-else.
height_1_8() ->
    % Targeting 2019-08-29 UTC
    269510.
-endif.

-ifdef(FORKS_RESET).
height_1_9() ->
    0.
-else.
height_1_9() ->
    % Targeting 2019-11-04 UTC
    315700.
-endif.

-ifdef(FORKS_RESET).
height_2_0() ->
    0.
-else.
height_2_0() ->
    % Targeting 2020-04-09 10:00 UTC
    422250.
-endif.

-ifdef(FORKS_RESET).
height_2_2() ->
    0.
-else.
height_2_2() ->
    % Targeting 2020-10-21 13:00 UTC
    552180.
-endif.

-ifdef(FORKS_RESET).
height_2_3() ->
    0.
-else.
height_2_3() ->
    % Targeting 2020-12-21 11:00 UTC
    591140.
-endif.

-ifdef(FORKS_RESET).
height_2_4() ->
    0.
-else.
height_2_4() ->
    % Targeting 2021-02-24 11:50 UTC
    633720.
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
    % Targeting 2023-03-06 14:00 UTC
    1132210.
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
    % Targeting 2023-05-30 16:00 UTC
    1189560.
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
    % Targeting 2023-10-04 14:00 UTC
    1275480.
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
    % Targeting 2023-12-05 14:00 UTC
    1316410.
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
    % Targeting 2024-03-26 14:00 UTC
    1391330.
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
    % Targeting 2024-11-13 14:00 UTC
    1547120.
-endif.
-endif.

-ifdef(FORK_2_9_HEIGHT).
height_2_9() ->
    ?FORK_2_9_HEIGHT.
-else.
-ifdef(FORKS_RESET).
height_2_9() ->
    0.
-else.
height_2_9() ->
    % Targeting 2025-02-03 14:00 UTC
    1602350.
-endif.
-endif.

-ifdef(FORK_2_9_6_HEIGHT).
height_2_9_6() ->
    ?FORK_2_9_6_HEIGHT.
-else.
-ifdef(FORKS_RESET).
height_2_9_6() ->
    0.
-else.
height_2_9_6() ->
    % Targeting 2026-09-13 12:00 UTC
    2_000_000.
-endif.
-endif.
