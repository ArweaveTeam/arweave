-module(ar_inflation).
-export([calculate/1, calculate_post_15_y1_extra/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% @doc Module responsible for managing and testing the inflation schedule of 
%%% the Arweave main network.

%% How many blocks are created in a year?
-define(BLOCKS_PER_YEAR, (30 * 24 * 365)).
%% Default test error tolerance in percent.
-define(DEFAULT_TOLERANCE_PERCENT, 0.001).
%% Height at which the 1.5.0.0 fork takes affect.
-define(FORK_15_HEIGHT, 95000).
%% Height at which block rewards began.
-define(REWARD_START_HEIGHT, 65700).
%% @doc BLOCK_PER_YEAR macro prior to 1.5.0.0 release.
-define(PRE_15_BLOCK_PER_YEAR, 525600 / (?TARGET_TIME/60) ).

%% @doc The number of extra tokens to grant for blocks between the 1.5.0.0 release
%% and the end of year one.
-define(POST_15_Y1_EXTRA, 13275279633337).

%% @doc Calculate the static reward received for mining a given block.
%% This reward portion depends only on block height, not the number of transactions.
calculate(Height) when Height =< ?FORK_15_HEIGHT ->
	pre_15_calculate(Height);
calculate(Height) when Height =< ?BLOCKS_PER_YEAR ->
    calculate_base(Height) + ?POST_15_Y1_EXTRA;
calculate(Height) ->
    calculate_base(Height).

calculate_base(Height) ->
	?AR(
        (0.2 * ?GENESIS_TOKENS * math:pow(2,-(Height)/?BLOCK_PER_YEAR) * math:log(2))
        /
        ?BLOCK_PER_YEAR
    ).

%% @doc Pre-1.5.0.0 style reward calculation.
pre_15_calculate(Height) when Height =< ?REWARD_DELAY ->
	1;
pre_15_calculate(Height) ->
	?AR(
        (0.2 * ?GENESIS_TOKENS * math:pow(2,-(Height-?REWARD_DELAY)/?PRE_15_BLOCK_PER_YEAR) * math:log(2))
        /
        ?PRE_15_BLOCK_PER_YEAR
    ).

%%% Helper functions

%% @doc Is the value X within TolerancePercent of Y.
is_in_tolerance(X, Y) ->
    is_in_tolerance(X, Y, ?DEFAULT_TOLERANCE_PERCENT).
is_in_tolerance(X, Y, TolerancePercent) ->
    Tolerance = TolerancePercent / 100,
    ( X >= ( Y * (1 - Tolerance ) ) ) and
    ( X =< ( Y + (Y * Tolerance ) ) ).

%% @doc Count the total inflation rewards for a given year.
year_sum_rewards(YearNum) ->
    year_sum_rewards(YearNum, fun calculate/1).
year_sum_rewards(YearNum, Fun) ->
    sum_rewards(
        Fun,
        (YearNum * ?BLOCKS_PER_YEAR),
        ((YearNum + 1) * ?BLOCKS_PER_YEAR)
    ).

%% @doc Calculate the reward sum between two blocks.
sum_rewards(Fun, Start, End) ->
    lists:sum(lists:map(Fun, lists:seq(Start, End))).

%% @doc Calculate the value used in the ?POST_15_Y1_EXTRA macro.
%% The value is memoized to avoid frequent large computational load.
calculate_post_15_y1_extra() ->
    Pre15 = erlang:trunc(sum_rewards(fun calculate/1, 0, ?FORK_15_HEIGHT)),
    Base = erlang:trunc(sum_rewards(fun calculate_base/1, 0, ?FORK_15_HEIGHT)),
    Post15Diff = Base - Pre15,
    erlang:trunc(Post15Diff / (?BLOCKS_PER_YEAR - ?FORK_15_HEIGHT)).

%%% Tests

%% @doc Test that the within tolerance helper function works as anticipated.
is_in_tolerance_test() ->
    true = is_in_tolerance(100, 100.5, 1),
    false = is_in_tolerance(100, 101.5, 1),
    true = is_in_tolerance(100.9, 100, 1),
    false = is_in_tolerance(101.1, 100, 1),
    true = is_in_tolerance(100.0001, 100, 0.01),
    false = is_in_tolerance(100.0001, 100, 0.00009),
    true = is_in_tolerance(?AR(100 * 1000000), ?AR(100 * 1000000) + 10, 0.01).

%%% Calculate and verify per-year expected and actual inflation.

year_1_test() ->
    true = is_in_tolerance(year_sum_rewards(0), ?AR(5500000)).

year_2_test() ->
    true = is_in_tolerance(year_sum_rewards(1), ?AR(2750000)).

year_3_test() ->
    true = is_in_tolerance(year_sum_rewards(2), ?AR(1375000)).

year_4_test() ->
    true = is_in_tolerance(year_sum_rewards(3), ?AR(687500)).

year_5_test() ->
    true = is_in_tolerance(year_sum_rewards(4), ?AR(343750)).

year_6_test() ->
    true = is_in_tolerance(year_sum_rewards(5), ?AR(171875)).

year_7_test() ->
    true = is_in_tolerance(year_sum_rewards(6), ?AR(85937.5)).

year_8_test() ->
    true = is_in_tolerance(year_sum_rewards(7), ?AR(42968.75)).

year_9_test() ->
    true = is_in_tolerance(year_sum_rewards(8), ?AR(21484.375)).

year_10_test() ->
    true = is_in_tolerance(year_sum_rewards(9), ?AR(10742.1875)).