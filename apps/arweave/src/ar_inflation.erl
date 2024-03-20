%%% @doc Module responsible for managing and testing the inflation schedule of 
%%% the Arweave main network.
-module(ar_inflation).

-export([calculate/1, blocks_per_year/1]).

-include_lib("arweave/include/ar_inflation.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Calculate the static reward received for mining a given block.
%% This reward portion depends only on block height, not the number of transactions.
-ifdef(DEBUG).
calculate(_Height) ->
	10.
-else.
calculate(Height) ->
	calculate2(Height).
-endif.

calculate2(Height) when Height =< ?FORK_15_HEIGHT ->
	pre_15_calculate(Height);
calculate2(Height) when Height =< ?PRE_25_BLOCKS_PER_YEAR ->
    calculate_base(Height) + ?POST_15_Y1_EXTRA;
calculate2(Height) ->
	case Height >= ar_fork:height_2_5() of
		true ->
			calculate_base(Height);
		false ->
			calculate_base_pre_fork_2_5(Height)
	end.

%% @doc An estimation for the number of blocks produced in a year.
%% Note: I've confirmed that when TARGET_BLOCK_TIME = 120 the following equation is
%% exactly equal to `30 * 24 * 365` when executed within an Erlang shell (i.e. 262800).
blocks_per_year(Height) ->
	((60 * 60 * 24 * 365) div ar_testnet:target_block_time(Height)).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Pre-1.5.0.0 style reward calculation.
pre_15_calculate(Height) ->
	RewardDelay = (?PRE_15_BLOCKS_PER_YEAR)/4,
	case Height =< RewardDelay of
		true ->
			1;
		false ->
			?WINSTON_PER_AR
				* 0.2
				* ?GENESIS_TOKENS
				* math:pow(2, -(Height - RewardDelay) / ?PRE_15_BLOCKS_PER_YEAR)
				* math:log(2)
				/ ?PRE_15_BLOCKS_PER_YEAR
	end.

calculate_base(Height) ->
	{Ln2Dividend, Ln2Divisor} = ?LN2,
	Dividend = Height * Ln2Dividend,
	Divisor = blocks_per_year(Height) * Ln2Divisor,
	Precision = ?INFLATION_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION,
	{EXDividend, EXDivisor} = ar_fraction:natural_exponent({Dividend, Divisor}, Precision),
	?GENESIS_TOKENS
		* ?WINSTON_PER_AR
		* EXDivisor
		* 2
		* Ln2Dividend
		div (
			10
			* blocks_per_year(Height)
			* Ln2Divisor
			* EXDividend
		).

calculate_base_pre_fork_2_5(Height) ->
	?WINSTON_PER_AR
		* (
			0.2
			* ?GENESIS_TOKENS
			* math:pow(2, -(Height) / ?PRE_25_BLOCKS_PER_YEAR)
			* math:log(2)
		)
		/ ?PRE_25_BLOCKS_PER_YEAR.

%%%===================================================================
%%% Tests.
%%%===================================================================

%% Test that the within tolerance helper function works as anticipated.
is_in_tolerance_test() ->
    true = is_in_tolerance(100, 100.5, 1),
    false = is_in_tolerance(100, 101.5, 1),
    true = is_in_tolerance(100.9, 100, 1),
    false = is_in_tolerance(101.1, 100, 1),
    true = is_in_tolerance(100.0001, 100, 0.01),
    false = is_in_tolerance(100.0001, 100, 0.00009),
    true = is_in_tolerance(?AR(100 * 1000000), ?AR(100 * 1000000) + 10, 0.01).

%%% Calculate and verify per-year expected and actual inflation.

year_1_test_() ->
	{timeout, 60, fun test_year_1/0}.

test_year_1() ->
    true = is_in_tolerance(year_sum_rewards(0), ?AR(5500000)).

year_2_test_() ->
	{timeout, 60, fun test_year_2/0}.

test_year_2() ->
    true = is_in_tolerance(year_sum_rewards(1), ?AR(2750000)).

year_3_test() ->
	{timeout, 60, fun test_year_3/0}.

test_year_3() ->
    true = is_in_tolerance(year_sum_rewards(2), ?AR(1375000)).

year_4_test() ->
	{timeout, 60, fun test_year_4/0}.

test_year_4() ->
    true = is_in_tolerance(year_sum_rewards(3), ?AR(687500)).

year_5_test() ->
	{timeout, 60, fun test_year_5/0}.

test_year_5() ->
    true = is_in_tolerance(year_sum_rewards(4), ?AR(343750)).

year_6_test() ->
	{timeout, 60, fun test_year_6/0}.

test_year_6() ->
    true = is_in_tolerance(year_sum_rewards(5), ?AR(171875)).

year_7_test() ->
	{timeout, 60, fun test_year_7/0}.

test_year_7() ->
    true = is_in_tolerance(year_sum_rewards(6), ?AR(85937.5)).

year_8_test() ->
	{timeout, 60, fun test_year_8/0}.

test_year_8() ->
    true = is_in_tolerance(year_sum_rewards(7), ?AR(42968.75)).

year_9_test() ->
	{timeout, 60, fun test_year_9/0}.

test_year_9() ->
    true = is_in_tolerance(year_sum_rewards(8), ?AR(21484.375)).

year_10_test() ->
	{timeout, 60, fun test_year_10/0}.

test_year_10() ->
    true = is_in_tolerance(year_sum_rewards(9), ?AR(10742.1875)).

%% @doc Is the value X within TolerancePercent of Y.
is_in_tolerance(X, Y) ->
    is_in_tolerance(X, Y, ?DEFAULT_TOLERANCE_PERCENT).
is_in_tolerance(X, Y, TolerancePercent) ->
    Tolerance = TolerancePercent / 100,
    ( X >= ( Y * (1 - Tolerance ) ) ) and
    ( X =< ( Y + (Y * Tolerance ) ) ).

%% @doc Count the total inflation rewards for a given year.
year_sum_rewards(YearNum) ->
    year_sum_rewards(YearNum, fun calculate2/1).
year_sum_rewards(YearNum, Fun) ->
    sum_rewards(
        Fun,
        (YearNum * trunc(?PRE_25_BLOCKS_PER_YEAR)),
        ((YearNum + 1) * trunc(?PRE_25_BLOCKS_PER_YEAR))
    ).

%% @doc Calculate the reward sum between two blocks.
sum_rewards(Fun, Start, End) ->
    lists:sum(lists:map(Fun, lists:seq(Start, End))).
