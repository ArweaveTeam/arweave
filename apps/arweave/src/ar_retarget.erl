%%% @doc A helper module for deciding when and which blocks will be retarget
%%% blocks, that is those in which change the current mining difficulty
%%% on the weave to maintain a constant block time.
%%% @end
-module(ar_retarget).

-export([is_retarget_height/1, is_retarget_block/1, maybe_retarget/5,
		calculate_difficulty/5, validate_difficulty/2,
		switch_to_linear_diff/1, switch_to_linear_diff_pre_fork_2_5/1, switch_to_log_diff/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%% A macro for checking if the given block is a retarget block.
%% Returns true if so, otherwise returns false.
-define(IS_RETARGET_BLOCK(X),
		(
			((X#block.height rem ?RETARGET_BLOCKS) == 0) and
			(X#block.height =/= 0)
		)
	).

%% A macro for checking if the given height is a retarget height.
%% Returns true if so, otherwise returns false.
-define(IS_RETARGET_HEIGHT(Height),
		(
			((Height rem ?RETARGET_BLOCKS) == 0) and
			(Height =/= 0)
		)
	).

%% @doc The unconditional difficulty reduction coefficient applied at the
%% first 2.5 block.
-define(DIFF_DROP_2_5, 2).

%% @doc The unconditional difficulty reduction coefficient applied at the
%% first 2.6 block.
-define(INITIAL_DIFF_DROP_2_6, 100).

%% @doc The additional difficulty reduction coefficient applied every 10 minutes at the
%% first 2.6 block.
-define(DIFF_DROP_2_6, 2).

%% @doc The unconditional difficulty reduction coefficient applied at the
%% first 2.7.2 block.
-define(INITIAL_DIFF_DROP_2_7_2, 10).

%% @doc The additional difficulty reduction coefficient applied every 10 minutes at the
%% first 2.7.2 block.
-define(DIFF_DROP_2_7_2, 2).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return true if the given height is a retarget height.
is_retarget_height(Height) ->
	?IS_RETARGET_HEIGHT(Height).

%% @doc Return true if the given block is a retarget block.
is_retarget_block(Block) ->
	?IS_RETARGET_BLOCK(Block).

maybe_retarget(Height, {CurPoA1Diff, CurDiff}, TS, LastRetargetTS, PrevTS) ->
	case ar_retarget:is_retarget_height(Height) of
		true ->
			NewDiff = calculate_difficulty(CurDiff, TS, LastRetargetTS, Height, PrevTS),
			{ar_difficulty:poa1_diff(NewDiff, Height), NewDiff};
		false ->
			{CurPoA1Diff, CurDiff}
	end.

calculate_difficulty(OldDiff, TS, Last, Height, PrevTS) ->
	Fork_1_7 = ar_fork:height_1_7(),
	Fork_1_8 = ar_fork:height_1_8(),
	Fork_1_9 = ar_fork:height_1_9(),
	Fork_2_4 = ar_fork:height_2_4(),
	Fork_2_5 = ar_fork:height_2_5(),
	Fork_2_6 = ar_fork:height_2_6(),
	Fork_2_7_2 = ar_fork:height_2_7_2(),
	Fork_Testnet = ar_testnet:height_testnet_fork(),
	case Height of
		_ when Height == Fork_Testnet ->
			calculate_difficulty_with_drop(OldDiff, TS, Last, Height, PrevTS, 100, 2);
		_ when Height == Fork_2_7_2 ->
			calculate_difficulty_with_drop(OldDiff, TS, Last, Height, PrevTS,
					?INITIAL_DIFF_DROP_2_7_2, ?DIFF_DROP_2_7_2);
		_ when Height == Fork_2_6 ->
			calculate_difficulty_with_drop(OldDiff, TS, Last, Height, PrevTS,
					?INITIAL_DIFF_DROP_2_6, ?DIFF_DROP_2_6);
		_ when Height > Fork_2_5 ->
			calculate_difficulty(OldDiff, TS, Last, Height);
		_ when Height == Fork_2_5 ->
			calculate_difficulty_at_2_5(OldDiff, TS, Last, Height, PrevTS);
		_ when Height > Fork_2_4 ->
			calculate_difficulty_after_2_4_before_2_5(OldDiff, TS, Last, Height);
		_ when Height == Fork_2_4 ->
			calculate_difficulty_at_2_4(OldDiff, TS, Last, Height);
		_ when Height >= Fork_1_9 ->
			calculate_difficulty_at_and_after_1_9_before_2_4(OldDiff, TS, Last, Height);
		_ when Height > Fork_1_8 ->
			calculate_difficulty_after_1_8_before_1_9(OldDiff, TS, Last, Height);
		_ when Height == Fork_1_8 ->
			switch_to_linear_diff_pre_fork_2_5(OldDiff);
		_ when Height == Fork_1_7 ->
			ar_difficulty:switch_to_randomx_fork_diff(OldDiff);
		_ ->
			calculate_difficulty_before_1_8(OldDiff, TS, Last, Height)
	end.

%% @doc Assert the new block has an appropriate difficulty.
validate_difficulty(NewB, OldB) ->
	case ar_retarget:is_retarget_block(NewB) of
		true ->
			(NewB#block.diff ==
				calculate_difficulty(
					OldB#block.diff, NewB#block.timestamp, OldB#block.last_retarget,
					NewB#block.height, OldB#block.timestamp));
		false ->
			(NewB#block.diff == OldB#block.diff) and
				(NewB#block.last_retarget == OldB#block.last_retarget)
	end.

%% @doc The number a hash must be greater than, to give the same odds of success
%% as the old-style Diff (number of leading zeros in the bitstring).
switch_to_linear_diff(LogDiff) ->
	?MAX_DIFF - ar_fraction:pow(2, 256 - LogDiff).

switch_to_linear_diff_pre_fork_2_5(Diff) ->
	erlang:trunc(math:pow(2, 256)) - erlang:trunc(math:pow(2, 256 - Diff)).

%% @doc only used for logging/metrics as the log diff is easier to understand than the linear diff
switch_to_log_diff(LinearDiff) ->
	256 - math:log2(?MAX_DIFF - LinearDiff).

%%%===================================================================
%%% Private functions.
%%%===================================================================

calculate_difficulty(OldDiff, TS, Last, Height) ->
	%% We only do retarget if the time it took to mine ?RETARGET_BLOCKS is bigger than
	%% or equal to RetargetToleranceUpperBound or smaller than or equal to
	%% RetargetToleranceLowerBound.
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	TargetTimeUpperBound = TargetTime + ar_testnet:target_block_time(Height),
	TargetTimeLowerBound = TargetTime - ar_testnet:target_block_time(Height),
	ActualTime = max(TS - Last, ar_block:get_max_timestamp_deviation()),

	case ActualTime < TargetTimeUpperBound andalso ActualTime > TargetTimeLowerBound of
		true ->
			OldDiff;
		false ->
			%% Scale difficulty by TargetTime / ActualTime
			%% If ActualTime is less than TargetTime it means we need to *increase* the difficulty,
			%% and vice versa.
			ar_difficulty:scale_diff(OldDiff, {TargetTime, ActualTime}, Height)
	end.

calculate_difficulty_at_2_5(OldDiff, TS, Last, Height, PrevTS) ->
	calculate_difficulty_with_drop(OldDiff, TS, Last, Height, PrevTS, ?DIFF_DROP_2_5,
			?DIFF_DROP_2_5).

calculate_difficulty_with_drop(OldDiff, TS, Last, Height, PrevTS, InitialCoeff, Coeff) ->
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	ActualTime = max(TS - Last, ar_block:get_max_timestamp_deviation()),
	Step = 10 * 60,
	%% Drop the difficulty InitialCoeff times right away, then drop extra Coeff times
	%% for every 10 minutes passed.
	ActualTime2 = ActualTime * InitialCoeff
			* ar_fraction:pow(Coeff, max(TS - PrevTS, 0) div Step),
	%% Scale difficulty by TargetTime / ActualTime2
	%% If ActualTime2 is less than TargetTime it means we need to *increase* the difficulty,
	%% and vice versa.
	ar_difficulty:scale_diff(OldDiff, {TargetTime, ActualTime2}, Height).

calculate_difficulty_after_2_4_before_2_5(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ?MAX_DIFF,
			MinDiff = ar_difficulty:min_difficulty(Height),
			DiffInverse = erlang:trunc((MaxDiff - OldDiff) * TimeDelta),
			ar_util:between(
				MaxDiff - DiffInverse,
				MinDiff,
				MaxDiff
			)
	end.

calculate_difficulty_at_2_4(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	ActualTime = TS - Last,
	%% Make the difficulty drop 10 times faster than usual. The difficulty
	%% after SPoRA is estimated to be around 10-100 times lower. In the worst
	%% case, the 10x adjustment leads to a block per 12 seconds on average,
	%% what is a reasonable lower bound on the block time. In case of the 100x
	%% reduction in difficulty, it would only take 100 minutes to adjust.
	TimeDelta = 10 * ActualTime / TargetTime,
	MaxDiff = ?MAX_DIFF,
	MinDiff = ar_difficulty:min_difficulty(Height),
	DiffInverse = erlang:trunc((MaxDiff - OldDiff) * TimeDelta),
	ar_util:between(
		MaxDiff - DiffInverse,
		MinDiff,
		MaxDiff
	).

calculate_difficulty_at_and_after_1_9_before_2_4(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ?MAX_DIFF,
			MinDiff = ar_difficulty:min_difficulty(Height),
			EffectiveTimeDelta = ar_util:between(
				ActualTime / TargetTime,
				1 / ?DIFF_ADJUSTMENT_UP_LIMIT,
				?DIFF_ADJUSTMENT_DOWN_LIMIT
			),
			DiffInverse = erlang:trunc((MaxDiff - OldDiff) * EffectiveTimeDelta),
			ar_util:between(
				MaxDiff - DiffInverse,
				MinDiff,
				MaxDiff
			)
	end.

calculate_difficulty_after_1_8_before_1_9(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ?MAX_DIFF,
			MinDiff = ar_difficulty:min_difficulty(Height),
			ar_util:between(
				MaxDiff - (MaxDiff - OldDiff) * ActualTime div TargetTime,
				max(MinDiff, OldDiff div 2),
				min(MaxDiff, OldDiff * 4)
			)
	end.

calculate_difficulty_before_1_8(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ar_testnet:target_block_time(Height),
	ActualTime = TS - Last,
	TimeError = abs(ActualTime - TargetTime),
	Diff = erlang:max(
		if
			TimeError < (TargetTime * ?RETARGET_TOLERANCE) -> OldDiff;
			TargetTime > ActualTime                        -> OldDiff + 1;
			true                                           -> OldDiff - 1
		end,
		ar_difficulty:min_difficulty(Height)
	),
	Diff.

%%%===================================================================
%%% Tests.
%%%===================================================================

%% Ensure that after a series of very fast mines, the diff increases.
simple_retarget_test_() ->
	{timeout, 300, fun() ->
		[B0] = ar_weave:init([]),
		ar_test_node:start(B0),
		lists:foreach(
			fun(Height) ->
				ar_test_node:mine(),
				ar_test_node:wait_until_height(Height)
			end,
			lists:seq(1, ?RETARGET_BLOCKS + 1)
		),
		true = ar_util:do_until(
			fun() ->
				[BH | _] = ar_node:get_blocks(),
				B = ar_storage:read_block(BH),
				B#block.diff > B0#block.diff
			end,
			1000,
			5 * 60 * 1000
		)
	end}.

calculate_difficulty_linear_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> 0 end}],
		fun test_calculate_difficulty_linear/0, 120).

test_calculate_difficulty_linear() ->
	Diff = switch_to_linear_diff(27),
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_BLOCK_TIME,
	Timestamp = os:system_time(seconds),
	%% The change is smaller than retarget tolerance.
	Retarget1 = Timestamp - TargetTime - ?TARGET_BLOCK_TIME + 1,
	?assertEqual(
		Diff,
		calculate_difficulty(Diff, Timestamp, Retarget1, 1)
	),
	Retarget2 = Timestamp - TargetTime + ?TARGET_BLOCK_TIME - 1,
	?assertEqual(
		Diff,
		calculate_difficulty(Diff, Timestamp, Retarget2, 1)
	),
	%% The change is not capped by ?DIFF_ADJUSTMENT_UP_LIMIT anymore.
	Retarget3 = Timestamp - TargetTime div (?DIFF_ADJUSTMENT_UP_LIMIT + 1),
	?assertEqual(
		(?DIFF_ADJUSTMENT_UP_LIMIT + 1) * hashes(Diff),
		hashes(
			calculate_difficulty(Diff, Timestamp, Retarget3, 1)
		)
	),
	%% The change is not capped by ?DIFF_ADJUSTMENT_DOWN_LIMIT anymore.
	Retarget4 = Timestamp - (?DIFF_ADJUSTMENT_DOWN_LIMIT + 2) * TargetTime,
	?assertEqual(
		hashes(Diff),
		(?DIFF_ADJUSTMENT_DOWN_LIMIT + 2) * hashes(
			calculate_difficulty(Diff, Timestamp, Retarget4, 1)
		)
	),
	%% The actual time is three times smaller.
	Retarget5 = Timestamp - TargetTime div 3,
	?assert(
		3.001 * hashes(Diff)
			> hashes(
				calculate_difficulty(Diff, Timestamp, Retarget5, 1)
			)
	),
	?assert(
		3.001 / 2 * hashes(Diff)
			> hashes( % Expect 2x drop at 2.5.
				calculate_difficulty_at_2_5(Diff, Timestamp, Retarget5, 0, Timestamp - 1)
			)
	),
	?assert(
		2.999 * hashes(Diff)
			< hashes(
				calculate_difficulty(Diff, Timestamp, Retarget5, 1)
			)
	),
	?assert(
		2.999 / 2 * hashes(Diff)
			< hashes( % Expect 2x drop at 2.5.
				calculate_difficulty_at_2_5(Diff, Timestamp, Retarget5, 0, Timestamp - 1)
			)
	),
	%% The actual time is two times bigger.
	Retarget6 = Timestamp - 2 * TargetTime,
	?assert(
		hashes(Diff)
			> 1.999 * hashes(
				calculate_difficulty(Diff, Timestamp, Retarget6, 1)
			)
	),
	?assert(
		hashes(Diff)
			> 3.999 * hashes( % Expect 2x drop at 2.5.
				calculate_difficulty_at_2_5(Diff, Timestamp, Retarget6, 0, Timestamp - 1)
			)
	),
	?assert(
		hashes(Diff)
			> 7.999 * hashes( % Expect extra 2x after 10 minutes.
				calculate_difficulty_at_2_5(Diff, Timestamp, Retarget6, 0, Timestamp - 600)
			)
	),
	?assert(
		hashes(Diff)
			< 2.001 * hashes(
				calculate_difficulty(Diff, Timestamp, Retarget6, 1)
			)
	),
	?assert(
		hashes(Diff)
			< 4.001 * hashes( % Expect 2x drop at 2.5.
				calculate_difficulty_at_2_5(Diff, Timestamp, Retarget6, 0, Timestamp - 1)
			)
	),
	?assert(
		hashes(Diff)
			< 8.001 * hashes( % Expect extra 2x after 10 minutes.
				calculate_difficulty_at_2_5(Diff, Timestamp, Retarget6, 0, Timestamp - 600)
			)
	).

hashes(Diff) ->
	MaxDiff = ?MAX_DIFF,
	MaxDiff div (MaxDiff - Diff).
