%%% @doc A helper module for deciding when and which blocks will be retarget
%%% blocks, that is those in which change the current mining difficulty
%%% on the weave to maintain a constant block time.
%%% @end
-module(ar_retarget).

-export([
	is_retarget_height/1,
	maybe_retarget/4,
	calculate_difficulty/4,
	validate_difficulty/2,
	switch_to_linear_diff/1
]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

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

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Checls if the given height is a retarget height.
%% Reteurns true if so, otherwise returns false.
%% @end
is_retarget_height(Height) ->
	?IS_RETARGET_HEIGHT(Height).

%% @doc Maybe set a new difficulty and last retarget, if the block is at
%% an appropriate retarget height, else returns the current diff
%% @end
maybe_retarget(Height, CurDiff, TS, Last) when ?IS_RETARGET_HEIGHT(Height) ->
	calculate_difficulty(
		CurDiff,
		TS,
		Last,
		Height
	);
maybe_retarget(_Height, CurDiff, _TS, _Last) ->
	CurDiff.

%% @doc Calculate a new difficulty, given an old difficulty and the period
%% since the last retarget occcurred.
%% @end
calculate_difficulty(OldDiff, TS, Last, Height) ->
	case
		{ar_fork:height_1_7(), ar_fork:height_1_8(), ar_fork:height_1_9(), ar_fork:height_2_4()}
	of
		{_, _, _, Height_2_4} when Height > Height_2_4 ->
			calculate_difficulty2(OldDiff, TS, Last, Height);
		{_, _, _, Height} ->
			calculate_difficulty_at_2_4(OldDiff, TS, Last, Height);
		{_, _, Height_1_9, _} when Height >= Height_1_9 ->
			calculate_difficulty_at_and_after_1_9_before_2_4(OldDiff, TS, Last, Height);
		{_, Height_1_8, _, _} when Height > Height_1_8 ->
			calculate_difficulty_after_1_8_before_1_9(OldDiff, TS, Last, Height);
		{_, Height, _, _} ->
			switch_to_linear_diff(OldDiff);
		{Height, _, _, _} ->
			switch_to_randomx_fork_diff(OldDiff);
		_ ->
			calculate_difficulty_before_1_8(OldDiff, TS, Last, Height)
	end.

%% @doc Validate that a new block has an appropriate difficulty.
validate_difficulty(NewB, OldB) when ?IS_RETARGET_BLOCK(NewB) ->
	(NewB#block.diff ==
		calculate_difficulty(
			OldB#block.diff,
			NewB#block.timestamp,
			OldB#block.last_retarget,
			NewB#block.height)
	);
validate_difficulty(NewB, OldB) ->
	(NewB#block.diff == OldB#block.diff) and
		(NewB#block.last_retarget == OldB#block.last_retarget).

%% @doc The number a hash must be greater than, to give the same odds of success
%% as the old-style Diff (number of leading zeros in the bitstring).
%% @end
switch_to_linear_diff(Diff) ->
	erlang:trunc(math:pow(2, 256)) - erlang:trunc(math:pow(2, 256 - Diff)).

%%%===================================================================
%%% Private functions.
%%%===================================================================

calculate_difficulty2(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ar_mine:max_difficulty(),
			MinDiff = ar_mine:min_difficulty(Height),
			DiffInverse = erlang:trunc((MaxDiff - OldDiff) * TimeDelta),
			between(
				MaxDiff - DiffInverse,
				MinDiff,
				MaxDiff
			)
	end.

calculate_difficulty_at_2_4(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	%% Make the difficulty drop 10 times faster than usual. The difficulty
	%% after SPoRA is estimated to be around 10-100 times lower. In the worst
	%% case, the 10x adjustment leads to a block per 12 seconds on average,
	%% what is a reasonable lower bound on the block time. In case of the 100x
	%% reduction in difficulty, it would only take 100 minutes to adjust.
	TimeDelta = 10 * ActualTime / TargetTime,
	MaxDiff = ar_mine:max_difficulty(),
	MinDiff = ar_mine:min_difficulty(Height),
	DiffInverse = erlang:trunc((MaxDiff - OldDiff) * TimeDelta),
	between(
		MaxDiff - DiffInverse,
		MinDiff,
		MaxDiff
	).

calculate_difficulty_at_and_after_1_9_before_2_4(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ar_mine:max_difficulty(),
			MinDiff = ar_mine:min_difficulty(Height),
			EffectiveTimeDelta = between(
				ActualTime / TargetTime,
				1 / ?DIFF_ADJUSTMENT_UP_LIMIT,
				?DIFF_ADJUSTMENT_DOWN_LIMIT
			),
			DiffInverse = erlang:trunc((MaxDiff - OldDiff) * EffectiveTimeDelta),
			between(
				MaxDiff - DiffInverse,
				MinDiff,
				MaxDiff
			)
	end.

calculate_difficulty_after_1_8_before_1_9(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ar_mine:max_difficulty(),
			MinDiff = ar_mine:min_difficulty(Height),
			between(
				MaxDiff - (MaxDiff - OldDiff) * ActualTime div TargetTime,
				max(MinDiff, OldDiff div 2),
				min(MaxDiff, OldDiff * 4)
			)
	end.

-ifdef(DEBUG).
switch_to_randomx_fork_diff(_) ->
	1.
-else.
switch_to_randomx_fork_diff(OldDiff) ->
	ar_mine:sha384_diff_to_randomx_diff(OldDiff) - 2.
-endif.

calculate_difficulty_before_1_8(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	TimeError = abs(ActualTime - TargetTime),
	Diff = erlang:max(
		if
			TimeError < (TargetTime * ?RETARGET_TOLERANCE) -> OldDiff;
			TargetTime > ActualTime                        -> OldDiff + 1;
			true                                           -> OldDiff - 1
		end,
		ar_mine:min_difficulty(Height)
	),
	Diff.

between(N, Min, _) when N < Min -> Min;
between(N, _, Max) when N > Max -> Max;
between(N, _, _) -> N.

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
				ar_node:mine(),
				ar_test_node:wait_until_height(Height)
			end,
			lists:seq(1, ?RETARGET_BLOCKS + 1)
		),
		true = ar_util:do_until(
			fun() ->
				[BH|_] = ar_node:get_blocks(),
				B = ar_storage:read_block(BH),
				B#block.diff > B0#block.diff
			end,
			1000,
			5 * 60 * 1000
		)
	end}.

calculate_difficulty_linear_test() ->
	Diff = switch_to_linear_diff(27),
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	Timestamp = os:system_time(seconds),
	%% The change is smaller than retarget tolerance.
	Retarget1 = Timestamp - erlang:trunc(TargetTime / (1 + ?RETARGET_TOLERANCE / 2)),
	%% We ignore retarget tolerance at fork 2.4.
	?assert(
		hashes(Diff) < (10 / (1 + ?RETARGET_TOLERANCE / 2) + 1) * hashes(
			calculate_difficulty(Diff, Timestamp, Retarget1, ar_fork:height_2_4())
		)
	),
	?assert(
		hashes(Diff) > (10 / (1 + ?RETARGET_TOLERANCE / 2) - 1) * hashes(
			calculate_difficulty(Diff, Timestamp, Retarget1, ar_fork:height_2_4())
		)
	),
	?assertEqual(
		Diff,
		calculate_difficulty(Diff, Timestamp, Retarget1, ar_fork:height_2_4() + 1)
	),
	Retarget2 = Timestamp - erlang:trunc(TargetTime * (1 + ?RETARGET_TOLERANCE / 2)),
	?assert(
		hashes(Diff) > (10 * (1 + ?RETARGET_TOLERANCE / 2) - 1) * hashes(
			calculate_difficulty(Diff, Timestamp, Retarget2, ar_fork:height_2_4())
		)
	),
	?assert(
		hashes(Diff) < (10 * (1 + ?RETARGET_TOLERANCE / 2) + 1) * hashes(
			calculate_difficulty(Diff, Timestamp, Retarget2, ar_fork:height_2_4())
		)
	),
	?assertEqual(
		Diff,
		calculate_difficulty(Diff, Timestamp, Retarget2, ar_fork:height_2_4() + 1)
	),
	%% The change is bigger than max allowed change up before 2.4 and not capped after.
	Retarget3 = Timestamp - TargetTime div (?DIFF_ADJUSTMENT_UP_LIMIT + 1),
	?assertEqual(
		?DIFF_ADJUSTMENT_UP_LIMIT * hashes(Diff),
		hashes(
			calculate_difficulty_at_and_after_1_9_before_2_4(Diff, Timestamp, Retarget3, 0)
		)
	),
	?assertEqual(
		(?DIFF_ADJUSTMENT_UP_LIMIT + 1) * hashes(Diff),
		hashes(
			calculate_difficulty(Diff, Timestamp, Retarget3, ar_fork:height_2_4() + 1)
		)
	),
	%% The change is bigger than max allowed change down before 2.4 and not capped after.
	Retarget4 = Timestamp - (?DIFF_ADJUSTMENT_DOWN_LIMIT + 1) * TargetTime,
	?assertEqual(
		hashes(Diff),
		?DIFF_ADJUSTMENT_DOWN_LIMIT * hashes(
			calculate_difficulty_at_and_after_1_9_before_2_4(Diff, Timestamp, Retarget4, 0)
		)
	),
	Retarget4_2 = Timestamp - (?DIFF_ADJUSTMENT_DOWN_LIMIT + 2) * TargetTime,
	?assertEqual(
		hashes(Diff),
		(?DIFF_ADJUSTMENT_DOWN_LIMIT + 2) * hashes(
			calculate_difficulty(Diff, Timestamp, Retarget4_2, ar_fork:height_2_4() + 1)
		)
	),
	%% The change is within limits.
	Retarget5 = Timestamp - TargetTime div (?DIFF_ADJUSTMENT_UP_LIMIT - 1),
	NewDiff = calculate_difficulty_at_and_after_1_9_before_2_4(Diff, Timestamp, Retarget5, 0),
	?assert(Diff < NewDiff),
	?assertEqual(
		(?DIFF_ADJUSTMENT_UP_LIMIT - 1) * hashes(Diff),
		hashes(NewDiff)
	),
	Retarget6 = Timestamp - ?DIFF_ADJUSTMENT_DOWN_LIMIT * TargetTime,
	NewDiff2 = calculate_difficulty_at_and_after_1_9_before_2_4(Diff, Timestamp, Retarget6, 0),
	?assert(Diff > NewDiff2),
	?assertEqual(
		hashes(Diff),
		?DIFF_ADJUSTMENT_DOWN_LIMIT * hashes(NewDiff2)
	),
	NewDiff3 = calculate_difficulty(Diff, Timestamp, Retarget5, ar_fork:height_2_4()),
	?assert(hashes(Diff) < 10 * hashes(NewDiff3)),
	?assert(hashes(Diff) > 3 * hashes(NewDiff3)),
	NewDiff4 = calculate_difficulty(Diff, Timestamp, Retarget5, ar_fork:height_2_4() + 1),
	?assert(Diff < NewDiff4),
	?assertEqual(
		(?DIFF_ADJUSTMENT_UP_LIMIT - 1) * hashes(Diff),
		hashes(NewDiff4)
	),
	NewDiff5 = calculate_difficulty(Diff, Timestamp, Retarget6, ar_fork:height_2_4()),
	?assert(Diff > NewDiff5),
	?assert(hashes(Diff) > (10 * ?DIFF_ADJUSTMENT_DOWN_LIMIT - 1) * hashes(NewDiff5)),
	?assert(hashes(Diff) < (10 * ?DIFF_ADJUSTMENT_DOWN_LIMIT + 1) * hashes(NewDiff5)),
	NewDiff6 = calculate_difficulty(Diff, Timestamp, Retarget6, ar_fork:height_2_4() + 1),
	?assert(Diff > NewDiff6),
	?assertEqual(
		hashes(Diff),
		?DIFF_ADJUSTMENT_DOWN_LIMIT * hashes(NewDiff6)
	).

hashes(Diff) ->
	MaxDiff = ar_mine:max_difficulty(),
	erlang:trunc(MaxDiff / (MaxDiff - Diff)).
