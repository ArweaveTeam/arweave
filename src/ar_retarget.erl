-module(ar_retarget).
-export([is_retarget_height/1]).
-export([maybe_retarget/4]).
-export([calculate_difficulty/4]).
-export([validate_difficulty/2]).
-export([switch_to_linear_diff/1]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").

%%% A helper module for deciding when and which blocks will be retarget
%%% blocks, that is those in which change the current mining difficulty
%%% on the weave to maintain a constant block time.


%% @doc A macro for checking if the given block is a retarget block.
%% Returns true if so, otherwise returns false.
-define(IS_RETARGET_BLOCK(X),
		(
			((X#block.height rem ?RETARGET_BLOCKS) == 0) and
			(X#block.height =/= 0)
		)
	).

%% @doc A macro for checking if the given height is a retarget height.
%% Returns true if so, otherwise returns false.
-define(IS_RETARGET_HEIGHT(Height),
		(
			((Height rem ?RETARGET_BLOCKS) == 0) and
			(Height =/= 0)
		)
	).

%% @doc Checls if the given height is a retarget height.
%% Reteurns true if so, otherwise returns false.
is_retarget_height(Height) ->
	?IS_RETARGET_HEIGHT(Height).

%% @doc Maybe set a new difficulty and last retarget, if the block is at
%% an appropriate retarget height, else returns the current diff
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
-ifdef(FIXED_DIFF).
calculate_difficulty(_OldDiff, _TS, _Last, _Height) ->
	?FIXED_DIFF.
-else.
calculate_difficulty(OldDiff, TS, Last, Height) ->
	case {ar_fork:height_1_7(), ar_fork:height_1_8()} of
		{Height, _} ->
			switch_to_randomx_fork_diff(OldDiff);
		{_, Height} ->
			switch_to_linear_diff(OldDiff);
		{_, H} when Height > H ->
			calculate_difficulty_linear(OldDiff, TS, Last, Height);
		_ ->
			calculate_difficulty1(OldDiff, TS, Last, Height)
	end.

calculate_difficulty1(OldDiff, TS, Last, Height) ->
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

calculate_difficulty_linear(OldDiff, TS, Last, Height) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	TimeDelta = ActualTime / TargetTime,
	case abs(1 - TimeDelta) < ?RETARGET_TOLERANCE of
		true ->
			OldDiff;
		false ->
			MaxDiff = ar_mine:max_difficulty(Height),
			MinDiff = ar_mine:min_difficulty(Height),
			between(
				MaxDiff - (MaxDiff - OldDiff) * ActualTime div TargetTime,
				max(MinDiff, ?DIFF_ADJUSTMENT_DOWN_LIMIT(OldDiff)),
				min(MaxDiff, ?DIFF_ADJUSTMENT_UP_LIMIT(OldDiff))
			)
	end.
-endif.

between(N, Min, _) when N < Min -> Min;
between(N, _, Max) when N > Max -> Max;
between(N, _, _) -> N.

%% @doc The number a hash must be greater than, to give the same odds of success
%% as the old-style Diff (number of leading zeros in the bitstring).
switch_to_linear_diff(Diff) ->
	erlang:trunc(math:pow(2, 256)) - erlang:trunc(math:pow(2, 256 - Diff)).

-ifdef(DEBUG).
switch_to_randomx_fork_diff(_) ->
	1.
-else.
switch_to_randomx_fork_diff(OldDiff) ->
	ar_mine:sha384_diff_to_randomx_diff(OldDiff) - 2.
-endif.

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

%%%
%%% Tests: ar_retarget
%%%

%% @doc Ensure that after a series of very fast mines, the diff increases.
simple_retarget_test_() ->
	{timeout, 300, fun() ->
		Node = ar_node:start([], ar_weave:init([])),
		lists:foreach(
			fun(_) ->
				ar_node:mine(Node),
				timer:sleep(500)
			end,
			lists:seq(1, ?RETARGET_BLOCKS + 1)
		),
		true = ar_util:do_until(
			fun() ->
				[BH|_] = ar_node:get_blocks(Node),
				B = ar_storage:read_block(BH, ar_node:get_hash_list(Node)),
				B#block.diff > ?DEFAULT_DIFF
			end,
			1000,
			5 * 60 * 1000
		)
	end}.
