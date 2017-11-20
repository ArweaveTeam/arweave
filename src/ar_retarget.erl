-module(ar_retarget).
-export([calculate_difficulty/3, maybe_retarget/2, validate/2]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").

%% Is this a retargeting block?
-define(IS_RETARGET_BLOCK(X),
	(((X#block.height rem ?RETARGET_BLOCKS) == 0) and (X#block.height =/= 0))).

%%% Functions for manipulating and calculating new difficulty values.

%% @doc Optionally re-calculate the difficulty of the next block, if
%% a retarget block height has been reached.
maybe_retarget(B, #block { last_retarget = Last }) when ?IS_RETARGET_BLOCK(B) ->
	B#block {
		diff =
			calculate_difficulty(
				B#block.diff,
				B#block.timestamp,
				Last
			),
		last_retarget = B#block.timestamp
	};
maybe_retarget(B, OldB) ->
	B#block { last_retarget = OldB#block.last_retarget }.

%% @doc Calculate a new difficulty, given an old difficulty and the retarget
%% period it produced.
calculate_difficulty(OldDiff, TS, OldTS) ->
	%% TODO: Improve the stability of this mechanism.
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - OldTS,
	TimeError = abs(ActualTime - TargetTime),
	if TimeError < (TargetTime * ?RETARGET_TOLERANCE) -> OldDiff;
	TargetTime > ActualTime -> OldDiff + 1;
	true -> OldDiff - 1
	end.

%% @doc Validate that a new block has an appropriate difficulty.
validate(NewB, OldB) when ?IS_RETARGET_BLOCK(NewB) ->
	(NewB#block.diff ==
		calculate_difficulty(
			OldB#block.diff,
			NewB#block.timestamp,
			OldB#block.last_retarget)
	)
	and (NewB#block.timestamp == NewB#block.last_retarget);
validate(NewB, OldB) ->
	NewB#block.last_retarget == OldB#block.last_retarget.

%%% Tests

%% @doc Ensure that after a series of very fast mines, the diff increases.
simple_retarget_test() ->
	Node = ar_node:start([], ar_weave:init([])),
	lists:foreach(
		fun(_) ->
			ar_node:mine(Node),
			receive after 250 -> ok end
		end,
		lists:seq(1, ?RETARGET_BLOCKS + 1)
	),
	[B|_] = ar_node:get_blocks(Node),
	true = ((ar_storage:read_block(B))#block.diff > ?DEFAULT_DIFF).
