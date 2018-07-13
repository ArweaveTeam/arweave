-module(ar_retarget).
-export([is_retarget_height/1]).
-export([maybe_retarget/2, maybe_retarget/4]).
-export([calculate_difficulty/3]).
-export([validate/2]).
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
    ((Height rem ?RETARGET_BLOCKS) == 0) and
    (Height =/= 0).

%% @doc Maybe set a new difficulty and last retarget, if the block is at
%% an appropriate retarget height, else returns the current diff
maybe_retarget(Height, CurDiff, TS, Last) when ?IS_RETARGET_HEIGHT(Height) ->
    calculate_difficulty(
        CurDiff,
        TS,
        Last
    );
maybe_retarget(_Height, CurDiff, _TS, _Last) ->
    CurDiff.
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
	B#block {
        last_retarget = OldB#block.last_retarget,
        diff = OldB#block.diff
    }.

%% @doc Calculate a new difficulty, given an old difficulty and the period
%% since the last retarget occcurred.
-ifdef(FIXED_DIFF).
calculate_difficulty(_OldDiff, _TS, _Last) -> ?FIXED_DIFF.
-else.
calculate_difficulty(OldDiff, TS, Last) ->
	TargetTime = ?RETARGET_BLOCKS * ?TARGET_TIME,
	ActualTime = TS - Last,
	TimeError = abs(ActualTime - TargetTime),
	if
        TimeError < (TargetTime * ?RETARGET_TOLERANCE) -> OldDiff;
	    TargetTime > ActualTime -> OldDiff + 1;
	    true -> OldDiff - 1
	end.
-endif.

%% @doc Validate that a new block has an appropriate difficulty.
validate(NewB, OldB) when ?IS_RETARGET_BLOCK(NewB) ->
	(NewB#block.diff ==
		calculate_difficulty(
			OldB#block.diff,
			NewB#block.timestamp,
			OldB#block.last_retarget)
	);
validate(NewB, OldB) ->
	NewB#block.last_retarget == OldB#block.last_retarget.


%%% Tests: ar_retarget

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
