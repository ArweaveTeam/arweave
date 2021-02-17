-module(ar_difficulty).

-export([next_cumulative_diff/3, multiply_diff_pre_fork_2_5/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_mine.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Calculate the cumulative difficulty for the next block.
next_cumulative_diff(OldCDiff, NewDiff, Height) ->
	case Height >= ?FORK_1_6 of
		true ->
			next_cumulative_diff2(OldCDiff, NewDiff, Height);
		false ->
			0
	end.

%% @doc Get a difficulty that makes it harder to mine by Multiplier number of times.
%% The function was used up to the fork 2.4 and must be reimplemented without the
%% floating point numbers in case the need arises after the fork 2.5.
%% @end
multiply_diff_pre_fork_2_5(Diff, Multiplier) ->
	?MAX_DIFF - erlang:trunc(1 / Multiplier * (?MAX_DIFF - Diff)).

%%%===================================================================
%%% Private functions.
%%%===================================================================

next_cumulative_diff2(OldCDiff, NewDiff, Height) ->
	Delta =
		case Height >= ar_fork:height_1_8() of
			false ->
				NewDiff * NewDiff;
			true  ->
				%% The number of hashes to try on average to find a solution.
				case Height >= ar_fork:height_2_5() of
					false ->
						erlang:trunc(?MAX_DIFF / (?MAX_DIFF - NewDiff));
					true ->
						?MAX_DIFF div (?MAX_DIFF - NewDiff)
				end
		end,
	OldCDiff + Delta.
