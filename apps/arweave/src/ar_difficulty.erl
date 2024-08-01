-module(ar_difficulty).

-export([get_hash_rate_fixed_ratio/1, next_cumulative_diff/3, multiply_diff_pre_fork_2_5/2,
			diff_pair/1, poa1_diff_multiplier/1, poa1_diff/2, scale_diff/3,
			min_difficulty/1, switch_to_randomx_fork_diff/1, sub_diff/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the block time hash rate for the given difficulty.
get_hash_rate_fixed_ratio(Block) ->
	Multiplier = poa1_diff_multiplier(Block#block.height),
	HashRate = ?MAX_DIFF div (?MAX_DIFF - Block#block.diff),
	case Multiplier > 1 of
		true ->
			HashRate * Multiplier div (Multiplier + 1);
		false ->
			HashRate
	end.

%% @doc Calculate the cumulative difficulty for the next block.
next_cumulative_diff(OldCDiff, NewDiff, Height) ->
	case Height >= ar_fork:height_1_6() of
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

diff_pair(Block) ->
	Diff = Block#block.diff,
	Height = Block#block.height,
	{poa1_diff(Diff, Height), Diff}.

poa1_diff_multiplier(Height) ->
	case Height >= ar_fork:height_2_7_2() of
		true ->
			?POA1_DIFF_MULTIPLIER;
		false ->
			1
	end.

poa1_diff(Diff, Height) ->
	Scale = {poa1_diff_multiplier(Height), 1},
	scale_diff(Diff, Scale, Height).

%% @doc Scale the difficulty by ScaleDividend/ScaleDivisor.
%% Example: scale_diff(Diff, {100, 1}, Height) will scale the difficulty by 100, increasing it
%% Example: scale_diff(Diff, {3, 10}, Height) will scale the difficulty by 3/10, decreasing it
scale_diff(Diff, {1, 1}, _Height) ->
	Diff;
scale_diff(Diff, {ScaleDividend, ScaleDivisor}, Height) ->
	MaxDiff = ?MAX_DIFF,
	MinDiff = min_difficulty(Height),
	%% Scale DiffInverse by ScaleDivisor/ScaleDividend because it's an inverse value.
	%% I.e. passing in {100, 1} will scale DiffInverse by 1/100 and *increase* the difficulty.
	DiffInverse = (MaxDiff - Diff) * ScaleDivisor div ScaleDividend,
	ar_util:between(
		MaxDiff - DiffInverse,
		MinDiff,
		MaxDiff - 1
	).

%% @doc Return the new difficulty computed such that N candidates have approximately the same
%% chances with the new difficulty as a single candidate with the Diff difficulty.
%%
%% Let the probability a candidate satisfies the new difficulty be x.
%% Let the probability a candidate satisfies the old diffiuclty be p.
%% Then, the probability at least one of the N candidates satisfies
%% the new difficulty is 1 - (1 - x) ^ N. We want it to be equal to p.
%% So, (1 - x) ^ N = 1 - p => x = 1 - 32th root of (1 - p).
%% The first three terms of the infinite series of (1 - p) ^ (1 / 32) are
%% 1 - (1 / 32) * p - (31 * p ^ 2)/(2 * 32 ^ 2).
%% Therefore, x is approximately p/32 + 31 * (p ^ 2) / (2 * 32 ^ 2).
%% x = NewReverseDiff / MaxDiff, p = ReverseDiff / MaxDiff.
sub_diff(infinity, _N) ->
	infinity;
sub_diff(Diff, N) ->
	MaxDiff = ?MAX_DIFF,
	ReverseDiff = MaxDiff - Diff,
	MaxDiffSquared = MaxDiff * MaxDiff,
	ReverseDiffSquared = ReverseDiff * ReverseDiff,
	Dividend = 2 * N * ReverseDiff * MaxDiff + (N - 1) * ReverseDiffSquared,
	Divisor = 2 * N * N * MaxDiffSquared,
	(MaxDiff * Divisor - Dividend  * MaxDiff) div Divisor.

-ifdef(DEBUG).
min_difficulty(_Height) ->
	1.
switch_to_randomx_fork_diff(_) ->
	1.
-else.
min_spora_difficulty(Height) ->
	?SPORA_MIN_DIFFICULTY(Height).

min_randomx_difficulty() ->
	min_sha384_difficulty() + ?RANDOMX_DIFF_ADJUSTMENT.

min_sha384_difficulty() ->
	31.

min_difficulty(Height) ->
	Diff =
		case Height >= ar_fork:height_1_7() of
			true ->
				case Height >= ar_fork:height_2_4() of
					true ->
						min_spora_difficulty(Height);
					false ->
						min_randomx_difficulty()
				end;
			false ->
				min_sha384_difficulty()
		end,
	case Height >= ar_fork:height_1_8() of
		true ->
			case Height >= ar_fork:height_2_5() of
				true ->
					ar_retarget:switch_to_linear_diff(Diff);
				false ->
					ar_retarget:switch_to_linear_diff_pre_fork_2_5(Diff)
			end;
		false ->
			Diff
	end.

sha384_diff_to_randomx_diff(Sha384Diff) ->
	max(Sha384Diff + ?RANDOMX_DIFF_ADJUSTMENT, min_randomx_difficulty()).

switch_to_randomx_fork_diff(OldDiff) ->
	sha384_diff_to_randomx_diff(OldDiff) - 2.
-endif.

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
