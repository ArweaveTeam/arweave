-module(ar_difficulty).

-export([next_cumulative_diff/3]).

%% @doc Calculate the cumulative difficulty for the next block.
next_cumulative_diff(OldCDiff, NewDiff, Height) ->
	Delta = case Height >= ar_fork:height_1_8() of
		false ->
			NewDiff * NewDiff;
		true  ->
			MaxDiff = ar_mine:max_difficulty(Height),
			%% The number of hashes to try on average to find a solution.
			erlang:trunc(MaxDiff / (MaxDiff - NewDiff))
	end,
	OldCDiff + Delta.
