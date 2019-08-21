-module(ar_difficulty_tests).

-include_lib("eunit/include/eunit.hrl").

next_cumul_diff_test() ->
	OldCDiff = 10,
	NewDiff = 25,
	Height1 = ar_fork:height_1_8() - 123,
	Exp1 = OldCDiff + (NewDiff * NewDiff),
	Exp2 = OldCDiff + erlang:trunc(math:pow(2, 256) / (math:pow(2, 256) - NewDiff)),
	Height2 = ar_fork:height_1_8() + 123,
	Act1 = ar_difficulty:next_cumulative_diff(OldCDiff, NewDiff, Height1),
	Act2 = ar_difficulty:next_cumulative_diff(OldCDiff, NewDiff, Height2),
	?assertEqual(Exp1, Act1),
	?assertEqual(Exp2, Act2).
