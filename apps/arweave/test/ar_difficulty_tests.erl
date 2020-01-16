-module(ar_difficulty_tests).

-include_lib("eunit/include/eunit.hrl").

next_cumul_diff_test() ->
	OldCDiff = 10,
	NewDiff = 25,
	Expected = OldCDiff + erlang:trunc(math:pow(2, 256) / (math:pow(2, 256) - NewDiff)),
	Actual = ar_difficulty:next_cumulative_diff(OldCDiff, NewDiff, 0),
	?assertEqual(Expected, Actual).
