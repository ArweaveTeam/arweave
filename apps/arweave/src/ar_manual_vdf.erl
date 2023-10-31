-module(ar_manual_vdf).

-export([manual_vdf/0]).

-include_lib("arweave/include/ar_vdf.hrl").

manual_vdf() ->
	PrevOutput = ar_util:decode(<<"W-5B1F7rYIayc_1d3nq3bsLanXNympgA3tkIXOT0EF8">>),
	StepNumber = 21499625,
	Salt = ar_vdf:step_number_to_salt_number(StepNumber-1),
	Salt2 = << Salt:256 >>,
	CheckpointCount = 0,
	SkipCheckpointCount = 0,
	Iterations = 1,
	{ok, Output, _CheckpointsBuffer} =
			ar_mine_randomx:vdf_sha2_nif(
				Salt2, PrevOutput, CheckpointCount, SkipCheckpointCount, Iterations),
	io:format("~n~n"),
	io:format("Salt: ~p~n", [ar_util:encode(Salt2)]),
	io:format("Output: ~p~n", [ar_util:encode(Output)]).
	