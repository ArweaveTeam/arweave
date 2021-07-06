-module(ar_vdf).

-export([compute/3, compute2/3, verify/7, verify2/7]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar.hrl").

%% default IterationCount = ?VDF_DIFFICULTY
compute(StartStepNumber, PrevOutput, IterationCount) ->
	Salt = << StartStepNumber:256 >>,
	ar_mine_randomx:vdf_sha2_nif(Salt, PrevOutput, ?VDF_STEP_COUNT_IN_CHECKPOINT - 1, 0,
			IterationCount).

-ifdef(DEBUG).
compute2(StartStepNumber, PrevOutput, IterationCount) ->
	{ok, Output, CheckpointBuffer} = compute(StartStepNumber, PrevOutput, IterationCount),
	Checkpoints = [Output | checkpoint_buffer_to_checkpoints(CheckpointBuffer)],
	timer:sleep(50),
	{ok, Output, Checkpoints}.
-else.
compute2(StartStepNumber, PrevOutput, IterationCount) ->
	{ok, Output, CheckpointBuffer} = compute(StartStepNumber, PrevOutput, IterationCount),
	Checkpoints = [Output | checkpoint_buffer_to_checkpoints(CheckpointBuffer)],
	{ok, Output, Checkpoints}.
-endif.

%% no reset in CheckpointGroups, then ResetStepNumber < StartStepNumber
%%   any number out of bounds of
%%   [StartStepNumber, StartStepNumber+groupListToSumStep(CheckpointGroups)]
verify(_StartStepNumber, _PrevOutput, [], _ResetStepNumber, _ResetSeed, _ThreadCount,
		_IterationCount) ->
	{true, []};
verify(StartStepNumber, PrevOutput, [{StepBetweenHashCount, HashCount, BufferHash}],
		ResetStepNumber, ResetSeed, ThreadCount, IterationCount) ->
	Salt = << StartStepNumber:256 >>,
	ResetSalt = << ResetStepNumber:256 >>,
	RestStepsSize = ?VDF_BYTE_SIZE * (HashCount - 1),
	case BufferHash of
		<< RestSteps:RestStepsSize/binary, LastStep:?VDF_BYTE_SIZE/binary >> ->
			case ar_mine_randomx:vdf_parallel_sha_verify_with_reset_nif(Salt, PrevOutput,
					HashCount - 1, StepBetweenHashCount - 1, IterationCount, RestSteps,
					LastStep, ResetSalt, ResetSeed, ThreadCount) of
				{ok, Steps} ->
					{true, Steps};
				_ ->
					false
			end;
		_ ->
			false
	end;
verify(StartStepNumber, PrevOutput, CheckpointGroups, ResetStepNumber, ResetSeed, ThreadCount,
		IterationCount) ->
	%% CheckpointGroups for last-step verification: [{1, 25, << ... >>}]
	%% CheckpointGroups for big verification:
	%%   [{25, 999, << .. >>}, {50, 1, << ... >>}, {100, 1, << ... >>}}]
	[HeadGroup | TailCheckpointGroups] = CheckpointGroups,
	[Head2Group | _] = TailCheckpointGroups,
	{_, _, Buffer2} = Head2Group,
	PrevOutputHeadGroup = binary:part(Buffer2, {byte_size(Buffer2), -?VDF_BYTE_SIZE}),
	%% verification order     End -> Start
	%% we must pass all TailCheckpointGroups checkpoints and start at ShiftedStartStepNumber
	ShiftedStartStepNumber = StartStepNumber + groupListToSumStep(TailCheckpointGroups),
	case verify(ShiftedStartStepNumber, PrevOutputHeadGroup, [HeadGroup], ResetStepNumber,
			ResetSeed, ThreadCount, IterationCount) of
		false ->
			false;
		{true, HeadSteps} ->
			case verify(StartStepNumber, PrevOutput, TailCheckpointGroups, ResetStepNumber,
					ResetSeed, ThreadCount, IterationCount) of
				false ->
					false;
				{true, TailSteps} ->
					%% Start := StartStepNumber
					%% End   := EndStepNumber (virtual)
					%% output order           Start -> End
					%% CheckpointGroups order Start -> End
					%% verification order     End -> Start
					%% CheckpointGroups[0] -> End
					{true, << TailSteps/binary, HeadSteps/binary >>}
			end
	end.

verify2(StartStepNumber, PrevOutput, Groups, ResetStepNumber, ResetSeed, ThreadCount,
		IterationCount) ->
	case verify(StartStepNumber, PrevOutput, Groups, ResetStepNumber, ResetSeed, ThreadCount,
			IterationCount) of
		false ->
			false;
		{true, CheckpointBuffer} ->
			{true, ar_util:take_every_nth(?VDF_STEP_COUNT_IN_CHECKPOINT,
					checkpoint_buffer_to_checkpoints(CheckpointBuffer))}
	end.

checkpoint_buffer_to_checkpoints(Buffer) ->
	checkpoint_buffer_to_checkpoints(Buffer, []).

checkpoint_buffer_to_checkpoints(<<>>, Checkpoints) ->
	Checkpoints;
checkpoint_buffer_to_checkpoints(<< Checkpoint:32/binary, Rest/binary >>, Checkpoints) ->
	checkpoint_buffer_to_checkpoints(Rest, [Checkpoint | Checkpoints]).

groupListToSumStep([])->
	0;
groupListToSumStep([{StepBetweenHashCount, HashCount, _}])->
	StepBetweenHashCount * HashCount;
groupListToSumStep([Head | Tail])->
	groupListToSumStep([Head]) + groupListToSumStep(Tail).
