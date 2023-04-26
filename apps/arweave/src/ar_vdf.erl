-module(ar_vdf).

-export([compute/3, compute2/3, verify/7, verify2/7,
		debug_sha_verify_no_reset/3, debug_sha_verify/5, vdf_sha2/2]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar.hrl").

step_number_to_salt_number(0) ->
	0;
step_number_to_salt_number(StepNumber) ->
	(StepNumber - 1) * 25 + 1.

%% default IterationCount = ?VDF_DIFFICULTY
compute(StartStepNumber, PrevOutput, IterationCount) ->
	Salt = step_number_to_salt_number(StartStepNumber - 1),
	SaltBinary = << Salt:256 >>,
	ar_mine_randomx:vdf_sha2_nif(SaltBinary, PrevOutput, ?VDF_CHECKPOINT_COUNT_IN_STEP - 1, 0,
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

%% no reset in CheckpointGroups, then ResetStepNumber < StartSalt
%%   any number out of bounds of
%%   [StartSalt, StartSalt+group_list_to_sum_step(CheckpointGroups)]
verify(_StartSalt, _PrevOutput, [], _ResetStepNumber, _ResetSeed, _ThreadCount,
		_IterationCount) ->
	{true, []};
verify(StartSalt, PrevOutput, [{StepBetweenHashCount, HashCount, BufferHash}],
		ResetSalt, ResetSeed, ThreadCount, IterationCount) ->
	StartSaltBinary = << StartSalt:256 >>,
	ResetSaltBinary = << ResetSalt:256 >>,
	RestStepsSize = ?VDF_BYTE_SIZE * (HashCount - 1),
	case BufferHash of
		<< RestSteps:RestStepsSize/binary, LastStep:?VDF_BYTE_SIZE/binary >> ->
			?LOG_ERROR([{event, ar_vdf_verify_start}, {start_step, StartSalt},
					{step_between_hash_count, StepBetweenHashCount}, {hash_count, HashCount},
					{reset_step_number, ResetSaltBinary}, {thread_count, ThreadCount},
					{iteration_count, IterationCount}, {pid, self()}]),
			ar_util:print_stacktrace(),
			StartTime = erlang:timestamp(),
			case ar_mine_randomx:vdf_parallel_sha_verify_with_reset_nif(StartSaltBinary, PrevOutput,
					HashCount - 1, StepBetweenHashCount - 1, IterationCount, RestSteps,
					LastStep, ResetSalt, ResetSeed, ThreadCount) of
				{ok, Steps} ->
					?LOG_ERROR([{event, ar_vdf_verify_done}, {pid, self()},
								{duration, timer:now_diff(erlang:timestamp(), StartTime) / 1000000},
					{start_step, StartSalt},
					{step_between_hash_count, StepBetweenHashCount}, {hash_count, HashCount},
					{reset_step_number, ResetSaltBinary}, {thread_count, ThreadCount},
					{iteration_count, IterationCount}]),
					{true, Steps};
				_ ->
					false
			end;
		_ ->
			false
	end;
verify(StartSalt, PrevOutput, CheckpointGroups, ResetSalt, ResetSeed, ThreadCount,
		IterationCount) ->
	%% CheckpointGroups for last-step verification: [{1, 25, << ... >>}]
	%% CheckpointGroups for big verification:
	%%   [{25, 999, << .. >>}, {50, 1, << ... >>}, {100, 1, << ... >>}}]
	[HeadGroup | TailCheckpointGroups] = CheckpointGroups,
	[Head2Group | _] = TailCheckpointGroups,
	{_, _, Buffer2} = Head2Group,
	PrevOutputHeadGroup = binary:part(Buffer2, {byte_size(Buffer2), -?VDF_BYTE_SIZE}),
	%% verification order     End -> Start
	%% we must pass all TailCheckpointGroups checkpoints and start at ShiftedStartSalt
	ShiftedStartSalt = StartSalt + group_list_to_sum_step(TailCheckpointGroups),
	case verify(ShiftedStartSalt, PrevOutputHeadGroup, [HeadGroup], ResetSalt,
			ResetSeed, ThreadCount, IterationCount) of
		false ->
			false;
		{true, HeadSteps} ->
			case verify(StartSalt, PrevOutput, TailCheckpointGroups, ResetSalt,
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
	StartSalt = step_number_to_salt_number(StartStepNumber),
	ResetSalt = step_number_to_salt_number(ResetStepNumber - 1),
	case verify(StartSalt, PrevOutput, Groups, ResetSalt, ResetSeed, ThreadCount,
			IterationCount) of
		false ->
			false;
		{true, CheckpointBuffer} ->
			{true, ar_util:take_every_nth(?VDF_CHECKPOINT_COUNT_IN_STEP,
					checkpoint_buffer_to_checkpoints(CheckpointBuffer))}
	end.

checkpoint_buffer_to_checkpoints(Buffer) ->
	checkpoint_buffer_to_checkpoints(Buffer, []).

checkpoint_buffer_to_checkpoints(<<>>, Checkpoints) ->
	Checkpoints;
checkpoint_buffer_to_checkpoints(<< Checkpoint:32/binary, Rest/binary >>, Checkpoints) ->
	checkpoint_buffer_to_checkpoints(Rest, [Checkpoint | Checkpoints]).

group_list_to_sum_step([])->
	0;
group_list_to_sum_step([{StepBetweenHashCount, HashCount, _}])->
	StepBetweenHashCount * HashCount;
group_list_to_sum_step([Head | Tail])->
	group_list_to_sum_step([Head]) + group_list_to_sum_step(Tail).


%%%===================================================================
%%% Debug implementations.
%%% Erlang implementations of of NIFs. Usee in tests.
%%%===================================================================


hash(0, _Salt, Input) ->
	Input;
hash(N, Salt, Input) ->
	hash(N - 1, Salt, crypto:hash(sha256, << Salt/binary, Input/binary >>)).

%% @doc An Erlang implementation of ar_vdf:compute2/3. Used in tests.
vdf_sha2(StepNumber, Output) ->
	Salt = step_number_to_salt_number(StepNumber - 1),
	{Output2, Checkpoints} =
		lists:foldl(
			fun(I, {Acc, L}) ->
				SaltBinary = << (Salt + I):256 >>,
				H = hash(?VDF_DIFFICULTY, SaltBinary, Acc),
				{H, [H | L]}
			end,
			{Output, []},
			lists:seq(0, 25 - 1)
		),
	timer:sleep(500),
	{ok, Output2, Checkpoints}.

%% @doc An Erlang implementation of ar_vdf:verify/7. Used in tests.
debug_sha_verify_no_reset(StepNumber, Output, Groups) ->
	Salt = step_number_to_salt_number(StepNumber),
	debug_verify_no_reset(Salt, Output, lists:reverse(Groups), []).

debug_verify_no_reset(_Salt, _Output, [], Steps) ->
	{true, Steps};
debug_verify_no_reset(Salt, Output, [{Size, N, Buffer} | Groups], Steps) ->
	true = Size == 1 orelse Size rem 25 == 0,
	{NextOutput, Steps2} =
		lists:foldl(
			fun(I, {Acc, S}) ->
				SaltBinary = << (Salt + I):256 >>,
				O2 = hash(?VDF_DIFFICULTY, SaltBinary, Acc),
				S2 = case (Salt + I) rem 25 of 0 -> [O2 | S]; _ -> S end,
				{O2, S2}
			end,
			{Output, []},
			lists:seq(0, Size - 1)
		),
	Salt2 = Salt + Size,
	case Buffer of
		<< NextOutput/binary >> when N == 1 ->
			debug_verify_no_reset(Salt2, NextOutput, Groups, Steps2 ++ Steps);
		<< NextOutput:32/binary, Rest/binary >> when N > 0 ->
			debug_verify_no_reset(Salt2, NextOutput, [{Size, N - 1, Rest} | Groups],
					Steps2 ++ Steps);
		_ ->
			false
	end.

%% @doc An Erlang implementation of ar_vdf:verify/7. Used in tests.
debug_sha_verify(StepNumber, Output, Groups, ResetStepNumber, ResetSeed) ->
	StartSalt = step_number_to_salt_number(StepNumber),
	ResetSalt = step_number_to_salt_number(ResetStepNumber - 1),
	debug_verify(StartSalt, Output, lists:reverse(Groups), ResetSalt, ResetSeed,
			[]).

debug_verify(_StartSalt, _Output, [], _ResetSalt, _ResetSeed, Steps) ->
	{true, Steps};
debug_verify(StartSalt, Output, [{Size, N, Buffer} | Groups], ResetSalt,
		ResetSeed, Steps) ->
	true = Size rem 25 == 0,
	{NextOutput, Steps2} =
		lists:foldl(
			fun(I, {Acc, S}) ->
				SaltBinary = << (StartSalt + I):256 >>,
				case I rem 25 /= 0 of
					true ->
						H = hash(?VDF_DIFFICULTY, SaltBinary, Acc),
						case (StartSalt + I) rem 25 of
							0 ->
								{H, [H | S]};
							_ ->
								{H, S}
						end;
					false ->
						Acc2 =
							case StartSalt + I == ResetSalt of
								true ->
									crypto:hash(sha256, << Acc/binary, ResetSeed/binary >>);
								false ->
									Acc
							end,
						H = hash(?VDF_DIFFICULTY, SaltBinary, Acc2),
						case (StartSalt + I) rem 25 of
							0 ->
								{H, [H | S]};
							_ ->
								{H, S}
						end
				end
			end,
			{Output, []},
			lists:seq(0, Size - 1)
		),
	case Buffer of
		<< NextOutput/binary >> when N == 1 ->
			debug_verify(StartSalt + Size, NextOutput, Groups, ResetSalt,
					ResetSeed, Steps2 ++ Steps);
		<< NextOutput:32/binary, Rest/binary >> when N > 0 ->
			debug_verify(StartSalt + Size, NextOutput,
					[{Size, N - 1, Rest} | Groups], ResetSalt, ResetSeed,
					Steps2 ++ Steps);
		_ ->
			false
	end.
