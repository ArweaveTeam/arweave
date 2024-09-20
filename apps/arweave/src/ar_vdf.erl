-module(ar_vdf).

-export([compute/3, compute2/3, verify/8, verify2/8,
		debug_sha_verify_no_reset/6, debug_sha_verify/8, debug_sha2/3,
		step_number_to_salt_number/1, checkpoint_buffer_to_checkpoints/1]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar.hrl").

step_number_to_salt_number(0) ->
	0;
step_number_to_salt_number(StepNumber) ->
	(StepNumber - 1) * ?VDF_CHECKPOINT_COUNT_IN_STEP + 1.

%% default IterationCount = ?VDF_DIFFICULTY
compute(StartStepNumber, PrevOutput, IterationCount) ->
	Salt = step_number_to_salt_number(StartStepNumber - 1),
	SaltBinary = << Salt:256 >>,
	ar_vdf_nif:vdf_sha2_nif(SaltBinary, PrevOutput, ?VDF_CHECKPOINT_COUNT_IN_STEP - 1, 0,
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
verify(StartSalt, PrevOutput, NumCheckpointsBetweenHashes, Hashes,
		ResetSalt, ResetSeed, ThreadCount, IterationCount) ->
	StartSaltBinary = << StartSalt:256 >>,
	ResetSaltBinary = << ResetSalt:256 >>,
	NumHashes = length(Hashes),
	HashBuffer = iolist_to_binary(Hashes),
	RestStepsSize = ?VDF_BYTE_SIZE * (NumHashes - 1),
	case HashBuffer of
		<< RestSteps:RestStepsSize/binary, LastStep:?VDF_BYTE_SIZE/binary >> ->
			case ar_vdf_nif:vdf_parallel_sha_verify_with_reset_nif(StartSaltBinary,
					PrevOutput, NumHashes - 1, NumCheckpointsBetweenHashes - 1,
					IterationCount, RestSteps, LastStep, ResetSaltBinary, ResetSeed,
					ThreadCount) of
				{ok, Steps} ->
					{true, Steps};
				_ ->
					false
			end;
		_ ->
			false
	end.

verify2(StartStepNumber, PrevOutput, NumCheckpointsBetweenHashes, Hashes,
		ResetStepNumber, ResetSeed, ThreadCount, IterationCount) ->
	StartSalt = step_number_to_salt_number(StartStepNumber),
	ResetSalt = step_number_to_salt_number(ResetStepNumber - 1),
	case verify(StartSalt, PrevOutput, NumCheckpointsBetweenHashes, Hashes,
			ResetSalt, ResetSeed, ThreadCount, IterationCount) of
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

%%%===================================================================
%%% Debug implementations.
%%% Erlang implementations of of NIFs. Usee in tests.
%%%===================================================================


hash(0, _Salt, Input) ->
	Input;
hash(N, Salt, Input) ->
	hash(N - 1, Salt, crypto:hash(sha256, << Salt/binary, Input/binary >>)).

%% @doc An Erlang implementation of ar_vdf:compute2/3. Used in tests.
debug_sha2(StepNumber, Output, IterationCount) ->
	Salt = step_number_to_salt_number(StepNumber - 1),
	{Output2, Checkpoints} =
		lists:foldl(
			fun(I, {Acc, L}) ->
				SaltBinary = << (Salt + I):256 >>,
				H = hash(IterationCount, SaltBinary, Acc),
				{H, [H | L]}
			end,
			{Output, []},
			lists:seq(0, ?VDF_CHECKPOINT_COUNT_IN_STEP - 1)
		),
	timer:sleep(500),
	{ok, Output2, Checkpoints}.

%% @doc An Erlang implementation of ar_vdf:verify/7. Used in tests.
debug_sha_verify_no_reset(StepNumber, Output, NumCheckpointsBetweenHashes, Hashes, _ThreadCount, IterationCount) ->
	Salt = step_number_to_salt_number(StepNumber),
	debug_verify_no_reset(Salt, Output, NumCheckpointsBetweenHashes, Hashes, [], IterationCount).

debug_verify_no_reset(Salt, Output, Size, Hashes, Steps, IterationCount) ->
	true = Size == 1 orelse Size rem ?VDF_CHECKPOINT_COUNT_IN_STEP == 0,
	{NextOutput, Steps2} =
		lists:foldl(
			fun(I, {Acc, S}) ->
				SaltBinary = << (Salt + I):256 >>,
				O2 = hash(IterationCount, SaltBinary, Acc),
				S2 = case (Salt + I) rem ?VDF_CHECKPOINT_COUNT_IN_STEP of 0 -> [O2 | S]; _ -> S end,
				{O2, S2}
			end,
			{Output, []},
			lists:seq(0, Size - 1)
		),
	Salt2 = Salt + Size,
	case Hashes of
		[ NextOutput ] ->
			{true, Steps2 ++ Steps};
		[ NextOutput | Rest ] ->
			debug_verify_no_reset(Salt2, NextOutput, Size, Rest, Steps2 ++ Steps, IterationCount);
		_ ->
			false
	end.

%% @doc An Erlang implementation of ar_vdf:verify/7. Used in tests.
debug_sha_verify(StepNumber, Output, NumCheckpointsBetweenHashes, Hashes, ResetStepNumber, ResetSeed, _ThreadCount, IterationCount) ->
	StartSalt = step_number_to_salt_number(StepNumber),
	ResetSalt = step_number_to_salt_number(ResetStepNumber - 1),
	debug_verify(StartSalt, Output, NumCheckpointsBetweenHashes, Hashes, ResetSalt, ResetSeed,
			[], IterationCount).

debug_verify(StartSalt, Output, Size, Hashes, ResetSalt,
		ResetSeed, Steps, IterationCount) ->
	true = Size rem ?VDF_CHECKPOINT_COUNT_IN_STEP == 0,
	{NextOutput, Steps2} =
		lists:foldl(
			fun(I, {Acc, S}) ->
				SaltBinary = << (StartSalt + I):256 >>,
				case I rem ?VDF_CHECKPOINT_COUNT_IN_STEP /= 0 of
					true ->
						H = hash(IterationCount, SaltBinary, Acc),
						case (StartSalt + I) rem ?VDF_CHECKPOINT_COUNT_IN_STEP of
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
						H = hash(IterationCount, SaltBinary, Acc2),
						case (StartSalt + I) rem ?VDF_CHECKPOINT_COUNT_IN_STEP of
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
	case Hashes of
		[ NextOutput ] ->
			{true, Steps2 ++ Steps};
		[ NextOutput | Rest ] ->
			debug_verify(StartSalt + Size, NextOutput,
					Size, Rest, ResetSalt, ResetSeed,
					Steps2 ++ Steps, IterationCount);
		_ ->
			false
	end.
