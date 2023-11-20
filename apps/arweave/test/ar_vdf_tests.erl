-module(ar_vdf_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-define(ENCODED_PREV_OUTPUT, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(RESET_SEED, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(MAX_THREAD_COUNT, 4).

%-define(TEST_VDF_DIFFICULTY, 15000000 div 25).
-define(TEST_VDF_DIFFICULTY, 10).

%%%===================================================================
%%% utils
%%%===================================================================

break_byte(Buf, Pos)->
	Head = binary:part(Buf, 0, Pos),
	Tail = binary:part(Buf, Pos+1, size(Buf)-Pos-1),
	ChangedByte = binary:at(Buf,Pos) bxor 1,
	<<Head/binary, ChangedByte, Tail/binary>>.

reset_mix(PrevOutput, ResetSeed) ->
	crypto:hash(sha256, << PrevOutput/binary, ResetSeed/binary >>).

%%%===================================================================

vdf_basic_test_() ->
	{timeout, 1000, fun test_vdf_basic_compute_verify_/0}.

% no reset
test_vdf_basic_compute_verify_() ->
	StartStepNumber1 = 2,
	StartStepNumber2 = 3,
	StartSalt1 = ar_vdf:step_number_to_salt_number(StartStepNumber1-1),
	StartSalt2 = ar_vdf:step_number_to_salt_number(StartStepNumber2-1),
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),

	ResetSalt = -1,

	{ok, Output1, Checkpoints1} =
			ar_vdf:compute2(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse(Checkpoints1)),

	{ok, _Output2, Checkpoints2} =
			ar_vdf:compute2(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	assert_verify(StartSalt2, ResetSalt, Output1, 1, lists:reverse(Checkpoints2)),

	Hashes = lists:reverse(Checkpoints1) ++ lists:reverse(Checkpoints2),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, Hashes),

	% test damage on any byte, arg (aka negative tests)
	ok = test_vdf_basic_compute_verify_break_(StartSalt1, PrevOutput, 1,
		Hashes, ResetSalt, ResetSeed),

	ok.

test_vdf_basic_compute_verify_break_(StartSalt, PrevOutput, StepBetweenHashCount,
		Hashes, ResetSalt, ResetSeed)->
	test_vdf_basic_compute_verify_break_(StartSalt, PrevOutput, StepBetweenHashCount,
		Hashes, ResetSalt, ResetSeed,
		size(iolist_to_binary(Hashes))-1).

test_vdf_basic_compute_verify_break_(_StartSalt, _PrevOutput, _StepBetweenHashCount,
		_Hashes, _ResetSalt, _ResetSeed, 0)->
	ok;

test_vdf_basic_compute_verify_break_(StartSalt, PrevOutput, StepBetweenHashCount,
		Hashes, ResetSalt, ResetSeed, BreakPos)->
	BufferHash = iolist_to_binary(Hashes),
	BufferHashBroken = break_byte(BufferHash, BreakPos),
	HashesBroken = ar_vdf:checkpoint_buffer_to_checkpoints(BufferHashBroken),
	false = ar_vdf:verify(StartSalt, PrevOutput,
		StepBetweenHashCount, HashesBroken, ResetSalt, ResetSeed,
		?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	test_vdf_basic_compute_verify_break_(StartSalt, PrevOutput, StepBetweenHashCount,
		Hashes, ResetSalt, ResetSeed, BreakPos-1).

assert_verify(StartSalt, ResetSalt, Output, NumCheckpointsBetweenHashes, Checkpoints) ->
	ResetSeed = ar_util:decode(?RESET_SEED),
	?assertEqual(
		{true, iolist_to_binary(Checkpoints)},
		ar_vdf:verify(
			StartSalt, Output, NumCheckpointsBetweenHashes, Checkpoints, ResetSalt, ResetSeed,
			?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY)
	).

vdf_reset_test_() ->
	{timeout, 1000, fun test_vdf_reset_verify_/0}.

test_vdf_reset_verify_() ->
	ok = test_vdf_reset_0_(),
	ok = test_vdf_reset_1_(),
	ok = test_vdf_reset_mid_checkpoint_(),
	ok.

test_vdf_reset_0_() ->
	StartStepNumber1 = 2,
	StartStepNumber2 = 3,
	StartSalt1 = ar_vdf:step_number_to_salt_number(StartStepNumber1-1),
	StartSalt2 = ar_vdf:step_number_to_salt_number(StartStepNumber2-1),
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),

	ResetSalt = StartSalt1,

	MixOutput = reset_mix(PrevOutput, ResetSeed),
	{ok, Output1, Checkpoints1} = ar_vdf:compute2(StartStepNumber1, MixOutput, ?TEST_VDF_DIFFICULTY),
	{ok, _Output2, Checkpoints2} = ar_vdf:compute2(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),

	% partial verify should work
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse(Checkpoints1)),
	assert_verify(StartSalt2, ResetSalt, Output1, 1, lists:reverse(Checkpoints2)),

	Hashes3 = lists:sublist(lists:reverse(Checkpoints2), 1, ?VDF_CHECKPOINT_COUNT_IN_STEP + 1),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse(Checkpoints1) ++ Hashes3),

	Hashes4 = lists:reverse(Checkpoints1) ++ lists:reverse(Checkpoints2),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, Hashes4),

	ok.

test_vdf_reset_1_() ->
	StartStepNumber1 = 2,
	StartStepNumber2 = 3,
	StartSalt1 = ar_vdf:step_number_to_salt_number(StartStepNumber1-1),
	StartSalt2 = ar_vdf:step_number_to_salt_number(StartStepNumber2-1),
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),

	ResetSalt = StartSalt2,

	{ok, Output1, Checkpoints1} = ar_vdf:compute2(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1, ResetSeed),
	{ok, _Output2, Checkpoints2} = ar_vdf:compute2(StartStepNumber2, MixOutput, ?TEST_VDF_DIFFICULTY),

	% partial verify should work
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse(Checkpoints1)),
	assert_verify(StartSalt2, ResetSalt, Output1, 1, lists:reverse(Checkpoints2)),

	Hash1 = lists:last(Checkpoints2),
	Hash2 = lists:nth(length(Checkpoints2) - 1, Checkpoints2),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse([Hash1 | Checkpoints1])),

	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse([Hash2, Hash1 | Checkpoints1])),

	Hashes5 = lists:reverse(Checkpoints1) ++ lists:reverse(Checkpoints2),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, Hashes5),

	ok.

test_vdf_reset_mid_checkpoint_() ->
	StartStepNumber1 = 2,
	StartStepNumber2 = 3,
	StartSalt1 = ar_vdf:step_number_to_salt_number(StartStepNumber1-1),
	StartSalt2 = ar_vdf:step_number_to_salt_number(StartStepNumber2-1),
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),

	% means inside 1 iteration
	ResetSaltFlat = 10,
	ResetSalt = StartSalt1 + ResetSaltFlat,

	Salt1 = << StartSalt1:256 >>,
	{ok, Output1Part1, LastStepCheckpoints1Part1} =
		ar_mine_randomx:vdf_sha2_nif(Salt1, PrevOutput, ResetSaltFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1Part1, ResetSeed),

	Salt2 = << ResetSalt:256 >>,
	{ok, Output1Part2, LastStepCheckpoints1Part2} =
		ar_mine_randomx:vdf_sha2_nif(Salt2, MixOutput, ?VDF_CHECKPOINT_COUNT_IN_STEP-ResetSaltFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	Output1 = Output1Part2,
	LastStepCheckpoints1 = <<
		LastStepCheckpoints1Part1/binary, Output1Part1/binary,
		LastStepCheckpoints1Part2/binary, Output1Part2/binary
	>>,
	Checkpoints1 = ar_vdf:checkpoint_buffer_to_checkpoints(LastStepCheckpoints1),

	{ok, _Output2, Checkpoints2} = ar_vdf:compute2(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),

	% partial verify should work
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse(Checkpoints1)),
	assert_verify(StartSalt2, ResetSalt, Output1, 1, lists:reverse(Checkpoints2)),

	Hash1 = lists:last(Checkpoints2),
	Hash2 = lists:nth(length(Checkpoints2) - 1, Checkpoints2),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse([Hash1 | Checkpoints1])),

	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, lists:reverse([Hash2, Hash1 | Checkpoints1])),

	Hashes5 = lists:reverse(Checkpoints1) ++ lists:reverse(Checkpoints2),
	assert_verify(StartSalt1, ResetSalt, PrevOutput, 1, Hashes5),

	ok.

compute_next_vdf_difficulty_test_block() ->
	Height1 = max(?BLOCK_TIME_HISTORY_BLOCKS, ?REWARD_HISTORY_BLOCKS),
	Height2 = Height1 + ?VDF_DIFFICULTY_RETARGET - Height1 rem ?VDF_DIFFICULTY_RETARGET,
	#block{
		height = Height2-1,
		nonce_limiter_info = #nonce_limiter_info{
			vdf_difficulty = 10000,
			next_vdf_difficulty = 10000
		},
		reward_history = lists:duplicate(?REWARD_HISTORY_BLOCKS, {<<>>, 10000, 10, 1}),
		block_time_history = lists:duplicate(?BLOCK_TIME_HISTORY_BLOCKS, {129, 135, 1}),
		price_per_gib_minute = 10000,
		scheduled_price_per_gib_minute = 15000
	}.

compute_next_vdf_difficulty_2_7_test_()->
	ar_test_node:test_with_mocked_functions(
		[{ar_fork, height_2_6, fun() -> -1 end},
		{ar_fork, height_2_7, fun() -> -1 end},
		{ar_fork, height_2_7_1, fun() -> infinity end}],
		fun() ->
			B = compute_next_vdf_difficulty_test_block(),
			10465 = ar_block:compute_next_vdf_difficulty(B),
			ok
		end).

compute_next_vdf_difficulty_2_7_1_test_()->
	ar_test_node:test_with_mocked_functions(
		[{ar_fork, height_2_6, fun() -> -1 end},
		{ar_fork, height_2_7, fun() -> -1 end},
		{ar_fork, height_2_7_1, fun() -> -1 end}],
		fun() ->
			B = compute_next_vdf_difficulty_test_block(),
			10046 = ar_block:compute_next_vdf_difficulty(B),
			ok
		end).
