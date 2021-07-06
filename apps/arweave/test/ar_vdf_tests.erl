-module(ar_vdf_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar_vdf.hrl").

-define(ENCODED_PREV_OUTPUT, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(RESET_SEED, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(MAX_THREAD_COUNT, 4).

% TODO test once before before release
% -define(TEST_VDF_DIFFICULTY, ?VDF_DIFFICULTY).
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
	{timeout, 500, fun test_vdf_basic_compute_verify_/0}.

% no reset
test_vdf_basic_compute_verify_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),

	ResetStepNumber = -1,

	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	BufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, BufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	BufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	BufferHash1_2 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash1_2} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	% test damage on any byte, arg (aka negative tests)
	ok = test_vdf_basic_compute_verify_break_(StartStepNumber1, PrevOutput, 1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	ok.

test_vdf_basic_compute_verify_break_(StartStepNumber, PrevOutput, StepBetweenHashCount, HashCount, BufferHash, ResetStepNumber, ResetSeed, ThreadCount, IterationCount)->
	test_vdf_basic_compute_verify_break_(StartStepNumber, PrevOutput, StepBetweenHashCount, HashCount, BufferHash, ResetStepNumber, ResetSeed, ThreadCount, IterationCount, size(BufferHash)-1).

test_vdf_basic_compute_verify_break_(_StartStepNumber, _PrevOutput, _StepBetweenHashCount, _HashCount, _BufferHash, _ResetStepNumber, _ResetSeed, _ThreadCount, _IterationCount, 0)->
	ok;

test_vdf_basic_compute_verify_break_(StartStepNumber, PrevOutput, StepBetweenHashCount, HashCount, BufferHash, ResetStepNumber, ResetSeed, ThreadCount, IterationCount, BreakPos)->
	BufferHashBroken = break_byte(BufferHash, BreakPos),
	false = ar_vdf:verify(StartStepNumber, PrevOutput, [{StepBetweenHashCount, HashCount, BufferHashBroken}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	test_vdf_basic_compute_verify_break_(StartStepNumber, PrevOutput, StepBetweenHashCount, HashCount, BufferHash, ResetStepNumber, ResetSeed, ThreadCount, IterationCount, BreakPos-1).


vdf_reset_test_() ->
	{timeout, 500, fun test_vdf_reset_verify_/0}.

test_vdf_reset_verify_() ->
	ok = test_vdf_reset_0_(),
	ok = test_vdf_reset_1_(),
	ok = test_vdf_reset_mid_checkpoint_(),
	ok.

test_vdf_reset_0_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber1,
	
	MixOutput = reset_mix(PrevOutput, ResetSeed),
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, MixOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	
	% partial verify should work
	BufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, BufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	<< Step1:?VDF_BYTE_SIZE/binary, _/binary >> = LastStepCheckpoints2,
	BufferHash3 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary>>,
	{true, BufferHash3} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT+1, BufferHash3}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash4 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash4} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash4}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_vdf_reset_1_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber2,
	
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1, ResetSeed),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, MixOutput, ?TEST_VDF_DIFFICULTY),
	
	% partial verify should work
	BufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, BufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	<< Step1:?VDF_BYTE_SIZE/binary, Step2:?VDF_BYTE_SIZE/binary, _/binary >> = LastStepCheckpoints2,
	BufferHash3 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary>>,
	{true, BufferHash3} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT+1, BufferHash3}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash4 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary, Step2/binary>>,
	{true, BufferHash4} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT+1+1, BufferHash4}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash5 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash5} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash5}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_vdf_reset_mid_checkpoint_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	% means inside 1 iteration
	ResetStepNumberFlat = 10,
	ResetStepNumber = StartStepNumber1+ResetStepNumberFlat,
	
	Salt1 = << StartStepNumber1:256 >>,
	{ok, Output1Part1, LastStepCheckpoints1Part1} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevOutput, ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1Part1, ResetSeed),
	Salt2 = << ResetStepNumber:256 >>,
	{ok, Output1Part2, LastStepCheckpoints1Part2} = ar_mine_randomx:vdf_sha2_nif(Salt2, MixOutput, ?VDF_STEP_COUNT_IN_CHECKPOINT-ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	Output1 = Output1Part2,
	LastStepCheckpoints1 = <<LastStepCheckpoints1Part1/binary, Output1Part1/binary, LastStepCheckpoints1Part2/binary>>,
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	
	% partial verify should work
	BufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, BufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	<< Step1:?VDF_BYTE_SIZE/binary, Step2:?VDF_BYTE_SIZE/binary, _/binary >> = LastStepCheckpoints2,
	BufferHash3 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary>>,
	{true, BufferHash3} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT+1, BufferHash3}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash4 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary, Step2/binary>>,
	{true, BufferHash4} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, ?VDF_STEP_COUNT_IN_CHECKPOINT+1+1, BufferHash4}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash5 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, BufferHash5} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash5}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.


vdf_skip_test_() ->
	{timeout, 500, fun test_vdf_skip_verify_/0}.

test_vdf_skip_verify_() ->
	ok = test_vdf_skip_(),
	ok = test_vdf_skip_reset_0_(),
	ok = test_vdf_skip_reset_1_(),
	ok = test_vdf_skip_reset_mid_checkpoint_(),
	ok.

test_vdf_skip_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = -1,
	
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	BufferHash1 = Output1,
	FullBufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, FullBufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	BufferHash2 = Output2,
	FullBufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash1_2 = <<Output1/binary, Output2/binary>>,
	FullBufferHash1_2 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash1_2} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_vdf_skip_reset_0_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber1,
	
	MixOutput = reset_mix(PrevOutput, ResetSeed),
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, MixOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	
	% partial verify should work
	BufferHash1 = Output1,
	FullBufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, FullBufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2 = Output2,
	FullBufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	<< Step1:?VDF_BYTE_SIZE/binary, _/binary >> = LastStepCheckpoints2,
	BufferHash3 = Step1,
	FullBufferHash3 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary>>,
	{true, FullBufferHash3} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT+1, 1, BufferHash3}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash4 = <<Output1/binary, Output2/binary>>,
	FullBufferHash4 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash4} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash4}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_vdf_skip_reset_1_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber2,
	
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1, ResetSeed),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, MixOutput, ?TEST_VDF_DIFFICULTY),
	
	% partial verify should work
	BufferHash1 = Output1,
	FullBufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, FullBufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2 = Output2,
	FullBufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	<< Step1:?VDF_BYTE_SIZE/binary, Step2:?VDF_BYTE_SIZE/binary, _/binary >> = LastStepCheckpoints2,
	BufferHash3 = Step1,
	FullBufferHash3 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary>>,
	{true, FullBufferHash3} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT+1, 1, BufferHash3}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash4 = Step2,
	FullBufferHash4 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary, Step2/binary>>,
	{true, FullBufferHash4} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT+1+1, 1, BufferHash4}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash5 = <<Output1/binary, Output2/binary>>,
	FullBufferHash5 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash5} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash5}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_vdf_skip_reset_mid_checkpoint_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	% means inside 1 iteration
	ResetStepNumberFlat = 10,
	ResetStepNumber = StartStepNumber1+ResetStepNumberFlat,
	
	Salt1 = << StartStepNumber1:256 >>,
	{ok, Output1Part1, LastStepCheckpoints1Part1} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevOutput, ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1Part1, ResetSeed),
	Salt2 = << ResetStepNumber:256 >>,
	{ok, Output1Part2, LastStepCheckpoints1Part2} = ar_mine_randomx:vdf_sha2_nif(Salt2, MixOutput, ?VDF_STEP_COUNT_IN_CHECKPOINT-ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	Output1 = Output1Part2,
	LastStepCheckpoints1 = <<LastStepCheckpoints1Part1/binary, Output1Part1/binary, LastStepCheckpoints1Part2/binary>>,
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	
	% partial verify should work
	BufferHash1 = Output1,
	FullBufferHash1 = <<LastStepCheckpoints1/binary, Output1/binary>>,
	{true, FullBufferHash1} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash1}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2 = Output2,
	FullBufferHash2 = <<LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash2} = ar_vdf:verify(StartStepNumber2, Output1, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	<< Step1:?VDF_BYTE_SIZE/binary, Step2:?VDF_BYTE_SIZE/binary, _/binary >> = LastStepCheckpoints2,
	BufferHash3 = Step1,
	FullBufferHash3 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary>>,
	{true, FullBufferHash3} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT+1, 1, BufferHash3}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash4 = Step2,
	FullBufferHash4 = <<LastStepCheckpoints1/binary, Output1/binary, Step1/binary, Step2/binary>>,
	{true, FullBufferHash4} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT+1+1, 1, BufferHash4}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	
	BufferHash5 = <<Output1/binary, Output2/binary>>,
	FullBufferHash5 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, FullBufferHash5} = ar_vdf:verify(StartStepNumber1, PrevOutput, [{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash5}], ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.


vdf_multigroup_test_() ->
	{timeout, 500, fun test_multigroup_verify_/0}.

test_multigroup_verify_() ->
	ok = test_multigroup_regular_(),
	ok = test_multigroup_reset_0_(),
	ok = test_multigroup_reset_1_(),
	ok = test_multigroup_reset_2_(),
	ok = test_multigroup_reset_0_plus_(),
	ok = test_multigroup_reset_1_plus_(),
	ok = test_multigroup_reset_2_plus_(),
	ok.

test_multigroup_regular_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = -1,
	
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	{ok, Output3, LastStepCheckpoints3} = ar_vdf:compute(StartStepNumber3, Output2, ?TEST_VDF_DIFFICULTY),
	
	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	
	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_multigroup_reset_0_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber1,
	
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	MixOutput = reset_mix(PrevOutput, ResetSeed),
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, MixOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	{ok, Output3, LastStepCheckpoints3} = ar_vdf:compute(StartStepNumber3, Output2, ?TEST_VDF_DIFFICULTY),
	
	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	
	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_multigroup_reset_1_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber2,
	
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1, ResetSeed),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, MixOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output3, LastStepCheckpoints3} = ar_vdf:compute(StartStepNumber3, Output2, ?TEST_VDF_DIFFICULTY),
	
	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	
	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_multigroup_reset_2_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumber = StartStepNumber3,
	
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output2, ResetSeed),
	{ok, Output3, LastStepCheckpoints3} = ar_vdf:compute(StartStepNumber3, MixOutput, ?TEST_VDF_DIFFICULTY),
	
	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	
	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_multigroup_reset_0_plus_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumberFlat = 10,
	ResetStepNumber = StartStepNumber1+ResetStepNumberFlat,
	
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	
	Salt1 = << StartStepNumber1:256 >>,
	{ok, Output1Part1, LastStepCheckpoints1Part1} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevOutput, ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output1Part1, ResetSeed),
	Salt2 = << ResetStepNumber:256 >>,
	{ok, Output1Part2, LastStepCheckpoints1Part2} = ar_mine_randomx:vdf_sha2_nif(Salt2, MixOutput, ?VDF_STEP_COUNT_IN_CHECKPOINT-ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	Output1 = Output1Part2,
	LastStepCheckpoints1 = <<LastStepCheckpoints1Part1/binary, Output1Part1/binary, LastStepCheckpoints1Part2/binary>>,
	
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),
	{ok, Output3, LastStepCheckpoints3} = ar_vdf:compute(StartStepNumber3, Output2, ?TEST_VDF_DIFFICULTY),
	
	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	
	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	
	ok.

test_multigroup_reset_1_plus_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),
	
	ResetStepNumberFlat = 10,
	ResetStepNumber = StartStepNumber2+ResetStepNumberFlat,
	
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	
	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	
	Salt1 = << StartStepNumber2:256 >>,
	{ok, Output2Part1, LastStepCheckpoints2Part1} = ar_mine_randomx:vdf_sha2_nif(Salt1, Output1, ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output2Part1, ResetSeed),
	Salt2 = << ResetStepNumber:256 >>,
	{ok, Output2Part2, LastStepCheckpoints2Part2} = ar_mine_randomx:vdf_sha2_nif(Salt2, MixOutput, ?VDF_STEP_COUNT_IN_CHECKPOINT-ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	Output2 = Output2Part2,
	LastStepCheckpoints2 = <<LastStepCheckpoints2Part1/binary, Output2Part1/binary, LastStepCheckpoints2Part2/binary>>,
	
	{ok, Output3, LastStepCheckpoints3} = ar_vdf:compute(StartStepNumber3, Output2, ?TEST_VDF_DIFFICULTY),
	
	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	
	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	% BufferHash1_1 = <<Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
		%{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	ok.

test_multigroup_reset_2_plus_() ->
	StartStepNumber1 = ?VDF_STEP_COUNT_IN_CHECKPOINT*1,
	StartStepNumber2 = ?VDF_STEP_COUNT_IN_CHECKPOINT*2,
	StartStepNumber3 = ?VDF_STEP_COUNT_IN_CHECKPOINT*3,
	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),
	ResetSeed = ar_util:decode(?RESET_SEED),

	ResetStepNumberFlat = 10,
	ResetStepNumber = StartStepNumber3+ResetStepNumberFlat,

	PrevOutput = ar_util:decode(?ENCODED_PREV_OUTPUT),

	{ok, Output1, LastStepCheckpoints1} = ar_vdf:compute(StartStepNumber1, PrevOutput, ?TEST_VDF_DIFFICULTY),
	{ok, Output2, LastStepCheckpoints2} = ar_vdf:compute(StartStepNumber2, Output1, ?TEST_VDF_DIFFICULTY),

	Salt1 = << StartStepNumber3:256 >>,
	{ok, Output3Part1, LastStepCheckpoints3Part1} = ar_mine_randomx:vdf_sha2_nif(Salt1, Output2, ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	MixOutput = reset_mix(Output3Part1, ResetSeed),
	Salt2 = << ResetStepNumber:256 >>,
	{ok, Output3Part2, LastStepCheckpoints3Part2} = ar_mine_randomx:vdf_sha2_nif(Salt2, MixOutput, ?VDF_STEP_COUNT_IN_CHECKPOINT-ResetStepNumberFlat-1, 0, ?TEST_VDF_DIFFICULTY),
	Output3 = Output3Part2,
	LastStepCheckpoints3 = <<LastStepCheckpoints3Part1/binary, Output3Part1/binary, LastStepCheckpoints3Part2/binary>>,

	FullBufferHash = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,

	BufferHash1_1 = <<Output1/binary, Output2/binary>>,
	% BufferHash1_1 = <<Output2/binary>>,
	BufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	Groups1_1 = [
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
		%{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash1_1}
	],
	PartBufferHash1_1 = <<LastStepCheckpoints1/binary, Output1/binary, LastStepCheckpoints2/binary, Output2/binary>>,
	{true, PartBufferHash1_1} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_1, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_2 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2}
	],
	PartBufferHash1_2 = <<LastStepCheckpoints3/binary, Output3/binary>>,
	{true, PartBufferHash1_2} = ar_vdf:verify(StartStepNumber3, Output2, Groups1_2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),
	Groups1_3 = [
		{1, ?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash1_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 2, BufferHash1_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups1_3, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	BufferHash2_1 = Output1,
	BufferHash2_2 = <<LastStepCheckpoints2/binary, Output2/binary, LastStepCheckpoints3/binary, Output3/binary>>,
	Groups2 = [
		{1, 2*?VDF_STEP_COUNT_IN_CHECKPOINT, BufferHash2_2},
		{?VDF_STEP_COUNT_IN_CHECKPOINT, 1, BufferHash2_1}
	],
	{true, FullBufferHash} = ar_vdf:verify(StartStepNumber1, PrevOutput, Groups2, ResetStepNumber, ResetSeed, ?MAX_THREAD_COUNT, ?TEST_VDF_DIFFICULTY),

	ok.
