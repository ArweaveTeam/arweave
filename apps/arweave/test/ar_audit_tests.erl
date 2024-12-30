-module(ar_audit_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

setup_replica_2_9() ->
	FastState = ar_mine_randomx:init_fast2(rxsquared, ?RANDOMX_PACKING_KEY, 0, 0,
			erlang:system_info(dirty_cpu_schedulers_online)),
	LightState = ar_mine_randomx:init_light2(rxsquared, ?RANDOMX_PACKING_KEY, 0, 0),
	{FastState, LightState}.

test_register(TestFun, Fixture) ->
	{timeout, 120, {with, Fixture, [TestFun]}}.

randomx_replica_2_9_suite_test_() ->
	{setup, fun setup_replica_2_9/0,
		fun (SetupData) ->
			[
				test_register(fun test_vectors/1, SetupData), % TODO move bottom
				test_register(fun test_state/1, SetupData),
				test_register(fun test_quick/1, SetupData),
				test_register(fun test_pack_unpack_sub_chunks/1, SetupData)
			]
		end
	}.


%% -------------------------------------------------------------------------------------------
%% replica_2_9 tests
%% -------------------------------------------------------------------------------------------
test_state({FastState, LightState}) ->

	?assertEqual(
		{ok, {rxsquared, fast, 67602036, 2097152}},
		ar_mine_randomx:info(FastState)
	),
	?assertEqual(
		{ok, {rxsquared, light, 0, 2097152}},
		ar_mine_randomx:info(LightState)
	),

	{ok, {_, _, _, ScratchpadSize}} = ar_mine_randomx:info(FastState),
	?assertEqual(?RANDOMX_SCRATCHPAD_SIZE, ScratchpadSize).

test_vectors({FastState, _LightState}) ->
	Hash = << 255:(8*64) >>,
	Scratchpad = << 255:(8*2097152) >>,
	{ok, OutHash1Real, Output1} = ar_rxsquared_nif:rsp_exec_test_nif(element(2, FastState), Hash, Scratchpad, 0, 0, 0, 8),
	Output1HashReal = crypto:hash(sha256, Output1),
	Output1HashExpd = << 23,173,31,182,17,62,103,254,86,234,161,194,62,234,
		176,71,171,120,18,186,252,150,107,106,65,5,197,85,
		108,100,151,250 >>,

	{ok, Output2v1} = ar_rxsquared_nif:rsp_mix_entropy_crc32_nif(Output1),
	Output2v1HashReal = crypto:hash(sha256, Output2v1),
	{ok, OutHash2Real, Output2v2} = ar_rxsquared_nif:rsp_exec_nif(element(2, FastState), Hash, Scratchpad, 0, 0, 0, 8),
	Output2v2HashReal = crypto:hash(sha256, Output2v2),
	Output2HashExpd = << 133,226,122,189,170,63,128,182,242,28,50,204,85,179,
		230,105,98,187,39,24,30,133,84,135,70,85,220,145,30,
		165,161,242 >>,
	OutHashExpd = << 137,100,229,43,87,136,2,64,101,172,17,65,106,94,24,
		209,195,201,194,250,35,211,175,73,15,102,11,25,12,
		147,231,196,194,48,55,194,181,47,41,136,101,215,179,
		124,181,223,195,140,217,56,206,55,144,184,44,131,86,
		206,247,3,124,167,34,75 >>,

	?assertEqual(Output1HashExpd, Output1HashReal),
	?assertEqual(Output2HashExpd, Output2v1HashReal),
	?assertEqual(Output2HashExpd, Output2v2HashReal),
	?assertEqual(OutHashExpd, OutHash1Real),
	?assertEqual(OutHashExpd, OutHash2Real),

	Key = << 1 >>,
	Entropy = ar_mine_randomx:randomx_generate_replica_2_9_entropy(FastState, Key),
	EntropyHash = crypto:hash(sha256, Entropy),
	EntropyHashExpd = << 56,199,231,119,170,151,220,154,45,204,70,193,80,68,
		46,50,136,31,35,102,141,77,19,66,191,127,97,183,230,
		119,243,151 >>,
	?assertEqual(EntropyHashExpd, EntropyHash),

	SubChunk = << 255:(8*8192) >>,
	EntropySubChunkIndex = 1,
	{ok, PackedOut} = ar_mine_randomx:randomx_encrypt_replica_2_9_sub_chunk({FastState, Entropy, SubChunk,
		EntropySubChunkIndex}),
	PackedOutHashReal = crypto:hash(sha256, PackedOut),
	PackedOutHashExpd = << 15,46,184,11,124,31,150,77,199,107,221,0,136,154,61,
		146,193,198,126,52,19,7,211,28,121,108,176,15,124,33,
		48,99 >>,
	?assertEqual(PackedOutHashExpd, PackedOutHashReal),
	{ok, SubChunkReal} = ar_mine_randomx:randomx_decrypt_replica_2_9_sub_chunk({FastState, Key, PackedOut,
		EntropySubChunkIndex}),
	?assertEqual(SubChunk, SubChunkReal),

	{ok, EntropyFused} = ar_rxsquared_nif:rsp_fused_entropy_nif(
		element(2, FastState),
		?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
		?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
		?REPLICA_2_9_RANDOMX_LANE_COUNT,
		?REPLICA_2_9_RANDOMX_DEPTH,
		0,
		0,
		0,
		?REPLICA_2_9_RANDOMX_ROUND_COUNT,
		Key
	),
	EntropyFusedHash = crypto:hash(sha256, EntropyFused),
	?assertEqual(EntropyHashExpd, EntropyFusedHash),

	ok.

test_quick({FastState, _LightState}) ->
	{ok, _, _} = ar_rxsquared_nif:rsp_exec_nif(
			element(2, FastState), << 0:(8*64) >>, << 0:(8*2097152) >>, 0, 0, 0, 8),
	{ok, _, _} = ar_rxsquared_nif:rsp_init_scratchpad_nif(
			element(2, FastState), <<"Some input">>, 0, 0, 0, 8),
	{ok, _} = ar_rxsquared_nif:rsp_mix_entropy_crc32_nif(<< 0:(8*2097152) >>),
	{ok, _} = ar_rxsquared_nif:rsp_mix_entropy_far_nif(<< 0:(8*2097152) >>),
	{ok, _} = ar_rxsquared_nif:rsp_feistel_encrypt_nif(
			<< 0:(8*2097152) >>, << 0:(8*2097152) >>),
	{ok, _} = ar_rxsquared_nif:rsp_feistel_decrypt_nif(
			<< 0:(8*2097152) >>, << 0:(8*2097152) >>).

test_pack_unpack_sub_chunks({State, _LightState}) ->
	Key = << 0:256 >>,
	SubChunk = << 0:(8192 * 8) >>,
	Entropy = ar_mine_randomx:randomx_generate_replica_2_9_entropy(State, Key),
	?assertEqual(268435456, byte_size(Entropy)),
	PackedSubChunks = pack_sub_chunks(SubChunk, Entropy, 0, SubChunk, State),
	?assert(lists:all(fun(PackedSubChunk) -> byte_size(PackedSubChunk) == 8192 end,
			PackedSubChunks)),
	unpack_sub_chunks(PackedSubChunks, 0, SubChunk, Entropy).

pack_sub_chunks(_SubChunk, _Entropy, Index, _PreviousSubChunk, _State)
		when Index == 32768 ->
	[];
pack_sub_chunks(SubChunk, Entropy, Index, PreviousSubChunk, State) ->
	{ok, PackedSubChunk} = ar_mine_randomx:randomx_encrypt_replica_2_9_sub_chunk(
			{State, Entropy, SubChunk, Index}),
	Note = io_lib:format("Packed a sub-chunk, index=~B.~n", [Index]),
	?assertNotEqual(PackedSubChunk, PreviousSubChunk, Note),
	[PackedSubChunk | pack_sub_chunks(SubChunk, Entropy, Index + 1, PackedSubChunk, State)].

unpack_sub_chunks([], _Index, _SubChunk, _Entropy) ->
	ok;
unpack_sub_chunks([PackedSubChunk | PackedSubChunks], Index, SubChunk, Entropy) ->
	{ok, UnpackedSubChunk} = ar_mine_randomx:randomx_decrypt_replica_2_9_sub_chunk2(
			{Entropy, PackedSubChunk, Index}),
	Note = io_lib:format("Unpacked a sub-chunk, index=~B.~n", [Index]),
	?assertEqual(SubChunk, UnpackedSubChunk, Note),
	unpack_sub_chunks(PackedSubChunks, Index + 1, SubChunk, Entropy).
