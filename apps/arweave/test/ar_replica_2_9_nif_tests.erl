-module(ar_replica_2_9_nif_tests).

-include_lib("eunit/include/eunit.hrl").

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
				test_register(fun test_pack_unpack_sub_chunks/1, SetupData)
			]
		end
	}.


%% -------------------------------------------------------------------------------------------
%% replica_2_9 tests
%% -------------------------------------------------------------------------------------------
test_state({FastState, LightState}) ->

	?assertEqual(
		{ok, {rxsquared, fast, 34047604, 2097152}},
		ar_mine_randomx:info(FastState)
	),
	?assertEqual(
		{ok, {rxsquared, light, 0, 2097152}},
		ar_mine_randomx:info(LightState)
	),

	{ok, {_, _, _, ScratchpadSize}} = ar_mine_randomx:info(FastState),
	?assertEqual(?RANDOMX_SCRATCHPAD_SIZE, ScratchpadSize).

test_vectors({FastState, _LightState}) ->
	Key = << 1 >>,
	Entropy = ar_mine_randomx:randomx_generate_replica_2_9_entropy(FastState, Key),
	EntropyHash = crypto:hash(sha256, Entropy),
	EntropyHashExpd = << 56,199,231,119,170,151,220,154,45,204,70,193,80,68,
		46,50,136,31,35,102,141,77,19,66,191,127,97,183,230,
		119,243,151 >>,
	?assertEqual(EntropyHashExpd, EntropyHash),

	Key2 = << 2 >>,
	Entropy2 = ar_mine_randomx:randomx_generate_replica_2_9_entropy(FastState, Key2),
	EntropyHash2 = crypto:hash(sha256, Entropy2),
	EntropyHashExpd2 = << 206,47,133,111,139,20,31,64,185,33,107,29,14,10,252,
		76,201,75,203,186,131,32,20,45,34,125,76,248,64,90,
		220,196 >>,
	?assertEqual(EntropyHashExpd2, EntropyHash2),

	SubChunk = << 255:(8*8192) >>,
	EntropySubChunkIndex = 1,
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_replica_2_9_sub_chunk(
		{FastState, Entropy, SubChunk, EntropySubChunkIndex}),
	PackedHashReal = crypto:hash(sha256, Packed),
	PackedHashExpd = << 15,46,184,11,124,31,150,77,199,107,221,0,136,154,61,
		146,193,198,126,52,19,7,211,28,121,108,176,15,124,33,
		48,99 >>,
	?assertEqual(PackedHashExpd, PackedHashReal),
	{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_replica_2_9_sub_chunk(
		{FastState, Key, Packed, EntropySubChunkIndex}),
	?assertEqual(SubChunk, Unpacked),

	ok.

test_pack_unpack_sub_chunks({State, _LightState}) ->
	Key = << 0:256 >>,
	SubChunk = << 0:(8192 * 8) >>,
	Entropy = ar_mine_randomx:randomx_generate_replica_2_9_entropy(State, Key),
	?assertEqual(8388608, byte_size(Entropy)),
	PackedSubChunks = pack_sub_chunks(SubChunk, Entropy, 0, SubChunk, State),
	?assert(lists:all(fun(PackedSubChunk) -> byte_size(PackedSubChunk) == 8192 end,
			PackedSubChunks)),
	unpack_sub_chunks(PackedSubChunks, 0, SubChunk, Entropy).

pack_sub_chunks(_SubChunk, _Entropy, Index, _PreviousSubChunk, _State)
		when Index == 1024 ->
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
