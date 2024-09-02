-module(ar_mine_randomx_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-define(ENCODED_HASH, <<"NcXUtn7gA42QoM8MtaS-vgVy8gJ21EE2YxV18mHndmM">>).
-define(ENCODED_NONCE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ENCODED_SEGMENT,
    <<"7XM3fgTCAY2GFpDjPZxlw4yw5cv8jNzZSZawywZGQ6_Ca-JDy2nX_MC2vjrIoDGp">>
).

encrypt_chunk(FastState, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES, _ExtraArgs) ->
	ar_mine_randomx:randomx_encrypt_chunk_nif(
		FastState, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES).

decrypt_chunk(FastState, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES, _ExtraArgs) ->
	ar_mine_randomx:randomx_decrypt_chunk_nif(
		FastState, Key, Chunk, byte_size(Chunk), PackingRounds, JIT, LargePages, HardwareAES).

reencrypt_chunk(FastState, Key1, Key2, Chunk, PackingRounds1, PackingRounds2,
		JIT, LargePages, HardwareAES, _ExtraArgs) ->
	ar_mine_randomx:randomx_reencrypt_chunk_nif(
		FastState, Key1, Key2, Chunk, byte_size(Chunk), PackingRounds1, PackingRounds2,
		JIT, LargePages, HardwareAES).

encrypt_composite_chunk(FastState, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES,
		[IterationCount, SubChunkCount] = _ExtraArgs) ->
	ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
		FastState, Key, Chunk, JIT, LargePages, HardwareAES, PackingRounds, 
		IterationCount, SubChunkCount).

decrypt_composite_chunk(FastState, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES,
		[IterationCount, SubChunkCount] = _ExtraArgs) ->
	ar_mine_randomx:randomx_decrypt_composite_chunk_nif(
		FastState, Key, Chunk, byte_size(Chunk), JIT, LargePages, HardwareAES,
		PackingRounds, IterationCount, SubChunkCount).

reencrypt_composite_chunk(FastState, Key1, Key2, Chunk, PackingRounds1, PackingRounds2,
		JIT, LargePages, HardwareAES, 
		[IterationCount1, IterationCount2, SubChunkCount1, SubChunkCount2] = _ExtraArgs) ->
	ar_mine_randomx:randomx_reencrypt_composite_chunk_nif(
		FastState, Key1, Key2, Chunk, JIT, LargePages, HardwareAES,
		PackingRounds1, PackingRounds2, IterationCount1, IterationCount2,
		SubChunkCount1, SubChunkCount2).

setup() ->
    {ok, FastState} = ar_mine_randomx:init_randomx_nif(
		?RANDOMX_PACKING_KEY, ?RANDOMX_HASHING_MODE_FAST, 0, 0,
			erlang:system_info(dirty_cpu_schedulers_online)),
	{ok, LightState} = ar_mine_randomx:init_randomx_nif(
		?RANDOMX_PACKING_KEY, ?RANDOMX_HASHING_MODE_LIGHT, 0, 0,
		erlang:system_info(dirty_cpu_schedulers_online)),
    {FastState, LightState}.

test_register(TestFun, Fixture) ->
	{timeout, 120, {with, Fixture, [TestFun]}}.

randomx_suite_test_() ->
	{setup, fun setup/0,
		fun (SetupData) ->
			[
				test_register(fun test_state/1, SetupData),
				test_register(fun test_regression/1, SetupData),
				test_register(fun test_empty_chunk_fails/1, SetupData),
				test_register(fun test_nif_wrappers/1, SetupData),
				test_register(fun test_pack_unpack/1, SetupData),
				test_register(fun test_repack/1, SetupData),
				test_register(fun test_input_changes_packing/1, SetupData),
				test_register(fun test_composite_packing/1, SetupData),
				test_register(fun test_composite_packs_incrementally/1, SetupData),
				test_register(fun test_composite_unpacked_sub_chunks/1, SetupData),
				test_register(fun test_composite_repack/1, SetupData),
				test_register(fun test_hash/1, SetupData)
			]
		end
	}.

%% -------------------------------------------------------------------------------------------
%% spora_2_6 and composite packing tests
%% -------------------------------------------------------------------------------------------
test_state({FastState, LightState}) ->
	%% The legacy dataset size is 568,433,920 bytes. Roughly 30 MiB more than 512 MiB.
	%% Our nifs don't have access to the raw dataset size used in the RandomX C code, but
	%% they have access to the dataset item count - which is just the size divided by 64.
	%% So the expected dataset size is 568,433,920 / 64 = 8,881,780 items.
	?assertEqual(
		{ok, fast, 8881780},
		ar_mine_randomx:randomx_info_nif(FastState)
	),
	%% Unfortunately we don't have access to the cache size. The randomx_info_nif will check
	%% that in fast mode the cache is not initialized, and in light mode the dataset is not
	%% initialized and return an error if either check fails.
	?assertEqual(
		{ok, light, 0},
		ar_mine_randomx:randomx_info_nif(LightState)
	).

test_regression({FastState, LightState}) ->
	%% Test all permutations of:
	%% 1. Light vs. fast state
	%% 2. spora_2_6 vs. depth-1 composite vs. depth-2 composite packing
	%% 3. JIT vs. no JIT
	test_regression(FastState,
		"ar_mine_randomx_tests/packed.spora26.bin", 0, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(FastState,
		"ar_mine_randomx_tests/packed.spora26.bin", 1, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(FastState,
		"ar_mine_randomx_tests/packed.composite.1.bin", 0, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(FastState,
		"ar_mine_randomx_tests/packed.composite.1.bin", 1, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(FastState,
		"ar_mine_randomx_tests/packed.composite.2.bin", 0, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(FastState,
		"ar_mine_randomx_tests/packed.composite.2.bin", 1, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState,
		"ar_mine_randomx_tests/packed.spora26.bin", 0, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(LightState,
		"ar_mine_randomx_tests/packed.spora26.bin", 1, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(LightState,
		"ar_mine_randomx_tests/packed.composite.1.bin", 0, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState,
		"ar_mine_randomx_tests/packed.composite.1.bin", 1, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState,
		"ar_mine_randomx_tests/packed.composite.2.bin", 0, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState,
		"ar_mine_randomx_tests/packed.composite.2.bin", 1, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	% Key = ar_test_node:load_fixture("ar_mine_randomx_tests/key.bin"),
	% UnpackedFixture = ar_test_node:load_fixture("ar_mine_randomx_tests/unpacked.bin"),
	% Dir = filename:dirname(?FILE),

	% {ok, Packed1} = ar_mine_randomx:randomx_encrypt_chunk_nif(
	% 	FastState, Key, UnpackedFixture, 8, 0, 0, 0),
	% Packed1Filename = filename:join([Dir, "fixtures", "ar_mine_randomx_tests", "packed.spora26.bin"]),
	% ok = file:write_file(Packed1Filename, Packed1),

	% {ok, Packed1} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
	% 	FastState, Key, UnpackedFixture, 0, 0, 0, 8, 1, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	% Packed1Filename = filename:join([Dir, "fixtures", "ar_mine_randomx_tests", "packed.composite.1.bin"]),
	% ok = file:write_file(Packed1Filename, Packed1),

	% {ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
	% 	FastState, Key, UnpackedFixture, 0, 0, 0, 8, 2, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	% Packed2Filename = filename:join([Dir, "fixtures", "ar_mine_randomx_tests", "packed.composite.2.bin"]),
	% ok = file:write_file(Packed2Filename, Packed2),

	ok.

test_regression(State, Fixture, JIT, ExtraArgs, EncryptFun, DecryptFun) ->
	Key = ar_test_node:load_fixture("ar_mine_randomx_tests/key.bin"),
	UnpackedFixture = ar_test_node:load_fixture("ar_mine_randomx_tests/unpacked.bin"),
	PackedFixture = ar_test_node:load_fixture(Fixture),

	{ok, Packed} = EncryptFun(State, Key, UnpackedFixture, 8, JIT, 0, 0, ExtraArgs),
	?assertEqual(PackedFixture, Packed),

	{ok, Unpacked} = DecryptFun(State, Key, PackedFixture, 8, JIT, 0, 0, ExtraArgs),
	?assertEqual(UnpackedFixture, Unpacked).


test_empty_chunk_fails({FastState, _LightState}) ->
	test_empty_chunk_fails(FastState, [], fun encrypt_chunk/8),
	test_empty_chunk_fails(FastState, [1, 32], fun encrypt_composite_chunk/8).

test_empty_chunk_fails(FastState, ExtraArgs, EncryptFun) ->
	try
		EncryptFun(FastState, crypto:strong_rand_bytes(32), <<>>, 1, 0, 0, 0, ExtraArgs),
		?assert(false, "Encrypt with an empty chunk should have failed")
	catch error:badarg ->
		ok
	end.

test_nif_wrappers({FastState, _LightState}) ->
	test_nif_wrappers(FastState,  crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 12)),
	test_nif_wrappers(FastState,  crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)).

test_nif_wrappers(State, Chunk) ->
	AddrA = crypto:strong_rand_bytes(32),
	AddrB = crypto:strong_rand_bytes(32),
	KeyA = crypto:strong_rand_bytes(32),
	KeyB= crypto:strong_rand_bytes(32),
	%% spora_26 randomx_encrypt_chunk 
	{ok, Packed_2_6A} = ar_mine_randomx:randomx_encrypt_chunk_nif(
		State, KeyA, Chunk, ?RANDOMX_PACKING_ROUNDS_2_6,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes()),
	?assertEqual({ok, Packed_2_6A},
		ar_mine_randomx:randomx_encrypt_chunk({spora_2_6, AddrA}, State, KeyA, Chunk)),

	%% composite randomx_encrypt_composite_chunk
	{ok, PackedCompositeA2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
		State, KeyA, Chunk,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
		?COMPOSITE_PACKING_ROUND_COUNT, 2, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	?assertEqual({ok, PackedCompositeA2},
		ar_mine_randomx:randomx_encrypt_chunk({composite, AddrA, 2}, State, KeyA, Chunk)),

	%% spora_2_6 randomx_decrypt_chunk
	?assertEqual({ok, Chunk},
		ar_mine_randomx:randomx_decrypt_chunk(
			{spora_2_6, AddrA}, State, KeyA, Packed_2_6A, byte_size(Chunk))),

	%% composite randomx_decrypt_composite_chunk
	?assertEqual({ok, Chunk},
		ar_mine_randomx:randomx_decrypt_chunk(
			{composite, AddrA, 2}, State, KeyA, PackedCompositeA2, byte_size(Chunk))),

	%% Prepare data for the reencryption tests
	{ok, Packed_2_6B} = ar_mine_randomx:randomx_encrypt_chunk_nif(
		State, KeyB, Chunk, ?RANDOMX_PACKING_ROUNDS_2_6,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes()),
	{ok, PackedCompositeA3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
		State, KeyA, Chunk,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
		?COMPOSITE_PACKING_ROUND_COUNT, 3, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	{ok, PackedCompositeB3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
		State, KeyB, Chunk,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
		?COMPOSITE_PACKING_ROUND_COUNT, 3, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
		
	%% spora_2_6 -> spora_2_6 randomx_reencrypt_chunk
	?assertEqual({ok, Packed_2_6B, Chunk},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{spora_2_6, AddrA}, {spora_2_6, AddrB},
			State, KeyA, KeyB, Packed_2_6A, byte_size(Chunk))),
	
	%% composite -> composite randomx_reencrypt_chunk
	?assertEqual({ok, PackedCompositeB3, Chunk},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{composite, AddrA, 2}, {composite, AddrB, 3},
			State, KeyA, KeyB, PackedCompositeA2, byte_size(Chunk))),
	?assertEqual({ok, PackedCompositeA3, none},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{composite, AddrA, 2}, {composite, AddrA, 3},
			State, KeyA, KeyA, PackedCompositeA2, byte_size(Chunk))),	
	
	%% spora_2_6 -> composite randomx_reencrypt_chunk
	?assertEqual({ok, PackedCompositeB3, Chunk},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{spora_2_6, AddrA}, {composite, AddrB, 3},
			State, KeyA, KeyB, Packed_2_6A, byte_size(Chunk))).

test_pack_unpack({FastState, _LightState}) ->
	test_pack_unpack(FastState, [], fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_pack_unpack(
		FastState, [1, 32], fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_pack_unpack(
		FastState, [2, 32], fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8).

test_pack_unpack(FastState, ExtraArgs, EncryptFun, DecryptFun) ->
	%% Add 3 0-bytes at the end to test automatic padding.
	ChunkWithoutPadding = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	Chunk = << ChunkWithoutPadding/binary, 0:24 >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed1} = EncryptFun(FastState, Key, Chunk, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed1)),
	?assertEqual({ok, Packed1},
		EncryptFun(FastState, Key, Chunk, 8, 0, 0, 0, ExtraArgs)),
	%% Run the decryption twice to test that the nif isn't corrupting any state.
	{ok, Unpacked1} = DecryptFun(FastState, Key, Packed1, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(Unpacked1, Chunk),
	?assertEqual({ok, Unpacked1}, 
		DecryptFun(FastState, Key, Packed1, 8, 0, 0, 0, ExtraArgs)),
	%% Run the encryption twice to test that the nif isn't corrupting any state.
	{ok, Packed2} = EncryptFun(FastState, Key, ChunkWithoutPadding, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(Packed2, Packed1),
	?assertEqual({ok, Packed2},
		EncryptFun(FastState, Key, ChunkWithoutPadding, 8, 0, 0, 0, ExtraArgs)).


test_repack({FastState, _LightState}) ->
	test_repack(FastState, [], [], fun encrypt_chunk/8, fun reencrypt_chunk/10),
	test_repack(
		FastState, [1, 32], [1, 1, 32, 32], 
		fun encrypt_composite_chunk/8, fun reencrypt_composite_chunk/10),
	test_repack(
		FastState, [2, 32], [2, 2, 32, 32], 
		fun encrypt_composite_chunk/8, fun reencrypt_composite_chunk/10).


test_repack(FastState, EncryptArgs, ReencryptArgs, EncryptFun, ReencryptFun) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 12),
	Key1 = crypto:strong_rand_bytes(32),
	Key2 = crypto:strong_rand_bytes(32),
	{ok, Packed1} = EncryptFun(FastState, Key1, Chunk, 8, 0, 0, 0, EncryptArgs),
	{ok, Packed2} = EncryptFun(FastState, Key2, Chunk, 8, 0, 0, 0, EncryptArgs),
	{ok, Repacked, RepackInput} =
			ReencryptFun(FastState, Key1, Key2, Packed1, 8, 8, 0, 0, 0, ReencryptArgs),
	?assertEqual(Chunk, binary:part(RepackInput, 0, byte_size(Chunk))),
	?assertEqual(Packed2, Repacked), 

	%% Reencrypt with different RandomX rounds.
	{ok, Repacked2, RepackInput2} =
			ReencryptFun(FastState, Key1, Key2, Packed1, 8, 10, 0, 0, 0, ReencryptArgs),
	?assertEqual(Chunk, binary:part(RepackInput2, 0, byte_size(Chunk))),
	?assertNotEqual(Packed2, Repacked2),

	?assertEqual({ok, Repacked2, RepackInput2},
			ReencryptFun(FastState, Key1, Key2, Packed1, 8, 10, 0, 0, 0, ReencryptArgs)). 

test_input_changes_packing({FastState, _LightState}) ->
	test_input_changes_packing(FastState, [], fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_input_changes_packing(
		FastState, [1, 32], fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	
	%% Also check arguments specific to composite packing:
	%% 
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = encrypt_composite_chunk(FastState, Key, Chunk, 8, 0, 0, 0, [1, 32]),
	%% A different iterations count.
	{ok, Packed2} = encrypt_composite_chunk(FastState, Key, Chunk, 8, 0, 0, 0, [2, 32]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed2)),
	?assertNotEqual(Packed2, Packed),

	{ok, Unpacked2} = decrypt_composite_chunk( FastState, Key, Packed, 8, 0, 0, 0, [2, 32]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked2)),
	?assertNotEqual(Unpacked2, Chunk),

	%% A different sub-chunk count.
	{ok, Packed3} = encrypt_composite_chunk(FastState, Key, Chunk, 8, 0, 0, 0, [1, 64]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed3)),
	?assertNotEqual(Packed3, Packed),

	{ok, Unpacked3} = decrypt_composite_chunk(FastState, Key, Packed, 8, 0, 0, 0, [1, 64]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked3)),
	?assertNotEqual(Unpacked3, Chunk).
	
test_input_changes_packing(FastState, ExtraArgs, EncryptFun, DecryptFun) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = EncryptFun(FastState, Key, Chunk, 8, 0, 0, 0, ExtraArgs),
	{ok, Unpacked} = DecryptFun(FastState, Key, Packed, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(Unpacked, Chunk),

	%% Pack a slightly different chunk to assert the packing is different for different data.
	<< ChunkPrefix:262143/binary, LastChunkByte:8 >> = Chunk,
	Chunk2 = << ChunkPrefix/binary, (LastChunkByte + 1):8 >>,
	{ok, Packed2} = EncryptFun(FastState, Key, Chunk2, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed2)),
	?assertNotEqual(Packed2, Packed),

	%% Unpack a slightly different chunk to assert the packing is different for different data.
	<< PackedPrefix:262143/binary, LastPackedByte:8 >> = Packed,
	Packed3 = << PackedPrefix/binary, (LastPackedByte + 1):8 >>,
	{ok, Unpacked2} = DecryptFun(FastState, Key, Packed3, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked2)),
	?assertNotEqual(Unpacked2, Chunk),

	%% Pack with a slightly different key.
	<< Prefix:31/binary, LastByte:8 >> = Key,
	Key2 = << Prefix/binary, (LastByte + 1):8 >>,
	{ok, Packed4} = EncryptFun(FastState, Key2, Chunk, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed4)),
	?assertNotEqual(Packed4, Packed),

	%% Unpack with a slightly different key.
	{ok, Unpacked3} = DecryptFun(FastState, Key2, Packed, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked3)),
	?assertNotEqual(Unpacked3, Chunk),

	%% Pack with a different RX program count.
	{ok, Packed5} = EncryptFun(FastState, Key, Chunk, 7, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed5)),
	?assertNotEqual(Packed5, Packed),

	%% Unpack with a different RX program count.
	{ok, Unpacked4} = DecryptFun(FastState, Key, Packed, 7, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked4)),
	?assertNotEqual(Unpacked4, Chunk).

%% -------------------------------------------------------------------------------------------
%% Composite packing tests
%% -------------------------------------------------------------------------------------------
test_composite_packing({FastState, _LightState}) ->
	ChunkWithoutPadding = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 5),
	Chunk = << ChunkWithoutPadding/binary, 0:(5 * 8) >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Chunk,
		0, 0, 0, 8, 1, 1),
	Key2 = crypto:hash(sha256, << Key/binary, ?DATA_CHUNK_SIZE:24 >>),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_chunk_nif(FastState, Key2, Chunk,
		8, % RANDOMX_PACKING_ROUNDS
		0, 0, 0),
	?assertEqual(Packed, Packed2),
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key,
		ChunkWithoutPadding, 0, 0, 0, 8, 1, 1),
	{ok, Packed4} = ar_mine_randomx:randomx_encrypt_chunk_nif(FastState, Key2, ChunkWithoutPadding,
		8, % RANDOMX_PACKING_ROUNDS
		0, 0, 0),
	?assertEqual(Packed3, Packed4).

test_composite_packs_incrementally({FastState, _LightState}) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed1} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Packed1,
		0, 0, 0, 8, 1, 32),
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Chunk,
		0, 0, 0, 8, 2, 32),
	?assertEqual(Packed2, Packed3),
	{ok, Packed4} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Chunk,
		0, 0, 0, 8, 3, 32),
	{ok, Packed5} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Packed1,
		0, 0, 0, 8, 2, 32),
	{ok, Packed6} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Packed2,
		0, 0, 0, 8, 1, 32),
	?assertEqual(Packed4, Packed5),
	?assertEqual(Packed4, Packed6).

test_composite_unpacked_sub_chunks({FastState, _LightState}) ->
	ChunkWithoutPadding = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	Chunk = << ChunkWithoutPadding/binary, 0:24 >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	SubChunks = split_chunk_into_sub_chunks(Packed, ?DATA_CHUNK_SIZE div 32, 0),
	UnpackedInSubChunks = iolist_to_binary(lists:reverse(lists:foldl(
		fun({SubChunk, Offset}, Acc) ->
			{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_composite_sub_chunk_nif(FastState,
					Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 1, Offset),
			[Unpacked | Acc]
		end,
		[],
		SubChunks
	))),
	?assertEqual(UnpackedInSubChunks, Chunk),
	Chunk2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key, Chunk2,
		0, 0, 0, 8, 3, 32),
	SubChunks2 = split_chunk_into_sub_chunks(Packed2, ?DATA_CHUNK_SIZE div 32, 0),
	UnpackedInSubChunks2 = iolist_to_binary(lists:reverse(lists:foldl(
		fun({SubChunk, Offset}, Acc) ->
			{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_composite_sub_chunk_nif(FastState,
				Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 3, Offset),
			[Unpacked | Acc]
		end,
		[],
		SubChunks2
	))),
	?assertEqual(UnpackedInSubChunks2, << Chunk2/binary, 0:24 >>).

split_chunk_into_sub_chunks(Bin, Size, Offset) ->
	case Bin of
		<<>> ->
			[];
		<< SubChunk:Size/binary, Rest/binary >> ->
			[{SubChunk, Offset} | split_chunk_into_sub_chunks(Rest, Size, Offset + Size)]
	end.

test_composite_repack({FastState, _LightState}) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 12),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key,
			Chunk, 0, 0, 0, 8, 2, 32),
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key,
			Chunk, 0, 0, 0, 8, 3, 32),
	{ok, Repacked_2_3, RepackInput} =
			ar_mine_randomx:randomx_reencrypt_composite_chunk_nif(FastState,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 3, 32, 32),
	?assertEqual(Packed2, RepackInput),
	?assertEqual(Packed3, Repacked_2_3),
	
	%% Repacking a composite chunk to same-key higher-diff composite chunk...
	{ok, Repacked_2_5, RepackInput} =
			ar_mine_randomx:randomx_reencrypt_composite_chunk_nif(FastState,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 5, 32, 32),
	{ok, Packed5} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key,
			Chunk, 0, 0, 0, 8, 5, 32),
	?assertEqual(Packed5, Repacked_2_5),
	Key2 = crypto:strong_rand_bytes(32),

	%% Repacking a composite chunk to different-key higher-diff composite chunk...
	{ok, RepackedDiffKey_2_3, RepackInput2} =
			ar_mine_randomx:randomx_reencrypt_composite_chunk_nif(FastState,
					Key, Key2, Packed2, 0, 0, 0, 8, 8, 2, 3, 32, 32),
	?assertEqual(<< Chunk/binary, 0:(12 * 8) >>, RepackInput2),
	{ok, Packed2_3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(FastState, Key2,
			Chunk, 0, 0, 0, 8, 3, 32),
	?assertNotEqual(Packed2, Packed2_3),
	?assertEqual(Packed2_3, RepackedDiffKey_2_3),
	try
		ar_mine_randomx:randomx_reencrypt_composite_chunk_nif(FastState,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 2, 32, 32),
		?assert(false, "randomx_reencrypt_composite_chunk_nif to reencrypt "
				"to same diff should have failed")
	catch error:badarg ->
		ok
	end,
	try
		ar_mine_randomx:randomx_reencrypt_composite_chunk_nif(FastState,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 1, 32, 32),
		?assert(false, "randomx_reencrypt_composite_chunk_nif to reencrypt "
				"to lower diff should have failed")
	catch error:badarg ->
		ok
	end.

test_hash({FastState, LightState}) ->
    ExpectedHash = ar_util:decode(?ENCODED_HASH),
    Nonce = ar_util:decode(?ENCODED_NONCE),
    Segment = ar_util:decode(?ENCODED_SEGMENT),
    Input = << Nonce/binary, Segment/binary >>,
	?assertEqual({ok, ExpectedHash},
			ar_mine_randomx:hash_nif(FastState, Input, 0, 0, 0)),
    ?assertEqual({ok, ExpectedHash},
			ar_mine_randomx:hash_nif(LightState, Input, 0, 0, 0)).
