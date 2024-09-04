-module(ar_mine_randomx_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-define(ENCODED_RX512_HASH, <<"NcXUtn7gA42QoM8MtaS-vgVy8gJ21EE2YxV18mHndmM">>).
-define(ENCODED_RX4096_HASH, <<"HqbpuoVNu8u4l4slkwnP3fvX9Q-mgjFH-3LgCyhMPPk">>).
-define(ENCODED_NONCE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ENCODED_SEGMENT,
    <<"7XM3fgTCAY2GFpDjPZxlw4yw5cv8jNzZSZawywZGQ6_Ca-JDy2nX_MC2vjrIoDGp">>
).

encrypt_chunk({rx512, RandomXState}, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES, _ExtraArgs) ->
	ar_rx512_nif:rx512_encrypt_chunk_nif(
		RandomXState, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES).

decrypt_chunk({rx512, RandomXState}, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES, _ExtraArgs) ->
	ar_rx512_nif:rx512_decrypt_chunk_nif(
		RandomXState, Key, Chunk, byte_size(Chunk), PackingRounds, JIT, LargePages, HardwareAES).

reencrypt_chunk({rx512, RandomXState}, Key1, Key2, Chunk, PackingRounds1, PackingRounds2,
		JIT, LargePages, HardwareAES, _ExtraArgs) ->
	ar_rx512_nif:rx512_reencrypt_chunk_nif(
		RandomXState, Key1, Key2, Chunk, byte_size(Chunk), PackingRounds1, PackingRounds2,
		JIT, LargePages, HardwareAES).

encrypt_composite_chunk({rx4096, RandomXState}, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES,
		[IterationCount, SubChunkCount] = _ExtraArgs) ->
	ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState, Key, Chunk, JIT, LargePages, HardwareAES, PackingRounds, 
		IterationCount, SubChunkCount).

decrypt_composite_chunk({rx4096, RandomXState}, Key, Chunk, PackingRounds, JIT, LargePages, HardwareAES,
		[IterationCount, SubChunkCount] = _ExtraArgs) ->
	ar_rx4096_nif:rx4096_decrypt_composite_chunk_nif(
		RandomXState, Key, Chunk, byte_size(Chunk), JIT, LargePages, HardwareAES,
		PackingRounds, IterationCount, SubChunkCount).

reencrypt_composite_chunk({rx4096, RandomXState}, Key1, Key2, Chunk, PackingRounds1, PackingRounds2,
		JIT, LargePages, HardwareAES, 
		[IterationCount1, IterationCount2, SubChunkCount1, SubChunkCount2] = _ExtraArgs) ->
	ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif(
		RandomXState, Key1, Key2, Chunk, JIT, LargePages, HardwareAES,
		PackingRounds1, PackingRounds2, IterationCount1, IterationCount2,
		SubChunkCount1, SubChunkCount2).

setup() ->
    FastState512 = ar_mine_randomx:init_fast2(rx512, ?RANDOMX_PACKING_KEY, 0, 0,
		erlang:system_info(dirty_cpu_schedulers_online)),
    LightState512 = ar_mine_randomx:init_light2(rx512, ?RANDOMX_PACKING_KEY, 0, 0),
    FastState4096 = ar_mine_randomx:init_fast2(rx4096, ?RANDOMX_PACKING_KEY, 0, 0,
		erlang:system_info(dirty_cpu_schedulers_online)),
    LightState4096 = ar_mine_randomx:init_light2(rx4096, ?RANDOMX_PACKING_KEY, 0, 0),
    {FastState512, LightState512, FastState4096, LightState4096}.

test_register(TestFun, Fixture) ->
	{timeout, 120, {with, Fixture, [TestFun]}}.

randomx_suite_test_() ->
	{setup, fun setup/0,
		fun (SetupData) ->
			[
				test_register(fun test_state/1, SetupData),
				test_register(fun test_bad_state/1, SetupData),
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
test_state({FastState512, LightState512, FastState4096, LightState4096}) ->
	%% The legacy dataset size is 568,433,920 bytes. Roughly 30 MiB more than 512 MiB.
	%% Our nifs don't have access to the raw dataset size used in the RandomX C code, but
	%% they have access to the dataset item count - which is just the size divided by 64.
	%% So the expected dataset size is 568,433,920 / 64 = 8,881,780 items.
	%% 
	%% The new dataset size is 4,326,530,304 bytes. Roughly 30 MiB more than 4 GiB.
	%% So the expected dataset size is 4,326,530,304 / 64 = 67,602,036 items.
	?assertEqual(
		{ok, {rx512, fast, 8881780}},
		ar_mine_randomx:info(FastState512)
	),
	?assertEqual(
		{ok, {rx4096, fast, 67602036}},
		ar_mine_randomx:info(FastState4096)
	),
	%% Unfortunately we don't have access to the cache size. The randomx_info_nif will check
	%% that in fast mode the cache is not initialized, and in light mode the dataset is not
	%% initialized and return an error if either check fails.
	?assertEqual(
		{ok, {rx512, light, 0}},
		ar_mine_randomx:info(LightState512)
	),
	?assertEqual(
		{ok, {rx4096, light, 0}},
		ar_mine_randomx:info(LightState4096)
	).

test_bad_state(_) ->
	BadState = {bad_mode, bad_state},
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:info(BadState)),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:hash(BadState, crypto:strong_rand_bytes(32))),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:randomx_encrypt_chunk(
			{spora_2_6, crypto:strong_rand_bytes(32)}, BadState,
			crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(?DATA_CHUNK_SIZE))),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:randomx_encrypt_chunk(
			{composite, 2, crypto:strong_rand_bytes(32)}, BadState,
			crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(?DATA_CHUNK_SIZE))),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:randomx_decrypt_chunk(
			{spora_2_6, crypto:strong_rand_bytes(32)}, BadState,
			crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
			?DATA_CHUNK_SIZE)),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:randomx_decrypt_chunk(
			{composite, 2, crypto:strong_rand_bytes(32)}, BadState,
			crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
			?DATA_CHUNK_SIZE)),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:randomx_decrypt_sub_chunk(
			{composite, 2, crypto:strong_rand_bytes(32)}, BadState,
			crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),0)),
	?assertEqual({exception, invalid_randomx_mode},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{spora_2_6, crypto:strong_rand_bytes(32)}, {spora_2_6, crypto:strong_rand_bytes(32)},
			BadState, crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(32), 
			crypto:strong_rand_bytes(?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE)),
	?assertEqual({exception, invalid_reencrypt_packing},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{composite, 2, crypto:strong_rand_bytes(32)},
			{composite, 2, crypto:strong_rand_bytes(32)},
			BadState, crypto:strong_rand_bytes(32), crypto:strong_rand_bytes(32), 
			crypto:strong_rand_bytes(?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE)).

test_regression({FastState512, LightState512, FastState4096, LightState4096}) ->
	%% Test all permutations of:
	%% 1. Light vs. fast state
	%% 2. spora_2_6 vs. depth-1 composite vs. depth-2 composite packing
	%% 3. JIT vs. no JIT
	%% 4. RandomX dataset size 512 vs. 4096
	%%   (this is handled implicitly by the legacy packing vs. composite packing)

	test_regression(FastState512,
		"ar_mine_randomx_tests/packed.spora26.bin", 0, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(FastState512,
		"ar_mine_randomx_tests/packed.spora26.bin", 1, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(FastState4096,
		"ar_mine_randomx_tests/packed.composite.1.bin", 0, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(FastState4096,
		"ar_mine_randomx_tests/packed.composite.1.bin", 1, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(FastState4096,
		"ar_mine_randomx_tests/packed.composite.2.bin", 0, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(FastState4096,
		"ar_mine_randomx_tests/packed.composite.2.bin", 1, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState512,
		"ar_mine_randomx_tests/packed.spora26.bin", 0, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(LightState512,
		"ar_mine_randomx_tests/packed.spora26.bin", 1, [],
		fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_regression(LightState4096,
		"ar_mine_randomx_tests/packed.composite.1.bin", 0, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState4096,
		"ar_mine_randomx_tests/packed.composite.1.bin", 1, [1, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState4096,
		"ar_mine_randomx_tests/packed.composite.2.bin", 0, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_regression(LightState4096,
		"ar_mine_randomx_tests/packed.composite.2.bin", 1, [2, 32],
		fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8).

test_regression(State, Fixture, JIT, ExtraArgs, EncryptFun, DecryptFun) ->
	Key = ar_test_node:load_fixture("ar_mine_randomx_tests/key.bin"),
	UnpackedFixture = ar_test_node:load_fixture("ar_mine_randomx_tests/unpacked.bin"),
	PackedFixture = ar_test_node:load_fixture(Fixture),

	{ok, Packed} = EncryptFun(State, Key, UnpackedFixture, 8, JIT, 0, 0, ExtraArgs),
	?assertEqual(PackedFixture, Packed, Fixture),

	{ok, Unpacked} = DecryptFun(State, Key, PackedFixture, 8, JIT, 0, 0, ExtraArgs),
	?assertEqual(UnpackedFixture, Unpacked, Fixture).

test_empty_chunk_fails({FastState512, _LightState512, FastState4096, _LightState4096}) ->
	test_empty_chunk_fails(FastState512, [], fun encrypt_chunk/8),
	test_empty_chunk_fails(FastState4096, [1, 32], fun encrypt_composite_chunk/8),
	test_empty_chunk_fails(FastState512, [], fun decrypt_chunk/8),
	test_empty_chunk_fails(FastState4096, [1, 32], fun decrypt_composite_chunk/8).

test_empty_chunk_fails(State, ExtraArgs, Fun) ->
	try
		Fun(State, crypto:strong_rand_bytes(32), <<>>, 1, 0, 0, 0, ExtraArgs),
		?assert(false, "Encrypt/Decrypt with an empty chunk should have failed")
	catch error:badarg ->
		ok
	end.

test_nif_wrappers({FastState512, _LightState512, FastState4096, _LightState4096}) ->
	test_nif_wrappers(FastState512, FastState4096,
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 12)),
	test_nif_wrappers(FastState512, FastState4096,
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)).

test_nif_wrappers(State512, State4096, Chunk) ->
	AddrA = crypto:strong_rand_bytes(32),
	AddrB = crypto:strong_rand_bytes(32),
	KeyA = crypto:strong_rand_bytes(32),
	KeyB= crypto:strong_rand_bytes(32),
	%% spora_26 randomx_encrypt_chunk 
	{ok, Packed_2_6A} = ar_rx512_nif:rx512_encrypt_chunk_nif(
		element(2, State512), KeyA, Chunk, ?RANDOMX_PACKING_ROUNDS_2_6,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes()),
	?assertEqual({ok, Packed_2_6A},
		ar_mine_randomx:randomx_encrypt_chunk({spora_2_6, AddrA}, State512, KeyA, Chunk)),

	%% composite randomx_encrypt_composite_chunk
	{ok, PackedCompositeA2} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		element(2, State4096), KeyA, Chunk,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
		?COMPOSITE_PACKING_ROUND_COUNT, 2, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	?assertEqual({ok, PackedCompositeA2},
		ar_mine_randomx:randomx_encrypt_chunk({composite, AddrA, 2}, State4096, KeyA, Chunk)),

	%% spora_2_6 randomx_decrypt_chunk
	?assertEqual({ok, Chunk},
		ar_mine_randomx:randomx_decrypt_chunk(
			{spora_2_6, AddrA}, State512, KeyA, Packed_2_6A, byte_size(Chunk))),

	%% composite randomx_decrypt_composite_chunk
	?assertEqual({ok, Chunk},
		ar_mine_randomx:randomx_decrypt_chunk(
			{composite, AddrA, 2}, State4096, KeyA, PackedCompositeA2, byte_size(Chunk))),

	%% Prepare data for the reencryption tests
	{ok, Packed_2_6B} = ar_rx512_nif:rx512_encrypt_chunk_nif(
		element(2, State512), KeyB, Chunk, ?RANDOMX_PACKING_ROUNDS_2_6,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes()),
	{ok, PackedCompositeA3} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		element(2, State4096), KeyA, Chunk,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
		?COMPOSITE_PACKING_ROUND_COUNT, 3, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	{ok, PackedCompositeB3} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		element(2, State4096), KeyB, Chunk,
		ar_mine_randomx:jit(), ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes(),
		?COMPOSITE_PACKING_ROUND_COUNT, 3, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
		
	%% spora_2_6 -> spora_2_6 randomx_reencrypt_chunk
	?assertEqual({ok, Packed_2_6B, Chunk},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{spora_2_6, AddrA}, {spora_2_6, AddrB},
			State512, KeyA, KeyB, Packed_2_6A, byte_size(Chunk))),
	
	%% composite -> composite randomx_reencrypt_chunk
	?assertEqual({ok, PackedCompositeB3, Chunk},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{composite, AddrA, 2}, {composite, AddrB, 3},
			State4096, KeyA, KeyB, PackedCompositeA2, byte_size(Chunk))),
	?assertEqual({ok, PackedCompositeA3, none},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{composite, AddrA, 2}, {composite, AddrA, 3},
			State4096, KeyA, KeyA, PackedCompositeA2, byte_size(Chunk))),	
	
	%% spora_2_6 -> composite randomx_reencrypt_chunk
	?assertEqual({exception, invalid_reencrypt_packing},
		ar_mine_randomx:randomx_reencrypt_chunk(
			{spora_2_6, AddrA}, {composite, AddrB, 3},
			State512, KeyA, KeyB, Packed_2_6A, byte_size(Chunk))).

test_pack_unpack({FastState512, _LightState512, FastState4096, _LightState4096}) ->
	test_pack_unpack(FastState512, [], fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_pack_unpack(
		FastState4096, [1, 32], fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	test_pack_unpack(
		FastState4096, [2, 32], fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8).

test_pack_unpack(State, ExtraArgs, EncryptFun, DecryptFun) ->
	%% Add 3 0-bytes at the end to test automatic padding.
	ChunkWithoutPadding = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	Chunk = << ChunkWithoutPadding/binary, 0:24 >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed1} = EncryptFun(State, Key, Chunk, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed1)),
	?assertEqual({ok, Packed1},
		EncryptFun(State, Key, Chunk, 8, 0, 0, 0, ExtraArgs)),
	%% Run the decryption twice to test that the nif isn't corrupting any state.
	{ok, Unpacked1} = DecryptFun(State, Key, Packed1, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(Unpacked1, Chunk),
	?assertEqual({ok, Unpacked1}, 
		DecryptFun(State, Key, Packed1, 8, 0, 0, 0, ExtraArgs)),
	%% Run the encryption twice to test that the nif isn't corrupting any state.
	{ok, Packed2} = EncryptFun(State, Key, ChunkWithoutPadding, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(Packed2, Packed1),
	?assertEqual({ok, Packed2},
		EncryptFun(State, Key, ChunkWithoutPadding, 8, 0, 0, 0, ExtraArgs)).


test_repack({FastState512, _LightState512, FastState4096, _LightState4096}) ->
	test_repack(FastState512, [], [], fun encrypt_chunk/8, fun reencrypt_chunk/10),
	test_repack(
		FastState4096, [1, 32], [1, 1, 32, 32], 
		fun encrypt_composite_chunk/8, fun reencrypt_composite_chunk/10),
	test_repack(
		FastState4096, [2, 32], [2, 2, 32, 32], 
		fun encrypt_composite_chunk/8, fun reencrypt_composite_chunk/10).


test_repack(State, EncryptArgs, ReencryptArgs, EncryptFun, ReencryptFun) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 12),
	Key1 = crypto:strong_rand_bytes(32),
	Key2 = crypto:strong_rand_bytes(32),
	{ok, Packed1} = EncryptFun(State, Key1, Chunk, 8, 0, 0, 0, EncryptArgs),
	{ok, Packed2} = EncryptFun(State, Key2, Chunk, 8, 0, 0, 0, EncryptArgs),
	{ok, Repacked, RepackInput} =
			ReencryptFun(State, Key1, Key2, Packed1, 8, 8, 0, 0, 0, ReencryptArgs),
	?assertEqual(Chunk, binary:part(RepackInput, 0, byte_size(Chunk))),
	?assertEqual(Packed2, Repacked), 

	%% Reencrypt with different RandomX rounds.
	{ok, Repacked2, RepackInput2} =
			ReencryptFun(State, Key1, Key2, Packed1, 8, 10, 0, 0, 0, ReencryptArgs),
	?assertEqual(Chunk, binary:part(RepackInput2, 0, byte_size(Chunk))),
	?assertNotEqual(Packed2, Repacked2),

	?assertEqual({ok, Repacked2, RepackInput2},
			ReencryptFun(State, Key1, Key2, Packed1, 8, 10, 0, 0, 0, ReencryptArgs)). 

test_input_changes_packing({FastState512, _LightState512, FastState4096, _LightState4096}) ->
	test_input_changes_packing(FastState512, [], fun encrypt_chunk/8, fun decrypt_chunk/8),
	test_input_changes_packing(
		FastState4096, [1, 32], fun encrypt_composite_chunk/8, fun decrypt_composite_chunk/8),
	
	%% Also check arguments specific to composite packing:
	%% 
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = encrypt_composite_chunk(FastState4096, Key, Chunk, 8, 0, 0, 0, [1, 32]),
	%% A different iterations count.
	{ok, Packed2} = encrypt_composite_chunk(FastState4096, Key, Chunk, 8, 0, 0, 0, [2, 32]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed2)),
	?assertNotEqual(Packed2, Packed),

	{ok, Unpacked2} = decrypt_composite_chunk( FastState4096, Key, Packed, 8, 0, 0, 0, [2, 32]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked2)),
	?assertNotEqual(Unpacked2, Chunk),

	%% A different sub-chunk count.
	{ok, Packed3} = encrypt_composite_chunk(FastState4096, Key, Chunk, 8, 0, 0, 0, [1, 64]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed3)),
	?assertNotEqual(Packed3, Packed),

	{ok, Unpacked3} = decrypt_composite_chunk(FastState4096, Key, Packed, 8, 0, 0, 0, [1, 64]),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked3)),
	?assertNotEqual(Unpacked3, Chunk).
	
test_input_changes_packing(State, ExtraArgs, EncryptFun, DecryptFun) ->
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = EncryptFun(State, Key, Chunk, 8, 0, 0, 0, ExtraArgs),
	{ok, Unpacked} = DecryptFun(State, Key, Packed, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(Unpacked, Chunk),

	%% Pack a slightly different chunk to assert the packing is different for different data.
	<< ChunkPrefix:262143/binary, LastChunkByte:8 >> = Chunk,
	Chunk2 = << ChunkPrefix/binary, (LastChunkByte + 1):8 >>,
	{ok, Packed2} = EncryptFun(State, Key, Chunk2, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed2)),
	?assertNotEqual(Packed2, Packed),

	%% Unpack a slightly different chunk to assert the packing is different for different data.
	<< PackedPrefix:262143/binary, LastPackedByte:8 >> = Packed,
	Packed3 = << PackedPrefix/binary, (LastPackedByte + 1):8 >>,
	{ok, Unpacked2} = DecryptFun(State, Key, Packed3, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked2)),
	?assertNotEqual(Unpacked2, Chunk),

	%% Pack with a slightly different key.
	<< Prefix:31/binary, LastByte:8 >> = Key,
	Key2 = << Prefix/binary, (LastByte + 1):8 >>,
	{ok, Packed4} = EncryptFun(State, Key2, Chunk, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed4)),
	?assertNotEqual(Packed4, Packed),

	%% Unpack with a slightly different key.
	{ok, Unpacked3} = DecryptFun(State, Key2, Packed, 8, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked3)),
	?assertNotEqual(Unpacked3, Chunk),

	%% Pack with a different RX program count.
	{ok, Packed5} = EncryptFun(State, Key, Chunk, 7, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed5)),
	?assertNotEqual(Packed5, Packed),

	%% Unpack with a different RX program count.
	{ok, Unpacked4} = DecryptFun(State, Key, Packed, 7, 0, 0, 0, ExtraArgs),
	?assertEqual(?DATA_CHUNK_SIZE, byte_size(Unpacked4)),
	?assertNotEqual(Unpacked4, Chunk).

%% -------------------------------------------------------------------------------------------
%% Composite packing tests
%% -------------------------------------------------------------------------------------------
test_composite_packing({FastState512, _LightState512, FastState4096, _LightState4096}) ->
	{rx512, RandomXState512} = FastState512,
	{rx4096, RandomXState4096} = FastState4096,
	ChunkWithoutPadding = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 5),
	Chunk = << ChunkWithoutPadding/binary, 0:(5 * 8) >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(RandomXState4096, Key, Chunk,
		0, 0, 0, 8, 1, 1),
	Key2 = crypto:hash(sha256, << Key/binary, ?DATA_CHUNK_SIZE:24 >>),
	{ok, Packed2} = ar_rx512_nif:rx512_encrypt_chunk_nif(RandomXState512, Key2, Chunk,
		8, % RANDOMX_PACKING_ROUNDS
		0, 0, 0),
	?assertNotEqual(Packed, Packed2),
	{ok, Packed3} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(RandomXState4096, Key,
		ChunkWithoutPadding, 0, 0, 0, 8, 1, 1),
	{ok, Packed4} = ar_rx512_nif:rx512_encrypt_chunk_nif(RandomXState512, Key2, ChunkWithoutPadding,
		8, % RANDOMX_PACKING_ROUNDS
		0, 0, 0),
	?assertNotEqual(Packed3, Packed4).

test_composite_packs_incrementally(
		{_FastState512, _LightState512, FastState4096, _LightState4096}) ->
	{rx4096, RandomXState4096} = FastState4096,
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed1} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Chunk, 0, 0, 0, 8, 1, 32),
	{ok, Packed2} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Packed1, 0, 0, 0, 8, 1, 32),
	{ok, Packed3} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Chunk, 0, 0, 0, 8, 2, 32),
	?assertEqual(Packed2, Packed3),
	{ok, Packed4} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Chunk, 0, 0, 0, 8, 3, 32),
	{ok, Packed5} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Packed1, 0, 0, 0, 8, 2, 32),
	{ok, Packed6} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Packed2, 0, 0, 0, 8, 1, 32),
	?assertEqual(Packed4, Packed5),
	?assertEqual(Packed4, Packed6).

test_composite_unpacked_sub_chunks(
		{_FastState512, _LightState512, FastState4096, _LightState4096}) ->
	{rx4096, RandomXState4096} = FastState4096,
	ChunkWithoutPadding = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	Chunk = << ChunkWithoutPadding/binary, 0:24 >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Chunk, 0, 0, 0, 8, 1, 32),
	SubChunks = split_chunk_into_sub_chunks(Packed, ?DATA_CHUNK_SIZE div 32, 0),
	UnpackedInSubChunks = iolist_to_binary(lists:reverse(lists:foldl(
		fun({SubChunk, Offset}, Acc) ->
			{ok, Unpacked} = ar_rx4096_nif:rx4096_decrypt_composite_sub_chunk_nif(
				RandomXState4096, Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 1, Offset),
			{ok, Unpacked2} = ar_rx4096_nif:rx4096_decrypt_composite_sub_chunk_nif(
				RandomXState4096, Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 1, Offset),
			?assertEqual(Unpacked, Unpacked2),
			[Unpacked | Acc]
		end,
		[],
		SubChunks
	))),
	?assertEqual(UnpackedInSubChunks, Chunk),
	Chunk2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 3),
	{ok, Packed2} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState4096, Key, Chunk2, 0, 0, 0, 8, 3, 32),
	SubChunks2 = split_chunk_into_sub_chunks(Packed2, ?DATA_CHUNK_SIZE div 32, 0),
	UnpackedInSubChunks2 = iolist_to_binary(lists:reverse(lists:foldl(
		fun({SubChunk, Offset}, Acc) ->
			{ok, Unpacked} = ar_rx4096_nif:rx4096_decrypt_composite_sub_chunk_nif(
				RandomXState4096, Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 3, Offset),
			{ok, Unpacked2} = ar_rx4096_nif:rx4096_decrypt_composite_sub_chunk_nif(
				RandomXState4096, Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 3, Offset),
			?assertEqual(Unpacked, Unpacked2),
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

test_composite_repack({_FastState512, _LightState512, FastState4096, _LightState4096}) ->
	{rx4096, RandomXState4096} = FastState4096,
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE - 12),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed2} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(RandomXState4096, Key,
			Chunk, 0, 0, 0, 8, 2, 32),
	{ok, Packed3} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(RandomXState4096, Key,
			Chunk, 0, 0, 0, 8, 3, 32),
	{ok, Repacked_2_3, RepackInput} =
			ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif(RandomXState4096,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 3, 32, 32),
	?assertEqual(Packed2, RepackInput),
	?assertEqual(Packed3, Repacked_2_3),
	
	%% Repacking a composite chunk to same-key higher-diff composite chunk...
	{ok, Repacked_2_5, RepackInput} =
			ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif(RandomXState4096,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 5, 32, 32),
	{ok, Packed5} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(RandomXState4096, Key,
			Chunk, 0, 0, 0, 8, 5, 32),
	?assertEqual(Packed5, Repacked_2_5),
	Key2 = crypto:strong_rand_bytes(32),

	%% Repacking a composite chunk to different-key higher-diff composite chunk...
	{ok, RepackedDiffKey_2_3, RepackInput2} =
			ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif(RandomXState4096,
					Key, Key2, Packed2, 0, 0, 0, 8, 8, 2, 3, 32, 32),
	?assertEqual(<< Chunk/binary, 0:(12 * 8) >>, RepackInput2),
	{ok, Packed2_3} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(RandomXState4096, Key2,
			Chunk, 0, 0, 0, 8, 3, 32),
	?assertNotEqual(Packed2, Packed2_3),
	?assertEqual(Packed2_3, RepackedDiffKey_2_3),
	try
		ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif(RandomXState4096,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 2, 32, 32),
		?assert(false, "ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif to reencrypt "
				"to same diff should have failed")
	catch error:badarg ->
		ok
	end,
	try
		ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif(RandomXState4096,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 1, 32, 32),
		?assert(false, "ar_rx4096_nif:rx4096_reencrypt_composite_chunk_nif to reencrypt "
				"to lower diff should have failed")
	catch error:badarg ->
		ok
	end.

test_hash({FastState512, LightState512, FastState4096, LightState4096}) ->
    ExpectedHash512 = ar_util:decode(?ENCODED_RX512_HASH),
	ExpectedHash4096 = ar_util:decode(?ENCODED_RX4096_HASH),
    Nonce = ar_util:decode(?ENCODED_NONCE),
    Segment = ar_util:decode(?ENCODED_SEGMENT),
    Input = << Nonce/binary, Segment/binary >>,
	?assertEqual(ExpectedHash512,
		ar_mine_randomx:hash(FastState512, Input, 0, 0, 0)),
	?assertEqual(ExpectedHash512,
		ar_mine_randomx:hash(LightState512, Input, 0, 0, 0)),
	?assertEqual(ExpectedHash4096,
		ar_mine_randomx:hash(FastState4096, Input, 0, 0, 0)),
	?assertEqual(ExpectedHash4096,
		ar_mine_randomx:hash(LightState4096, Input, 0, 0, 0)).