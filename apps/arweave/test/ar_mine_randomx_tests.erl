-module(ar_mine_randomx_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-define(ENCODED_KEY, <<"UbkeSd5Det8s6uLyuNJwCDFOZMQFa2zvsdKJ0k694LM">>).
-define(ENCODED_HASH, <<"QQYWA46qnFENL4OTQdGU8bWBj5OKZ2OOPyynY3izung">>).
-define(ENCODED_NONCE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ENCODED_SEGMENT,
    <<"7XM3fgTCAY2GFpDjPZxlw4yw5cv8jNzZSZawywZGQ6_Ca-JDy2nX_MC2vjrIoDGp">>
).

setup() ->
    Key = ar_util:decode(?ENCODED_KEY),
    {ok, State} = ar_mine_randomx:init_fast_nif(Key, 0, 0, 4),
    {Key, State}.

test_register(TestFun, Fixture) ->
	{timeout, 60, {with, Fixture, [TestFun]}}.

randomx_suite_test_() ->
	{setup, fun setup/0,
		fun (SetupData) ->
			[
				test_register(fun test_randomx_pack_unpack_composite_chunk/1, SetupData),
				test_register(fun test_randomx_backwards_compatibility/1, SetupData),
				test_register(fun test_randomx_pack_unpack/1, SetupData),
				test_register(fun test_randomx_long/1, SetupData)
			]
		end
	}.

test_randomx_pack_unpack_composite_chunk({_Key, State}) ->
	composite_chunk_assert_fails_on_empty_chunk(State),
	composite_chunk_assert_packs(State),
	composite_chunk_assert_unpacks_packed(State),
	composite_chunk_assert_different_input_leads_to_different_packing(State),
	composite_chunk_assert_packs_incrementally(State),
	composite_chunk_assert_unpacks_sub_chunks(State),
	composite_chunk_assert_repacks_from_spora_2_6(State),
	composite_chunk_assert_repacks(State).

composite_chunk_assert_fails_on_empty_chunk(State) ->
	try
		ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
			State,
			crypto:strong_rand_bytes(32),
			<<>>,
			0, 0, 0,
			1, % RANDOMX_PACKING_ROUNDS
			1, % iterations
			32 % sub-chunk count
		 ),
		?assert(false, "randomx_encrypt_composite_chunk_nif with an "
				"empty chunk should have failed")
	catch error:badarg ->
		ok
	end.

composite_chunk_assert_packs(State) ->
	?debugFmt("Asserting composite packing works...", []),
	ChunkWithoutPadding = crypto:strong_rand_bytes(262144 - 5),
	Chunk = << ChunkWithoutPadding/binary, 0:(5 * 8) >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 1),
	Key2 = crypto:hash(sha256, << Key/binary, 262144:24 >>),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key2, Chunk,
		8, % RANDOMX_PACKING_ROUNDS
		0, 0, 0),
	?assertEqual(Packed, Packed2),
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
		ChunkWithoutPadding, 0, 0, 0, 8, 1, 1),
	{ok, Packed4} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key2, ChunkWithoutPadding,
		8, % RANDOMX_PACKING_ROUNDS
		0, 0, 0),
	?assertEqual(Packed3, Packed4).

composite_chunk_assert_unpacks_packed(State) ->
	?debugFmt("Asserting composite packing and unpacking works...", []),
	%% Add 3 0-bytes at the end to test automatic padding.
	ChunkWithoutPadding = crypto:strong_rand_bytes(262144 - 3),
	Chunk = << ChunkWithoutPadding/binary, 0:24 >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	?assertEqual(262144, byte_size(Packed)),
	{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_composite_chunk_nif(State, Key,
			Packed, byte_size(Packed), 0, 0, 0, 8, 1, 32),
	?assertEqual(Unpacked, Chunk),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
		ChunkWithoutPadding,
		0, 0, 0, 8, 1, 32),
	?assertEqual(Packed2, Packed).

composite_chunk_assert_different_input_leads_to_different_packing(State) ->
	?debugFmt("Asserting composite packing is sensitive to input...", []),
	Chunk = crypto:strong_rand_bytes(262144),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	?assertEqual(262144, byte_size(Packed)),
	{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_composite_chunk_nif(State, Key,
			Packed, byte_size(Packed), 0, 0, 0, 8, 1, 32),
	?assertEqual(Unpacked, Chunk),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	?assertEqual(Packed2, Packed),
	%% Pack a slightly different chunk to assert the packing is different for different data.
	<< ChunkPrefix:262143/binary, LastChunkByte:8 >> = Chunk,
	Chunk2 = << ChunkPrefix/binary, (LastChunkByte + 1):8 >>,
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk2,
		0, 0, 0, 8, 1, 32),
	?assertEqual(262144, byte_size(Packed3)),
	?assertNotEqual(Packed3, Packed),
	%% Pack with a slightly different key.
	<< Prefix:31/binary, LastByte:8 >> = Key,
	Key2 = << Prefix/binary, (LastByte + 1):8 >>,
	{ok, Packed4} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key2, Chunk,
		0, 0, 0, 8, 1, 32),
	?assertEqual(262144, byte_size(Packed4)),
	?assertNotEqual(Packed4, Packed),
	%% A different RX program count.
	{ok, Packed5} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 7, 1, 32),
	?assertEqual(262144, byte_size(Packed5)),
	?assertNotEqual(Packed5, Packed),
	%% A different iterations count.
	{ok, Packed6} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 2, 32),
	?assertEqual(262144, byte_size(Packed6)),
	?assertNotEqual(Packed6, Packed),
	%% A different sub-chunk count.
	{ok, Packed7} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 64),
	?assertEqual(262144, byte_size(Packed7)),
	?assertNotEqual(Packed7, Packed).

composite_chunk_assert_packs_incrementally(State) ->
	?debugFmt("Asserting incremental packing works...", []),
	Chunk = crypto:strong_rand_bytes(262144 - 3),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed1} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Packed1,
		0, 0, 0, 8, 1, 32),
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 2, 32),
	?assertEqual(Packed2, Packed3),
	{ok, Packed4} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 3, 32),
	{ok, Packed5} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Packed1,
		0, 0, 0, 8, 2, 32),
	{ok, Packed6} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Packed2,
		0, 0, 0, 8, 1, 32),
	?assertEqual(Packed4, Packed5),
	?assertEqual(Packed4, Packed6).

composite_chunk_assert_unpacks_sub_chunks(State) ->
	?debugFmt("Asserting sub-chunk unpacking works...", []),
	ChunkWithoutPadding = crypto:strong_rand_bytes(262144 - 3),
	Chunk = << ChunkWithoutPadding/binary, 0:24 >>,
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk,
		0, 0, 0, 8, 1, 32),
	SubChunks = split_chunk_into_sub_chunks(Packed, 262144 div 32, 0),
	UnpackedInSubChunks = iolist_to_binary(lists:reverse(lists:foldl(
		fun({SubChunk, Offset}, Acc) ->
			{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_composite_sub_chunk_nif(State,
					Key, SubChunk, byte_size(SubChunk), 0, 0, 0, 8, 1, Offset),
			[Unpacked | Acc]
		end,
		[],
		SubChunks
	))),
	?assertEqual(UnpackedInSubChunks, Chunk),
	Chunk2 = crypto:strong_rand_bytes(262144 - 3),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key, Chunk2,
		0, 0, 0, 8, 3, 32),
	SubChunks2 = split_chunk_into_sub_chunks(Packed2, 262144 div 32, 0),
	UnpackedInSubChunks2 = iolist_to_binary(lists:reverse(lists:foldl(
		fun({SubChunk, Offset}, Acc) ->
			{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_composite_sub_chunk_nif(State,
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

composite_chunk_assert_repacks_from_spora_2_6(State) ->
	?debugFmt("Repacking a legacy chunk to composite...", []),
	Chunk = crypto:strong_rand_bytes(262144 - 12),
	Key = crypto:strong_rand_bytes(32),
	{ok, PackedComposite} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
			Chunk, 0, 0, 0, 8, 2, 32),
	Key2 = crypto:strong_rand_bytes(32),
	{ok, Packed} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key2, Chunk, 8, 0, 0, 0),
	{ok, Repacked, UnpackedChunk} =
			ar_mine_randomx:randomx_reencrypt_legacy_to_composite_chunk_nif(State,
					Key2, Key, Packed, 0, 0, 0, 8, 8, 2, 32),
	?assertEqual(<< Chunk/binary, 0:(12 * 8) >>, UnpackedChunk),
	?assertEqual(PackedComposite, Repacked),
	%% The same with a full-size chunk.
	?debugFmt("Repacking a full-size legacy chunk to composite...", []),
	Chunk2 = crypto:strong_rand_bytes(262144),
	{ok, PackedComposite2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
			Chunk2, 0, 0, 0, 8, 2, 32),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key2, Chunk2, 16, 0, 0, 0),
	{ok, Repacked2, UnpackedChunk2} =
			ar_mine_randomx:randomx_reencrypt_legacy_to_composite_chunk_nif(State,
					Key2, Key, Packed2, 0, 0, 0, 16, 8, 2, 32),
	?assertEqual(PackedComposite2, Repacked2),
	?assertEqual(Chunk2, UnpackedChunk2).

composite_chunk_assert_repacks(State) ->
	?debugFmt("Repacking a composite chunk to same-key higher-diff composite chunk...", []),
	Chunk = crypto:strong_rand_bytes(262144 - 12),
	Key = crypto:strong_rand_bytes(32),
	{ok, Packed2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
			Chunk, 0, 0, 0, 8, 2, 32),
	{ok, Packed3} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
			Chunk, 0, 0, 0, 8, 3, 32),
	{ok, Repacked_2_3, RepackInput} =
			ar_mine_randomx:randomx_reencrypt_composite_to_composite_chunk_nif(State,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 3, 32, 32),
	?assertEqual(Packed2, RepackInput),
	?assertEqual(Packed3, Repacked_2_3),
	?debugFmt("Repacking a composite chunk to same-key higher-diff composite chunk...", []),
	{ok, Repacked_2_5, RepackInput} =
			ar_mine_randomx:randomx_reencrypt_composite_to_composite_chunk_nif(State,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 5, 32, 32),
	{ok, Packed5} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key,
			Chunk, 0, 0, 0, 8, 5, 32),
	?assertEqual(Packed5, Repacked_2_5),
	Key2 = crypto:strong_rand_bytes(32),
	?debugFmt("Repacking a composite chunk to different-key higher-diff composite chunk...",
			[]),
	{ok, Repacked_2_2, RepackInput2} =
			ar_mine_randomx:randomx_reencrypt_composite_to_composite_chunk_nif(State,
					Key, Key2, Packed2, 0, 0, 0, 8, 8, 2, 2, 32, 32),
	?assertEqual(<< Chunk/binary, 0:(12 * 8) >>, RepackInput2),
	{ok, Packed2_2} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(State, Key2,
			Chunk, 0, 0, 0, 8, 2, 32),
	?assertNotEqual(Packed2, Packed2_2),
	?assertEqual(Packed2_2, Repacked_2_2),
	try
		ar_mine_randomx:randomx_reencrypt_composite_to_composite_chunk_nif(State,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 2, 32, 32),
		?assert(false, "randomx_reencrypt_composite_to_composite_chunk_nif to reencrypt "
				"to same diff should have failed")
	catch error:badarg ->
		ok
	end,
	try
		ar_mine_randomx:randomx_reencrypt_composite_to_composite_chunk_nif(State,
					Key, Key, Packed2, 0, 0, 0, 8, 8, 2, 1, 32, 32),
		?assert(false, "randomx_reencrypt_composite_to_composite_chunk_nif to reencrypt "
				"to lower diff should have failed")
	catch error:badarg ->
		ok
	end.

test_randomx_backwards_compatibility({Key, State}) ->
    ExpectedHash = ar_util:decode(?ENCODED_HASH),
    Nonce = ar_util:decode(?ENCODED_NONCE),
    Segment = ar_util:decode(?ENCODED_SEGMENT),
    Input = << Nonce/binary, Segment/binary >>,
    {ok, Hash} = ar_mine_randomx:hash_fast_nif(State, Input, 0, 0, 0),
	?assertEqual(ExpectedHash, Hash),
    {ok, LightState} = ar_mine_randomx:init_light_nif(Key, 0, 0),
    ?assertEqual({ok, ExpectedHash},
			ar_mine_randomx:hash_light_nif(LightState, Input, 0, 0, 0)),
	?assertEqual(
		ok, ar_mine_randomx:release_state_nif(LightState), "first release"),
	?assertEqual(
		ok, ar_mine_randomx:release_state_nif(LightState), "re-releasing should be fine").

test_randomx_long({_Key, State}) ->
	Nonce = ar_util:decode(?ENCODED_NONCE),
	Segment = ar_util:decode(?ENCODED_SEGMENT),
	Input = << Nonce/binary, Segment/binary >>,
	ExpectedHash = ar_util:decode(?ENCODED_HASH),
	?debugFmt("Hashing with entropy...", []),
	{ok, Hash, OutEntropy} = ar_mine_randomx:hash_fast_long_with_entropy_nif(State, Input,
			8, 0, 0, 0),
	%% Compute it again, the result must be the same.
	?debugFmt("Hashing again with the same input...", []),
	{ok, Hash, OutEntropy} = ar_mine_randomx:hash_fast_long_with_entropy_nif(State, Input,
			8, 0, 0, 0),
	?debugFmt("Hashing with a different input...", []),
	{ok, DifferentHash, DifferentEntropy} = ar_mine_randomx:hash_fast_long_with_entropy_nif(
			State, crypto:strong_rand_bytes(48), 8, 0, 0, 0),
	?debugFmt("Hashing without entropy...", []),
    {ok, PlainHash} = ar_mine_randomx:hash_fast_nif(State, Input, 0, 0, 0),
	?assertEqual(PlainHash, Hash),
	?assertNotEqual(DifferentHash, Hash),
	?assertNotEqual(DifferentEntropy, OutEntropy),
	?assertEqual(ExpectedHash, Hash),
	ExpectedEntropy = read_entropy_fixture(),
	?assertEqual(ExpectedEntropy, OutEntropy).

is_zero(<< 0:8, Rest/binary >>) ->
	is_zero(Rest);
is_zero(<<>>) ->
	true;
is_zero(_Rest) ->
	false.

test_randomx_pack_unpack({_Key, State}) ->
	Root = crypto:strong_rand_bytes(32),

	try
		ar_mine_randomx:randomx_encrypt_chunk_nif(
			State,
			crypto:hash(sha256, << 0:256, Root/binary >>),
			<<>>,
			1, % RANDOMX_PACKING_ROUNDS
			0, 0, 0),
		?assert(false, "randomx_encrypt_chunk_nif with an empty chunk should have failed")
	catch error:badarg ->
		ok
	end,

	Cases = [
		{<<1>>, 1, Root},
		{<<1>>, 2, Root},
		{<<0>>, 1, crypto:strong_rand_bytes(32)},
		{<<0>>, 2, crypto:strong_rand_bytes(32)},
		{<<0>>, 1234234534535, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(2), 234134234, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(3), 333, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(15), 9999999999999999999999999999,
				crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(16), 16, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(256 * 1024), 100000000000000, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(256 * 1024 - 1), 100000000000000,
				crypto:strong_rand_bytes(32)}
	],
	lists:foreach(
		fun({Chunk, Offset, TXRoot}) ->
			Key1 = crypto:hash(sha256, << Offset:256, TXRoot/binary >>),
			Key2 = crypto:strong_rand_bytes(32),
			{ok, Packed1} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key1, Chunk,
					1, % RANDOMX_PACKING_ROUNDS
					0, 0, 0),
			{ok, Packed2} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key2, Chunk,
					2, % RANDOMX_PACKING_ROUNDS
					0, 0, 0),
			?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed1)),
			?assertEqual(?DATA_CHUNK_SIZE, byte_size(Packed2)),
			?assertNotEqual(Packed1, Chunk),
			?assertNotEqual(Packed2, Chunk),
			{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_chunk_nif(State, Key1,
					Packed1, byte_size(Packed1),
					1, % RANDOMX_PACKING_ROUNDS
					0, 0, 0),

			Padding = binary:part(Unpacked, byte_size(Chunk),
					byte_size(Packed1) - byte_size(Chunk)),
			?assertEqual(true, is_zero(Padding)),
			Unpadded = binary:part(Unpacked, 0, byte_size(Chunk)),
			?assertEqual(Chunk, Unpadded),

			{ok, Repacked, Intermediate} = ar_mine_randomx:randomx_reencrypt_chunk_nif(State,
					Key1, Key2, Packed1, byte_size(Packed1),
					1, % RANDOMX_PACKING_ROUNDS
					2, % RANDOMX_PACKING_ROUNDS
					0, 0, 0),
			?assertEqual(Packed2, Repacked),
			?assertEqual(Chunk, binary:part(Intermediate, 0, byte_size(Chunk)))
		end,
		Cases
	).

read_entropy_fixture() ->
	{ok, Cwd} = file:get_cwd(),
	Path = filename:join(Cwd, "./apps/arweave/test/ar_mine_randomx_entropy_fixture"),
	{ok, FileData} = file:read_file(Path),
	ar_util:decode(FileData).
