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
				test_register(fun test_pick_recall_byte/1, SetupData),
				test_register(fun test_randomx_backwards_compatibility/1, SetupData),
				test_register(fun test_randomx_pack_unpack/1, SetupData),
				test_register(fun test_randomx_long/1, SetupData)
			]
		end
	}.

test_pick_recall_byte({_Key, State}) ->
	PrevH = ar_util:decode(
			<<"HXhU_CnCqHzn-AAPmWNcPCFb4XASvq0wiU9n59dbkl1C5xVf_nf5h4N5t0E-dGIp">>),
	ExpectedNonce = ar_util:decode(<<"rtYRrtzI77B52ZtfYLSdWYqrgz0Evqyo-aMZXIkZWco">>),
	Segment = ar_util:decode(
			<<"NYdaNPNU3Nyp74gzKT4JdC5j9aoSrwFlIFUPdOqy7vPtvwBgFle4rLfuyKwhfHlh">>),
	{ok, ExpectedH0} = ar_mine_randomx:hash_fast_nif(State,
			<< ExpectedNonce/binary, Segment/binary >>, 0, 0, 0),
	PartitionUpperBound = 24063907917401,
	{ok, ExpectedByte} = ar_mine:pick_recall_byte(ExpectedH0, PrevH, PartitionUpperBound),
	Ref = make_ref(),
	HashPID = spawn_link(
		fun() ->
			receive {EncodedByte, H0, Nonce, Thread, Ref} ->
				assert_bulk_hash(
					{ExpectedH0, H0},
					{ExpectedNonce, Nonce},
					{self(), Thread},
					{ExpectedByte, EncodedByte})
			after 5000 ->
				?assert(false, "Did not hear from NIF for too long")
			end
		end),
	HashWithEntropyPID = spawn_link(
		fun() ->
			% When hashing with entropy the message includes an extra Entropy term
			receive {EncodedByte, H0, _Entropy, Nonce, Thread, Ref} ->
				assert_bulk_hash(
					{ExpectedH0, H0},
					{ExpectedNonce, Nonce},
					{self(), Thread},
					{ExpectedByte, EncodedByte})
			after 5000 ->
				?assert(false, "Did not hear from NIF for too long")
			end
		end),
	ok = ar_mine_randomx:bulk_hash_fast_nif(State,
			ExpectedNonce, ExpectedNonce, Segment, PrevH,
			binary:encode_unsigned(PartitionUpperBound, big), [HashPID], [HashPID],
			Ref, 1, 0, 0, 0),
	ok = ar_mine_randomx:bulk_hash_fast_long_with_entropy_nif(State,
			ExpectedNonce, ExpectedNonce, Segment, PrevH,
			binary:encode_unsigned(PartitionUpperBound, big),
			[HashWithEntropyPID], [HashWithEntropyPID], Ref, 1, 8, 0, 0, 0),
	% sleep so that we give time for the PID to receive the message or timeout
	timer:sleep(5000).

assert_bulk_hash(
		{ExpectedH0, H0},
		{ExpectedNonce, Nonce},
		{ExpectedThread, Thread},
		{ExpectedByte, EncodedByte}) ->
	?assertEqual(ExpectedH0, H0),
	?assertEqual(ExpectedNonce, Nonce),
	?assertEqual(ExpectedThread, Thread),
	Byte = binary:decode_unsigned(EncodedByte),
	?assertEqual(ExpectedByte, Byte).

test_randomx_backwards_compatibility({Key, State}) ->
    ExpectedHash = ar_util:decode(?ENCODED_HASH),
    Nonce = ar_util:decode(?ENCODED_NONCE),
    Segment = ar_util:decode(?ENCODED_SEGMENT),
    Input = << Nonce/binary, Segment/binary >>,
    {ok, Hash} = ar_mine_randomx:hash_fast_nif(State, Input, 0, 0, 0),
	?assertEqual(ExpectedHash, Hash),
	PrevH = crypto:strong_rand_bytes(48),
	PartitionUpperBound = 123456789,
	Ref = make_ref(),
	PIDs = [spawn_link(
		fun() ->
			receive {EncodedByte, HashLocal, NonceLocal, Thread, Ref} ->
				Byte = binary:decode_unsigned(EncodedByte),
				{ok, ExpectedByte} = ar_mine:pick_recall_byte(HashLocal, PrevH,
						PartitionUpperBound),
				?assertEqual(ExpectedByte, Byte),
				?assertEqual(self(), Thread),
				InputLocal = << NonceLocal/binary, Segment/binary >>,
				{ok, ExpectedHashLocal} = ar_mine_randomx:hash_fast_nif(State, InputLocal,
						0, 0, 0),
				?assertEqual(ExpectedHashLocal, HashLocal)
			after 10000 ->
				?assert(false, "Did not hear from NIF for too long.")
			end
		end) || _ <- [1, 2]],
	ok = ar_mine_randomx:bulk_hash_fast_nif(State, Nonce, Nonce, Segment, PrevH,
			binary:encode_unsigned(PartitionUpperBound, big), PIDs, PIDs, Ref, 2, 0, 0, 0),
	Diff = binary:encode_unsigned(binary:decode_unsigned(Hash, big) - 1),
    {true, Hash} = ar_mine_randomx:hash_fast_verify_nif(State, Diff, Input, 0, 0, 0),
    false = ar_mine_randomx:hash_fast_verify_nif(State, Hash, Input, 0, 0, 0),
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
	{ok, Hash, OutEntropy} = ar_mine_randomx:hash_fast_long_with_entropy_nif(State, Input, 8, 0,
			0, 0),
	%% Compute it again, the result must be the same.
	{ok, Hash, OutEntropy} = ar_mine_randomx:hash_fast_long_with_entropy_nif(State, Input, 8, 0,
			0, 0),
	{ok, DifferentHash, DifferentEntropy} = ar_mine_randomx:hash_fast_long_with_entropy_nif(State,
			crypto:strong_rand_bytes(48), 8, 0, 0, 0),
    {ok, PlainHash} = ar_mine_randomx:hash_fast_nif(State, Input, 0, 0, 0),
	?assertEqual(PlainHash, Hash),
	?assertNotEqual(DifferentHash, Hash),
	?assertNotEqual(DifferentEntropy, OutEntropy),
	?assertEqual(ExpectedHash, Hash),
	ExpectedEntropy = read_entropy_fixture(),
	?assertEqual(ExpectedEntropy, OutEntropy),
	%% Assert bulk_hash_fast_long_with_entropy_nif produces the same hash.
	PrevH = crypto:strong_rand_bytes(48),
	PartitionUpperBound = 123456789,
	Ref = make_ref(),
	PIDs = [spawn_link(
		fun() ->
			receive {EncodedByte, HashLocal, EntropyLocal, NonceLocal, Thread, Ref} ->
				Byte = binary:decode_unsigned(EncodedByte),
				{ok, ExpectedByte} = ar_mine:pick_recall_byte(HashLocal, PrevH,
						PartitionUpperBound),
				?assertEqual(ExpectedByte, Byte),
				?assertEqual(self(), Thread),
				InputLocal = << NonceLocal/binary, Segment/binary >>,
				{ok, ExpectedHashLocal, ExpectedEntropyLocal} =
						ar_mine_randomx:hash_fast_long_with_entropy_nif(State, InputLocal,
								8, 0, 0, 0),
				?assertEqual(ExpectedHashLocal, HashLocal),
				?assertEqual(ExpectedEntropyLocal, EntropyLocal)
			after 10000 ->
				?assert(false, "Did not hear from NIF for too long.")
			end
		end) || _ <- [1, 2]],
	ok = ar_mine_randomx:bulk_hash_fast_long_with_entropy_nif(State, Nonce, Nonce, Segment,
			PrevH, binary:encode_unsigned(PartitionUpperBound, big), PIDs, PIDs, Ref, 2,
			8, 0, 0, 0).

is_zero(<< 0:8, Rest/binary >>) ->
	is_zero(Rest);
is_zero(<<>>) ->
	true;
is_zero(_Rest) ->
	false.

test_randomx_pack_unpack({_Key, State}) ->
	Root = crypto:strong_rand_bytes(32),

	%% All chunks are padded to 256 KiB on the client side so
	%% randomx_encrypt_nif should never receive an input of a
	%% different size. We assert the behaviour here just for
	%% the extra clarity.
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
		{crypto:strong_rand_bytes(256 * 1024 - 1), 100000000000000, crypto:strong_rand_bytes(32)}
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
			
			Padding = binary:part(Unpacked, byte_size(Chunk), byte_size(Packed1) - byte_size(Chunk)),
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
	Dir = filename:dirname(?FILE),
	Path = filename:join(Dir, "ar_mine_randomx_entropy_fixture"),
	{ok, FileData} = file:read_file(Path),
	ar_util:decode(FileData).
