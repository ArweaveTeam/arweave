-module(ar_mine_randomx_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar_mine.hrl").

-define(ENCODED_KEY, <<"UbkeSd5Det8s6uLyuNJwCDFOZMQFa2zvsdKJ0k694LM">>).
-define(ENCODED_HASH, <<"QQYWA46qnFENL4OTQdGU8bWBj5OKZ2OOPyynY3izung">>).
-define(ENCODED_NONCE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ENCODED_SEGMENT,
    <<"7XM3fgTCAY2GFpDjPZxlw4yw5cv8jNzZSZawywZGQ6_Ca-JDy2nX_MC2vjrIoDGp">>
).
-define(ENCODED_TENTH_HASH, <<"DmwCVUMtDnUCwxcTClAOhNjxk1am6030OwGDSHfaOh4">>).

randomx_suite_test_() ->
	{timeout, 500, fun test_randomx_/0}.

test_randomx_() ->
	Key = ar_util:decode(?ENCODED_KEY),
	{ok, State} = ar_mine_randomx:init_fast_nif(Key, 0, 0, 4),
	test_randomx_backwards_compatibility(State, Key),
	test_randomx_pack_unpack(State),
	test_randomx_long(State),
	test_pick_recall_byte().

test_pick_recall_byte() ->
	Key = ar_util:decode(<<"__9yhVR_fRz07vzsXsmjglYsLvXELQ5xWNS-S5OtN5I">>),
	{ok, State} = ar_mine_randomx:init_fast_nif(Key, 0, 0, 4),
	PrevH = ar_util:decode(
			<<"HXhU_CnCqHzn-AAPmWNcPCFb4XASvq0wiU9n59dbkl1C5xVf_nf5h4N5t0E-dGIp">>),
	ExpectedNonce = ar_util:decode(<<"rtYRrtzI77B52ZtfYLSdWYqrgz0Evqyo-aMZXIkZWco">>),
	Segment = ar_util:decode(
			<<"NYdaNPNU3Nyp74gzKT4JdC5j9aoSrwFlIFUPdOqy7vPtvwBgFle4rLfuyKwhfHlh">>),
	{ok, ExpectedH0} = ar_mine_randomx:hash_fast_nif(State,
			<< ExpectedNonce/binary, Segment/binary >>, 0, 0, 0),
	SearchSpaceUpperBound = 24063907917401,
	{ok, ExpectedByte} = ar_mine:pick_recall_byte(ExpectedH0, PrevH, SearchSpaceUpperBound),
	Ref = make_ref(),
	[PID1, PID2] = [spawn_link(
		fun() ->
			receive {EncodedByte, H0, Nonce, Thread, Ref} ->
				?assertEqual(ExpectedH0, H0),
				?assertEqual(ExpectedNonce, Nonce),
				?assertEqual(self(), Thread),
				Byte = binary:decode_unsigned(EncodedByte),
				?assertEqual(ExpectedByte, Byte)
			after 10000 ->
				?assert(false, "Did not hear from NIF for too long.")
			end
		end) || _ <- [1, 2]],
	ok = ar_mine_randomx:bulk_hash_fast_nif(State, ExpectedNonce, ExpectedNonce, Segment, PrevH,
			binary:encode_unsigned(SearchSpaceUpperBound, big), [PID1], [PID1], Ref, 1, 0, 0, 0),
	ok = ar_mine_randomx:bulk_hash_fast_long_with_entropy_nif(State, ExpectedNonce, ExpectedNonce,
			Segment, PrevH, binary:encode_unsigned(SearchSpaceUpperBound, big), [PID2], [PID2],
			Ref, 1, 8, 0, 0, 0).

test_randomx_backwards_compatibility(State, Key) ->
    ExpectedHash = ar_util:decode(?ENCODED_HASH),
    Nonce = ar_util:decode(?ENCODED_NONCE),
    Segment = ar_util:decode(?ENCODED_SEGMENT),
    Input = << Nonce/binary, Segment/binary >>,
    {ok, Hash} = ar_mine_randomx:hash_fast_nif(State, Input, 0, 0, 0),
	?assertEqual(ExpectedHash, Hash),
	PrevH = crypto:strong_rand_bytes(48),
	SearchSpaceUpperBound = 123456789,
	Ref = make_ref(),
	PIDs = [spawn_link(
		fun() ->
			receive {EncodedByte, HashLocal, NonceLocal, Thread, Ref} ->
				Byte = binary:decode_unsigned(EncodedByte),
				{ok, ExpectedByte} = ar_mine:pick_recall_byte(HashLocal, PrevH,
						SearchSpaceUpperBound),
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
			binary:encode_unsigned(SearchSpaceUpperBound, big), PIDs, PIDs, Ref, 2, 0, 0, 0),
	Diff = binary:encode_unsigned(binary:decode_unsigned(Hash, big) - 1),
    {true, Hash} = ar_mine_randomx:hash_fast_verify_nif(State, Diff, Input, 0, 0, 0),
    false = ar_mine_randomx:hash_fast_verify_nif(State, Hash, Input, 0, 0, 0),
    {ok, LightState} = ar_mine_randomx:init_light_nif(Key, 0, 0),
    ?assertEqual({ok, ExpectedHash},
			ar_mine_randomx:hash_light_nif(LightState, Input, 0, 0, 0)).

test_randomx_long(State) ->
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
	SearchSpaceUpperBound = 123456789,
	Ref = make_ref(),
	PIDs = [spawn_link(
		fun() ->
			receive {EncodedByte, HashLocal, EntropyLocal, NonceLocal, Thread, Ref} ->
				Byte = binary:decode_unsigned(EncodedByte),
				{ok, ExpectedByte} = ar_mine:pick_recall_byte(HashLocal, PrevH,
						SearchSpaceUpperBound),
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
			PrevH, binary:encode_unsigned(SearchSpaceUpperBound, big), PIDs, PIDs, Ref, 2,
			8, 0, 0, 0).

test_randomx_pack_unpack(State) ->
	Root = crypto:strong_rand_bytes(32),
	Cases = [
		{<<>>, 0, Root},
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
			Key = crypto:hash(sha256, << Offset:256, TXRoot/binary >>),
			{ok, Packed} = ar_mine_randomx:randomx_encrypt_chunk_nif(State, Key, Chunk,
					1, % RANDOMX_PACKING_ROUNDS
					0, 0, 0),
			ExpectedSize =
				case byte_size(Chunk) of
					0 ->
						%% All chunks are padded to 256 KiB on the client side so
						%% randomx_encrypt_nif should never receive an input of a
						%% different size. We assert the behaviour here just for
						%% the extra clarity.
						0;
					_ ->
						((byte_size(Chunk) - 1) div 64 + 1) * 64
				end,
			?assertEqual(ExpectedSize, byte_size(Packed)),
			case Chunk of
				<<>> -> ok;
				_ ->
					?assertNotEqual(Packed, Chunk),
					{ok, Unpacked} = ar_mine_randomx:randomx_decrypt_chunk_nif(State, Key,
							Packed, byte_size(Chunk),
							1, % RANDOMX_PACKING_ROUNDS
							0, 0, 0),
					?assertEqual(Chunk, Unpacked)
			end
		end,
		Cases
	).

read_entropy_fixture() ->
	Dir = filename:dirname(?FILE),
	Path = filename:join(Dir, "ar_mine_randomx_entropy_fixture"),
	{ok, FileData} = file:read_file(Path),
	ar_util:decode(FileData).
