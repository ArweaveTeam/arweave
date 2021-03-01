-module(ar_mine_randomx_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ENCODED_KEY, <<"UbkeSd5Det8s6uLyuNJwCDFOZMQFa2zvsdKJ0k694LM">>).
-define(ENCODED_HASH, <<"QQYWA46qnFENL4OTQdGU8bWBj5OKZ2OOPyynY3izung">>).
-define(ENCODED_NONCE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ENCODED_SEGMENT,
    <<"7XM3fgTCAY2GFpDjPZxlw4yw5cv8jNzZSZawywZGQ6_Ca-JDy2nX_MC2vjrIoDGp">>
).
-define(ENCODED_TENTH_HASH, <<"DmwCVUMtDnUCwxcTClAOhNjxk1am6030OwGDSHfaOh4">>).

randomx_backwards_compatibility_test_() ->
    {timeout, 240, fun test_randomx_backwards_compatibility/0}.

test_randomx_backwards_compatibility() ->
    Key = ar_util:decode(?ENCODED_KEY), 
    {ok, State} = ar_mine_randomx:init_fast_nif(Key, 0, 0, 4),
    ExpectedHash = ar_util:decode(?ENCODED_HASH),
    Nonce = ar_util:decode(?ENCODED_NONCE),
    Segment = ar_util:decode(?ENCODED_SEGMENT),
    Input = << Nonce/binary, Segment/binary >>,
    {ok, Hash} = ar_mine_randomx:hash_fast_nif(State, Input, 0, 0, 0),
	?assertEqual(ExpectedHash, Hash),
	PrevH = crypto:strong_rand_bytes(48),
	SearchSpaceUpperBound = 123456789,
	{ok, ExpectedByte} = ar_mine:pick_recall_byte(Hash, PrevH, SearchSpaceUpperBound),
	Ref = make_ref(),
	PIDs = [spawn_link(
		fun() ->
			receive {EncodedByte, ExpectedHash, Nonce, Thread, Ref} ->
				Byte = binary:decode_unsigned(EncodedByte),
				?assertEqual(ExpectedByte, Byte),
				?assertEqual(self(), Thread)
			after 10000 ->
				?assert(false, "Did not hear from NIF for too long.")
			end
		end) || _ <- [1, 2]],
	ok = ar_mine_randomx:bulk_hash_fast_nif(
		State,
		Nonce,
		Nonce,
		Segment,
		PrevH,
		binary:encode_unsigned(SearchSpaceUpperBound, big),
		PIDs,
		PIDs,
		Ref,
		2,
		0,
		0,
		0
	),
	Diff = binary:encode_unsigned(binary:decode_unsigned(Hash, big) - 1),
    {true, Hash} = ar_mine_randomx:hash_fast_verify_nif(State, Diff, Input, 0, 0, 0),
    false = ar_mine_randomx:hash_fast_verify_nif(State, Hash, Input, 0, 0, 0),
    {ok, LightState} = ar_mine_randomx:init_light_nif(Key, 0, 0),
    ?assertEqual({ok, ExpectedHash}, ar_mine_randomx:hash_light_nif(LightState, Input, 0, 0, 0)).
