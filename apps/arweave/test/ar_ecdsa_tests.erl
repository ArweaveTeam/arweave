-module(ar_ecdsa_tests).

-include("../include/ar.hrl").

-include_lib("eunit/include/eunit.hrl").

sign_ecrecover_test() ->
	{{_, PrivBytes, PubBytes}, _} = ar_wallet:new({?ECDSA_SIGN_ALG, secp256k1}),
	% Just call. It should not fail
	ar_wallet:hash_pub_key(PubBytes),
	Msg = <<"This is a test message!">>,
	SigRecoverable = secp256k1_nif:sign(Msg, PrivBytes),
	?assertEqual(byte_size(SigRecoverable), 65),

	% recid byte
	<<CompactSig:64/binary, RecId:8>> = SigRecoverable,
	?assert(lists:member(RecId, [0, 1, 2, 3])),

	% deterministic Sig
	NewSig = secp256k1_nif:sign(Msg, PrivBytes),
	?assertEqual(NewSig, SigRecoverable),

	% Recover pk
	{true, RecoveredBytes} = secp256k1_nif:ecrecover(Msg, SigRecoverable),
	io:format("Prv ~p~n", [PrivBytes]),
	io:format("Pub ~p~n", [PubBytes]),
	?assertEqual(RecoveredBytes, PubBytes),

	BadRecidSig = <<CompactSig:64/binary, 4:8>>,
	{false, <<>>} = secp256k1_nif:ecrecover(Msg, BadRecidSig),

	BadMsg = <<"This is a bad test message!">>,
	{true, ArbitraryPubBytes1} = secp256k1_nif:ecrecover(BadMsg, SigRecoverable),
	?assertNotEqual(PubBytes, ArbitraryPubBytes1),

	% recover and verify returns true for arbitrary message, but non matching PK
	{true, ArbitraryPubBytes2} = secp256k1_nif:ecrecover(crypto:strong_rand_bytes(100), SigRecoverable),
	?assertNotEqual(PubBytes, ArbitraryPubBytes2),

	ok.
