-module(ec_secp256k1).
-include("ar.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    new/0,
    sign/2, sign_recoverable/2,
    verify/3, verify/2,
    to_public/1,
    serialize/2, deserializePublic/2, deserializePrivate/2,
    identifier/1, from_identifier/1,
    recover_pk/2
]).

-type ecdsa_digest_type() :: sha256.
-type serialization_formats() :: raw | jwk.

-export_type([serialization_formats/0, ecdsa_digest_type/0]).

-define(SigUpperBound, binary:decode_unsigned(<<16#7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0:256>>)).
-define(SigDiv, binary:decode_unsigned(<<16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141:256>>)).


-spec new() -> PrivateKey :: public_key:ecdsa_private_key().
new() ->
    case secp256k1_nif:generate_key() of
        {error, Reason} ->
            ?LOG_ERROR([{event, secp256k1_generate_key}, {reason, Reason}]),
            erlang:halt();
        {ok, {PrivBytes, PubBytes}} ->
            #'ECPrivateKey'{
                version=1,
                privateKey=PrivBytes,
                parameters={namedCurve, secp256k1},
                publicKey=PubBytes,
                attributes=asn1_NOVALUE
            }
    end.


-spec to_public(PrivateKey :: public_key:ecdsa_private_key()) -> public_key:ecdsa_public_key().
to_public({_, _, _, _, PubBytes, _}) ->
    ECPoint = #'ECPoint'{point=PubBytes},
    {ECPoint, {namedCurve, secp256k1}}.

-spec sign(PlainText :: binary() | {digest, Digest :: binary()}, PrivateKey :: public_key:ecdsa_private_key()) -> binary().
sign({digest, Digest}, {_, _, PrivBytes, _, _, _}) when byte_size(Digest) == 32 ->
    {ok, Signature} = secp256k1_nif:sign(Digest, PrivBytes),
    Signature;
sign(PlainText, PrivateKey) ->
    Digest = crypto:hash(sha256, PlainText),
    sign({digest, Digest}, PrivateKey).

-spec sign_recoverable(PlainText :: binary() | {digest, Digest :: binary()}, PrivateKey :: public_key:ecdsa_private_key()) -> binary().
sign_recoverable({digest, Digest}, {_, _, PrivBytes, _, _, _}) when byte_size(Digest) == 32 ->
    {ok, Signature} = secp256k1_nif:sign_recoverable(Digest, PrivBytes),
    Signature;
sign_recoverable(PlainText, PrivateKey) ->
    Digest = crypto:hash(sha256, PlainText),
    sign_recoverable({digest, Digest}, PrivateKey).


-spec verify(PlainText :: binary() | {digest, Digest :: binary()}, Signature :: binary(), PublicKey :: public_key:ecdsa_public_key()) -> boolean().
verify({digest, Digest}, Signature, {#'ECPoint'{point=PubBytes}, {namedCurve, secp256k1}}) when byte_size(Signature) == 64, byte_size(Digest) == 32  ->
    case secp256k1_nif:verify(Digest, Signature, PubBytes) of
        {ok, Result} -> Result;
        {error, _Reason} -> false
    end;
verify({digest, Digest}, Signature, {#'ECPoint'{point=PubBytes}, {namedCurve, secp256k1}}) when byte_size(Signature) == 65, byte_size(Digest) == 32 ->
    case secp256k1_nif:verify_recoverable(Digest, Signature, PubBytes) of
        {ok, Result} -> Result;
        {error, _Reason} -> false
    end;
verify(PlainText, Signature, {#'ECPoint'{}, {namedCurve, secp256k1}} = PublicKey) ->
    Digest = crypto:hash(sha256, PlainText),
    verify({digest, Digest}, Signature, PublicKey).

-spec verify(PlainText :: binary() | {digest, Digest :: binary()}, Signature :: binary()) -> {boolean(), binary()}.
verify({digest, Digest}, Signature) when byte_size(Signature) == 65, byte_size(Digest) == 32  ->
    case secp256k1_nif:recover_pk_and_verify(Digest, Signature) of
        {ok, Result} -> Result;
        {error, _Reason} -> {false, <<>>}
    end;
verify(PlainText, Signature) ->
    Digest = crypto:hash(sha256, PlainText),
    verify({digest, Digest}, Signature).

-spec recover_pk(PlainText :: binary() | {digest, Digest :: binary()}, RecoverableSignature :: binary()) -> public_key:ecdsa_public_key().
recover_pk({digest, Digest}, RecoverableSignature) when byte_size(RecoverableSignature) == 65, byte_size(Digest) == 32 ->
    case secp256k1_nif:recover_pk(Digest, RecoverableSignature) of
        {ok, PubBytes} ->
            ECPoint = #'ECPoint'{point=PubBytes},
            {ok, {ECPoint, {namedCurve, secp256k1}}};
        {error, _} = Err -> Err
    end;
recover_pk(PlainText, RecoverableSignature) ->
    Digest = crypto:hash(sha256, PlainText),
    recover_pk({digest, Digest}, RecoverableSignature).


-spec serialize(Format :: serialization_formats(), PublicKey :: public_key:ecdsa_public_key() | public_key:ecdsa_private_key()) -> binary().
serialize(Format, {{'ECPoint', PubBytes}, {namedCurve, secp256k1}})
  when Format == raw, byte_size(PubBytes) == 65 ->
    <<4:8, XY/binary>> = PubBytes,
    <<X:32/binary, Y:32/binary>> = XY,
    Prefix = case binary:last(Y) rem 2 of
        0 -> <<2:8>>;
        1 -> <<3:8>>
    end,
    <<Prefix/binary, X/binary>>;
serialize(Format, {{'ECPoint', PubBytes}, {namedCurve, secp256k1}})
  when Format == jwk, byte_size(PubBytes) == 65 ->
    <<4:8, XY/binary>> = PubBytes,
    <<X:32/binary, Y:32/binary>> = XY,
    ar_serialize:jsonify(
        {
            [
                {kty, <<"EC">>},
                {crv, <<"secp256k1">>},
                {x, ar_util:encode(X)},
                {y, ar_util:encode(Y)}
            ]
        }
    );
serialize(Format, {_, _, PrivBytes, _, _, _})
  when Format == raw, byte_size(PrivBytes) == 32 ->
    PrivBytes;
serialize(Format, {_, _, PrivBytes, _, PubBytes, _})
  when Format == jwk, byte_size(PrivBytes) == 32, byte_size(PubBytes) == 65 ->
    <<4:8, XY/binary>> = PubBytes,
    <<X:32/binary, Y:32/binary>> = XY,
    ar_serialize:jsonify(
        {
            [
                {kty, <<"EC">>},
                {crv, <<"secp256k1">>},
                {d, ar_util:encode(PrivBytes)},
                {x, ar_util:encode(X)},
                {y, ar_util:encode(Y)}
            ]
        }
    ).


-spec deserializePublic(Format :: serialization_formats(), PublicKey :: binary()) -> public_key:ecdsa_public_key().
deserializePublic(Format, PublicKeyBytes) when Format == raw, byte_size(PublicKeyBytes) == 33 ; byte_size(PublicKeyBytes) == 65 ->
    {{'ECPoint', PublicKeyBytes}, {namedCurve, secp256k1}};
deserializePublic(Format, PublicKeyJWK) when Format == jwk ->
    {PublicKey} = ar_serialize:dejsonify(PublicKeyJWK),
    {<<"kty">>, <<"EC">>} = lists:keyfind(<<"kty">>, 1, PublicKey),
    {<<"x">>, XB64} = lists:keyfind(<<"x">>, 1, PublicKey),
    {<<"y">>, YB64} = lists:keyfind(<<"y">>, 1, PublicKey),
    X = ar_util:decode(XB64),
    Y = ar_util:decode(YB64),
    PubBytes = <<4:8, X/binary, Y/binary>>,
    deserializePublic(raw, PubBytes).

% @doc all identifiers need to have an odd length. hence, zero padding as the raw compressed PK is 33 byte long.
-spec identifier(PublicKey :: public_key:ecdsa_public_key()) -> binary().
identifier(PublicKey) ->
	Raw = serialize(raw, PublicKey),
    Padded = <<Raw/binary, 0:8>>,
    <<2:8, Padded/binary>>.

-spec from_identifier(Identifier :: binary()) -> public_key:ecdsa_public_key().
from_identifier(Identifier) ->
    <<2:8, CompressedBytes:33/binary, 0:8>> = Identifier,
	deserializePublic(raw, CompressedBytes).

-spec deserializePrivate(Format :: serialization_formats(), PrivateKey :: binary()) -> public_key:ecdsa_private_key().
deserializePrivate(Format, PrivateKeyBytes) when Format == raw, byte_size(PrivateKeyBytes) == 32 ->
    {PubBytes, _} = crypto:generate_key(ecdh, secp256k1, PrivateKeyBytes),
    #'ECPrivateKey'{version=1, privateKey=PrivateKeyBytes,parameters={namedCurve, secp256k1}, publicKey=PubBytes,attributes=asn1_NOVALUE};
deserializePrivate(Format, PrivateKeyJWK) when Format == jwk ->
    {PrivateKey} = ar_serialize:dejsonify(PrivateKeyJWK),
    {<<"kty">>, <<"EC">>} = lists:keyfind(<<"kty">>, 1, PrivateKey),
    {<<"d">>, PrivB64} = lists:keyfind(<<"d">>, 1, PrivateKey),
    PrivBytes = ar_util:decode(PrivB64),
    deserializePrivate(raw, PrivBytes).


%%%===================================================================
%%% Tests.
%%%===================================================================

de_serialization_test() ->
	{_, _, PrivBytes, _, PubBytes, _} = SK = new(),
    PK = to_public(SK),
    SKRaw = serialize(raw, SK),
    ?assert(byte_size(SKRaw) =:= 32),
    ?assertEqual(PrivBytes, SKRaw),
    PKCompressed = serialize(raw, PK),
    ?assert(byte_size(PKCompressed) =:= 33),
    <<4:8, X:32/binary, Y:32/binary >> = PubBytes,
    <<Prefix:1/binary, X:32/binary>> = PKCompressed,
    case binary:last(Y) rem 2 of
        0 -> ?assertEqual(Prefix, <<2:8>>);
        1 -> ?assertEqual(Prefix, <<3:8>>)
    end,
    SKJWK = serialize(jwk, SK),
    ?assertEqual(serialize(raw, deserializePrivate(jwk, SKJWK)), SKRaw),
    PKJWK = serialize(jwk, PK),
    ?assertEqual(serialize(raw, deserializePublic(jwk, PKJWK)), PKCompressed),

    Identifier = identifier(PK),
    ?assertEqual(byte_size(Identifier), 35),
    <<2:8, PKCompressed:33/binary, 0:8>> = Identifier.

sign_verify_test() ->
    {_, _, PrivBytes, _, PubBytes, _} = SK = new(),
    PK = to_public(SK),
    Identifier = identifier(PK),
    Msg = <<"This is a test message!">>,
    Sig = sign(Msg, SK),
    ?assertEqual(byte_size(Sig), 64),
    ?assert(verify(Msg, Sig, PK)),
    ?assert(verify(Msg, Sig, from_identifier(Identifier))),

    % deterministic Sig
    NewSig = sign(Msg, SK),
    ?assertEqual(NewSig, Sig),

    % different Message
    Msg2 = <<"This is another test message!">>,
    Sig2 = sign(Msg2, SK),
    ?assertEqual(byte_size(Sig2), 64),
    ?assertNotEqual(Sig, Sig2),
    ?assert(verify(Msg2, Sig2, PK)),
    ?assertNot(verify(Msg, Sig2, PK)),
    ?assertNot(verify(Msg2, Sig, PK)),

    % different key
    {_, _, OtherPrivBytes, _, OtherPubBytes, _} = SKOther = new(),
    OtherIdentifier = identifier(to_public(SKOther)),
    ?assertNotEqual(Identifier, OtherIdentifier),
    ?assertNotEqual(PrivBytes, OtherPrivBytes),
    ?assertNotEqual(PubBytes, OtherPubBytes),
    OtherSig = sign(Msg, SKOther),
    ?assertEqual(byte_size(OtherSig), 64),
    ?assert(verify(Msg, OtherSig, to_public(SKOther))),
    ?assertNot(verify(Msg, OtherSig, to_public(SK))),
    ?assertNot(verify(Msg, OtherSig, from_identifier(Identifier))),
    ?assertNotEqual(Sig, OtherSig).

sign_verify_recoverable_test() ->
    {_, _, _, _, PubBytes, _} = SK = new(),
    PK = to_public(SK),
    Identifier = identifier(PK),
    Msg = <<"This is a test message!">>,
    SigRecoverable = sign_recoverable(Msg, SK),
    ?assertEqual(byte_size(SigRecoverable), 65),
    ?assert(verify(Msg, SigRecoverable, PK)),
    ?assert(verify(Msg, SigRecoverable, from_identifier(Identifier))),

    % recid byte
    Sig = sign(Msg, SK),
    <<CompactSig:64/binary, RecId:8>> = SigRecoverable,
    ?assertEqual(CompactSig, Sig),
    ?assert(lists:member(RecId, [0, 1, 2, 3])),
    ?assert(verify(Msg, Sig, PK)),

    % deterministic Sig
    NewSig = sign_recoverable(Msg, SK),
    ?assertEqual(NewSig, SigRecoverable),

    % Recover pk
    {ok, {#'ECPoint'{point=RecoveredBytes}, _} = RecoveredPK} = recover_pk(Msg, SigRecoverable),
    ?assert(verify(Msg, SigRecoverable, RecoveredPK)),
    ?assertEqual(RecoveredBytes, PubBytes),

    % Needs 65 byte long sig
    ?assertException(error, badarg, recover_pk(Msg, Sig)),

    BadSig = <<0:8, CompactSig:63/binary, RecId:8>>,
    case recover_pk(Msg, BadSig) of
        {error, Reason} -> ?assertEqual(Reason, "Failed to recover public key.");
        {ok, {#'ECPoint'{point=BadSigBytes}, _} } -> ?assertNotEqual(BadSigBytes, PubBytes)
    end,
    BadRecidSig = <<CompactSig:64/binary, 4:8>>,
    ?assertEqual({error,  "Invalid signature recid. recid >= 0 && recid <= 3."}, recover_pk(Msg, BadRecidSig)),

    BadMsg = <<"This is a bad test message!">>,
    {ok,  {#'ECPoint'{point=BadMsgBytes}, _} = BadPublicKey} = recover_pk(BadMsg, SigRecoverable),
    ?assertNotEqual(BadMsgBytes, PubBytes),
    % bad public key will verify the bad message
    ?assert(verify(BadMsg, SigRecoverable, BadPublicKey)),
    % bad public key will NOT verify the correct message
    ?assertNot(verify(Msg, SigRecoverable, BadPublicKey)),

    % recover and verify at once
    ?assertEqual({true, PubBytes}, verify(Msg, SigRecoverable)),
    ?assertEqual({false, <<>>}, verify(Msg, BadRecidSig)),

    % recover and verify returns true for arbitrary message, but non matching PK
    {true, ArbitraryPubBytes} = verify(crypto:strong_rand_bytes(100), SigRecoverable),
    ?assertNotEqual(PubBytes, ArbitraryPubBytes).

fixtures_test() ->
    Test = fun(Root) ->
        fun() ->
            {ok, Msg} = file:read_file(filename:join(Root, "msg.bin")),
            {ok, Sig} = file:read_file(filename:join(Root, "sig.bin")),
            io:format("~p~n", [filename:join(Root, "sk.json")]),
            Wallet = ar_wallet:load_keyfile(filename:join(Root, "sk.json")),
            {{_, SK, Identifier}, {_, Identifier}} = Wallet,
            ?assert(verify(Msg, Sig, from_identifier(Identifier))),
            NewSig = sign(Msg, SK),
            ?assert(verify(Msg, NewSig, to_public(SK))),
            % fixture signatures created with random nonce
            ?assertNotEqual(Sig, NewSig),
            % deterministic signatures
            AnotherSig = sign(Msg, SK),
            ?assertEqual(AnotherSig, NewSig)
        end
    end,
    {ok, Cwd} = file:get_cwd(),
    [
        {"Erlang OpenSSL", Test(filename:join(Cwd, "./apps/arweave/test/fixtures/secp256k1/erlang"))},
        {"RustCrypto K256 Crate", Test(filename:join(Cwd, "./apps/arweave/test/fixtures/secp256k1/RustCrypto-k256"))}
    ].
