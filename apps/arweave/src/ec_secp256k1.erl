-module(ec_secp256k1).

-include_lib("public_key/include/public_key.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([new/0, sign/3, verify/4, to_public/1, serialize/2, deserializePublic/2, deserializePrivate/2, identifier/1, from_identifier/1]).

-type ecdsa_digest_type() :: sha256.
-type serialization_formats() :: raw | jwk.

-export_type([serialization_formats/0, ecdsa_digest_type/0]).

-define(SigUpperBound, binary:decode_unsigned(<<16#7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF5D576E7357A4501DDFE92F46681B20A0:256>>)).
-define(SigDiv, binary:decode_unsigned(<<16#FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141:256>>)).


-spec new() -> PrivateKey :: public_key:ecdsa_private_key().
new() ->
    public_key:generate_key({namedCurve, secp256k1}).


-spec to_public(PrivateKey :: public_key:ecdsa_private_key()) -> public_key:ecdsa_public_key().
to_public({_, _, _, _, PubBytes, _}) ->
    ECPoint = #'ECPoint'{point=PubBytes},
    {ECPoint, {namedCurve, secp256k1}}.

-spec sign(DigestOrPlainText :: binary() | {digest, binary()}, DigestType :: ecdsa_digest_type(), PrivateKey :: public_key:ecdsa_private_key()) -> binary().
sign(DigestOrPlainText, DigestType, {_, _, PrivBytes, _, _, _}) ->
    DERSignature = crypto:sign(ecdsa, DigestType, DigestOrPlainText, [PrivBytes, secp256k1]),
    case check_low_s(DERSignature) of
        {valid, _, _} -> DERSignature;
        {invalid, R, S} -> public_key:der_encode('ECDSA-Sig-Value', #'ECDSA-Sig-Value'{ r = R, s = ?SigDiv - S})
    end.


-spec verify(Message :: binary() | {digest, binary()}, DigestType :: ecdsa_digest_type(), Signature :: binary(), PublicKey :: public_key:ecdsa_public_key()) -> boolean().
verify(DigestOrPlainText, DigestType, Signature, {#'ECPoint'{point=PubBytes}, {namedCurve, secp256k1}}) when byte_size(Signature) == 64 ->
    case check_low_s(Signature) of
        {valid, R, S} ->
            DERSignature = public_key:der_encode('ECDSA-Sig-Value', #'ECDSA-Sig-Value'{ r = R, s = S}),
            crypto:verify(ecdsa, DigestType, DigestOrPlainText, DERSignature, [PubBytes, secp256k1]);
        {invalid, _, _} ->
            false
    end;
verify(DigestOrPlainText, DigestType, DERSignature, {#'ECPoint'{point=PubBytes}, {namedCurve, secp256k1}}) ->
    case check_low_s(DERSignature) of
        {valid, _, _} ->
            crypto:verify(ecdsa, DigestType, DigestOrPlainText, DERSignature, [PubBytes, secp256k1]);
        {invalid, _, _} ->
            false
    end.


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

%% @private
%% ensures compatible signatures according to https://github.com/bitcoin/bips/blob/master/bip-0062.mediawiki
check_low_s(Signature) when byte_size(Signature) == 64 ->
    <<RBin:32/binary, SBin:32/binary>> = Signature,
    check_low_s(binary:decode_unsigned(RBin), binary:decode_unsigned(SBin));
check_low_s(Signature) ->
    case catch public_key:der_decode('ECDSA-Sig-Value', Signature) of
        {'EXIT', _} -> {invalid, undefined, undefined};
        #'ECDSA-Sig-Value'{ r = R, s = S }  -> check_low_s(R, S)
    end.

%% @private
check_low_s(R, S) ->
    case S =< ?SigUpperBound of
        true ->
            {valid, R, S};
        false ->
            {invalid, R, S}
    end.

%% @doc Ensure that parsing of core command line options functions correctly.
de_serialization_test() ->
	SK = new(),
    PK = to_public(SK),
    SKRaw = serialize(raw, SK),
    ?assert(byte_size(SKRaw) =:= 32),
    PKRaw = serialize(raw, PK),
    ?assert(byte_size(PKRaw) =:= 33),
    SKJWK = serialize(jwk, SK),
    ?assertEqual(serialize(raw, deserializePrivate(jwk, SKJWK)), SKRaw),
    PKJWK = serialize(jwk, PK),
    ?assertEqual(serialize(raw, deserializePublic(jwk, PKJWK)), PKRaw).
