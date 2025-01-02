%%% @doc Utilities for manipulating wallets.
-module(ar_wallet).

-export([new/0, new/1, new_keyfile/0, new_keyfile/1, new_keyfile/2,
		serialize/2, load_key/1, load_keyfile/1, wallet_filepath/1, get_or_create_wallet/1,
		to_address/1, to_address/2, identifier/1, from_identifier/1, identifier_to_type/1,
		sign/2, verify/3, verify_pre_fork_2_4/3,
		base64_address_with_optional_checksum_to_decoded_address/1,
		base64_address_with_optional_checksum_to_decoded_address_safe/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-include_lib("public_key/include/public_key.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Generate a new wallet public key and private key.
new() ->
	new(?DEFAULT_KEY_TYPE).

new({?RSA_SIGN_ALG, 65537} = KeyType) ->
	PrivateKey = rsa_pss:new(?RSA_PRIV_KEY_SZ),
	PrivateExponent = rsa_pss:serialize(raw, PrivateKey),
	Identifier = identifier(to_public(PrivateKey)),
	{{KeyType, PrivateExponent, Identifier}, {KeyType, Identifier}};
new(KeyType) ->
    PrivateKey = case KeyType of
		{?ECDSA_SIGN_ALG, secp256k1} -> ec_secp256k1:new();
		{?EDDSA_SIGN_ALG, ed25519} -> ec_secp256k1:new()
	end,
	Identifier = identifier(to_public(PrivateKey)),
	{{KeyType, PrivateKey, Identifier}, {KeyType, Identifier}}.

new_keyfile() ->
    new_keyfile(?DEFAULT_KEY_TYPE).

new_keyfile(KeyType) ->
    new_keyfile(KeyType, local_access).

%% @doc Generate a new wallet public and private key, with a corresponding keyfile.
%% The provided key is used as part of the file name.
new_keyfile({?RSA_SIGN_ALG, 65537} = KeyType, AccessMode) ->
	PrivateKey = rsa_pss:new(?RSA_PRIV_KEY_SZ),
	JWK = serialize(jwk, PrivateKey),
	PrivateExponent = rsa_pss:serialize(raw, PrivateKey),
	Identifier = identifier(to_public(PrivateKey)),
	Filename = wallet_filepath(AccessMode, Identifier),
	Wallet = {{KeyType, PrivateExponent, Identifier}, {KeyType, Identifier}},
	create_keyfile(Wallet, Filename, JWK);
new_keyfile(KeyType, AccessMode) ->
	{{_, PrivateKey, Identifier}, _} = Wallet = new(KeyType),
	JWK = serialize(jwk, PrivateKey),
	Filename = wallet_filepath(AccessMode, Identifier),
	create_keyfile(Wallet, Filename, JWK).

%% @doc Convert PrivateKey to PublicKey.
-spec to_public(Key :: public_key:rsa_private_key() | public_key:ecdsa_private_key()) -> public_key:rsa_public_key() | public_key:ecdsa_public_key().
to_public(#'RSAPrivateKey'{} = PrivateKey) ->
	rsa_pss:to_public(PrivateKey);
to_public(#'ECPrivateKey'{} = PrivateKey) ->
	ec_secp256k1:to_public(PrivateKey).

%% @doc Serialize PrivateKey or PublicKey
-spec serialize(Format :: raw | jwk, Key :: public_key:rsa_private_key() | public_key:ecdsa_private_key() | public_key:rsa_public_key() | public_key:ecdsa_public_key()) -> binary().
serialize(Format, #'RSAPrivateKey'{} = PrivateKey) ->
	rsa_pss:serialize(Format, PrivateKey);
serialize(Format, #'RSAPublicKey'{} = PublicKey) ->
	rsa_pss:serialize(Format, PublicKey);
serialize(Format, #'ECPrivateKey'{} = PrivateKey) ->
	ec_secp256k1:serialize(Format, PrivateKey);
serialize(Format, {#'ECPoint'{}, {namedCurve, secp256k1}} = PublicKey) ->
	ec_secp256k1:serialize(Format, PublicKey);
%% backward compatibility for RSA keys
serialize(raw, Key) when is_binary(Key) ->
	Key.

%% @doc Convert PrivateKey to PublicKey.
-spec identifier(PublicKey :: public_key:rsa_public_key() | public_key:ecdsa_public_key()) -> binary().
identifier(#'RSAPublicKey'{} = PublicKey) ->
	rsa_pss:identifier(PublicKey);
identifier({#'ECPoint'{}, {namedCurve, secp256k1}} = PublicKey) ->
	ec_secp256k1:identifier(PublicKey).

%% @doc All identifiers must have an odd byte_size,
%% except for RSA keys where the Identifier is the raw modulus bytes.
identifier_to_type(Identifier) when byte_size(Identifier) rem 2 == 0 ->
	{?RSA_SIGN_ALG, 65537};
identifier_to_type(Identifier)->
	<<TypeByte:8, _/binary>> = Identifier,
	case TypeByte of
		2 -> {?ECDSA_SIGN_ALG, secp256k1};
		_ -> {error, invalid_prefix, TypeByte}
	end.

%% @doc Generate a public address from Key's Identifer.
%% Public addresses are SHA256(Identifier).
to_address({{_, _, _}, {_, Identifier}}) ->
	to_address(Identifier);
to_address({_, Identifier}) ->
	to_address(Identifier);
to_address({_, _, Identifier}) ->
	to_address(Identifier);
to_address(Identifer) ->
	crypto:hash(?HASH_ALG, Identifer).

%% Small keys are not secure, nobody is using them, the clause
%% is for backwards-compatibility.
to_address(PubKey, {?RSA_SIGN_ALG, 65537}) when bit_size(PubKey) == 256 ->
	PubKey;
to_address(PubKey, {?RSA_SIGN_ALG, 65537}) ->
	to_address(PubKey).


wallet_filepath(WalletName) ->
	{ok, Config} = application:get_env(arweave, config),
	Filename = lists:flatten(["arweave_keyfile_", binary_to_list(WalletName), ".json"]),
	filename:join([Config#config.data_dir, ?WALLET_DIR, Filename]).

%% @doc Read the keyfile for the key with the given address from disk.
%% Return not_found if arweave_keyfile_[addr].json or [addr].json is not found
%% in [data_dir]/?WALLET_DIR.
load_key(Addr) ->
	Path = wallet_filepath(ar_util:encode(Addr)),
	case filelib:is_file(Path) of
		false ->
			Path2 = wallet_filepath2(ar_util:encode(Addr)),
			case filelib:is_file(Path2) of
				false ->
					not_found;
				true ->
					load_keyfile(Path2)
			end;
		true ->
			load_keyfile(Path)
	end.

%% @doc Extract the public and private key from a keyfile.
load_keyfile(File) ->
	{ok, Body} = file:read_file(File),
	{Key} = ar_serialize:dejsonify(Body),
	case lists:keyfind(<<"kty">>, 1, Key) of
		{<<"kty">>, <<"EC">>} ->
			KeyType = {?ECDSA_SIGN_ALG, secp256k1},
			{<<"d">>, PrivB64} = lists:keyfind(<<"d">>, 1, Key),
			PrivBytes = ar_util:decode(PrivB64),
			PrivateKey = ec_secp256k1:deserializePrivate(raw, PrivBytes),
			Identifier = identifier(to_public(PrivateKey)),
			{{KeyType, PrivateKey, Identifier}, {KeyType, Identifier}};
		{<<"kty">>, <<"RSA">>} ->
			KeyType = {?RSA_SIGN_ALG, 65537},
			{<<"n">>, ModulusB64} = lists:keyfind(<<"n">>, 1, Key),
			{<<"d">>, PrivateExpB64} = lists:keyfind(<<"d">>, 1, Key),
			M = ar_util:decode(ModulusB64),
			PrivateExponent = ar_util:decode(PrivateExpB64),
			PrivateKey = #'RSAPrivateKey'{
				modulus = binary:decode_unsigned(M),
				publicExponent = 65537,
				privateExponent = binary:decode_unsigned(PrivateExponent)
			},
			Identifier = identifier(to_public(PrivateKey)),
			{{KeyType, PrivateExponent, Identifier}, {KeyType, Identifier}}
	end.


%% @doc Sign some data with a private key.
sign({{KeyAlg, PublicExpnt}, Priv, Pub}, Data)
		when KeyAlg =:= ?RSA_SIGN_ALG andalso PublicExpnt =:= 65537 ->
	rsa_pss:sign(
		Data,
		sha256,
		#'RSAPrivateKey'{
			publicExponent = PublicExpnt,
			modulus = binary:decode_unsigned(Pub),
			privateExponent = binary:decode_unsigned(Priv)
		}
	);
sign({_, #'ECPrivateKey'{} = PrivateKey, _}, Data)->
	ec_secp256k1:sign(Data, sha256, PrivateKey).

%% @doc Verify that a signature is correct.
verify({_, Identifier}, Data, Sig) when is_binary(Identifier) ->
	verify(Identifier, Data, Sig);
verify(Identifier, Data, Sig) when is_binary(Identifier) ->
	case identifier_to_type(Identifier) of
		{?RSA_SIGN_ALG, 65537} ->
			rsa_pss:verify(
				Data, sha256, Sig, rsa_pss:from_identifier(Identifier));
		{?ECDSA_SIGN_ALG, secp256k1} ->
			ec_secp256k1:verify(Data, sha256, Sig, ec_secp256k1:from_identifier(Identifier));
		{invalid_prefix, _} -> invalid_identifier
	end;
verify({_, {#'ECPoint'{}, {namedCurve, secp256k1}} = PublicKey}, Data, Sig) ->
	ec_secp256k1:verify(Data, sha256, Sig, PublicKey).

-spec from_identifier(Identifier :: binary()) -> public_key:rsa_public_key() | public_key:ecdsa_public_key().
from_identifier(Identifier) ->
	case identifier_to_type(Identifier) of
		{?RSA_SIGN_ALG, 65537} -> Identifier;
		{?ECDSA_SIGN_ALG, secp256k1} -> ec_secp256k1:from_identifier(Identifier);
		{invalid_prefix, _} -> invalid_identifier
	end.


%% @doc Verify that a signature is correct. The function was used to verify
%% transactions until the fork 2.4. It rejects a valid transaction when the
%% key modulus bit size is less than 4096. The new method (verify/3) successfully
%% verifies all the historical transactions so this function is not used anywhere
%% after the fork 2.4.
verify_pre_fork_2_4({{KeyAlg, PublicExpnt}, Pub}, Data, Sig)
		when KeyAlg =:= ?RSA_SIGN_ALG andalso PublicExpnt =:= 65537 ->
	rsa_pss:verify_legacy(
		Data,
		sha256,
		Sig,
		#'RSAPublicKey'{
			publicExponent = PublicExpnt,
			modulus = binary:decode_unsigned(Pub)
		}
	).

base64_address_with_optional_checksum_to_decoded_address(AddrBase64) ->
	Size = byte_size(AddrBase64),
	case Size > 7 of
		false ->
			ar_util:decode(AddrBase64);
		true ->
			case AddrBase64 of
				<< MainBase64url:(Size - 7)/binary, ":", ChecksumBase64url:6/binary >> ->
					AddrDecoded = ar_util:decode(MainBase64url),
					case byte_size(AddrDecoded) < 20 of
						true -> throw({error, invalid_address});
						false -> ok
					end,
					case byte_size(AddrDecoded) > 64 of
						true -> throw({error, invalid_address});
						false -> ok
					end,
					Checksum = ar_util:decode(ChecksumBase64url),
					case decoded_address_to_checksum(AddrDecoded) =:= Checksum of
						true -> AddrDecoded;
						false -> throw({error, invalid_address_checksum})
					end;
				_ ->
					ar_util:decode(AddrBase64)
			end
	end.

base64_address_with_optional_checksum_to_decoded_address_safe(AddrBase64)->
	try
		D = base64_address_with_optional_checksum_to_decoded_address(AddrBase64),
		{ok, D}
	catch
		_:_ ->
			{error, invalid}
	end.

%% @doc Read a wallet of one of the given types from disk. Files modified later are prefered.
%% If no file is found, create one of the type standing first in the list.
get_or_create_wallet(Types) ->
	{ok, Config} = application:get_env(arweave, config),
	WalletDir = filename:join(Config#config.data_dir, ?WALLET_DIR),
	Entries =
		lists:reverse(lists:sort(filelib:fold_files(
			WalletDir,
			"(.*\\.json$)",
			false,
			fun(F, Acc) ->
				 [{filelib:last_modified(F), F} | Acc]
			end,
			[])
		)),
	get_or_create_wallet(Entries, Types).

get_or_create_wallet([], [Type | _]) ->
	ar_wallet:new_keyfile(Type);
get_or_create_wallet([{_LastModified, F} | Entries], Types) ->
	{{Type, _, _}, _} = W = load_keyfile(F),
	case lists:member(Type, Types) of
		true ->
			W;
		false ->
			get_or_create_wallet(Entries, Types)
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

decoded_address_to_checksum(AddrDecoded) ->
	Crc = erlang:crc32(AddrDecoded),
	<< Crc:32 >>.

decoded_address_to_base64_address_with_checksum(AddrDecoded) ->
	Checksum = decoded_address_to_checksum(AddrDecoded),
	AddrBase64 = ar_util:encode(AddrDecoded),
	ChecksumBase64 = ar_util:encode(Checksum),
	<< AddrBase64/binary, ":", ChecksumBase64/binary >>.

wallet_filepath(WalletAccessCode, Identifier) ->
	WalletName = case WalletAccessCode of
		local_access -> ar_util:encode(to_address(Identifier));
		_ -> WalletAccessCode
	end,
	wallet_filepath(WalletName).
wallet_filepath2(WalletName) ->
	{ok, Config} = application:get_env(arweave, config),
	Filename = lists:flatten([binary_to_list(WalletName), ".json"]),
	filename:join([Config#config.data_dir, ?WALLET_DIR, Filename]).

create_keyfile(Wallet, Filename, Content) ->
	case filelib:ensure_dir(Filename) of
		ok ->
			case ar_storage:write_file_atomic(Filename, Content) of
				ok ->
					Wallet;
				Error2 ->
					Error2
			end;
		Error ->
			Error
	end.
%%%===================================================================
%%% Tests.
%%%===================================================================

wallet_sign_verify_test() ->
	TestData = <<"TEST DATA">>,
	{Priv, Pub} = new(),
	Signature = sign(Priv, TestData),
	true = verify(Pub, TestData, Signature).

invalid_signature_test() ->
	TestData = <<"TEST DATA">>,
	{Priv, Pub} = new(),
	<< _:32, Signature/binary >> = sign(Priv, TestData),
	false = verify(Pub, TestData, << 0:32, Signature/binary >>).

%% @doc Check generated keyfiles can be retrieved.
generate_keyfile_test() ->
	{Priv, Pub} = new_keyfile(),
	FileName = wallet_filepath(ar_util:encode(to_address(Pub))),
	{Priv, Pub} = load_keyfile(FileName).

checksum_test() ->
	{_, Pub} = new(),
	Addr = to_address(Pub),
	AddrBase64 = ar_util:encode(Addr),
	AddrBase64Wide = decoded_address_to_base64_address_with_checksum(Addr),
	Addr = base64_address_with_optional_checksum_to_decoded_address(AddrBase64Wide),
	Addr = base64_address_with_optional_checksum_to_decoded_address(AddrBase64),
	%% 64 bytes, for future.
	CorrectLongAddress = <<"0123456789012345678901234567890123456789012345678901234567890123">>,
	CorrectCheckSum = decoded_address_to_checksum(CorrectLongAddress),
	CorrectLongAddressBase64 = ar_util:encode(CorrectLongAddress),
	CorrectCheckSumBase64 = ar_util:encode(CorrectCheckSum),
	CorrectLongAddressWithChecksumBase64 = <<CorrectLongAddressBase64/binary, ":", CorrectCheckSumBase64/binary>>,
	case catch base64_address_with_optional_checksum_to_decoded_address(CorrectLongAddressWithChecksumBase64) of
		{error, _} -> throw({error, correct_long_address_should_bypass});
		_ -> ok
	end,
	%% 65 bytes.
	InvalidLongAddress = <<"01234567890123456789012345678901234567890123456789012345678901234">>,
	InvalidLongAddressBase64 = ar_util:encode(InvalidLongAddress),
	case catch base64_address_with_optional_checksum_to_decoded_address(<<InvalidLongAddressBase64/binary, ":MDA">>) of
		{'EXIT', _} -> ok
	end,
	%% 100 bytes.
	InvalidLongAddress2 = <<"0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789">>,
	InvalidLongAddress2Base64 = ar_util:encode(InvalidLongAddress2),
	case catch base64_address_with_optional_checksum_to_decoded_address(<<InvalidLongAddress2Base64/binary, ":MDA">>) of
		{'EXIT', _} -> ok
	end,
	%% 10 bytes
	InvalidShortAddress = <<"0123456789">>,
	InvalidShortAddressBase64 = ar_util:encode(InvalidShortAddress),
	case catch base64_address_with_optional_checksum_to_decoded_address(<<InvalidShortAddressBase64/binary, ":MDA">>) of
		{'EXIT', _} -> ok
	end,
	InvalidChecksum = ar_util:encode(<< 0:32 >>),
	case catch base64_address_with_optional_checksum_to_decoded_address(
			<< AddrBase64/binary, ":", InvalidChecksum/binary >>) of
		{error, invalid_address_checksum} -> ok
	end,
	case catch base64_address_with_optional_checksum_to_decoded_address(<<":MDA">>) of
		{'EXIT', _} -> ok
	end.
