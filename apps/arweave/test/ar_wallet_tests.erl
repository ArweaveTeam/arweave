-module(ar_wallet_tests).

-include_lib("eunit/include/eunit.hrl").

wallet_sign_verify_test_() ->
	TestWalletSignVerify = fun(KeyTypeEnc) ->
		fun() ->
			KeyType = ar_serialize:binary_to_signature_type(KeyTypeEnc),
			{Priv, Pub} = ar_wallet:new(KeyType),
			TestData = <<"TEST DATA">>,
			Signature = ar_wallet:sign(Priv, TestData),
			true = ar_wallet:verify(Pub, TestData, Signature)
		end
	end,
	[
		{"rsa_pss_65537", TestWalletSignVerify(<<"rsa_pss_65537">>)},
		{"ecdsa_secp256k1", TestWalletSignVerify(<<"ecdsa_secp256k1">>)},
		{"eddsa_ed25519", TestWalletSignVerify(<<"eddsa_ed25519">>)}
	].

invalid_signature_test_() ->
    TestInvalidSignature = fun(KeyTypeEnc) ->
        fun() ->
			KeyType = ar_serialize:binary_to_signature_type(KeyTypeEnc),
			{Priv, Pub} = ar_wallet:new(KeyType),
           	TestData = <<"TEST DATA">>,
			<< _:32, Signature/binary >> = ar_wallet:sign(Priv, TestData),
			false = ar_wallet:verify(Pub, TestData, << 0:32, Signature/binary >>)
        end
    end,
    [
        {"rsa_pss_65537", TestInvalidSignature(<<"rsa_pss_65537">>)},
        {"ecdsa_secp256k1", TestInvalidSignature(<<"ecdsa_secp256k1">>)},
		{"eddsa_ed25519", TestInvalidSignature(<<"eddsa_ed25519">>)}
    ].

%% @doc Check generated keyfiles can be retrieved.
generate_keyfile_test_() ->
	GenerateKeyFile = fun(KeyTypeEnc) ->
		fun() ->
			KeyType = ar_serialize:binary_to_signature_type(KeyTypeEnc),
			{Priv, Pub} = ar_wallet:new_keyfile(KeyType, wallet_address),
			FileName = ar_wallet:wallet_filepath(ar_util:encode(ar_wallet:to_address(Pub))),
			{Priv, Pub} = ar_wallet:load_keyfile(FileName)
		end
	end,
	[
		{"rsa_pss_65537", GenerateKeyFile(<<"rsa_pss_65537">>)},
		{"ecdsa_secp256k1", GenerateKeyFile(<<"ecdsa_secp256k1">>)},
		{"eddsa_ed25519", GenerateKeyFile(<<"eddsa_ed25519">>)}
	].

load_keyfile_test_() ->
    TestLoadKeyfile = fun(KeyTypeEnc) ->
        fun() ->
            {Priv, Pub = {KeyType, _}} = ar_wallet:load_keyfile(wallet_fixture_path(KeyTypeEnc)),
            KeyType = ar_serialize:binary_to_signature_type(KeyTypeEnc),
            TestData = <<"TEST DATA">>,
            Signature = ar_wallet:sign(Priv, TestData),
            true = ar_wallet:verify(Pub, TestData, Signature)
        end
    end,
    [
        {"rsa_pss_65537", TestLoadKeyfile(<<"rsa_pss_65537">>)},
        {"ecdsa_secp256k1", TestLoadKeyfile(<<"ecdsa_secp256k1">>)},
        {"eddsa_ed25519", TestLoadKeyfile(<<"eddsa_ed25519">>)}
    ].

%% @doc Ensure that to_address'ing twice does not result in double hashing.
address_double_encode_test() ->
	{_, Pub} = ar_wallet:new(),
	Addr = ar_wallet:to_address(Pub),
	Addr = ar_wallet:to_address(Addr).

wallet_fixture_path(KeyTypeEnc) ->
	Dir = filename:dirname(?FILE),
	filename:join(Dir, "ar_wallet_tests_" ++ binary_to_list(KeyTypeEnc) ++ "_fixture.json").
