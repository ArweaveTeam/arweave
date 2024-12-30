-module(ar_wallet_tests).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

wallet_sign_verify_test_() ->
	{timeout, 30, fun test_wallet_sign_verify/0}.

test_wallet_sign_verify() ->
	TestWalletSignVerify = fun(KeyType) ->
		fun() ->
			{Priv, Pub} = ar_wallet:new(KeyType),
			TestData = <<"TEST DATA">>,
			Signature = ar_wallet:sign(Priv, TestData),
			true = ar_wallet:verify(Pub, TestData, Signature)
		end
	end,
	[
		{"RSA_65537", TestWalletSignVerify({?RSA_SIGN_ALG, 65537})},
		{"EC_SECP256K1", TestWalletSignVerify({?ECDSA_SIGN_ALG, secp256k1})}
	].

invalid_signature_test_() ->
    TestInvalidSignature = fun(KeyType) ->
        fun() ->
			{Priv, Pub} = ar_wallet:new(KeyType),
           	TestData = <<"TEST DATA">>,
			<< _:32, Signature/binary >> = ar_wallet:sign(Priv, TestData),
			false = ar_wallet:verify(Pub, TestData, << 0:32, Signature/binary >>)
        end
    end,
    [
        {"RSA_65537", TestInvalidSignature({?RSA_SIGN_ALG, 65537})},
		{"EC_SECP256K1", TestInvalidSignature({?ECDSA_SIGN_ALG, secp256k1})}
    ].

%% @doc Check generated keyfiles can be retrieved.
generate_keyfile_test_() ->
	GenerateKeyFile = fun(KeyType) ->
		fun() ->
			{{_, PrivateKey, _}, {_, Identifier} = Pub} = ar_wallet:new_keyfile(KeyType),
			FileName = ar_wallet:wallet_filepath(ar_util:encode(ar_wallet:to_address(Identifier))),
			{{_, Priv, _}, Pub} = ar_wallet:load_keyfile(FileName),
			?assertEqual(ar_wallet:serialize(raw, Priv), ar_wallet:serialize(raw, PrivateKey))
		end
	end,
	[
		{"RSA_65537", GenerateKeyFile({?RSA_SIGN_ALG, 65537})},
		{"EC_SECP256K1", GenerateKeyFile({?ECDSA_SIGN_ALG, secp256k1})}
	].

load_keyfile_test_() ->
    TestLoadKeyfile = fun(Path) ->
        fun() ->
            {Priv, Pub} = ar_wallet:load_keyfile(wallet_fixture_path(Path)),
            TestData = <<"TEST DATA">>,
            Signature = ar_wallet:sign(Priv, TestData),
            true = ar_wallet:verify(Pub, TestData, Signature)
        end
    end,
    [
        {"PS256_65537", TestLoadKeyfile(<<"PS256_65537">>)},
        {"ES256K", TestLoadKeyfile(<<"ES256K">>)}
    ].

wallet_fixture_path(KeyTypeEnc) ->
	{ok, Cwd} = file:get_cwd(),
	filename:join(Cwd, "./apps/arweave/test/ar_wallet_tests_" ++ binary_to_list(KeyTypeEnc) ++ "_fixture.json").
