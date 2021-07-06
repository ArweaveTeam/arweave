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
		{"PS256_65537", TestWalletSignVerify(<<"PS256_65537">>)},
		{"ES256K", TestWalletSignVerify(<<"ES256K">>)},
		{"Ed25519", TestWalletSignVerify(<<"Ed25519">>)}
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
        {"PS256_65537", TestInvalidSignature(<<"PS256_65537">>)},
        {"ES256K", TestInvalidSignature(<<"ES256K">>)},
		{"Ed25519", TestInvalidSignature(<<"Ed25519">>)}
    ].

%% @doc Check generated keyfiles can be retrieved.
generate_keyfile_test_() ->
	GenerateKeyFile = fun(KeyTypeEnc) ->
		fun() ->
			KeyType = ar_serialize:binary_to_signature_type(KeyTypeEnc),
			{Priv, Pub} = ar_wallet:new_keyfile(KeyType),
			FileName = ar_wallet:wallet_filepath(ar_util:encode(ar_wallet:to_address(Pub))),
			{Priv, Pub} = ar_wallet:load_keyfile(FileName)
		end
	end,
	[
		{"PS256_65537", GenerateKeyFile(<<"PS256_65537">>)},
		{"ES256K", GenerateKeyFile(<<"ES256K">>)},
		{"Ed25519", GenerateKeyFile(<<"Ed25519">>)}
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
        {"PS256_65537", TestLoadKeyfile(<<"PS256_65537">>)},
        {"ES256K", TestLoadKeyfile(<<"ES256K">>)},
        {"Ed25519", TestLoadKeyfile(<<"Ed25519">>)}
    ].

wallet_fixture_path(KeyTypeEnc) ->
	Dir = filename:dirname(?FILE),
	filename:join(Dir, "ar_wallet_tests_" ++ binary_to_list(KeyTypeEnc) ++ "_fixture.json").
