-module(ar_wallet_tests).

-include_lib("eunit/include/eunit.hrl").

load_keyfile_test_() ->
    TestLoadKeyfile = fun(KeyTypeEnc) ->
        fun() ->
            {Priv, Pub = {KeyType, _}} = ar_wallet:load_keyfile(wallet_fixture_path(KeyTypeEnc)),
            KeyType = ar_serialize:list_to_signature_type(KeyTypeEnc),
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

wallet_fixture_path(KeyTypeEnc) ->
	Dir = filename:dirname(?FILE),
	filename:join(Dir, "ar_wallet_tests_" ++ binary_to_list(KeyTypeEnc) ++ "_fixture.json").
