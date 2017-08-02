-module(ar_wallet).
-export([new/0]).
-define(PUBLIC_EXPNT, 17489).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Utilities for manipulating wallets.

%% Generate a new wallet public key and private key.
new() ->
	{Pub, Priv} = crypto:generate_key(?SIGN_ALG, {?PRIV_KEY_SZ, ?PUBLIC_EXPNT}),
	{Priv, Pub}.

wallet_sign_verify_test() ->
	TestData = <<"TEST DATA">>,
	{Priv, Pub} = new(),
	Signature = crypto:sign(?SIGN_ALG, ?HASH_ALG, TestData, Priv),
	true = crypto:verify(?SIGN_ALG, ?HASH_ALG, TestData, Signature, Pub).

invalid_signature_test() ->
	TestData = <<"TEST DATA">>,
	{Priv, Pub} = new(),
	<< _:32, Signature/binary >> = crypto:sign(?SIGN_ALG, ?HASH_ALG, TestData, Priv),
	false = crypto:verify(?SIGN_ALG, ?HASH_ALG, TestData, << 0:32, Signature/binary >>, Pub).
