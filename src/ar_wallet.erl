-module(ar_wallet).
-export([new/0, sign/2, verify/3]).
-define(PUBLIC_EXPNT, 17489).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Utilities for manipulating wallets.

%% @doc Generate a new wallet public key and private key.
new() ->
	{[_, Pub], [_, Pub, Priv|_]} = {[_, Pub], [_, Pub, Priv|_]} = crypto:generate_key(?SIGN_ALG, {?PRIV_KEY_SZ, ?PUBLIC_EXPNT}),
	{{Priv, Pub}, Pub}.

%% @doc Sign some data with a private key.
sign({Priv, Pub}, Data) ->
	crypto:sign(
		?SIGN_ALG,
		?HASH_ALG,
		Data,
		[generate_exponent_binary(), Pub, Priv]
	).

%% @doc Verify that a signature is correct.
verify(Key, Data, Sig) ->
	crypto:verify(
		?SIGN_ALG,
		?HASH_ALG,
		Data,
		Sig,
		[generate_exponent_binary(), Key]
	).

%% @doc Regenerate the binary encoded public exponent that crypto:generate_key/2
%% returns.
generate_exponent_binary() -> binary:encode_unsigned(?PUBLIC_EXPNT).

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
