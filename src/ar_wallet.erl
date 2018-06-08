-module(ar_wallet).
-export([new/0, sign/2, verify/3, to_address/1, new_keyfile/0, load_keyfile/1, to_binary/1]).
-define(PUBLIC_EXPNT, 65537).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

%%% Utilities for manipulating wallets.

%% @doc Generate a new wallet public key and private key.
new() ->
	{[_, Pub], [_, Pub, Priv|_]} = {[_, Pub], [_, Pub, Priv|_]}
		= crypto:generate_key(?SIGN_ALG, {?PRIV_KEY_SZ, ?PUBLIC_EXPNT}),
	{{Priv, Pub}, Pub}.


%% @doc Generates a new wallet public and private key, with a corresponding keyfile
new_keyfile() ->
	{[Expnt, Pub], [Expnt, Pub, Priv, P1, P2, E1, E2, C]} =
		crypto:generate_key(rsa, {?PRIV_KEY_SZ, ?PUBLIC_EXPNT}),
		Key = 
			ar_serialize:jsonify(
				{
					[
						{kty, <<"RSA">>},
						{ext, true},
						{e, ar_util:encode(Expnt)},
						{n, ar_util:encode(Pub)},
						{d, ar_util:encode(Priv)},
						{p, ar_util:encode(P1)},
						{q, ar_util:encode(P2)},
						{dp, ar_util:encode(E1)},
						{dq, ar_util:encode(E2)},
						{qi, ar_util:encode(C)}
					]
				}
			),
		FileName = "wallets/arweave_keyfile_" ++ binary_to_list(ar_util:encode(to_address(Pub))) ++ ".json",
		filelib:ensure_dir(FileName),
		file:write_file(FileName, Key),
	{{Priv, Pub}, Pub}.

%% @doc Extracts the public and private key from a keyfile
load_keyfile(File) ->
	{ok, Body} = file:read_file(File),
	{Key} = ar_serialize:dejsonify(Body),
	{<<"n">>, PubEncoded} = lists:keyfind(<<"n">>, 1, Key),
	Pub = ar_util:decode(PubEncoded),
	{<<"d">>, PrivEncoded} = lists:keyfind(<<"d">>, 1, Key),
	Priv = ar_util:decode(PrivEncoded),
	{{Priv, Pub}, Pub}.

%% @doc Sign some data with a private key.
sign({Priv, Pub}, Data) ->
	rsa_pss:sign(
		Data,
		sha256,
		#'RSAPrivateKey'{
			publicExponent = ?PUBLIC_EXPNT,
			modulus = binary:decode_unsigned(Pub),
			privateExponent = binary:decode_unsigned(Priv)
		}
	).


%% @doc Verify that a signature is correct.
verify(Key, Data, Sig) ->
	rsa_pss:verify(
		Data,
		sha256,
		Sig,
		#'RSAPublicKey'{
			publicExponent = ?PUBLIC_EXPNT,
			modulus = binary:decode_unsigned(Key)
		}
	).

%% @doc Generate an address from a public key.
to_address(Addr) when ?IS_ADDR(Addr) -> Addr;
to_address({{_, Pub}, Pub}) -> to_address(Pub);
to_address({_, Pub}) -> to_address(Pub);
to_address(PubKey) ->
    crypto:hash(?HASH_ALG, PubKey).

to_binary({Addr, Quantity, LastTx}) ->
    <<
        (Addr)/binary,
        (integer_to_binary(Quantity))/binary,
        (LastTx)/binary
    >>.


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

%% @doc Ensure that to_address'ing twice does not result in double hashing.
address_double_encode_test() ->
	{_, Pub} = new(),
	Addr = to_address(Pub),
	Addr = to_address(Addr).

%%doc Check generated keyfiles can be retrieved
generate_keyfile_test() ->
	{Priv, Pub} = new_keyfile(),
	FileName = "wallets/arweave_keyfile_" ++ binary_to_list(ar_util:encode(to_address(Pub))) ++ ".json",
	{Priv, Pub} = load_keyfile(FileName).

%% @doc Check keyfile generation
assign_wallet_test() ->
	{_, Pub} = new_keyfile(),
	Address = to_address(Pub),
	B0 = ar_weave:init([{Address, ?AR(0), <<>>}]),
	Node1 = ar_node:start([], B0, 0, Address),
	ar_node:mine(Node1), % Mine B1
	receive after 500 -> ok end,
	Reward = erlang:trunc(ar_node:calculate_reward(1, 0)),
	Reward = ar_node:get_balance(Node1, Pub).