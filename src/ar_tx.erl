-module(ar_tx).
-export([new/0, new/1, new/2, sign/3, to_binary/1, verify/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Transaction creation, signing and verification for Archain.

%% @doc Generate a new transaction for entry onto a weave.
new() ->
	#tx { id = generate_id() }.
new(Data) ->
	#tx { id = generate_id(), type = data, data = Data }.
new(Dest, Qty) ->
	#tx { id = generate_id(), type = transfer, quantity = Qty, target = Dest, data = <<>> }.

%% @doc Create an ID for an object on the weave.
generate_id() -> crypto:strong_rand_bytes(32).

%% @doc Generate a hashable binary from a #tx object.
to_binary(T) ->
	<< (T#tx.id)/binary, (T#tx.data)/binary, (T#tx.quantity):64 >>.

%% @doc Sign ('claim ownership') of a transaction. After it is signed, it can be
%% placed onto a block and verified at a later date.
sign(TX, PrivKey, PubKey) ->
	TX#tx {
		owner = PubKey,
		signature = crypto:sign(?SIGN_ALG, ?HASH_ALG, to_binary(TX), PrivKey)
	}.

%% @doc Ensure that a transaction's signature is valid.
verify(TX) ->
	crypto:verify(
		rsa,
		sha256,
		to_binary(TX),
		TX#tx.signature,
		TX#tx.owner
	).

sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>),
	{Priv, Pub} = ar_wallet:new(),
	true = verify(sign(NewTX, Priv, Pub)).

forge_test() ->
	NewTX = new(<<"TEST DATA">>),
	{Priv, Pub} = ar_wallet:new(),
	false = verify((sign(NewTX, Priv, Pub))#tx { data = <<"FAKE DATA">> }).
