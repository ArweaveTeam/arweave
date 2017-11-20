-module(ar_tx).
-export([new/0, new/1, new/2, sign/3, to_binary/1, verify/1, verify_txs/1]).
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
	%% TODO: Add tags to signature
	<<
		(T#tx.owner)/binary,
		(T#tx.target)/binary,
		(T#tx.id)/binary,
		(T#tx.data)/binary,
		(list_to_binary(integer_to_list(T#tx.quantity)))/binary
	>>.

%% @doc Sign ('claim ownership') of a transaction. After it is signed, it can be
%% placed onto a block and verified at a later date.
sign(TX, PrivKey, PubKey) ->
	NewTX = TX#tx{ owner = PubKey },
	NewTX#tx {
		signature = ar_wallet:sign(PrivKey, to_binary(NewTX))
	}.

%% @doc Ensure that a transaction's signature is valid.
%% TODO: Ensure that DEBUG is false in production releases(!!!)
-ifdef(DEBUG).
verify(#tx { signature = <<>> }) -> true;
verify(TX) ->
	ar_wallet:verify(TX#tx.owner, to_binary(TX), TX#tx.signature).
-else.
verify(TX) ->
	ar_wallet:verify(TX#tx.owner, to_binary(TX), TX#tx.signature).
-endif.

%% @doc Ensure that all TXs in a list verify correctly.
verify_txs(TXs) ->
	false = lists:any(fun(T) -> not verify(T) end, TXs).

sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>),
	{Priv, Pub} = ar_wallet:new(),
	true = verify(sign(NewTX, Priv, Pub)).

forge_test() ->
	NewTX = new(<<"TEST DATA">>),
	{Priv, Pub} = ar_wallet:new(),
	false = verify((sign(NewTX, Priv, Pub))#tx { data = <<"FAKE DATA">> }).
