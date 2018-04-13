-module(ar_tx).
-export([new/0, new/1, new/2, new/3, new/4, sign/2, sign/3, to_binary/1, verify/2, verify_txs/2]).
-export([calculate_min_tx_cost/2, tx_cost_above_min/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Transaction creation, signing and verification for Archain.

%% @doc Generate a new transaction for entry onto a weave.
new() ->
	#tx { id = generate_id() }.
new(Data) ->
	#tx { id = generate_id(), data = Data }.
new(Data, Reward) ->
	#tx { id = generate_id(), data = Data, reward = Reward }.
new(Data, Reward, Last) ->
	#tx { id = generate_id(), last_tx = Last, data = Data, reward = Reward }.
new(Dest, Reward, Qty, Last) when bit_size(Dest) == ?HASH_SZ ->
	#tx {
		id = generate_id(),
		last_tx = Last,
		quantity = Qty,
		target = Dest,
		data = <<>>,
		reward = Reward
	};
new(Dest, Reward, Qty, Last) ->
	% Convert wallets to addresses before building transactions.
	new(ar_wallet:to_address(Dest), Reward, Qty, Last).

%% @doc Create an ID for an object on the weave.
generate_id() -> crypto:strong_rand_bytes(32).

%% @doc Generate a hashable binary from a #tx object.
to_binary(T) ->
	<<
		(T#tx.owner)/binary,
		(T#tx.target)/binary,
		(T#tx.id)/binary,
		(T#tx.data)/binary,
		(list_to_binary(integer_to_list(T#tx.quantity)))/binary,
		(list_to_binary(integer_to_list(T#tx.reward)))/binary,
		(T#tx.last_tx)/binary
	>>.

%% @doc Sign ('claim ownership') of a transaction. After it is signed, it can be
%% placed onto a block and verified at a later date.
sign(TX, {PrivKey, PubKey}) -> sign(TX, PrivKey, PubKey).
sign(TX, PrivKey, PubKey) ->
	NewTX = TX#tx{ owner = PubKey },
	NewTX#tx {
		signature = ar_wallet:sign(PrivKey, to_binary(NewTX))
	}.

%% @doc Ensure that a transaction's signature is valid.
%% TODO: Ensure that DEBUG is false in production releases(!!!)
-ifdef(DEBUG).
verify(#tx { signature = <<>> }, _) -> true;
verify(TX, Diff) ->
	ar:d(ar_wallet:verify(TX#tx.owner, to_binary(TX), TX#tx.signature)) and
	ar:d(tx_cost_above_min(TX, Diff)) and
	ar:d(tx_field_size_limit(TX)).
-else.
verify(TX, Diff) ->
	ar:d(ar_wallet:verify(TX#tx.owner, to_binary(TX), TX#tx.signature)) and
	ar:d(tx_cost_above_min(TX, Diff)) and
	ar:d(tx_field_size_limit(TX)).
-endif.

%% @doc Ensure that all TXs in a list verify correctly.
verify_txs([], _) ->
	true;
verify_txs(TXs, Diff) ->
	false = lists:any(fun(T) -> not verify(T, Diff) end, TXs).

%% @doc Transaction cost above proscribed minimum.
tx_cost_above_min(TX, Diff) ->
	TX#tx.reward >= calculate_min_tx_cost(byte_size(TX#tx.data), Diff).

%Calculate the minimum transaction cost for a TX with data size Size
%the constant 3208 is the max byte size of each of the other fields
calculate_min_tx_cost(Size, Diff) ->
	((Size+3208) * ?COST_PER_BYTE * ?DIFF_CENTER) div Diff.

tx_field_size_limit(TX) ->
	(byte_size(TX#tx.id) =< 32) and
	(byte_size(TX#tx.last_tx) =< 32) and
	(byte_size(TX#tx.owner) =< 512) and
	(byte_size(list_to_binary(TX#tx.tags)) =< 2048) and
	(byte_size(TX#tx.target) =< 32) and
	(byte_size(integer_to_binary(TX#tx.quantity)) =< 21) and
	(byte_size(TX#tx.signature) =< 512) and
	(byte_size(integer_to_binary(TX#tx.reward)) =< 21).


%%% TESTS %%%
%% TODO: Write a more stringent reject_tx_below_min test

sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	true = verify(sign(NewTX, Priv, Pub), 1).

forge_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	false = verify((sign(NewTX, Priv, Pub))#tx { data = <<"FAKE DATA">> }, 1).

tx_cost_above_min_test() ->
	TestTX = new(<<"TEST DATA">>, ?AR(10)),
	true = tx_cost_above_min(TestTX, 1).

reject_tx_below_min_test() ->
	TestTX = new(<<"TEST DATA">>, 1),
	false = tx_cost_above_min(TestTX, 10).
