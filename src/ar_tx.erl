-module(ar_tx).
-export([new/0, new/1, new/2, new/3, new/4, sign/2, sign/3, to_binary/1, verify/1, verify_txs/1]).
-export([calculate_min_tx_cost/1, tx_cost_above_min/1]).
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
	#tx { id = generate_id(), last_tx = Last, type = data, data = Data, reward = Reward }.
new(Dest, Reward, Qty, Last) when bit_size(Dest) == ?HASH_SZ ->
	#tx {
		id = generate_id(),
		last_tx = Last,
		type = transfer,
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
verify(#tx { signature = <<>> }) -> true;
verify(TX) ->
	ar_wallet:verify(TX#tx.owner, to_binary(TX), TX#tx.signature) and tx_cost_above_min(TX).
-else.
verify(TX) ->
	ar_wallet:verify(TX#tx.owner, to_binary(TX), TX#tx.signature) and tx_cost_above_min(TX).
-endif.

verify_txs(TXs) ->
	false = lists:any(fun(T) -> not verify(T) end, TXs).

%% @doc Transaction cost above proscribed minimum.
tx_cost_above_min(TX) ->
	TX#tx.reward >= calculate_min_tx_cost(byte_size(to_binary(TX))).

%% @doc Calculate the minimum cost for this transaction. (Size in Bytes)
calculate_min_tx_cost(Size) ->
	% 1 AR per mb + 1 winston (in winstons).
	1 + erlang:trunc((Size / (1024 * 1024)) * ?WINSTON_PER_AR).

%% @doc Ensure that all TXs in a list verify correctly.

%%% TESTS %%%

sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	true = verify(sign(NewTX, Priv, Pub)).

forge_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	false = verify((sign(NewTX, Priv, Pub))#tx { data = <<"FAKE DATA">> }).

calculate_min_tx_cost_test() ->
	TestTX = new(<<"TEST DATA">>, ?AR(10)),
	Size = byte_size(to_binary(TestTX)),
	% Result hand calculated.
	40054322 = calculate_min_tx_cost(Size).

tx_cost_above_min_test() ->
	TestTX = new(<<"TEST DATA">>, ?AR(10)),
	true = tx_cost_above_min(TestTX).
