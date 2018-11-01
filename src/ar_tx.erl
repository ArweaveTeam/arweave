-module(ar_tx).
-export([new/0, new/1, new/2, new/3, new/4]).
-export([sign/2, sign/3, verify/3, verify_txs/3, signature_data_segment/1]).
-export([tx_to_binary/1, tags_to_binary/1, calculate_min_tx_cost/2, calculate_min_tx_cost/4, check_last_tx/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Transaction creation, signing and verification for Arweave.

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

%% @doc A testing function used for giving transactions an ID being placed on the weave.
%% This function is for testing only. A tx with this ID will not pass verification.
%% A valid tx id in the Arweave network is a SHA256 hash of the signature.
generate_id() -> crypto:strong_rand_bytes(32).

%% @doc Generate a hashable binary from a #tx object.
%% NB: This function cannot be used for signature data as the id and signature fields are not set.
tx_to_binary(T) ->
	<<
		(T#tx.id)/binary,
		(T#tx.last_tx)/binary,
		(T#tx.owner)/binary,
		(tags_to_binary(T#tx.tags))/binary,
		(T#tx.target)/binary,
		(list_to_binary(integer_to_list(T#tx.quantity)))/binary,
		(T#tx.data)/binary,
		(T#tx.signature)/binary,
		(list_to_binary(integer_to_list(T#tx.reward)))/binary
	>>.

%% @doc Generate the data segment to be signed for a given TX.
signature_data_segment(T) ->
	<<
		(T#tx.owner)/binary,
		(T#tx.target)/binary,
		(T#tx.data)/binary,
		(list_to_binary(integer_to_list(T#tx.quantity)))/binary,
		(list_to_binary(integer_to_list(T#tx.reward)))/binary,
		(T#tx.last_tx)/binary,
		(tags_to_binary(T#tx.tags))/binary
	>>.

%% @doc Cryptographicvally sign ('claim ownership') of a transaction.
%% After it is signed, it can be placed into a block and verified at a later date.
sign(TX, {PrivKey, PubKey}) -> sign(TX, PrivKey, PubKey).
sign(TX, PrivKey, PubKey) ->
	NewTX = TX#tx{ owner = PubKey },
	Sig = ar_wallet:sign(PrivKey, signature_data_segment(NewTX)),
	ID = crypto:hash(?HASH_ALG, <<Sig/binary>>),
	NewTX#tx {
		signature = Sig, id = ID
	}.

%% @doc Verify whether a transaction is valid to be regossiped and mined on.
%% NB: The signature verification is the last step due to the potential of an attacker
%% submitting an arbitrary length signature field and having it processed.
-ifdef(DEBUG).
verify(#tx { signature = <<>> }, _, _) -> true;
verify(TX, Diff, WalletList) ->
	Checks = [
		{TX#tx.quantity >= 0,
			tx_quantity_positive, "tx_quantity_negative "},
		{tx_cost_above_min(TX, Diff, WalletList),
			tx_above_min_cost, "tx_too_cheap "},
		{tx_field_size_limit(TX),
			tx_field_size_verify, "tx_fields_too_large "},
		{tag_field_legal(TX),
			tx_tag_field_legal, "tag_field_illegally_specified "},
		{check_last_tx(WalletList, TX),
			tx_last_tx_legal, "last_tx_not_valid "},
		{tx_verify_hash(TX),
			tx_verify_hash, "tx_id_not_valid "},
		{ar_wallet_list:validate(ar_wallet_list:apply_txs(WalletList, [TX])),
			tx_wallet_list_valid, "tx_wallet_list_invalid"},
		{ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature),
			tx_wallet_verify, "tx_signature_not_valid "}
	],
	{Result, Report, Reason} = lists:foldl(
		fun({ThisResult, ThisReport, ThisReason}, {AccResult, AccReport, AccReason}) ->
			NewResult = case AccResult of
				false -> false;
				true  -> ThisResult
			end,
			NewReport = [{ThisReport, ThisResult} | AccReport],
			NewReason = case ThisResult of
				false -> [ThisReason | AccReason];
				true  -> AccReason
			end,
			{NewResult, NewReport, NewReason}
		end,
		{true, [{validate_tx, ar_util:encode(TX#tx.id)}], []},
		Checks),
	ar:report(lists:reverse(Report)),
	maybe_store_reason(TX#tx.id, Result, Reason),
	Result.
-else.
verify(TX, Diff, WalletList) ->
	Checks = [
		{TX#tx.quantity >= 0,
			tx_quantity_positive, "tx_quantity_negative "},
		{(ar_wallet:to_address(TX#tx.owner) =/= TX#tx.target), %% not in debug
			tx_owner_not_target, "tx_owner_is_target "},
		{tx_cost_above_min(TX, Diff, WalletList),
			tx_above_min_cost, "tx_too_cheap "},
		{tx_field_size_limit(TX),
			tx_field_size_verify, "tx_fields_too_large "},
		{tag_field_legal(TX),
			tx_tag_field_legal, "tag_field_illegally_specified "},
		{check_last_tx(WalletList, TX),
			tx_last_tx_legal, "last_tx_not_valid "},
		{tx_verify_hash(TX),
			tx_verify_hash, "tx_id_not_valid "},
		{ar_wallet_list:validate(ar_wallet_list:apply_txs(WalletList, [TX])),
			tx_wallet_list_valid, "tx_wallet_list_invalid"},
		{ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature),
			tx_wallet_verify, "tx_signature_not_valid "}
	],
	{Result, Report, Reason} = lists:foldl(
		fun({ThisResult, ThisReport, ThisReason}, {AccResult, AccReport, AccReason}) ->
			NewResult = case AccResult of
				false -> false;
				true  -> ThisResult
			end,
			NewReport = [{ThisReport, ThisResult} | AccReport],
			NewReason = case ThisResult of
				false -> [ThisReason | AccReason];
				true  -> AccReason
			end,
			{NewResult, NewReport, NewReason}
		end,
		{true, [{validate_tx, ar_util:encode(TX#tx.id)}], []},
		Checks),
	% ar:report(lists:reverse(Report)),
	maybe_store_reason(TX#tx.id, Result, Reason),
	Result.
-endif.

maybe_store_reason(ID, false, Reason) ->
	ar_tx_db:put(ID, Reason);
maybe_store_reason(_, true, _) ->
	pass.

%% @doc Verify a list of transactions.
%% Returns true if the entire set verifies otherwise false.
verify_txs([], _, _) ->
	true;
verify_txs([T|TXs], Diff, WalletList) ->
	case verify(T, Diff, WalletList) of
		true  -> verify_txs(TXs, Diff, ar_wallet_list:apply_tx(WalletList, T));
		false -> false
	end.

%% @doc Ensure that transaction cost above prescribed minimum.
tx_cost_above_min(TX, Diff) ->
	TX#tx.reward >=
		(calculate_min_tx_cost(byte_size(TX#tx.data), Diff) + ?WALLET_GEN_FEE).
tx_cost_above_min(TX, Diff, WalletList) ->
	TX#tx.reward >=
		calculate_min_tx_cost(byte_size(TX#tx.data), Diff, WalletList, TX#tx.target).

%% @doc Calculate the minimum transaction cost for a TX with the given data size.
%% The constant 3210 is the max byte size of each of the other fields.
%% Cost per byte is static unless size is bigger than 10mb, at which
%% point cost per byte starts increasing.
calculate_min_tx_cost(DataSize, Diff) ->
	Size = 3210 + DataSize,
	CurveSteepness = 2,
	ThisDiff = erlang:max(Diff, ?DIFF_CENTER),
	BaseCost = CurveSteepness*(Size*?COST_PER_BYTE) / (ThisDiff - (?DIFF_CENTER - CurveSteepness)),
	erlang:trunc(BaseCost * math:pow(1.2, Size/(1024*1024))).
calculate_min_tx_cost(DataSize, Diff, WalletList, Addr) ->
	ar_wallet_list:calculate_tx_gen_fee(WalletList, Addr)
	+ calculate_min_tx_cost(DataSize, Diff).

%% @doc Check whether each field in a transaction is within the given byte size limits.
tx_field_size_limit(TX) ->
	case tag_field_legal(TX) of
		true ->
			(byte_size(TX#tx.id) =< 32) and
			(byte_size(TX#tx.last_tx) =< 32) and
			(byte_size(TX#tx.owner) =< 512) and
			(byte_size(tags_to_binary(TX#tx.tags)) =< 2048) and
			(byte_size(TX#tx.target) =< 32) and
			(byte_size(integer_to_binary(TX#tx.quantity)) =< 21) and
			(byte_size(TX#tx.data) =< 6000000) and
			(byte_size(TX#tx.signature) =< 512) and
			(byte_size(integer_to_binary(TX#tx.reward)) =< 21);
		false -> false
	end.

%% @doc Verify that the transactions ID is a hash of its signature.
tx_verify_hash(#tx {signature = Sig, id = ID}) ->
	ID == crypto:hash(?HASH_ALG, <<Sig/binary>>).

%% @doc Check that the structure of the txs tag field is in the expected
%% key value format.
tag_field_legal(TX) ->
	lists:all(
		fun(X) ->
			case X of
				{_, _} -> true;
				_ -> false
			end
		end,
		TX#tx.tags
	).

%% @doc Convert a transactions key-value tags to binary a format.
tags_to_binary(Tags) ->
	list_to_binary(
		lists:foldr(
			fun({Name, Value}, Acc) ->
				[Name, Value | Acc]
			end,
			[],
			Tags
		)
	).

%% @doc A check if the transaction's last_tx field and owner match the expected
%% value found in the wallet list, if so returns true else false.
-ifdef(DEBUG).
check_last_tx([], _) -> true;
check_last_tx(_, #tx{owner = <<>>}) -> true;
check_last_tx(WalletList, #tx{owner = Owner, last_tx = Last}) ->
	Address = ar_wallet:to_address(Owner),
	ar_wallet_list:check_address_last_tx(WalletList, Address, Last).
-else.
check_last_tx([], _) -> true;
check_last_tx(WalletList, #tx{owner = Owner, last_tx = Last}) ->
	Address = ar_wallet:to_address(Owner),
	ar_wallet_list:check_address_last_tx(WalletList, Address, Last).
-endif.

%%% Tests: ar_tx

%% @doc Ensure that a public and private key pair can be used to sign and verify data.
sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	true = verify(sign(NewTX, Priv, Pub), 1, []).

%% @doc Ensure that a forged transaction does not pass verification.
forge_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	false = verify((sign(NewTX, Priv, Pub))#tx { data = <<"FAKE DATA">> }, 1, []).

%% @doc Ensure that a transaction above the minimum tx cost are accepted.
tx_cost_above_min_test() ->
	TestTX = new(<<"TEST DATA">>, ?AR(10)),
	true = tx_cost_above_min(TestTX, 1).

%% @doc Ensure that a transaction below the minimum tx cost are rejected.
reject_tx_below_min_test() ->
	TestTX = new(<<"TEST DATA">>, 1),
	false = tx_cost_above_min(TestTX, 10).

%% @doc Ensure that the check_last_tx function only validates transactions in which
%% last tx field matches that expected within the wallet list.
check_last_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{_Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		{Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), TX#tx.id),
		TX3 = ar_tx:new(Pub1, ?AR(1), ?AR(300), TX#tx.id),
		SignedTX2 = sign(TX2, Priv2, Pub2),
		SignedTX3 = sign(TX3, Priv3, Pub3),
		WalletList =
			[
				{ar_wallet:to_address(Pub1), 1000, <<>>},
				{ar_wallet:to_address(Pub2), 2000, TX#tx.id},
				{ar_wallet:to_address(Pub3), 3000, <<>>}
			],
		false = check_last_tx(WalletList, SignedTX3),
		true = check_last_tx(WalletList, SignedTX2)
	end}.
