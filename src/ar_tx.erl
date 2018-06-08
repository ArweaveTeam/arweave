-module(ar_tx).
-export([new/0, new/1, new/2, new/3, new/4, sign/2, sign/3, tx_to_binary/1, verify/3, verify_txs/3, signature_data_segment/1]).
-export([calculate_min_tx_cost/2, tx_cost_above_min/2, check_last_tx/2]).
-export([check_last_tx_test_slow/0]).
-export([tags_to_binary/1]).
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
	% ar:d({owner, T#tx.owner}),
	% ar:d({target, T#tx.target}),
	% ar:d({data, T#tx.data}),
	% ar:d({quantity, T#tx.quantity}),
	% ar:d({reward, T#tx.reward}),
	% ar:d({last, T#tx.last_tx}),
	% ar:d({tags, T#tx.tags}),
	% ar:d({timestamp, T#tx.timestamp})
	<<
		(T#tx.owner)/binary,
		(T#tx.target)/binary,
		(T#tx.data)/binary,
		(list_to_binary(integer_to_list(T#tx.quantity)))/binary,
		(list_to_binary(integer_to_list(T#tx.reward)))/binary,
		(T#tx.last_tx)/binary,
		(tags_to_binary(T#tx.tags))/binary
	>>.

%% @doc Cryptographicvally sign ('claim ownership') of a transaction. After it is signed, it can be
%% placed onto a block and verified at a later date.
sign(TX, {PrivKey, PubKey}) -> sign(TX, PrivKey, PubKey).
sign(TX, PrivKey, PubKey) ->
	NewTX = TX#tx{ owner = PubKey },
	Sig = ar_wallet:sign(PrivKey, signature_data_segment(NewTX)),
	ID = crypto:hash(?HASH_ALG, <<Sig/binary>>),
	NewTX#tx {
		signature = Sig, id = ID
	}.

%% @doc Ensure that a transaction is valid
-ifdef(DEBUG).
verify(#tx { signature = <<>> }, _, _) -> true;
verify(TX, Diff, WalletList) ->
	% ar:report(
	% 	[
	% 		{validate_tx, ar_util:encode(TX#tx.id)},
	% 		{tx_wallet_verify, ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature)},
	% 		{tx_above_min_cost, tx_cost_above_min(TX, Diff)},
	% 		{tx_field_size_verify, tx_field_size_limit(TX)},
	% 		{tx_tag_field_legal, tag_field_legal(TX)},
	% 		{tx_last_tx_legal, check_last_tx(WalletList, TX)},
	% 		{tx_verify_hash, tx_verify_hash(TX)}
	% 	]
	% ),
	case 
		ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature) and
		tx_cost_above_min(TX, Diff) and
		tx_field_size_limit(TX) and
		tag_field_legal(TX) and
		check_last_tx(WalletList, TX) and
		tx_verify_hash(TX) and
		ar_node:validate_wallet_list(ar_node:apply_txs(WalletList, [TX]))
	of
		true -> true;
		false ->
			Reason = 
				lists:map(
					fun({A, _}) -> A end,
					lists:filter(
						fun({_, TF}) ->
							case TF of
								true -> true;
								false -> false
							end
						end,
						[
							{"tx_signature_not_valid ", ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature)},
							{"tx_too_cheap ", tx_cost_above_min(TX, Diff)},
							{"tx_fields_too_large ", tx_field_size_limit(TX)},
							{"tag_field_illegally_specified ", tag_field_legal(TX)},
							{"last_tx_not_valid ", check_last_tx(WalletList, TX)},
							{"tx_id_not_valid ", tx_verify_hash(TX)}
						]
					)
				),
			ar_tx_db:put(TX#tx.id, Reason),		
			false
	end.
-else.
verify(TX, Diff, WalletList) ->
	% ar:report(
	% 	[
	% 		{validate_tx, ar_util:encode(ar_wallet:to_address(TX#tx.owner))},
	% 		{tx_wallet_verify, ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature)},
	% 		{tx_above_min_cost, tx_cost_above_min(TX, Diff)},
	% 		{tx_field_size_verify, tx_field_size_limit(TX)},
	% 		{tx_tag_field_legal, tag_field_legal(TX)},
	% 		{tx_lasttx_legal, check_last_tx(WalletList, TX)},
	% 		{tx_verify_hash, tx_verify_hash(TX)}
	% 	]
	% ),
	case 
		ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature) and
		tx_cost_above_min(TX, Diff) and
		tx_field_size_limit(TX) and
		tag_field_legal(TX) and
		check_last_tx(WalletList, TX) and
		tx_verify_hash(TX) and
		ar_node:validate_wallet_list(ar_node:apply_txs(WalletList, [TX]))
	of
		true -> true;
		false ->
			Reason = 
				lists:map(
					fun({A, _}) -> A end,
					lists:filter(
						fun({_, TF}) ->
							case TF of
								true -> true;
								false -> false
							end
						end,
						[
							{"tx_signature_not_valid ", ar_wallet:verify(TX#tx.owner, signature_data_segment(TX), TX#tx.signature)},
							{"tx_too_cheap ", tx_cost_above_min(TX, Diff)},
							{"tx_fields_too_large ", tx_field_size_limit(TX)},
							{"tag_field_illegally_specified ", tag_field_legal(TX)},
							{"last_tx_not_valid ", check_last_tx(WalletList, TX)},
							{"tx_id_not_valid ", tx_verify_hash(TX)}
						]
					)
				),
			ar_tx_db:put(TX#tx.id, Reason),		
			false
	end.
-endif.

%% @doc Ensure that all TXs in a list verify correctly.
verify_txs([], _, _) ->
	true;
verify_txs(TXs, Diff, WalletList) ->
	do_verify_txs(TXs, Diff, WalletList).
do_verify_txs([], _, _) ->
	true;
do_verify_txs([T|TXs], Diff, WalletList) ->
	case verify(T, Diff, WalletList) of
		true -> do_verify_txs(TXs, Diff, ar_node:apply_tx(WalletList, T));
		false -> false
	end.

%% @doc Transaction cost above proscribed minimum.
tx_cost_above_min(TX, Diff) ->
	TX#tx.reward >= calculate_min_tx_cost(byte_size(TX#tx.data), Diff).

%% @doc Calculate the minimum transaction cost for a TX with data size Size
%% the constant 3208 is the max byte size of each of the other fields
%% Cost per byte is static unless size is bigger than 10mb, at which
%% point cost per byte starts increasing linearly.
% calculate_min_tx_cost(Size, Diff) when Size < 10*1024*1024 ->
% 	((Size+3208) * ?COST_PER_BYTE * ?DIFF_CENTER) div Diff;
% calculate_min_tx_cost(Size, Diff) ->
% 	(Size*(Size+3208) * ?COST_PER_BYTE * ?DIFF_CENTER) div (Diff*10*1024*1024).

calculate_min_tx_cost(DataSize, Diff) when Diff >= ?DIFF_CENTER ->
	Size = 3210 + DataSize,
	CurveSteepness = 2,
	BaseCost = CurveSteepness*(Size*?COST_PER_BYTE) / (Diff - (?DIFF_CENTER - CurveSteepness)),
	erlang:trunc(BaseCost * math:pow(1.2, Size/(1024*1024)));
calculate_min_tx_cost(DataSize, _Diff) ->
	Size = 3210 + DataSize,
	CurveSteepness = 2,
	BaseCost = CurveSteepness*(Size*?COST_PER_BYTE) / (?DIFF_CENTER - (?DIFF_CENTER - CurveSteepness)),
	erlang:trunc(BaseCost * math:pow(1.2, Size/(1024*1024))).
%% @doc Check whether each field in a transaction is within the given
%% byte size limits
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

%% @doc Verify that the transactions ID is a hash of its signature
tx_verify_hash(#tx {signature = Sig, id = ID}) ->
	ID == crypto:hash(
		?HASH_ALG,
		<<Sig/binary>>
	).

%% @doc Check of the tag field of the TX is structured in a legal way
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

%% @doc Convert a transactions tags to binary format
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

%% @doc Check if the transactions last TX and owner match the wallet list
-ifdef(DEBUG).
check_last_tx([], _) ->
	true;
check_last_tx(_WalletList, TX) when TX#tx.owner == <<>> -> true;
check_last_tx(WalletList, TX) ->
	Address = ar_wallet:to_address(TX#tx.owner),
	case lists:keyfind(Address, 1, WalletList) of
		{Address, _Quantity, Last} ->
			Last == TX#tx.last_tx;
		_ -> false
	end.
-else.
check_last_tx([], _) ->
	true;
check_last_tx(WalletList, TX) ->
	Address = ar_wallet:to_address(TX#tx.owner),
	case lists:keyfind(Address, 1, WalletList) of
		{Address, _Quantity, Last} -> Last == TX#tx.last_tx;
		_ -> false
	end.
-endif.


%%% Tests
sign_tx_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	true = verify(sign(NewTX, Priv, Pub), 1, []).

forge_test() ->
	NewTX = new(<<"TEST DATA">>, ?AR(10)),
	{Priv, Pub} = ar_wallet:new(),
	false = verify((sign(NewTX, Priv, Pub))#tx { data = <<"FAKE DATA">> }, 1, []).

tx_cost_above_min_test() ->
	TestTX = new(<<"TEST DATA">>, ?AR(10)),
	true = tx_cost_above_min(TestTX, 1).

reject_tx_below_min_test() ->
	TestTX = new(<<"TEST DATA">>, 1),
	false = tx_cost_above_min(TestTX, 10).

check_last_tx_test_slow() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), TX#tx.id),
	% SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	% SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
	WalletList =
		[
			{ar_wallet:to_address(Pub1), 1000, <<>>},
			{ar_wallet:to_address(Pub2), 2000, TX#tx.id},
			{ar_wallet:to_address(Pub3), 3000, <<>>}
		],
	check_last_tx(WalletList, TX2).