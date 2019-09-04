-module(ar_tx_replay_pool).

-export([verify_tx/6, verify_block_txs/6, pick_txs_to_mine/6]).
-export([pick_txs_to_keep_in_mempool/5]).

-include("ar.hrl").

%%% This module contains functions for transaction verification. It relies on
%%% some verification helpers from the ar_tx and ar_node_utils modules.
%%% The module should be used to verify transactions on-edge, validate
%%% new blocks' transactions, pick transactions to include into a block, and
%%% remove no longer valid transactions from the mempool after accepting a new block.

-record(state, {
	%% Maps block hash to the set of TX IDs included in it.
	weave_map = maps:new(),
	%% Block hash list containing last ?MAX_TX_ANCHOR_DEPTH blocks.
	bhl = []
}).

-record(mempool, {
	tx_id_set = sets:new()
}).

%% @doc Verify that a transaction against the given mempool, wallet list, recent
%% weave txs, current block height, current difficulty, and current time.
%% The mempool is used to look for the same transaction there and to make sure
%% the transaction does not reference another transaction from the mempool.
%% The mempool is NOT used to verify shared resources like funds,
%% wallet list references, and data size. Therefore, the function is suitable
%% for on-edge verification where we want to accept potentially conflicting
%% transactions to avoid consensus issues later.
verify_tx(TX, Diff, Height, BlockTXPairs, MempoolTXs, WalletList) ->
	WeaveState = create_state(BlockTXPairs),
	TXIDSet = sets:from_list(
		[MempoolTX#tx.id || MempoolTX <- MempoolTXs]
	),
	Mempool = #mempool {
		tx_id_set = TXIDSet
	},
	verify_tx(
		general_verification,
		TX,
		Diff,
		Height,
		os:system_time(seconds),
		WalletList,
		WeaveState,
		Mempool
	).

%% @doc Verify the transactions are valid for the block taken into account
%% the given current difficulty and height, the previous blocks' wallet list,
%% and recent weave transactions.
verify_block_txs(TXs, Diff, Height, Timestamp, WalletList, BlockTXPairs) ->
	WeaveState = create_state(BlockTXPairs),
	{VerifiedTXs, _, _} = apply_txs(
		TXs,
		Diff,
		Height,
		Timestamp,
		WalletList,
		WeaveState,
		#mempool {}
	),
	case length(VerifiedTXs) of
		L when L == length(TXs) ->
			valid;
		_ ->
			invalid
	end.

%% @doc Pick a list of transactions from the mempool to mine on.
%% Transactions have to be valid when applied on top of each other taken
%% into account the current height and diff, recent weave transactions, and
%% the wallet list. The total data size of chosen transactions does not
%% exceed the block size limit. Before a valid subset of transactions is chosen,
%% transactions are sorted from biggest to smallest and then from oldest
%% block anchors to newest.
pick_txs_to_mine(BlockTXPairs, Height, Diff, Timestamp, WalletList, TXs) ->
	WeaveState = create_state(BlockTXPairs),
	{VerifiedTXs, _, _} = apply_txs(
		sort_txs_by_data_size_and_anchor(TXs, WeaveState#state.bhl),
		Diff,
		Height,
		Timestamp,
		WalletList,
		WeaveState,
		#mempool {}
	),
	case ar_fork:height_1_8() of
		H when Height >= H ->
			pick_txs_under_size_limit(VerifiedTXs);
		_ ->
			VerifiedTXs
	end.

%% @doc Choose transactions to keep in the mempool after a new block is
%% accepted. Transactions are verified independently from each other
%% taking into account the given difficulty and height of the new block,
%% the new recent weave transactions, the new wallet list, and the current time.
pick_txs_to_keep_in_mempool(BlockTXPairs, TXs, Diff, Height, WalletList) ->
	WeaveState = create_state(BlockTXPairs),
	Mempool = #mempool{},
	WalletMap = ar_node_utils:wallet_map_from_wallet_list(WalletList),
	lists:filter(
		fun(TX) ->
			case verify_tx(
				general_verification,
				TX,
				Diff,
				Height,
				os:system_time(seconds),
				WalletMap,
				WeaveState,
				Mempool
			) of
				{valid, _, _} ->
					true;
				{invalid, _} ->
					false
			end
		end,
		TXs
	).

%% PRIVATE

create_state(BlockTXPairs) ->
	MaxDepthTXs = lists:sublist(BlockTXPairs, ?MAX_TX_ANCHOR_DEPTH),
	{BHL, Map} = lists:foldr(
		fun({BH, TXIDs}, {BHL, Map}) ->
			{[BH | BHL], maps:put(BH, sets:from_list(TXIDs), Map)}
		end,
		{[], maps:new()},
		MaxDepthTXs
	),
	#state {
		weave_map = Map,
		bhl = BHL
	}.

verify_tx(general_verification, TX, Diff, Height, Timestamp, FloatingWallets, WeaveState, Mempool) ->
	case ar_tx:verify(TX, Diff, Height, FloatingWallets, Timestamp) of
		true ->
			verify_tx(last_tx_in_mempool, TX, Diff, Height, FloatingWallets, WeaveState, Mempool);
		false ->
			{invalid, tx_verification_failed}
	end.

verify_tx(last_tx_in_mempool, TX, Diff, Height, FloatingWallets, WeaveState, Mempool) ->
	ShouldContinue = case ar_fork:height_1_8() of
		H when Height >= H ->
			%% Only verify after fork 1.8 otherwise it causes a soft fork
			%% since current nodes can accept blocks with a chain of last_tx
			%% references. The check would still fail on edge pre 1.8 since
			%% TX is validated against a previous blocks' wallet list then.
			case sets:is_element(TX#tx.last_tx, Mempool#mempool.tx_id_set) of
				true ->
					ar_tx_db:put_error_codes(TX#tx.id, ["last_tx_in_mempool"]),
					{invalid, last_tx_in_mempool};
				false ->
					continue
			end;
		_ ->
			continue
	end,
	case ShouldContinue of
		continue ->
			verify_tx(
				last_tx,
				TX,
				Diff,
				Height,
				FloatingWallets,
				WeaveState,
				Mempool
			);
		{invalid, Reason} ->
			{invalid, Reason}
	end;
verify_tx(last_tx, TX, Diff, Height, FloatingWallets, WeaveState, Mempool) ->
	case ar_tx:check_last_tx(FloatingWallets, TX) of
		true ->
			NewMempool = Mempool#mempool {
				tx_id_set = sets:add_element(TX#tx.id, Mempool#mempool.tx_id_set)
			},
			NewFW = ar_node_utils:apply_tx(FloatingWallets, TX, Height),
			{valid, NewFW, NewMempool};
		false ->
			verify_tx(anchor_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool)
	end;
verify_tx(anchor_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool) ->
	case lists:member(TX#tx.last_tx, WeaveState#state.bhl) of
		false ->
			ar_tx_db:put_error_codes(TX#tx.id, ["tx_bad_anchor"]),
			{invalid, tx_bad_anchor};
		true ->
			verify_tx(weave_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool)
	end;
verify_tx(weave_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool) ->
	case weave_map_contains_tx(TX#tx.id, WeaveState#state.weave_map) of
		true ->
			ar_tx_db:put_error_codes(TX#tx.id, ["tx_already_in_weave"]),
			{invalid, tx_already_in_weave};
		false ->
			verify_tx(mempool_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool)
	end;
verify_tx(mempool_check, TX, _Diff, Height, FloatingWallets, _WeaveState, Mempool) ->
	case sets:is_element(TX#tx.id, Mempool#mempool.tx_id_set) of
		true ->
			ar_tx_db:put_error_codes(TX#tx.id, ["tx_already_in_mempool"]),
			{invalid, tx_already_in_mempool};
		false ->
			NewMempool = Mempool#mempool {
				tx_id_set = sets:add_element(TX#tx.id, Mempool#mempool.tx_id_set)
			},
			NewFW = ar_node_utils:apply_tx(FloatingWallets, TX, Height),
			{valid, NewFW, NewMempool}
	end.

weave_map_contains_tx(TXID, WeaveMap) ->
	lists:any(
		fun(BH) ->
			sets:is_element(TXID, maps:get(BH, WeaveMap))
		end,
		maps:keys(WeaveMap)
	).

apply_txs(TXs, Diff, Height, Timestamp, WalletList, WeaveState, Mempool) ->
	WalletMap = ar_node_utils:wallet_map_from_wallet_list(WalletList),
	lists:foldl(
		fun(TX, {VerifiedTXs, FloatingWalletMap, FloatingMempool}) ->
			case verify_tx(
				general_verification,
				TX,
				Diff,
				Height,
				Timestamp,
				FloatingWalletMap,
				WeaveState,
				FloatingMempool
			) of
				{valid, NewFWM, NewMempool} ->
					{VerifiedTXs ++ [TX], NewFWM, NewMempool};
				{invalid, _} ->
					{VerifiedTXs, FloatingWalletMap, FloatingMempool}
			end
		end,
		{[], WalletMap, Mempool},
		TXs
	).

pick_txs_under_size_limit(TXs) ->
	{_, _, TXsUnderSizeLimit} = lists:foldl(
		fun(TX, {TotalSize, TXCount, PickedTXs}) ->
			TXSize = byte_size(TX#tx.data),
			NewTotalSize = TXSize + TotalSize,
			NewTXCount = TXCount + 1,
			case {NewTotalSize, NewTXCount} of
				{S, _} when S > ?BLOCK_TX_DATA_SIZE_LIMIT ->
					{TotalSize, TXCount, PickedTXs};
				{_, C} when C > ?BLOCK_TX_COUNT_LIMIT ->
					{TotalSize, TXCount, PickedTXs};
				_ ->
					{NewTotalSize, NewTXCount, PickedTXs ++ [TX]}
			end
		end,
		{0, 0, []},
		TXs
	),
	TXsUnderSizeLimit.

sort_txs_by_data_size_and_anchor(TXs, BHL) ->
	lists:sort(fun(TX1, TX2) -> compare_txs(TX1, TX2, BHL) end, TXs).

compare_txs(TX1, TX2, BHL) ->
	case {lists:member(TX1#tx.last_tx, BHL), lists:member(TX2#tx.last_tx, BHL)} of
		{false, _} ->
			true;
		{true, false} ->
			false;
		{true, true} ->
			compare_txs_by_size(TX1, TX2, BHL)
	end.

compare_txs_by_size(TX1, TX2, BHL) ->
	case byte_size(TX1#tx.data) == byte_size(TX2#tx.data) of
		true ->
			compare_anchors(TX1, TX2, BHL);
		false ->
			byte_size(TX1#tx.data) > byte_size(TX2#tx.data)
	end.

compare_anchors(_Anchor1, _Anchor2, []) ->
	true;
compare_anchors(Anchor, Anchor, _) ->
	true;
compare_anchors(Anchor1, _Anchor2, [Anchor1 | _]) ->
	false;
compare_anchors(_Anchor1, Anchor2, [Anchor2 | _]) ->
	true;
compare_anchors(Anchor1, Anchor2, [_ | Anchors]) ->
	compare_anchors(Anchor1, Anchor2, Anchors).
