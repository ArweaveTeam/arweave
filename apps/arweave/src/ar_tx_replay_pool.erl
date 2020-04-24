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

%% @doc Verify that a transaction against the given mempool, wallet list, recent
%% weave txs, current block height, current difficulty, and current time.
%% The mempool is used to look for the same transaction there and to make sure
%% the transaction does not reference another transaction from the mempool.
%% The mempool is NOT used to verify shared resources like funds,
%% wallet list references, and data size. Therefore, the function is suitable
%% for on-edge verification where we want to accept potentially conflicting
%% transactions to avoid consensus issues later.
verify_tx(TX, Diff, Height, BlockTXPairs, Mempool, WalletList) ->
	WeaveState = create_state(BlockTXPairs),
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
		maps:new()
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
%% transactions are sorted from highest to lowest utility and then from oldest
%% block anchors to newest.
pick_txs_to_mine(BlockTXPairs, Height, Diff, Timestamp, WalletList, TXs) ->
	WeaveState = create_state(BlockTXPairs),
	{VerifiedTXs, _, _} = apply_txs(
		sort_txs_by_utility_and_anchor(TXs, WeaveState#state.bhl),
		Diff,
		Height,
		Timestamp,
		WalletList,
		WeaveState,
		maps:new()
	),
	pick_txs_under_size_limit(VerifiedTXs).

%% @doc Choose transactions to keep in the mempool after a new block is
%% accepted. Transactions are verified independently from each other
%% taking into account the given difficulty and height of the new block,
%% the new recent weave transactions, the new wallet list, and the current time.
pick_txs_to_keep_in_mempool(BlockTXPairs, TXs, Diff, Height, WalletList) ->
	WeaveState = create_state(BlockTXPairs),
	WalletMap = ar_node_utils:wallet_map_from_wallet_list(WalletList),
	InvalidTXs = maps:fold(
		fun(_, {TX, _}, InvalidTXs) ->
			case verify_tx(
				general_verification,
				TX,
				Diff,
				Height,
				os:system_time(seconds),
				WalletMap,
				WeaveState,
				maps:new(),
				do_not_verify_signature
			) of
				{valid, _, _} ->
					InvalidTXs;
				{invalid, Reason} ->
					[{TX, Reason} | InvalidTXs]
			end
		end,
		[],
		TXs
	),
	{lists:foldl(
		fun({TX, _}, ValidTXs) ->
			maps:remove(TX#tx.id, ValidTXs)
		end,
		TXs,
		InvalidTXs
	), InvalidTXs}.

%% PRIVATE

create_state(BlockTXPairs) ->
	MaxDepthTXs = lists:sublist(BlockTXPairs, ?MAX_TX_ANCHOR_DEPTH),
	{BHL, Map} = lists:foldr(
		fun({BH, SizeTaggedTXs}, {BHL, Map}) ->
			TXIDs = [TXID || {{TXID, _}, _} <- SizeTaggedTXs],
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
	verify_tx(general_verification, TX, Diff, Height, Timestamp, FloatingWallets, WeaveState, Mempool, verify_signature).

verify_tx(general_verification, TX, Diff, Height, Timestamp, FloatingWallets, WeaveState, Mempool, VerifySignature) ->
	case ar_tx:verify(TX, Diff, Height, FloatingWallets, Timestamp, VerifySignature) of
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
			case maps:is_key(TX#tx.last_tx, Mempool) of
				true ->
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
			NewMempool = maps:put(TX#tx.id, no_tx, Mempool),
			NewFW = ar_node_utils:apply_tx(FloatingWallets, TX, Height),
			{valid, NewFW, NewMempool};
		false ->
			verify_tx(anchor_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool)
	end;
verify_tx(anchor_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool) ->
	case lists:member(TX#tx.last_tx, WeaveState#state.bhl) of
		false ->
			{invalid, tx_bad_anchor};
		true ->
			verify_tx(weave_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool)
	end;
verify_tx(weave_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool) ->
	case weave_map_contains_tx(TX#tx.id, WeaveState#state.weave_map) of
		true ->
			{invalid, tx_already_in_weave};
		false ->
			verify_tx(mempool_check, TX, Diff, Height, FloatingWallets, WeaveState, Mempool)
	end;
verify_tx(mempool_check, TX, _Diff, Height, FloatingWallets, _WeaveState, Mempool) ->
	case maps:is_key(TX#tx.id, Mempool) of
		true ->
			{invalid, tx_already_in_mempool};
		false ->
			NewMempool = maps:put(TX#tx.id, no_tx, Mempool),
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
			TXSize = case TX#tx.format of
				1 ->
					TX#tx.data_size;
				2 ->
					0
			end,
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

sort_txs_by_utility_and_anchor(TXs, BHL) ->
	lists:sort(fun(TX1, TX2) -> compare_txs(TX1, TX2, BHL) end, TXs).

compare_txs(TX1, TX2, BHL) ->
	case {lists:member(TX1#tx.last_tx, BHL), lists:member(TX2#tx.last_tx, BHL)} of
		{false, _} ->
			true;
		{true, false} ->
			false;
		{true, true} ->
			compare_txs_by_utility(TX1, TX2, BHL)
	end.

compare_txs_by_utility(TX1, TX2, BHL) ->
	U1 = ar_tx_queue:utility(TX1),
	U2 = ar_tx_queue:utility(TX2),
	case U1 == U2 of
		true ->
			compare_anchors(TX1, TX2, BHL);
		false ->
			U1 > U2
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
