%%% @doc This module contains functions for transaction verification. It relies on
%%% some verification helpers from the ar_tx and ar_node_utils modules.
%%% The module should be used to verify transactions on-edge, validate
%%% new blocks' transactions, pick transactions to include into a block, and
%%% remove no longer valid transactions from the mempool after accepting a new block.
%%% @end
-module(ar_tx_replay_pool).

-export([verify_tx/1, verify_tx/2, verify_block_txs/1, pick_txs_to_mine/1]).

-include_lib("arweave/include/ar.hrl").

%% @doc Verify that a transaction against the given mempool, wallet list, recent
%% weave txs, current block height, current difficulty, and current time.
%% The mempool is used to look for the same transaction there and to make sure
%% the transaction does not reference another transaction from the mempool.
%% The mempool is NOT used to verify shared resources like funds,
%% wallet list references, and data size. Therefore, the function is suitable
%% for on-edge verification where we want to accept potentially conflicting
%% transactions to avoid consensus issues later.
verify_tx(Args) ->
	verify_tx(Args, verify_signature).

verify_tx(Args, VerifySignature) ->
	{TX, Rate, Height, BlockAnchors, RecentTXMap, Mempool, WalletList} = Args,
	verify_tx2({TX, Rate, Height, os:system_time(seconds), WalletList, BlockAnchors, RecentTXMap,
			Mempool, VerifySignature}).

%% @doc Verify the transactions are valid for the block taken into account
%% the given current difficulty and height, the previous blocks' wallet list,
%% and recent weave transactions.
%% @end
verify_block_txs(Args) ->
	{TXs, Rate, Height, Timestamp, WalletList, BlockAnchors, RecentTXMap} = Args,
	verify_block_txs(
		TXs,
		{Rate, Height, Timestamp, WalletList, BlockAnchors, RecentTXMap, maps:new(), 0, 0}
	).

verify_block_txs([], _Args) ->
	valid;
verify_block_txs([TX | TXs], Args) ->
	{Rate, Height, Timestamp, Wallets, BlockAnchors, RecentTXMap, Mempool, C, Size} = Args,
	case verify_tx2({
		TX,
		Rate,
		Height,
		Timestamp,
		Wallets,
		BlockAnchors,
		RecentTXMap,
		Mempool,
		verify_signature
	}) of
		valid ->
			NewMempool = maps:put(TX#tx.id, no_tx, Mempool),
			NewWallets = ar_node_utils:apply_tx(Wallets, TX, Height),
			NewSize =
				case TX of
					#tx{ format = 1 } ->
						Size + TX#tx.data_size;
					_ ->
						Size
				end,
			NewCount = C + 1,
			AboveFork1_8 = Height >= ar_fork:height_1_8(),
			CountExceedsLimit = NewCount > ?BLOCK_TX_COUNT_LIMIT,
			SizeExceedsLimit = NewSize > ?BLOCK_TX_DATA_SIZE_LIMIT,
			case {AboveFork1_8, CountExceedsLimit, SizeExceedsLimit} of
				{true, true, _} ->
					invalid;
				{true, _, true} ->
					invalid;
				_ ->
					verify_block_txs(
						TXs,
						{
							Rate,
							Height,
							Timestamp,
							NewWallets,
							BlockAnchors,
							RecentTXMap,
							NewMempool,
							NewCount,
							NewSize
						}
					)
			end;
		{invalid, _} ->
			invalid
	end.

%% @doc Pick a list of transactions from the mempool to mine on.
%% Transactions have to be valid when applied on top of each other taken
%% into account the current height and diff, recent weave transactions, and
%% the wallet list. The total data size of chosen transactions does not
%% exceed the block size limit. Before a valid subset of transactions is chosen,
%% transactions are sorted from highest to lowest utility and then from oldest
%% block anchors to newest.
%% @end
pick_txs_to_mine(Args) ->
	{BlockAnchors, RecentTXMap, Height, Rate, Timestamp, Wallets, TXs} = Args,
	pick_txs_under_size_limit(
		sort_txs_by_utility_and_anchor(TXs, BlockAnchors),
		{
			Rate,
			Height,
			Timestamp,
			Wallets,
			BlockAnchors,
			RecentTXMap,
			maps:new(),
			0,
			0
		}
	).

%%%===================================================================
%%% Private functions.
%%%===================================================================

verify_tx2(Args) ->
	{TX, Rate, Height, Timestamp, FloatingWallets, BlockAnchors, RecentTXMap, Mempool,
			VerifySignature} = Args,
	case ar_tx:verify(TX, Rate, Height, FloatingWallets, Timestamp, VerifySignature) of
		true ->
			verify_anchor(TX, Height, FloatingWallets, BlockAnchors, RecentTXMap, Mempool);
		false ->
			{invalid, tx_verification_failed}
	end.

verify_anchor(TX, Height, FloatingWallets, BlockAnchors, RecentTXMap, Mempool) ->
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
			verify_last_tx(TX, FloatingWallets, BlockAnchors, RecentTXMap, Mempool);
		{invalid, Reason} ->
			{invalid, Reason}
	end.

verify_last_tx(TX, FloatingWallets, BlockAnchors, RecentTXMap, Mempool) ->
	case ar_tx:check_last_tx(FloatingWallets, TX) of
		true ->
			valid;
		false ->
			verify_block_anchor(TX, BlockAnchors, RecentTXMap, Mempool)
	end.

verify_block_anchor(TX, BlockAnchors, RecentTXMap, Mempool) ->
	case lists:member(TX#tx.last_tx, BlockAnchors) of
		false ->
			{invalid, tx_bad_anchor};
		true ->
			verify_tx_in_weave(TX, RecentTXMap, Mempool)
	end.

verify_tx_in_weave(TX, RecentTXMap, Mempool) ->
	case maps:is_key(TX#tx.id, RecentTXMap) of
		true ->
			{invalid, tx_already_in_weave};
		false ->
			verify_tx_in_mempool(TX, Mempool)
	end.

verify_tx_in_mempool(TX, Mempool) ->
	case maps:is_key(TX#tx.id, Mempool) of
		true ->
			{invalid, tx_already_in_mempool};
		false ->
			valid
	end.

pick_txs_under_size_limit([], _Args) ->
	[];
pick_txs_under_size_limit([TX | TXs], Args) ->
	{Rate, Height, Timestamp, Wallets, BlockAnchors, RecentTXMap, Mempool, Size, Count} = Args,
	case verify_tx2({TX, Rate, Height, Timestamp, Wallets, BlockAnchors, RecentTXMap, Mempool,
			verify_signature}) of
		valid ->
			NewMempool = maps:put(TX#tx.id, no_tx, Mempool),
			NewWallets = ar_node_utils:apply_tx(Wallets, TX, Height),
			NewSize =
				case TX of
					#tx{ format = 1 } ->
						Size + TX#tx.data_size;
					_ ->
						Size
				end,
			NewCount = Count + 1,
			CountExceedsLimit = NewCount > ?BLOCK_TX_COUNT_LIMIT,
			SizeExceedsLimit = NewSize > ?BLOCK_TX_DATA_SIZE_LIMIT,
			case CountExceedsLimit orelse SizeExceedsLimit of
				true ->
					pick_txs_under_size_limit(
						TXs,
						{
							Rate,
							Height,
							Timestamp,
							Wallets,
							BlockAnchors,
							RecentTXMap,
							Mempool,
							Size,
							Count
						}
					);
				false ->
					[TX | pick_txs_under_size_limit(
						TXs,
						{
							Rate,
							Height,
							Timestamp,
							NewWallets,
							BlockAnchors,
							RecentTXMap,
							NewMempool,
							NewSize,
							NewCount
						}
					)]
			end;
		{invalid, _} ->
			pick_txs_under_size_limit(
				TXs,
				{
					Rate,
					Height,
					Timestamp,
					Wallets,
					BlockAnchors,
					RecentTXMap,
					Mempool,
					Size,
					Count
				}
			)
	end.

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
	U1 = ar_tx:utility(TX1),
	U2 = ar_tx:utility(TX2),
	case U1 == U2 of
		true ->
			compare_anchors(TX1#tx.last_tx, TX2#tx.last_tx, BHL);
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
