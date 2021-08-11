%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_node).

-export([
	get_blocks/0,
	get_block_index/0, is_in_block_index/1, get_height/0,
	get_balance/1,
	get_last_tx/1,
	get_wallets/1,
	get_wallet_list_chunk/2,
	get_current_diff/0, get_diff/0,
	get_pending_txs/0, get_pending_txs/1, get_ready_for_mining_txs/0, is_a_pending_tx/1,
	get_current_usd_to_ar_rate/0,
	get_current_block_hash/0,
	get_block_index_entry/1,
	get_2_0_hash_of_1_0_block/1,
	is_joined/0,
	get_block_anchors/0, get_recent_txs_map/0,
	mine/0,
	add_tx/1,
	get_mempool_size/0,
	get_block_shadow_from_cache/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mine.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_blocks() ->
	get_block_index().

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_block_index() ->
	case ets:lookup(node_state, is_joined) of
		[{_, true}] ->
			[{block_index, BI}] = ets:lookup(node_state, block_index),
			BI;
		_ ->
			[]
	end.

%% @doc Get pending transactions. This includes:
%% 1. The transactions currently staying in the priority queue.
%% 2. The transactions on timeout waiting to be distributed around the network.
%% 3. The transactions ready to be and being mined.
%% @end
get_pending_txs() ->
	get_pending_txs([]).

get_pending_txs(Opts) ->
	case {lists:member(as_map, Opts), lists:member(id_only, Opts)} of
		{true, false} ->
			[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
			maps:map(
				fun(TXID, _Value) ->
					[{{tx, TXID}, TX}] = ets:lookup(node_state, {tx, TXID}),
					TX
				end,
				Map
			);
		{true, true} ->
			[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
			Map;
		{false, true} ->
			[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
			maps:keys(Map);
		{false, false} ->
			[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
			maps:fold(
				fun(TXID, _Value, Acc) ->
					[{{tx, TXID}, TX}] = ets:lookup(node_state, {tx, TXID}),
					[TX | Acc]
				end,
				[],
				Map
			)
	end.

%% @doc Return true if a tx with the given identifier is pending.
is_a_pending_tx(TXID) ->
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	maps:is_key(TXID, Map).

%% @doc Get the list of being mined or ready to be mined transactions.
%% The list does _not_ include transactions in the priority queue or
%% those on timeout waiting for network propagation.
%% @end
get_ready_for_mining_txs() ->
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	maps:fold(
		fun
			(TXID, ready_for_mining, Acc) ->
				[{{tx, TXID}, TX}] = ets:lookup(node_state, {tx, TXID}),
				[TX | Acc];
			(_, _, Acc) ->
				Acc
		end,
		[],
		Map
	).

%% @doc Return true if the given block hash is found in the block index.
is_in_block_index(H) ->
	[{block_index, BI}] = ets:lookup(node_state, block_index),
	case lists:search(fun({BH, _, _}) -> BH == H end, BI) of
		{value, _} ->
			true;
		false ->
			false
	end.

%% @doc Get the current block hash.
get_current_block_hash() ->
	case ets:lookup(node_state, current) of
		[{current, H}] ->
			H;
		[] ->
			not_joined
	end.

%% @doc Get the block index entry by height.
get_block_index_entry(Height) ->
	case ets:lookup(node_state, is_joined) of
		[] ->
			not_joined;
		[{_, false}] ->
			not_joined;
		[{_, true}] ->
			Props =
				ets:select(
					node_state,
					[{{'$1', '$2'},
						[{'or',
							{'==', '$1', height},
							{'==', '$1', block_index}}], ['$_']}]
				),
			CurrentHeight = proplists:get_value(height, Props),
			BI = proplists:get_value(block_index, Props),
			case Height > CurrentHeight of
				true ->
					not_found;
				false ->
					lists:nth(CurrentHeight - Height + 1, BI)
			end
	end.

%% @doc Get the 2.0 hash for a 1.0 block.
%% Before 2.0, to compute a block hash, the complete wallet list
%% and all the preceding hashes were required. Getting a wallet list
%% and a hash list for every historical block to verify it belongs to
%% the weave is very costly. Therefore, a list of 2.0 hashes for 1.0
%% blocks was computed and stored along with the network client.
%% @end
get_2_0_hash_of_1_0_block(Height) ->
	[{hash_list_2_0_for_1_0_blocks, HL}] = ets:lookup(node_state, hash_list_2_0_for_1_0_blocks),
	Fork_2_0 = ar_fork:height_2_0(),
	case Height > Fork_2_0 of
		true ->
			invalid_height;
		false ->
			lists:nth(Fork_2_0 - Height, HL)
	end.

%% @doc Return the current height of the blockweave.
get_height() ->
	case ets:lookup(node_state, height) of
		[{height, Height}] ->
			Height;
		[] ->
			-1
	end.

%% @doc Check whether the node has joined the network.
is_joined() ->
	case ets:lookup(node_state, is_joined) of
		[{is_joined, IsJoined}] ->
			IsJoined;
		[] ->
			false
	end.

%% @doc Returns the estimated future difficulty of the currently mined block.
%% The function name is confusing and needs to be changed.
%% @end
get_current_diff() ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', last_retarget},
					{'==', '$1', diff}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	Diff = proplists:get_value(diff, Props),
	LastRetarget = proplists:get_value(last_retarget, Props),
	ar_retarget:maybe_retarget(
		Height + 1,
		Diff,
		os:system_time(seconds),
		LastRetarget
	).

%% @doc Get the currently estimated USD to AR exchange rate.
get_current_usd_to_ar_rate() ->
	[{_, Rate}] = ets:lookup(node_state, usd_to_ar_rate),
	Rate.

%% @doc Returns the difficulty of the current block (the last applied one).
get_diff() ->
	[{diff, Diff}] = ets:lookup(node_state, diff),
	Diff.

%% @doc Returns a list of block anchors corrsponding to the current state -
%% the hashes of the recent blocks that can be used in transactions as anchors.
%% @end
get_block_anchors() ->
	[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
	BlockAnchors.

%% @doc Return a map TXID -> ok containing all the recent transaction identifiers.
%% Used for preventing replay attacks.
%% @end
get_recent_txs_map() ->
	[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
	RecentTXMap.

%% @doc Return memory pool size
get_mempool_size() ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	MempoolSize.

%% @doc Get the block shadow from the block cache.
get_block_shadow_from_cache(H) ->
	ar_block_cache:get(block_cache, H).

%% @doc Get the current balance of a given wallet address.
%% The balance returned is in relation to the nodes current wallet list.
%% @end
get_balance(Addr) when ?IS_ADDR(Addr) ->
	ar_wallets:get_balance(Addr);
get_balance(WalletID) ->
	get_balance(ar_wallet:to_address(WalletID)).

%% @doc Get the last tx id associated with a given wallet address.
%% Should the wallet not have made a tx the empty binary will be returned.
%% @end
get_last_tx(Addr) when ?IS_ADDR(Addr) ->
	{ok, ar_wallets:get_last_tx(Addr)};
get_last_tx(WalletID) ->
	get_last_tx(ar_wallet:to_address(WalletID)).

%% @doc Return a map address => {balance, last tx} for the given addresses.
get_wallets(Addresses) ->
	ar_wallets:get(Addresses).

%% @doc Return a chunk of wallets from the tree with the given root hash starting
%% from the Cursor address.
%% @end
get_wallet_list_chunk(RootHash, Cursor) ->
	ar_wallets:get_chunk(RootHash, Cursor).

%% @doc Trigger a node to start mining a block.
mine() ->
	gen_server:cast(ar_node_worker, mine).

%% @doc Add a transaction to the memory pool, ready for mining.
add_tx(TX)->
	ar_events:send(tx, {ready_for_mining, TX}).
