%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_node).

-export([get_recent_block_hash_by_height/1, get_blocks/0, get_block_index/0,
		get_current_block/0, get_current_diff/0,
		is_in_block_index/1, get_block_index_and_height/0,
		get_height/0, get_weave_size/0, get_balance/1, get_last_tx/1, get_ready_for_mining_txs/0,
		get_current_usd_to_ar_rate/0, get_current_block_hash/0,
		get_block_index_entry/1, get_2_0_hash_of_1_0_block/1, is_joined/0, get_block_anchors/0,
		get_recent_txs_map/0, get_mempool_size/0,
		get_block_shadow_from_cache/1, get_recent_partition_upper_bound_by_prev_h/1,
		get_block_txs_pairs/0, get_partition_upper_bound/1, get_nth_or_last/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Return the hash of the block of the given Height. Return not_found
%% if Height is bigger than the current height or too small.
get_recent_block_hash_by_height(Height) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', block_anchors}}], ['$_']}]
		),
	CurrentHeight = proplists:get_value(height, Props),
	Anchors = proplists:get_value(block_anchors, Props),
	case Height > CurrentHeight orelse Height =< CurrentHeight - length(Anchors) of
		true ->
			not_found;
		false ->
			lists:nth(CurrentHeight - Height + 1, Anchors)
	end.

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_blocks() ->
	get_block_index().

%% @doc Get the current block index (the list of {block hash, weave size, tx root} triplets).
get_block_index() ->
	case ets:lookup(node_state, is_joined) of
		[{_, true}] ->
			element(2, get_block_index_and_height());
		_ ->
			[]
	end.

%% @doc Return the current tip block. Assume the node has joined the network and
%% initialized the state.
get_current_block() ->
	[{_, Current}] = ets:lookup(node_state, current),
	ar_block_cache:get(block_cache, Current).

%% @doc Return the current network difficulty. Assume the node has joined the network and
%% initialized the state.
get_current_diff() ->
	[{_, Diff}] = ets:lookup(node_state, diff),
	Diff.

get_block_index_and_height() ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', recent_block_index}}], ['$_']}]
		),
	CurrentHeight = proplists:get_value(height, Props),
	RecentBI = proplists:get_value(recent_block_index, Props),
	{CurrentHeight, merge(RecentBI,
			ar_block_index:get_list(CurrentHeight - length(RecentBI)))}.

merge([Elem | BI], BI2) ->
	[Elem | merge(BI, BI2)];
merge([], BI) ->
	BI.

%% @doc Get the list of being mined or ready to be mined transactions.
%% The list does _not_ include transactions waiting for network propagation.
get_ready_for_mining_txs() ->
	gb_sets:fold(
		fun
			({_Utility, TXID, ready_for_mining}, Acc) ->
				[TXID | Acc];
			(_, Acc) ->
				Acc
		end,
		[],
		ar_mempool:get_priority_set()
	).

%% @doc Return true if the given block hash is found in the block index.
is_in_block_index(H) ->
	ar_block_index:member(H).

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
			ar_block_index:get_element_by_height(Height)
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

get_weave_size() ->
	case ets:lookup(node_state, weave_size) of
		[{weave_size, WeaveSize}] ->
			WeaveSize;
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

%% @doc Get the currently estimated USD to AR exchange rate.
get_current_usd_to_ar_rate() ->
	[{_, Rate}] = ets:lookup(node_state, usd_to_ar_rate),
	Rate.

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
get_balance({SigType, PubKey}) ->
	get_balance(ar_wallet:to_address(PubKey, SigType));
get_balance(MaybeRSAPub) when byte_size(MaybeRSAPub) == 512 ->
	%% A legacy feature where we may search the public key instead of address.
	ar_wallets:get_balance(ar_wallet:to_rsa_address(MaybeRSAPub));
get_balance(Addr) ->
	ar_wallets:get_balance(Addr).

%% @doc Get the last tx id associated with a given wallet address.
%% Should the wallet not have made a tx the empty binary will be returned.
get_last_tx({SigType, PubKey}) ->
	get_last_tx(ar_wallet:to_address(PubKey, SigType));
get_last_tx(MaybeRSAPub) when byte_size(MaybeRSAPub) == 512 ->
	%% A legacy feature where we may search the public key instead of address.
	get_last_tx(ar_wallet:to_rsa_address(MaybeRSAPub));
get_last_tx(Addr) ->
	{ok, ar_wallets:get_last_tx(Addr)}.

get_recent_partition_upper_bound_by_prev_h(H) ->
	get_recent_partition_upper_bound_by_prev_h(H, 0).

%% @doc Get the list of the recent {H, TXIDs} pairs sorted from latest to earliest.
get_block_txs_pairs() ->
	[{_, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
	BlockTXPairs.

get_nth_or_last(N, BI) ->
	case length(BI) < N of
		true ->
			lists:last(BI);
		false ->
			lists:nth(N, BI)
	end.

get_partition_upper_bound(BI) ->
	element(2, get_nth_or_last(?SEARCH_SPACE_UPPER_BOUND_DEPTH, BI)).

get_recent_partition_upper_bound_by_prev_h(H, Diff) ->
	case ar_block_cache:get_block_and_status(block_cache, H) of
		{_B, on_chain} ->
			[{_, BI}] = ets:lookup(node_state, recent_block_index),
			Genesis = length(BI) =< ?SEARCH_SPACE_UPPER_BOUND_DEPTH,
			get_recent_partition_upper_bound_by_prev_h(H, Diff, BI, Genesis);
		{#block{ indep_hash = H2, previous_block = PrevH, weave_size = WeaveSize }, _} ->
			case Diff == ?SEARCH_SPACE_UPPER_BOUND_DEPTH - 1 of
				true ->
					{H2, WeaveSize};
				false ->
					get_recent_partition_upper_bound_by_prev_h(PrevH, Diff + 1)
			end;
		not_found ->
			?LOG_INFO([{event, prev_block_not_found}, {h, ar_util:encode(H)}, {depth, Diff}]),
			not_found
	end.

get_recent_partition_upper_bound_by_prev_h(H, Diff, [{H, _, _} | _] = BI, Genesis) ->
	PartitionUpperBoundDepth = ?SEARCH_SPACE_UPPER_BOUND_DEPTH,
	Depth = PartitionUpperBoundDepth - Diff,
	case length(BI) < Depth of
		true ->
			case Genesis of
				true ->
					{H2, PartitionUpperBound, _TXRoot} = lists:last(BI),
					{H2, PartitionUpperBound};
				false ->
					not_found
			end;
		false ->
			{H2, PartitionUpperBound, _TXRoot} = lists:nth(Depth, BI),
			{H2, PartitionUpperBound}
	end;
get_recent_partition_upper_bound_by_prev_h(H, Diff, [_ | BI], Genesis) ->
	get_recent_partition_upper_bound_by_prev_h(H, Diff, BI, Genesis);
get_recent_partition_upper_bound_by_prev_h(H, Diff, [], _Genesis) ->
	?LOG_INFO([{event, prev_block_not_found_when_scanning_recent_block_index},
			{h, ar_util:encode(H)}, {depth, Diff}]),
	not_found.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_recent_partition_upper_bound_by_prev_h_short_cache_test() ->
	ar_block_cache:new(block_cache, B0 = test_block(1, 1, <<>>)),
	H0 = B0#block.indep_hash,
	BI = lists:reverse([{H0, 20, <<>>}
			| [{crypto:strong_rand_bytes(48), 20, <<>>} || _ <- lists:seq(1, 99)]]),
	ets:insert(node_state, {recent_block_index, BI}),
	?assertEqual(not_found, get_recent_partition_upper_bound_by_prev_h(B0#block.indep_hash)),
	?assertEqual(not_found,
			get_recent_partition_upper_bound_by_prev_h(crypto:strong_rand_bytes(48))),
	{HPrev, _, _} = lists:nth(length(BI) - ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 2, BI),
	?assertEqual(not_found, get_recent_partition_upper_bound_by_prev_h(HPrev)),
	{H, _, _} = lists:nth(length(BI) - ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 1, BI),
	?assertEqual(not_found, get_recent_partition_upper_bound_by_prev_h(H)),
	add_blocks(tl(lists:reverse(BI)), 2, 2, H0),
	?assertEqual(not_found, get_recent_partition_upper_bound_by_prev_h(HPrev)),
	?assertEqual({H0, 20}, get_recent_partition_upper_bound_by_prev_h(H)),
	{HNext, _, _} = lists:nth(length(BI) - ?SEARCH_SPACE_UPPER_BOUND_DEPTH, BI),
	{H1, _, _} = lists:nth(99, BI),
	?assertEqual({H1, 20}, get_recent_partition_upper_bound_by_prev_h(HNext)).

get_recent_partition_upper_bound_by_prev_h_genesis_test() ->
	ar_block_cache:new(block_cache, B0 = test_block(0, 1, <<>>)),
	H0 = B0#block.indep_hash,
	ets:insert(node_state, {recent_block_index, [{H0, 20, <<>>}]}),
	?assertEqual({H0, 20}, get_recent_partition_upper_bound_by_prev_h(H0)).

test_block(Height, CDiff, PrevH) ->
	test_block(crypto:strong_rand_bytes(48), Height, CDiff, PrevH).

test_block(H, Height, CDiff, PrevH) ->
	#block{ indep_hash = H, height = Height, cumulative_diff = CDiff, previous_block = PrevH }.

add_blocks([{H, _, _} | BI], Height, CDiff, PrevH) ->
	ar_block_cache:add_validated(block_cache, test_block(H, Height, CDiff, PrevH)),
	ar_block_cache:mark_tip(block_cache, H),
	add_blocks(BI, Height + 1, CDiff + 1, H);
add_blocks([], _Height, _CDiff, _PrevH) ->
	ok.
