-module(ar_tx_validator).

-export([validate/1]).

-include_lib("arweave/include/ar.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

validate(TX) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', wallet_list},
					{'==', '$1', recent_txs_map},
					{'==', '$1', block_anchors},
					{'==', '$1', usd_to_ar_rate},
					{'==', '$1', price_per_gib_minute},
					{'==', '$1', kryder_plus_rate_multiplier},
					{'==', '$1', denomination},
					{'==', '$1', redenomination_height}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	WL = proplists:get_value(wallet_list, Props),
	RecentTXMap = proplists:get_value(recent_txs_map, Props),
	BlockAnchors = proplists:get_value(block_anchors, Props),
	USDToARRate = proplists:get_value(usd_to_ar_rate, Props),
	PricePerGiBMinute = proplists:get_value(price_per_gib_minute, Props),
	KryderPlusRateMultiplier = proplists:get_value(kryder_plus_rate_multiplier, Props),
	Denomination = proplists:get_value(denomination, Props),
	RedenominationHeight = proplists:get_value(redenomination_height, Props),
	Wallets = ar_wallets:get(WL, ar_tx:get_addresses([TX])),
	Mempool = ar_mempool:get_map(),
	Result = ar_tx_replay_pool:verify_tx({TX, USDToARRate, PricePerGiBMinute,
			KryderPlusRateMultiplier, Denomination, Height, RedenominationHeight, BlockAnchors,
			RecentTXMap, Mempool, Wallets}),
	Result2 =
		case {Result, TX#tx.format == 2 andalso byte_size(TX#tx.data) /= 0} of
			{valid, true} ->
				Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, TX#tx.data),
				SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
				SizeTaggedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks),
				{Root, _} = ar_merkle:generate_tree(SizeTaggedChunkIDs),
				Size = byte_size(TX#tx.data),
				case {Root, Size} == {TX#tx.data_root, TX#tx.data_size} of
					true ->
						valid;
					false ->
						{invalid, invalid_data_root_size}
				end;
			_ ->
				Result
		end,
	case Result2 of
		valid ->
			case TX#tx.format of
				2 ->
					{valid, TX};
				1 ->
					case TX#tx.data_size > 0 of
						true ->
							%% Compute the data root so that we can inform ar_data_sync about
							%% it so that it can accept the chunks. One may notice here that
							%% in case of v1 transactions, chunks arrive together with the tx
							%% header. However, we send the data root to ar_data_sync in
							%% advance, otherwise ar_header_sync may fail to store the chunks
							%% when persisting the transaction as registering the data roots of
							%% a confirmed block is an asynchronous procedure
							%% (see ar_data_sync:add_tip_block called in ar_node_worker) which
							%% does not always complete before ar_header_sync attempts the
							%% insertion.
							V1Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, TX#tx.data),
							SizeTaggedV1Chunks = ar_tx:chunks_to_size_tagged_chunks(V1Chunks),
							SizeTaggedV1ChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
									SizeTaggedV1Chunks),
							{DataRoot, _} = ar_merkle:generate_tree(SizeTaggedV1ChunkIDs),
							{valid, TX#tx{ data_root = DataRoot }};
						false ->
							{valid, TX}
					end
			end;
		_ ->
			Result2
	end.
