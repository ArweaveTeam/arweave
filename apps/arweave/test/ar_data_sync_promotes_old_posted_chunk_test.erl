-module(ar_data_sync_promotes_old_posted_chunk_test).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

posted_old_chunk_does_not_repair_already_stored_storage_module_test_() ->
	{timeout, 240,
		fun test_posted_old_chunk_does_not_repair_already_stored_storage_module/0}.

test_posted_old_chunk_does_not_repair_already_stored_storage_module() ->
	?LOG_INFO([{event, test_posted_old_chunk_does_not_repair_already_stored_storage_module},
			{module, ?MODULE}]),
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModule = {ar_block:partition_size(), 0,
			ar_test_node:get_default_storage_module_packing(Addr, 0)},
	Wallet = ar_test_data_sync:setup_nodes(#{ addr => Addr, storage_modules => [StorageModule] }),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, _DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{AbsoluteEndOffset, Proof}] = ar_test_data_sync:build_proofs(B, TX, Chunks),
	StoreID = ar_storage_module:id(StorageModule),
	ChunkSize = byte_size(hd(Chunks)),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	wait_until_chunk_is_queryable(AbsoluteEndOffset, Proof),
	age_chunk_until_mature(AbsoluteEndOffset, ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 6),
	true = ar_data_sync:is_disk_space_sufficient(StoreID),
	ok = wait_until_store_synced(AbsoluteEndOffset, StoreID),

	%% Recreate the repair path: the packed bytes still exist, but the sync metadata does not.
	_ = seed_storage_module_chunk(AbsoluteEndOffset, B#block.tx_root, Proof, StorageModule),
	delete_sync_records(AbsoluteEndOffset, ChunkSize, [?DEFAULT_MODULE, StoreID]),

	?assertEqual(false, is_store_synced(AbsoluteEndOffset, StoreID)),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_chunk(main, AbsoluteEndOffset)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),

	?assertEqual(
		{error, timeout},
		ar_util:do_until(
			fun() ->
				is_store_synced(AbsoluteEndOffset, StoreID)
			end,
			200,
			15_000
		)
	),
	?assertEqual(
		{error, timeout},
		ar_util:do_until(
			fun() ->
				case ar_test_node:get_chunk(main, AbsoluteEndOffset) of
					{ok, {{<<"200">>, _}, _, _, _, _}} ->
						true;
					_ ->
						false
				end
			end,
			200,
			15_000
		)
	).

age_chunk_until_mature(AbsoluteEndOffset, AttemptsLeft) ->
	case ar_data_sync:get_disk_pool_threshold() >= AbsoluteEndOffset of
		true ->
			ok;
		false when AttemptsLeft > 0 ->
			Height = ar_node:get_height(),
			ar_test_node:mine(main),
			assert_wait_until_height(main, Height + 1),
			age_chunk_until_mature(AbsoluteEndOffset, AttemptsLeft - 1);
		false ->
			?assert(false)
	end.

seed_storage_module_chunk(AbsoluteEndOffset, TXRoot, Proof, StorageModule) ->
	StoreID = ar_storage_module:id(StorageModule),
	Packing = ar_storage_module:get_packing(StoreID),
	Chunk = ar_util:decode(maps:get(chunk, Proof)),
	DataPath = ar_util:decode(maps:get(data_path, Proof)),
	ChunkSize = byte_size(Chunk),
	{ok, PackedChunk, _} = ar_packing_server:repack(Packing, unpacked,
			AbsoluteEndOffset, TXRoot, Chunk, ChunkSize),
	ChunkDataKey = iolist_to_binary(
		["seed-old-chunk-", integer_to_binary(erlang:unique_integer([positive]))]
	),
	ChunkMetadata = #chunk_metadata{
		chunk_data_key = ChunkDataKey,
		chunk_size = ChunkSize,
		data_path = DataPath
	},
	?assert(
		lists:member(
			ar_data_sync:write_chunk(AbsoluteEndOffset, ChunkMetadata, PackedChunk, Packing,
					StoreID),
			[{ok, Packing}, {error, already_stored}]
		)
	),
	PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),
	?assertEqual(true, ar_sync_record:is_recorded(PaddedOffset, ar_chunk_storage, StoreID)),
	PackedChunk.

wait_until_chunk_is_queryable(AbsoluteEndOffset, Proof) ->
	ExpectedProof = #{
		chunk => maps:get(chunk, Proof),
		data_path => maps:get(data_path, Proof),
		tx_path => maps:get(tx_path, Proof)
	},
	ar_test_data_sync:wait_until_syncs_chunks(main, [{AbsoluteEndOffset, ExpectedProof}], infinity).

wait_until_store_synced(AbsoluteEndOffset, StoreID) ->
	?assertEqual(
		true,
		ar_util:do_until(
			fun() ->
				is_store_synced(AbsoluteEndOffset, StoreID)
			end,
			200,
			15_000
		)
	),
	ok.

delete_sync_records(AbsoluteEndOffset, ChunkSize, StoreIDs) ->
	PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),
	StartOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset - ChunkSize),
	lists:foreach(
		fun(StoreID) ->
			ok = ar_sync_record:delete(PaddedOffset, StartOffset, ar_data_sync, StoreID),
			case StoreID of
				?DEFAULT_MODULE ->
					ok;
				_ ->
					ok = ar_footprint_record:delete(PaddedOffset, StoreID)
			end
		end,
		StoreIDs
	).

is_store_synced(AbsoluteEndOffset, StoreID) ->
	case ar_sync_record:is_recorded(AbsoluteEndOffset, ar_data_sync, StoreID) of
		false ->
			false;
		_ ->
			true
	end.
