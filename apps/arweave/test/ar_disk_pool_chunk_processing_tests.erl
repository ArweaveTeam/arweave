-module(ar_disk_pool_chunk_processing_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_data_sync.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

%% -------------------------------------------------------------------
%% Test descriptors
%% -------------------------------------------------------------------

orphaned_chunk_cleanup_test_() ->
	{timeout, 120, fun test_orphaned_chunk_cleanup/0}.

immature_chunk_indexing_test_() ->
	{timeout, 120, fun test_immature_chunk_indexing/0}.

blacklisted_byte_skipped_test_() ->
	{timeout, 120, fun test_blacklisted_byte_skipped/0}.

chunk_cache_full_defers_processing_test_() ->
	{timeout, 120, fun test_chunk_cache_full_defers_processing/0}.

chunk_data_not_found_resilience_test_() ->
	{timeout, 120, fun test_chunk_data_not_found_resilience/0}.

may_conclude_accumulation_test_() ->
	{timeout, 180, fun test_may_conclude_accumulation/0}.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

post_chunk_proof(DataRoot, DataTree, Chunks) ->
	[{_ChunkEndOffset, Proof}] = ar_test_data_sync:build_proofs(
		DataRoot,
		DataTree,
		Chunks,
		#{ proof_offset => end_offset }
	),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))).

wait_until_disk_pool_empty(Timeout) ->
	ar_util:do_until(
		fun() ->
			ar_disk_pool:debug_get_chunks() == []
		end,
		200,
		Timeout
	).

wait_until_disk_pool_not_empty(Timeout) ->
	ar_util:do_until(
		fun() ->
			ar_disk_pool:debug_get_chunks() /= []
		end,
		200,
		Timeout
	).

wait_until_disk_pool_size(ExpectedSize, Timeout) ->
	ar_util:do_until(
		fun() ->
			length(ar_disk_pool:debug_get_chunks()) == ExpectedSize
		end,
		200,
		Timeout
	).

%% -------------------------------------------------------------------
%% When a chunk's data root is not on chain AND not in the disk pool
%% data roots ETS table, the chunk should be deleted from the disk pool
%% index during the scan.
%% -------------------------------------------------------------------
test_orphaned_chunk_cleanup() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	#{ tx := TX, data_root := DataRoot, data_tree := DataTree, chunks := Chunks } =
		ar_test_data_sync:make_fixed_data_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]
		),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Chunk should now be in the disk pool.
	true = wait_until_disk_pool_not_empty(10_000),
	%% Simulate orphaning: remove the data root from the disk pool data roots ETS.
	%% Since we haven't mined, the data root is NOT in the data root index either.
	DataRootID = ar_data_roots:id(DataRoot, ?DATA_CHUNK_SIZE),
	ar_disk_pool:delete_data_root(DataRootID),
	%% The disk pool scan should pick up the orphaned chunk and delete it.
	true = wait_until_disk_pool_empty(30_000),
	?assertEqual([], ar_disk_pool:debug_get_chunks()).

%% -------------------------------------------------------------------
%% A chunk whose data root is on chain but whose absolute offset is
%% still above the disk pool threshold (fewer than
%% SEARCH_SPACE_UPPER_BOUND_DEPTH confirmations) should be indexed as
%% unpacked but remain in the disk pool.
%% -------------------------------------------------------------------
test_immature_chunk_indexing() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	#{ tx := TX, data_root := DataRoot, data_tree := DataTree, chunks := Chunks } =
		ar_test_data_sync:make_fixed_data_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]
		),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Mine 1 block — the TX is now on chain but with only 1 confirmation,
	%% well within SEARCH_SPACE_UPPER_BOUND_DEPTH = 3.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	timer:sleep(2_000),
	%% The chunk should be indexed as unpacked at its immature offset
	%% (process_immature_chunk_offset path) but remain in the disk pool
	%% since CanRemoveFromDiskPool is forced to false for immature offsets.
	DiskPoolChunks = ar_disk_pool:debug_get_chunks(),
	?assertNotEqual([], DiskPoolChunks),
	%% Now mine enough blocks to push the chunk past the disk pool threshold.
	%% SEARCH_SPACE_UPPER_BOUND_DEPTH = 3, so we need the chunk's offset
	%% to be at or below the weave size at (current_height - 3).
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% The chunk should eventually be moved out of the disk pool
	%% (matured and packed into storage, then deleted from disk pool).
	true = wait_until_disk_pool_empty(30_000).

%% -------------------------------------------------------------------
%% When a chunk's absolute offset is blacklisted, the matured chunk
%% offset processing should skip that offset. The chunk should still
%% be cleaned from the disk pool (CanRemoveFromDiskPool is preserved), but
%% should NOT appear in the sync record at the blacklisted offset.
%% -------------------------------------------------------------------
test_blacklisted_byte_skipped() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	#{ tx := TX, data_root := DataRoot, data_tree := DataTree, chunks := Chunks } =
		ar_test_data_sync:make_fixed_data_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]
		),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Mine and confirm the chunk.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	%% Compute the absolute end offset of the chunk. The genesis weave is 3 chunks
	%% (3 * DATA_CHUNK_SIZE = 786432 bytes). Our TX adds DATA_CHUNK_SIZE more.
	%% The TX data starts at the weave size at the start of the block it was mined in.
	%% Get the actual end offset from the TX index.
	{ok, {TXOffset, _}} = ar_data_sync:get_tx_offset(TX#tx.id),
	AbsoluteEndOffset = TXOffset,
	%% Blacklist this byte offset before the chunk matures.
	ar_ets_intervals:add(ar_tx_blacklist_offsets, AbsoluteEndOffset,
		AbsoluteEndOffset - 1),
	%% Mine enough blocks to push past the disk pool threshold.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% The chunk should be cleaned from the disk pool (CanRemoveFromDiskPool preserved
	%% for blacklisted offsets) but NOT stored at the blacklisted offset.
	true = wait_until_disk_pool_empty(30_000),
	StoreID = ar_storage_module:id(hd(StorageModules)),
	?assertEqual(false,
		ar_sync_record:is_recorded(AbsoluteEndOffset, ar_data_sync, StoreID)),
	%% Clean up the blacklist entry.
	ar_ets_intervals:delete(ar_tx_blacklist_offsets, AbsoluteEndOffset,
		AbsoluteEndOffset - 1).

%% -------------------------------------------------------------------
%% When the chunk cache is full, the matured chunk offset processing
%% should defer (CanRemoveFromDiskPool set to false) and retry later. Once the
%% cache drains, the chunk should be processed normally.
%% -------------------------------------------------------------------
test_chunk_cache_full_defers_processing() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	#{ tx := TX, data_root := DataRoot, data_tree := DataTree, chunks := Chunks } =
		ar_test_data_sync:make_fixed_data_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]
		),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Mine blocks to confirm the chunk.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	%% Saturate the chunk cache to prevent processing.
	[{_, Limit}] = ets:lookup(ar_data_sync_state, chunk_cache_size_limit),
	ets:insert(ar_data_sync_state, {chunk_cache_size, Limit + 100}),
	%% Mine the remaining blocks to push past the threshold.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% The chunk should still be in the disk pool because the cache is full.
	timer:sleep(5_000),
	?assertNotEqual([], ar_disk_pool:debug_get_chunks()),
	%% Drain the cache.
	ets:insert(ar_data_sync_state, {chunk_cache_size, 0}),
	%% The chunk should now process and leave the disk pool.
	true = wait_until_disk_pool_empty(30_000).

%% -------------------------------------------------------------------
%% When the chunk data file is missing from the default store during
%% matured chunk processing, an error is logged but the scan continues
%% without crashing. The chunk stays in the disk pool for retry.
%% -------------------------------------------------------------------
test_chunk_data_not_found_resilience() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	StoreID = ar_storage_module:id(hd(StorageModules)),
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	#{ tx := MissingTX, data_root := MissingDataRoot,
		data_tree := MissingDataTree, chunks := MissingChunks } =
		ar_test_data_sync:make_fixed_data_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]
		),
	ar_test_node:assert_post_tx_to_peer(main, MissingTX),
	post_chunk_proof(MissingDataRoot, MissingDataTree, MissingChunks),
	#{ tx := HealthyTX, data_root := HealthyDataRoot,
		data_tree := HealthyDataTree, chunks := HealthyChunks } =
		ar_test_data_sync:make_fixed_data_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]
		),
	ar_test_node:assert_post_tx_to_peer(main, HealthyTX),
	post_chunk_proof(HealthyDataRoot, HealthyDataTree, HealthyChunks),
	%% Wait for both chunks to appear in the disk pool.
	true = wait_until_disk_pool_size(2, 10_000),
	%% Delete the chunk data for the missing-data chunk only.
	{_MissingKey, MissingValue} =
		hd([Entry || {_Key, Value} = Entry <- ar_disk_pool:debug_get_chunks(),
				element(3, parse_disk_pool_chunk(Value)) == MissingDataRoot]),
	{_MissingOffset, _MissingChunkSize, _MissingDataRoot2, _MissingTXSize,
		MissingChunkDataKey, _MissingPB, _MissingPS, _MissingPR} =
			parse_disk_pool_chunk(MissingValue),
	ok = ar_data_sync:delete_chunk_data(MissingChunkDataKey, "default"),
	%% Mine blocks to confirm and mature both chunks.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% The healthy chunk should still be processed even though the first one failed,
	%% proving the scan keeps moving forward.
	{ok, {HealthyTXOffset, _}} = ar_data_sync:get_tx_offset(HealthyTX#tx.id),
	HealthyAbsoluteEndOffset = HealthyTXOffset,
	ar_test_node:wait_until_syncs_offset(HealthyAbsoluteEndOffset, StoreID, 30_000),
	%% The failed chunk should remain in the disk pool for retry, while the healthy one
	%% should have been removed after successful processing.
	true = wait_until_disk_pool_size(1, 30_000),
	[{_RemainingKey, RemainingValue}] = ar_disk_pool:debug_get_chunks(),
	{_RemainingOffset, _RemainingChunkSize, RemainingDataRoot, _RemainingTXSize,
		_RemainingChunkDataKey, _RemainingPB, _RemainingPS, _RemainingPR} =
			parse_disk_pool_chunk(RemainingValue),
	?assertEqual(MissingDataRoot, RemainingDataRoot).

%% -------------------------------------------------------------------
%% When the same data root appears at multiple weave offsets (same
%% data uploaded in different TXs), and one offset is immature while
%% another is mature, CanRemoveFromDiskPool should be false. The chunk stays
%% in the disk pool until all offsets are processed. After all offsets
%% mature, the chunk should be cleaned from the disk pool.
%% -------------------------------------------------------------------
test_may_conclude_accumulation() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	StoreID = ar_storage_module:id(hd(StorageModules)),
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks))),
	%% Post first TX with this data root — will mine in block 1.
	{TX1, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	%% Mine an empty block 2 to create separation.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	%% Post second TX with the same data root — will mine in block 3.
	%% This creates more offset separation between TX1 and TX2.
	%% Use a high reward to avoid tx_too_cheap after fee recalculation.
	{TX2, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}, v2, ?AR(10)),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	%% Mine block 4. SEARCH_SPACE_UPPER_BOUND_DEPTH = 3 means:
	%% - threshold = weave_size at height (4 - 3) = height 1
	%% - TX1 data starts at genesis weave size, ends within height 1 → mature
	%% - TX2 data starts at weave_size at height 2, ends within height 3 → immature
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% Wait until the mature offset from TX1 is processed, then verify the
	%% chunk still remains in the disk pool because TX2's offset is still immature.
	{ok, {TX1Offset, _}} = ar_data_sync:get_tx_offset(TX1#tx.id),
	TX1AbsoluteEndOffset = TX1Offset,
	ar_test_node:wait_until_syncs_offset(TX1AbsoluteEndOffset, StoreID, 30_000),
	DiskPoolChunks = ar_disk_pool:debug_get_chunks(),
	?assert(
		lists:any(
			fun({_Key, Value}) ->
				element(3, parse_disk_pool_chunk(Value)) == DataRoot
			end,
			DiskPoolChunks
		)
	),
	%% Mine more blocks so TX2's offset also matures.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 5),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 6),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 7),
	%% Now both offsets should be mature. After the next scan cycle,
	%% the chunk should be cleaned from the disk pool.
	true = wait_until_disk_pool_empty(30_000).

%% -------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------

parse_disk_pool_chunk(Bin) ->
	case binary_to_term(Bin, [safe]) of
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, true, false, false};
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, PassesStrict} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, true, PassesStrict, false};
		R ->
			R
	end.
