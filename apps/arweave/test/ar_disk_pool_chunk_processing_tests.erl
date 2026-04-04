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

make_chunk() ->
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks))),
	{DataRoot, DataTree, Chunks}.

post_chunk_proof(DataRoot, DataTree, Chunks) ->
	Offset = ?DATA_CHUNK_SIZE,
	DataSize = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, Offset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(Offset),
		data_size => integer_to_binary(DataSize)
	},
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

%% -------------------------------------------------------------------
%% Gap 1: Orphaned chunk cleanup
%%
%% When a chunk's data root is not on chain AND not in the disk pool
%% data roots ETS table, the chunk should be deleted from the disk pool
%% index during the scan.
%% -------------------------------------------------------------------

test_orphaned_chunk_cleanup() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	{DataRoot, DataTree, Chunks} = make_chunk(),
	{TX, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Chunk should now be in the disk pool.
	true = wait_until_disk_pool_not_empty(10_000),
	%% Simulate orphaning: remove the data root from the disk pool data roots ETS.
	%% Since we haven't mined, the data root is NOT in the data root index either.
	%% This puts us in the {not_found, false} branch of process_item.
	DataRootKey = << DataRoot/binary, ?DATA_CHUNK_SIZE:?OFFSET_KEY_BITSIZE >>,
	ets:delete(ar_disk_pool_data_roots, DataRootKey),
	%% The disk pool scan should pick up the orphaned chunk and delete it.
	true = wait_until_disk_pool_empty(30_000),
	?assertEqual([], ar_disk_pool:debug_get_chunks()).

%% -------------------------------------------------------------------
%% Gap 3: Immature chunk indexing
%%
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
	{DataRoot, DataTree, Chunks} = make_chunk(),
	{TX, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Mine 1 block — the TX is now on chain but with only 1 confirmation,
	%% well within SEARCH_SPACE_UPPER_BOUND_DEPTH = 3.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	timer:sleep(2_000),
	%% The chunk should be indexed as unpacked at its immature offset
	%% (process_immature_chunk_offset path) but remain in the disk pool
	%% since MayConclude is forced to false for immature offsets.
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
%% Gap 4: Blacklisted byte skipped
%%
%% When a chunk's absolute offset is blacklisted, the matured chunk
%% offset processing should skip that offset. The chunk should still
%% be cleaned from the disk pool (MayConclude is preserved), but
%% should NOT appear in the sync record at the blacklisted offset.
%% -------------------------------------------------------------------

test_blacklisted_byte_skipped() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	{DataRoot, DataTree, Chunks} = make_chunk(),
	{TX, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Mine and confirm the chunk.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	%% Compute the absolute end offset of the chunk. The genesis weave is 3 chunks
	%% (3 * DATA_CHUNK_SIZE = 786432 bytes). Our TX adds DATA_CHUNK_SIZE more.
	%% The TX data starts at the weave size at the start of the block it was mined in.
	%% Get the actual offset from the TX index.
	{ok, {TXOffset, _}} = ar_data_sync:get_tx_offset(TX#tx.id),
	AbsoluteEndOffset = TXOffset + ?DATA_CHUNK_SIZE,
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
	%% The chunk should be cleaned from the disk pool (MayConclude preserved
	%% for blacklisted offsets) but NOT stored at the blacklisted offset.
	true = wait_until_disk_pool_empty(30_000),
	StoreID = ar_storage_module:id(hd(StorageModules)),
	?assertEqual(false,
		ar_sync_record:is_recorded(AbsoluteEndOffset, ar_data_sync, StoreID)),
	%% Clean up the blacklist entry.
	ar_ets_intervals:delete(ar_tx_blacklist_offsets, AbsoluteEndOffset,
		AbsoluteEndOffset - 1).

%% -------------------------------------------------------------------
%% Gap 6: Chunk cache full defers processing
%%
%% When the chunk cache is full, the matured chunk offset processing
%% should defer (MayConclude set to false) and retry later. Once the
%% cache drains, the chunk should be processed normally.
%% -------------------------------------------------------------------

test_chunk_cache_full_defers_processing() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	{DataRoot, DataTree, Chunks} = make_chunk(),
	{TX, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
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
%% Gap 7: Chunk data not found resilience
%%
%% When the chunk data file is missing from the default store during
%% matured chunk processing, an error is logged but the scan continues
%% without crashing. The chunk stays in the disk pool for retry.
%% -------------------------------------------------------------------

test_chunk_data_not_found_resilience() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	{DataRoot, DataTree, Chunks} = make_chunk(),
	{TX, _} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	post_chunk_proof(DataRoot, DataTree, Chunks),
	%% Wait for chunk to appear in disk pool.
	true = wait_until_disk_pool_not_empty(10_000),
	%% Get the chunk data key from the disk pool entry and delete the data.
	[{_Key, Value}] = ar_disk_pool:debug_get_chunks(),
	{_Offset, _ChunkSize, _DataRoot2, _TXSize, ChunkDataKey,
		_PB, _PS, _PR} = parse_disk_pool_chunk(Value),
	ok = ar_data_sync:delete_chunk_data(ChunkDataKey, "default"),
	%% Mine blocks to confirm and mature the chunk.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% Give the disk pool scan time to encounter the missing chunk data.
	%% The process should NOT crash — verify by checking it's still alive.
	timer:sleep(5_000),
	DefaultSyncPid = whereis(ar_data_sync:name("default")),
	?assertNotEqual(undefined, DefaultSyncPid),
	?assert(is_process_alive(DefaultSyncPid)).

%% -------------------------------------------------------------------
%% Gap 8: MayConclude accumulation across multiple offsets
%%
%% When the same data root appears at multiple weave offsets (same
%% data uploaded in different TXs), and one offset is immature while
%% another is mature, MayConclude should be false. The chunk stays
%% in the disk pool until all offsets are processed. After all offsets
%% mature, the chunk should be cleaned from the disk pool.
%% -------------------------------------------------------------------

test_may_conclude_accumulation() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
		ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
		#{ addr => Addr, storage_modules => StorageModules }),
	{DataRoot, DataTree, Chunks} = make_chunk(),
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
	%% The chunk should still be in the disk pool because TX2's offset
	%% is immature, forcing MayConclude = false.
	timer:sleep(5_000),
	DiskPoolChunks = ar_disk_pool:debug_get_chunks(),
	?assertNotEqual([], DiskPoolChunks),
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
