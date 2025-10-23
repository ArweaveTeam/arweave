-module(ar_sync_pack_mine_tests).

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%% --------------------------------------------------------------------------------------------
%% Fixtures
%% --------------------------------------------------------------------------------------------
setup_source_node(PackingType) ->
	SourceNode = peer1,
	SinkNode = peer2,
	ar_test_node:stop(SinkNode),
	ar_test_node:stop(SourceNode),
	{Blocks, _SourceAddr, Chunks} = ar_e2e:start_source_node(SourceNode, PackingType, wallet_a),

	{Blocks, Chunks, PackingType}.

instantiator(GenesisData, SinkPackingType, TestFun) ->
	{timeout, 600, {with, {GenesisData, SinkPackingType}, [TestFun]}}.
	
%% --------------------------------------------------------------------------------------------
%% Test Registration
%% --------------------------------------------------------------------------------------------

replica_2_9_syncing_test_() ->
	{setup, fun () -> setup_source_node(replica_2_9) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, replica_2_9, fun test_sync_pack_mine/1),
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

spora_2_6_sync_pack_mine_test_() ->
	{setup, fun () -> setup_source_node(spora_2_6) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, replica_2_9, fun test_sync_pack_mine/1),
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

unpacked_sync_pack_mine_test_() ->
	{setup, fun () -> setup_source_node(unpacked) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, replica_2_9, fun test_sync_pack_mine/1),
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

% Note: we should limit the number of tests run per setup_source_node to 5, if it gets
% too long then the source node may hit a difficulty adjustment, which can impact the
% results.
unpacked_edge_case_test_() ->
	{setup, fun () -> setup_source_node(unpacked) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, {replica_2_9, unpacked}, 
						fun test_unpacked_and_packed_sync_pack_mine/1),
					instantiator(GenesisData, {unpacked, replica_2_9}, 
						fun test_unpacked_and_packed_sync_pack_mine/1),
					instantiator(GenesisData, replica_2_9, 
						fun test_entropy_first_sync_pack_mine/1),
					instantiator(GenesisData, replica_2_9, 
						fun test_entropy_last_sync_pack_mine/1)
				]
		end}.

spora_2_6_edge_case_test_() ->
	{setup, fun () -> setup_source_node(spora_2_6) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, {replica_2_9, unpacked}, 
						fun test_unpacked_and_packed_sync_pack_mine/1),
					instantiator(GenesisData, {unpacked, replica_2_9}, 
						fun test_unpacked_and_packed_sync_pack_mine/1),
					instantiator(GenesisData, replica_2_9, 
						fun test_entropy_first_sync_pack_mine/1),
					instantiator(GenesisData, replica_2_9, 
						fun test_entropy_last_sync_pack_mine/1)
				]
		end}.

unpacked_small_module_test_() ->
	{setup, fun () -> setup_source_node(unpacked) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, replica_2_9, 
						fun test_small_module_aligned_sync_pack_mine/1),
					instantiator(GenesisData, replica_2_9, 
						fun test_small_module_unaligned_sync_pack_mine/1)
				]
	end}.
	
spora_2_6_small_module_test_() ->
	{setup, fun () -> setup_source_node(spora_2_6) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, replica_2_9, 
						fun test_small_module_aligned_sync_pack_mine/1),
					instantiator(GenesisData, replica_2_9, 
						fun test_small_module_unaligned_sync_pack_mine/1)
				]
		end}.

disk_pool_threshold_test_() ->
	[
		instantiator(unpacked, replica_2_9, fun test_disk_pool_threshold/1),
		instantiator(unpacked, spora_2_6, fun test_disk_pool_threshold/1),
		instantiator(spora_2_6, replica_2_9, fun test_disk_pool_threshold/1),
		instantiator(spora_2_6, spora_2_6, fun test_disk_pool_threshold/1),
		instantiator(spora_2_6, unpacked, fun test_disk_pool_threshold/1)
	].

%% --------------------------------------------------------------------------------------------
%% test_sync_pack_mine
%% --------------------------------------------------------------------------------------------
test_sync_pack_mine({{Blocks, Chunks, SourcePackingType}, SinkPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType]),
	?LOG_INFO([{event, test_sync_pack_mine}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	SinkPacking = start_sink_node(SinkNode, SourceNode, B0, SinkPackingType),

	RangeStart = ar_block:partition_size(),
	RangeEnd = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking),

	%% Partition 1 and half of partition 2 are below the disk pool threshold
	ar_e2e:assert_syncs_range(SinkNode,	SinkPacking, RangeStart, RangeEnd),
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

	case SinkPackingType of
		unpacked ->
			ok;
		_ ->
			ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),
			ok
	end.

test_unpacked_and_packed_sync_pack_mine(
		{{Blocks, _Chunks, SourcePackingType}, {PackingType1, PackingType2}}) ->
	ar_e2e:delayed_print(<<" ~p -> {~p, ~p} ">>, [SourcePackingType, PackingType1, PackingType2]),
	?LOG_INFO([{event, test_unpacked_and_packed_sync_pack_mine}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, {PackingType1, PackingType2}}]),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	{SinkPacking1, SinkPacking2} = start_sink_node(
		SinkNode, SourceNode, B0, PackingType1, PackingType2),

	RangeStart1 = ar_block:partition_size(),
	RangeEnd1 = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking1),

	%% Data exists as both packed and unmpacked, so will exist in the global sync record
	%% even though replica_2_9 data is filtered out.
	ar_e2e:assert_syncs_range(SinkNode, RangeStart1, RangeEnd1),
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking1),
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking2),
	%% XXX: we should be able to assert the chunks here, but since we have two
	%% storage modules configured and are querying the replica_2_9 chunk, GET /chunk gets
	%% confused and tries to load the unpacked chunk, which then fails within the middleware
	%% handler and 404s. To fix we'd need to update GET /chunk to query all matching
	%% storage modules and then find the best one to return. But since this is a rare edge
	%% case, we'll just disable the assertion for now.
	%% ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),
	
	MinablePacking = case PackingType1 of
		unpacked -> SinkPacking2;
		_ -> SinkPacking1
	end,
	ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, MinablePacking),
	ok.
	

test_entropy_first_sync_pack_mine({{Blocks, Chunks, SourcePackingType}, SinkPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType]),
	?LOG_INFO([{event, test_entropy_first_sync_pack_mine}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),
	{ok, Config} = ar_test_node:get_config(SinkNode),
	
	Module = {ar_block:partition_size(), 1, SinkPacking},
	StoreID = ar_storage_module:id(Module),
	StorageModules = [ Module ],


	%% 1. Run node with no sync jobs so that it only prepares entropy
	Config2 = Config#config{
		peers = [ar_test_node:peer_ip(SourceNode)],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true,
		mining_addr = SinkAddr,
		sync_jobs = 0
	},
	?assertEqual(ar_test_node:peer_name(SinkNode),
		ar_test_node:start_other_node(SinkNode, B0, Config2, true)
	),

	RangeStart = ar_block:partition_size(),
	RangeEnd = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking),

	ar_e2e:assert_has_entropy(SinkNode, RangeStart, RangeEnd, StoreID),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked_padded),

	%% Delete two chunks of entropy from storage to test that the node will heal itself.
	%% 1. Delete the chunk from disk as well as all sync records.
	%% 2. Delete the chunk only from disk, but keep it in the sync records.
	DeleteOffset1 = RangeStart + ?DATA_CHUNK_SIZE,
	ar_test_node:remote_call(SinkNode, ar_chunk_storage, delete,
		[DeleteOffset1, StoreID]),
	DeleteOffset2 = DeleteOffset1 + ?DATA_CHUNK_SIZE,
	ar_test_node:remote_call(SinkNode, ar_chunk_storage, delete_chunk,
		[DeleteOffset2, StoreID]),

	%% 2. Run node with sync jobs so that it syncs and packs data
	ar_test_node:restart_with_config(SinkNode, Config2#config{
		sync_jobs = 100
	}),

	ar_e2e:assert_syncs_range(SinkNode, SinkPacking, RangeStart, RangeEnd),
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked_padded),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

	%% 3. Make sure the data is minable
	ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),
	ok.

test_entropy_last_sync_pack_mine({{Blocks, Chunks, SourcePackingType}, SinkPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType]),
	?LOG_INFO([{event, test_entropy_last_sync_pack_mine}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),
	{ok, Config} = ar_test_node:get_config(SinkNode),
	
	Module = {ar_block:partition_size(), 1, SinkPacking},
	StoreID = ar_storage_module:id(Module),
	StorageModules = [ Module ],

	%% 1. Run node with no replica_2_9 workers so that it only syncs chunks
	Config2 = Config#config{
		peers = [ar_test_node:peer_ip(SourceNode)],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true,
		mining_addr = SinkAddr,
		replica_2_9_workers = 0
	},
	?assertEqual(ar_test_node:peer_name(SinkNode),
		ar_test_node:start_other_node(SinkNode, B0, Config2, true)
	),

	RangeStart = ar_block:partition_size(),
	RangeEnd = 2*ar_block:partition_size() + ar_storage_module:get_overlap(SinkPacking),

	ar_e2e:assert_syncs_range(SinkNode, SinkPacking, RangeStart, RangeEnd),
	ar_e2e:assert_partition_size(SinkNode, 1, unpacked_padded),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked),

	%% 2. Run node with sync jobs so that it syncs and packs data
	ar_test_node:restart_with_config(SinkNode, Config2#config{
		replica_2_9_workers = 8
	}),

	ar_e2e:assert_has_entropy(SinkNode, RangeStart, RangeEnd, StoreID),
	ar_e2e:assert_syncs_range(SinkNode, SinkPacking, RangeStart, RangeEnd),
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked_padded),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

	%% 3. Make sure the data is minable
	ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),
	ok.

test_small_module_aligned_sync_pack_mine({{Blocks, Chunks, SourcePackingType}, SinkPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType]),
	?LOG_INFO([{event, test_small_module_aligned_sync_pack_mine}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),
	{ok, Config} = ar_test_node:get_config(SinkNode),

	Module = {floor(0.5 * ar_block:partition_size()), 2, SinkPacking},
	StoreID = ar_storage_module:id(Module),
	StorageModules = [ Module ],

	%% Sync the second half of partition 1
	Config2 = Config#config{
		peers = [ar_test_node:peer_ip(SourceNode)],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true,
		mining_addr = SinkAddr
	},
	?assertEqual(ar_test_node:peer_name(SinkNode),
		ar_test_node:start_other_node(SinkNode, B0, Config2, true)
	),

	RangeStart = floor(ar_block:partition_size()),
	RangeEnd = floor(1.5 * ar_block:partition_size()),
	Partition = ar_node:get_partition_number(RangeStart),
	RangeSize = ar_e2e:aligned_partition_size(SinkNode, Partition, SinkPacking),

	%% Make sure the expected data was synced
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking, RangeSize),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked_padded),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, lists:sublist(Chunks, 1, 4)),
	ar_e2e:assert_syncs_range(SinkNode, SinkPacking, RangeStart, RangeEnd),

	%% Make sure no extra entropy was generated
	AlignedStart = ar_util:floor_int(RangeStart, ?DATA_CHUNK_SIZE),
	AlignedEnd = ar_util:ceil_int(RangeEnd, ?DATA_CHUNK_SIZE),
	ar_e2e:assert_has_entropy(SinkNode, AlignedStart, AlignedEnd, StoreID),
	ar_e2e:assert_no_entropy(SinkNode, AlignedEnd, 2 * ar_block:partition_size(), StoreID),

	%% Make sure the data is minable
	ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),
	ok.

test_small_module_unaligned_sync_pack_mine({{Blocks, Chunks, SourcePackingType}, SinkPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType]),
	?LOG_INFO([{event, test_small_module_unaligned_sync_pack_mine}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	Wallet = ar_test_node:remote_call(SinkNode, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = ar_e2e:packing_type_to_packing(SinkPackingType, SinkAddr),
	{ok, Config} = ar_test_node:get_config(SinkNode),

	Module = {floor(0.5 * ar_block:partition_size()), 3, SinkPacking},
	StoreID = ar_storage_module:id(Module),
	StorageModules = [ Module ],

	%% Sync the second half of partition 1
	Config2 = Config#config{
		peers = [ar_test_node:peer_ip(SourceNode)],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true,
		mining_addr = SinkAddr
	},
	?assertEqual(ar_test_node:peer_name(SinkNode),
		ar_test_node:start_other_node(SinkNode, B0, Config2, true)
	),

	RangeStart = floor(1.5 * ar_block:partition_size()),
	RangeEnd = floor(2 * ar_block:partition_size()),
	Partition = ar_node:get_partition_number(RangeStart),
	RangeSize = ar_e2e:aligned_partition_size(SinkNode, Partition, SinkPacking),

	%% Make sure the expected data was synced	
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking, RangeSize),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked_padded),
	ar_e2e:assert_empty_partition(SinkNode, 1, unpacked),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, lists:sublist(Chunks, 5, 8)),
	%% Even though the packing type is replica_2_9, the data will still exist in the
	%% default partition as unpacked - and so will exist in the global sync record.
	ar_e2e:assert_syncs_range(SinkNode, RangeStart, RangeEnd),

	%% Make sure no extra entropy was generated
	AlignedStart = ar_util:floor_int(RangeStart, ?DATA_CHUNK_SIZE),
	AlignedEnd = ar_util:ceil_int(RangeEnd, ?DATA_CHUNK_SIZE),
	ar_e2e:assert_has_entropy(SinkNode, AlignedStart, AlignedEnd, StoreID),
	ar_e2e:assert_no_entropy(SinkNode, 0, AlignedStart, StoreID),

	%% Make sure the data is minable
	ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),
	ok.

test_disk_pool_threshold({SourcePackingType, SinkPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType]),
	?LOG_INFO([{event, test_disk_pool_threshold}, {module, ?MODULE},
		{from_packing_type, SourcePackingType}, {to_packing_type, SinkPackingType}]),

	SourceNode = peer1,
	SinkNode = peer2,

	%% When the source packing type is unpacked, this setup process performs some
	%% extra disk pool checks:
	%% 1. spin up a spora_2_6 node and mine some blocks
	%% 2. some chunks are below the disk pool threshold and some above
	%% 3. spin up an unpacked node and sync from spora_2_6
	%% 4. shut down the spora_2_6 node
	%% 5. now the unpacked node should have synced all of the chunks, both above and below
	%%    the disk pool threshold
	%% 6. proceed with test and spin up the sink node and confirm it too can sink all chunks
	%%    from the unpacked source node - both above and below the disk pool threshold
	{Blocks, Chunks, SourcePackingType} = setup_source_node(SourcePackingType),
	[B0 | _] = Blocks,

	SinkPacking = start_sink_node(SinkNode, SourceNode, B0, SinkPackingType),
	RangeStart = floor(2 * ar_block:partition_size()),
	RangeEnd = floor(2.5 * ar_block:partition_size()),
	RangeSize = ar_util:ceil_int(RangeEnd, ?DATA_CHUNK_SIZE) - ar_util:floor_int(RangeStart, ?DATA_CHUNK_SIZE),

	%% Partition 1 and half of partition 2 are below the disk pool threshold
	ar_e2e:assert_syncs_range(SinkNode, SinkPacking, ar_block:partition_size(), 4*ar_block:partition_size()),
	ar_e2e:assert_partition_size(SinkNode, 1, SinkPacking),
	ar_e2e:assert_partition_size(SinkNode, 2, SinkPacking, RangeSize),
	ar_e2e:assert_empty_partition(SinkNode, 3, SinkPacking),
	ar_e2e:assert_does_not_sync_range(SinkNode, 0, ar_block:partition_size()),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

	case SinkPackingType of
		unpacked ->
			ok;
		_ ->
			ar_e2e:assert_mine_and_validate(SinkNode, SourceNode, SinkPacking),

			%% Now that we mined a block, the rest of partition 2 is below the disk pool
			%% threshold
			ar_e2e:assert_syncs_range(SinkNode, SinkPacking, ar_block:partition_size(), 4*ar_block:partition_size()),
			ar_e2e:assert_partition_size(SinkNode, 2, SinkPacking, ar_util:ceil_int(ar_block:partition_size(), ?DATA_CHUNK_SIZE)),
			%% All of partition 3 is still above the disk pool threshold,
			%% except for one chunk crossing the disk pool threshold: 6291456 > 6029312.
			%% The overlap chunk is NOT synced because its end offset is above
			%% the threshold so the disk pool process does not consider it mature.
			ar_e2e:assert_partition_size(SinkNode, 3, SinkPacking, ?DATA_CHUNK_SIZE),
			ar_e2e:assert_does_not_sync_range(SinkNode, 0, ar_block:partition_size()),
			ok
	end.

start_sink_node(Node, SourceNode, B0, PackingType) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = ar_e2e:packing_type_to_packing(PackingType, SinkAddr),
	{ok, Config} = ar_test_node:get_config(Node),
	
	StorageModules = [
		{ar_block:partition_size(), 1, SinkPacking},
		{ar_block:partition_size(), 2, SinkPacking},
		{ar_block:partition_size(), 3, SinkPacking},
		{ar_block:partition_size(), 4, SinkPacking},
		{ar_block:partition_size(), 5, SinkPacking},
		{ar_block:partition_size(), 6, SinkPacking},
		{ar_block:partition_size(), 10, SinkPacking}
	],
	?assertEqual(ar_test_node:peer_name(Node),
		ar_test_node:start_other_node(Node, B0, Config#config{
			peers = [ar_test_node:peer_ip(SourceNode)],
			start_from_latest_state = true,
			storage_modules = StorageModules,
			auto_join = true,
			mining_addr = SinkAddr
		}, true)
	),

	SinkPacking.

start_sink_node(Node, SourceNode, B0, PackingType1, PackingType2) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking1 = ar_e2e:packing_type_to_packing(PackingType1, SinkAddr),
	SinkPacking2 = ar_e2e:packing_type_to_packing(PackingType2, SinkAddr),
	{ok, Config} = ar_test_node:get_config(Node),
	
	StorageModules = [
		{ar_block:partition_size(), 1, SinkPacking1},
		{ar_block:partition_size(), 1, SinkPacking2}
	],

	?assertEqual(ar_test_node:peer_name(Node),
		ar_test_node:start_other_node(Node, B0, Config#config{
			peers = [ar_test_node:peer_ip(SourceNode)],
			start_from_latest_state = true,
			storage_modules = StorageModules,
			auto_join = true,
			mining_addr = SinkAddr
		}, true)
	),
	{SinkPacking1, SinkPacking2}.
