-module(ar_sync_record_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

sync_record_test_() ->
	[
		{timeout, 120, fun test_sync_record/0}
	].

test_sync_record() ->
	SleepTime = 1000,
	DiskPoolStart = ?PARTITION_SIZE,
	PartitionStart = ?PARTITION_SIZE - ?DATA_CHUNK_SIZE,
	WeaveSize = 4 * ?DATA_CHUNK_SIZE,
	[B0] = ar_weave:init([], 1, WeaveSize),
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	{ok, Config} = application:get_env(arweave, config),
	try
		CompositePartition = {?PARTITION_SIZE, 0, {composite, RewardAddr, 1}},
		CompositePartitionID = ar_storage_module:id(CompositePartition),
		%% Add a replica.2.9 partition, which should be ignored
		ReplicaPartition = {?PARTITION_SIZE, 0, {replica_2_9, RewardAddr}},
		ReplicaPartitionID = ar_storage_module:id(ReplicaPartition),
		StorageModules = [CompositePartition, ReplicaPartition],
		ar_test_node:start(B0, RewardAddr, Config, StorageModules),
		Options = #{ format => etf, random_subset => false },

		%% Genesis data only
		{ok, Binary1} = ar_global_sync_record:get_serialized_sync_record(Options),
		{ok, Global1} = ar_intervals:safe_from_etf(Binary1),

		?assertEqual([{1048576, 0}], ar_intervals:to_list(Global1)),
		?assertEqual(not_found,
			ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, "default")),
		?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, CompositePartitionID)),

		%% Add a replica.2.9 chunk, which should be ignored
		ar_sync_record:add(DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, ar_data_sync, ReplicaPartitionID),
		%% Add a diskpool chunk
		ar_sync_record:add(
			DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, ar_data_sync, "default"),
		timer:sleep(SleepTime),
		{ok, Binary2} = ar_global_sync_record:get_serialized_sync_record(Options),
		{ok, Global2} = ar_intervals:safe_from_etf(Binary2),

		?assertEqual([{1048576, 0},{DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart}],
			ar_intervals:to_list(Global2)),
		?assertEqual({DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart},
			ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, "default")),
		?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, CompositePartitionID)),

		%% Remove the diskpool chunk
		ar_sync_record:delete(
			DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, ar_data_sync, "default"),
		timer:sleep(SleepTime),
		{ok, Binary3} = ar_global_sync_record:get_serialized_sync_record(Options),
		{ok, Global3} = ar_intervals:safe_from_etf(Binary3),
		?assertEqual([{1048576, 0},{DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart}],
			ar_intervals:to_list(Global3)),
		%% We need to explicitly declare global removal
		ar_events:send(sync_record,
				{global_remove_range, DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart}),
		true = ar_util:do_until(
				fun() ->
					{ok, Binary4} = ar_global_sync_record:get_serialized_sync_record(Options),
					{ok, Global4} = ar_intervals:safe_from_etf(Binary4),
					[{1048576, 0}] == ar_intervals:to_list(Global4) end,
				200,
				5000),

		%% Add a storage module chunk
		ar_sync_record:add(
			PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, CompositePartitionID),
		timer:sleep(SleepTime),
		{ok, Binary5} = ar_global_sync_record:get_serialized_sync_record(Options),
		{ok, Global5} = ar_intervals:safe_from_etf(Binary5),

		?assertEqual([{1048576, 0},{PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
			ar_intervals:to_list(Global5)),
		?assertEqual(not_found,
			ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, "default")),
		?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, CompositePartitionID)),
		?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
				ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, CompositePartitionID)),

		%% Remove the storage module chunk
		ar_sync_record:delete(
			PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, CompositePartitionID),
		timer:sleep(SleepTime),
		?assertEqual([{1048576, 0},{PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
			ar_intervals:to_list(Global5)),
		ar_events:send(sync_record,
				{global_remove_range, PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}),
		true = ar_util:do_until(
				fun() ->
					{ok, Binary6} = ar_global_sync_record:get_serialized_sync_record(Options),
					{ok, Global6} = ar_intervals:safe_from_etf(Binary6),
					[{1048576, 0}] == ar_intervals:to_list(Global6) end,
				200,
				1000),
		?assertEqual(not_found,
			ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, "default")),
		?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, CompositePartitionID)),
		?assertEqual(not_found,
				ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, CompositePartitionID)),

		%% Add chunk to both diskpool and storage module
		ar_sync_record:add(
			PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, "default"),
		ar_sync_record:add(
			PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, CompositePartitionID),
		timer:sleep(SleepTime),
		{ok, Binary6} = ar_global_sync_record:get_serialized_sync_record(Options),
		{ok, Global6} = ar_intervals:safe_from_etf(Binary6),

		?assertEqual([{1048576, 0}, {PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
			ar_intervals:to_list(Global6)),
		?assertEqual({PartitionStart+?DATA_CHUNK_SIZE,PartitionStart},
			ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, "default")),
		?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, CompositePartitionID)),
		?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
			ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, CompositePartitionID)),

		%% Now remove it from just the diskpool
		ar_sync_record:delete(
			PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, "default"),
		timer:sleep(SleepTime),
		{ok, Binary7} = ar_global_sync_record:get_serialized_sync_record(Options),
		{ok, Global7} = ar_intervals:safe_from_etf(Binary7),

		?assertEqual([{1048576, 0}, {PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
			ar_intervals:to_list(Global7)),
		?assertEqual(not_found,
			ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, "default")),
		?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, CompositePartitionID)),
		?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
			ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, CompositePartitionID)),

		ar_test_node:stop()
	after
		ok = application:set_env(arweave, config, Config)
	end.
