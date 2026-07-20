-module(ar_sync_record_tests).


-include("ar.hrl").
-include("ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

sync_record_test_() ->
    [
        {timeout, ?TEST_NODE_TIMEOUT, fun test_sync_record/0}
    ].

test_sync_record() ->
    SleepTime = 1000,
    DiskPoolStart = ar_block:partition_size(),
    PartitionStart = ar_block:partition_size() - ?DATA_CHUNK_SIZE,
    WeaveSize = 4 * ?DATA_CHUNK_SIZE,
    [B0] = ar_weave:init([], 1, WeaveSize),
    RewardAddr = ar_test_node:generate_address(main),
    arweave_config:with_test_config(fun() ->
        Partition = {ar_block:partition_size(), 0, {spora_2_6, RewardAddr}},
        PartitionID = ar_storage_module:id(Partition),
        StorageModules = [arweave_config:storage_module_to_config(Partition)],
        ar_test_node:start(B0, RewardAddr, #{[storage_modules] => StorageModules}),
        Options = #{ format => etf, random_subset => false },

        %% Genesis data only
        ok = ar_test_await:global_sync_record_matches(Options, [{1048576, 0}]),
        ?assertEqual(not_found,
            ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, PartitionID)),

        %% Add a diskpool chunk
        ar_sync_record:add(
            DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, unpacked, ar_data_sync, ?DEFAULT_MODULE),
        ok = ar_test_await:global_sync_record_matches(Options,
            [{1048576, 0}, {DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart}]),
        ?assertEqual({DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart},
            ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, PartitionID)),

        %% Remove the diskpool chunk
        ar_sync_record:delete(
            DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, ar_data_sync, ?DEFAULT_MODULE),
        timer:sleep(SleepTime),
        {ok, Binary3} = ar_global_sync_record:get_serialized_sync_record(Options),
        {ok, Global3} = ar_intervals:safe_from_etf(Binary3),
        ?assertEqual([{1048576, 0},{DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart}],
            ar_intervals:to_list(Global3)),
        %% We need to explicitly declare global removal
        ar_events:send(sync_record,
                {global_remove_range, DiskPoolStart, DiskPoolStart+?DATA_CHUNK_SIZE}),
        ok = ar_test_await:global_sync_record_matches(Options, [{1048576, 0}]),

        %% Add a storage module chunk
        ar_sync_record:add(
            PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, unpacked, ar_data_sync, PartitionID),
        timer:sleep(SleepTime),
        {ok, Binary5} = ar_global_sync_record:get_serialized_sync_record(Options),
        {ok, Global5} = ar_intervals:safe_from_etf(Binary5),

        ?assertEqual([{1048576, 0},{PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
            ar_intervals:to_list(Global5)),
        ?assertEqual(not_found,
            ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, PartitionID)),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
                ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, PartitionID)),

        %% Remove the storage module chunk
        ar_sync_record:delete(
            PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, PartitionID),
        timer:sleep(SleepTime),
        ?assertEqual([{1048576, 0},{PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
            ar_intervals:to_list(Global5)),
        ar_events:send(sync_record,
                {global_remove_range, PartitionStart, PartitionStart+?DATA_CHUNK_SIZE}),
        ok = ar_test_await:global_sync_record_matches(Options, [{1048576, 0}]),
        ?assertEqual(not_found,
            ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, PartitionID)),
        ?assertEqual(not_found,
                ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, PartitionID)),

        %% Add chunk to both diskpool and storage module
        ar_sync_record:add(
            PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, unpacked, ar_data_sync, ?DEFAULT_MODULE),
        ar_sync_record:add(
            PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, unpacked, ar_data_sync, PartitionID),
        ok = ar_test_await:global_sync_record_matches(Options,
            [{1048576, 0}, {PartitionStart+?DATA_CHUNK_SIZE, PartitionStart}]),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE,PartitionStart},
            ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, PartitionID)),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
            ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, PartitionID)),

        %% Now remove it from just the diskpool
        ar_sync_record:delete(
            PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, ar_data_sync, ?DEFAULT_MODULE),
        timer:sleep(SleepTime),
        {ok, Binary7} = ar_global_sync_record:get_serialized_sync_record(Options),
        {ok, Global7} = ar_intervals:safe_from_etf(Binary7),

        ?assertEqual([{1048576, 0}, {PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
            ar_intervals:to_list(Global7)),
        ?assertEqual(not_found,
            ar_sync_record:get_interval(DiskPoolStart+1, ar_data_sync, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, ar_sync_record:get_interval(1, ar_data_sync, PartitionID)),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
            ar_sync_record:get_interval(PartitionStart+1, ar_data_sync, PartitionID)),

        ar_test_node:stop()
    end).
