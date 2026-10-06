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
    DiskPoolStart = arweave_lib_constants:partition_size(),
    PartitionStart = arweave_lib_constants:partition_size() - ?DATA_CHUNK_SIZE,
    WeaveSize = 4 * ?DATA_CHUNK_SIZE,
    [B0] = ar_weave:init([], 1, WeaveSize),
    RewardAddr = ar_test_node:generate_address(main),
    arweave_config:internal_with_test_config(fun() ->
        Partition = {0, arweave_lib_constants:partition_size(), {spora_2_6, RewardAddr}},
        PartitionID = arweave_storage_module:id(Partition),
        ar_test_node:start(B0, RewardAddr, #{[storage_modules] => [Partition]}),
        Options = #{ format => etf, random_subset => false },

        %% Genesis data only
        ok = ar_test_await:global_sync_record_matches(Options, [{1048576, 0}]),
        ?assertEqual(not_found,
            arweave_storage:get_interval(DiskPoolStart+1, any_packing, {ar_data_sync, byte}, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, arweave_storage:get_interval(1, any_packing, {ar_data_sync, byte}, PartitionID)),

        %% Add a diskpool chunk
        arweave_storage:add_sync_record(DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, unpacked, {ar_data_sync, byte}, ?DEFAULT_MODULE),
        ok = ar_test_await:global_sync_record_matches(Options,
            [{1048576, 0}, {DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart}]),
        ?assertEqual({DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart},
            arweave_storage:get_interval(DiskPoolStart+1, any_packing, {ar_data_sync, byte}, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, arweave_storage:get_interval(1, any_packing, {ar_data_sync, byte}, PartitionID)),

        %% Remove the diskpool chunk
        arweave_storage:delete_sync_record(DiskPoolStart+?DATA_CHUNK_SIZE, DiskPoolStart, {ar_data_sync, byte}, ?DEFAULT_MODULE),
        timer:sleep(SleepTime),
        {ok, Binary3} = arweave_storage_global_sync_record:get_serialized_sync_record(Options),
        {ok, Global3} = arweave_lib_intervals:safe_from_etf(Binary3),
        ?assertEqual([{1048576, 0},{DiskPoolStart+?DATA_CHUNK_SIZE,DiskPoolStart}],
            arweave_lib_intervals:to_list(Global3)),
        %% We need to explicitly declare global removal
        ar_events:send(sync_record,
                {global_remove_range, DiskPoolStart, DiskPoolStart+?DATA_CHUNK_SIZE}),
        ok = ar_test_await:global_sync_record_matches(Options, [{1048576, 0}]),

        %% Add a storage module chunk
        arweave_storage:add_sync_record(PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, unpacked, {ar_data_sync, byte}, PartitionID),
        timer:sleep(SleepTime),
        {ok, Binary5} = arweave_storage_global_sync_record:get_serialized_sync_record(Options),
        {ok, Global5} = arweave_lib_intervals:safe_from_etf(Binary5),

        ?assertEqual([{1048576, 0},{PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
            arweave_lib_intervals:to_list(Global5)),
        ?assertEqual(not_found,
            arweave_storage:get_interval(DiskPoolStart+1, any_packing, {ar_data_sync, byte}, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, arweave_storage:get_interval(1, any_packing, {ar_data_sync, byte}, PartitionID)),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
                arweave_storage:get_interval(PartitionStart+1, any_packing, {ar_data_sync, byte}, PartitionID)),

        %% Remove the storage module chunk
        arweave_storage:delete_sync_record(PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, {ar_data_sync, byte}, PartitionID),
        timer:sleep(SleepTime),
        ?assertEqual([{1048576, 0},{PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
            arweave_lib_intervals:to_list(Global5)),
        ar_events:send(sync_record,
                {global_remove_range, PartitionStart, PartitionStart+?DATA_CHUNK_SIZE}),
        ok = ar_test_await:global_sync_record_matches(Options, [{1048576, 0}]),
        ?assertEqual(not_found,
            arweave_storage:get_interval(DiskPoolStart+1, any_packing, {ar_data_sync, byte}, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, arweave_storage:get_interval(1, any_packing, {ar_data_sync, byte}, PartitionID)),
        ?assertEqual(not_found,
                arweave_storage:get_interval(PartitionStart+1, any_packing, {ar_data_sync, byte}, PartitionID)),

        %% Add chunk to both diskpool and storage module
        arweave_storage:add_sync_record(PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, unpacked, {ar_data_sync, byte}, ?DEFAULT_MODULE),
        arweave_storage:add_sync_record(PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, unpacked, {ar_data_sync, byte}, PartitionID),
        ok = ar_test_await:global_sync_record_matches(Options,
            [{1048576, 0}, {PartitionStart+?DATA_CHUNK_SIZE, PartitionStart}]),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE,PartitionStart},
            arweave_storage:get_interval(PartitionStart+1, any_packing, {ar_data_sync, byte}, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, arweave_storage:get_interval(1, any_packing, {ar_data_sync, byte}, PartitionID)),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
            arweave_storage:get_interval(PartitionStart+1, any_packing, {ar_data_sync, byte}, PartitionID)),

        %% Now remove it from just the diskpool
        arweave_storage:delete_sync_record(PartitionStart+?DATA_CHUNK_SIZE, PartitionStart, {ar_data_sync, byte}, ?DEFAULT_MODULE),
        timer:sleep(SleepTime),
        {ok, Binary7} = arweave_storage_global_sync_record:get_serialized_sync_record(Options),
        {ok, Global7} = arweave_lib_intervals:safe_from_etf(Binary7),

        ?assertEqual([{1048576, 0}, {PartitionStart+?DATA_CHUNK_SIZE,PartitionStart}],
            arweave_lib_intervals:to_list(Global7)),
        ?assertEqual(not_found,
            arweave_storage:get_interval(DiskPoolStart+1, any_packing, {ar_data_sync, byte}, ?DEFAULT_MODULE)),
        ?assertEqual({1048576, 0}, arweave_storage:get_interval(1, any_packing, {ar_data_sync, byte}, PartitionID)),
        ?assertEqual({PartitionStart+?DATA_CHUNK_SIZE, PartitionStart},
            arweave_storage:get_interval(PartitionStart+1, any_packing, {ar_data_sync, byte}, PartitionID)),

        ar_test_node:stop()
    end).
