-module(ar_sync_record_tests).

-include("ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Joining with genesis data populates both the store and global sync records.
sync_record_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun genesis_data_populates_sync_records/0}.

%% @doc Real node startup records genesis chunks without disk-pool-only ranges.
genesis_data_populates_sync_records() ->
    PartitionEnd = arweave_constants:partition_size(),
    %% Four genesis chunks leave a gap before the partition's end.
    WeaveSize = 4 * ?DATA_CHUNK_SIZE,
    [B0] = ar_weave:init([], 1, WeaveSize),
    RewardAddr = ar_test_node:generate_address(main),
    arweave_config:internal_with_test_config(fun() ->
        Partition = {0, PartitionEnd, {spora_2_6, RewardAddr}},
        #store_info{id = PartitionID} = arweave_storage:store_info(Partition),
        ar_test_node:start(B0, RewardAddr, #{[storage_modules] => [Partition]}),
        try
            Options = #{format => etf, random_subset => false},
            ?assertEqual(
                ok,
                ar_test_await:global_sync_record_matches(Options, [
                    {WeaveSize, 0}
                ])
            ),
            ?assertEqual(
                not_found,
                arweave_storage:get_interval(
                    PartitionEnd + 1,
                    any_packing,
                    {ar_data_sync, byte},
                    ?DEFAULT_MODULE
                )
            ),
            ?assertEqual(
                {WeaveSize, 0},
                arweave_storage:get_interval(
                    1, any_packing, {ar_data_sync, byte}, PartitionID
                )
            )
        after
            ar_test_node:stop()
        end
    end).
