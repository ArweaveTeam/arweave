-module(arweave_entropy_preparation_SUITE).
-test_category([fast]).
-export([all/0, entropy_offsets/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [entropy_offsets].


-record(state, {
                store_id,
                packing,
                module_start,
                module_end,
                cursor,
                prepare_status = undefined,
                %% Footprints of each partition the module keeps.
                footprint_limit
               }).

-define(DEVICE_LOCK_WAIT, 100).


%%%===================================================================
%%% Tests.
%%%===================================================================

entropy_offsets_test_() ->
    ar_test_node:test_with_all_nodes_mocked([
                                             {arweave_lib_constants, strict_data_split_threshold, fun() -> 700_000 end}
                                            ],
                                            fun test_entropy_offsets/0, 30).

entropy_offsets(_Config) ->
    entropy_offsets_test_().



test_entropy_offsets() ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    ?assertEqual(2 * ?DATA_CHUNK_SIZE, SectorSize),

    Module0 = {0, arweave_lib_constants:partition_size(), unpacked},
    Module1 = {arweave_lib_constants:partition_size(), 2 * arweave_lib_constants:partition_size(), unpacked},

    {_ModuleStart0, ModuleEnd0} = arweave_storage_module:module_range(Module0),
    {_ModuleStart1, ModuleEnd1} = arweave_storage_module:module_range(Module1),

    PaddedModuleEnd0 = arweave_lib_constants:get_chunk_bucket_end(ModuleEnd0),
    PaddedModuleEnd1 = arweave_lib_constants:get_chunk_bucket_end(ModuleEnd1),

    ?assertEqual(2097152, PaddedModuleEnd0, "1"),
    ?assertEqual(4194304, PaddedModuleEnd1, "2"),

    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(0, PaddedModuleEnd0), "3"), %% bucket end: 262144
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(1000, PaddedModuleEnd0), "4"), %% bucket end: 262144
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(262144, PaddedModuleEnd0), "5"), %% bucket end: 262144

    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(524288, PaddedModuleEnd0), "6"), %% bucket end: 524288

    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(699999, PaddedModuleEnd0), "7"), %% bucket end: 524288
    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(700000, PaddedModuleEnd0), "8"), %% bucket end: 524288
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(700001, PaddedModuleEnd0), "9"), %% bucket end: 786432

    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(786432, PaddedModuleEnd0), "10"), %% bucket end: 786432
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(786433, PaddedModuleEnd0), "11"), %% bucket end: 786432
    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(1048576, PaddedModuleEnd0), "12"), %% bucket end: 1048576
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(1835007, PaddedModuleEnd0), "13"), %% bucket end: 1835008
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(1835008, PaddedModuleEnd0), "14"), %% bucket end: 1835008
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(1835009, PaddedModuleEnd0), "15"), %% bucket end: 1835008

    %% entropy partition is determined by the bucket *start* offset. So offsets that are in
    %% recall partition 1 may still be in entropy partition 0 (e.g. 2000001, 2097152)
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(1999999, PaddedModuleEnd0), "16"), %% bucket end: 1835008
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(2000000, PaddedModuleEnd0), "17"), %% bucket end: 1835008
    ?assertEqual([262144, 786432, 1310720, 1835008], arweave_entropy_preparation:entropy_offsets(2000001, PaddedModuleEnd0), "18"), %% bucket end: 1835008
    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(2097152, PaddedModuleEnd0), "19"), %% bucket end: 2097152
    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(2097153, PaddedModuleEnd0), "20"), %% bucket end: 2097152

    %% Even when ModuleEnd is high, we should limit entropy to the current entropy partition.
    ?assertEqual([524288, 1048576, 1572864, 2097152], arweave_entropy_preparation:entropy_offsets(2097152, PaddedModuleEnd1), "21"), %% bucket end: 2097152

    %% Retstrict offsets to module end.
    ?assertEqual([524288, 1048576, 1572864], arweave_entropy_preparation:entropy_offsets(2097152, 2_000_000), "22"), %% bucket end: 2097152

    %% Entropy partition 1
    ?assertEqual([2359296, 2883584, 3407872, 3932160], arweave_entropy_preparation:entropy_offsets(2359297, PaddedModuleEnd1), "23"), %% bucket end: 2359296
    ?assertEqual([2621440, 3145728, 3670016, 4194304], arweave_entropy_preparation:entropy_offsets(2621441, PaddedModuleEnd1), "24"), %% bucket end: 2621440

    ok.