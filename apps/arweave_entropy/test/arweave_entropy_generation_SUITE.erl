-module(arweave_entropy_generation_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [entropy_offsets].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    meck:new(arweave_constants, [passthrough]),
    meck:expect(arweave_constants, strict_data_split_threshold, fun() ->
        700_000
    end),
    Config.

end_per_testcase(_, _) ->
    meck:unload(arweave_constants).

%%====================================================================
%% Test cases
%%====================================================================

entropy_offsets(_Config) ->
    SectorSize = arweave_constants:get_replica_2_9_entropy_sector_size(),
    ?assertEqual(2 * ?DATA_CHUNK_SIZE, SectorSize),

    Module0 = {0, arweave_constants:partition_size(), unpacked},
    Module1 = {
        arweave_constants:partition_size(),
        2 * arweave_constants:partition_size(),
        unpacked
    },

    #store_info{effective_range = {_ModuleStart0, ModuleEnd0}} =
        arweave_storage:store_info(Module0),
    #store_info{effective_range = {_ModuleStart1, ModuleEnd1}} =
        arweave_storage:store_info(Module1),

    PaddedModuleEnd0 = arweave_storage:get_chunk_bucket_end(ModuleEnd0),
    PaddedModuleEnd1 = arweave_storage:get_chunk_bucket_end(ModuleEnd1),

    ?assertEqual(2097152, PaddedModuleEnd0, "1"),
    ?assertEqual(4194304, PaddedModuleEnd1, "2"),

    %% bucket end: 262144
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(0, PaddedModuleEnd0),
        "3"
    ),
    %% bucket end: 262144
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(1000, PaddedModuleEnd0),
        "4"
    ),
    %% bucket end: 262144
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(262144, PaddedModuleEnd0),
        "5"
    ),

    %% bucket end: 524288
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(524288, PaddedModuleEnd0),
        "6"
    ),

    %% bucket end: 524288
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(699999, PaddedModuleEnd0),
        "7"
    ),
    %% bucket end: 524288
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(700000, PaddedModuleEnd0),
        "8"
    ),
    %% bucket end: 786432
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(700001, PaddedModuleEnd0),
        "9"
    ),

    %% bucket end: 786432
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(786432, PaddedModuleEnd0),
        "10"
    ),
    %% bucket end: 786432
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(786433, PaddedModuleEnd0),
        "11"
    ),
    %% bucket end: 1048576
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(1048576, PaddedModuleEnd0),
        "12"
    ),
    %% bucket end: 1835008
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(1835007, PaddedModuleEnd0),
        "13"
    ),
    %% bucket end: 1835008
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(1835008, PaddedModuleEnd0),
        "14"
    ),
    %% bucket end: 1835008
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(1835009, PaddedModuleEnd0),
        "15"
    ),

    %% entropy partition is determined by the bucket *start* offset. So offsets that are in
    %% recall partition 1 may still be in entropy partition 0 (e.g. 2000001, 2097152)

    %% bucket end: 1835008
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(1999999, PaddedModuleEnd0),
        "16"
    ),
    %% bucket end: 1835008
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(2000000, PaddedModuleEnd0),
        "17"
    ),
    %% bucket end: 1835008
    ?assertEqual(
        [262144, 786432, 1310720, 1835008],
        arweave_entropy_generation:entropy_offsets(2000001, PaddedModuleEnd0),
        "18"
    ),
    %% bucket end: 2097152
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(2097152, PaddedModuleEnd0),
        "19"
    ),
    %% bucket end: 2097152
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(2097153, PaddedModuleEnd0),
        "20"
    ),

    %% Even when ModuleEnd is high, we should limit entropy to the current entropy partition.

    %% bucket end: 2097152
    ?assertEqual(
        [524288, 1048576, 1572864, 2097152],
        arweave_entropy_generation:entropy_offsets(2097152, PaddedModuleEnd1),
        "21"
    ),

    %% Restrict offsets to module end.

    %% bucket end: 2097152
    ?assertEqual(
        [524288, 1048576, 1572864],
        arweave_entropy_generation:entropy_offsets(2097152, 2_000_000),
        "22"
    ),

    %% Entropy partition 1

    %% bucket end: 2359296
    ?assertEqual(
        [2359296, 2883584, 3407872, 3932160],
        arweave_entropy_generation:entropy_offsets(2359297, PaddedModuleEnd1),
        "23"
    ),
    %% bucket end: 2621440
    ?assertEqual(
        [2621440, 3145728, 3670016, 4194304],
        arweave_entropy_generation:entropy_offsets(2621441, PaddedModuleEnd1),
        "24"
    ),

    ok.
