-module(arweave_storage_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        metadata_without_host,
        module_metadata_packings,
        module_metadata_repack_and_default,
        module_metadata_before_storage_start,
        module_metadata_legacy_directory,
        module_metadata_paths_follow_data_dir,
        concurrent_metadata_labels,
        public_api_delegates,
        public_api_hides_storage_internals,
        configuration_refreshes_cached_ranges,
        configured_coverage_selectors,
        store_lookup_ordering,
        padding_boundaries,
        shared_partition_geometry,
        restart_recreates_metadata_cache
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    [{config_snapshot, arweave_config:internal_snapshot()} | Config].

end_per_testcase(_, Config) ->
    arweave_constants:internal_reset_partition_size_override(),
    arweave_config:internal_restore(proplists:get_value(config_snapshot, Config)).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Storage metadata and range coverage work without starting the host node.
metadata_without_host(_) ->
    ?assertEqual(undefined, whereis(ar_sup)),
    ?assertEqual(whereis(arweave_storage_sup), ets:info(arweave_storage, owner)),
    PartitionSize = arweave_constants:partition_size(),
    Module = {0, PartitionSize, unpacked},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Module]}),
    #store_info{id = StoreID} =
        Info =
        arweave_storage:store_info(Module),
    ?assertEqual("storage_module_0_unpacked", StoreID),
    ?assertEqual(StoreID, Info#store_info.label),
    ?assertEqual(Info, arweave_storage:store_info(StoreID)),
    ?assertEqual(unpacked, Info#store_info.packing),
    ?assertEqual(
        {0, PartitionSize + arweave_storage:get_overlap(unpacked)},
        Info#store_info.effective_range
    ),
    ?assertEqual(
        [{0, PartitionSize, StoreID}],
        arweave_storage:covering_ranges(0, PartitionSize, StoreID)
    ).

%% @doc Each packing format yields consistent IDs, paths, ranges and mining
%% metadata.
module_metadata_packings(_) ->
    %% A custom, unaligned range distinguishes configured bytes from overlap.
    Start = ?DATA_CHUNK_SIZE + 1,
    End = 3 * ?DATA_CHUNK_SIZE + 1,
    Addr = <<0:256>>,
    EncodedAddr = binary_to_list(arweave_util:encode(Addr)),
    Prefix =
        "storage_module_" ++ integer_to_list(Start) ++ "_" ++
            integer_to_list(End) ++ "_",
    lists:foreach(
        fun({Packing, MiningAddr, Difficulty, Suffix}) ->
            Module = {Start, End, Packing},
            Info = arweave_storage:store_info(Module),
            Label = Info#store_info.label,
            ?assert(is_list(Label)),
            ?assert(lists:prefix(Prefix, Label)),
            EffectiveEnd = End + arweave_storage:get_overlap(Packing),
            Path = filename:join([
                arweave_config:get([data_dir]), "storage_modules", Prefix ++ Suffix
            ]),
            ?assertEqual(
                #store_info{
                    id = Prefix ++ Suffix,
                    label = Label,
                    configured_range = {Start, End},
                    effective_range = {Start, EffectiveEnd},
                    padded_range = {
                        max(
                            0,
                            arweave_constants:get_chunk_padded_offset(Start) -
                                ?DATA_CHUNK_SIZE
                        ),
                        arweave_constants:get_chunk_padded_offset(EffectiveEnd)
                    },
                    disk_dir_name = Prefix ++ Suffix,
                    path = Path,
                    chunk_storage_path = filename:join(Path, "chunk_storage"),
                    repack_in_place = false,
                    packing = Packing,
                    mining_address = MiningAddr,
                    packing_difficulty = Difficulty
                },
                Info
            )
        end,
        [
            {unpacked, undefined, 0, "unpacked"},
            {{spora_2_6, Addr}, Addr, 0, EncodedAddr},
            {
                {replica_2_9, Addr},
                Addr,
                ?REPLICA_2_9_PACKING_DIFFICULTY,
                EncodedAddr ++ ".replica.2.9"
            }
        ]
    ).

%% @doc Metadata distinguishes repack sources, the default module and unknown
%% store IDs.
module_metadata_repack_and_default(_) ->
    PartitionSize = arweave_constants:partition_size(),
    Module = {0, PartitionSize, unpacked},
    #store_info{id = StoreID} = arweave_storage:store_info(Module),
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [],
        [repack_modules] => [
            #{
                partition => 0,
                from_format => unpacked,
                to_format => replica_2_9,
                to_address => <<0:256>>
            }
        ]
    }),
    %% Repacking metadata describes the source module, not its target packing.
    ?assertEqual(
        arweave_storage:store_info(Module),
        arweave_storage:store_info(StoreID)
    ),
    DataDir = arweave_config:get([data_dir]),
    ?assertEqual(
        #store_info{
            id = ?DEFAULT_MODULE,
            label = ?DEFAULT_MODULE,
            padded_range = {-1, -1},
            disk_dir_name = ?DEFAULT_MODULE,
            path = DataDir,
            chunk_storage_path = filename:join(DataDir, "chunk_storage"),
            repack_in_place = false,
            configured_range = {0, infinity},
            effective_range = {0, infinity},
            packing = unpacked,
            mining_address = undefined,
            packing_difficulty = 0
        },
        arweave_storage:store_info(?DEFAULT_MODULE)
    ),
    ?assert((arweave_storage:store_info(StoreID))#store_info.repack_in_place),
    ok = arweave_config:internal_force_config(#{[repack_modules] => []}),
    ?assertEqual(not_found, arweave_storage:store_info(StoreID)),
    ?assertEqual(not_found, arweave_storage:store_info("missing_store")),
    ?assertEqual(not_found, arweave_storage:store_info(missing_store)).

%% @doc Paths and module metadata remain available before storage starts,
%% without a label cache.
module_metadata_before_storage_start(Config) ->
    Module = {0, arweave_constants:partition_size(), {replica_2_9, <<0:256>>}},
    DataDir = filename:join(proplists:get_value(priv_dir, Config), "offline"),
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [Module],
        [data_dir] => DataDir
    }),
    Info = arweave_storage:store_info(Module),
    ?assertEqual(
        filename:join([DataDir, "storage_modules", Info#store_info.disk_dir_name]),
        Info#store_info.path
    ),
    ?assertEqual(
        filename:join(Info#store_info.path, "chunk_storage"),
        Info#store_info.chunk_storage_path
    ),
    ?assert(is_list(Info#store_info.label)),
    ok = application:stop(arweave_storage),
    try
        ?assertEqual(undefined, ets:info(arweave_storage)),
        ?assertEqual(
            Info#store_info{label = undefined},
            arweave_storage:store_info(Module)
        ),
        ?assertEqual(
            Info#store_info{label = undefined},
            arweave_storage:store_info(Info#store_info.id)
        )
    after
        application:ensure_all_started(arweave_storage)
    end,
    Restarted = arweave_storage:store_info(Module),
    ?assert(is_list(Restarted#store_info.label)),
    ?assertEqual(
        Info#store_info{label = undefined},
        Restarted#store_info{label = undefined}
    ).

%% @doc Legacy directory names preserve canonical IDs and resolve the correct
%% on-disk paths.
module_metadata_legacy_directory(_) ->
    %% One two-chunk bucket, beginning at bucket index two.
    Size = 2 * ?DATA_CHUNK_SIZE,
    Module = {2 * Size, 3 * Size, unpacked},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Module]}),
    Current = arweave_storage:store_info(Module),
    ?assertEqual(
        Current#store_info.id,
        Current#store_info.disk_dir_name
    ),
    ok = arweave_config:internal_force_config(#{[config_dialect] => legacy}),
    Legacy = arweave_storage:store_info(Current#store_info.id),
    ?assertEqual(
        "storage_module_" ++ integer_to_list(Size) ++ "_2_unpacked",
        Legacy#store_info.disk_dir_name
    ),
    ?assertEqual(Current#store_info.id, Legacy#store_info.id),
    ?assertEqual(Legacy, arweave_storage:store_info(Module)),
    DataDir = arweave_config:get([data_dir]),
    ?assertEqual(
        filename:join([DataDir, "storage_modules", Current#store_info.id]),
        Current#store_info.path
    ),
    ?assertEqual(
        filename:join([DataDir, "storage_modules", Legacy#store_info.disk_dir_name]),
        Legacy#store_info.path
    ),
    ?assertEqual(
        filename:join(Legacy#store_info.path, "chunk_storage"),
        Legacy#store_info.chunk_storage_path
    ),
    ?assertEqual(
        filename:join(Legacy#store_info.chunk_storage_path, "0"),
        arweave_storage:chunk_filepath("0", Legacy#store_info.id)
    ).

%% @doc Module and chunk paths follow data_dir changes without creating
%% directories.
module_metadata_paths_follow_data_dir(Config) ->
    Module = {0, arweave_constants:partition_size(), unpacked},
    %% A repack target can be queried by tuple before it is configured by ID.
    Target = {0, arweave_constants:partition_size(), {replica_2_9, <<0:256>>}},
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [Module],
        [repack_modules] => [],
        [config_dialect] => current
    }),
    lists:foreach(
        fun(Directory) ->
            DataDir = filename:join(proplists:get_value(priv_dir, Config), Directory),
            ok = arweave_config:internal_force_config(#{[data_dir] => DataDir}),
            Info = arweave_storage:store_info(Module),
            ?assertEqual(Info, arweave_storage:store_info(Info#store_info.id)),
            ?assertEqual(
                filename:join([DataDir, "storage_modules", Info#store_info.id]),
                Info#store_info.path
            ),
            ?assertEqual(
                filename:join(Info#store_info.path, "chunk_storage"),
                Info#store_info.chunk_storage_path
            ),
            ?assertEqual(
                filename:join(Info#store_info.chunk_storage_path, "0"),
                arweave_storage:chunk_filepath("0", Info#store_info.id)
            ),
            Default = arweave_storage:store_info(?DEFAULT_MODULE),
            ?assertEqual(DataDir, Default#store_info.path),
            ?assertEqual(
                filename:join(DataDir, "chunk_storage"),
                Default#store_info.chunk_storage_path
            ),
            ?assertEqual(
                filename:join(Default#store_info.chunk_storage_path, "0"),
                arweave_storage:chunk_filepath("0", ?DEFAULT_MODULE)
            ),
            TargetInfo = arweave_storage:store_info(Target),
            ?assertEqual(not_found, arweave_storage:store_info(TargetInfo#store_info.id)),
            ?assertEqual(
                filename:join([DataDir, "storage_modules", TargetInfo#store_info.id]),
                TargetInfo#store_info.path
            ),
            ?assertNot(filelib:is_dir(Info#store_info.path))
        end,
        ["first-root", "second-root"]
    ).

%% @doc Concurrent metadata lookups assign one consistent label to the same
%% packing address.
concurrent_metadata_labels(_) ->
    Partition = arweave_constants:partition_size(),
    %% Eight different addresses, requested 32 times each, contend on the cache.
    Modules = [
        {0, Partition, {replica_2_9, crypto:strong_rand_bytes(32)}}
     || _ <- lists:seq(1, 8)
    ],
    Infos = arweave_util:pmap(
        fun arweave_storage:store_info/1,
        lists:append(lists:duplicate(32, Modules))
    ),
    ?assertEqual(
        8,
        length(
            lists:usort(
                [Info#store_info.label || Info <- Infos]
            )
        )
    ),
    lists:foreach(
        fun(Module) ->
            Info = arweave_storage:store_info(Module),
            ?assert(is_list(Info#store_info.label)),
            SameID = [
                Other
             || Other <- Infos,
                Other#store_info.id =:= Info#store_info.id
            ],
            ?assertEqual(lists:duplicate(32, Info), SameID)
        end,
        Modules
    ).

%% @doc Each public storage function is a thin delegate to an app-internal
%% module.
public_api_delegates(_) ->
    %% code:which/1 returns cover_compiled for instrumented modules.
    Beam = code:where_is_file("arweave_storage.beam"),
    {ok, {arweave_storage, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(Beam, [abstract_code]),
    lists:foreach(
        fun({function, _, Name, _, Clauses}) ->
            lists:foreach(
                fun({clause, _, _, _, Body}) ->
                    [{call, _, {remote, _, {atom, _, Target}, _}, _}] = Body,
                    ?assert(lists:prefix("arweave_storage_", atom_to_list(Target)))
                end,
                Clauses
            ),
            ?assertNotEqual(stop, Name)
        end,
        [Form || {function, _, Name, _, _} = Form <- Forms, Name /= stop]
    ).

%% @doc The public facade omits retired metadata getters and internal storage
%% operations.
public_api_hides_storage_internals(_) ->
    lists:foreach(
        fun({Function, Arity}) ->
            ?assertNot(erlang:function_exported(arweave_storage, Function, Arity))
        end,
        [
            {id, 1},
            {get, 2},
            {get_all, 1},
            {get_all, 2},
            {has_any, 1},
            {has_range, 2},
            {get_cover, 3},
            {has_sync_record, 2},
            {get_sync_record, 2},
            {get_interval, 3},
            {get_intersection_size, 4},
            {is_footprint_record_supported, 3},
            {chunk_sync_record_id, 1},
            {get_serialized_sync_buckets, 0},
            {get_serialized_footprint_buckets, 0},
            {is_entropy_storage_ready, 1},
            {get_filepath, 2},
            {list_files, 2},
            {write_chunk, 4},
            {delete_chunk_file, 2},
            {add_sync_record, 4},
            {is_recorded, 2},
            {is_recorded, 3},
            {get_next_synced_interval, 4},
            {get_next_synced_interval, 5},
            {get_next_unsynced_interval, 4},
            {get_next_unsynced_interval, 5},
            {label, 1},
            {get_module, 1},
            {get_range, 1},
            {get_padded_range, 1},
            {get_packing, 1},
            {disk_dir_name, 1},
            {storage_module_path, 2},
            {get_chunk_storage_path, 2},
            {is_repack_in_place, 1},
            {sync_record_name, 1},
            {chunk_storage_name, 1},
            {entropy_storage_name, 1},
            {acquire_semaphore, 1},
            {release_semaphore, 1},
            {get_handle_by_filepath, 1},
            {record_stored_chunk, 5},
            {record_chunk_with_entropy, 5}
        ]
    ).

%% @doc Coverage selectors preserve overlap for offsets but not ranges, and
%% never treat the default store as unbounded configured coverage.
configured_coverage_selectors(_) ->
    Chunk = ?DATA_CHUNK_SIZE,
    Packing = {replica_2_9, <<0:256>>},
    ModuleA = {0, Chunk, unpacked},
    ModuleB = {Chunk, 2 * Chunk, Packing},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [ModuleA, ModuleB]}),
    #store_info{id = StoreA} = arweave_storage:store_info(ModuleA),
    #store_info{id = StoreB} = arweave_storage:store_info(ModuleB),
    ?assert(arweave_storage:covers_offset(Chunk, unpacked, StoreA)),
    ?assertNot(arweave_storage:covers_offset(0, any_packing, any_store)),
    ?assertNot(arweave_storage:covers_offset(Chunk, Packing, any_store)),
    ?assert(arweave_storage:covers_offset(Chunk + 1, Packing, StoreB)),
    ?assertNot(arweave_storage:covers_offset(Chunk + 1, unpacked, StoreB)),
    %% Offset coverage includes the exact overlap end; full range coverage does not.
    End = Chunk + arweave_storage:get_overlap(unpacked),
    ?assert(arweave_storage:covers_offset(End, any_packing, StoreA)),
    ?assertNot(arweave_storage:covers_offset(End + 1, any_packing, StoreA)),
    ?assertNot(arweave_storage:covers_range(0, End, any_packing, StoreA)),
    ?assert(arweave_storage:covers_range(0, 2 * Chunk, any_packing, any_store)),
    ?assertNot(arweave_storage:covers_range(0, 2 * Chunk, unpacked, any_store)),
    ?assert(arweave_storage:covers_range(Chunk, 2 * Chunk, Packing, StoreB)),
    %% Revisit the unrestricted query after changing the cached selection.
    ?assert(arweave_storage:covers_range(0, 2 * Chunk, any_packing, any_store)),
    ?assertNot(arweave_storage:covers_offset(1, any_packing, missing_store)),
    ?assertNot(arweave_storage:covers_range(0, Chunk, any_packing, missing_store)),
    ok = arweave_config:internal_force_config(#{[storage_modules] => []}),
    ?assertNot(arweave_storage:covers_offset(1, any_packing, any_store)),
    ?assertNot(arweave_storage:covers_range(0, Chunk, any_packing, any_store)),
    ?assertNot(arweave_storage:covers_offset(1, any_packing, ?DEFAULT_MODULE)),
    %% Empty ranges retain the old vacuous-coverage result.
    ?assert(arweave_storage:covers_range(Chunk, Chunk, any_packing, any_store)).

%% @doc Renamed lookups preserve reverse configuration order and packing
%% preference, while new filters restrict rather than reorder candidates.
store_lookup_ordering(_) ->
    Chunk = ?DATA_CHUNK_SIZE,
    Packing = {replica_2_9, <<0:256>>},
    ModuleA = {0, Chunk, unpacked},
    ModuleB = {0, Chunk, Packing},
    ModuleC = {4 * Chunk, 5 * Chunk, unpacked},
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [ModuleA, ModuleB, ModuleC]
    }),
    ?assertEqual([ModuleB, ModuleA], arweave_storage:covering_stores(1, any_packing)),
    ?assertEqual([ModuleA], arweave_storage:covering_stores(1, unpacked)),
    ?assertEqual([ModuleB], arweave_storage:intersecting_stores(0, Chunk, Packing)),
    ?assertEqual(
        [ModuleB, ModuleA],
        arweave_storage:intersecting_stores(0, Chunk, any_packing)
    ),
    ?assertEqual([], arweave_storage:covering_stores(0, any_packing)),
    ?assertEqual([], arweave_storage:intersecting_stores(2 * Chunk, 3 * Chunk, any_packing)),
    ?assertEqual(ModuleB, arweave_storage:covering_store(1, Packing)),
    ?assertEqual(ModuleA, arweave_storage:covering_store(1, unpacked)),
    ?assertEqual(not_found, arweave_storage:covering_store(0, unpacked)),
    #store_info{id = StoreB} = arweave_storage:store_info(ModuleB),
    ?assertEqual(
        [{0, Chunk, StoreB}],
        arweave_storage:covering_ranges(0, Chunk, ModuleB)
    ).

%% @doc Changing configured modules invalidates previously cached range
%% coverage.
configuration_refreshes_cached_ranges(_) ->
    Chunk = ?DATA_CHUNK_SIZE,
    %% Disjoint one-chunk modules make stale range coverage observable.
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [{0, Chunk, unpacked}]
    }),
    ?assert(arweave_storage:covers_range(0, Chunk, any_packing, any_store)),
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [{2 * Chunk, 3 * Chunk, unpacked}]
    }),
    ?assertNot(arweave_storage:covers_range(0, Chunk, any_packing, any_store)),
    ?assert(arweave_storage:covers_range(2 * Chunk, 3 * Chunk, any_packing, any_store)).

%% @doc Module ranges include overlap and use protocol padding at the split
%% threshold.
padding_boundaries(_) ->
    Threshold = arweave_constants:strict_data_split_threshold(),
    Chunk = ?DATA_CHUNK_SIZE,
    %% Offsets at the threshold remain unchanged; later offsets round up.
    ?assertEqual(Threshold, arweave_constants:get_chunk_padded_offset(Threshold)),
    ?assertEqual(
        Threshold + Chunk,
        arweave_constants:get_chunk_padded_offset(Threshold + 1)
    ),
    ?assertEqual(
        Threshold + Chunk,
        arweave_constants:get_chunk_padded_offset(Threshold + Chunk)
    ),
    ?assertEqual(
        Threshold + 2 * Chunk,
        arweave_constants:get_chunk_padded_offset(Threshold + Chunk + 1)
    ),
    Module = {Threshold, Threshold + 1, unpacked},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Module]}),
    Overlap = arweave_storage:get_overlap(unpacked),
    End = Threshold + ((Overlap + Chunk) div Chunk) * Chunk,
    ?assertEqual(
        {max(0, Threshold - Chunk), End},
        (arweave_storage:store_info(Module))#store_info.padded_range
    ),
    ?assertEqual(not_found, arweave_storage:store_info("missing_store")),
    ?assertEqual(
        {-1, -1},
        (arweave_storage:store_info(?DEFAULT_MODULE))#store_info.padded_range
    ).

%% @doc Module IDs use the shared partition-size override when recognizing whole
%% partitions.
shared_partition_geometry(_) ->
    PartitionSize = arweave_constants:partition_size(),
    Module = {0, PartitionSize, unpacked},
    ?assertEqual(
        "storage_module_0_unpacked",
        (arweave_storage:store_info(Module))#store_info.id
    ),
    %% Doubling the partition makes the original module a custom range.
    arweave_constants:internal_override_partition_size(2 * PartitionSize),
    Expected =
        "storage_module_0_" ++ integer_to_list(PartitionSize) ++
            "_unpacked",
    ?assertEqual(Expected, (arweave_storage:store_info(Module))#store_info.id),
    ?assertEqual(
        "storage_module_0_unpacked",
        (arweave_storage:store_info({0, 2 * PartitionSize, unpacked}))#store_info.id
    ).

%% @doc Restarting the storage application recreates an empty, app-owned
%% metadata table.
restart_recreates_metadata_cache(_) ->
    ok = application:stop(arweave_storage),
    try
        ?assertEqual(undefined, ets:info(arweave_storage)),
        ?assertEqual(
            {ok, [arweave_storage]},
            application:ensure_all_started(arweave_storage)
        ),
        ?assertEqual(
            whereis(arweave_storage_sup),
            ets:info(arweave_storage, owner)
        ),
        ?assertEqual([], ets:tab2list(arweave_storage))
    after
        application:ensure_all_started(arweave_storage)
    end.
