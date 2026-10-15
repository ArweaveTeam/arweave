-module(arweave_tools_doctor_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        configures_paths_before_directory_check,
        directory_check_uses_config,
        bitmap_splits_missing_buckets_by_entropy
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_config),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    [{config_snapshot, arweave_config:internal_snapshot()} | Config].

end_per_testcase(_, Config) ->
    arweave_config:internal_restore(proplists:get_value(config_snapshot, Config)).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Doctor commands configure data_dir before resolving or checking storage
%% paths.
configures_paths_before_directory_check(Config) ->
    ModuleArg = "{\"partition\":0,\"packing_format\":\"unpacked\"}",
    %% Stop at the directory check: no merge, benchmark or database startup.
    ok = meck:new(arweave_tools_doctor, [passthrough, no_link]),
    ok = meck:expect(arweave_tools_doctor, check_module_dir, fun(ModuleOrID) ->
        put(checked_store_info, arweave_storage:store_info(ModuleOrID)),
        false
    end),
    try
        check_configured_paths(Config, merge, fun(DataDir) ->
            arweave_tools_doctor_merge:main([
                DataDir, ModuleArg, "unused-source"
            ])
        end),
        check_configured_paths(Config, bench, fun(DataDir) ->
            arweave_tools_doctor_bench:main(["1", DataDir, ModuleArg])
        end),
        check_configured_paths(Config, bitmap, fun(DataDir) ->
            arweave_tools_doctor_inspect:main(["bitmap", DataDir, ModuleArg])
        end),
        ?assert(meck:validate(arweave_tools_doctor))
    after
        meck:unload(arweave_tools_doctor)
    end.

%% @doc Directory checks use the configured root for both module tuples and IDs.
directory_check_uses_config(Config) ->
    DataDir = filename:join(
        proplists:get_value(priv_dir, Config), "directory-check"
    ),
    Module = {0, arweave_lib_constants:partition_size(), unpacked},
    ok = arweave_config:internal_force_config(#{
        [data_dir] => DataDir,
        [storage_modules] => [Module]
    }),
    Info = arweave_storage:store_info(Module),
    ?assertEqual(false, arweave_tools_doctor:check_module_dir(Module)),
    ok = filelib:ensure_dir(filename:join(Info#store_info.path, "placeholder")),
    ?assertEqual(true, arweave_tools_doctor:check_module_dir(Module)),
    ?assertEqual(
        true, arweave_tools_doctor:check_module_dir(Info#store_info.id)
    ).

%%====================================================================
%% Helpers
%%====================================================================

check_configured_paths(Config, Command, Run) ->
    DataDir = filename:join(
        proplists:get_value(priv_dir, Config), atom_to_list(Command)
    ),
    erase(checked_store_info),
    ?assertEqual(false, Run(DataDir)),
    ?assertEqual(DataDir, arweave_config:get([data_dir])),
    Info = erase(checked_store_info),
    ?assertMatch(#store_info{}, Info),
    ?assertEqual(
        filename:join([DataDir, "storage_modules", Info#store_info.id]),
        Info#store_info.path
    ),
    ?assertEqual(
        filename:join(Info#store_info.path, "chunk_storage"),
        Info#store_info.chunk_storage_path
    ),
    ?assertNot(filelib:is_dir(DataDir)).

%% @doc Bitmap buckets without a chunk are prepared (entropy) or holes
%% (missing), and footprints past a store's limit belong to no one (none).
bitmap_splits_missing_buckets_by_entropy(_Config) ->
    %% A partition 1 module: the test profile's partition is 2,000,000 bytes,
    %% so the partition boundary falls inside the bucket ending at 8 chunks,
    %% where the module's first sector starts. Its entropy partition is four
    %% two-chunk sectors, one result row each. Each sector's first bucket is
    %% in footprint 1 and its second in footprint 0.
    Bucket = fun(K) -> (8 + K) * ?DATA_CHUNK_SIZE end,
    Packing = {replica_2_9, <<0:256>>},
    Stored = [Bucket(0), Bucket(3)],
    Prepared = [Bucket(1), Bucket(4)],
    ModuleStart = arweave_lib_constants:partition_size(),
    ModuleEnd = 2 * arweave_lib_constants:partition_size(),
    Mocked = [arweave_storage, ar_footprint_limit, ar_data_sync],
    [ok = meck:new(M, [passthrough, no_link]) || M <- Mocked],
    ok = meck:expect(arweave_storage, store_info,
        fun(_) -> #store_info{packing = Packing} end),
    ok = meck:expect(ar_footprint_limit, get, fun(_) ->
        arweave_lib_constants:get_replica_2_9_footprints_per_partition()
    end),
    ok = meck:expect(ar_data_sync, get_chunk_metadata_range, fun(_, _, _) ->
        {ok, maps:from_list(
            [{Offset, #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE}}
                || Offset <- Stored])}
    end),
    ok = meck:expect(arweave_storage, is_recorded,
        fun(Offset, any_packing, _, _) ->
            case lists:member(Offset, Stored) of
                true -> {true, Packing};
                false -> false
            end
        end),
    ok = meck:expect(arweave_storage, is_storage_supported,
        fun(_, _, _) -> true end),
    ok = meck:expect(arweave_storage, is_entropy_recorded,
        fun(Offset, _, _) -> lists:member(Offset, Prepared) end),
    Packings = fun() ->
        arweave_tools_chunk_visualization:get_chunk_packings(
            ModuleStart, ModuleEnd, store)
    end,
    try
        ?assertEqual(
            [[Packing, entropy], [missing, Packing],
                [entropy, missing], [missing, missing]],
            Packings()
        ),
        %% Keeping only footprint 0 leaves every sector's first bucket to no
        %% one, unless a chunk is already stored there.
        ok = meck:expect(ar_footprint_limit, get, fun(_) -> 1 end),
        ?assertEqual(
            [[Packing, entropy], [none, Packing],
                [none, missing], [none, missing]],
            Packings()
        ),
        %% Entropy does not apply to other packings.
        Spora = {spora_2_6, <<0:256>>},
        ok = meck:expect(arweave_storage, store_info,
            fun(_) -> #store_info{packing = Spora} end),
        ?assertEqual(
            [[Packing, missing], [missing, Packing],
                [missing, missing], [missing, missing]],
            Packings()
        )
    after
        [meck:unload(M) || M <- Mocked]
    end.
