-module(arweave_tools_doctor_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() -> [configures_paths_before_directory_check, directory_check_uses_config].

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
    Module = {0, arweave_constants:partition_size(), unpacked},
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
