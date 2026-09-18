-module(arweave_tools_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        load_without_node,
        doctor_dispatch,
        unknown_command_status,
        packing_benchmark_arguments,
        hash_calibration_options,
        vdf_calibration_options
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_tools),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    {ok, #{level := Level}} = logger:get_handler_config(default),
    [{console_level, Level} | Config].

end_per_testcase(_, Config) ->
    logger:set_handler_config(
        default, level, proplists:get_value(console_level, Config)
    ).

%%====================================================================
%% Test cases
%%====================================================================

%% Loading tools must not boot the node or add tools to its runtime dependencies.
load_without_node(_) ->
    ?assertEqual(undefined, whereis(ar_sup)),
    ?assertNot(lists:keymember(arweave, 1, application:which_applications())),
    {ok, ToolsModules} = application:get_key(arweave_tools, modules),
    ?assert(lists:member(arweave_tools_doctor, ToolsModules)),
    ?assert(lists:member(arweave_tools_bench_packing, ToolsModules)),
    ?assertNot(lists:member(ar_bench_hash, ToolsModules)),
    ?assertNot(lists:member(ar_bench_vdf, ToolsModules)),
    ?assertNot(lists:member(ar_snapshot, ToolsModules)),
    %% Production startup still calls calibration without a tools dependency.
    ?assertEqual([], calls_to_modules(ar, ToolsModules)),
    ?assertEqual([], calls_to_modules(ar_packing_server, ToolsModules)).

%% Each doctor command receives its arguments intact and retains success status 0.
doctor_dispatch(_) ->
    %% Spaces in paths and JSON arguments must not split argument boundaries.
    Args = ["/data with spaces", "{\"partition\": 0}"],
    ?assertEqual(
        {0, Args},
        doctor_command_result("merge", arweave_tools_doctor_merge, Args)
    ),
    ?assertEqual(
        {0, Args},
        doctor_command_result("bench", arweave_tools_doctor_bench, Args)
    ),
    ?assertEqual(
        {0, Args},
        doctor_command_result("dump", arweave_tools_doctor_dump, Args)
    ),
    ?assertEqual(
        {0, Args},
        doctor_command_result("inspect", arweave_tools_doctor_inspect, Args)
    ),
    ?assertEqual(
        {0, Args},
        doctor_command_result("snapshot", arweave_tools_doctor_snapshot, Args)
    ).

unknown_command_status(_) ->
    %% The shell launcher can reject unknown commands before reaching this API.
    ?assertEqual(1, arweave_tools:run(["unknown"])).

packing_benchmark_arguments(_) ->
    %% Keep paths intact without the real benchmark's disk I/O; the smoke
    %% covers benchmark dispatch, and the tests below check hash/VDF options.
    Args = ["dir", "/data with spaces"],
    with_mock(
        arweave_tools_bench_packing,
        run_benchmark_from_cli,
        fun(Received) ->
            self() ! {command_args, Received},
            ok
        end,
        fun() ->
            %% Benchmark completion retains the legacy exit status 1.
            ?assertEqual(
                1, arweave_tools:run(["benchmark", "packing" | Args])
            ),
            ?assertEqual(Args, take_command_args())
        end
    ).

%% Hash CLI flags reach the existing runtime calibration without real RandomX work.
hash_calibration_options(_) ->
    with_mock(
        ar_mine_randomx,
        init_fast2,
        fun(rx512, _, 1, 0, _) -> test_randomx_state end,
        fun() ->
            with_mock(
                ar_bench_hash,
                run_benchmark,
                fun(State, JIT, LargePages, HardwareAES) ->
                    self() !
                        {command_args, {State, JIT, LargePages, HardwareAES}},
                    %% Millisecond-scale stand-ins used only for formatting.
                    {1000, 2000}
                end,
                fun() ->
                    ?assertEqual(
                        1,
                        arweave_tools:run([
                            "benchmark",
                            "hash",
                            "randomx",
                            "512",
                            "jit",
                            "1",
                            "large_pages",
                            "0",
                            "hw_aes",
                            "0"
                        ])
                    ),
                    ?assertEqual(
                        {test_randomx_state, 1, 0, 0}, take_command_args()
                    )
                end
            )
        end
    ).

%% VDF CLI flags reach the existing calibration, including algorithm selection.
vdf_calibration_options(_) ->
    with_mock(
        ar_bench_vdf,
        run_benchmark,
        fun(Mode, Difficulty, Verify) ->
            self() ! {command_args, {Mode, Difficulty, Verify}},
            ok
        end,
        fun() ->
            %% Two is the existing minimum VDF difficulty accepted by the CLI.
            ?assertEqual(
                1,
                arweave_tools:run([
                    "benchmark",
                    "vdf",
                    "mode",
                    "openssl",
                    "difficulty",
                    "2",
                    "verify",
                    "true"
                ])
            ),
            ?assertEqual({openssl, 2, true}, take_command_args())
        end
    ).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Return the exit status and arguments received by a mocked doctor command.
doctor_command_result(Command, Module, Args) ->
    with_mock(
        Module,
        main,
        fun(Received) ->
            self() ! {command_args, Received},
            true
        end,
        fun() ->
            ExitStatus = arweave_tools:run(["doctor", Command | Args]),
            {ExitStatus, take_command_args()}
        end
    ).

%% @doc Return compiled external calls targeting any of the supplied modules.
calls_to_modules(Module, TargetModules) ->
    case beam_lib:chunks(code:which(Module), [imports]) of
        {ok, {Module, [{imports, Calls}]}} ->
            lists:filter(
                fun({TargetModule, _Function, _Arity}) ->
                    lists:member(TargetModule, TargetModules)
                end,
                Calls
            );
        Error ->
            Error
    end.

with_mock(Module, Function, Implementation, Test) ->
    meck:new(Module, [passthrough, no_link]),
    try
        meck:expect(Module, Function, Implementation),
        Test()
    after
        meck:unload(Module)
    end.

take_command_args() ->
    receive
        {command_args, Args} -> Args
    after 0 -> no_args
    end.
