-module(arweave_tools_doctor_snapshot_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Export below the tip so rejoining must discard the two later blocks.
-define(MINED_HEIGHT, 7).
-define(SNAPSHOT_HEIGHT, 5).

suite() -> [{timetrap, {seconds, ?TEST_NODE_TIMEOUT}}].

all() -> [snapshot_export].

init_per_testcase(_, Config) ->
    PrivDir = proplists:get_value(priv_dir, Config),
    LogDir = filename:join(PrivDir, "node-logs"),
    %% The node owns global services and stops dependencies during restart;
    %% keep those changes out of the BEAM running the other tools suites.
    {ok, Peer, _Node} = peer:start(#{
        name => peer:random_name("main-snapshot"),
        host => "127.0.0.1",
        longnames => true,
        connection => standard_io,
        %% Bound this single-node fixture's scheduler/thread footprint.
        args => ["+S", "4:4", "-pa" | code:get_path()],
        env => [
            {"ARWEAVE_PROJECT_ROOT", PrivDir},
            {"AR_LOGGING_PATH", LogDir},
            {"AR_LOG_DIR", LogDir}
        ]
    }),
    try
        ok = call(Peer, file, set_cwd, [PrivDir]),
        ok = call(Peer, arweave_config, start, []),
        ok = call(Peer, arweave_limiter, start, []),
        ok = call(Peer, ar_test_runner, start_for_tests, [test]),
        [{peer, Peer} | Config]
    catch
        Class:Reason:Stack ->
            peer:stop(Peer),
            erlang:raise(Class, Reason, Stack)
    end.

end_per_testcase(_, Config) ->
    peer:stop(proplists:get_value(peer, Config)).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Export an earlier chain height, rejoin from it, and resume mining.
snapshot_export(Config) ->
    Peer = proplists:get_value(peer, Config),
    [B0] = call(Peer, ar_weave, init, []),
    call(Peer, ar_test_node, start, [B0]),
    lists:foreach(
        fun(Height) ->
            call(Peer, ar_test_node, mine, []),
            ?assertMatch(
                {ok, _}, call(Peer, ar_test_await, node_height, [main, Height])
            )
        end,
        lists:seq(1, ?MINED_HEIGHT)
    ),
    {ok, BI} = call(Peer, ar_test_await, node_height, [main, ?MINED_HEIGHT]),
    {TipH, TipWeaveSize, _} = lists:nth(length(BI) - ?SNAPSHOT_HEIGHT, BI),
    DataDir = call(Peer, arweave_config, get, [[data_dir]]),
    SnapshotDir = filename:join(
        proplists:get_value(priv_dir, Config), "snapshot"
    ),
    call(Peer, ar_test_node, stop, []),
    ok = call(Peer, ar_test_await, ar_kv_stopped, [10_000]),
    ?assertEqual(
        0,
        call(Peer, arweave_tools, run, [
            [
                "doctor",
                "snapshot",
                DataDir,
                SnapshotDir,
                "height",
                integer_to_list(?SNAPSHOT_HEIGHT)
            ]
        ])
    ),
    %% The snapshot command's temporary RPC process owns ar_kv_sup; its exit
    %% closes the databases, which must finish before the node restarts.
    ok = call(Peer, ar_test_await, ar_kv_stopped, [10_000]),
    ?assertEqual({ok, ["rocksdb"]}, file:list_dir(SnapshotDir)),
    call(Peer, ar_test_node, start, [
        #{
            b0 => B0,
            config => #{[join, start_from_state] => SnapshotDir}
        }
    ]),
    ?assertEqual(?SNAPSHOT_HEIGHT, call(Peer, ar_node, get_height, [])),
    ?assertMatch(
        #block{indep_hash = TipH, weave_size = TipWeaveSize},
        call(Peer, ar_node, get_current_block, [])
    ),
    call(Peer, ar_test_node, mine, []),
    ?assertMatch(
        {ok, _},
        call(Peer, ar_test_await, node_height, [main, ?SNAPSHOT_HEIGHT + 1])
    ).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Run a fixture operation on the isolated node within the test timeout.
call(Peer, Module, Function, Args) ->
    peer:call(Peer, Module, Function, Args, ?TEST_NODE_TIMEOUT * 1000).
