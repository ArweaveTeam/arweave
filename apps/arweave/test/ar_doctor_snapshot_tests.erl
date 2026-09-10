-module(ar_doctor_snapshot_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MINED_HEIGHT, 7).
-define(SNAPSHOT_HEIGHT, 5).

snapshot_export_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_snapshot_export/0}.

%% @doc Mine a chain, export the offline snapshot of an earlier height and
%% start a node from it.
test_snapshot_export() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    lists:foreach(
        fun(Height) ->
            ar_test_node:mine(),
            ?assertMatch({ok, _}, ar_test_await:node_height(main, Height))
        end,
        lists:seq(1, ?MINED_HEIGHT)),
    {ok, BI} = ar_test_await:node_height(main, ?MINED_HEIGHT),
    {TipH, TipWeaveSize, _} = lists:nth(length(BI) - ?SNAPSHOT_HEIGHT, BI),
    DataDir = arweave_config:get([data_dir]),
    SnapshotDir = filename:join(filename:dirname(DataDir),
            filename:basename(DataDir) ++ "_snapshot"),
    _ = file:del_dir_r(SnapshotDir),
    ar_test_node:stop(),
    ok = ar_test_await:ar_kv_stopped(10_000),
    ?assertEqual(true, ar_doctor_snapshot:main([DataDir, SnapshotDir,
            "height", integer_to_list(?SNAPSHOT_HEIGHT)])),
    ok = stop_ar_kv(),
    ?assertEqual({ok, ["rocksdb"]}, file:list_dir(SnapshotDir)),
    ar_test_node:start(#{ b0 => B0,
            config => #{ [join, start_from_state] => SnapshotDir } }),
    ?assertEqual(?SNAPSHOT_HEIGHT, ar_node:get_height()),
    ?assertMatch(#block{ indep_hash = TipH, weave_size = TipWeaveSize },
            ar_node:get_current_block()),
    ar_test_node:mine(),
    ?assertMatch({ok, _},
            ar_test_await:node_height(main, ?SNAPSHOT_HEIGHT + 1)).

%% @doc Stop the ar_kv supervisor the doctor started in this process.
stop_ar_kv() ->
    Pid = whereis(ar_kv_sup),
    unlink(Pid),
    exit(Pid, shutdown),
    ar_test_await:ar_kv_stopped(10_000).
