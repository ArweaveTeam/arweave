-module(arweave_tools_doctor_snapshot_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Export below the tip so rejoining must discard the later blocks. Mine far
%% enough that the node settles its set above the snapshot tip, so the export
%% keeps only a part of the set: with the test consensus window of 10 blocks,
%% the node settles up to 24 - 10 - 1 = 13 and the snapshot up to 12 - 10 = 2.
-define(MINED_HEIGHT, 24).
-define(SNAPSHOT_HEIGHT, 12).

%% The number of blocks with an ECDSA transaction, from the block 1.
-define(ECDSA_TX_COUNT, 3).

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
    TXs = [ecdsa_tx() || _ <- lists:seq(1, ?ECDSA_TX_COUNT)],
    [B0] = call(Peer, ar_weave, init, [
        [{ar_tx:get_owner_address(TX), ?AR(10), <<>>} || TX <- TXs]
    ]),
    call(Peer, ar_test_node, start, [B0]),
    lists:foreach(
        fun(Height) ->
            case Height =< ?ECDSA_TX_COUNT of
                true ->
                    call(Peer, ar_test_node, assert_post_tx_to_peer, [
                        main, lists:nth(Height, TXs)
                    ]);
                false ->
                    ok
            end,
            call(Peer, ar_test_node, mine, []),
            ?assertMatch(
                {ok, _}, call(Peer, ar_test_await, node_height, [main, Height])
            )
        end,
        lists:seq(1, ?MINED_HEIGHT)
    ),
    {ok, BI} = call(Peer, ar_test_await, node_height, [main, ?MINED_HEIGHT]),
    Window = call(Peer, arweave_lib_constants, get_consensus_window_size, []),
    %% The node settles the blocks up to one below the consensus window, above
    %% the snapshot tip.
    NodeSettledHeight = ?MINED_HEIGHT - Window - 1,
    ?assertMatch(
        {ok, {NodeSettledHeight, _, ?ECDSA_TX_COUNT, _}},
        call(Peer, ar_ecdsa_settled, get_state, [])
    ),
    ?assert(NodeSettledHeight > ?SNAPSHOT_HEIGHT),
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
    {ok, Files} = file:list_dir(SnapshotDir),
    ?assertEqual(["manifest.json", "rocksdb"], lists:sort(Files)),
    {ok, ManifestJSON} = file:read_file(
        filename:join(SnapshotDir, "manifest.json")
    ),
    ?assertEqual(
        {ok, #{<<"height">> => ?SNAPSHOT_HEIGHT,
            <<"indep_hash">> => arweave_lib_util:encode(TipH),
            <<"weave_size">> => TipWeaveSize}},
        ar_serialize:json_decode(ManifestJSON, [return_maps])
    ),
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
    %% The snapshot carries the set up to the consensus window below its tip.
    SettledHeight = ?SNAPSHOT_HEIGHT - Window,
    {SettledH, _, _} = lists:nth(length(BI) - SettledHeight, BI),
    ?assertMatch(
        {ok, {SettledHeight, SettledH, SettledHeight, _}},
        call(Peer, ar_ecdsa_settled, get_state, [])
    ),
    ?assertEqual(
        [Height =< SettledHeight || Height <- lists:seq(1, ?ECDSA_TX_COUNT)],
        [
            call(Peer, ar_ecdsa_settled, is_settled, [
                TX#tx.id, ?SNAPSHOT_HEIGHT
            ])
         || TX <- TXs
        ]
    ),
    call(Peer, ar_test_node, mine, []),
    ?assertMatch(
        {ok, _},
        call(Peer, ar_test_await, node_height, [main, ?SNAPSHOT_HEIGHT + 1])
    ).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Return an ECDSA-signed transaction from a new wallet. It carries no
%% data: the node starting from the snapshot has no peer to sync data from.
ecdsa_tx() ->
    ar_tx:sign(
        #tx{format = 2, reward = ?AR(1), denomination = 1},
        ar_wallet:new(?ECDSA_KEY_TYPE)
    ).

%% @doc Run a fixture operation on the isolated node within the test timeout.
call(Peer, Module, Function, Args) ->
    peer:call(Peer, Module, Function, Args, ?TEST_NODE_TIMEOUT * 1000).
