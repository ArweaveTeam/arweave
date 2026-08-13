-module(ar_test_node).

%% The new, more flexible, and more user-friendly interface.
-export([boot_peers/1, wait_for_peers/1,
        restart/0, restart/1,
        start_other_node/4, start_node/2, start_node/3,
        start_coordinated/1,
        base_cm_config/1, mine/1,
        http_get_block/2, get_blocks/1,
        mock_to_force_invalid_h1/0, mock_to_force_cross_node_h2/0,
        mainnet_packing_mocks/0,
        get_difficulty_for_invalid_hash/0, invalid_solution/0,
        valid_solution/0, remote_call/4, remote_call/5,
        generate_address/1,
        storage_module_packing/2, storage_module_config/2, storage_module_config/3,
        storage_module_configs/1,
        wide_storage_modules/2, wide_storage_modules/3, get_genesis_chunk/1,
        all_peers/1, new_custom_size_rsa_wallet/1,
        project_root/0]).

%% The "legacy" interface.
-export([start/0, start/1, start/2, start/3,
        stop/0, stop/1, start_peer/2, start_peer/3, start_peer/4, peer_name/1, peer_port/1,
        stop_peers/1, stop_peer/1, restart_peer_beam/1, connect_peers/2, connect_to_peer/1,
        disconnect_peers/2, disconnect_from/1,
        join/3, join_on/1, join_on/2, rejoin_on/1,
        generate_join_config/0, generate_join_config/1,
        peer_ip/1, get_node_namespace/0, get_unused_port/0,
        with_gossip_paused/2,

        mine/0, get_tx_anchor/1, get_tx_confirmations/2, get_tx_price/2, get_tx_price/3,
        get_optimistic_tx_price/2, get_optimistic_tx_price/3,
        sign_tx/1, sign_tx/2, sign_tx/3, sign_v1_tx/1, sign_v1_tx/2, sign_v1_tx/3,

        post_tx_to_peer/2, post_tx_to_peer/3, assert_post_tx_to_peer/2, assert_post_tx_to_peer/3,
        post_and_mine/2, post_block/2, post_block/3, send_new_block/2,
        await_post_block/2, await_post_block/3, sign_block/3,
        get_chunk/2, get_chunk/3, get_chunk_proof/2, post_chunk/2,
        get_unconfirmed_chunk/3,
        random_v1_data/1, assert_get_tx_data/3,
        post_tx_json/2,
        wait_until_syncs_genesis_data/0, wait_until_syncs_genesis_data/1,

        mock_all_nodes/1, run_with_mocked/3,
        test_with_all_nodes_mocked/2,
        test_with_all_nodes_mocked/3]).

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_mining.hrl").


-include_lib("eunit/include/eunit.hrl").

%% 5 minutes. Test-mode mining completes in seconds when healthy, so this
%% ceiling is generous headroom while keeping a stalled run's failure
%% surfacing quickly.
%% Sometimes takes a while on a slow machine
-define(PEER_START_TIMEOUT, 500_000).
%% Set the maximum number of retry attempts
-define(MAX_BOOT_RETRIES, 3).

-define(MAX_MINERS, 3).

-define(REMOTE_CALL_TIMEOUT, 500_000).
-define(CONNECT_TO_PEER_TIMEOUT, 500_000).
-define(TEST_MOCKED_FUNCTIONS_TIMEOUT, 500). %% in seconds
-define(POST_AND_MINE_TIMEOUT, 500_000).
-define(READ_BLOCK_TIMEOUT, 500_000).
-define(GET_TX_DATA_TIMEOUT, 200_000).
%% Restart remote calls must clear `?TIMEOUT_NODE_JOINED' (ar_test_await)
%% plus `ar_packing_server' cold start (re-inits RandomX datasets each
%% restart in the e2e profile, ~36s) plus all sup-tree children.
-define(RESTART_TIMEOUT, 300_000).
-define(TEST_HTTP_CLIENT_KEEPALIVE, 4_000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

all_peers(test) ->
    [{test, peer1}, {test, peer2}, {test, peer3}, {test, peer4}];
all_peers(e2e) ->
    [{e2e, peer1}, {e2e, peer2}].

%% @doc Generate a fresh default wallet address on a test node.
%% `main' generates locally; peer atoms generate on the peer via `remote_call/4'.
generate_address(main) ->
    ar_wallet:to_address(ar_wallet:new_keyfile());
generate_address(Node) ->
    ar_wallet:to_address(remote_call(Node, ar_wallet, new_keyfile, [])).

new_custom_size_rsa_wallet(Size) ->
    KeyType = ?RSA_KEY_TYPE,
    PublicExpnt = 65537,
    {[Expnt, Pub], [Expnt, Pub, Priv, P1, P2, E1, E2, C]} =
        crypto:generate_key(rsa, {Size * 8, PublicExpnt}),
    Key =
        ar_serialize:jsonify(
            {
                [
                    {kty, <<"RSA">>},
                    {ext, true},
                    {e, arweave_util:encode(Expnt)},
                    {n, arweave_util:encode(Pub)},
                    {d, arweave_util:encode(Priv)},
                    {p, arweave_util:encode(P1)},
                    {q, arweave_util:encode(P2)},
                    {dp, arweave_util:encode(E1)},
                    {dq, arweave_util:encode(E2)},
                    {qi, arweave_util:encode(C)}
                ]
            }
        ),
    Filename = ar_wallet:wallet_filepath(wallet_address, Pub, KeyType),
    case filelib:ensure_dir(Filename) of
        ok ->
            case ar_storage:write_file_atomic(Filename, Key) of
                ok ->
                    {{KeyType, Priv, Pub}, {KeyType, Pub}};
                Error2 ->
                    Error2
            end;
        Error ->
            Error
    end.

boot_peers([]) ->
    ok;
boot_peers([{TestType, Node} | Peers]) ->
    boot_peer(TestType, Node),
    boot_peers(Peers);
boot_peers(TestType) ->
    boot_peers(all_peers(TestType)).

boot_peer(TestType, Node) ->
    try_boot_peer(TestType, Node, ?MAX_BOOT_RETRIES).

try_boot_peer(_TestType, _Node, 0) ->
    %% You might log an error or handle this case specifically
    %% as per your application logic.
    {error, max_retries_exceeded};
try_boot_peer(TestType, Node, Retries) ->
    NodeName = peer_name(Node),
    Port = get_unused_port(),
    Cookie = erlang:get_cookie(),
    Paths = code:get_path(),
    %% `erl -pa A B C' prepends each path (effective order `C B A'), so
    %% reverse the main path to give peers the same module load order.
    PeerPaths = lists:reverse(Paths),
    ProjectRoot = project_root(),
    filelib:ensure_dir(filename:join(ProjectRoot, ".tmp/")),
    Schedulers = erlang:system_info(schedulers_online),
    %% Anchor every relative path to ProjectRoot via a leading `cd' so
    %% `-config config/sys.config' resolves under CT, which runs the BEAM
    %% from its per-run log dir. AR_DATA_DIR is made absolute for the same
    %% reason: a relative value would resolve against the wrong root when
    %% a main-BEAM caller joins it to a file path (e.g. `file:rename').
    RawCommand = string:join([
        "cd ~s &&",
        "AR_DATA_DIR=~s/.tmp/data_~s_~s",
        "AR_PORT=~p",
        "AR_JOIN_AUTO=false",
        "AR_DISABLE_DEVICE_LIMIT=true",
        "AR_DEBUG=true",
        "AR_NETWORK_CLIENT_HTTP_KEEPALIVE=4000",
        %% Cap dirty CPU schedulers (RandomX packing/hashing) to `+S';
        %% left at nproc (~64), co-tenant test nodes oversubscribe the box
        %% and starve the CI runner agent's heartbeat.
        "erl +S ~B:~B +SDcpu ~B",
        %% Shared test VM policy, notably `+sbwt none' so idle schedulers
        %% sleep rather than starve the CI runner's heartbeat.
        "-args_file config/vm.args.test",
        "-pa", "~s",
        "-config", "config/sys.config",
        "-noshell",
        "-name", "~s",
        "-setcookie", "~s",
        "-run ar main",
        "> ~s-~s.out 2>&1"
    ], " "),
    CommandParams = [
        ProjectRoot,
        ProjectRoot,
        atom_to_list(TestType),
        NodeName,
        Port,
        Schedulers,
        Schedulers,
        Schedulers,
        string:join(PeerPaths, " "),
        NodeName,
        Cookie,
        Node,
        get_node_namespace()
    ],
    Cmd = io_lib:format(RawCommand, CommandParams),
    run_command(Node, Cmd),
    case ar_test_await:http_ready(Node) of
        ok ->
            io:format("~s started at port ~p.~n", [NodeName, Port]),
            {node(), NodeName};
        {error, Reason} ->
            io:format("Error starting ~s: ~p. Retries left: ~p~n", [NodeName, Reason, Retries]),
            try_boot_peer(TestType, Node, Retries - 1)
    end.

%%--------------------------------------------------------------------
%% @doc run a command in asynchronous way using `spawn/1' instead of
%% using `&' from shell feature.
%% @end
%%--------------------------------------------------------------------
run_command(Node, Command) ->
    spawn(fun() -> run_command_init(Node, Command) end).

%% @hidden
run_command_init(Node, Command) ->
    io:format("Launching peer (~p) ~p: ~s~n", [self(), Node, Command]),
    try
        Result = os:cmd(Command),
        io:format("command result: ~p~n", [Result])
    catch
        E:R:S ->
            io:format("failed command: ~p:~p:~p~n", [E,R,S])
    end.

wait_for_peers([]) ->
    ok;
wait_for_peers([{_TestType, Node} | Peers]) ->
    ok = ar_test_await:http_ready(Node),
    wait_for_peers(Peers);
wait_for_peers(TestType) ->
    wait_for_peers(all_peers(TestType)).

self_node() ->
    list_to_atom(get_node()).

peer_name(Node) ->
    list_to_atom(
        atom_to_list(Node) ++ "-" ++ get_node_namespace() ++ "@127.0.0.1"
    ).

peer_port(Node) ->
    case get({peer_port, Node}) of
        undefined ->
            Port = ar_test_node:remote_call(
                Node, arweave_config, get, [[port]]),
            put({peer_port, Node}, Port),
            Port;
        Port ->
            Port
    end.

stop_peers([]) ->
    ok;
stop_peers([{_TestType, Node} | Peers]) ->
    stop_peer(Node),
    stop_peers(Peers);
stop_peers(TestType) ->
    stop_peers(all_peers(TestType)).

stop_peer(Node) ->
    try
        rpc:call(peer_name(Node), init, stop, [], 30000)
    catch
        E:R:S ->
            io:format("stop_peer error: ~p:~p:~p~n", [E,R,S]),
            %% we don't care if the node is already stopped
            ok
    end.

%% @doc Kill the peer's BEAM, wipe its on-disk data dir, and boot a fresh
%% one in its place, giving a test fixture a clean VM and filesystem free
%% of state or stray storage-module dirs from prior tests. The cached
%% `peer_port' is dropped so callers re-fetch the new BEAM's port.
restart_peer_beam(Node) ->
    NodeName = peer_name(Node),
    stop_peer(Node),
    ok = ar_test_await:node_down(NodeName),
    timer:sleep(500),
    wipe_peer_data_dir(Node),
    {_, NodeName} = boot_peer(e2e, Node),
    erlang:erase({peer_port, Node}),
    ok = ar_test_await:http_ready(Node).

%% @doc Recursively delete `Node''s data directory. Safe to call when
%% the peer BEAM is stopped; if the directory does not exist this is a
%% no-op.
wipe_peer_data_dir(Node) ->
    NodeName = atom_to_list(peer_name(Node)),
    DataDir = filename:join(project_root(),
        ".tmp/data_e2e_" ++ NodeName),
    case file:del_dir_r(DataDir) of
        ok -> ok;
        {error, enoent} -> ok
    end.

peer_ip({external, Peer}) ->
    Peer;
peer_ip(Node) ->
    {127, 0, 0, 1, peer_port(Node)}.

%% @doc Apply a map of per-leaf option_keys to the local node's
%% options registry. Test-mode invariants (`[disable_device_limit] =>
%% true`, the test HTTP keepalive) are layered on top of caller
%% overrides — they cannot be disabled by a caller's map.
%%
%% Overrides must be a map of `[option_key_segment, ...] => Value'.
%% `[storage_modules]` / `[repack_modules]` entries may be runtime
%% tuples or canonical maps - `arweave_config` normalizes them at set
%% time.
update_config(Overrides) when is_map(Overrides) ->
    Final = maps:merge(Overrides, #{
        [disable_device_limit]                  => true,
        [network, client, http, keepalive]      => ?TEST_HTTP_CLIENT_KEEPALIVE
    }),
    case arweave_config:force_config(Final) of
        ok ->
            ?LOG_INFO("Updated Config:"),
            arweave_config:log(),
            ok;
        {error, Failures} ->
            ?LOG_WARNING([{event, update_config_failed}, {failures, Failures}]),
            {error, Failures}
    end.

start_other_node(Node, B0, Overrides, WaitUntilSync) when is_map(Overrides) ->
    remote_call(Node, ar_test_node, start_node, [B0, Overrides, WaitUntilSync],
        ?RESTART_TIMEOUT).

%% @doc Start a node with the given genesis block, applying the given
%% override map via the options registry before starting the application.
start_node(B0, Overrides) when is_map(Overrides) ->
    start_node(B0, Overrides, true).
start_node(B0, Overrides, WaitUntilSync) when is_map(Overrides) ->
    ?LOG_INFO("Starting node"),
    clean_up_and_stop(),
    prometheus:start(),
    arweave_config:start(),
    DataDir = arweave_config:get([data_dir]),
    write_genesis_files(DataDir, B0),
    ok = update_config(Overrides),
    ok = arweave_limiter:start(),
    start_dependencies(),
    ar_test_await:node_joined(main),
    case WaitUntilSync of
        true ->
            wait_until_syncs_genesis_data();
        false ->
            ok
    end,
    ?LOG_INFO("Node started"),
    erlang:node().

%% @doc Launch the given number (>= 1, =< ?MAX_MINERS) of the mining nodes in the coordinated
%% mode plus an exit node and a validator node.
%% Return [Node1, ..., NodeN, ExitNode, ValidatorNode].
start_coordinated(MiningNodeCount) when MiningNodeCount >= 1, MiningNodeCount =< ?MAX_MINERS ->
    %% Set weave larger than what we'll cover with the 3 nodes so that every node can find
    %% a solution.
    [B0] = ar_weave:init([], get_difficulty_for_invalid_hash(), ar_block:partition_size() * 5),
    ExitPeer = peer_ip(peer1),
    ValidatorPeer = peer_ip(main),
    MinerNodes = lists:sublist([peer2, peer3, peer4], MiningNodeCount),

    BaseCMConfig = base_cm_config([ValidatorPeer]),
    RewardAddr = maps:get([mining, address], BaseCMConfig),
    ExitNodeOverrides = BaseCMConfig#{
        [mining, enabled] => true,
        [peers, local] => [arweave_util:format_peer(peer_ip(P)) || P <- MinerNodes]
    },
    %% Validator boots WITHOUT any trusted peers — `validate_trusted_peers/0'
    %% would otherwise try to GET each peer's network info during init,
    %% and the not-yet-running exit peer (and the validator's own
    %% not-yet-listening HTTP server) would both fail that probe and
    %% trigger `init:stop(1)'. We strip the `[peers, *, trusted]'
    %% entries from the base by building the override from scratch.
    ValidatorBase = maps:without(
        [[peers, trusted]],
        BaseCMConfig),
    ValidatorNodeOverrides = ValidatorBase#{
        [mining, enabled] => false,
        [cm, enabled] => false,
        [cm, api_secret] => not_set
    },

    %% Start the validator first so that its HTTP server is available when
    %% other nodes validate it as a trusted peer during startup.
    remote_call(main, ar_test_node, start_node, [B0, ValidatorNodeOverrides]),
    remote_call(peer1, ar_test_node, start_node, [B0, ExitNodeOverrides]), %% exit node

    lists:foreach(
        fun(I) ->
            MinerNode = lists:nth(I, MinerNodes),
            MinerPeers = lists:filter(fun(Peer) -> Peer /= MinerNode end, MinerNodes),
            MinerPeerIPs = [peer_ip(Peer) || Peer <- MinerPeers],
            MinerOverrides = BaseCMConfig#{
                [peers, cm_exit] => arweave_util:format_peer(ExitPeer),
                [peers, cm_peer] => [arweave_util:format_peer(Peer) || Peer <- MinerPeerIPs],
                [peers, local] => [arweave_util:format_peer(Peer) || Peer <- MinerPeerIPs ++ [ExitPeer]]
            },
            MinerStorageModules =
                get_cm_storage_modules(RewardAddr, I, MiningNodeCount),
            remote_call(MinerNode, ar_test_node, start_node,
                [B0, MinerOverrides#{
                    [storage_modules] => MinerStorageModules}, true])
        end,
        lists:seq(1, MiningNodeCount)
    ),

    MinerNodes ++ [peer1, main].

%% @doc Return a base map of overrides used to start a coordinated-mining node.
base_cm_config(Peers) ->
    RewardAddr = generate_address(peer1),
    maps:merge(#{[peers, trusted] => [arweave_util:format_peer(Peer) || Peer <- Peers]}, #{
        [mining, cache_size]                    => 128,
        [join, start_from_latest_state]         => true,
        [join, auto]                            => true,
        [mining, address]                       => RewardAddr,
        [mining, hashing_threads]               => 1,
        [disk_pool, workers]                       => 2,
        [gossip, header, workers]              => 2,
        [features, serve_tx_data_without_limits] => true,
        [features, serve_wallet_lists]          => true,
        [features, pack_served_chunks]          => true,
        [vdf, is_public_server]                 => true,
        [debug]                                 => true,
        [cm, enabled]                           => true,
        [cm, api_secret]                        => <<"test_coordinated_mining_secret">>,
        [cm, poll_interval]                     => 2000,
        [disable_device_limit]                  => true
    }).

mine() ->
    ar_node_worker:mine_one_block().

%% @doc Start mining on the given node. The node will be mining until it finds a block.
mine(Node) ->
    remote_call(Node, ar_test_node, mine, []).

%% @doc Fetch and decode a binary-encoded block by hash H from the HTTP API of the
%% given node. Return {ok, B} | {error, Reason}.
http_get_block(H, Node) ->
    Port = remote_call(Node, arweave_config, get, [[port]]),
    Peer = {127, 0, 0, 1, Port},
    case ar_http:req(#{ peer => Peer, method => get,
            path => "/block2/hash/" ++ binary_to_list(arweave_util:encode(H)) }) of
        {ok, {{<<"200">>, _}, _, BlockBin, _, _}} ->
            ar_serialize:binary_to_block(BlockBin);
        {error, Reason} ->
            {error, Reason};
        {ok, {{StatusCode, _}, _, Body, _, _}} ->
            {error, {StatusCode, Body}}
    end.

get_blocks(Node) ->
    remote_call(Node, ar_node, get_blocks, []).

invalid_solution() ->
    <<0:256>>.

valid_solution() ->
    <<255:256>>.

mock_to_force_invalid_h1() ->
    {
        ar_block, compute_h1,
        fun(H0, Nonce, Chunk1) ->
            %% First call the original compute_h1 function
            meck:passthrough([H0, Nonce, Chunk1]),
            %% Then return invalid solutions
            {invalid_solution(), invalid_solution()}
        end
    }.

%% @doc Suppress single-partition H2 wins so cross-node coordination tests
%% are deterministic. 
mock_to_force_cross_node_h2() ->
    {
        ar_mining_server, prepare_and_post_solution,
        fun(#mining_candidate{ cm_lead_peer = not_set }) ->
                ok;
            (CandidateOrSolution) ->
                meck:passthrough([CandidateOrSolution])
        end
    }.

%% @doc Mock out packing-related constants to replicate mainnet behavior.
mainnet_packing_mocks() ->
    [
        {ar_block, partition_size, fun() -> 3_600_000_000_000 end},
        {ar_block, strict_data_split_threshold, fun() -> 30_607_159_107_830 end},
        {ar_storage_module, get_overlap, fun(_) -> 104_857_600 end},
        {ar_block, get_sub_chunks_per_replica_2_9_entropy, fun() -> 1024 end},
        {ar_block, get_replica_2_9_entropy_sector_size, fun() -> 3_515_875_328 end}
    ].

get_difficulty_for_invalid_hash() ->
    %% Set the difficulty just high enough to exclude the invalid_solution(), this lets
    %% us selectively disable one- or two-chunk mining in tests.
    binary:decode_unsigned(invalid_solution(), big) + 1.

%%%===================================================================
%%% Private functions.
%%%===================================================================

start_dependencies() ->
    ok = arweave_limiter:start(),
    {ok, _} = application:ensure_all_started(arweave, temporary),
    ok.

clean_up_and_stop() ->
    _ = stop(),
    DataDir = arweave_config:get([data_dir]),
    ?LOG_DEBUG([{event, clean_up_and_stop}, {data_dir, DataDir}]),
    ok = filelib:ensure_dir(DataDir),
    {ok, Entries} = file:list_dir_all(DataDir),
    lists:foreach(
        fun    ("wallets") ->
                ok;
            (Entry) ->
                ?LOG_DEBUG([{event, clean_up_and_stop},
                    {delete, filename:join(DataDir, Entry)}]),
                ok = file:del_dir_r(filename:join(DataDir, Entry))
        end,
        Entries
    ),
    %% Wipe the entire arweave_config store and return to load mode,
    %% then re-run the standard bootstrap. Bootstrap re-reads
    %% the `AR_*' env vars set by `ar_test_runner:start_for_tests/1'
    %% (main) or `try_boot_peer/3' (peers), so per-VM scaffolding
    %% (`[data_dir]', `[port]', ...) survives reset via the same path
    %% the node uses at first boot. No test-only env handling.
    ok = arweave_config:restore(#{store => [], runtime => false}),
    ok = arweave_config:bootstrap([]),
    ok.

write_genesis_files(DataDir, B0) ->
    BH = B0#block.indep_hash,
    BlockDir = filename:join(DataDir, ?BLOCK_DIR),
    ok = filelib:ensure_dir(BlockDir ++ "/"),
    BlockFilepath = filename:join(BlockDir, binary_to_list(arweave_util:encode(BH)) ++ ".bin"),
    ok = file:write_file(BlockFilepath, ar_serialize:block_to_binary(B0)),
    TXDir = filename:join(DataDir, ?TX_DIR),
    ok = filelib:ensure_dir(TXDir ++ "/"),
    lists:foreach(
        fun(TX) ->
            TXID = TX#tx.id,
            TXFilepath = filename:join(TXDir, binary_to_list(arweave_util:encode(TXID)) ++ ".json"),
            TXJSON = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
            ok = file:write_file(TXFilepath, TXJSON)
        end,
        B0#block.txs
    ),
    _ = ar_kv:create_ets(),
    {ok, _} = ar_kv:start_link(),
    try
        ok = ar_kv:open(#{
            path => filename:join([DataDir, ?ROCKS_DB_DIR, "reward_history_db"]),
            name => reward_history_db}),
        ok = ar_kv:open(#{
            path => filename:join([DataDir, ?ROCKS_DB_DIR, "block_time_history_db"]),
            name => block_time_history_db}),
        ok = ar_kv:open(#{
            path => filename:join([DataDir, ?ROCKS_DB_DIR, "block_index_db"]),
            name => block_index_db}),
        H = B0#block.indep_hash,
        WeaveSize = B0#block.weave_size,
        TXRoot = B0#block.tx_root,
        ok = ar_kv:put(block_index_db, << 0:256 >>,
                term_to_binary({H, WeaveSize, TXRoot, <<>>})),
        ok = ar_kv:put(reward_history_db, H, term_to_binary(hd(B0#block.reward_history))),
        case ar_fork:height_2_7() of
            0 ->
                ok = ar_kv:put(block_time_history_db, H,
                        term_to_binary(hd(B0#block.block_time_history)));
            _ ->
                ok
        end
    after
        case whereis(ar_kv) of
            undefined ->
                ok;
            _ ->
                _ = catch gen_server:stop(ar_kv),
                ok
        end,
        case ets:info(ar_kv) of
            undefined ->
                ok;
            _ ->
                _ = ets:delete(ar_kv)
        end
    end,
    WalletListDir = filename:join(DataDir, ?WALLET_LIST_DIR),
    ok = filelib:ensure_dir(WalletListDir ++ "/"),
    RootHash = B0#block.wallet_list,
    WalletListFilepath =
        filename:join(WalletListDir, binary_to_list(arweave_util:encode(RootHash)) ++ ".json"),
    WalletListJSON =
        ar_serialize:jsonify(
            ar_serialize:wallet_list_to_json_struct(B0#block.reward_addr, false,
                    B0#block.account_tree)
        ),
    ok = file:write_file(WalletListFilepath, WalletListJSON).

%% @doc Wait until every chunk in `[Left, Right)' is recorded under
%% `Packing' (or any packing when `Packing = any').
wait_until_syncs_data(Left, Right, WeaveSize, _Packing)
        when Left >= Right orelse
            Left >= WeaveSize orelse
            (Right - Left < ?DATA_CHUNK_SIZE) orelse
            (WeaveSize - Left < ?DATA_CHUNK_SIZE) ->
    ok;
wait_until_syncs_data(Left, Right, WeaveSize, any) ->
    ok = ar_test_await:chunk_recorded(main, Left + 1, #{}),
    wait_until_syncs_data(Left + ?DATA_CHUNK_SIZE, Right, WeaveSize, any);
wait_until_syncs_data(Left, Right, WeaveSize, Packing) ->
    ok = ar_test_await:chunk_recorded(main, Left + 1, #{packing => Packing}),
    wait_until_syncs_data(Left + ?DATA_CHUNK_SIZE, Right, WeaveSize, Packing).

get_cm_storage_modules(RewardAddr, 1, 1) ->
    %% When there's only 1 node it covers all 3 storage modules.
    get_cm_storage_modules(RewardAddr, 1, 3) ++
    get_cm_storage_modules(RewardAddr, 2, 3) ++
    get_cm_storage_modules(RewardAddr, 3, 3);
get_cm_storage_modules(RewardAddr, N, MiningNodeCount)
        when MiningNodeCount == 2 orelse MiningNodeCount == 3 ->
    %% skip partitions so that no two nodes can mine the same range even accounting for ?OVERLAP
    %% Note that replica_2_9 modules do not have ?OVERLAP.
    RangeNumber = lists:nth(N, [0, 2, 4]),
    [{RangeNumber * ar_block:partition_size(),
        (RangeNumber + 1) * ar_block:partition_size(),
        storage_module_packing(RewardAddr, 0)}].

remote_call(Node, Module, Function, Args) ->
    remote_call(Node, Module, Function, Args, ?REMOTE_CALL_TIMEOUT).

remote_call(Node, Module, Function, Args, Timeout) ->
    NodeName = peer_name(Node),
    case node() == NodeName of
        true ->
            apply(Module, Function, Args);
        false ->
            Key = rpc:async_call(NodeName, Module, Function, Args),
            Result = arweave_util:do_until(
                fun() ->
                    case rpc:nb_yield(Key) of
                        timeout ->
                            false;
                        {value, Reply} ->
                            {ok, Reply}
                    end
                end,
                200,
                Timeout
            ),
            case Result of
                {error, timeout} ->
                    ?LOG_ERROR("Timed out (~pms) waiting for the rpc reply; module: ~p, function: ~p, "
                            "args: ~p, node: ~p.~n", [Timeout, Module, Function, Args, Node]);
                _ ->
                    ok
            end,
            ?assertMatch({ok, _}, Result),
            element(2, Result)
    end.

%%%===================================================================
%%% Legacy public interface.
%%%===================================================================

%% @doc Start a fresh node.
start() ->
    start(#{}).

start(Options) when is_map(Options) ->
    prometheus:start(),
    arweave_config:start(),
    ok = arweave_limiter:start(),
    B0 =
        case maps:get(b0, Options, not_set) of
            not_set ->
                hd(ar_weave:init());
            Value ->
                Value
        end,
    RewardAddr =
        case maps:get(addr, Options, not_set) of
            not_set ->
                generate_address(main);
            Addr ->
                Addr
        end,
    %% `config' is an override map `#{Key => Value}'. An empty map
    %% means "use whatever's currently in the options registry + the test
    %% defaults applied by `start/3'".
    Overrides =
        case maps:get(config, Options, not_set) of
            not_set ->
                #{};
            Value2 when is_map(Value2) ->
                Value2
        end,
    InlineOverrides = maps:filter(fun(Key, _Value) -> is_list(Key) end, Options),
    AllOverrides = maps:merge(InlineOverrides, Overrides),
    StorageOverrides =
        case maps:is_key([storage_modules], AllOverrides) of
            true ->
                #{};
            false ->
                storage_module_config(RewardAddr, [0], Options)
        end,
    start(B0, RewardAddr, maps:merge(StorageOverrides, AllOverrides));
start(B0) ->
    start(#{ b0 => B0 }).
start(B0, RewardAddr) ->
    start(#{ b0 => B0, addr => RewardAddr }).

%% @doc Start a fresh node with the given genesis block, mining address, and
%% caller overrides (`#{Key => Value}' — see `update_config/1' for the
%% key shape).
start(B0, RewardAddr, Overrides) when is_map(Overrides) ->
    StorageOverrides =
        case maps:is_key([storage_modules], Overrides) of
            true ->
                #{};
            false ->
                storage_module_config(RewardAddr, [0])
        end,
    start_with_overrides(B0, RewardAddr, maps:merge(StorageOverrides, Overrides)).

start_with_overrides(B0, RewardAddr, Overrides) when is_map(Overrides) ->
    clean_up_and_stop(),
    prometheus:start(),
    arweave_config:start(),
    DataDir = arweave_config:get([data_dir]),
    write_genesis_files(DataDir, B0),
    %% Test-mode defaults. Test invariants applied by `update_config/1'
    %% always win; caller overrides win over these defaults but lose to
    %% the invariants. (Peer-role resets happen in `clean_up_and_stop/0'
    %% via the writer module — they can't be expressed per-leaf.)
    TestDefaults = #{
        [join, start_from_latest_state]         => true,
        [join, auto]                            => true,
        [mining, address]                       => RewardAddr,
        [disk_space_check_frequency]            => 1000,
        [disk_pool, workers]                       => 2,
        [gossip, header, workers]              => 2,
        [features, serve_tx_data_without_limits] => true,
        [features, serve_wallet_lists]          => true,
        [debug]                                 => true
    },
    ok = update_config(maps:merge(TestDefaults, Overrides)),
    ok = arweave_limiter:start(),
    start_dependencies(),
    ar_test_await:node_joined(main),
    wait_until_syncs_genesis_data().

restart() ->
    ?LOG_INFO("Restarting node"),
    stop(),
    start_dependencies(),
    ar_test_await:node_joined(main).

restart(Node) ->
    remote_call(Node, ?MODULE, restart, [], ?RESTART_TIMEOUT).

start_peer(Node, Args) when is_map(Args) ->
    ?LOG_DEBUG([{event, start_peer}, {peer, Node}]),
    remote_call(Node, ?MODULE, start, [Args], ?PEER_START_TIMEOUT),
    ar_test_await:node_joined(Node),
    wait_until_syncs_genesis_data(Node);

%% @doc Start a fresh peer node with the given genesis block.
start_peer(Node, B0) ->
    start_peer(Node, #{ b0 => B0 }).

%% @doc Start a fresh peer node with the given genesis block and mining address.
start_peer(Node, B0, RewardAddr) ->
    start_peer(Node, #{ b0 => B0, addr => RewardAddr }).

%% @doc Start a fresh peer node with the given genesis block, mining address, and config.
start_peer(Node, B0, RewardAddr, Config) ->
    start_peer(Node, #{ b0 => B0, addr => RewardAddr, config => Config }).

%% @doc Fetch the fee estimation and the denomination (call GET /price2/[size])
%% from the given node.
get_tx_price(Node, DataSize) ->
    get_tx_price(Node, DataSize, <<>>).

%% @doc Fetch the fee estimation and the denomination (call GET /price2/[size]/[addr])
%% from the given node.
get_tx_price(Node, DataSize, Target) ->
    Peer = peer_ip(Node),
    Path = "/price/" ++ integer_to_list(DataSize) ++ "/"
            ++ binary_to_list(arweave_util:encode(Target)),
    {ok, {{<<"200">>, _}, _, Reply, _, _}} =
        ar_http:req(#{
            method => get,
            peer => Peer,
            path => Path
        }),
    Fee = binary_to_integer(Reply),
    {Fee2, Denomination} = get_tx_price2(Node, DataSize, Target),
    case Fee2 of
        Fee ->
            {Fee, Denomination};
        _ ->
            ?assert(false, io_lib:format("Fee mismatch, expected: ~B, got: ~B.", [Fee, Fee2]))
    end.

get_tx_price2(Node, DataSize, Target) ->
    Path = "/price2/" ++ integer_to_list(DataSize) ++ "/"
            ++ binary_to_list(arweave_util:encode(Target)),
    {ok, {{<<"200">>, _}, _, Reply, _, _}} =
        ar_http:req(#{
            method => get,
            peer => peer_ip(Node),
            path => Path
        }),
    Map = jiffy:decode(Reply, [return_maps]),
    {binary_to_integer(maps:get(<<"fee">>, Map)), maps:get(<<"denomination">>, Map)}.

get_tx_denomination(Node, DataSize, Target) ->
    {_, Denomination} = get_tx_price2(Node, DataSize, Target),
    Denomination.

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]) from the given node.
get_optimistic_tx_price(Node, DataSize) ->
    get_optimistic_tx_price(Node, DataSize, <<>>).

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]/[addr]) from the given
%% node.
get_optimistic_tx_price(Node, DataSize, Target) ->
    Path = "/optimistic_price/" ++ integer_to_list(DataSize) ++ "/"
            ++ binary_to_list(arweave_util:encode(Target)),
    {ok, {{<<"200">>, _}, _, Reply, _, _}} =
        ar_http:req(#{
            method => get,
            peer => peer_ip(Node),
            path => Path
        }),
    binary_to_integer(maps:get(<<"fee">>, jiffy:decode(Reply, [return_maps]))).

%% @doc Return a signed format=2 transaction with the minimum required fee fetched from
%% GET /price/0 on the peer1 node.
sign_tx(Wallet) ->
    sign_tx(peer1, Wallet, #{ format => 2 }, fun ar_tx:sign/2).

%% @doc Return a signed format=2 transaction with properties from the given Args map.
%% If the reward is not in Args, fetch it from GET /price/{data_size}
%% or GET /price/{data_size}/{target} (if the target is specified) on the peer1 node.
%% Use sign_tx/3 when Args includes a last_tx fetched from another node.
sign_tx(Wallet, Args) ->
    sign_tx(peer1, Wallet, insert_root(Args#{ format => 2 }), fun ar_tx:sign/2).

%% @doc Like sign_tx/2, but use the given Node to fetch the fee estimation and default
%% block anchor from.
sign_tx(Node, Wallet, Args) ->
    sign_tx(Node, Wallet, insert_root(Args#{ format => 2 }), fun ar_tx:sign/2).

%% @doc Like sign_tx/1 but return a format=1 transaction.
sign_v1_tx(Wallet) ->
    sign_tx(peer1, Wallet, #{ format => 1 }, fun ar_tx:sign_v1/2).

%% @doc Like sign_tx/2 but return a format=1 transaction.
%% Use sign_v1_tx/3 when TXParams includes a last_tx fetched from another node.
sign_v1_tx(Wallet, TXParams) ->
    sign_tx(peer1, Wallet, TXParams#{ format => 1 }, fun ar_tx:sign_v1/2).

%% @doc Like sign_tx/3 but return a format=1 transaction.
sign_v1_tx(Node, Wallet, Args) ->
    sign_tx(Node, Wallet, Args#{ format => 1 }, fun ar_tx:sign_v1/2).

%%%===================================================================
%%% Legacy private functions.
%%%===================================================================

insert_root(Params) ->
    case {maps:get(data, Params, <<>>), maps:get(data_root, Params, <<>>)} of
        {<<>>, _} ->
            Params;
        {Data, <<>>} ->
            TX = ar_tx:generate_chunk_tree(#tx{ data = Data }),
            Params#{ data_root => TX#tx.data_root };
        _ ->
            Params
    end.

sign_tx(Node, Wallet, Args, SignFun) ->
    {_, {_, Pub}} = Wallet,
    Data = maps:get(data, Args, <<>>),
    DataSize = maps:get(data_size, Args, byte_size(Data)),
    Format = maps:get(format, Args, 2),
    Target = maps:get(target, Args, <<>>),
    {Fee, Denomination} = tx_fee_and_denomination(Node, DataSize, Target, Format, Args),
    SignFun(
        (ar_tx:new())#tx{
            owner = Pub,
            reward = Fee,
            data = Data,
            target = Target,
            quantity = maps:get(quantity, Args, 0),
            tags = maps:get(tags, Args, []),
            last_tx = maps:get(last_tx, Args, get_tx_anchor(Node)),
            data_size = DataSize,
            data_root = maps:get(data_root, Args, <<>>),
            format = Format,
            denomination = Denomination
        },
        Wallet
    ).

tx_fee_and_denomination(Node, DataSize, Target, Format, Args) ->
    case maps:get(reward, Args, none) of
        none ->
            {Fee, Denomination} = get_tx_price(Node, DataSize, Target),
            {tx_fee(Format, Fee), maps:get(denomination, Args, Denomination)};
        AssignedFee ->
            Denomination =
                case maps:find(denomination, Args) of
                    {ok, AssignedDenomination} ->
                        AssignedDenomination;
                    error ->
                        get_tx_denomination(Node, DataSize, Target)
                end,
            {AssignedFee, Denomination}
    end.

tx_fee(1, Fee) ->
    %% Make sure the v1 tx is not malleable by assigning a fee with only
    %% the first digit being non-zero.
    FirstDigit = binary_to_integer(binary:part(integer_to_binary(Fee), {0, 1})),
    Len = length(integer_to_list(Fee)),
    trunc((FirstDigit + 1) * math:pow(10, Len - 1));
tx_fee(_, Fee) ->
    Fee.

stop() ->
    %% If the arweave app is still starting (this node's boot answers HTTP
    %% before ar:start/2 returns, so a stop can arrive mid-boot), let the
    %% start finish first: application:stop/1 reports a starting app as
    %% not_started, and stopping the dependency apps below would then yank
    %% prometheus out from under the still-running ar:start/2, failing the
    %% permanent app and halting the BEAM.
    _ = wait_for_app_start_settled(arweave, 120_000),
    %% Match the ar_kv supervisor shutdown window so RocksDB can close before
    %% the next test wipes or reuses the data directory.
    case stop_application(arweave, 300_000) of
        ok ->
            ok;
        {error, {not_started, arweave}} ->
            ok;
        {error, timeout} ->
            ?LOG_WARNING([{event, application_stop_timeout}, {app, arweave}]),
            force_stop_application(arweave)
    end,
    ar:stop_dependencies(),
    arweave_limiter:stop().

wait_for_app_start_settled(App, Timeout) ->
    Start = erlang:monotonic_time(millisecond),
    wait_for_app_start_settled(App, Start, Timeout).

wait_for_app_start_settled(App, Start, Timeout) ->
    Starting = proplists:get_value(starting, application:info(), []),
    case lists:keymember(App, 1, Starting) of
        false ->
            ok;
        true ->
            case erlang:monotonic_time(millisecond) - Start > Timeout of
                true ->
                    {error, timeout};
                false ->
                    timer:sleep(100),
                    wait_for_app_start_settled(App, Start, Timeout)
            end
    end.

stop_application(App, Timeout) ->
    Parent = self(),
    Ref = make_ref(),
    Start = erlang:monotonic_time(millisecond),
    Pid = spawn(fun() -> Parent ! {Ref, application:stop(App)} end),
    receive
        {Ref, ok} ->
            ar_test_await:application_stopped(App, remaining_timeout(Start, Timeout));
        {Ref, Result} ->
            Result
    after Timeout ->
        exit(Pid, kill),
        {error, timeout}
    end.

remaining_timeout(Start, Timeout) ->
    max(0, Timeout - (erlang:monotonic_time(millisecond) - Start)).

force_stop_application(App) ->
    case application_controller:get_master(App) of
        Master when is_pid(Master) ->
            exit(Master, kill),
            _ = ar_test_await:application_stopped(App, 10_000),
            _ = ar_test_await:ar_kv_stopped(10_000),
            ok;
        _ ->
            ok
    end.

stop(Node) ->
    remote_call(Node, ar_test_node, stop, []).

rejoin_on(#{ node := Node, join_on := JoinOnNode } = Options) ->
    Overrides = maps:get(config, Options, generate_join_config(Node)),
    join_on(#{ node => Node, join_on => JoinOnNode, config => Overrides }, true).

generate_join_config(Node) ->
    remote_call(Node, ar_test_node, generate_join_config, []).

%% @doc Build a map of overrides for a fresh join. Returns the
%% per-leaf override map; the matching set of storage modules is
%% applied by `join/3' via `write_list/1' (the writer handles
%% clear-before-write so they're authoritative).
generate_join_config() ->
    RewardAddr = generate_address(main),
    #{[mining, address] => RewardAddr}.

%% @doc The default storage modules implied by a join config —
%% the first default test module packed for the config's mining address.
%% Returns `[]' when the config has no `[mining, address]' (the caller is
%% expected to pass an explicit `storage_modules' arg in that case).
generate_join_storage_modules(JoinConfig, Options) ->
    case maps:get([mining, address], JoinConfig, not_set) of
        not_set ->
            [];
        RewardAddr ->
            wide_storage_modules(RewardAddr, [0], Options)
    end.

join_on(Params) ->
    join_on(Params, false).

join_on(#{ node := Node, join_on := JoinOnNode } = Params, Rejoin) ->
    {BaseOverrides, HasConfig} =
        case maps:get(config, Params, not_set) of
            not_set ->
                {#{}, false};
            Value ->
                {Value, true}
        end,
    AddressOverrides =
        case {maps:get(addr, Params, not_set), HasConfig} of
            {not_set, true} ->
                #{};
            {not_set, false} ->
                generate_join_config(Node);
            {Addr, _} ->
                #{[mining, address] => Addr}
        end,
    InlineOverrides = maps:filter(fun(Key, _Value) -> is_list(Key) end, Params),
    Overrides = maps:merge(InlineOverrides, maps:merge(AddressOverrides, BaseOverrides)),
    %% Storage modules in priority order:
    %%   1. caller-supplied `[storage_modules]' override wins;
    %%   2. `Overrides' carries a `[mining, address]' → regenerate to
    %%      match the new address (`addr' and fresh joins take this path);
    %%   3. otherwise leave the prior modules untouched (`not_set').
    %%      The caller is supplying a narrow override (e.g. flipping
    %%      `[mining, enabled]'); replacing modules here would strand
    %%      on-disk chunks already keyed off the existing modules.
    StorageOverrides =
        case maps:is_key([storage_modules], Overrides) of
            true ->
                #{};
            false ->
                case maps:is_key([mining, address], Overrides) of
                    true ->
                        #{[storage_modules] =>
                            generate_join_storage_modules(Overrides, Params)};
                    false ->
                        #{}
                end
        end,
    remote_call(Node, ar_test_node, join,
        [JoinOnNode, Rejoin, maps:merge(StorageOverrides, Overrides)],
        ?REMOTE_CALL_TIMEOUT).

join(JoinOnNode, Rejoin, Overrides) when is_map(Overrides) ->
    Peer = peer_ip(JoinOnNode),
    case Rejoin of
        true ->
            stop(),
            %% Keep current config values but return the lifecycle to load
            %% mode, which the boot validators (e.g.
            %% `ar_node_worker:validate_trusted_peers/1') expect during
            %% ar_sup init.
            Snapshot = arweave_config:snapshot(),
            ok = arweave_config:restore(Snapshot#{runtime => false});
        false ->
            clean_up_and_stop()
    end,
    prometheus:start(),
    arweave_config:start(),
    %% Join-time defaults; caller overrides win on entries the caller
    %% sets. Routed through update_config/1 because the spec
    %% store is already in runtime mode after restart and these
    %% overrides include static specs (peers, auto_join, ...).
    JoinDefaults = #{
        [join, start_from_latest_state] => false,
        [join, auto]                    => true,
        [peers, trusted]                => [arweave_util:format_peer(Peer)]
    },
    ok = update_config(maps:merge(JoinDefaults, Overrides)),
    start_dependencies(),
    ar_test_await:node_joined(main),
    whereis(ar_node_worker).

%% @doc Return the default packing tuple for a test storage module.
%% Options currently supports `packing' to force `spora_2_6' or `replica_2_9'.
storage_module_packing(RewardAddr, Index) ->
    storage_module_packing(RewardAddr, Index, #{}).

storage_module_packing(RewardAddr, _Index, Options) ->
    case maps:get(packing, Options, not_set) of
        spora_2_6 ->
            {spora_2_6, RewardAddr};
        replica_2_9 ->
            {replica_2_9, RewardAddr};
        not_set ->
            case ar_fork:height_2_9() of
                0 -> {replica_2_9, RewardAddr};
                _ -> {spora_2_6, RewardAddr}
            end
    end.

%% @doc Storage module lists are passed to `[storage_modules]`
%% overrides as-is - `arweave_config` accepts runtime tuples directly.
storage_module_configs(StorageModules) ->
    StorageModules.

%% @doc Return `#{[storage_modules] => Configs}` for wide modules covering `Partitions`.
storage_module_config(RewardAddr, Partitions) ->
    storage_module_config(RewardAddr, Partitions, #{}).

storage_module_config(RewardAddr, Partitions, Options) ->
    #{
        [storage_modules] => storage_module_configs(
            wide_storage_modules(RewardAddr, Partitions, Options))
    }.

%% @doc Return wide runtime module tuples `{RangeStart, RangeEnd,
%% Packing}`. Each module spans `10 * ar_block:partition_size()`.
%% Use `storage_module_config/2,3` when building override maps.
wide_storage_modules(RewardAddr, Partitions) ->
    wide_storage_modules(RewardAddr, Partitions, #{}).

wide_storage_modules(RewardAddr, Partitions, Options) ->
    Size = 10 * ar_block:partition_size(),
    [{Partition * Size, (Partition + 1) * Size,
            storage_module_packing(RewardAddr, Partition, Options)}
        || Partition <- Partitions].

connect_peers(Node, Peer) ->
    remote_call(Node, ar_test_node, connect_to_peer, [Peer]).

connect_to_peer(Node) ->
    %% Unblock connections possibly blocked in the prior test code.
    ar_http:unblock_peer_connections(),
    remote_call(Node, ar_http, unblock_peer_connections, []),
    Peer = peer_ip(Node),
    Self = self_node(),
    %% Make requests to the nodes to make them discover each other.
    {ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
        ar_http:req(#{
            method => get,
            peer => Peer,
            path => "/info",
            headers => p2p_headers(Self)
        }),
    ok = ar_test_await:peer_listed(Node, peer_ip(Self)),
    {ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
        ar_http:req(#{
            method => get,
            peer => peer_ip(Self),
            path => "/info",
            headers => p2p_headers(Node)
        }),
    ok = ar_test_await:peer_listed(main, Peer).

disconnect_peers(Node, Peer) ->
    remote_call(Node, ar_test_node, disconnect_from, [Peer]).

disconnect_from(Node) ->
    ar_http:block_peer_connections(),
    remote_call(Node, ar_http, block_peer_connections, []).

with_gossip_paused(Node, Fun) when is_function(Fun, 0) ->
    ok = remote_call(Node, ar_bridge, stop_gossip, []),
    try
        Fun()
    after
        ok = remote_call(Node, ar_bridge, start_gossip, [])
    end.

wait_until_syncs_genesis_data(Node) ->
    ok = remote_call(Node, ar_test_node, wait_until_syncs_genesis_data, [], 100_000).

wait_until_syncs_genesis_data() ->
    ok = ar_test_await:node_joined(main),
    B = ar_node:get_current_block(),
    WeaveSize = B#block.weave_size,
    ?LOG_INFO([{event, wait_until_syncs_genesis_data}, {status, initial_sync_started},
        {weave_size, WeaveSize}]),
    StorageModules = arweave_config:storage_modules(),
    [wait_until_syncs_data(Start, End, WeaveSize, any)
            || {Start, End, _Packing} <- StorageModules],
    ?LOG_INFO([{event, wait_until_syncs_genesis_data}, {status, initial_sync_complete}]),
    %% Once the data is stored in the disk pool, make the storage modules
    %% copy the missing data over from each other. This procedure is executed on startup
    %% but the disk pool did not have any data at the time.
    [
        ar_chunk_copy:start_copy(ar_storage_module:id(M))
        || M <- StorageModules
    ],
    [wait_until_syncs_data(Start, End, WeaveSize, Packing)
            || {Start, End, Packing} <- StorageModules],
    ?LOG_INFO([{event, wait_until_syncs_genesis_data}, {status, cross_module_sync_complete}]),
    ok.

assert_post_tx_to_peer(Node, TX) ->
    assert_post_tx_to_peer(Node, TX, true).

assert_post_tx_to_peer(Node, TX, Wait) ->
    assert_post_tx_to_peer(Node, TX, Wait, 3).

assert_post_tx_to_peer(Node, TX, Wait, Retries) ->
    {ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_peer(Node, TX, Wait, Retries).

post_tx_to_peer(Node, TX) ->
    post_tx_to_peer(Node, TX, true).

post_tx_to_peer(Node, TX, Wait) ->
    post_tx_to_peer(Node, TX, Wait, 3).

post_tx_to_peer(Node, TX, Wait, Retries) ->
    Reply = post_tx_json(Node, ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))),
    case Reply of
        {ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
            case Wait of
                true ->
                    ?assertEqual(ok, ar_test_await:txs_ready_for_mining(Node, [TX]));
                false ->
                    ok
            end;
        _ when Retries > 0 ->
            ?debugFmt("Failed to post transaction, retrying. Error: ~p~nRetries: ~p~n", [Reply, Retries]),
            timer:sleep(3000),
            post_tx_to_peer(Node, TX, Wait, Retries - 1);
        _ ->
            ErrorInfo =
                case Reply of
                    {ok, {{StatusCode, _}, _, Text, _, _}} ->
                        {StatusCode, Text};
                    Other ->
                        Other
                end,
            Addr =
                case TX#tx.owner of
                    <<>> ->
                        DataSegment = ar_tx:generate_signature_data_segment(TX),
                        {ok, PubKey} = ar_wallet:recover_key(DataSegment,
                                TX#tx.signature, TX#tx.signature_type),
                        ar_wallet:to_address(PubKey, TX#tx.signature_type);
                    _ ->
                        ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)
            end,
            ?debugFmt(
                "Failed to post transaction.~nTX: ~s.~nTX format: ~B.~nTX fee: ~B.~n"
                "TX size: ~B.~nTX last_tx: ~s.~nTX owner: ~s.~nTX owner address: ~s.~n"
                "Error(s): ~p.~nReply: ~p.~n",
                [arweave_util:encode(TX#tx.id), TX#tx.format, TX#tx.reward,
                    TX#tx.data_size, arweave_util:encode(TX#tx.last_tx),
                    arweave_util:encode(TX#tx.owner),
                    arweave_util:encode(Addr),
                    remote_call(Node, ar_tx_db, get_error_codes, [TX#tx.id]),
                    ErrorInfo]),
            noop
    end,
    Reply.

post_tx_json(Node, JSON) ->
    ar_http:req(#{
        method => post,
        peer => peer_ip(Node),
        path => "/tx",
        body => JSON
    }).

get_tx_anchor(Node) ->
    {ok, {{<<"200">>, _}, _, Reply, _, _}} =
        ar_http:req(#{
            method => get,
            peer => peer_ip(Node),
            path => "/tx_anchor"
        }),
    arweave_util:decode(Reply).

get_tx_confirmations(Node, TXID) ->
    Response =
        ar_http:req(#{
            method => get,
            peer => peer_ip(Node),
            path => "/tx/" ++ binary_to_list(arweave_util:encode(TXID)) ++ "/status"
        }),
    case Response of
        {ok, {{<<"200">>, _}, _, Reply, _, _}} ->
            {Status} = ar_serialize:dejsonify(Reply),
            lists:keyfind(<<"number_of_confirmations">>, 1, Status);
        {ok, {{<<"404">>, _}, _, _, _, _}} ->
            -1
    end.

%% Returns a {Setup, Cleanup} pair for use as an eunit fixture. The
%% local side delegates to `ar_test_util' (which is also what local-only
%% `ar_test_util:with_mocked/2,3' uses); on top of that, this function
%% broadcasts each mock to every peer node via `remote_call'. That
%% peer-side broadcast is the reason `test_with_all_nodes_mocked'
%% requires the slow path — fast-tagged modules whose mocks don't need
%% to be visible on peers should use `ar_test_util:with_mocked/2,3'
%% instead.
%% @doc Return the setup and cleanup functions mocking the functions on every
%% node. See run_with_mocked/3 for a scoped variant.
mock_all_nodes(Mocks) ->
    Nodes = [main | [Node || {_TestType, Node} <- all_peers(test)]],
    {fun() -> mock_nodes(Nodes, Mocks) end,
     fun(Modules) -> unmock_nodes(Nodes, Modules) end}.

%% @doc Run Fun with the functions mocked on the given nodes (main included
%% when listed) and unmock them afterwards.
run_with_mocked(Nodes, Mocks, Fun) ->
    Modules = mock_nodes(Nodes, Mocks),
    try
        Fun()
    after
        unmock_nodes(Nodes, Modules)
    end.

%% @doc Mock the functions on the given nodes. Return the mocked modules, to
%% pass to unmock_nodes/2.
mock_nodes(Nodes, Mocks) ->
    Modules = lists:usort([Module || {Module, _, _} <- Mocks]),
    with_meck_lock(fun() ->
        lists:foreach(fun(Node) ->
            [remote_call(Node, ar_test_util, new_mock, [Module, [no_link, passthrough]])
                || Module <- Modules],
            [remote_call(Node, ar_test_util, mock_function, [Module, F, Mock])
                || {Module, F, Mock} <- Mocks]
        end, Nodes)
    end),
    Modules.

unmock_nodes(Nodes, Modules) ->
    with_meck_lock(fun() ->
        [remote_call(Node, ar_test_util, unmock_module, [Module])
            || Node <- Nodes, Module <- Modules]
    end),
    ok.

%% @doc Execute Fun under a distributed lock to avoid concurrent meck operations.
with_meck_lock(Fun) when is_function(Fun, 0) ->
    global:trans({arweave, meck_lock}, Fun).

test_with_all_nodes_mocked(Functions, TestFun) ->
    test_with_all_nodes_mocked(Functions, TestFun, ?TEST_MOCKED_FUNCTIONS_TIMEOUT).

test_with_all_nodes_mocked(Functions, TestFun, Timeout) ->
    {Setup, Cleanup} = mock_all_nodes(Functions),
    {
        foreach,
        Setup, Cleanup,
        [{timeout, Timeout, TestFun}]
    }.

post_and_mine(#{ miner := Node, await_on := AwaitOnNode }, TXs) ->
    CurrentHeight = remote_call(Node, ar_node, get_height, []),
    lists:foreach(fun(TX) -> assert_post_tx_to_peer(Node, TX) end, TXs),
    mine(Node),
    {ok, [{H, _, _} | _]} = ar_test_await:node_height(AwaitOnNode, CurrentHeight + 1),
    remote_call(AwaitOnNode, ar_test_await, block_stored, [H, true],
      ?POST_AND_MINE_TIMEOUT).

post_block(B, ExpectedResult) when not is_list(ExpectedResult) ->
    post_block(B, [ExpectedResult], peer_ip(main));
post_block(B, ExpectedResults) ->
    post_block(B, ExpectedResults, peer_ip(main)).

post_block(B, ExpectedResults, Peer) ->
    Result = send_new_block_with_retry(Peer, B, ExpectedResults, 2),
    ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, Result),
    await_post_block(B, ExpectedResults, Peer).

send_new_block(Peer, B) ->
    ar_http_iface_client:send_block_binary(Peer, B#block.indep_hash,
            ar_serialize:block_to_binary(B)).

send_new_block_with_retry(Peer, B, ExpectedResults, RetriesLeft) ->
    Result = send_new_block(Peer, B),
    case should_retry_post_block_response(Result, ExpectedResults, RetriesLeft) of
        true ->
            timer:sleep(50),
            send_new_block_with_retry(Peer, B, ExpectedResults, RetriesLeft - 1);
        false ->
            Result
    end.

should_retry_post_block_response(_Result, _ExpectedResults, 0) ->
    false;
should_retry_post_block_response({error, {stream_error, closed}}, _ExpectedResults, _RetriesLeft) ->
    true;
should_retry_post_block_response({error, client_error}, [valid], _RetriesLeft) ->
    false;
should_retry_post_block_response({error, client_error}, _ExpectedResults, _RetriesLeft) ->
    true;
should_retry_post_block_response(_Result, _ExpectedResults, _RetriesLeft) ->
    false.

await_post_block(B, ExpectedResults) ->
    await_post_block(B, ExpectedResults, peer_ip(main)).

await_post_block(#block{ indep_hash = H } = B, ExpectedResults, Peer) ->
    PostGossipFailureCodes = [invalid_denomination,
            invalid_double_signing_proof_same_signature, invalid_double_signing_proof_cdiff,
            invalid_double_signing_proof_same_address,
            invalid_double_signing_proof_not_in_reward_history,
            invalid_double_signing_proof_already_banned,
            invalid_double_signing_proof_invalid_signature,
            mining_address_banned, invalid_account_anchors, invalid_reward_pool,
            invalid_miner_reward, invalid_debt_supply, invalid_reward_history_hash,
            invalid_kryder_plus_rate_multiplier_latch, invalid_kryder_plus_rate_multiplier,
            invalid_wallet_list],
    receive
        {event, block, {rejected, Reason, H, Peer2}} ->
            case lists:member(Reason, PostGossipFailureCodes) of
                true ->
                    ?assertEqual(no_peer, Peer2);
                false ->
                    ?assertEqual(Peer, Peer2)
            end,
            case lists:member(Reason, ExpectedResults) of
                true ->
                    ok;
                _ ->
                    ?assert(false, iolist_to_binary(io_lib:format("Unexpected "
                            "validation failure: ~p. Expected: ~p.",
                            [Reason, ExpectedResults])))
            end;
        {event, block, {new, #block{ indep_hash = H }, #{ source := {peer, Peer} }}} ->
            case ExpectedResults of
                [valid] ->
                    ok;
                _ ->
                    case lists:any(fun(FailureCode) -> not lists:member(FailureCode,
                            PostGossipFailureCodes) end, ExpectedResults) of
                        true ->
                            ?assert(false, iolist_to_binary(io_lib:format("Unexpected "
                                    "validation success. Expected: ~p.", [ExpectedResults])));
                        false ->
                            await_post_block(B, ExpectedResults)
                    end
            end
    after 60_000 ->
            ?assert(false, iolist_to_binary(io_lib:format("Timed out. Expected: ~p.",
                    [ExpectedResults])))
    end.

sign_block(#block{ cumulative_diff = CDiff } = B, PrevB, {Priv, Pub}) ->
    B2 = B#block{ reward_key = Pub, reward_addr = ar_wallet:to_address(Pub) },
    SignedH = ar_block:generate_signed_hash(B2),
    PrevCDiff = PrevB#block.cumulative_diff,
    SignaturePreimage = ar_block:get_block_signature_preimage(CDiff, PrevCDiff,
            << (B#block.previous_solution_hash)/binary, SignedH/binary >>,
            B#block.height),
    Signature = ar_wallet:sign(Priv, SignaturePreimage),
    H = ar_block:indep_hash2(SignedH, Signature),
    B2#block{ indep_hash = H, signature = Signature }.

get_chunk(Node, Offset) ->
    get_chunk(Node, Offset, undefined).

get_chunk(Node, Offset, Packing) ->
    Headers = case Packing of
        undefined -> [];
        _ ->
            PackingBinary = iolist_to_binary(ar_serialize:encode_packing(Packing, false)),
            [{<<"x-packing">>, PackingBinary}]
    end,
    ar_http:req(#{
        method => get,
        peer => peer_ip(Node),
        path => "/chunk/" ++ integer_to_list(Offset),
        headers => [{<<"x-bucket-based-offset">>, <<"true">>} | Headers]
    }).

get_chunk_proof(Node, Offset) ->
    ar_http:req(#{
        method => get,
        peer => peer_ip(Node),
        path => "/chunk_proof/" ++ integer_to_list(Offset),
        headers => [{<<"x-bucket-based-offset">>, <<"true">>}]
    }).

post_chunk(Node, Proof) ->
    Peer = peer_ip(Node),
    ar_http:req(#{
        method => post,
        peer => Peer,
        path => "/chunk",
        body => Proof
    }).

get_unconfirmed_chunk(Node, EncodedTXID, RelativeEndOffset) ->
    ar_http:req(#{
        method => get,
        peer => peer_ip(Node),
        path => "/unconfirmed_chunk/" ++ binary_to_list(EncodedTXID)
                ++ "/" ++ integer_to_list(RelativeEndOffset)
    }).

random_v1_data(Size) ->
    %% Make sure v1 txs do not end with a digit, otherwise they are malleable.
    << (crypto:strong_rand_bytes(Size - 1))/binary, <<"a">>/binary >>.

assert_get_tx_data(Node, TXID, ExpectedData) ->
    ?debugFmt("Polling for data of ~s.", [arweave_util:encode(TXID)]),
    Peer = peer_ip(Node),
    ok = ar_test_await:http_tx_data_matches(Node, TXID, ExpectedData),
    {ok, {{<<"200">>, _}, _, OffsetJSON, _, _}}
            = ar_http:req(#{ method => get, peer => Peer,
                    path => "/tx/" ++ binary_to_list(arweave_util:encode(TXID)) ++ "/offset" }),
    Map = jiffy:decode(OffsetJSON, [return_maps]),
    Offset = binary_to_integer(maps:get(<<"offset">>, Map)),
    Size = binary_to_integer(maps:get(<<"size">>, Map)),
    ?assertEqual(ExpectedData, get_tx_data_in_chunks(Offset, Size, Peer)),
    ?assertEqual(ExpectedData, get_tx_data_in_chunks_traverse_forward(Offset, Size, Peer)).

get_tx_data_in_chunks(Offset, Size, Peer) ->
    get_tx_data_in_chunks(Offset, Offset - Size, Peer, []).

get_tx_data_in_chunks(Offset, Start, _Peer, Bin) when Offset =< Start ->
    arweave_util:encode(iolist_to_binary(Bin));
get_tx_data_in_chunks(Offset, Start, Peer, Bin) ->
    JSON = get_tx_data_chunk(Peer, Offset),
    Map = jiffy:decode(JSON, [return_maps]),
    Chunk = arweave_util:decode(maps:get(<<"chunk">>, Map)),
    get_tx_data_in_chunks(Offset - byte_size(Chunk), Start, Peer, [Chunk | Bin]).

get_tx_data_in_chunks_traverse_forward(Offset, Size, Peer) ->
    get_tx_data_in_chunks_traverse_forward(Offset, Offset - Size, Peer, []).

get_tx_data_in_chunks_traverse_forward(Offset, Start, _Peer, Bin) when Offset =< Start ->
    arweave_util:encode(iolist_to_binary(lists:reverse(Bin)));
get_tx_data_in_chunks_traverse_forward(Offset, Start, Peer, Bin) ->
    JSON = get_tx_data_chunk(Peer, Start + 1),
    Map = jiffy:decode(JSON, [return_maps]),
    Chunk = arweave_util:decode(maps:get(<<"chunk">>, Map)),
    get_tx_data_in_chunks_traverse_forward(Offset, Start + byte_size(Chunk), Peer,
            [Chunk | Bin]).

get_tx_data_chunk(Peer, Offset) ->
    Path = "/chunk/" ++ integer_to_list(Offset),
    Deadline = erlang:monotonic_time(millisecond) + ?GET_TX_DATA_TIMEOUT,
    get_tx_data_chunk(Peer, Path, Offset, Deadline).

get_tx_data_chunk(Peer, Path, Offset, Deadline) ->
    case ar_http:req(#{ method => get, peer => Peer, path => Path }) of
        {ok, {{<<"200">>, _}, _, JSON, _, _}} ->
            JSON;
        Response ->
            case {is_retryable_tx_data_chunk_response(Response),
                    erlang:monotonic_time(millisecond) < Deadline} of
                {true, true} ->
                    timer:sleep(200),
                    get_tx_data_chunk(Peer, Path, Offset, Deadline);
                _ ->
                    ?debugFmt("Failed to fetch TX data chunk. Offset: ~B. Peer: ~s. "
                            "Response: ~p.~n",
                            [Offset, arweave_util:format_peer(Peer), Response]),
                    ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, Response)
            end
    end.

is_retryable_tx_data_chunk_response({error, client_error}) ->
    true;
is_retryable_tx_data_chunk_response({ok, {{<<"404">>, _}, _, _, _, _}}) ->
    true;
is_retryable_tx_data_chunk_response(_) ->
    false.

get_node_namespace() ->
    % Return the namespace part (everything after first - and before @)
    {_, Namespace} = split_node_name(),
    Namespace.

%% @doc Absolute path to the project root, used to anchor peer-boot shell
%% commands. Reads `ARWEAVE_PROJECT_ROOT' (set by `bin/e2e', since CT runs
%% the BEAM from its per-run log dir), falling back to CWD when unset.
project_root() ->
    case os:getenv("ARWEAVE_PROJECT_ROOT") of
        false ->
            {ok, Cwd} = file:get_cwd(),
            Cwd;
        Root ->
            Root
    end.

get_node() ->
    % Return the name part (everything before first -)
    {Name, _} = split_node_name(),
    Name.

split_node_name() ->
    % First split by '@' to separate host part
    [NamePart, _Host] = string:split(atom_to_list(node()), "@"),
    % Then split by first '-' to get name and namespace
    case string:split(NamePart, "-", leading) of
        [Name, Namespace] -> {Name, Namespace};
        [Name] -> {Name, ""}  % Handle case where there is no '-'
    end.

get_unused_port() ->
  {ok, ListenSocket} = gen_tcp:listen(0, [{port, 0}]),
  {ok, Port} = inet:port(ListenSocket),
  gen_tcp:close(ListenSocket),
  Port.

p2p_headers(Node) ->
    [
        {<<"x-p2p-port">>, integer_to_binary(peer_port(Node))},
        {<<"x-release">>, integer_to_binary(?RELEASE_NUMBER)}
    ].

%% @doc: get the genesis chunk between a given start and end offset.
-spec get_genesis_chunk(integer()) -> binary().
-spec get_genesis_chunk(integer(), integer()) -> binary().
get_genesis_chunk(EndOffset) ->
    StartOffset = case EndOffset rem ?DATA_CHUNK_SIZE of
        0 ->
            EndOffset - ?DATA_CHUNK_SIZE;
        _ ->
            (EndOffset div ?DATA_CHUNK_SIZE) * ?DATA_CHUNK_SIZE
    end,
    get_genesis_chunk(StartOffset, EndOffset).

get_genesis_chunk(StartOffset, EndOffset) ->
    Size = EndOffset - StartOffset,
    StartValue = StartOffset div 4,
    ar_weave:generate_data(StartValue, Size, <<>>).
