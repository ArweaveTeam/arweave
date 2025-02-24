-module(ar_test_node).

%% The new, more flexible, and more user-friendly interface.
-export([boot_peers/1, wait_for_peers/1, get_config/1,set_config/2,
		wait_until_joined/0, wait_until_joined/1, 
		restart/0, restart/1, restart_with_config/1, restart_with_config/2,
		start_other_node/4, start_node/2, start_node/3, start_coordinated/1, base_cm_config/1, mine/1,
		wait_until_height/1, wait_until_height/2, wait_until_height/3, assert_wait_until_height/2, http_get_block/2, get_blocks/1,
		mock_to_force_invalid_h1/0, get_difficulty_for_invalid_hash/0, invalid_solution/0,
		valid_solution/0, new_mock/2, mock_function/3, unmock_module/1, remote_call/4,
		load_fixture/1,
		get_default_storage_module_packing/2, get_genesis_chunk/1,
		all_nodes/1, new_custom_size_rsa_wallet/1]).

%% The "legacy" interface.
-export([start/0, start/1, start/2, start/3, start/4,
		stop/0, stop/1, start_peer/2, start_peer/3, start_peer/4, peer_name/1, peer_port/1,
		stop_peers/1, stop_peer/1, connect_to_peer/1, disconnect_from/1,
		join/2, join_on/1, rejoin_on/1,
		peer_ip/1, get_node_namespace/0, get_unused_port/0,

		mine/0, get_tx_anchor/1, get_tx_confirmations/2, get_tx_price/2, get_tx_price/3,
		get_optimistic_tx_price/2, get_optimistic_tx_price/3,
		sign_tx/1, sign_tx/2, sign_tx/3, sign_v1_tx/1, sign_v1_tx/2, sign_v1_tx/3,

		wait_until_block_index/1, wait_until_block_index/2,
		wait_until_receives_txs/1,
		assert_wait_until_receives_txs/1, assert_wait_until_receives_txs/2,
		post_tx_to_peer/2, post_tx_to_peer/3, assert_post_tx_to_peer/2, assert_post_tx_to_peer/3,
		post_and_mine/2, post_block/2, post_block/3, send_new_block/2,
		await_post_block/2, await_post_block/3, sign_block/3, read_block_when_stored/1,
		read_block_when_stored/2, get_chunk/2, get_chunk/3, get_chunk_proof/2, post_chunk/2,
		random_v1_data/1, assert_get_tx_data/3,
		assert_data_not_found/2, post_tx_json/2,
		wait_until_syncs_genesis_data/0, wait_until_syncs_genesis_data/1,

		mock_functions/1, test_with_mocked_functions/2, test_with_mocked_functions/3]).

-include("../include/ar.hrl").
-include("../include/ar_config.hrl").
-include("../include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%% May occasionally take quite long on a slow CI server, expecially in tests
%% with height >= 20 (2 difficulty retargets).
-define(WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT, 500_000).
-define(WAIT_UNTIL_RECEIVES_TXS_TIMEOUT, 500_000).

%% Sometimes takes a while on a slow machine
-define(PEER_START_TIMEOUT, 500_000).
%% Set the maximum number of retry attempts
-define(MAX_BOOT_RETRIES, 3).

-define(MAX_MINERS, 3).

% define check timeout and interval, used with ar_util:do_until/3.
-define(NODE_READY_CHECK_INTERVAL, 200).
-define(NODE_READY_CHECK_TIMEOUT, 500_000).
-define(REMOTE_CALL_TIMEOUT, 500_000).
-define(CONNECT_TO_PEER_TIMEOUT, 500_000).
-define(BLOCK_INDEX_TIMEOUT, 500_000).
-define(TEST_MOCKED_FUNCTIONS_TIMEOUT, 500). %% in seconds
-define(POST_AND_MINE_TIMEOUT, 500_000).
-define(READ_BLOCK_TIMEOUT, 500_000).
-define(GET_TX_DATA_TIMEOUT, 200_000).
-define(WAIT_UNTIL_JOINED_TIMEOUT, 200_000).
-define(WAIT_SYNCS_DATA_TIMEOUT, 200_000).

%%%===================================================================
%%% Public interface.
%%%===================================================================
all_peers(test) ->
	[{test, peer1}, {test, peer2}, {test, peer3}, {test, peer4}];
all_peers(e2e) ->
	[{e2e, peer1}, {e2e, peer2}].

all_nodes(TestType) ->
	[{TestType, main} | all_peers(TestType)].

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
					{e, ar_util:encode(Expnt)},
					{n, ar_util:encode(Pub)},
					{d, ar_util:encode(Priv)},
					{p, ar_util:encode(P1)},
					{q, ar_util:encode(P2)},
					{dp, ar_util:encode(E1)},
					{dq, ar_util:encode(E2)},
					{qi, ar_util:encode(C)}
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
    %% You might log an error or handle this case specifically as per your application logic.
    {error, max_retries_exceeded};
try_boot_peer(TestType, Node, Retries) ->
    NodeName = peer_name(Node),
    Port = get_unused_port(),
    Cookie = erlang:get_cookie(),

	Paths = code:get_path(),

    filelib:ensure_dir("./.tmp"),
	Schedulers = erlang:system_info(schedulers_online),
    Cmd = io_lib:format(
        "erl +S ~B:~B -pa ~s -config config/sys.config -noshell " ++
		"-name ~s -setcookie ~s -run ar main debug port ~p " ++
        "data_dir .tmp/data_~s_~s no_auto_join " ++
		"> ~s-~s.out 2>&1 &",
        [Schedulers, Schedulers, string:join(Paths, " "), NodeName, Cookie, Port,
			atom_to_list(TestType), NodeName, Node, get_node_namespace()]),
	io:format("Launching peer ~p: ~s~n", [Node, Cmd]),
    os:cmd(Cmd),
    case wait_until_node_is_ready(NodeName) of
        {ok, _Node} ->
            io:format("~s started at port ~p.~n", [NodeName, Port]),
            {node(), NodeName};
        {error, Reason} ->
            io:format("Error starting ~s: ~p. Retries left: ~p~n", [NodeName, Reason, Retries]),
            try_boot_peer(TestType, Node, Retries - 1)
    end.

wait_for_peers([]) ->
	ok;
wait_for_peers([{_TestType, Node} | Peers]) ->
	wait_for_peer(Node),
	wait_for_peers(Peers);
wait_for_peers(TestType) ->
	wait_for_peers(all_peers(TestType)).

wait_for_peer(Node) ->
	remote_call(Node, application, ensure_all_started, [arweave, permanent], 60000).

self_node() ->
	list_to_atom(get_node()).

peer_name(Node) ->
	list_to_atom(
		atom_to_list(Node) ++ "-" ++ get_node_namespace() ++ "@127.0.0.1"
	).

peer_port(Node) ->
	{ok, Config} = ar_test_node:remote_call(Node, application, get_env, [arweave, config]),
	Config#config.port.

stop_peers([]) ->
	ok;
stop_peers([{_TestType, Node} | Peers]) ->
	stop_peer(Node),
	stop_peers(Peers);
stop_peers(TestType) ->
	stop_peers(all_peers(TestType)).

stop_peer(Node) ->
	try
		rpc:call(peer_name(Node), init, stop, [])
	catch
		_:_ ->
			%% we don't care if the node is already stopped
			ok
	end.

peer_ip({external, Peer}) ->
	Peer;
peer_ip(Node) ->
	{127, 0, 0, 1, peer_port(Node)}.

wait_until_joined(Node) ->
	remote_call(Node, ar_test_node, wait_until_joined, []).

%% @doc Wait until the node joins the network (initializes the state).
wait_until_joined() ->
	ar_util:do_until(
		fun() -> ar_node:is_joined() end,
		100,
		?WAIT_UNTIL_JOINED_TIMEOUT
	 ).

get_config(Node) ->
	remote_call(Node, application, get_env, [arweave, config]).

set_config(Node, Config) ->
	remote_call(Node, application, set_env, [arweave, config, Config]).

update_config(Config) ->
	{ok, BaseConfig} = application:get_env(arweave, config),
	Config2 = BaseConfig#config{
		start_from_latest_state = Config#config.start_from_latest_state,
		auto_join = Config#config.auto_join,
		mining_addr = Config#config.mining_addr,
		sync_jobs = Config#config.sync_jobs,
		replica_2_9_workers = Config#config.replica_2_9_workers,
		disk_pool_jobs = Config#config.disk_pool_jobs,
		header_sync_jobs = Config#config.header_sync_jobs,
		enable = Config#config.enable ++ BaseConfig#config.enable,
		mining_cache_size_mb = Config#config.mining_cache_size_mb,
		debug = Config#config.debug,
		coordinated_mining = Config#config.coordinated_mining,
		cm_api_secret = Config#config.cm_api_secret,
		cm_poll_interval = Config#config.cm_poll_interval,
		peers = Config#config.peers,
		cm_exit_peer = Config#config.cm_exit_peer,
		cm_peers = Config#config.cm_peers,
		local_peers = Config#config.local_peers,
		mine = Config#config.mine,
		storage_modules = Config#config.storage_modules,
		repack_in_place_storage_modules = Config#config.repack_in_place_storage_modules
	},
	ok = application:set_env(arweave, config, Config2),
	?LOG_INFO("Updated Config:"),
	ar_config:log_config(Config2),
	Config2.

start_other_node(Node, B0, Config, WaitUntilSync) ->
	remote_call(Node, ar_test_node, start_node, [B0, Config, WaitUntilSync], 90000).

%% @doc Start a node with the given genesis block and configuration.
start_node(B0, Config) ->
	start_node(B0, Config, true).
start_node(B0, Config, WaitUntilSync) ->
	clean_up_and_stop(),
	{ok, BaseConfig} = application:get_env(arweave, config),
	write_genesis_files(BaseConfig#config.data_dir, B0),
	update_config(Config),
	ar:start_dependencies(),
	wait_until_joined(),
	case WaitUntilSync of
		true ->
			wait_until_syncs_genesis_data();
		false ->
			ok
	end,
	erlang:node().

%% @doc Launch the given number (>= 1, =< ?MAX_MINERS) of the mining nodes in the coordinated
%% mode plus an exit node and a validator node.
%% Return [Node1, ..., NodeN, ExitNode, ValidatorNode].
start_coordinated(MiningNodeCount) when MiningNodeCount >= 1, MiningNodeCount =< ?MAX_MINERS ->
	%% Set weave larger than what we'll cover with the 3 nodes so that every node can find
	%% a solution.
	[B0] = ar_weave:init([], get_difficulty_for_invalid_hash(), ?PARTITION_SIZE * 5),
	ExitPeer = peer_ip(peer1),
	ValidatorPeer = peer_ip(main),
	MinerNodes = lists:sublist([peer2, peer3, peer4], MiningNodeCount),

	BaseCMConfig = base_cm_config([ValidatorPeer]),
	RewardAddr = BaseCMConfig#config.mining_addr,
	ExitNodeConfig = BaseCMConfig#config{
		mine = true,
		local_peers = [peer_ip(Peer) || Peer <- MinerNodes]
	},
	ValidatorNodeConfig = BaseCMConfig#config{
		mine = false,
		peers = [ExitPeer],
		coordinated_mining = false,
		cm_api_secret = not_set
	},

	remote_call(peer1, ar_test_node, start_node, [B0, ExitNodeConfig]), %% exit node
	remote_call(main, ar_test_node, start_node, [B0, ValidatorNodeConfig]), %% validator node

	lists:foreach(
		fun(I) ->
			MinerNode = lists:nth(I, MinerNodes),
			MinerPeers = lists:filter(fun(Peer) -> Peer /= MinerNode end, MinerNodes),
			MinerPeerIPs = [peer_ip(Peer) || Peer <- MinerPeers],

			MinerConfig = BaseCMConfig#config{
				cm_exit_peer = ExitPeer,
				cm_peers = MinerPeerIPs,
				local_peers = MinerPeerIPs ++ [ExitPeer],
				storage_modules = get_cm_storage_modules(RewardAddr, I, MiningNodeCount)
			},
			remote_call(MinerNode, ar_test_node, start_node, [B0, MinerConfig])
		end,
		lists:seq(1, MiningNodeCount)
	),

	MinerNodes ++ [peer1, main].

base_cm_config(Peers) ->
	RewardAddr = ar_wallet:to_address(remote_call(peer1, ar_wallet, new_keyfile, [])),
	#config{
		start_from_latest_state = true,
		auto_join = true,
		mining_addr = RewardAddr,
		sync_jobs = 2,
		disk_pool_jobs = 2,
		header_sync_jobs = 2,
		enable = [search_in_rocksdb_when_mining, serve_tx_data_without_limits,
				serve_wallet_lists, pack_served_chunks, public_vdf_server],
		debug = true,
		peers = Peers,
		coordinated_mining = true,
		cm_api_secret = <<"test_coordinated_mining_secret">>,
		cm_poll_interval = 2000
	}.

mine() ->
	gen_server:cast(ar_node_worker, mine).

%% @doc Start mining on the given node. The node will be mining until it finds a block.
mine(Node) ->
	remote_call(Node, ar_test_node, mine, []).

%% @doc Fetch and decode a binary-encoded block by hash H from the HTTP API of the
%% given node. Return {ok, B} | {error, Reason}.
http_get_block(H, Node) ->
	{ok, Config} = remote_call(Node, application, get_env, [arweave, config]),
	Port = Config#config.port,
	Peer = {127, 0, 0, 1, Port},
	case ar_http:req(#{ peer => Peer, method => get,
			path => "/block2/hash/" ++ binary_to_list(ar_util:encode(H)) }) of
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
		fun(_H0, _Nonce, _Chunk1) ->
			{invalid_solution(), invalid_solution()}
		end
	}.

get_difficulty_for_invalid_hash() ->
	%% Set the difficulty just high enough to exclude the invalid_solution(), this lets
	%% us selectively disable one- or two-chunk mining in tests.
	binary:decode_unsigned(invalid_solution(), big) + 1.

load_fixture(Fixture) ->
	Dir = filename:dirname(?FILE),
	FixtureFilename = filename:join([Dir, "fixtures", Fixture]),
	{ok, Data} = file:read_file(FixtureFilename),
	Data.

%%%===================================================================
%%% Private functions.
%%%===================================================================

clean_up_and_stop() ->
	Config = stop(),
	ok = filelib:ensure_dir(Config#config.data_dir),
	{ok, Entries} = file:list_dir_all(Config#config.data_dir),
	lists:foreach(
		fun	("wallets") ->
				ok;
			(Entry) ->
				ok = file:del_dir_r(filename:join(Config#config.data_dir, Entry))
		end,
		Entries
	).

write_genesis_files(DataDir, B0) ->
	BH = B0#block.indep_hash,
	BlockDir = filename:join(DataDir, ?BLOCK_DIR),
	ok = filelib:ensure_dir(BlockDir ++ "/"),
	BlockFilepath = filename:join(BlockDir, binary_to_list(ar_util:encode(BH)) ++ ".bin"),
	ok = file:write_file(BlockFilepath, ar_serialize:block_to_binary(B0)),
	TXDir = filename:join(DataDir, ?TX_DIR),
	ok = filelib:ensure_dir(TXDir ++ "/"),
	lists:foreach(
		fun(TX) ->
			TXID = TX#tx.id,
			TXFilepath = filename:join(TXDir, binary_to_list(ar_util:encode(TXID)) ++ ".json"),
			TXJSON = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
			ok = file:write_file(TXFilepath, TXJSON)
		end,
		B0#block.txs
	),
	_ = ar_kv:create_ets(),
	{ok, _} = ar_kv:start_link(),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "reward_history_db"), reward_history_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "block_time_history_db"),
			block_time_history_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "block_index_db"), block_index_db),
	H = B0#block.indep_hash,
	WeaveSize = B0#block.weave_size,
	TXRoot = B0#block.tx_root,
	ok = ar_kv:put(block_index_db, << 0:256 >>, term_to_binary({H, WeaveSize, TXRoot, <<>>})),
	ok = ar_kv:put(reward_history_db, H, term_to_binary(hd(B0#block.reward_history))),
	case ar_fork:height_2_7() of
		0 ->
			ok = ar_kv:put(block_time_history_db, H,
					term_to_binary(hd(B0#block.block_time_history)));
		_ ->
			ok
	end,
	ok = gen_server:stop(ar_kv),
	_ = ets:delete(ar_kv),
	WalletListDir = filename:join(DataDir, ?WALLET_LIST_DIR),
	ok = filelib:ensure_dir(WalletListDir ++ "/"),
	RootHash = B0#block.wallet_list,
	WalletListFilepath =
		filename:join(WalletListDir, binary_to_list(ar_util:encode(RootHash)) ++ ".json"),
	WalletListJSON =
		ar_serialize:jsonify(
			ar_serialize:wallet_list_to_json_struct(B0#block.reward_addr, false,
					B0#block.account_tree)
		),
	ok = file:write_file(WalletListFilepath, WalletListJSON).

wait_until_syncs_data(Left, Right, WeaveSize, _Packing)
  		when Left >= Right orelse
			Left >= WeaveSize orelse
			(Right - Left < ?DATA_CHUNK_SIZE) orelse
			(WeaveSize - Left < ?DATA_CHUNK_SIZE) ->
	ok;
wait_until_syncs_data(Left, Right, WeaveSize, Packing) ->
	true = ar_util:do_until(
		fun() ->
			case Packing of
				any ->
					case ar_sync_record:is_recorded(Left + 1, ar_data_sync) of
						false ->
							false;
						_ ->
							true
					end;
				_ ->
					case ar_sync_record:is_recorded(Left + 1, {ar_data_sync, Packing}) of
						{{true, _}, _} ->
							true;
						_ ->
							false
					end
			end
		end,
		1000,
		?WAIT_SYNCS_DATA_TIMEOUT
	),
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
	[{?PARTITION_SIZE, RangeNumber, get_default_storage_module_packing(RewardAddr, 0)}].

remote_call(Node, Module, Function, Args) ->
	remote_call(Node, Module, Function, Args, ?REMOTE_CALL_TIMEOUT).

remote_call(Node, Module, Function, Args, Timeout) ->
	NodeName = peer_name(Node),
	case node() == NodeName of
		true ->
			apply(Module, Function, Args);
		false ->
			Key = rpc:async_call(NodeName, Module, Function, Args),
			Result = ar_util:do_until(
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
				ar_wallet:to_address(ar_wallet:new_keyfile());
			Addr ->
				Addr
		end,
	Config =
		case maps:get(config, Options, not_set) of
			not_set ->
				element(2, application:get_env(arweave, config));
			Value2 ->
				Value2
		end,
	StorageModules =
		case maps:get(storage_modules, Options, not_set) of
			not_set ->
				[{20 * 1024 * 1024, N,
						get_default_storage_module_packing(RewardAddr, N, Options)}
					|| N <- lists:seq(0, 8)];
			Value3 ->
				Value3
		end,
	start(B0, RewardAddr, Config, StorageModules);
start(B0) ->
	start(#{ b0 => B0 }).
start(B0, RewardAddr) ->
	start(#{ b0 => B0, addr => RewardAddr }).

%% @doc Start a fresh node with the given genesis block, mining address, and config.
start(B0, RewardAddr, Config) ->
	StorageModules = [{20 * 1024 * 1024, N, get_default_storage_module_packing(RewardAddr, N)}
			|| N <- lists:seq(0, 8)],
	start(B0, RewardAddr, Config, StorageModules).

%% @doc Start a fresh node with the given genesis block, mining address, config,
%% and storage modules.
%%
%% Note: the Config provided here is written to disk. This is fine if it's the default Config,
%% but if you've modified any of the Config fields for your test, please restore the default
%% Config after the test is done. Otherwise the tests that run after yours may fail.
start(B0, RewardAddr, Config, StorageModules) ->
	clean_up_and_stop(),
	write_genesis_files(Config#config.data_dir, B0),
	ok = application:set_env(arweave, config, Config#config{
		start_from_latest_state = true,
		auto_join = true,
		peers = [],
		cm_exit_peer = not_set,
		cm_peers = [],
		local_peers = [],
		mining_addr = RewardAddr,
		storage_modules = StorageModules,
		disk_space_check_frequency = 1000,
		sync_jobs = 2,
		disk_pool_jobs = 2,
		header_sync_jobs = 2,
		enable = [search_in_rocksdb_when_mining, serve_tx_data_without_limits,
				double_check_nonce_limiter, serve_wallet_lists | Config#config.enable],
		debug = true
	}),
	ar:start_dependencies(),
	wait_until_joined(),
	wait_until_syncs_genesis_data().

restart() ->
	?LOG_INFO("Restarting node"),
	stop(),
	ar:start_dependencies(),
	wait_until_joined().

restart_with_config(Config) ->
	?LOG_INFO("Restarting node with new config"),
	stop(),

	update_config(Config),

	ar:start_dependencies(),
	wait_until_joined().

restart(Node) ->
	remote_call(Node, ?MODULE, restart, [], 90000).

restart_with_config(Node, Config) ->
	remote_call(Node, ?MODULE, restart_with_config, [Config], 90000).

start_peer(Node, Args) when is_map(Args) ->
	remote_call(Node, ?MODULE, start, [Args], ?PEER_START_TIMEOUT),
	wait_until_joined(Node),
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
			++ binary_to_list(ar_util:encode(Target)),
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path
		}),
	Fee = binary_to_integer(Reply),
	Path2 = "/price2/" ++ integer_to_list(DataSize) ++ "/"
			++ binary_to_list(ar_util:encode(Target)),
	{ok, {{<<"200">>, _}, _, Reply2, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path2
		}),
	Map = jiffy:decode(Reply2, [return_maps]),
	case binary_to_integer(maps:get(<<"fee">>, Map)) of
		Fee ->
			{Fee, maps:get(<<"denomination">>, Map)};
		Fee2 ->
			?assert(false, io_lib:format("Fee mismatch, expected: ~B, got: ~B.", [Fee, Fee2]))
	end.

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]) from the given node.
get_optimistic_tx_price(Node, DataSize) ->
	get_optimistic_tx_price(Node, DataSize, <<>>).

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]/[addr]) from the given
%% node.
get_optimistic_tx_price(Node, DataSize, Target) ->
	Path = "/optimistic_price/" ++ integer_to_list(DataSize) ++ "/"
			++ binary_to_list(ar_util:encode(Target)),
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
%% If the fee is not in Args, fetch it from GET /price/{data_size}
%% or GET /price/{data_size}/{target} (if the target is specified) on the peer1 node.
sign_tx(Wallet, Args) ->
	sign_tx(peer1, Wallet, insert_root(Args#{ format => 2 }), fun ar_tx:sign/2).

%% @doc Like sign_tx/2, but use the given Node to fetch the fee estimation and
%% block anchor from.
sign_tx(Node, Wallet, Args) ->
	sign_tx(Node, Wallet, insert_root(Args#{ format => 2 }), fun ar_tx:sign/2).

%% @doc Like sign_tx/1 but return a format=1 transaction.
sign_v1_tx(Wallet) ->
	sign_tx(peer1, Wallet, #{}, fun ar_tx:sign_v1/2).

%% @doc Like sign_tx/2 but return a format=1 transaction.
sign_v1_tx(Wallet, TXParams) ->
	sign_tx(peer1, Wallet, TXParams, fun ar_tx:sign_v1/2).

%% @doc Like sign_tx/3 but return a format=1 transaction.
sign_v1_tx(Node, Wallet, Args) ->
	sign_tx(Node, Wallet, Args, fun ar_tx:sign_v1/2).

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
	Format = maps:get(format, Args, 1),
	{Fee, Denomination} = get_tx_price(Node, DataSize, maps:get(target, Args, <<>>)),
	Fee2 =
		case {Format, maps:get(reward, Args, none)} of
			{1, none} ->
				%% Make sure the v1 tx is not malleable by assigning a fee with only
				%% the first digit being non-zero.
				FirstDigit = binary_to_integer(binary:part(integer_to_binary(Fee), {0, 1})),
				Len = length(integer_to_list(Fee)),
				Fee3 = trunc((FirstDigit + 1) * math:pow(10, Len - 1)),
				Fee3;
			{_, none} ->
				Fee;
			{_, AssignedFee} ->
				AssignedFee
		end,
	SignFun(
		(ar_tx:new())#tx{
			owner = Pub,
			reward = Fee2,
			data = Data,
			target = maps:get(target, Args, <<>>),
			quantity = maps:get(quantity, Args, 0),
			tags = maps:get(tags, Args, []),
			last_tx = maps:get(last_tx, Args, get_tx_anchor(Node)),
			data_size = DataSize,
			data_root = maps:get(data_root, Args, <<>>),
			format = Format,
			denomination = maps:get(denomination, Args, Denomination)
		},
		Wallet
	).

stop() ->
	{ok, Config} = application:get_env(arweave, config),
	application:stop(arweave),
	ar:stop_dependencies(),
	Config.

stop(Node) ->
	remote_call(Node, ar_test_node, stop, []).

rejoin_on(#{ node := Node, join_on := JoinOnNode }) ->
	join_on(#{ node => Node, join_on => JoinOnNode }, true).

join_on(#{ node := Node, join_on := JoinOnNode }) ->
	join_on(#{ node => Node, join_on => JoinOnNode }, false).

join_on(#{ node := Node, join_on := JoinOnNode }, Rejoin) ->
	remote_call(Node, ar_test_node, join, [JoinOnNode, Rejoin], ?REMOTE_CALL_TIMEOUT).

join(JoinOnNode, Rejoin) ->
	Peer = peer_ip(JoinOnNode),
	{ok, Config} = application:get_env(arweave, config),
	case Rejoin of
		true ->
			stop();
		false ->
			clean_up_and_stop()
	end,
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{?PARTITION_SIZE, N,
			get_default_storage_module_packing(RewardAddr, N)} || N <- lists:seq(0, 4)],
	ok = application:set_env(arweave, config, Config#config{
		start_from_latest_state = false,
		mining_addr = RewardAddr,
		storage_modules = StorageModules,
		auto_join = true,
		peers = [Peer]
	}),
	ar:start_dependencies(),
	whereis(ar_node_worker).

get_default_storage_module_packing(RewardAddr, Index) ->
	get_default_storage_module_packing(RewardAddr, Index, #{}).

get_default_storage_module_packing(RewardAddr, Index, Options) ->
	case {ar_fork:height_2_9(), ar_fork:height_2_8()} of
		{infinity, infinity} ->
			{spora_2_6, RewardAddr};
		{infinity, 0} ->
			{composite, RewardAddr, 1};
		{0, 0} ->
			case maps:get(packing, Options, not_set) of
				spora_2_6 ->
					{spora_2_6, RewardAddr};
				{composite, PackingDiff} ->
					{composite, RewardAddr, PackingDiff};
				replica_2_9 ->
					{replica_2_9, RewardAddr};
				not_set ->
					{replica_2_9, RewardAddr}
			end;
		_ ->
			case maps:get(packing, Options, not_set) of
				spora_2_6 ->
					{spora_2_6, RewardAddr};
				{composite, PackingDiff} ->
					{composite, RewardAddr, PackingDiff};
				replica_2_9 ->
					{replica_2_9, RewardAddr};
				not_set ->
					case Index rem 3 of
						0 ->
							{spora_2_6, RewardAddr};
						1 ->
							{composite, RewardAddr, 1};
						_ ->
							{replica_2_9, RewardAddr}
					end
			end
	end.

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
	true = ar_util:do_until(
		fun() ->
			Peers = remote_call(Node, ar_peers, get_peers, [lifetime]),
			lists:member(peer_ip(Self), Peers)
		end,
		100,
		?CONNECT_TO_PEER_TIMEOUT
	),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => peer_ip(Self),
			path => "/info",
			headers => p2p_headers(Node)
		}),
	ar_util:do_until(
		fun() ->
			lists:member(Peer, ar_peers:get_peers(lifetime))
		end,
		100,
	        ?CONNECT_TO_PEER_TIMEOUT
	).

disconnect_from(Node) ->
	ar_http:block_peer_connections(),
	remote_call(Node, ar_http, block_peer_connections, []).

wait_until_syncs_genesis_data(Node) ->
	ok = remote_call(Node, ar_test_node, wait_until_syncs_genesis_data, [], 100_000).

wait_until_syncs_genesis_data() ->
	{ok, Config} = application:get_env(arweave, config),
	B = ar_node:get_current_block(),
	WeaveSize = B#block.weave_size,
	?LOG_INFO([{event, wait_until_syncs_genesis_data}, {status, initial_sync_started},
		{weave_size, WeaveSize}]),
	[wait_until_syncs_data(N * Size, (N + 1) * Size, WeaveSize, any)
			|| {Size, N, _Packing} <- Config#config.storage_modules],
	?LOG_INFO([{event, wait_until_syncs_genesis_data}, {status, initial_sync_complete}]),
	%% Once the data is stored in the disk pool, make the storage modules
	%% copy the missing data over from each other. This procedure is executed on startup
	%% but the disk pool did not have any data at the time.
	[gen_server:cast(list_to_atom("ar_data_sync_" ++ ar_storage_module:label(Module)),
			sync_data) || Module <- Config#config.storage_modules],
	[wait_until_syncs_data(N * Size, (N + 1) * Size, WeaveSize, Packing)
			|| {Size, N, Packing} <- Config#config.storage_modules],
	?LOG_INFO([{event, wait_until_syncs_genesis_data}, {status, cross_module_sync_complete}]),
	ok.

wait_until_height(Node, TargetHeight) ->
	wait_until_height(Node, TargetHeight, true).

wait_until_height(Node, TargetHeight, Strict) ->
	{BI, Height} = case Node of 
		main ->
			{
				wait_until_height(TargetHeight),
				ar_node:get_height()
			};
		_ -> 
			{
				remote_call(Node, ?MODULE, wait_until_height, [TargetHeight],
					?WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT + 500),
				remote_call(Node, ar_node, get_height, [])
			}
	end,
	case Strict of
		true ->
			?assertEqual(TargetHeight, Height, 
				iolist_to_binary(io_lib:format("Node ~p not at the expected height", [Node])));
		false ->
			ok
	end,
	BI.

wait_until_height(TargetHeight) ->
	{ok, BI} = ar_util:do_until(
		fun() ->
			case ar_node:get_blocks() of
				BI when length(BI) - 1 >= TargetHeight ->
					{ok, BI};
				_ ->
					false
			end
		end,
		100,
		?WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT
	),
	BI.

assert_wait_until_height(Node, TargetHeight) ->
	BI = wait_until_height(Node, TargetHeight),
	?assert(is_list(BI), iolist_to_binary(io_lib:format("Got ~p.", [BI]))),
	BI.

wait_until_block_index(Node, BI) ->
	remote_call(Node, ?MODULE, wait_until_block_index, [BI]).

wait_until_block_index(BI) ->
	ar_util:do_until(
		fun() ->
			case ar_node:get_blocks() of
				BI ->
					ok;
				_ ->
					false
			end
		end,
		100,
		?BLOCK_INDEX_TIMEOUT
	).

%% Safely perform an rpc:call/4 and return results in a tagged tuple.
safe_remote_call(Node, Module, Function, Args) ->
    try rpc:call(Node, Module, Function, Args) of
        Result -> {ok, Result}
    catch
        error:Reason ->
            %% Log the error if necessary
            io:format("Remote call error: ~p~n", [Reason]),
            {error, Reason};
        _:_ ->
            %% Catching other exceptions, returning a general error.
            {error, unknown}
    end.

wait_until_node_is_ready(NodeName) ->
    ar_util:do_until(
        fun() ->
            case net_adm:ping(NodeName) of
                pong ->
                    %% The node is reachable, doing a second check.
		    % safe_remote_call(NodeName, erlang, is_alive, []);
		    RemoteApps =
			case safe_remote_call(NodeName, application, which_applications, []) of
			    {ok, R} when is_list(R) -> R;
			    _ -> []
			end,
		    case lists:keyfind(arweave, 1, RemoteApps) of
			{arweave, _, _} -> {ok, ready};
			_ -> false
		    end;
                pang ->
                    %% Node is not reachable.
                    false
            end
        end,
        ?NODE_READY_CHECK_INTERVAL,
        ?NODE_READY_CHECK_TIMEOUT
    ).

assert_wait_until_receives_txs(TXs) ->
	?assertEqual(ok, wait_until_receives_txs(TXs)).

assert_wait_until_receives_txs(Node, TXs) ->
	?assertEqual(ok, wait_until_receives_txs(Node, TXs)).

wait_until_receives_txs(Node, TXs) ->
	remote_call(Node, ?MODULE, wait_until_receives_txs, [TXs],
					?WAIT_UNTIL_RECEIVES_TXS_TIMEOUT + 500).

wait_until_receives_txs(TXs) ->
	ar_util:do_until(
		fun() ->
			MinedTXIDs = ar_node:get_ready_for_mining_txs(),
			case lists:all(fun(TX) -> lists:member(TX#tx.id, MinedTXIDs) end, TXs) of
				true ->
					ok;
				_ ->
					false
			end
		end,
		100,
		?WAIT_UNTIL_RECEIVES_TXS_TIMEOUT
	).

assert_post_tx_to_peer(Node, TX) ->
	assert_post_tx_to_peer(Node, TX, true).

assert_post_tx_to_peer(Node, TX, Wait) ->
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_peer(Node, TX, Wait).

post_tx_to_peer(Node, TX) ->
	post_tx_to_peer(Node, TX, true).

post_tx_to_peer(Node, TX, Wait) ->
	Reply = post_tx_json(Node, ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
			case Wait of
				true ->
					assert_wait_until_receives_txs(Node, [TX]);
				false ->
					ok
			end;
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
						ar_wallet:to_address(
							ar_wallet:recover_key(DataSegment, TX#tx.signature, TX#tx.signature_type),
							TX#tx.signature_type);
					_ ->
						ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type)
			end,
			?debugFmt(
				"Failed to post transaction.~nTX: ~s.~nTX format: ~B.~nTX fee: ~B.~n"
				"TX size: ~B.~nTX last_tx: ~s.~nTX owner: ~s.~nTX owner address: ~s.~n"
				"Error(s): ~p.~nReply: ~p.~n",
				[ar_util:encode(TX#tx.id), TX#tx.format, TX#tx.reward,
					TX#tx.data_size, ar_util:encode(TX#tx.last_tx),
					ar_util:encode(TX#tx.owner),
					ar_util:encode(Addr),
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
	ar_util:decode(Reply).

get_tx_confirmations(Node, TXID) ->
	Response =
		ar_http:req(#{
			method => get,
			peer => peer_ip(Node),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/status"
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Reply, _, _}} ->
			{Status} = ar_serialize:dejsonify(Reply),
			lists:keyfind(<<"number_of_confirmations">>, 1, Status);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			-1
	end.

new_mock(Module, Options) ->
	try
		meck:new(Module, Options)
	catch
		error:E ->
			?LOG_ERROR("Error creating mock for ~p: ~p", [Module, E])
	end.

mock_function(Module, Fun, Mock) ->
	try
		meck:expect(Module, Fun, Mock)
	catch
		error:E ->
			?LOG_ERROR("Error setting mock for ~p: ~p", [Module, E])
	end.

unmock_module(Module) ->
	try
		meck:unload(Module)
	catch
		error:E ->
			?LOG_ERROR("Error unloading mock for ~p: ~p", [Module, E])
	end.

mock_functions(Functions) ->
	{
		fun() ->
			lists:foldl(
				fun({Module, Fun, Mock}, Mocked) ->
					NewMocked = case maps:get(Module, Mocked, false) of
						false ->
							new_mock(Module, [passthrough]),
							lists:foreach(
								fun({_TestType, Node}) ->
									remote_call(Node, ar_test_node, new_mock,
											[Module, [no_link, passthrough]])
								end,
								all_peers(test)),
							maps:put(Module, true, Mocked);
						true ->
							Mocked
					end,
					mock_function(Module, Fun, Mock),
					lists:foreach(
						fun({_TestType, Node}) ->
							remote_call(Node, ar_test_node, mock_function,
									[Module, Fun, Mock])
						end,
						all_peers(test)),
					NewMocked
				end,
				maps:new(),
				Functions
			)
		end,
		fun(Mocked) ->
			maps:fold(
				fun(Module, _, _) ->
					unmock_module(Module),
					lists:foreach(
						fun({_Build, Node}) ->
							remote_call(Node, ar_test_node, unmock_module, [Module])
						end,
						all_peers(test))
				end,
				noop,
				Mocked
			)
		end
	}.

test_with_mocked_functions(Functions, TestFun) ->
	test_with_mocked_functions(Functions, TestFun, ?TEST_MOCKED_FUNCTIONS_TIMEOUT).

test_with_mocked_functions(Functions, TestFun, Timeout) ->
	{Setup, Cleanup} = mock_functions(Functions),
	{
		foreach,
		Setup, Cleanup,
		[{timeout, Timeout, TestFun}]
	}.

post_and_mine(#{ miner := Node, await_on := AwaitOnNode }, TXs) ->
	CurrentHeight = remote_call(Node, ar_node, get_height, []),
	lists:foreach(fun(TX) -> assert_post_tx_to_peer(Node, TX) end, TXs),
	mine(Node),
	[{H, _, _} | _] = wait_until_height(AwaitOnNode, CurrentHeight + 1),
	remote_call(AwaitOnNode, ar_test_node, read_block_when_stored, [H, true],
	  ?POST_AND_MINE_TIMEOUT).

post_block(B, ExpectedResult) when not is_list(ExpectedResult) ->
	post_block(B, [ExpectedResult], peer_ip(main));
post_block(B, ExpectedResults) ->
	post_block(B, ExpectedResults, peer_ip(main)).

post_block(B, ExpectedResults, Peer) ->
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B)),
	await_post_block(B, ExpectedResults, Peer).

send_new_block(Peer, B) ->
	ar_http_iface_client:send_block_binary(Peer, B#block.indep_hash,
			ar_serialize:block_to_binary(B)).

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

read_block_when_stored(H) ->
	read_block_when_stored(H, false).

read_block_when_stored(H, IncludeTXs) ->
	{ok, B} = ar_util:do_until(
		fun() ->
			case ar_storage:read_block(H) of
				unavailable ->
					unavailable;
				B2 ->
					ar_util:do_until(
						fun() ->
							TXs = ar_storage:read_tx(B2#block.txs),
							case lists:any(fun(TX) -> TX == unavailable end, TXs) of
								true ->
									false;
								false ->
									case IncludeTXs of
										true ->
											{ok, B2#block{ txs = TXs }};
										false ->
											{ok, B2}
									end
							end
						end,
						100,
						?READ_BLOCK_TIMEOUT
					)
			end
		end,
		200,
		?READ_BLOCK_TIMEOUT
	),
	B.

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

random_v1_data(Size) ->
	%% Make sure v1 txs do not end with a digit, otherwise they are malleable.
	<< (crypto:strong_rand_bytes(Size - 1))/binary, <<"a">>/binary >>.

assert_get_tx_data(Node, TXID, ExpectedData) ->
	?debugFmt("Polling for data of ~s.", [ar_util:encode(TXID)]),
	Peer = peer_ip(Node),
	true = ar_util:do_until(
		fun() ->
			case ar_http:req(#{ method => get, peer => Peer,
					path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data" }) of
				{ok, {{<<"200">>, _}, _, ExpectedData, _, _}} ->
					true;
				{ok, {{<<"404">>, _}, _, _, _, _}} ->
					false;
				{ok, {{<<"200">>, _}, _, OtherData, _, _}} ->
					?debugFmt("Got unexpected tx data response. TXID: ~s. Peer: ~s. "
							"Expected data size: ~B, got data size: ~B.~n",
							[ar_util:encode(TXID), ar_util:format_peer(Peer),
								byte_size(ExpectedData), byte_size(OtherData)]);
				UnexpectedResponse ->
					?debugFmt("Got unexpected tx data response. TXID: ~s. Peer: ~s. "
							" response: ~p.~n",
							[ar_util:encode(TXID), ar_util:format_peer(Peer),
									UnexpectedResponse]),
					false
			end
		end,
		200,
		?GET_TX_DATA_TIMEOUT
	),
	{ok, {{<<"200">>, _}, _, OffsetJSON, _, _}}
			= ar_http:req(#{ method => get, peer => Peer,
					path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/offset" }),
	Map = jiffy:decode(OffsetJSON, [return_maps]),
	Offset = binary_to_integer(maps:get(<<"offset">>, Map)),
	Size = binary_to_integer(maps:get(<<"size">>, Map)),
	?assertEqual(ExpectedData, get_tx_data_in_chunks(Offset, Size, Peer)),
	?assertEqual(ExpectedData, get_tx_data_in_chunks_traverse_forward(Offset, Size, Peer)).

get_tx_data_in_chunks(Offset, Size, Peer) ->
	get_tx_data_in_chunks(Offset, Offset - Size, Peer, []).

get_tx_data_in_chunks(Offset, Start, _Peer, Bin) when Offset =< Start ->
	ar_util:encode(iolist_to_binary(Bin));
get_tx_data_in_chunks(Offset, Start, Peer, Bin) ->
	{ok, {{<<"200">>, _}, _, JSON, _, _}}
			= ar_http:req(#{ method => get, peer => Peer,
					path => "/chunk/" ++ integer_to_list(Offset) }),
	Map = jiffy:decode(JSON, [return_maps]),
	Chunk = ar_util:decode(maps:get(<<"chunk">>, Map)),
	get_tx_data_in_chunks(Offset - byte_size(Chunk), Start, Peer, [Chunk | Bin]).

get_tx_data_in_chunks_traverse_forward(Offset, Size, Peer) ->
	get_tx_data_in_chunks_traverse_forward(Offset, Offset - Size, Peer, []).

get_tx_data_in_chunks_traverse_forward(Offset, Start, _Peer, Bin) when Offset =< Start ->
	ar_util:encode(iolist_to_binary(lists:reverse(Bin)));
get_tx_data_in_chunks_traverse_forward(Offset, Start, Peer, Bin) ->
	{ok, {{<<"200">>, _}, _, JSON, _, _}}
			= ar_http:req(#{ method => get, peer => Peer,
					path => "/chunk/" ++ integer_to_list(Start + 1) }),
	Map = jiffy:decode(JSON, [return_maps]),
	Chunk = ar_util:decode(maps:get(<<"chunk">>, Map)),
	get_tx_data_in_chunks_traverse_forward(Offset, Start + byte_size(Chunk), Peer,
			[Chunk | Bin]).

assert_data_not_found(Node, TXID) ->
	Peer = peer_ip(Node),
	?assertMatch({ok, {{<<"404">>, _}, _, _Binary, _, _}},
			ar_http:req(#{ method => get, peer => Peer,
					path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data" })).

get_node_namespace() ->
	% Return the namespace part (everything after first - and before @)
	{_, Namespace} = split_node_name(),
	Namespace.

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