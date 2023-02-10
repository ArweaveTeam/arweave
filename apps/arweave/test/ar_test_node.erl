-module(ar_test_node).

-export([start/0, start/1, start/2, start/3, start/4, slave_start/0, slave_start/1,
		slave_start/2, slave_start/3, wait_until_joined/0,
		get_tx_price/1, get_tx_price/2, get_tx_price/3,
		get_optimistic_tx_price/1, get_optimistic_tx_price/2, get_optimistic_tx_price/3,
		sign_tx/1, sign_tx/2, sign_tx/3, sign_v1_tx/1, sign_v1_tx/2, sign_v1_tx/3,
		get_balance/1, get_balance/2, get_balance_by_address/2,
		stop/0, slave_stop/0, connect_to_slave/0, disconnect_from_slave/0,
		slave_call/3, slave_call/4,
		slave_mine/0, wait_until_height/1, slave_wait_until_height/1,
		assert_slave_wait_until_height/1, wait_until_block_index/1,
		assert_wait_until_block_index/1, assert_slave_wait_until_block_index/1,
		wait_until_receives_txs/1,
		assert_wait_until_receives_txs/1, assert_slave_wait_until_receives_txs/1,
		post_tx_to_slave/1, post_tx_to_slave/2, post_tx_to_master/1, post_tx_to_master/2,
		assert_post_tx_to_slave/1, assert_post_tx_to_master/1,
		get_tx_anchor/0,
		get_tx_anchor/1, join/1, rejoin/1, join_on_slave/0, rejoin_on_slave/0,
		join_on_master/0, rejoin_on_master/0,
		get_last_tx/1, get_last_tx/2, get_tx_confirmations/2,
		test_with_mocked_functions/2,
		post_and_mine/2, read_block_when_stored/1,
		read_block_when_stored/2, get_chunk/1, get_chunk/2, post_chunk/1, post_chunk/2,
		random_v1_data/1, assert_get_tx_data/3, assert_get_tx_data_master/2,
		assert_get_tx_data_slave/2, assert_data_not_found_master/1,
		assert_data_not_found_slave/1, post_tx_json_to_master/1,
		post_tx_json_to_slave/1, master_peer/0, slave_peer/0,
		wait_until_syncs_genesis_data/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% May occasionally take quite long on a slow CI server, expecially in tests
%% with height >= 20 (2 difficulty retargets).
-define(WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT, 180000).
-define(WAIT_UNTIL_RECEIVES_TXS_TIMEOUT, 30000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start a fresh master node.
start() ->
	[B0] = ar_weave:init(),
	start(B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
			element(2, application:get_env(arweave, config))).

%% @doc Start a fresh master node with the given genesis block.
start(B0) ->
	start(B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
			element(2, application:get_env(arweave, config))).

%% @doc Start a fresh master node with the given genesis block and mining address.
start(B0, RewardAddr) ->
	start(B0, RewardAddr, element(2, application:get_env(arweave, config))).

%% @doc Start a fresh master node with the given genesis block, mining address, and config.
start(B0, RewardAddr, Config) ->
	%% Configure the storage modules to cover 200 MiB of the weave.
	StorageModules = lists:flatten([[{20 * 1024 * 1024, N, {spora_2_6, RewardAddr}},
			{20 * 1024 * 1024, N, spora_2_5}] || N <- lists:seq(0, 8)]),
	start(B0, RewardAddr, Config, StorageModules).

%% @doc Start a fresh master node with the given genesis block, mining address, config,
%% and storage modules.
start(B0, RewardAddr, Config, StorageModules) ->
	clean_up_and_stop(),
	write_genesis_files(Config#config.data_dir, B0),
	ok = application:set_env(arweave, config, Config#config{
		start_from_block_index = true,
		peers = [],
		mining_addr = RewardAddr,
		storage_modules = StorageModules,
		disk_space_check_frequency = 1000,
		sync_jobs = 2,
		packing_rate = 20,
		disk_pool_jobs = 2,
		header_sync_jobs = 2,
		enable = [search_in_rocksdb_when_mining, serve_tx_data_without_limits,
				double_check_nonce_limiter, legacy_storage_repacking, serve_wallet_lists],
		mining_server_chunk_cache_size_limit = 4,
		debug = true
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	wait_until_joined(),
	wait_until_syncs_genesis_data(),
	{whereis(ar_node_worker), B0}.

%% @doc Start a fresh slave node.
slave_start() ->
	Slave = slave_call(?MODULE, start, [], 20000),
	slave_wait_until_joined(),
	slave_wait_until_syncs_genesis_data(),
	Slave.

%% @doc Start a fresh slave node with the given genesis block.
slave_start(B) ->
	Slave = slave_call(?MODULE, start, [B], 20000),
	slave_wait_until_joined(),
	slave_wait_until_syncs_genesis_data(),
	Slave.

%% @doc Start a fresh slave node with the given genesis block and mining address.
slave_start(B0, RewardAddr) ->
	Slave = slave_call(?MODULE, start, [B0, RewardAddr], 20000),
	slave_wait_until_joined(),
	slave_wait_until_syncs_genesis_data(),
	Slave.

%% @doc Start a fresh slave node with the given genesis block, mining address, and config.
slave_start(B0, RewardAddr, Config) ->
	Slave = slave_call(?MODULE, start, [B0, RewardAddr, Config], 20000),
	slave_wait_until_joined(),
	slave_wait_until_syncs_genesis_data(),
	Slave.

%% @doc Wait until the master node joins the network (initializes the state).
wait_until_joined() ->
	ar_util:do_until(
		fun() -> ar_node:is_joined() end,
		100,
		60 * 1000
	 ).

%% @doc Fetch the fee estimation and the denomination (call GET /price2/[size])
%% from the slave node.
get_tx_price(DataSize) ->
	get_tx_price(slave, DataSize).

%% @doc Fetch the fee estimation and the denomination (call GET /price2/[size])
%% from the given node.
get_tx_price(Node, DataSize) ->
	get_tx_price(Node, DataSize, <<>>).

%% @doc Fetch the fee estimation and the denomination (call GET /price2/[size]/[addr])
%% from the given node.
get_tx_price(Node, DataSize, Target) ->
	Peer = case Node of slave -> slave_peer(); master -> master_peer() end,
	{_, _, _, _, Port} = Peer,
	Path = "/price/" ++ integer_to_list(DataSize) ++ "/"
			++ binary_to_list(ar_util:encode(Target)),
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path,
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Fee = binary_to_integer(Reply),
	Path2 = "/price2/" ++ integer_to_list(DataSize) ++ "/"
			++ binary_to_list(ar_util:encode(Target)),
	{ok, {{<<"200">>, _}, _, Reply2, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path2,
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Map = jiffy:decode(Reply2, [return_maps]),
	case binary_to_integer(maps:get(<<"fee">>, Map)) of
		Fee ->
			{Fee, maps:get(<<"denomination">>, Map)};
		Fee2 ->
			?assert(false, io_lib:format("Fee mismatch, expected: ~B, got: ~B.", [Fee, Fee2]))
	end.

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]) from the slave node.
get_optimistic_tx_price(DataSize) ->
	get_optimistic_tx_price(slave, DataSize).

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]) from the given node.
get_optimistic_tx_price(Node, DataSize) ->
	get_optimistic_tx_price(Node, DataSize, <<>>).

%% @doc Fetch the optimistic fee estimation (call GET /price/[size]/[addr]) from the given
%% node.
get_optimistic_tx_price(Node, DataSize, Target) ->
	Peer = case Node of slave -> slave_peer(); master -> master_peer() end,
	{_, _, _, _, Port} = Peer,
	Path = "/optimistic_price/" ++ integer_to_list(DataSize) ++ "/"
			++ binary_to_list(ar_util:encode(Target)),
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path,
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	binary_to_integer(maps:get(<<"fee">>, jiffy:decode(Reply, [return_maps]))).

%% @doc Return a signed format=2 transaction with the minimum required fee fetched from
%% GET /price/0 on the slave node.
sign_tx(Wallet) ->
	sign_tx(slave, Wallet, #{ format => 2 }, fun ar_tx:sign/2).

%% @doc Return a signed format=2 transaction with properties from the given Args map.
%% If the fee is not in Args, fetch it from GET /price/{data_size}
%% or GET /price/{data_size}/{target} (if the target is specified) on the slave node.
sign_tx(Wallet, Args) ->
	sign_tx(slave, Wallet, insert_root(Args#{ format => 2 }), fun ar_tx:sign/2).

%% @doc Like sign_tx/2, but use the given Node to fetch the fee estimation and
%% block anchor from.
sign_tx(Node, Wallet, Args) ->
	sign_tx(Node, Wallet, insert_root(Args#{ format => 2 }), fun ar_tx:sign/2).

%% @doc Like sign_tx/1 but return a format=1 transaction.
sign_v1_tx(Wallet) ->
	sign_tx(slave, Wallet, #{}, fun ar_tx:sign_v1/2).

%% @doc Like sign_tx/2 but return a format=1 transaction.
sign_v1_tx(Wallet, TXParams) ->
	sign_tx(slave, Wallet, TXParams, fun ar_tx:sign_v1/2).

%% @doc Like sign_tx/3 but return a format=1 transaction.
sign_v1_tx(Node, Wallet, Args) ->
	sign_tx(Node, Wallet, Args, fun ar_tx:sign_v1/2).

%% @doc Return the current balance of the account with the given public key.
%% Request it from the slave node.
get_balance(Pub) ->
	get_balance_by_address(slave, ar_wallet:to_address(Pub)).

%% @doc Return the current balance of the account with the given public key.
%% Request it from the given node.
get_balance(Node, Pub) ->
	get_balance_by_address(Node, ar_wallet:to_address(Pub)).

%% @doc Return the current balance of the given account (request it from the given node).
get_balance_by_address(Node, Address) ->
	Peer = case Node of slave -> slave_peer(); master -> master_peer() end,
	Port = element(5, Peer),
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(ar_util:encode(Address)) ++ "/balance",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Balance = binary_to_integer(Reply),
	B =
		case Node of
			master ->
				ar_node:get_current_block();
			slave ->
				slave_call(ar_node, get_current_block, [])
		end,
	{ok, {{<<"200">>, _}, _, Reply2, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet_list/" ++ binary_to_list(ar_util:encode(B#block.wallet_list))
					++ "/" ++ binary_to_list(ar_util:encode(Address)) ++ "/balance",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	case binary_to_integer(Reply2) of
		Balance ->
			Balance;
		Balance2 ->
			?assert(false, io_lib:format("Expected: ~B, got: ~B.~n", [Balance, Balance2]))
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

clean_up_and_stop() ->
	Config = stop(),
	{ok, Entries} = file:list_dir_all(Config#config.data_dir),
	lists:foreach(
		fun	("wallets") ->
				ok;
			(Entry) ->
				file:del_dir_r(filename:join(Config#config.data_dir, Entry))
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
	BI = [ar_util:block_index_entry_from_block(B0)],
	BIBin = term_to_binary({BI, B0#block.reward_history}),
	HashListDir = filename:join(DataDir, ?HASH_LIST_DIR),
	ok = filelib:ensure_dir(HashListDir ++ "/"),
	BIFilepath = filename:join(HashListDir, <<"last_block_index_and_reward_history.bin">>),
	ok = file:write_file(BIFilepath, BIBin),
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
	ok = ar:stop_dependencies(),
	Config.

slave_stop() ->
	slave_call(ar_test_node, stop, [], 20000).

join_on_slave() ->
	join(slave_peer()).

rejoin_on_slave() ->
	join(slave_peer(), true).

rejoin(Peer) ->
	join(Peer, true).

join(Peer) ->
	join(Peer, false).

join(Peer, Rejoin) ->
	{ok, Config} = application:get_env(arweave, config),
	case Rejoin of
		true ->
			stop();
		false ->
			clean_up_and_stop()
	end,
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModulePacking = case ar_fork:height_2_6() of infinity -> spora_2_5;
			_ -> {spora_2_6, RewardAddr} end,
	%% Configure the storage modules to cover 100 MiB of the weave.
	StorageModules = [{20 * 1024 * 1024, N, StorageModulePacking} || N <- lists:seq(0, 4)],
	ok = application:set_env(arweave, config, Config#config{
		start_from_block_index = false,
		mining_addr = RewardAddr,
		storage_modules = StorageModules,
		peers = [Peer]
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	whereis(ar_node_worker).

join_on_master() ->
	slave_call(ar_test_node, join, [master_peer()], 20000).

rejoin_on_master() ->
	slave_call(ar_test_node, rejoin, [master_peer()], 20000).

connect_to_slave() ->
	%% Unblock connections possibly blocked in the prior test code.
	ar_peers:unblock_connections(),
	slave_call(ar_peers, unblock_connections, []),
	ar_poller:resume(),
	slave_call(ar_poller, resume, []),
	%% Make requests to the nodes to make them discover each other.
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => slave_peer(),
			path => "/info",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, master_peer()))},
					{<<"X-Release">>, integer_to_binary(?RELEASE_NUMBER)}]
		}),
	true = ar_util:do_until(
		fun() ->
			[master_peer()] == slave_call(ar_peers, get_peers, [])
		end,
		200,
		5000
	),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/info",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, slave_peer()))},
					{<<"X-Release">>, integer_to_binary(?RELEASE_NUMBER)}]
		}),
	true = ar_util:do_until(
		fun() ->
			[slave_peer()] == ar_peers:get_peers()
		end,
		200,
		5000
	).

disconnect_from_slave() ->
	ar_poller:pause(),
	slave_call(ar_poller, pause, []),
	ar_peers:block_connections(),
	slave_call(ar_peers, block_connections, []).

slave_call(Module, Function, Args) ->
	slave_call(Module, Function, Args, 10000).

slave_call(Module, Function, Args, Timeout) ->
	Key = rpc:async_call('slave@127.0.0.1', Module, Function, Args),
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
			?debugFmt("Timed out waiting for the rpc reply; module: ~p, function: ~p, "
					"args: ~p.~n", [Module, Function, Args]);
		_ ->
			ok
	end,
	?assertMatch({ok, _}, Result),
	element(2, Result).

slave_mine() ->
	slave_call(ar_node, mine, []).

wait_until_syncs_genesis_data() ->
	WeaveSize = (ar_node:get_current_block())#block.weave_size,
	wait_until_syncs_genesis_data(0, WeaveSize).

wait_until_syncs_genesis_data(Offset, WeaveSize) when Offset >= WeaveSize ->
	ok;
wait_until_syncs_genesis_data(Offset, WeaveSize) ->
	{ok, Config} = application:get_env(arweave, config),
	Packing_2_6 = {spora_2_6, Config#config.mining_addr},
	true = ar_util:do_until(
		fun() ->
			IsRecorded =
				case ar_fork:height_2_6() > 0 of
					true ->
						ar_sync_record:is_recorded(Offset + 1, {ar_data_sync, spora_2_5});
					false ->
						ar_sync_record:is_recorded(Offset + 1, {ar_data_sync, Packing_2_6})
				end,
			case IsRecorded of
				{{true, _}, _} ->
					true;
				_ ->
					false
			end
		end,
		200,
		10000
	),
	wait_until_syncs_genesis_data(Offset + ?DATA_CHUNK_SIZE, WeaveSize).

slave_wait_until_joined() ->
	ar_util:do_until(
		fun() -> slave_call(ar_node, is_joined, []) end,
		100,
		60 * 1000
	 ).

slave_wait_until_syncs_genesis_data() ->
	ok = slave_call(ar_test_node, wait_until_syncs_genesis_data, [], 60000).

wait_until_height(TargetHeight) ->
	{ok, BI} = ar_util:do_until(
		fun() ->
			case ar_node:get_blocks() of
				BI when length(BI) - 1 == TargetHeight ->
					{ok, BI};
				_ ->
					false
			end
		end,
		100,
		?WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT
	),
	BI.

slave_wait_until_height(TargetHeight) ->
	slave_call(?MODULE, wait_until_height, [TargetHeight],
			?WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT + 500).

assert_slave_wait_until_height(TargetHeight) ->
	BI = slave_call(?MODULE, wait_until_height, [TargetHeight],
			?WAIT_UNTIL_BLOCK_HEIGHT_TIMEOUT + 500),
	?assert(is_list(BI), iolist_to_binary(io_lib:format("Got ~p.", [BI]))),
	BI.

assert_wait_until_block_index(BI) ->
	?assertEqual(ok, wait_until_block_index(BI)).

assert_slave_wait_until_block_index(BI) ->
	?assertEqual(ok, slave_wait_until_block_index(BI)).

slave_wait_until_block_index(BI) ->
	slave_call(?MODULE, wait_until_block_index, [BI]).

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
		60 * 1000
	).

assert_wait_until_receives_txs(TXs) ->
	?assertEqual(ok, wait_until_receives_txs(TXs)).

wait_until_receives_txs(TXs) ->
	ar_util:do_until(
		fun() ->
			MinedTXIDs = [TX#tx.id || TX <- ar_node:get_ready_for_mining_txs()],
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

assert_slave_wait_until_receives_txs(TXs) ->
	?assertEqual(ok, slave_call(?MODULE, wait_until_receives_txs, [TXs],
			?WAIT_UNTIL_RECEIVES_TXS_TIMEOUT + 500)).

assert_post_tx_to_slave(TX) ->
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_slave(TX).

assert_post_tx_to_master(TX) ->
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_master(TX).

post_tx_to_master(TX) ->
	post_tx_to_master(TX, true).

post_tx_to_master(TX, Wait) ->
	Reply = post_tx_json_to_master(ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
			case Wait of
				true ->
					assert_wait_until_receives_txs([TX]);
				false ->
					ok
			end;
		_ ->
			?debugFmt("Failed to post transaction ~s. Error DB entries: ~p.",
					[ar_util:encode(TX#tx.id), ar_tx_db:get_error_codes(TX#tx.id)]),
			noop
	end,
	Reply.

post_tx_to_slave(TX) ->
	post_tx_to_slave(TX, true).

post_tx_to_slave(TX, Wait) ->
	Reply = post_tx_json_to_slave(ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
			case Wait of
				true ->
					assert_slave_wait_until_receives_txs([TX]);
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
			?debugFmt(
				"Failed to post transaction. TX: ~s. TX format: ~B. TX fee: ~B. TX size: ~B. "
				"TX last_tx: ~s. Error(s): ~p. Reply: ~p.~n",
				[ar_util:encode(TX#tx.id), TX#tx.format, TX#tx.reward, TX#tx.data_size,
					ar_util:encode(TX#tx.last_tx),
					slave_call(ar_tx_db, get_error_codes, [TX#tx.id]), ErrorInfo]),
			noop
	end,
	Reply.

post_tx_json_to_master(JSON) ->
	post_tx_json(JSON, master_peer()).

post_tx_json_to_slave(JSON) ->
	post_tx_json(JSON, slave_peer()).

post_tx_json(JSON, Peer) ->
	{_, _, _, _, Port} = Peer,
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx",
		headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}],
		body => JSON
	}).

get_tx_anchor() ->
	get_tx_anchor(slave).

get_tx_anchor(slave) ->
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => slave_peer(),
			path => "/tx_anchor",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, slave_peer()))}]
		}),
	ar_util:decode(Reply);
get_tx_anchor(master) ->
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/tx_anchor",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, master_peer()))}]
		}),
	ar_util:decode(Reply).

get_last_tx(Key) ->
	get_last_tx(slave, Key).

get_last_tx(slave, {_, Pub}) ->
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => slave_peer(),
			path => "/wallet/"
					++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub)))
					++ "/last_tx",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, slave_peer()))}]
		}),
	ar_util:decode(Reply);
get_last_tx(master, {_, Pub}) ->
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/wallet/"
					++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub)))
					++ "/last_tx",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, master_peer()))}]
		}),
	ar_util:decode(Reply).

get_tx_confirmations(slave, TXID) ->
	Response =
		ar_http:req(#{
			method => get,
			peer => slave_peer(),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/status",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, slave_peer()))}]
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Reply, _, _}} ->
			{Status} = ar_serialize:dejsonify(Reply),
			element(2, lists:keyfind(<<"number_of_confirmations">>, 1, Status));
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			-1
	end;
get_tx_confirmations(master, TXID) ->
	Response =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/status",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(element(5, master_peer()))}]
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Reply, _, _}} ->
			{Status} = ar_serialize:dejsonify(Reply),
			lists:keyfind(<<"number_of_confirmations">>, 1, Status);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			-1
	end.

test_with_mocked_functions(Functions, TestFun) ->
	{
		foreach,
		fun() ->
			lists:foldl(
				fun({Module, Fun, Mock}, Mocked) ->
					NewMocked = case maps:get(Module, Mocked, false) of
						false ->
							meck:new(Module, [passthrough]),
							slave_call(meck, new, [Module, [no_link, passthrough]]),
							maps:put(Module, true, Mocked);
						true ->
							Mocked
					end,
					meck:expect(Module, Fun, Mock),
					slave_call(meck, expect, [Module, Fun, Mock]),
					NewMocked
				end,
				maps:new(),
				Functions
			)
		end,
		fun(Mocked) ->
			maps:fold(
				fun(Module, _, _) ->
					meck:unload(Module),
					slave_call(meck, unload, [Module])
				end,
				noop,
				Mocked
			)
		end,
		[{timeout, 900, TestFun}]
	}.

post_and_mine(#{ miner := Miner, await_on := AwaitOn }, TXs) ->
	CurrentHeight = case Miner of
		{slave, _MiningNode} ->
			Height = slave_call(ar_node, get_height, []),
			lists:foreach(fun(TX) -> assert_post_tx_to_slave(TX) end, TXs),
			slave_mine(),
			Height;
		{master, _MiningNode} ->
			Height = ar_node:get_height(),
			lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
			ar_node:mine(),
			Height
	end,
	case AwaitOn of
		{master, _AwaitNode} ->
			[{H, _, _} | _] = wait_until_height(CurrentHeight + 1),
			read_block_when_stored(H, true);
		{slave, _AwaitNode} ->
			[{H, _, _} | _] = slave_wait_until_height(CurrentHeight + 1),
			slave_call(ar_test_node, read_block_when_stored, [H, true], 20000)
	end.

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
						200,
						30000
					)
			end
		end,
		200,
		60000
	),
	B.

get_chunk(Offset) ->
	get_chunk(master, Offset).

get_chunk(master, Offset) ->
	get_chunk2(master_peer(), Offset);

get_chunk(slave, Offset) ->
	get_chunk2(slave_peer(), Offset).

get_chunk2(Peer, Offset) ->
	ar_http:req(#{
		method => get,
		peer => Peer,
		path => "/chunk/" ++ integer_to_list(Offset),
		headers => [{<<"x-bucket-based-offset">>, <<"true">>}]
	}).

post_chunk(Proof) ->
	post_chunk(master, Proof).

post_chunk(master, Proof) ->
	post_chunk2(master_peer(), Proof);

post_chunk(slave, Proof) ->
	post_chunk2(slave_peer(), Proof).

post_chunk2(Peer, Proof) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/chunk",
		body => Proof
	}).

random_v1_data(Size) ->
	%% Make sure v1 txs do not end with a digit, otherwise they are malleable.
	<< (crypto:strong_rand_bytes(Size - 1))/binary, <<"a">>/binary >>.

assert_get_tx_data_master(TXID, ExpectedData) ->
	assert_get_tx_data(master_peer(), TXID, ExpectedData).

assert_get_tx_data_slave(TXID, ExpectedData) ->
	assert_get_tx_data(slave_peer(), TXID, ExpectedData).

assert_get_tx_data(Peer, TXID, ExpectedData) ->
	?debugFmt("Polling for data of ~s.", [ar_util:encode(TXID)]),
	true = ar_util:do_until(
		fun() ->
			case ar_http:req(#{ method => get, peer => Peer,
					path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data" }) of
				{ok, {{<<"200">>, _}, _, ExpectedData, _, _}} ->
					true;
				{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
					false;
				_UnexpectedResponse ->
					?debugFmt("Got unexpected tx data response. TXID: ~s. Peer: ~s.~n",
							[ar_util:encode(TXID), ar_util:format_peer(Peer)]),
					false
			end
		end,
		200,
		120 * 1000
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

assert_data_not_found_master(TXID) ->
	assert_data_not_found(master_peer(), TXID).

assert_data_not_found_slave(TXID) ->
	assert_data_not_found(slave_peer(), TXID).

assert_data_not_found(Peer, TXID) ->
	?assertMatch({ok, {{<<"200">>, _}, _, <<>>, _, _}},
			ar_http:req(#{ method => get, peer => Peer,
					path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data" })).

master_peer() ->
	{ok, Config} = application:get_env(arweave, config),
	{127, 0, 0, 1, Config#config.port}.

slave_peer() ->
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	{127, 0, 0, 1, Config#config.port}.
