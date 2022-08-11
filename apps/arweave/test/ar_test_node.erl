-module(ar_test_node).

-export([start/1, start/2, start/3, slave_start/1, slave_start/2, wait_until_joined/0,
		connect_to_slave/0, disconnect_from_slave/0, slave_call/3, slave_call/4,
		slave_add_tx/1, slave_mine/0, wait_until_height/1, slave_wait_until_height/1,
		assert_slave_wait_until_height/1, wait_until_block_block_index/1,
		assert_wait_until_block_block_index/1, wait_until_receives_txs/1,
		assert_wait_until_receives_txs/1, assert_slave_wait_until_receives_txs/1,
		post_tx_to_slave/1, post_tx_to_slave/2, post_tx_to_master/1, post_tx_to_master/2,
		assert_post_tx_to_slave/1, assert_post_tx_to_master/1, sign_tx/1, sign_tx/2,
		sign_tx/3, sign_v1_tx/1, sign_v1_tx/2, sign_v1_tx/3, get_tx_anchor/0,
		get_tx_anchor/1, join/1, rejoin/1, join_on_slave/0, rejoin_on_slave/0,
		join_on_master/0, rejoin_on_master/0,
		get_last_tx/1, get_last_tx/2, get_tx_confirmations/2,
		get_balance/1, test_with_mocked_functions/2,
		get_tx_price/1, post_and_mine/2, read_block_when_stored/1,
		read_block_when_stored/2, get_chunk/1, get_chunk/2, post_chunk/1, post_chunk/2,
		random_v1_data/1, assert_get_tx_data/3, assert_get_tx_data_master/2,
		assert_get_tx_data_slave/2, assert_data_not_found_master/1,
		assert_data_not_found_slave/1, post_tx_json_to_master/1,
		post_tx_json_to_slave/1, master_peer/0, slave_peer/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

slave_start(MaybeB) ->
	Slave = slave_call(?MODULE, start, [MaybeB]),
	slave_wait_until_joined(),
	Slave.

slave_start(B0, RewardAddr) ->
	Slave = slave_call(?MODULE, start, [B0, RewardAddr]),
	slave_wait_until_joined(),
	Slave.

start(no_block) ->
	[B0] = ar_weave:init([]),
	start(B0, unclaimed, element(2, application:get_env(arweave, config)));
start(B0) ->
	start(B0, unclaimed, element(2, application:get_env(arweave, config))).

start(B0, RewardAddr) ->
	start(B0, RewardAddr, element(2, application:get_env(arweave, config))).

start(B0, RewardAddr, Config) ->
	%% Currently, ar_weave:init stores the wallet tree on disk. Tests call ar_weave:init,
	%% it returns the block header (which does not contain the wallet tree), the block header
	%% is passed here where we want to erase the previous storage and at the same time
	%% keep the genesis data to start a new weave.
	WalletList = read_wallet_list(B0#block.wallet_list),
	clean_up_and_stop(),
	write_genesis_files(Config#config.data_dir, B0, WalletList),
	ok = application:set_env(arweave, config, Config#config{
		start_from_block_index = true,
		peers = [],
		mining_addr = RewardAddr,
		disk_space_check_frequency = 1000,
		header_sync_jobs = 4,
		enable = [search_in_rocksdb_when_mining, serve_arql, serve_wallet_txs,
			serve_wallet_deposits, arql_tags_index, serve_tx_data_without_limits],
		debug = true
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	wait_until_joined(),
	{whereis(ar_node_worker), B0}.

read_wallet_list(RootHash) ->
	case ar_rpc:call(master, ar_storage, read_wallet_list, [RootHash], 10000) of
		{ok, Tree} ->
			Tree;
		_ ->
			%% The tree is supposed to be stored by either of the nodes - the one
			%% where ar_weave:init was called.
			{ok, Tree} = slave_call(ar_storage, read_wallet_list, [RootHash]),
			Tree
	end.

clean_up_and_stop() ->
	Config = stop(),
	os:cmd("rm -r " ++ Config#config.data_dir ++ "/*").

stop() ->
	{ok, Config} = application:get_env(arweave, config),
	ok = application:stop(arweave),
	ok = ar:stop_dependencies(),
	Config.

write_genesis_files(DataDir, B0, WalletList) ->
	BH = B0#block.indep_hash,
	BlockDir = filename:join(DataDir, ?BLOCK_DIR),
	ok = filelib:ensure_dir(BlockDir ++ "/"),
	BlockFilepath = filename:join(BlockDir, binary_to_list(ar_util:encode(BH)) ++ ".json"),
	BlockJSON = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B0)),
	ok = file:write_file(BlockFilepath, BlockJSON),
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
	BIJSON = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(BI)),
	HashListDir = filename:join(DataDir, ?HASH_LIST_DIR),
	ok = filelib:ensure_dir(HashListDir ++ "/"),
	BIFilepath = filename:join(HashListDir, <<"last_block_index.json">>),
	ok = file:write_file(BIFilepath, BIJSON),
	WalletListDir = filename:join(DataDir, ?WALLET_LIST_DIR),
	ok = filelib:ensure_dir(WalletListDir ++ "/"),
	RootHash = B0#block.wallet_list,
	WalletListFilepath =
		filename:join(WalletListDir, binary_to_list(ar_util:encode(RootHash)) ++ ".json"),
	WalletListJSON =
		ar_serialize:jsonify(
			ar_serialize:wallet_list_to_json_struct(B0#block.reward_addr, false, WalletList)
		),
	ok = file:write_file(WalletListFilepath, WalletListJSON).

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
	ok = application:set_env(arweave, config, Config#config{
		start_from_block_index = false,
		peers = [Peer]
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	whereis(ar_node_worker).

join_on_master() ->
	slave_call(ar_test_node, join, [master_peer()]).

rejoin_on_master() ->
	slave_call(ar_test_node, rejoin, [master_peer()]).

connect_to_slave() ->
	%% Unblock connections possibly blocked in the prior test code.
	ar_peers:unblock_connections(),
	slave_call(ar_peers, unblock_connections, []),
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
	ar_peers:block_connections(),
	slave_call(ar_peers, block_connections, []).

slave_call(Module, Function, Args) ->
	slave_call(Module, Function, Args, 60000).

slave_call(Module, Function, Args, Timeout) ->
	ar_rpc:call(slave, Module, Function, Args, Timeout).

slave_add_tx(TX) ->
	slave_call(ar_node, add_tx, [TX]).

slave_mine() ->
	slave_call(ar_node, mine, []).

wait_until_joined() ->
	ar_util:do_until(
		fun() -> ar_node:is_joined() end,
		100,
		60 * 1000
	 ).

slave_wait_until_joined() ->
	ar_util:do_until(
		fun() -> slave_call(ar_node, is_joined, []) end,
		100,
		60 * 1000
	 ).

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
		60 * 1000
	),
	BI.

slave_wait_until_height(TargetHeight) ->
	slave_call(?MODULE, wait_until_height, [TargetHeight]).

assert_slave_wait_until_height(TargetHeight) ->
	BI = slave_call(?MODULE, wait_until_height, [TargetHeight]),
	?assert(is_list(BI)),
	BI.

assert_wait_until_block_block_index(BI) ->
	?assertEqual(ok, wait_until_block_block_index(BI)).

wait_until_block_block_index(BI) ->
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
		60 * 1000
	).

assert_slave_wait_until_receives_txs(TXs) ->
	?assertEqual(ok, slave_call(?MODULE, wait_until_receives_txs, [TXs])).

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

sign_tx(Wallet) ->
	sign_tx(slave, Wallet, #{ format => 2 }, fun ar_tx:sign/2).

sign_tx(Wallet, TXParams) ->
	sign_tx(slave, Wallet, insert_root(TXParams#{ format => 2 }), fun ar_tx:sign/2).

sign_tx(Node, Wallet, TXParams) ->
	sign_tx(Node, Wallet, insert_root(TXParams#{ format => 2 }), fun ar_tx:sign/2).

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

sign_v1_tx(Wallet) ->
	sign_tx(slave, Wallet, #{}, fun ar_tx:sign_v1/2).

sign_v1_tx(Wallet, TXParams) ->
	sign_tx(slave, Wallet, TXParams, fun ar_tx:sign_v1/2).

sign_v1_tx(Node, Wallet, TXParams) ->
	sign_tx(Node, Wallet, TXParams, fun ar_tx:sign_v1/2).

sign_tx(Node, Wallet, TXParams, SignFun) ->
	{_, Pub} = Wallet,
	Data = maps:get(data, TXParams, <<>>),
	DataSize = maps:get(data_size, TXParams, byte_size(Data)),
	Reward = case maps:get(reward, TXParams, none) of
		none ->
			get_tx_price(Node, DataSize, maps:get(target, TXParams, <<>>));
		AssignedReward ->
			AssignedReward
	end,
	SignFun(
		(ar_tx:new())#tx {
			owner = Pub,
			reward = Reward,
			data = Data,
			target = maps:get(target, TXParams, <<>>),
			quantity = maps:get(quantity, TXParams, 0),
			tags = maps:get(tags, TXParams, []),
			last_tx = maps:get(last_tx, TXParams, <<>>),
			data_size = DataSize,
			data_root = maps:get(data_root, TXParams, <<>>),
			format = maps:get(format, TXParams, 1)
		},
		Wallet
	).

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

get_balance(Pub) ->
	Peer = slave_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub)))
					++ "/balance",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	binary_to_integer(Reply).

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
		[
			{timeout, 120, TestFun}
		]
	}.

get_tx_price(DataSize) ->
	get_tx_price(slave, DataSize, <<>>).

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
	binary_to_integer(Reply).

post_and_mine(#{ miner := Miner, await_on := AwaitOn }, TXs) ->
	CurrentHeight = case Miner of
		{slave, _MiningNode} ->
			Height = slave_call(ar_node, get_height, []),
			lists:foreach(fun(TX) -> assert_post_tx_to_slave(TX) end, TXs),
			case AwaitOn of
				{master, _} ->
					assert_wait_until_receives_txs(TXs);
				_ ->
					ok
			end,
			slave_mine(),
			Height;
		{master, _MiningNode} ->
			Height = ar_node:get_height(),
			lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
			case AwaitOn of
				{slave, _} ->
					assert_slave_wait_until_receives_txs(TXs);
				_ ->
					ok
			end,
			ar_node:mine(),
			Height
	end,
	case AwaitOn of
		{master, _AwaitNode} ->
			[{H, _, _} | _] = wait_until_height(CurrentHeight + 1),
			read_block_when_stored(H, true);
		{slave, _AwaitNode} ->
			[{H, _, _} | _] = slave_wait_until_height(CurrentHeight + 1),
			slave_call(ar_test_node, read_block_when_stored, [H, true])
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
						10000
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
				UnexpectedResponse ->
					?debugFmt("Got unexpected response: ~p.", [UnexpectedResponse]),
					false
			end
		end,
		100,
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
	get_tx_data_in_chunks_traverse_forward(Offset, Start + byte_size(Chunk), Peer, [Chunk | Bin]).

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
