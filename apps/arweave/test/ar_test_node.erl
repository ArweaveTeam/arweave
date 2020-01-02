-module(ar_test_node).

-export([start/1, start/2, start/3, slave_start/1, slave_start/2]).
-export([connect_to_slave/0, disconnect_from_slave/0]).
-export([slave_call/3, slave_call/4]).
-export([gossip/2, slave_gossip/2, slave_add_tx/2, slave_mine/1]).
-export([wait_until_height/2, slave_wait_until_height/2]).
-export([assert_slave_wait_until_height/2]).
-export([wait_until_block_block_index/2]).
-export([assert_wait_until_block_block_index/2]).
-export([wait_until_receives_txs/2]).
-export([assert_wait_until_receives_txs/2]).
-export([assert_slave_wait_until_receives_txs/2]).
-export([post_tx_to_slave/2, post_tx_to_master/2]).
-export([assert_post_tx_to_slave/2, assert_post_tx_to_master/2]).
-export([sign_tx/1, sign_tx/2, sign_tx/3]).
-export([sign_tx_pre_fork_2_0/1, sign_tx_pre_fork_2_0/2, sign_tx_pre_fork_2_0/3]).
-export([get_tx_anchor/0, get_tx_anchor/1]).
-export([join/1]).
-export([get_last_tx/1, get_last_tx/2]).
-export([get_tx_confirmations/2]).
-export([get_balance/1]).
-export([test_with_mocked_functions/2]).
-export([get_tx_price/1]).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

start(no_block) ->
	[B0] = ar_weave:init([]),
	start(B0, unclaimed);
start(B0) ->
	start(B0, unclaimed).

start(B0, RewardAddr) ->
	ar_storage:write_full_block(B0),
	start(B0, {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}, RewardAddr).

slave_start(no_block) ->
	[B0] = slave_call(ar_weave, init, [[]]),
	slave_start(B0, unclaimed);
slave_start(B0) ->
	slave_start(B0, unclaimed).

slave_start(B0, RewardAddr) ->
	slave_call(ar_storage, write_full_block, [B0]),
	slave_call(?MODULE, start, [B0, {127, 0, 0, 1, ar_meta_db:get(port)}, RewardAddr]).

start(B0, Peer, RewardAddr) ->
	ar_storage:clear(),
	ar_tx_queue:stop(),
	ok = kill_if_running([http_bridge_node, http_entrypoint_node]),
	Node = ar_node:start([], [B0], 0, RewardAddr),
	ar_http_iface_server:reregister(http_entrypoint_node, Node),
	ar_meta_db:reset_peer(Peer),
	Bridge = ar_bridge:start([], Node, ar_meta_db:get(port)),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	{Node, B0}.

kill_if_running([Name | Names]) ->
	case whereis(Name) of
		undefined ->
			do_nothing;
		PID ->
			exit(PID, kill)
	end,
	kill_if_running(Names);
kill_if_running([]) ->
	ok.

join(Peer) ->
	Node = ar_node:start([Peer]),
	ar_http_iface_server:reregister(http_entrypoint_node, Node),
	ar_meta_db:reset_peer(Peer),
	Bridge = ar_bridge:start([], Node, ar_meta_db:get(port)),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	Node.

connect_to_slave() ->
	%% Connect the nodes by making two HTTP calls.
	%%
	%% After a request to a peer, the peer is recorded in ar_meta_db but
	%% not in the remote peer list. So we need to remove it from ar_meta_db
	%% otherwise it's not added to the remote peer list when it makes a request
	%% to us in turn.
	MasterPort = ar_meta_db:get(port),
	slave_call(ar_meta_db, reset_peer, [{127, 0, 0, 1, MasterPort}]),
	SlavePort = slave_call(ar_meta_db, get, [port]),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, SlavePort},
			"/info",
			[{<<"X-P2p-Port">>, integer_to_binary(MasterPort)}]
		),
	ar_meta_db:reset_peer({127, 0, 0, 1, SlavePort}),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, MasterPort},
			"/info",
			[{<<"X-P2p-Port">>, integer_to_binary(SlavePort)}]
		).

disconnect_from_slave() ->
	%% Disconnects master from slave so that they do not share blocks
	%% and transactions unless they were bound by ar_node:add_peers/2.
	%% All HTTP requests made in this module are made with the
	%% x-p2p-port HTTP header corresponding to the listening port of
	%% the receiving node so that nodes do not start peering with each
	%% other again without an explicit request.
	SlaveBridge = slave_call(erlang, whereis, [http_bridge_node]),
	slave_call(ar_bridge, set_remote_peers, [SlaveBridge, []]),
	MasterBridge = whereis(http_bridge_node),
	ar_bridge:set_remote_peers(MasterBridge, []).

gossip(off, Node) ->
	ar_node:set_loss_probability(Node, 1);
gossip(on, Node) ->
	ar_node:set_loss_probability(Node, 0).

slave_call(Module, Function, Args) ->
	slave_call(Module, Function, Args, 60000).

slave_call(Module, Function, Args, Timeout) ->
	ar_rpc:call(slave, Module, Function, Args, Timeout).

slave_gossip(off, Node) ->
	slave_call(?MODULE, gossip, [off, Node]);
slave_gossip(on, Node) ->
	slave_call(?MODULE, gossip, [on, Node]).

slave_add_tx(Node, TX) ->
	slave_call(ar_node, add_tx, [Node, TX]).

slave_mine(Node) ->
	slave_call(ar_node, mine, [Node]).

wait_until_height(Node, TargetHeight) ->
	{ok, BI} = ar_util:do_until(
		fun() ->
			case ar_node:get_blocks(Node) of
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

slave_wait_until_height(Node, TargetHeight) ->
	slave_call(?MODULE, wait_until_height, [Node, TargetHeight]).

assert_slave_wait_until_height(Node, TargetHeight) ->
	BI = slave_call(?MODULE, wait_until_height, [Node, TargetHeight]),
	?assert(is_list(BI)),
	BI.

assert_wait_until_block_block_index(Node, BI) ->
	?assertEqual(ok, wait_until_block_block_index(Node, BI)).

wait_until_block_block_index(Node, BI) ->
	ar_util:do_until(
		fun() ->
			case ar_node:get_blocks(Node) of
				BI ->
					ok;
				_ ->
					false
			end
		end,
		100,
		60 * 1000
	).

assert_wait_until_receives_txs(Node, TXs) ->
	?assertEqual(ok, wait_until_receives_txs(Node, TXs)).

wait_until_receives_txs(Node, TXs) ->
	ar_util:do_until(
		fun() ->
			MinedTXs = ar_node:get_mined_txs(Node),
			case lists:all(fun(TX) -> lists:member(TX, MinedTXs) end, TXs) of
				true ->
					ok;
				_ ->
					false
			end
		end,
		100,
		10 * 1000
	).

assert_slave_wait_until_receives_txs(Node, TXs) ->
	?assertEqual(ok, slave_call(?MODULE, wait_until_receives_txs, [Node, TXs])).

post_tx_to_slave(Slave, TX) ->
	SlavePort = slave_call(ar_meta_db, get, [port]),
	SlaveIP = {127, 0, 0, 1, SlavePort},
	Reply =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[{<<"X-P2p-Port">>, integer_to_binary(SlavePort)}],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
		),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
			assert_slave_wait_until_receives_txs(Slave, [TX]);
		_ ->
			ar:console(
				"Failed to post transaction. Error DB entries: ~p~n",
				[slave_call(ar_tx_db, get_error_codes, [TX#tx.id])]
			),
			noop
	end,
	Reply.

assert_post_tx_to_slave(Slave, TX) ->
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_slave(Slave, TX).

assert_post_tx_to_master(Master, TX) ->
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_master(Master, TX).

post_tx_to_master(Master, TX) ->
	Port = ar_meta_db:get(port),
	MasterIP = {127, 0, 0, 1, Port},
	Reply =
		ar_httpc:request(
			<<"POST">>,
			MasterIP,
			"/tx",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
		),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
			assert_wait_until_receives_txs(Master, [TX]);
		_ ->
			ar:console(
				"Failed to post transaction. Error DB entries: ~p~n",
				[ar_tx_db:get_error_codes(TX#tx.id)]
			),
			noop
	end,
	Reply.

sign_tx(Wallet) ->
	sign_tx(slave, Wallet, #{}, fun ar_tx:sign/2).

sign_tx(Wallet, TXParams) ->
	sign_tx(slave, Wallet, TXParams, fun ar_tx:sign/2).

sign_tx(Node, Wallet, TXParams) ->
	sign_tx(Node, Wallet, TXParams, fun ar_tx:sign/2).

sign_tx_pre_fork_2_0(Wallet) ->
	sign_tx(slave, Wallet, #{}, fun ar_tx:sign_pre_fork_2_0/2).

sign_tx_pre_fork_2_0(Wallet, TXParams) ->
	sign_tx(slave, Wallet, TXParams, fun ar_tx:sign_pre_fork_2_0/2).

sign_tx_pre_fork_2_0(Node, Wallet, TXParams) ->
	sign_tx(Node, Wallet, TXParams, fun ar_tx:sign_pre_fork_2_0/2).

sign_tx(Node, Wallet, TXParams, SignFun) ->
	{_, Pub} = Wallet,
	Data = maps:get(data, TXParams, <<>>),
	Reward = case maps:get(reward, TXParams, none) of
		none ->
			get_tx_price(Node, byte_size(Data));
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
			data_size = byte_size(Data)
		},
		Wallet
	).

get_tx_anchor() ->
	get_tx_anchor(slave).

get_tx_anchor(slave) ->
	SlavePort = slave_call(ar_meta_db, get, [port]),
	IP = {127, 0, 0, 1, SlavePort},
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/tx_anchor",
			[{<<"X-P2p-Port">>, integer_to_binary(SlavePort)}]
		),
	ar_util:decode(Reply);
get_tx_anchor(master) ->
	Port = ar_meta_db:get(port),
	IP = {127, 0, 0, 1, Port},
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/tx_anchor",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		),
	ar_util:decode(Reply).

get_last_tx(Key) ->
	get_last_tx(slave, Key).

get_last_tx(slave, {_, Pub}) ->
	Port = slave_call(ar_meta_db, get, [port]),
	IP = {127, 0, 0, 1, Port},
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub))) ++ "/last_tx",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		),
	ar_util:decode(Reply);
get_last_tx(master, {_, Pub}) ->
	Port = ar_meta_db:get(port),
	IP = {127, 0, 0, 1, Port},
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub))) ++ "/last_tx",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		),
	ar_util:decode(Reply).

get_tx_confirmations(slave, TXID) ->
	Port = slave_call(ar_meta_db, get, [port]),
	IP = {127, 0, 0, 1, Port},
	Response =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/status",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Reply, _, _}} ->
			{Status} = ar_serialize:dejsonify(Reply),
			lists:keyfind(<<"number_of_confirmations">>, 1, Status);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			-1
	end;
get_tx_confirmations(master, TXID) ->
	Port = ar_meta_db:get(port),
	IP = {127, 0, 0, 1, Port},
	Response =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/status",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Reply, _, _}} ->
			{Status} = ar_serialize:dejsonify(Reply),
			lists:keyfind(<<"number_of_confirmations">>, 1, Status);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			-1
	end.

get_balance(Pub) ->
	Port = slave_call(ar_meta_db, get, [port]),
	IP = {127, 0, 0, 1, Port},
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			IP,
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub))) ++ "/balance",
			[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		),
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
	get_tx_price(slave, DataSize).

get_tx_price(Node, DataSize) ->
	{IP, Port} = case Node of
		slave ->
			P = slave_call(ar_meta_db, get, [port]),
			{{127, 0, 0, 1, P}, P};
		master ->
			P = ar_meta_db:get(port),
			{{127, 0, 0, 1, P}, P}
	end,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} = case Node of
		slave ->
			ar_httpc:request(
				<<"GET">>,
				IP,
				"/price/" ++ integer_to_binary(DataSize),
				[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
			);
		master ->
			ar_httpc:request(
				<<"GET">>,
				IP,
				"/price/" ++ integer_to_binary(DataSize),
				[{<<"X-P2p-Port">>, integer_to_binary(Port)}]
			)
	end,
	binary_to_integer(Reply).
