-module(ar_test_node).

-export([start/1, start/2, slave_start/1, connect_to_slave/0, slave_call/3, slave_call/4]).
-export([gossip/2, slave_gossip/2, slave_add_tx/2, slave_mine/1]).
-export([wait_until_height/2, slave_wait_until_height/2]).
-export([wait_until_block_hash_list/2]).
-export([assert_wait_until_block_hash_list/2]).
-export([wait_until_receives_txs/2]).
-export([assert_wait_until_receives_txs/2]).
-export([assert_slave_wait_until_receives_txs/2]).
-export([post_tx_to_slave/2]).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

start(no_block) ->
	[B0] = ar_weave:init([]),
	start(B0);
start(B0) ->
	start(B0, {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}).

slave_start(no_block) ->
	[B0] = ar_weave:init([]),
	slave_start(B0);
slave_start(B0) ->
	slave_call(?MODULE, start, [B0, {127, 0, 0, 1, ar_meta_db:get(port)}]).

start(B0, Peer) ->
	ar_storage:clear(),
	Node = ar_node:start([], [B0]),
	ar_http_iface_server:reregister(http_entrypoint_node, Node),
	ar_meta_db:reset_peer(Peer),
	Bridge = ar_bridge:start([], Node, ar_meta_db:get(port)),
	ar_http_iface_server:reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	{Node, B0}.

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
		slave_call(
			ar_httpc,
			request,
			[
				<<"GET">>,
				{127, 0, 0, 1, MasterPort},
				"/info",
				[{<<"X-P2p-Port">>, integer_to_binary(SlavePort)}]
			]
		).

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
	{ok, BHL} = ar_util:do_until(
		fun() ->
			case ar_node:get_blocks(Node) of
				BHL when length(BHL) - 1 == TargetHeight ->
					{ok, BHL};
				_ ->
					false
			end
		end,
		100,
		60 * 1000
	),
	BHL.

slave_wait_until_height(Node, TargetHeight) ->
	slave_call(?MODULE, wait_until_height, [Node, TargetHeight]).

assert_wait_until_block_hash_list(Node, BHL) ->
	?assertEqual(ok, wait_until_block_hash_list(Node, BHL)).

wait_until_block_hash_list(Node, BHL) ->
	ar_util:do_until(
		fun() ->
			case ar_node:get_blocks(Node) of
				BHL ->
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
			KnownTXs = ar_node:get_all_known_txs(Node),
			case lists:all(fun(TX) -> lists:member(TX, KnownTXs) end, TXs) of
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
	SlaveIP = {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
	Reply =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
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
