-module(ar_test_node).

-export([start/2, connect_to_slave/0, slave_run/3, slave_run/4]).
-export([slave_gossip/2, slave_add_tx/2, slave_mine/1]).

start(no_block, Peer) ->
	[B0] = ar_weave:init([]),
	start(B0, Peer);
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
	%% Connect the nodes by making an HTTP call.
	SlavePort = slave_run(ar_meta_db, get, [port]),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, SlavePort},
			"/info",
			[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}]
		).

slave_run(Module, Function, Args) ->
	slave_run(Module, Function, Args, 5000).

slave_run(Module, Function, Args, Timeout) ->
	ar_rpc:call(slave, Module, Function, Args, Timeout).

slave_gossip(on, Node) ->
	slave_run(ar_node, set_loss_probability, [Node, 0]);
slave_gossip(off, Node) ->
	slave_run(ar_node, set_loss_probability, [Node, 1]).

slave_add_tx(Node, TX) ->
	slave_run(ar_node, add_tx, [Node, TX]).

slave_mine(Node) ->
	slave_run(ar_node, mine, [Node]),
	timer:sleep(100).
