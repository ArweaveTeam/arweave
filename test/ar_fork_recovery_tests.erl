-module(ar_fork_recovery_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

missing_txs_fork_recovery_test() ->
	%% Mine two blocks with transactions on the slave node but do not gossip the transactions in advance.
	%% The master node should reject the first block, but accept the second one and fork recover.
	%%
	%% Start a remote node.
	Peer = {127, 0, 0, 1, ar_meta_db:get(port)},
	{SlaveNode, B0} = ar_rpc:call(slave, ar_test_node, start, [no_block, Peer], 5000),
	%% Start a local node and connect to slave.
	{MasterNode, B0} = ar_test_node:start(B0, {127, 0, 0, 1, ar_rpc:call(slave, ar_meta_db, get, [port], 5000)}),
	ar_test_node:connect_to_slave(),
	%% Turn off gossip and add a TX to the slave.
	ar_test_node:slave_gossip(off, SlaveNode),
	TX1 = ar_tx:new(),
	ar_test_node:slave_add_tx(SlaveNode, TX1),
	%% Turn on gossip and mine a block.
	ar_test_node:slave_gossip(on, SlaveNode),
	?assertEqual([TX1], ar_rpc:call(slave, ar_node, get_all_known_txs, [SlaveNode], 5000)),
	?assertEqual([], ar_node:get_all_known_txs(MasterNode)),
	ar_test_node:slave_mine(SlaveNode),
	%% Expect the local node to reject the block.
	?assertEqual(1, length(ar_node:get_hash_list(MasterNode))),
	%% Turn off gossip again and add the second TX.
	ar_test_node:slave_gossip(off, SlaveNode),
	TX2 = ar_tx:new(),
	ar_test_node:slave_add_tx(SlaveNode, TX2),
	%% Turn on gossip and mine a block.
	ar_test_node:slave_gossip(on, SlaveNode),
	?assertEqual([TX2], ar_rpc:call(slave, ar_node, get_all_known_txs, [SlaveNode], 5000)),
	?assertEqual([], ar_node:get_all_known_txs(MasterNode)),
	ar_test_node:slave_mine(SlaveNode),
	%% Expect the local node to fork recover.
	ok = ar_util:do_until(
		fun() ->
			case ar_node:get_hash_list(MasterNode) of
				[_H1, _H2, _H3|_Rest] ->
					ok;
				_ ->
					not_ok
			end
		end,
		10,
		5000
	).
