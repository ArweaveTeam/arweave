-module(ar_fork_recovery_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, start/2, slave_start/1, slave_call/3]).
-import(ar_test_node, [assert_slave_wait_until_receives_txs/2]).

missing_txs_fork_recovery_test() ->
	%% Mine two blocks with transactions on the slave node but do not gossip the transactions in advance.
	%% The master node should reject the first block, but accept the second one and fork recover.
	%%
	%% Start a remote node.
	{SlaveNode, B0} = slave_start(no_block),
	%% Start a local node and connect to slave.
	{MasterNode, B0} = start(B0),
	ar_test_node:connect_to_slave(),
	%% Turn off gossip and add a TX to the slave.
	ar_test_node:slave_gossip(off, SlaveNode),
	TX1 = ar_tx:new(),
	ar_test_node:slave_add_tx(SlaveNode, TX1),
	assert_slave_wait_until_receives_txs(SlaveNode, [TX1]),
	%% Turn on gossip and mine a block.
	ar_test_node:slave_gossip(on, SlaveNode),
	?assertEqual([], ar_node:get_all_known_txs(MasterNode)),
	ar_test_node:slave_mine(SlaveNode),
	timer:sleep(1000),
	%% Expect the local node to reject the block.
	?assertEqual(1, length(ar_node:get_hash_list(MasterNode))),
	%% Turn off gossip again and add the second TX.
	ar_test_node:slave_gossip(off, SlaveNode),
	TX2 = ar_tx:new(),
	ar_test_node:slave_add_tx(SlaveNode, TX2),
	assert_slave_wait_until_receives_txs(SlaveNode, [TX2]),
	%% Turn on gossip and mine a block.
	ar_test_node:slave_gossip(on, SlaveNode),
	?assertEqual([], ar_node:get_all_known_txs(MasterNode)),
	ar_test_node:slave_mine(SlaveNode),
	%% Expect the local node to fork recover.
	ar_test_node:wait_until_height(MasterNode, 2).

recall_block_missing_multiple_txs_fork_recovery_test() ->
	%% Create a genesis block with two transactions but do not store them on the master node.
	%% Mine a block with two transactions on the slave node without gossiping them.
	%% Mine an empty block on the slave and gossip it.
	%% The master is expected to reject the first block and successfully fork recover
	%% to the second although the recall block misses transactions in this scenario.
	%%
	%% Create a genesis block with two transactions.
	GenesisTXs = [ar_tx:new(), ar_tx:new()],
	[EmptyB] = ar_weave:init([]),
	B = EmptyB#block{ txs = GenesisTXs },
	B0 = B#block { indep_hash = ar_weave:indep_hash(B) },
	%% Start a remote node.
	{SlaveNode, _} = slave_start(B0),
	%% Start a local node and connect to slave. Do not use start/1 to avoid writing txs to disk.
	{MasterNode, _} = start(B0, {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}),
	ar_test_node:connect_to_slave(),
	%% Turn the gossip off and add two transactions to the slave node.
	ar_test_node:slave_gossip(off, SlaveNode),
	{_, Pub} = ar_wallet:new(),
	ar_test_node:slave_add_tx(
		SlaveNode,
		(ar_tx:new())#tx{ owner = ar_wallet:to_address(Pub) }
	),
	{_, AnotherPub} = ar_wallet:new(),
	ar_test_node:slave_add_tx(
		SlaveNode,
		(ar_tx:new())#tx{ owner = ar_wallet:to_address(AnotherPub) }
	),
	?assertEqual(2, length(slave_call(ar_node, get_all_known_txs, [SlaveNode]))),
	%% Turn the gossip back on and mine a block on the slave.
	ar_test_node:slave_gossip(on, SlaveNode),
	ar_test_node:slave_mine(SlaveNode),
	ar_test_node:slave_wait_until_height(SlaveNode, 1),
	%% Expect it to reject the block since it misses transactions.
	?assertEqual([B0#block.indep_hash], ar_node:get_hash_list(MasterNode)),
	%% Mine another one. Its recall block would be either 0 or 1 - both have two txs,
	%% neither of those txs are known by master.
	ar_test_node:slave_mine(SlaveNode),
	FinalBHL = ar_test_node:slave_wait_until_height(SlaveNode, 2),
	%% Expect the master node to recover.
	ar_test_node:assert_wait_until_block_hash_list(MasterNode, FinalBHL).
