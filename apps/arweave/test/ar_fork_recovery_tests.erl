-module(ar_fork_recovery_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
	start/1, slave_start/1, connect_to_slave/0, disconnect_from_slave/0,
	slave_mine/1, mine_until_fork/3,
	wait_until_height/2, slave_wait_until_height/2,
	sign_tx/2, slave_add_tx/2, assert_slave_wait_until_receives_txs/2,
	read_block_when_stored/1
]).

height_plus_one_fork_recovery_test_() ->
	{timeout, 20, fun test_height_plus_one_fork_recovery/0}.

test_height_plus_one_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine an extra block on one of them.
	%% Expect the other one to recover. Repeat.
	{SlaveNode, B0} = slave_start(no_block),
	{MasterNode, B0} = start(B0),
	connect_to_slave(),
	Height = mine_until_fork(MasterNode, SlaveNode, 0),
	ar_node:mine(MasterNode),
	MasterBI = wait_until_height(MasterNode, Height + 1),
	?assertEqual(MasterBI, slave_wait_until_height(SlaveNode, Height + 1)),
	Height2 = mine_until_fork(MasterNode, SlaveNode, Height + 1),
	slave_mine(SlaveNode),
	SlaveBI = ar_test_node:slave_wait_until_height(SlaveNode, Height2 + 1),
	?assertEqual(SlaveBI, wait_until_height(MasterNode, Height2 + 1)).

height_plus_three_fork_recovery_test_() ->
	{timeout, 20, fun test_height_plus_three_fork_recovery/0}.

test_height_plus_three_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine three extra blocks on one of them.
	%% Expect the other one to recover.
	{SlaveNode, B0} = slave_start(no_block),
	{MasterNode, B0} = start(B0),
	connect_to_slave(),
	Height = mine_until_fork(MasterNode, SlaveNode, 0),
	disconnect_from_slave(),
	slave_mine(SlaveNode),
	slave_wait_until_height(SlaveNode, Height + 1),
	connect_to_slave(),
	ar_node:mine(MasterNode),
	wait_until_height(MasterNode, Height + 1),
	disconnect_from_slave(),
	slave_mine(SlaveNode),
	slave_wait_until_height(SlaveNode, Height + 2),
	connect_to_slave(),
	ar_node:mine(MasterNode),
	wait_until_height(MasterNode, Height + 2),
	ar_node:mine(MasterNode),
	MasterBI = wait_until_height(MasterNode, Height + 3),
	?assertEqual(MasterBI, slave_wait_until_height(SlaveNode, Height + 3)).

missing_txs_fork_recovery_test_() ->
	{timeout, 20, fun test_missing_txs_fork_recovery/0}.

test_missing_txs_fork_recovery() ->
	%% Mine two blocks with transactions on the slave node
	%% but do not gossip the transactions. The master node
	%% is expected fetch the missing transactions and apply the block.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{SlaveNode, _} = slave_start(B0),
	{MasterNode, _} = start(B0),
	TX1 = sign_tx(Key, #{}),
	slave_add_tx(SlaveNode, TX1),
	assert_slave_wait_until_receives_txs(SlaveNode, [TX1]),
	connect_to_slave(),
	?assertEqual([], ar_node:get_pending_txs(MasterNode)),
	slave_mine(SlaveNode),
	[{H1, _, _} | _] = wait_until_height(MasterNode, 1),
	?assertEqual(1, length((read_block_when_stored(H1))#block.txs)).
