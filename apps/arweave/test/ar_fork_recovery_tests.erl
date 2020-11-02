-module(ar_fork_recovery_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
	start/1, slave_start/1, connect_to_slave/0, disconnect_from_slave/0,
	slave_mine/0,
	wait_until_height/1, slave_wait_until_height/1,
	sign_tx/2, slave_add_tx/1,
	read_block_when_stored/1
]).

height_plus_one_fork_recovery_test_() ->
	{timeout, 20, fun test_height_plus_one_fork_recovery/0}.

test_height_plus_one_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine an extra block on one of them.
	%% Expect the other one to recover.
	{_SlaveNode, B0} = slave_start(no_block),
	{_MasterNode, B0} = start(B0),
	slave_mine(),
	slave_wait_until_height(1),
	connect_to_slave(),
	ar_node:mine(),
	wait_until_height(1),
	ar_node:mine(),
	MasterBI = wait_until_height(2),
	?assertEqual(MasterBI, slave_wait_until_height(2)),
	disconnect_from_slave(),
	ar_node:mine(),
	wait_until_height(3),
	connect_to_slave(),
	slave_mine(),
	slave_wait_until_height(3),
	slave_mine(),
	SlaveBI = slave_wait_until_height(4),
	?assertEqual(SlaveBI, wait_until_height(4)).

height_plus_three_fork_recovery_test_() ->
	{timeout, 20, fun test_height_plus_three_fork_recovery/0}.

test_height_plus_three_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine three extra blocks on one of them.
	%% Expect the other one to recover.
	{_SlaveNode, B0} = slave_start(no_block),
	{_MasterNode, B0} = start(B0),
	slave_mine(),
	slave_wait_until_height(1),
	connect_to_slave(),
	ar_node:mine(),
	wait_until_height(1),
	disconnect_from_slave(),
	slave_mine(),
	slave_wait_until_height(2),
	connect_to_slave(),
	ar_node:mine(),
	wait_until_height(2),
	disconnect_from_slave(),
	slave_mine(),
	slave_wait_until_height(3),
	connect_to_slave(),
	ar_node:mine(),
	wait_until_height(3),
	ar_node:mine(),
	MasterBI = wait_until_height(4),
	?assertEqual(MasterBI, slave_wait_until_height(4)).

missing_txs_fork_recovery_test_() ->
	{timeout, 120, fun test_missing_txs_fork_recovery/0}.

test_missing_txs_fork_recovery() ->
	%% Mine two blocks with transactions on the slave node
	%% but do not gossip the transactions. The master node
	%% is expected fetch the missing transactions and apply the block.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{_SlaveNode, _} = slave_start(B0),
	{_MasterNode, _} = start(B0),
	TX1 = sign_tx(Key, #{}),
	slave_add_tx(TX1),
	assert_slave_wait_until_receives_txs([TX1]),
	connect_to_slave(),
	?assertEqual([], ar_node:get_pending_txs()),
	slave_mine(),
	[{H1, _, _} | _] = wait_until_height(1),
	?assertEqual(1, length((read_block_when_stored(H1))#block.txs)).
