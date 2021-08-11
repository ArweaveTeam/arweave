-module(ar_fork_recovery_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
	start/1, slave_start/1, connect_to_slave/0, disconnect_from_slave/0,
	slave_mine/0,
	wait_until_height/1, slave_wait_until_height/1, assert_slave_wait_until_receives_txs/1,
	sign_tx/2, slave_add_tx/1,
	read_block_when_stored/1,
	slave_call/3
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

orphaned_txs_are_remined_after_fork_recovery_test_() ->
	{timeout, 120, fun test_orphaned_txs_are_remined_after_fork_recovery/0}.

test_orphaned_txs_are_remined_after_fork_recovery() ->
	%% Mine a transaction on slave, mine two blocks on master to
	%% make the transaction orphaned. Mine a block on slave and
	%% assert the transaction is re-mined.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{_SlaveNode, _} = slave_start(B0),
	{_MasterNode, _} = start(B0),
	disconnect_from_slave(),
	TX = #tx{ id = TXID } = sign_tx(Key, #{}),
	slave_add_tx(TX),
	assert_slave_wait_until_receives_txs([TX]),
	slave_mine(),
	[{H1, _, _} | _] = slave_wait_until_height(1),
	H1TXIDs = (slave_call(ar_test_node, read_block_when_stored, [H1]))#block.txs,
	?assertEqual([TXID], H1TXIDs),
	connect_to_slave(),
	ar_node:mine(),
	[{H2, _, _} | _] = wait_until_height(1),
	ar_node:mine(),
	[{H3, _, _}, {H2, _, _}, {_, _, _}] = wait_until_height(2),
	?assertMatch([{H3, _, _}, {H2, _, _}, {_, _, _}], slave_wait_until_height(2)),
	slave_mine(),
	[{H4, _, _} | _] = slave_wait_until_height(3),
	H4TXIDs = (slave_call(ar_test_node, read_block_when_stored, [H4]))#block.txs,
	?assertEqual([TXID], H4TXIDs).

invalid_block_with_high_cumulative_difficulty_test_() ->
	{timeout, 30, fun test_invalid_block_with_high_cumulative_difficulty/0}.

test_invalid_block_with_high_cumulative_difficulty() ->
	%% Submit an alternative fork with valid blocks weaker than the tip and
	%% an invalid block on top, much stronger than the tip. Make sure the node
	%% ignores the invalid block and continues to build on top of the valid fork.
	{_SlaveNode, B0} = slave_start(no_block),
	{_MasterNode, B0} = start(B0),
	disconnect_from_slave(),
	slave_mine(),
	[{H1, _, _} | _] = slave_wait_until_height(1),
	connect_to_slave(),
	ar_node:mine(),
	[{H2, _, _} | _] = wait_until_height(1),
	?assertNotEqual(H2, H1),
	B1 = read_block_when_stored(H2),
	{B2, BDS} = fake_block_with_strong_cumulative_difficulty(B1, 10000000000000000),
	?assertMatch(
	    {ok, {{<<"200">>, _}, _, _, _, _}},
	    ar_http_iface_client:send_new_block({127, 0, 0, 1, 1984}, B2, BDS)
	),
	ar_node:mine(),
	%% Assert the nodes have continued building on the original fork.
	[{H3, _, _} | _] = slave_wait_until_height(2),
	?assertNotEqual(B2#block.indep_hash, H3),
	{_, B3} = ar_http_iface_client:get_block_shadow([{127, 0, 0, 1, 1983}], 1),
	?assertEqual(H2, B3#block.indep_hash).

fake_block_with_strong_cumulative_difficulty(B, CDiff) ->
	#block{
	    indep_hash = H,
	    height = Height,
	    timestamp = Timestamp,
	    nonce = Nonce,
	    poa = #poa{ chunk = Chunk }
	} = B,
	B2 =
	    B#block{
	        height = Height + 1,
	        cumulative_diff = CDiff,
	        previous_block = H,
	        hash_list_merkle = ar_block:compute_hash_list_merkle(B)
	    },
	BDS = ar_block:generate_block_data_segment(B2),
	H0 = ar_weave:hash(BDS, Nonce, Height + 1),
	B3 = B2#block{ hash = ar_mine:spora_solution_hash(H, Timestamp, H0, Chunk, Height + 1) },
	{B3#block{ indep_hash = ar_weave:indep_hash(B3) }, BDS}.
