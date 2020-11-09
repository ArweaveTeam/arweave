-module(ar_firewall_distributed_tests).

-include_lib("arweave/src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, slave_mine/0, connect_to_slave/0]).
-import(ar_test_node, [assert_slave_wait_until_receives_txs/1]).
-import(ar_test_node, [wait_until_height/1, assert_slave_wait_until_height/1]).
-import(ar_test_node, [sign_v1_tx/2, assert_post_tx_to_slave/1]).

node_validates_blocks_with_rejected_tx_test_() ->
	{timeout, 60, fun test_node_validates_blocks_with_rejected_tx/0}.

test_node_validates_blocks_with_rejected_tx() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	{_SlaveNode, _} = slave_start(B0),
	{_Node, _} = start(B0),
	connect_to_slave(),
	%% Post a good tx and a bad tx to the remote node. The signature
	%% for the "bad" transaction is configured in the test fixture.
	BadTX = sign_v1_tx(Key, #{ data => <<"BADCONTENT1">> }),
	assert_post_tx_to_slave(BadTX),
	GoodTX = sign_v1_tx(
		Key,
		#{ data => <<"GOOD CONTENT">>, last_tx => B0#block.indep_hash }
	),
	assert_post_tx_to_slave(GoodTX),
	assert_slave_wait_until_receives_txs([GoodTX, BadTX]),
	%% Configure the firewall to reject one of the txs submitted to the remote node.
	ar_meta_db:put(
		content_policy_files,
		[filename:dirname(?FILE) ++ "/test_sig.txt"]
	),
	ar_firewall:reload(),
	%% Mine the txs into a block on the remote node.
	slave_mine(),
	assert_slave_wait_until_height(1),
	wait_until_height(1),
	%% Expect the local node to store the last block and the valid tx.
	[{H, _, _} | _] = BI = ar_node:get_block_index(),
	B = ar_storage:read_block(H),
	[BadTXID, GoodTXID] = B#block.txs,
	?assertEqual(GoodTX#tx.id, (ar_storage:read_tx(GoodTXID))#tx.id),
	%% Expect the local node to not store the invalid tx.
	?assertEqual(unavailable, ar_storage:read_tx(BadTXID)),
	%% Expect the remote node to store both transactions.
	RemoteB = ar_rpc:call(slave, ar_storage, read_block, [H, BI], 5000),
	[BadTXID, GoodTXID] = RemoteB#block.txs,
	?assertEqual(
		BadTXID,
		(ar_rpc:call(slave, ar_storage, read_tx, [BadTXID], 5000))#tx.id
	),
	?assertEqual(
		GoodTXID,
		(ar_rpc:call(slave, ar_storage, read_tx, [GoodTXID], 5000))#tx.id
	).
