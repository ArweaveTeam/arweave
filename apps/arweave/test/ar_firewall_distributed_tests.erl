-module(ar_firewall_distributed_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, slave_mine/1]).
-import(ar_test_node, [assert_slave_wait_until_receives_txs/2]).
-import(ar_test_node, [wait_until_height/2, assert_slave_wait_until_height/2]).

node_validates_blocks_with_rejected_tx_test_() ->
	{timeout, 60, fun test_node_validates_blocks_with_rejected_tx/0}.

test_node_validates_blocks_with_rejected_tx() ->
	%% Start a remote node.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	{SlaveNode, _} = slave_start(B0),
	{Node, _} = start(B0),
	ar_node:mine(Node),
	wait_until_height(Node, 1),
	slave_mine(SlaveNode),
	assert_slave_wait_until_height(SlaveNode, 1),
	%% Post the first tx to the remote node. This should also make the second node peer with the first one.
	TX1 = ar_tx:sign_pre_fork_2_0(
		(ar_tx:new())#tx{ data = <<"BADCONTENT1">>, owner = Pub, reward = ?AR(1) },
		Key
	),
	SlavePort = ar_rpc:call(slave, ar_meta_db, get, [port], 5000),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, SlavePort},
			"/tx",
			[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX1))
		),
	assert_slave_wait_until_receives_txs(SlaveNode, [TX1]),
	%% Start a local node.
	%% Configure the firewall to reject one of the txs submitted to the remote node.
	ar_meta_db:put(content_policy_files, [filename:dirname(?FILE) ++ "/test_sig.txt"]),
	ar_firewall:reload(),
	%% Mine the tx into a block on the remote node.
	slave_mine(SlaveNode),
	assert_slave_wait_until_height(SlaveNode, 2),
	timer:sleep(1000),
	%% Expect the local node to reject the block.
	?assertEqual(2, length(ar_node:get_block_index(Node))),
	%% Post the second tx to the remote node.
	TX2 = ar_tx:sign_pre_fork_2_0(
		(ar_tx:new())#tx{ data = <<"GOOD CONTENT">>, owner = Pub, reward = ?AR(1), last_tx = TX1#tx.id },
		Key
	),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, SlavePort},
			"/tx",
			[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX2))
		),
	assert_slave_wait_until_receives_txs(SlaveNode, [TX2]),
	%% Mine the second tx into a block.
	slave_mine(SlaveNode),
	%% Expect the local node to fork recover to the block.
	wait_until_height(Node, 3),
	%% Expect the local node to store the last block and the valid tx.
	[{H3, _} | _] = BI3 = ar_node:get_block_index(Node),
	B2 = ar_storage:read_block(H3, BI3),
	[B2TX] = B2#block.txs,
	?assertEqual(TX2#tx.id, (ar_storage:read_tx(B2TX))#tx.id),
	%% Expect the local node to store the previous block without the invalid tx.
	[_ | [{H2, _} | _] = BI2] = BI3,
	B1 = ar_storage:read_block(H2, BI2),
	[B1TX] = B1#block.txs,
	?assertEqual(unavailable, ar_storage:read_tx(B1TX)),
	%% Expect the remote node to store both transactions.
	RemoteB2 = ar_rpc:call(slave, ar_storage, read_block, [H2, BI2], 5000),
	[RemoteTX2] = RemoteB2#block.txs,
	?assertEqual(RemoteTX2, (ar_rpc:call(slave, ar_storage, read_tx, [RemoteTX2], 5000))#tx.id),
	RemoteB1 = ar_rpc:call(slave, ar_storage, read_block, [H2, BI2], 5000),
	[RemoteTX1] = RemoteB1#block.txs,
	?assertEqual(RemoteTX1, (ar_rpc:call(slave, ar_storage, read_tx, [RemoteTX1], 5000))#tx.id).
