-module(ar_multiple_txs_per_wallet_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0, slave_mine/1, gossip/2]).
-import(ar_test_node, [wait_until_receives_txs/2, slave_wait_until_receives_txs/2]).
-import(ar_test_node, [wait_until_height/2, slave_wait_until_height/2, slave_call/3]).

-import(ar_test_fork, [test_on_fork/3]).

accepts_gossips_and_mines_test_() ->
	test_on_fork(height_1_8, 0, fun accepts_gossips_and_mines/0).

keeps_txs_after_new_block_test_() ->
	test_on_fork(height_1_8, 0, fun keeps_txs_after_new_block/0).

returns_error_when_txs_exceed_balance_test_() ->
	test_on_fork(height_1_8, 0, fun returns_error_when_txs_exceed_balance/0).

accepts_gossips_and_mines() ->
	%% Post two transactions from the same wallet to a node.
	%% Expect them to be accepted, gossiped to the peer and included into the block.
	%% Expect the block to be accepted by the peer.
	%%
	%% Create a genesis block with a wallet.
	{Priv, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	%% Start the nodes.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Post two transactions from the same wallet to slave.
	TX1 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1) },
	SignedTX1 = ar_tx:sign(TX1, {Priv, Pub}),
	TX2 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = SignedTX1#tx.id },
	SignedTX2 = ar_tx:sign(TX2, {Priv, Pub}),
	SlaveIP = {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX1))
		),
	slave_wait_until_receives_txs(Slave, [SignedTX1]),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX2))
		),
	slave_wait_until_receives_txs(Slave, [SignedTX1, SignedTX2]),
	%% Expect both transactions to be gossiped to master.
	wait_until_receives_txs(Master, [SignedTX1, SignedTX2]),
	%% Mine a block.
	slave_mine(Slave),
	%% Expect both transactions to be included into block.
	SlaveBHL = slave_wait_until_height(Slave, 1),
	?assertEqual(
		[SignedTX1#tx.id, SignedTX2#tx.id],
		(slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]))#block.txs
	),
	?assertEqual(SignedTX1, slave_call(ar_storage, read_tx, [SignedTX1#tx.id])),
	?assertEqual(SignedTX2, slave_call(ar_storage, read_tx, [SignedTX2#tx.id])),
	%% Expect the block to be accepted by master.
	BHL = wait_until_height(Master, 1),
	?assertEqual(
		[SignedTX1#tx.id, SignedTX2#tx.id],
		(ar_storage:read_block(hd(BHL), BHL))#block.txs
	),
	?assertEqual(SignedTX1, ar_storage:read_tx(SignedTX1#tx.id)),
	?assertEqual(SignedTX2, ar_storage:read_tx(SignedTX2#tx.id)).

keeps_txs_after_new_block() ->
	%% Post two transactions from the same wallet to a node but do not gossip them.
	%% Mine an empty block on a different node and gossip it to the node with transactions.
	%% Expect the block to be accepted.
	%% Expect both transactions to be kept in the mempool.
	%% Mine a block on the node with transactions, expect them to be included into the block.
	%%
	%% Create a genesis block with a wallet.
	{Priv, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	%% Start the nodes.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Disable gossip so that slave does not receive transactions.
	gossip(off, Master),
	%% Post two transactions from the same wallet to master.
	TX1 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1) },
	SignedTX1 = ar_tx:sign(TX1, {Priv, Pub}),
	TX2 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = SignedTX1#tx.id },
	SignedTX2 = ar_tx:sign(TX2, {Priv, Pub}),
	MasterIP = {127, 0, 0, 1, ar_meta_db:get(port)},
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			MasterIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX1))
		),
	wait_until_receives_txs(Master, [SignedTX1]),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			MasterIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX2))
		),
	wait_until_receives_txs(Master, [SignedTX1, SignedTX2]),
	?assertEqual([], slave_call(ar_node, get_all_known_txs, [Slave])),
	%% Enable gossip and mine an empty block on slave.
	gossip(on, Master),
	slave_mine(Slave),
	%% Expect master to receive the block.
	BHL = wait_until_height(Master, 1),
	?assertEqual([], (ar_storage:read_block(hd(BHL), BHL))#block.txs),
	%% Expect master to still have two transactions in the mempool.
	wait_until_receives_txs(Master, [SignedTX1, SignedTX2]),
	%% Mine a block on master and expect both transactions to be included.
	ar_node:mine(Master),
	BHL2 = wait_until_height(Master, 2),
	?assertEqual(
		[SignedTX1#tx.id, SignedTX2#tx.id],
		(ar_storage:read_block(hd(BHL2), BHL2))#block.txs
	).

returns_error_when_txs_exceed_balance() ->
	%% Create a genesis block with a wallet.
	{Priv, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	%% Start the nodes.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Post two transactions spending exactly the amount available.
	TX1 = (ar_tx:new())#tx{ owner = Pub, quantity = ?AR(4), reward = ?AR(6) },
	SignedTX1 = ar_tx:sign(TX1, {Priv, Pub}),
	TX2 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(10), last_tx = SignedTX1#tx.id },
	SignedTX2 = ar_tx:sign(TX2, {Priv, Pub}),
	SlaveIP = {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX1))
		),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX2))
		),
	TX3 = (ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = SignedTX2#tx.id },
	SignedTX3 = ar_tx:sign(TX3, {Priv, Pub}),
	%% Post the third one and expect the balance exceeded error.
	{ok, {{<<"400">>, _}, _, <<"Waiting TXs exceed balance for wallet.">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX3))
		),
	%% Expect the first two to be included into the block.
	slave_mine(Slave),
	SlaveBHL = slave_wait_until_height(Slave, 1),
	?assertEqual(
		[SignedTX1#tx.id, SignedTX2#tx.id],
		(slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]))#block.txs
	),
	BHL = wait_until_height(Master, 1),
	?assertEqual(
		[SignedTX1#tx.id, SignedTX2#tx.id],
		(ar_storage:read_block(hd(BHL), BHL))#block.txs
	).
