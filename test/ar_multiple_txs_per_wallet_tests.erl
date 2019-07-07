-module(ar_multiple_txs_per_wallet_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0]).
-import(ar_test_node, [slave_mine/1, gossip/2]).
-import(ar_test_node, [assert_wait_until_receives_txs/2]).
-import(ar_test_node, [assert_slave_wait_until_receives_txs/2]).
-import(ar_test_node, [wait_until_height/2, slave_wait_until_height/2]).
-import(ar_test_node, [slave_call/3]).
-import(ar_test_node, [post_tx_to_slave/2]).

-import(ar_test_fork, [test_on_fork/3]).

accepts_gossips_and_mines_test_() ->
	test_on_fork(height_1_8, 0, fun accepts_gossips_and_mines/0).

keeps_txs_after_new_block_test_() ->
	test_on_fork(height_1_8, 0, fun keeps_txs_after_new_block/0).

returns_error_when_txs_exceed_balance_test_() ->
	test_on_fork(height_1_8, 0, fun returns_error_when_txs_exceed_balance/0).

rejects_transactions_above_the_size_limit_test_() ->
	test_on_fork(height_1_8, 0, fun rejects_transactions_above_the_size_limit/0).

mines_blocks_under_the_size_limit_test_() ->
	{timeout, 60, test_on_fork(height_1_8, 0, fun mines_blocks_under_the_size_limit/0)}.

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
	assert_slave_wait_until_receives_txs(Slave, [SignedTX1]),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			SlaveIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX2))
		),
	assert_slave_wait_until_receives_txs(Slave, [SignedTX1, SignedTX2]),
	%% Expect both transactions to be gossiped to master.
	assert_wait_until_receives_txs(Master, [SignedTX1, SignedTX2]),
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
	assert_wait_until_receives_txs(Master, [SignedTX1]),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			MasterIP,
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(SignedTX2))
		),
	assert_wait_until_receives_txs(Master, [SignedTX1, SignedTX2]),
	?assertEqual([], slave_call(ar_node, get_all_known_txs, [Slave])),
	%% Connect the nodes mine an empty block on slave.
	connect_to_slave(),
	slave_mine(Slave),
	%% Expect master to receive the block.
	BHL = wait_until_height(Master, 1),
	?assertEqual([], (ar_storage:read_block(hd(BHL), BHL))#block.txs),
	%% Expect master to still have two transactions in the mempool.
	assert_wait_until_receives_txs(Master, [SignedTX1, SignedTX2]),
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

rejects_transactions_above_the_size_limit() ->
	%% Create a genesis block with a wallet.
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(20), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(20), <<>>}
	]),
	%% Start the node.
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	SmallData = << <<1>> || _ <- lists:seq(1, ?TX_DATA_SIZE_LIMIT) >>,
	BigData = << <<1>> || _ <- lists:seq(1, ?TX_DATA_SIZE_LIMIT + 1) >>,
	GoodTX = ar_tx:sign((ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = SmallData }, Key1),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_slave(Slave, GoodTX),
	BadTX = ar_tx:sign((ar_tx:new())#tx{ owner = Pub2, reward = ?AR(10), data = BigData }, Key2),
	{ok, {{<<"400">>, _}, _, <<"Transaction verification failed.">>, _, _}} = post_tx_to_slave(Slave, BadTX),
	{ok, ["tx_fields_too_large"]} = slave_call(ar_tx_db, get_error_codes, [BadTX#tx.id]).

mines_blocks_under_the_size_limit() ->
	%% Post a bunch of transactions from two different wallets so that their total data size
	%% is three times the limit for blocks. Expect them to be mined into three blocks so that
	%% each block fits under the limit.
	%%
	%% Create a genesis block with two wallets.
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	]),
	%% Start the nodes.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Prepare data and transactions.
	Chunk1 = << <<1>> || _ <- lists:seq(1, ?TX_DATA_SIZE_LIMIT) >>,
	Chunk2 = << <<1>> || _ <- lists:seq(1, (?TX_DATA_SIZE_LIMIT) - 5) >>,
	Chunk3 = << <<1>> || _ <- lists:seq(1, 5) >>,
	Chunk4 = << <<1>> || _ <- lists:seq(1, (?TX_DATA_SIZE_LIMIT) - 1) >>,
	Chunk5 = <<1>>,
	%% Block 1: 1 TX.
	Wallet1TX1 = ar_tx:sign((ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk1 }, Key1),
	%% Block 2: 2 TXs from different wallets.
	Wallet2TX1 = ar_tx:sign((ar_tx:new())#tx{ owner = Pub2, reward = ?AR(10), data = Chunk2 }, Key2),
	Wallet1TX2 = ar_tx:sign((ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk3, last_tx = Wallet1TX1#tx.id }, Key1),
	%% Block 3: 2 TXs from the same wallet.
	Wallet1TX3 = ar_tx:sign((ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk4, last_tx = Wallet1TX2#tx.id }, Key1),
	Wallet1TX4 = ar_tx:sign((ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk5, last_tx = Wallet1TX3#tx.id }, Key1),
	%% Post transactions.
	lists:foreach(
		fun(TX) ->
			{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_slave(Slave, TX)
		end,
		[Wallet1TX1, Wallet2TX1, Wallet1TX2, Wallet1TX3, Wallet1TX4]
	),
	%% Mine three blocks, expect the transactions there.
	slave_mine(Slave),
	SlaveBHL1 = slave_wait_until_height(Slave, 1),
	?assertEqual(
		[Wallet1TX1#tx.id],
		(slave_call(ar_storage, read_block, [hd(SlaveBHL1), SlaveBHL1]))#block.txs
	),
	BHL1 = wait_until_height(Master, 1),
	?assertEqual(
		[Wallet1TX1#tx.id],
		(ar_storage:read_block(hd(BHL1), BHL1))#block.txs
	),
	slave_mine(Slave),
	SlaveBHL2 = slave_wait_until_height(Slave, 2),
	?assertEqual(
		[Wallet2TX1#tx.id, Wallet1TX2#tx.id],
		(slave_call(ar_storage, read_block, [hd(SlaveBHL2), SlaveBHL2]))#block.txs
	),
	BHL2 = wait_until_height(Master, 2),
	?assertEqual(
		[Wallet2TX1#tx.id, Wallet1TX2#tx.id],
		(ar_storage:read_block(hd(BHL2), BHL2))#block.txs
	),
	slave_mine(Slave),
	SlaveBHL3 = slave_wait_until_height(Slave, 3),
	?assertEqual(
		[Wallet1TX3#tx.id, Wallet1TX4#tx.id],
		(slave_call(ar_storage, read_block, [hd(SlaveBHL3), SlaveBHL3]))#block.txs
	),
	BHL3 = wait_until_height(Master, 3),
	?assertEqual(
		[Wallet1TX3#tx.id, Wallet1TX4#tx.id],
		(ar_storage:read_block(hd(BHL3), BHL3))#block.txs
	).
