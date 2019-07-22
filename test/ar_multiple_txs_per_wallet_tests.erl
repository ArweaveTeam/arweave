-module(ar_multiple_txs_per_wallet_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0]).
-import(ar_test_node, [slave_mine/1]).
-import(ar_test_node, [assert_wait_until_receives_txs/2]).
-import(ar_test_node, [wait_until_height/2, slave_wait_until_height/2]).
-import(ar_test_node, [slave_call/3]).
-import(ar_test_node, [post_tx_to_slave/2, post_tx_to_master/2]).
-import(ar_test_node, [assert_post_tx_to_slave/2]).
-import(ar_test_fork, [test_on_fork/3]).

accepts_gossips_and_mines_test_() ->
	Key = {_, Pub} = ar_wallet:new(),
	Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>}],
	[B0] = ar_weave:init(Wallets),
	lists:map(
		fun({Name, TXs}) ->
			test_on_fork(
				height_1_8,
				0,
				{Name, fun() -> accepts_gossips_and_mines(B0, TXs) end}
			)
		end,
		[
			one_wallet_list_one_block_anchored_txs(Key, B0),
			two_block_anchored_txs(Key, B0)
		]
	).

keeps_txs_after_new_block_test_() ->
	Key = {_, Pub} = ar_wallet:new(),
	Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>}],
	[B0] = ar_weave:init(Wallets),
	lists:map(
		fun({{FirstName, FirstTXSet}, {SecondName, SecondTXSet}}) ->
			Name = lists:flatten(
				io_lib:format("First set: ~s, second set: ~s", [FirstName, SecondName])
			),
			test_on_fork(
				height_1_8,
				0,
				{Name, fun() -> keeps_txs_after_new_block(B0, FirstTXSet, SecondTXSet) end}
			)
		end,
		[
			%% Master receives the second set then the first set. Slave only
			%% receives the second set.
			{two_block_anchored_txs(Key, B0), {"No transactions", []}},
			{{"No transactions", []}, two_block_anchored_txs(Key, B0)},
			{two_block_anchored_txs(Key, B0), two_block_anchored_txs(Key, B0)}

		]
	).

returns_error_when_txs_exceed_balance_test_() ->
	lists:map(
		fun({Name, B0, TXs, ExceedBalanceTX}) ->
			test_on_fork(
				height_1_8,
				0,
				{Name, fun() -> returns_error_when_txs_exceed_balance(B0, TXs, ExceedBalanceTX) end}
			)
		end,
		[
			block_anchor_txs_spending_balance_plus_one_more(),
			mixed_anchor_txs_spending_balance_plus_one_more()
		]
	).

rejects_transactions_above_the_size_limit_test_() ->
	test_on_fork(height_1_8, 0, fun rejects_transactions_above_the_size_limit/0).

accepts_at_most_one_wallet_list_anchored_tx_per_block_test_() ->
	test_on_fork(height_1_8, 0, fun accepts_at_most_one_wallet_list_anchored_tx_per_block/0).

does_not_allow_to_spend_mempool_tokens_test_() ->
	test_on_fork(height_1_8, 0, fun does_not_allow_to_spend_mempool_tokens/0).

does_not_allow_to_replay_empty_wallet_txs_test_() ->
	test_on_fork(height_1_8, 0, fun does_not_allow_to_replay_empty_wallet_txs/0).

mines_blocks_under_the_size_limit_test_() ->
	lists:map(
		fun({Name, B0, TXGroups}) ->
			test_on_fork(
				height_1_8,
				0,
				{Name, fun() -> mines_blocks_under_the_size_limit(B0, TXGroups) end}
			)
		end,
		[
			grouped_block_anchored_txs(),
			grouped_mixed_anchored_txs()
		]
	).

rejects_txs_with_outdated_anchors_test_() ->
	test_on_fork(height_1_8, 0, fun rejects_txs_with_outdated_anchors/0).

rejects_txs_exceeding_mempool_limit_test_() ->
	test_on_fork(height_1_8, 0, fun rejects_txs_exceeding_mempool_limit/0).

accepts_gossips_and_mines(B0, TXs) ->
	%% Post the given transactions made from the given wallets to a node.
	%%
	%% Expect them to be accepted, gossiped to the peer and included into the block.
	%% Expect the block to be accepted by the peer.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Post the transactions to slave.
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX),
			%% Expect transactions to be gossiped to master.
			assert_wait_until_receives_txs(Master, [TX])
		end,
		TXs
	),
	%% Mine a block.
	slave_mine(Slave),
	%% Expect both transactions to be included into block.
	SlaveBHL = slave_wait_until_height(Slave, 1),
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	?assertEqual(
		TXIDs,
		(slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]))#block.txs
	),
	lists:foreach(
		fun(TX) ->
			?assertEqual(TX, slave_call(ar_storage, read_tx, [TX#tx.id]))
		end,
		TXs
	),
	%% Expect the block to be accepted by master.
	BHL = wait_until_height(Master, 1),
	?assertEqual(
		TXIDs,
		(ar_storage:read_block(hd(BHL), BHL))#block.txs
	),
	lists:foreach(
		fun(TX) ->
			?assertEqual(TX, ar_storage:read_tx(TX#tx.id))
		end,
		TXs
	).

keeps_txs_after_new_block(B0, FirstTXSet, SecondTXSet) ->
	%% Post the transactions from the first set to a node but do not gossip them.
	%% Post transactiongs from the second set to both nodes.
	%% Mine a block with transactions from the second set on a different node
	%% and gossip it to the node with transactions.
	%%
	%% Expect the block to be accepted.
	%% Expect transactions from the difference between the two sets to be kept in the mempool.
	%% Mine a block on the first node, expect the difference to be included into the block.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	%% Do not connect the nodes so that slave does not receive txs.
	%% Post transactions from the first set to master.
	lists:foreach(
		fun(TX) ->
			post_tx_to_master(Master, TX)
		end,
		SecondTXSet ++ FirstTXSet
	),
	?assertEqual([], slave_call(ar_node, get_all_known_txs, [Slave])),
	%% Post transactions from the second set to slave.
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		SecondTXSet
	),
	%% Connect the nodes and mine a block on slave.
	connect_to_slave(),
	slave_mine(Slave),
	%% Expect master to receive the block.
	BHL = wait_until_height(Master, 1),
	SecondSetTXIDs = lists:map(fun(TX) -> TX#tx.id end, SecondTXSet),
	?assertEqual(SecondSetTXIDs, (ar_storage:read_block(hd(BHL), BHL))#block.txs),
	%% Expect master to have the set difference in the mempool.
	assert_wait_until_receives_txs(Master, FirstTXSet -- SecondTXSet),
	%% Mine a block on master and expect both transactions to be included.
	ar_node:mine(Master),
	BHL2 = wait_until_height(Master, 2),
	SetDifferenceTXIDs = lists:map(fun(TX) -> TX#tx.id end, FirstTXSet -- SecondTXSet),
	?assertEqual(
		SetDifferenceTXIDs,
		(ar_storage:read_block(hd(BHL2), BHL2))#block.txs
	).

returns_error_when_txs_exceed_balance(B0, TXs, ExceedBalanceTX) ->
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Post the given transactions that do not exceed the balance.
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		TXs
	),
	%% Expect the balance exceeding transactions to be included
	%% into the mempool cause it can be potentially included by
	%% other nodes.
	assert_post_tx_to_slave(Slave, ExceedBalanceTX),
	%% Expect only the first two to be included into the block.
	slave_mine(Slave),
	SlaveBHL = slave_wait_until_height(Slave, 1),
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	?assertEqual(
		TXIDs,
		(slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]))#block.txs
	),
	BHL = wait_until_height(Master, 1),
	?assertEqual(
		TXIDs,
		(ar_storage:read_block(hd(BHL), BHL))#block.txs
	),
	%% Post the balance exceeding transaction again
	%% and expect the balance exceeded error.
	slave_call(ets, delete, [ignored_ids, ExceedBalanceTX#tx.id]),
	{ok, {{<<"400">>, _}, _, <<"Waiting TXs exceed balance for wallet.">>, _, _}} =
		ar_httpc:request(
			<<"POST">>,
			{127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
			"/tx",
			[],
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(ExceedBalanceTX))
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
	assert_post_tx_to_slave(Slave, GoodTX),
	BadTX = ar_tx:sign((ar_tx:new())#tx{ owner = Pub2, reward = ?AR(10), data = BigData }, Key2),
	{ok, {{<<"400">>, _}, _, <<"Transaction verification failed.">>, _, _}} = post_tx_to_slave(Slave, BadTX),
	{ok, ["tx_fields_too_large"]} = slave_call(ar_tx_db, get_error_codes, [BadTX#tx.id]).

accepts_at_most_one_wallet_list_anchored_tx_per_block() ->
	%% Post a TX, mine a block.
	%% Post another TX referencing the first one.
	%% Post the third TX referencing the second one.
	%%
	%% Expect the third to be rejected.
	%%
	%% Post the fourth TX referencing the block.
	%%
	%% Expect the fourth TX to be accepted and mined into a block.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1) },
		Key
	),
	assert_post_tx_to_slave(Slave, TX1),
	slave_mine(Slave),
	slave_wait_until_height(Slave, 1),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = TX1#tx.id },
		Key
	),
	assert_post_tx_to_slave(Slave, TX2),
	TX3 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = TX2#tx.id },
		Key
	),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx from mempool).">>, _, _}} = post_tx_to_slave(Slave, TX3),
	TX4 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = B0#block.indep_hash },
		Key
	),
	assert_post_tx_to_slave(Slave, TX4),
	slave_mine(Slave),
	SlaveBHL = slave_wait_until_height(Slave, 2),
	B2 = slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]),
	?assertEqual([TX2#tx.id, TX4#tx.id], B2#block.txs).

does_not_allow_to_spend_mempool_tokens() ->
	%% Post a transaction sending tokens to a wallet with few tokens.
	%% Post the second transaction spending the new tokens.
	%%
	%% Expect the second transaction to be rejected.
	%%
	%% Mine a block.
	%% Post another transaction spending the rest of tokens from the new wallet.
	%%
	%% Expect the transaction to be accepted.
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(20), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(0), <<>>}
	]),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub1,
			target = ar_wallet:to_address(Pub2),
			reward = ?AR(1),
			quantity = ?AR(2)
		},
		Key1
	),
	assert_post_tx_to_slave(Slave, TX1),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub2,
			target = ar_wallet:to_address(Pub1),
			reward = ?AR(1),
			quantity = ?AR(1),
			last_tx = B0#block.indep_hash,
			tags = [{<<"nonce">>, <<"1">>}]
		},
		Key2
	),
	{ok, {{<<"400">>, _}, _, <<"Waiting TXs exceed balance for wallet.">>, _, _}} = post_tx_to_slave(Slave, TX2),
	slave_mine(Slave),
	SlaveBHL = slave_wait_until_height(Slave, 1),
	B1 = slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]),
	?assertEqual([TX1#tx.id], B1#block.txs),
	TX3 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub2,
			target = ar_wallet:to_address(Pub1),
			reward = ?AR(1),
			quantity = ?AR(1),
			last_tx = B1#block.indep_hash,
			tags = [{<<"nonce">>, <<"3">>}]
		},
		Key2
	),
	assert_post_tx_to_slave(Slave, TX3),
	slave_mine(Slave),
	SlaveBHL2 = slave_wait_until_height(Slave, 2),
	B2 = slave_call(ar_storage, read_block, [hd(SlaveBHL2), SlaveBHL2]),
	?assertEqual([TX3#tx.id], B2#block.txs).

does_not_allow_to_replay_empty_wallet_txs() ->
	%% Create a new wallet by sending some tokens to it. Mine a block.
	%% Send the tokens back so that the wallet balance is back to zero. Mine a block.
	%% Send the same amount of tokens to the same wallet again. Mine a block.
	%% Try to replay the transaction which sent the tokens back (before and after mining).
	%%
	%% Expect the replay to be rejected.
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(50), <<>>}
	]),
	{Slave, _} = slave_start(B0),
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub1,
			target = ar_wallet:to_address(Pub2),
			reward = ?AR(6),
			quantity = ?AR(2)
		},
		Key1
	),
	assert_post_tx_to_slave(Slave, TX1),
	slave_mine(Slave),
	slave_wait_until_height(Slave, 1),
	SlaveIP = {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
	GetBalancePath = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub2))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			SlaveIP,
			"/wallet/" ++ GetBalancePath ++ "/balance",
			[]
		),
	Balance = binary_to_integer(Body),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub2,
			target = ar_wallet:to_address(Pub1),
			reward = Balance - ?AR(1),
			quantity = ?AR(1)
		},
		Key2
	),
	assert_post_tx_to_slave(Slave, TX2),
	slave_mine(Slave),
	slave_wait_until_height(Slave, 2),
	{ok, {{<<"200">>, _}, _, Body2, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			SlaveIP,
			"/wallet/" ++ GetBalancePath ++ "/balance",
			[]
		),
	?assertEqual(0, binary_to_integer(Body2)),
	TX3 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub1,
			target = ar_wallet:to_address(Pub2),
			reward = ?AR(6),
			quantity = ?AR(2),
			last_tx = TX1#tx.id
		},
		Key1
	),
	assert_post_tx_to_slave(Slave, TX3),
	slave_mine(Slave),
	slave_wait_until_height(Slave, 3),
	%% Remove the replay TX from the ingnore list (to simulate e.g. a node restart).
	slave_call(ets, delete, [ignored_ids, TX2#tx.id]),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		post_tx_to_slave(Slave, TX2).

mines_blocks_under_the_size_limit(B0, TXGroups) ->
	%% Post the given transactions grouped by block size to a node.
	%%
	%% Expect them to be mined into the corresponding number of blocks so that
	%% each block fits under the limit.
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX),
			assert_wait_until_receives_txs(Master, [TX])
		end,
		lists:flatten(TXGroups)
	),
	%% Mine blocks, expect the transactions there.
	lists:foldl(
		fun(Group, Height) ->
			slave_mine(Slave),
			SlaveBHL = slave_wait_until_height(Slave, Height),
			GroupTXIDs = lists:map(fun(TX) -> TX#tx.id end, Group),
			?assertEqual(
				GroupTXIDs,
				(slave_call(ar_storage, read_block, [hd(SlaveBHL), SlaveBHL]))#block.txs
			),
			Height + 1
		end,
		1,
		TXGroups
	).

rejects_txs_with_outdated_anchors() ->
	%% Post a transaction anchoring the block at ?MAX_TX_ANCHOR_DEPTH + 1.
	%%
	%% Expect the transaction to be rejected.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{Slave, _} = slave_start(B0),
	slave_mine_blocks(Slave, ?MAX_TX_ANCHOR_DEPTH),
	slave_wait_until_height(Slave, ?MAX_TX_ANCHOR_DEPTH),
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{
			owner = Pub,
			reward = ?AR(1),
			last_tx = B0#block.indep_hash
		},
		Key
	),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		post_tx_to_slave(Slave, TX1).

rejects_txs_exceeding_mempool_limit() ->
	%% Post transactions which exceed the mempool size limit.
	%%
	%% Expect the exceeding transaction to be rejected.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{Slave, _} = slave_start(B0),
	BigChunk = << <<1>> || _ <- lists:seq(1, ?TX_DATA_SIZE_LIMIT) >>,
	TXs = lists:map(
		fun(N) ->
			ar_tx:sign(
				(ar_tx:new())#tx{
					owner = Pub,
					reward = ?AR(1),
					last_tx = B0#block.indep_hash,
					data = BigChunk,
					tags = [{<<"nonce">>, integer_to_binary(N)}]
				},
				Key
			)
		end,
		lists:seq(1, 6)
	),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		lists:sublist(TXs, 5)
	),
	{ok, {{<<"400">>, _}, _, <<"Mempool is full.">>, _, _}} =
		post_tx_to_slave(Slave, lists:last(TXs)).

one_wallet_list_one_block_anchored_txs(Key = {_, Pub}, B0) ->
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1) },
		Key
	),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = B0#block.indep_hash },
		Key
	),
	{"One transaction with wallet list anchor followed by one with block anchor", [TX1, TX2]}.

two_block_anchored_txs(Key = {_, Pub}, B0) ->
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = B0#block.indep_hash },
		Key
	),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = B0#block.indep_hash },
		Key
	),
	{"Two transactions with block anchor", [TX1, TX2]}.

block_anchor_txs_spending_balance_plus_one_more() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, quantity = ?AR(4), reward = ?AR(6), last_tx = B0#block.indep_hash },
		Key
	),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(10), last_tx = B0#block.indep_hash },
		Key
	),
	ExceedBalanceTX = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = B0#block.indep_hash },
		Key
	),
	{"Three transactions with block anchor", B0, [TX1, TX2], ExceedBalanceTX}.

mixed_anchor_txs_spending_balance_plus_one_more() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, quantity = ?AR(4), reward = ?AR(6) },
		Key
	),
	TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(5), last_tx = B0#block.indep_hash  },
		Key
	),
	TX3 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(2), last_tx = B0#block.indep_hash },
		Key
	),
	TX4 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(3), last_tx = B0#block.indep_hash  },
		Key
	),
	ExceedBalanceTX = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub, reward = ?AR(1), last_tx = B0#block.indep_hash },
		Key
	),
	{"Five transactions with mixed anchors", B0, [TX1, TX2, TX3, TX4], ExceedBalanceTX}.

grouped_block_anchored_txs() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Wallets = [
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	],
	[B0] = ar_weave:init(Wallets),
	%% Expect transactions to be chosen from biggest to smallest.
	Chunk1 = << <<1>> || _ <- lists:seq(1, ?TX_DATA_SIZE_LIMIT) >>,
	Chunk2 = << <<1>> || _ <- lists:seq(1, (?TX_DATA_SIZE_LIMIT) - 1) >>,
	Chunk3 = <<1>>,
	Chunk4 = << <<1>> || _ <- lists:seq(1, (?TX_DATA_SIZE_LIMIT) - 5) >>,
	Chunk5 = << <<1>> || _ <- lists:seq(1, 5) >>,
	%% Block 1: 1 TX.
	Wallet1TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk1, last_tx = B0#block.indep_hash },
		Key1
	),
	%% Block 2: 2 TXs from different wallets.
	Wallet2TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub2, reward = ?AR(10), data = Chunk2, last_tx = B0#block.indep_hash },
		Key2
	),
	Wallet1TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk3, last_tx = B0#block.indep_hash },
		Key1
	),
	%% Block 3: 2 TXs from the same wallet.
	Wallet1TX3 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk4, last_tx = B0#block.indep_hash },
		Key1
	),
	Wallet1TX4 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk5, last_tx = B0#block.indep_hash },
		Key1
	),
	{"Five transactions with block anchor", B0, [[Wallet1TX1], [Wallet2TX1, Wallet1TX2], [Wallet1TX3, Wallet1TX4]]}.

grouped_mixed_anchored_txs() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Wallets = [
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	],
	[B0] = ar_weave:init(Wallets),
	%% Expect transactions to be chosen from biggest to smallest.
	Chunk1 = << <<1>> || _ <- lists:seq(1, ?TX_DATA_SIZE_LIMIT) >>,
	Chunk2 = << <<1>> || _ <- lists:seq(1, (?TX_DATA_SIZE_LIMIT) - 1) >>,
	Chunk3 = <<1>>,
	Chunk4 = << <<1>> || _ <- lists:seq(1, (?TX_DATA_SIZE_LIMIT) - 5) >>,
	Chunk5 = << <<1>> || _ <- lists:seq(1, 5) >>,
	%% Block 1: 1 TX.
	Wallet1TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk1 },
		Key1
	),
	%% Block 2: 2 TXs from different wallets.
	Wallet2TX1 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub2, reward = ?AR(10), data = Chunk2, last_tx = B0#block.indep_hash },
		Key2
	),
	Wallet1TX2 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk3, last_tx = B0#block.indep_hash },
		Key1
	),
	%% Block 3: 2 TXs from the same wallet.
	Wallet1TX3 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk4, last_tx = B0#block.indep_hash },
		Key1
	),
	Wallet1TX4 = ar_tx:sign(
		(ar_tx:new())#tx{ owner = Pub1, reward = ?AR(10), data = Chunk5, last_tx = B0#block.indep_hash },
		Key1
	),
	{"Five transactions with mixed anchors", B0, [[Wallet1TX1], [Wallet2TX1, Wallet1TX2], [Wallet1TX3, Wallet1TX4]]}.

slave_mine_blocks(Slave, TargetHeight) ->
	slave_mine_blocks(Slave, 1, TargetHeight).

slave_mine_blocks(_Slave, Height, TargetHeight) when Height == TargetHeight + 1 ->
	ok;
slave_mine_blocks(Slave, Height, TargetHeight) ->
	slave_mine(Slave),
	slave_wait_until_height(Slave, Height),
	slave_mine_blocks(Slave, Height + 1, TargetHeight).
