-module(ar_multiple_txs_per_wallet_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0,
	slave_mine/0, join_on_slave/0, assert_wait_until_receives_txs/1,
	wait_until_height/1, assert_slave_wait_until_height/1,
	slave_call/3, assert_wait_until_block_block_index/1, post_tx_to_slave/1,
	post_tx_to_slave/2, post_tx_to_master/1, assert_post_tx_to_slave/1,
	assert_post_tx_to_master/1, sign_tx/2, sign_tx/3, sign_v1_tx/1, sign_v1_tx/2, sign_v1_tx/3,
	get_tx_anchor/0, get_tx_anchor/1, get_tx_confirmations/2,
	disconnect_from_slave/0, read_block_when_stored/1, random_v1_data/1, slave_peer/0]).

accepts_gossips_and_mines_test_() ->
	PrepareTestFor = fun(BuildTXSetFun) ->
		fun() ->
			%% The weave has to be initialised under the fork so that
			%% we can get the correct price estimations according
			%% to the new pricinig model.
			Key = {_, Pub} = ar_wallet:new(),
			Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>}],
			[B0] = ar_weave:init(Wallets),
			accepts_gossips_and_mines(B0, BuildTXSetFun(Key, B0))
		end
	end,
	[
		{timeout, 120, {
			"One transaction with wallet list anchor followed by one with block anchor",
			PrepareTestFor(fun one_wallet_list_one_block_anchored_txs/2)
		}},
		{timeout, 120, {
			"Two transactions with block anchor",
			PrepareTestFor(fun two_block_anchored_txs/2)
		}}
	].

keeps_txs_after_new_block_test_() ->
	PrepareTestFor = fun(BuildFirstTXSetFun, BuildSecondTXSetFun) ->
		fun() ->
			Key = {_, Pub} = ar_wallet:new(),
			Wallets = [{ar_wallet:to_address(Pub), ?AR(5), <<>>}],
			[B0] = ar_weave:init(Wallets),
			keeps_txs_after_new_block(
				B0,
				BuildFirstTXSetFun(Key, B0),
				BuildSecondTXSetFun(Key, B0)
			)
		end
	end,
	[
		%% Master receives the second set then the first set. Slave only
		%% receives the second set.
		{timeout, 120, {
			"First set: two block anchored txs, second set: empty",
			PrepareTestFor(fun two_block_anchored_txs/2, fun empty_tx_set/2)
		}},
		{timeout, 120, {
			"First set: empty, second set: two block anchored txs",
			PrepareTestFor(fun empty_tx_set/2, fun two_block_anchored_txs/2)
		}},
		{timeout, 120, {
			"First set: two block anchored txs, second set: two block anchored txs",
			PrepareTestFor(fun two_block_anchored_txs/2, fun two_block_anchored_txs/2)
		}}
	].

returns_error_when_txs_exceed_balance_test_() ->
	PrepareTestFor = fun(BuildTXSetFun) ->
		fun() ->
			{B0, TXs, ExceedBalanceTX} = BuildTXSetFun(),
			returns_error_when_txs_exceed_balance(B0, TXs, ExceedBalanceTX)
		end
	end,
	[
		{timeout, 120, {
			"Three transactions with block anchor",
			PrepareTestFor(fun block_anchor_txs_spending_balance_plus_one_more/0)
		}},
		{timeout, 120, {
			"Five transactions with mixed anchors",
			PrepareTestFor(fun mixed_anchor_txs_spending_balance_plus_one_more/0)
		}}
	].

mines_blocks_under_the_size_limit_test_() ->
	PrepareTestFor = fun(BuildTXSetFun) ->
		fun() ->
			{B0, TXGroups} = BuildTXSetFun(),
			mines_blocks_under_the_size_limit(B0, TXGroups)
		end
	end,
	[
		{
			"Five transactions with block anchors",
			{timeout, 30, PrepareTestFor(fun() -> grouped_txs() end)}
		}
	].

joins_network_successfully_test_() ->
	{timeout, 240, fun joins_network_successfully/0}.

recovers_from_forks_test_() ->
	{timeout, 120, fun() -> recovers_from_forks(7) end}.

accepts_gossips_and_mines(B0, TXFuns) ->
	%% Post the given transactions made from the given wallets to a node.
	%%
	%% Expect them to be accepted, gossiped to the peer and included into the block.
	%% Expect the block to be accepted by the peer.
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	%% Sign here after the node has started to get the correct price
	%% estimation from it.
	TXs = lists:map(fun(TXFun) -> TXFun() end, TXFuns),
	connect_to_slave(),
	%% Post the transactions to slave.
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX),
			%% Expect transactions to be gossiped to master.
			assert_wait_until_receives_txs([TX])
		end,
		TXs
	),
	%% Mine a block.
	slave_mine(),
	%% Expect both transactions to be included into block.
	SlaveBI = assert_slave_wait_until_height(1),
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	?assertEqual(
		lists:sort(TXIDs),
		lists:sort((slave_call(ar_test_node, read_block_when_stored, [hd(SlaveBI)]))#block.txs)
	),
	lists:foreach(
		fun(TX) ->
			?assertEqual(TX, slave_call(ar_storage, read_tx, [TX#tx.id]))
		end,
		TXs
	),
	%% Expect the block to be accepted by master.
	BI = wait_until_height(1),
	?assertEqual(
		lists:sort(TXIDs),
		lists:sort((read_block_when_stored(hd(BI)))#block.txs)
	),
	lists:foreach(
		fun(TX) ->
			?assertEqual(TX, ar_storage:read_tx(TX#tx.id))
		end,
		TXs
	).

keeps_txs_after_new_block(B0, FirstTXSetFuns, SecondTXSetFuns) ->
	%% Post the transactions from the first set to a node but do not gossip them.
	%% Post transactiongs from the second set to both nodes.
	%% Mine a block with transactions from the second set on a different node
	%% and gossip it to the node with transactions.
	%%
	%% Expect the block to be accepted.
	%% Expect transactions from the difference between the two sets to be kept in the mempool.
	%% Mine a block on the first node, expect the difference to be included into the block.
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	%% Sign here after the node has started to get the correct price
	%% estimation from it.
	FirstTXSet = lists:map(fun(TXFun) -> TXFun() end, FirstTXSetFuns),
	SecondTXSet = lists:map(fun(TXFun) -> TXFun() end, SecondTXSetFuns),
	%% Disconnect the nodes so that slave does not receive txs.
	disconnect_from_slave(),
	%% Post transactions from the first set to master.
	lists:foreach(
		fun(TX) ->
			post_tx_to_master(TX)
		end,
		SecondTXSet ++ FirstTXSet
	),
	?assertEqual([], slave_call(ar_node, get_pending_txs, [])),
	%% Post transactions from the second set to slave.
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		SecondTXSet
	),
	%% Connect the nodes and mine a block on slave.
	connect_to_slave(),
	slave_mine(),
	%% Expect master to receive the block.
	BI = wait_until_height(1),
	SecondSetTXIDs = lists:map(fun(TX) -> TX#tx.id end, SecondTXSet),
	?assertEqual(lists:sort(SecondSetTXIDs), lists:sort((read_block_when_stored(hd(BI)))#block.txs)),
	%% Expect master to have the set difference in the mempool.
	assert_wait_until_receives_txs(FirstTXSet -- SecondTXSet),
	%% Mine a block on master and expect both transactions to be included.
	ar_node:mine(),
	BI2 = wait_until_height(2),
	SetDifferenceTXIDs = lists:map(fun(TX) -> TX#tx.id end, FirstTXSet -- SecondTXSet),
	?assertEqual(
		lists:sort(SetDifferenceTXIDs),
		lists:sort((read_block_when_stored(hd(BI2)))#block.txs)
	).

returns_error_when_txs_exceed_balance(B0, TXs, ExceedBalanceTX) ->
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Post the given transactions that do not exceed the balance.
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	%% Expect the balance exceeding transactions to be included
	%% into the mempool cause it can be potentially included by
	%% other nodes.
	assert_post_tx_to_slave(ExceedBalanceTX),
	assert_wait_until_receives_txs(TXs),
	%% Expect only the first two to be included into the block.
	slave_mine(),
	SlaveBI = assert_slave_wait_until_height(1),
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	?assertEqual(
		lists:sort(TXIDs),
		lists:sort((slave_call(ar_test_node, read_block_when_stored, [hd(SlaveBI)]))#block.txs)
	),
	BI = wait_until_height(1),
	?assertEqual(
		lists:sort(TXIDs),
		lists:sort((read_block_when_stored(hd(BI)))#block.txs)
	),
	%% Post the balance exceeding transaction again
	%% and expect the balance exceeded error.
	slave_call(ets, delete, [ignored_ids, ExceedBalanceTX#tx.id]),
	{ok, {{<<"400">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
			path => "/tx",
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(ExceedBalanceTX))
		}),
	?assertEqual({ok, ["overspend"]}, ar_tx_db:get_error_codes(ExceedBalanceTX#tx.id)).

rejects_transactions_above_the_size_limit_test_() ->
	{timeout, 60, fun test_rejects_transactions_above_the_size_limit/0}.

test_rejects_transactions_above_the_size_limit() ->
	%% Create a genesis block with a wallet.
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(20), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(20), <<>>}
	]),
	%% Start the node.
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	SmallData = random_v1_data(?TX_DATA_SIZE_LIMIT),
	BigData = random_v1_data(?TX_DATA_SIZE_LIMIT + 1),
	GoodTX = sign_v1_tx(Key1, #{ data => SmallData }),
	assert_post_tx_to_slave(GoodTX),
	BadTX = sign_v1_tx(Key2, #{ data => BigData }),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"Transaction verification failed.">>, _, _}},
		post_tx_to_slave(BadTX)
	),
	?assertMatch(
		{ok, ["tx_fields_too_large"]},
		slave_call(ar_tx_db, get_error_codes, [BadTX#tx.id])
	).

accepts_at_most_one_wallet_list_anchored_tx_per_block_test_() ->
	{timeout, 60, fun test_accepts_at_most_one_wallet_list_anchored_tx_per_block/0}.

test_accepts_at_most_one_wallet_list_anchored_tx_per_block() ->
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
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	TX1 = sign_v1_tx(Key),
	assert_post_tx_to_slave(TX1),
	slave_mine(),
	assert_slave_wait_until_height(1),
	TX2 = sign_v1_tx(Key, #{ last_tx => TX1#tx.id }),
	assert_post_tx_to_slave(TX2),
	TX3 = sign_v1_tx(Key, #{ last_tx => TX2#tx.id }),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx from mempool).">>, _, _}} = post_tx_to_slave(TX3),
	TX4 = sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }),
	assert_post_tx_to_slave(TX4),
	slave_mine(),
	SlaveBI = assert_slave_wait_until_height(2),
	B2 = slave_call(ar_test_node, read_block_when_stored, [hd(SlaveBI)]),
	?assertEqual([TX2#tx.id, TX4#tx.id], B2#block.txs).

does_not_allow_to_spend_mempool_tokens_test_() ->
	{timeout, 60, fun test_does_not_allow_to_spend_mempool_tokens/0}.

test_does_not_allow_to_spend_mempool_tokens() ->
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
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	TX1 = sign_v1_tx(Key1, #{ target => ar_wallet:to_address(Pub2), reward => ?AR(1), quantity => ?AR(2) }),
	assert_post_tx_to_slave(TX1),
	TX2 = sign_v1_tx(
		Key2,
		#{
			target => ar_wallet:to_address(Pub1),
			reward => ?AR(1),
			quantity => ?AR(1),
			last_tx => B0#block.indep_hash,
			tags => [{<<"nonce">>, <<"1">>}]
		}
	),
	{ok, {{<<"400">>, _}, _, _, _, _}} = post_tx_to_slave(TX2),
	?assertEqual({ok, ["overspend"]}, slave_call(ar_tx_db, get_error_codes, [TX2#tx.id])),
	slave_mine(),
	SlaveBI = assert_slave_wait_until_height(1),
	B1 = slave_call(ar_test_node, read_block_when_stored, [hd(SlaveBI)]),
	?assertEqual([TX1#tx.id], B1#block.txs),
	TX3 = sign_v1_tx(
		Key2,
		#{
			target => ar_wallet:to_address(Pub1),
			reward => ?AR(1),
			quantity => ?AR(1),
			last_tx => B1#block.indep_hash,
			tags => [{<<"nonce">>, <<"3">>}]
		}
	),
	assert_post_tx_to_slave(TX3),
	slave_mine(),
	SlaveBI2 = assert_slave_wait_until_height(2),
	B2 = slave_call(ar_test_node, read_block_when_stored, [hd(SlaveBI2)]),
	?assertEqual([TX3#tx.id], B2#block.txs).

does_not_allow_to_replay_empty_wallet_txs_test_() ->
	{timeout, 60, fun test_does_not_allow_to_replay_empty_wallet_txs/0}.

test_does_not_allow_to_replay_empty_wallet_txs() ->
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
	{_Slave, _} = slave_start(B0),
	TX1 = sign_v1_tx(
		Key1,
		#{
			target => ar_wallet:to_address(Pub2),
			reward => ?AR(6),
			quantity => ?AR(2)
		}
	),
	assert_post_tx_to_slave(TX1),
	slave_mine(),
	assert_slave_wait_until_height(1),
	SlaveIP = {127, 0, 0, 1, slave_call(ar_meta_db, get, [port])},
	GetBalancePath = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub2))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => SlaveIP,
			path => "/wallet/" ++ GetBalancePath ++ "/balance"
		}),
	Balance = binary_to_integer(Body),
	TX2 = sign_v1_tx(
		Key2,
		#{
			target => ar_wallet:to_address(Pub1),
			reward => Balance - ?AR(1),
			quantity => ?AR(1)
		}
	),
	assert_post_tx_to_slave(TX2),
	slave_mine(),
	assert_slave_wait_until_height(2),
	{ok, {{<<"200">>, _}, _, Body2, _, _}} =
		ar_http:req(#{
			method => get,
			peer => SlaveIP,
			path => "/wallet/" ++ GetBalancePath ++ "/balance"
		}),
	?assertEqual(0, binary_to_integer(Body2)),
	TX3 = sign_v1_tx(
		Key1,
		#{
			target => ar_wallet:to_address(Pub2),
			reward => ?AR(6),
			quantity => ?AR(2),
			last_tx => TX1#tx.id
		}
	),
	assert_post_tx_to_slave(TX3),
	slave_mine(),
	assert_slave_wait_until_height(3),
	%% Remove the replay TX from the ingnore list (to simulate e.g. a node restart).
	slave_call(ets, delete, [ignored_ids, TX2#tx.id]),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		post_tx_to_slave(TX2).

mines_blocks_under_the_size_limit(B0, TXGroups) ->
	%% Post the given transactions grouped by block size to a node.
	%%
	%% Expect them to be mined into the corresponding number of blocks so that
	%% each block fits under the limit.
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX),
			assert_wait_until_receives_txs([TX])
		end,
		lists:flatten(TXGroups)
	),
	%% Mine blocks, expect the transactions there.
	lists:foldl(
		fun(Group, Height) ->
			slave_mine(),
			SlaveBI = assert_slave_wait_until_height(Height),
			GroupTXIDs = lists:map(fun(TX) -> TX#tx.id end, Group),
			?assertEqual(
				lists:sort(GroupTXIDs),
				lists:sort(
					(slave_call(ar_test_node, read_block_when_stored, [hd(SlaveBI)]))#block.txs
				),
				io_lib:format("Height ~B", [Height])
			),
			assert_slave_wait_until_txs_are_stored(GroupTXIDs),
			Height + 1
		end,
		1,
		TXGroups
	).

assert_slave_wait_until_txs_are_stored(TXIDs) ->
	ar_util:do_until(
		fun() ->
			lists:all(fun(TX) -> is_record(TX, tx) end, ar_storage:read_tx(TXIDs))
		end,
		200,
		2000
	).

mines_format_2_txs_without_size_limit() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	ChunkSize = ?MEMPOOL_DATA_SIZE_LIMIT div (?BLOCK_TX_COUNT_LIMIT + 1),
	lists:foreach(
		fun(N) ->
			TX = sign_tx(
				Key,
				#{
					last_tx => B0#block.indep_hash,
					data => << <<1>> || _ <- lists:seq(1, ChunkSize) >>,
					tags => [{<<"nonce">>, integer_to_binary(N)}]
				}
			),
			assert_post_tx_to_slave(TX),
			assert_wait_until_receives_txs([TX])
		end,
		lists:seq(1, ?BLOCK_TX_COUNT_LIMIT + 1)
	),
	ar_node:mine(),
	[{H, _, _} | _] = wait_until_height(1),
	B = read_block_when_stored(H),
	?assertEqual(?BLOCK_TX_COUNT_LIMIT, length(B#block.txs)),
	TotalSize = lists:sum([(ar_storage:read_tx(TXID))#tx.data_size || TXID <- B#block.txs]),
	?assert(TotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT).

rejects_txs_with_outdated_anchors_test_() ->
	{timeout, 60, fun() ->
		%% Post a transaction anchoring the block at ?MAX_TX_ANCHOR_DEPTH + 1.
		%%
		%% Expect the transaction to be rejected.
		Key = {_, Pub} = ar_wallet:new(),
		[B0] = ar_weave:init([
			{ar_wallet:to_address(Pub), ?AR(20), <<>>}
		]),
		{_Slave, _} = slave_start(B0),
		slave_mine_blocks(?MAX_TX_ANCHOR_DEPTH),
		assert_slave_wait_until_height(?MAX_TX_ANCHOR_DEPTH),
		TX1 = sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }),
		{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
			post_tx_to_slave(TX1)
	end}.

drops_v1_txs_exceeding_mempool_limit_test_() ->
	{timeout, 60, fun test_drops_v1_txs_exceeding_mempool_limit/0}.

test_drops_v1_txs_exceeding_mempool_limit() ->
	%% Post transactions which exceed the mempool size limit.
	%%
	%% Expect the exceeding transaction to be dropped.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{_Slave, _} = slave_start(B0),
	BigChunk = random_v1_data(?TX_DATA_SIZE_LIMIT - ?TX_SIZE_BASE),
	TXs = lists:map(
		fun(N) ->
			sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash,
					data => BigChunk, tags => [{<<"nonce">>, integer_to_binary(N)}] })
		end,
		lists:seq(1, 6)
	),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		lists:sublist(TXs, 5)
	),
	{ok, Mempool1} = ar_http_iface_client:get_mempool(slave_peer()),
	%% The transactions have the same utility therefore they are sorted in the
	%% order of submission.
	?assertEqual([TX#tx.id || TX <- lists:sublist(TXs, 5)], Mempool1),
	Last = lists:last(TXs),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_slave(Last, false),
	{ok, Mempool2} = ar_http_iface_client:get_mempool(slave_peer()),
	%% There is no place for the last transaction in the mempool.
	?assertEqual([TX#tx.id || TX <- lists:sublist(TXs, 5)], Mempool2).

drops_v2_txs_exceeding_mempool_limit_test_() ->
	{timeout, 60, fun drops_v2_txs_exceeding_mempool_limit/0}.

drops_v2_txs_exceeding_mempool_limit() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{_Slave, _} = slave_start(B0),
	BigChunk = crypto:strong_rand_bytes(?TX_DATA_SIZE_LIMIT div 2),
	TXs = lists:map(
		fun(N) ->
			sign_tx(Key, #{ last_tx => B0#block.indep_hash,
					data => case N of 11 -> << BigChunk/binary, BigChunk/binary >>;
							_ -> BigChunk end,
					tags => [{<<"nonce">>, integer_to_binary(N)}] })
		end,
		lists:seq(1, 11)
	),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		lists:sublist(TXs, 10)
	),
	{ok, Mempool1} = ar_http_iface_client:get_mempool(slave_peer()),
	%% The transactions have the same utility therefore they are sorted in the
	%% order of submission.
	?assertEqual([TX#tx.id || TX <- lists:sublist(TXs, 10)], Mempool1),
	Last = lists:last(TXs),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_slave(Last, false),
	{ok, Mempool2} = ar_http_iface_client:get_mempool(slave_peer()),
	%% The last TX is twice as big and twice as valuable so it replaces two
	%% other transactions in the memory pool.
	?assertEqual([Last#tx.id | [TX#tx.id || TX <- lists:sublist(TXs, 8)]], Mempool2),
	%% Strip the data out. Expect the header to be accepted.
	StrippedTX = sign_tx(Key, #{ last_tx => B0#block.indep_hash,
			data => BigChunk, tags => [{<<"nonce">>, integer_to_binary(12)}] }),
	assert_post_tx_to_slave(StrippedTX#tx{ data = <<>> }),
	{ok, Mempool3} = ar_http_iface_client:get_mempool(slave_peer()),
	?assertEqual([Last#tx.id] ++ [TX#tx.id || TX <- lists:sublist(TXs, 8)]
			++ [StrippedTX#tx.id], Mempool3).

mines_format_2_txs_without_size_limit_test_() ->
	{timeout, 60, fun mines_format_2_txs_without_size_limit/0}.

joins_network_successfully() ->
	%% Start a node and mine ?MAX_TX_ANCHOR_DEPTH blocks, some of them
	%% with transactions.
	%%
	%% Join this node by another node.
	%% Post a transaction with an outdated anchor to the new node.
	%% Expect it to be rejected.
	%%
	%% Expect all the transactions to be present on the new node.
	%%
	%% Isolate the nodes. Mine 1 block with a transaction anchoring the
	%% oldest block possible on slave. Mine a block on master so that it stops
	%% tracking the block just referenced by slave. Reconnect the nodes, mine another
	%% block with transactions anchoring the oldest block possible on slave.
	%% Expect master to fork recover successfully.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>},
		{Addr = crypto:strong_rand_bytes(32), ?AR(20), <<>>},
		{crypto:strong_rand_bytes(32), ?AR(20), <<>>}
	]),
	{_Slave, _} = slave_start(B0),
	{TXs, _} = lists:foldl(
		fun(Height, {TXs, LastTX}) ->
			{TX, AnchorType} = case rand:uniform(4) of
				1 ->
					{sign_v1_tx(Key, #{ last_tx => LastTX }), tx_anchor};
				2 ->
					{sign_v1_tx(Key,
						#{
							last_tx => get_tx_anchor(),
							tags => [{<<"nonce">>, integer_to_binary(rand:uniform(100))}]
						}
					), block_anchor};
				3 ->
					{sign_tx(Key, #{ last_tx => LastTX, target => Addr }), tx_anchor};
				4 ->
					{sign_tx(Key,
						#{
							last_tx => get_tx_anchor(),
							tags => [{<<"nonce">>, integer_to_binary(rand:uniform(100))}]
						}
					), block_anchor}
			end,
			assert_post_tx_to_slave(TX),
			slave_mine(),
			assert_slave_wait_until_height(Height),
			ar_util:do_until(
				fun() ->
					slave_call(ar_node, get_pending_txs, []) == []
				end,
				200,
				1000
			),
			{TXs ++ [{TX, AnchorType}], TX#tx.id}
		end,
		{[], <<>>},
		lists:seq(1, ?MAX_TX_ANCHOR_DEPTH)
	),
	_Master = join_on_slave(),
	BI = slave_call(ar_node, get_block_index, []),
	assert_wait_until_block_block_index(BI),
	TX1 = sign_tx(Key, #{ last_tx => element(1, lists:nth(?MAX_TX_ANCHOR_DEPTH + 1, BI)) }),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		post_tx_to_master(TX1),
	%% Expect transactions to be on master.
	lists:foreach(
		fun({TX, _}) ->
			?assert(
				ar_util:do_until(
					fun() ->
						get_tx_confirmations(master, TX#tx.id) > 0
					end,
					100,
					20000
				)
			)
		end,
		TXs
	),
	lists:foreach(
		fun({TX, AnchorType}) ->
			Reply = post_tx_to_master(TX),
			case AnchorType of
				tx_anchor ->
					?assertMatch(
						{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}},
						Reply
					);
				block_anchor ->
					RecentBHL = lists:sublist(?BI_TO_BHL(BI), ?MAX_TX_ANCHOR_DEPTH),
					case lists:member(TX#tx.last_tx, RecentBHL) of
						true ->
							?assertMatch(
								{ok, {{<<"400">>, _}, _,
									<<"Transaction is already on the weave.">>, _, _}},
								Reply
							);
						false ->
							?assertMatch(
								{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}},
								Reply
							)
					end	
			end
		end,
		TXs
	),
	disconnect_from_slave(),
	TX2 = sign_tx(Key, #{ last_tx => element(1, lists:nth(?MAX_TX_ANCHOR_DEPTH, BI)) }),
	assert_post_tx_to_master(TX2),
	ar_node:mine(),
	wait_until_height(?MAX_TX_ANCHOR_DEPTH + 1),
	connect_to_slave(),
	TX3 = sign_tx(Key, #{ last_tx => element(1, lists:nth(?MAX_TX_ANCHOR_DEPTH, BI)) }),
	assert_post_tx_to_slave(TX3),
	slave_mine(),
	BI2 = assert_slave_wait_until_height(?MAX_TX_ANCHOR_DEPTH + 1),
	TX4 = sign_tx(Key, #{ last_tx => element(1, lists:nth(?MAX_TX_ANCHOR_DEPTH, BI2)) }),
	assert_post_tx_to_slave(TX4),
	assert_wait_until_receives_txs([TX4]),
	slave_mine(),
	BI3 = assert_slave_wait_until_height(?MAX_TX_ANCHOR_DEPTH + 2),
	BI3 = wait_until_height(?MAX_TX_ANCHOR_DEPTH + 2),
	?assertEqual([TX4#tx.id], (read_block_when_stored(hd(BI3)))#block.txs),
	?assertEqual([TX3#tx.id], (read_block_when_stored(hd(BI2)))#block.txs).

recovers_from_forks(ForkHeight) ->
	%% Mine a number of blocks with transactions on slave and master in sync,
	%% then mine another bunch independently.
	%%
	%% Mine an extra block on slave to make master fork recover to it.
	%% Expect the fork recovery to be successful.
	%%
	%% Try to replay all the past transactions on master. Expect the transactions to be rejected.
	%%
	%% Resubmit all the transactions from the orphaned fork. Expect them to be accepted
	%% and successfully mined into a block.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	{_Slave, _} = slave_start(B0),
	{_Master, _} = start(B0),
	connect_to_slave(),
	PreForkTXs = lists:foldl(
		fun(Height, TXs) ->
			TX = sign_v1_tx(
				Key,
				#{ last_tx => get_tx_anchor(), tags => [{<<"nonce">>, random_nonce()}] }
			),
			assert_post_tx_to_slave(TX),
			assert_wait_until_receives_txs([TX]),
			slave_mine(),
			BI = assert_slave_wait_until_height(Height),
			BI = wait_until_height(Height),
			slave_assert_block_txs([TX], BI),
			assert_block_txs([TX], BI),
			TXs ++ [TX]
		end,
		[],
		lists:seq(1, ForkHeight)
	),
	PostTXToMaster =
		fun() ->
			UnsignedTX = #{ last_tx => get_tx_anchor(master), tags => [{<<"nonce">>, random_nonce()}] },
			TX = case rand:uniform(2) of
				1 ->
					sign_tx(master, Key, UnsignedTX);
				2 ->
					sign_v1_tx(master, Key, UnsignedTX)
			end,
			assert_post_tx_to_master(TX),
			[TX]
		end,
	PostTXToSlave =
		fun() ->
			UnsignedTX = #{ last_tx => get_tx_anchor(), tags => [{<<"nonce">>, random_nonce()}] },
			TX = case rand:uniform(2) of
				1 ->
					sign_tx(Key, UnsignedTX);
				2 ->
					sign_v1_tx(Key, UnsignedTX)
			end,
			assert_post_tx_to_slave(TX),
			[TX]
		end,
	{MasterPostForkTXs, SlavePostForkTXs} = lists:foldl(
		fun(Height, {MasterTXs, SlaveTXs}) ->
			disconnect_from_slave(),
			UpdatedMasterTXs = MasterTXs ++ ([NewMasterTX] = PostTXToMaster()),
			ar_node:mine(),
			BI = wait_until_height(Height),
			assert_block_txs([NewMasterTX], BI),
			UpdatedSlaveTXs = SlaveTXs ++ ([NewSlaveTX] = PostTXToSlave()),
			connect_to_slave(),
			slave_mine(),
			SlaveBI = assert_slave_wait_until_height(Height),
			slave_assert_block_txs([NewSlaveTX], SlaveBI),
			{UpdatedMasterTXs, UpdatedSlaveTXs}
		end,
		{[], []},
		lists:seq(ForkHeight + 1, 9)
	),
	TX2 = sign_tx(Key, #{ last_tx => get_tx_anchor(), tags => [{<<"nonce">>, random_nonce()}] }),
	assert_post_tx_to_slave(TX2),
	assert_wait_until_receives_txs([TX2]),
	slave_mine(),
	assert_slave_wait_until_height(10),
	wait_until_height(10),
	forget_txs(
		PreForkTXs ++
		MasterPostForkTXs ++
		SlavePostForkTXs ++
		[TX2]
	),
	%% Assert pre-fork transactions, the transactions which came during
	%% fork recovery, and the freshly created transaction are in the
	%% weave.
	lists:foreach(
		fun(TX) ->
			?assert(
				ar_util:do_until(
					fun() ->
						get_tx_confirmations(master, TX#tx.id) > 0
					end,
					100,
					1000
				)
			),
			{ok, {{<<"400">>, _}, _, _, _, _}} =
				post_tx_to_master(TX)
		end,
		PreForkTXs ++ SlavePostForkTXs ++ [TX2]
	),
	%% Assert the block anchored transactions from the abandoned fork are
	%% back in the memory pool.
	lists:foreach(
		fun(TX) ->
			{ok, {{<<"208">>, _}, _, <<"Transaction already processed.">>, _, _}} =
				ar_http:req(#{
					method => post,
					peer => {127, 0, 0, 1, 1984},
					path => "/tx",
					headers => [{<<"X-P2p-Port">>, <<"1984">>}],
					body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
				})
		end,
		MasterPostForkTXs
	).

one_wallet_list_one_block_anchored_txs(Key, B0) ->
	%% Sign only after the node has started to get the correct price
	%% estimation from it.
	TX1Fun = fun() -> sign_v1_tx(Key) end,
	TX2Fun = fun() -> sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }) end,
	[TX1Fun, TX2Fun].

two_block_anchored_txs(Key, B0) ->
	%% Sign only after the node has started to get the correct price
	%% estimation from it.
	TX1Fun = fun() -> sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }) end,
	TX2Fun = fun() -> sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }) end,
	[TX1Fun, TX2Fun].

empty_tx_set(_Key, _B0) ->
	[].

block_anchor_txs_spending_balance_plus_one_more() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	TX1 = sign_v1_tx(Key, #{
		quantity => ?AR(4),
		target => crypto:strong_rand_bytes(32),
		reward => ?AR(6),
		last_tx => B0#block.indep_hash
	}),
	TX2 = sign_v1_tx(Key, #{ reward => ?AR(10), last_tx => B0#block.indep_hash }),
	ExceedBalanceTX = sign_v1_tx(Key, #{ reward => ?AR(1), last_tx => B0#block.indep_hash }),
	{B0, [TX1, TX2], ExceedBalanceTX}.

mixed_anchor_txs_spending_balance_plus_one_more() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	TX1 = sign_v1_tx(Key, #{
		quantity => ?AR(4),
		reward => ?AR(6),
		target => crypto:strong_rand_bytes(32)
	}),
	TX2 = sign_v1_tx(Key, #{ reward => ?AR(5), last_tx => B0#block.indep_hash }),
	TX3 = sign_v1_tx(Key, #{ reward => ?AR(2), last_tx => B0#block.indep_hash }),
	TX4 = sign_v1_tx(Key, #{ reward => ?AR(3), last_tx => B0#block.indep_hash }),
	ExceedBalanceTX = sign_v1_tx(Key, #{ reward => ?AR(1), last_tx => B0#block.indep_hash }),
	{B0, [TX1, TX2, TX3, TX4], ExceedBalanceTX}.

grouped_txs() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Wallets = [
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	],
	[B0] = ar_weave:init(Wallets),
	Chunk1 = random_v1_data(?TX_DATA_SIZE_LIMIT),
	Chunk2 = <<"a">>,
	Rate = ?INITIAL_USD_TO_AR_PRE_FORK_2_5,
	Size1 = ar_tx:get_weave_size_increase(byte_size(Chunk1), 1),
	TX1 =
		sign_v1_tx(Key1, #{
			reward => ar_tx:get_tx_fee(Size1, Rate, 0, B0#block.timestamp),
			data => Chunk1,
			last_tx => <<>>
		}),
	Size2 = ar_tx:get_weave_size_increase(byte_size(Chunk2), 2),
	TX2 =
		sign_v1_tx(Key2, #{
			reward => ar_tx:get_tx_fee(Size2, Rate, 0, B0#block.timestamp),
			data => Chunk2,
			last_tx => B0#block.indep_hash
		}),
	%% TX1 is expected to be mined first because wallet list anchors are mined first while
	%% the price per byte should be the same since we assigned the minimum required fees.
	{B0, [[TX1], [TX2]]}.

slave_mine_blocks(TargetHeight) ->
	slave_mine_blocks(1, TargetHeight).

slave_mine_blocks(Height, TargetHeight) when Height == TargetHeight + 1 ->
	ok;
slave_mine_blocks(Height, TargetHeight) ->
	slave_mine(),
	assert_slave_wait_until_height(Height),
	slave_mine_blocks(Height + 1, TargetHeight).

forget_txs(TXs) ->
	lists:foreach(
		fun(TX) ->
			ets:delete(ignored_ids, TX#tx.id)
		end,
		TXs
	).

slave_assert_block_txs(TXs, BI) ->
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	B = slave_call(ar_test_node, read_block_when_stored, [hd(BI)]),
	?assertEqual(lists:sort(TXIDs), lists:sort(B#block.txs)).

assert_block_txs(TXs, BI) ->
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	B = read_block_when_stored(hd(BI)),
	?assertEqual(lists:sort(TXIDs), lists:sort(B#block.txs)).

random_nonce() ->
	integer_to_binary(rand:uniform(1000000)).
