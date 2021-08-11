-module(ar_node_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(
	ar_test_node, [
		slave_mine/0,
		assert_wait_until_receives_txs/1, assert_slave_wait_until_height/1,
		slave_call/3, slave_add_tx/1, add_peer/1
	]
).

%% @doc Ensure that the hieght of the node can be correctly obtained externally.
get_height_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{_Node1, _} = ar_test_node:start(B0),
	0 = ar_node:get_height(),
	ar_node:mine(),
	ar_test_node:wait_until_height(1).

%% @doc Test retrieval of the current block hash.
get_current_block_hash_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{_Node1, _} = ar_test_node:start(B0),
	?assertEqual(B0#block.indep_hash, ar_node:get_current_block_hash()).

%% @doc Ensure that a 'claimed' block triggers a non-zero mining reward.
mining_reward_test() ->
	{_Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([]),
	{_Node1, _} = ar_test_node:start(B0, ar_wallet:to_address(Pub1)),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	?assert(ar_node:get_balance(Pub1) > 0).

%% @doc Check that other nodes accept a new block and associated mining reward.
multi_node_mining_reward_test_() ->
	{timeout, 20, fun() ->
		{_Priv1, Pub1} = ar_wallet:new(),
		[B0] = ar_weave:init([]),
		ar_test_node:start(B0),
		ar_test_node:slave_start(B0, ar_wallet:to_address(Pub1)),
		slave_call(ar_test_node, add_peer, [ar_meta_db:get(port)]),
		slave_mine(),
		ar_test_node:wait_until_height(1),
		?assert(ar_node:get_balance(Pub1) > 0)
	end}.

%% @doc Ensure that TX replay attack mitigation works.
replay_attack_test_() ->
	{timeout, 120, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		add_peer(slave_call(ar_meta_db, get, [port])),
		ar_node:add_tx(SignedTX),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_node:add_tx(SignedTX),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
		?assertEqual(?AR(8999), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(1000), slave_call(ar_node, get_balance, [Pub2]))
	end}.

%% @doc Create two new wallets and a blockweave with a wallet balance.
%% Create and verify execution of a signed exchange of value tx.
wallet_transaction_test_() ->
	{timeout, 60, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		add_peer(slave_call(ar_meta_db, get, [port])),
		ar_node:add_tx(SignedTX),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_test_node:slave_wait_until_height(1),
		?assertEqual(?AR(999), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(9000), slave_call(ar_node, get_balance, [Pub2]))
	end}.

%% @doc Wallet0 -> Wallet1 | mine | Wallet1 -> Wallet2 | mine | check
wallet_two_transaction_test_() ->
	{timeout, 60, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
		SignedTX2 = ar_tx:sign_v1(TX2, Priv2, Pub2),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
		ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		add_peer(slave_call(ar_meta_db, get, [port])),
		slave_call(ar_test_node, add_peer, [ar_meta_db:get(port)]),
		ar_node:add_tx(SignedTX),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		slave_add_tx(SignedTX2),
		slave_call(ar_test_node, wait_until_receives_txs, [[SignedTX2]]),
		slave_mine(),
		ar_test_node:wait_until_height(2),
		?AR(999) = ar_node:get_balance(Pub1),
		?AR(8499) = ar_node:get_balance(Pub2),
		?AR(500) = ar_node:get_balance(Pub3)
	end}.

%% @doc Wallet0 -> Wallet1 { with tags } | mine | check
mine_tx_with_key_val_tags_test_() ->
	{timeout, 10, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
		{_Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		add_peer(slave_call(ar_meta_db, get, [port])),
		ar_storage:write_tx([SignedTX]),
		ar_node:add_tx(SignedTX),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_node:mine(),
		ar_test_node:slave_wait_until_height(1),
		[{B1Hash, _, _} | _] = slave_call(ar_node, get_blocks, []),
		#block { txs = TXs } = ar_storage:read_block(B1Hash),
		?assertEqual([SignedTX], ar_storage:read_tx(TXs))
	end}.

%% @doc Ensure that TX Id threading functions correctly (in the positive case).
tx_threading_test_() ->
	{timeout, 60, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub2, ?AR(1), ?AR(1000), SignedTX#tx.id),
		SignedTX2 = ar_tx:sign_v1(TX2, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		add_peer(slave_call(ar_meta_db, get, [port])),
		ar_node:add_tx(SignedTX),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_node:add_tx(SignedTX2),
		ar_test_node:wait_until_receives_txs([SignedTX2]),
		ar_node:mine(),
		ar_test_node:wait_until_height(2),
		?assertEqual(?AR(7998), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(2000), slave_call(ar_node, get_balance, [Pub2]))
	end}.

%% @doc Ensure that TX Id threading functions correctly (in the negative case).
bogus_tx_thread_test_() ->
	{timeout, 60, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		TX2 = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<"INCORRECT TX ID">>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		SignedTX2 = ar_tx:sign_v1(TX2, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		add_peer(slave_call(ar_meta_db, get, [port])),
		ar_node:add_tx(SignedTX),
		ar_test_node:wait_until_receives_txs([SignedTX]),
		ar_node:mine(),
		ar_test_node:wait_until_height(1),
		ar_node:add_tx(SignedTX2),
		ar_test_node:wait_until_receives_txs([SignedTX2]),
		ar_node:mine(),
		ar_test_node:slave_wait_until_height(2),
		?assertEqual(?AR(8999), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(1000), slave_call(ar_node, get_balance, [Pub2]))
	end}.

persisted_mempool_test_() ->
	{timeout, 10, fun test_persisted_mempool/0}.

test_persisted_mempool() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:slave_start(B0),
	ar_test_node:disconnect_from_slave(),
	%% Pause the distribution queue so that transactions do not become ready for mining -
	%% it happens very quickly in debug mode.
	ar_tx_queue:set_pause(true),
	SignedTX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(master) }),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_test_node:post_tx_to_master(SignedTX, false),
	true = ar_util:do_until(
		fun() ->
			maps:is_key(SignedTX#tx.id, ar_node:get_pending_txs([as_map]))
		end,
		100,
		1000
	),
	ok = application:stop(arweave),
	ok = ar:stop_dependencies(),
	%% Rejoin the network. Expect the pending transactions to be picked up and distributed.
	{ok, Config} = application:get_env(arweave, config),
	ok = application:set_env(arweave, config, Config#config{
		start_from_block_index = false,
		peers = [{127, 0, 0, 1, ar_test_node:slave_call(ar_meta_db, get, [port])}]
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	ar_test_node:wait_until_joined(),
	assert_wait_until_receives_txs([SignedTX]),
	ar_node:mine(),
	[{H, _, _} | _] = assert_slave_wait_until_height(1),
	B = ar_test_node:read_block_when_stored(H),
	?assertEqual([SignedTX#tx.id], B#block.txs).
