-module(ar_node_tests).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Ensure that the hieght of the node can be correctly obtained externally.
get_height_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{Node1, _} = ar_test_node:start(B0),
	0 = ar_node:get_height(Node1),
	ar_node:mine(Node1),
	ar_test_node:wait_until_height(Node1, 1).

%% @doc Test retrieval of the current block hash.
get_current_block_hash_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{Node1, _} = ar_test_node:start(B0),
	?assertEqual(B0#block.indep_hash, ar_node:get_current_block_hash(Node1)).

%% @doc Ensure that nodes will not re-gossip txs more than once.
single_tx_regossip_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{Node1, _} = ar_test_node:start(B0),
	ar_node:add_peers(Node1, self()),
	InitGS = ar_gossip:init([Node1]),
	TX = ar_tx:new(<<"TEST DATA">>),
	% Send transaction first time.
	ar_gossip:send(InitGS, {add_tx, TX}),
	receive
		#gs_msg{data = {add_tx, TX1}} ->
			?assertEqual(TX, TX1)
	end,
	% Send transaction second time.
	ar_gossip:send(InitGS, {add_tx, TX}),
	receive
		#gs_msg{data = {add_tx, TX}} ->
			error("TX re-gossiped")
	after 1000 ->
		ok
	end.

%% @doc Run a small, non-auto-mining blockweave. Mine blocks.
tiny_network_with_reward_pool_test() ->
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	{Node1, _} = ar_test_node:start(B0),
	ar_test_node:slave_start(B0),
	Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
	ar_node:add_peers(Node1, Node2),
	ar_node:set_reward_addr(Node1, << 0:256 >>),
	ar_node:mine(Node1),
	ar_test_node:wait_until_height(Node1, 1),
	ar_node:mine(Node1),
	ar_test_node:wait_until_height(Node1, 2),
	ar_test_node:wait_until_height(Node1, 2),
	Bs2 = ar_test_node:slave_call(ar_node, get_blocks, [Node2]),
	2 = (hd(ar_storage:read_block(Bs2)))#block.height.

%% @doc Ensure that a 'claimed' block triggers a non-zero mining reward.
mining_reward_test() ->
	{_Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([]),
	{Node1, _} = ar_test_node:start(B0, ar_wallet:to_address(Pub1)),
	ar_node:mine(Node1),
	ar_test_node:wait_until_height(Node1, 1),
	?assert(ar_node:get_balance(Node1, Pub1) > 0).

%% @doc Check that other nodes accept a new block and associated mining reward.
multi_node_mining_reward_test_() ->
	{timeout, 20, fun() ->
		{_Priv1, Pub1} = ar_wallet:new(),
		[B0] = ar_weave:init([]),
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0, ar_wallet:to_address(Pub1)),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node2, Node1),
		ar_node:mine(Node2),
		ar_test_node:wait_until_height(Node1, 1),
		?assert(ar_node:get_balance(Node1, Pub1) > 0)
	end}.

%% @doc Ensure that TX replay attack mitigation works.
replay_attack_test_() ->
	{timeout, 120, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 1),
		ar_node:add_tx(Node1, SignedTX),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 2),
		?assertEqual(?AR(8999), ar_node:get_balance(Node2, Pub1)),
		?assertEqual(?AR(1000), ar_node:get_balance(Node2, Pub2))
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
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 1),
		ar_test_node:wait_until_height(Node2, 1),
		?AR(999) = ar_node:get_balance(Node2, Pub1),
		?AR(9000) = ar_node:get_balance(Node2, Pub2)
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
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node1, Node2),
		ar_node:add_peers(Node2, Node1),
		ar_node:add_tx(Node1, SignedTX),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 1),
		ar_node:add_tx(Node2, SignedTX2),
		ar_test_node:wait_until_receives_txs(Node2, [SignedTX2]),
		ar_node:mine(Node2),
		ar_test_node:wait_until_height(Node1, 2),
		?AR(999) = ar_node:get_balance(Node1, Pub1),
		?AR(8499) = ar_node:get_balance(Node1, Pub2),
		?AR(500) = ar_node:get_balance(Node1, Pub3)
	end}.

%% @doc Wallet0 -> Wallet1 { with tags } | mine | check
mine_tx_with_key_val_tags_test_() ->
	{timeout, 10, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node1, Node2),
		ar_storage:write_tx([SignedTX]),
		ar_node:add_tx(Node1, SignedTX),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node2, 1),
		[{B1Hash, _, _} | _] = ar_node:get_blocks(Node2),
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
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 1),
		ar_node:add_tx(Node1, SignedTX2),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX2]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 2),
		?AR(7998) = ar_node:get_balance(Node2, Pub1),
		?AR(2000) = ar_node:get_balance(Node2, Pub2)
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
		{Node1, _} = ar_test_node:start(B0),
		ar_test_node:slave_start(B0),
		Node2 = {http_entrypoint_node, 'slave@127.0.0.1'},
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node1, 1),
		ar_node:add_tx(Node1, SignedTX2),
		ar_test_node:wait_until_receives_txs(Node1, [SignedTX2]),
		ar_node:mine(Node1),
		ar_test_node:wait_until_height(Node2, 2),
		?AR(8999) = ar_node:get_balance(Node2, Pub1),
		?AR(1000) = ar_node:get_balance(Node2, Pub2)
	end}.
