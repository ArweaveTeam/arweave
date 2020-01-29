%%%
%%% @doc Unit tests of the node process.
%%%

-module(ar_node_tests).

-compile(export_all).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%
%%% Tests.
%%%

%% @doc Ensure that the hieght of the node can be correctly obtained externally.
get_height_test() ->
	ar_storage:clear(),
	B0 = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	Node1 = ar_node:start([self()], B0),
	0 = ar_node:get_height(Node1),
	ar_node:mine(Node1),
	timer:sleep(1000),
	1 = ar_node:get_height(Node1).

%% @doc Test retrieval of the current block hash.
get_current_block_hash_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	Node1 = ar_node:start([self()], [B0]),
	?assertEqual(B0#block.indep_hash, ar_node:get_current_block_hash(Node1)).

%% @doc Ensure that nodes will not re-gossip txs more than once.
single_tx_regossip_test() ->
	ar_storage:clear(),
	B0 = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	Node1 = ar_node:start([self()], B0),
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

%% @doc Cancel a pending tx.
%% Sends two TXs, from two different wallets, cancels one, then checks that
%% the other progressed correctly.
cancel_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		AllowedTarget = crypto:strong_rand_bytes(32),
		CancelTarget = crypto:strong_rand_bytes(32),
		AllowedTX = ar_tx:new(AllowedTarget, ?AR(1), ?AR(1000), <<>>),
		CancelTX = ar_tx:new(CancelTarget, ?AR(1), ?AR(9000), <<>>),
		% 1000 AR from Wallet1 -> AllowedTarget, 1 AR fee.
		SignedAllowedTX = ar_tx:sign_pre_fork_2_0(AllowedTX, Priv1, Pub1),
		% 9000 AR from Wallet2 -> CANCELLED.
		SignedCancelTX = ar_tx:sign_pre_fork_2_0(CancelTX, Priv2, Pub2),
		B0 =
			ar_weave:init(
				[
					{ar_wallet:to_address(Pub1), ?AR(10000), <<>>},
					{ar_wallet:to_address(Pub2), ?AR(10000), <<>>}
				]
			),
		Node1 = ar_node:start([], B0),
		ar_node:add_tx(Node1, SignedAllowedTX),
		ar_node:add_tx(Node1, SignedCancelTX),
		timer:sleep(300),
		ar_storage:write_tx(SignedAllowedTX),
		ar_storage:write_tx(SignedCancelTX),
		timer:sleep(300),
		Sig = ar_wallet:sign(Priv2, SignedCancelTX#tx.id),
		ar_node:cancel_tx(Node1, SignedCancelTX#tx.id, Sig),
		timer:sleep(300),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(300),
		?AR(8999) = ar_node:get_balance(Node1, Pub1),
		?AR(10000) = ar_node:get_balance(Node1, ar_wallet:to_address(Pub2)),
		?AR(1000) = ar_node:get_balance(Node1, AllowedTarget),
		?AR(0) = ar_node:get_balance(Node1, CancelTarget)
	end}.


%% @doc Ensure bogus TX cancellation requests are ignored.
bogus_cancel_tx_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		AllowedTarget = crypto:strong_rand_bytes(32),
		AllowedTX = ar_tx:new(AllowedTarget, ?AR(1), ?AR(1000), <<>>),
		% 1000 AR from Wallet1 -> AllowedTarget, 1 AR fee.
		SignedAllowedTX = ar_tx:sign_pre_fork_2_0(AllowedTX, Priv1, Pub1),
		B0 =
			ar_weave:init(
				[
					{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}
				]
			),
		Node1 = ar_node:start([], B0),
		ar_node:add_tx(Node1, SignedAllowedTX),
		timer:sleep(300),
		ar_storage:write_tx(SignedAllowedTX),
		timer:sleep(300),
		ar_node:cancel_tx(Node1, SignedAllowedTX#tx.id, crypto:strong_rand_bytes(512)),
		timer:sleep(300),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(300),
		?AR(8999) = ar_node:get_balance(Node1, ar_wallet:to_address(Pub1)),
		?AR(1000) = ar_node:get_balance(Node1, AllowedTarget)
	end}.

%% @doc Run a small, non-auto-mining blockweave. Mine blocks.
tiny_network_with_reward_pool_test() ->
	ar_storage:clear(),
	B0 = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	Node1 = ar_node:start([], B0),
	ar_storage:write_block(B0),
	Node2 = ar_node:start([Node1], B0),
	ar_node:set_reward_addr(Node1, << 0:256 >>),
	ar_node:add_peers(Node1, Node2),
	timer:sleep(1000),
	ar_node:mine(Node1),
	timer:sleep(1000),
	ar_node:mine(Node1),
	timer:sleep(1000),
	Bs2 = ar_node:get_blocks(Node2),
	2 = (hd(ar_storage:read_block(Bs2, Bs2)))#block.height.

%% @doc Check the current block can be retrieved
get_current_block_test() ->
	ar_storage:clear(),
	ar:report([node, test, weave_init]),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	B1 = ar_node:get_current_block(Node),
	?assertEqual(B0, B1).

%% @doc Run a small, non-auto-mining blockweave. Mine blocks.
tiny_blockweave_with_mining_test() ->
	ar_storage:clear(),
	B0 = ar_weave:init([]),
	Node1 = ar_node:start([], B0),
	ar_storage:write_block(B0),
	Node2 = ar_node:start([Node1], B0),
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	timer:sleep(2000),
	Bs = ar_node:get_blocks(Node2),
	1 = (hd(ar_storage:read_block(Bs, Bs)))#block.height.

%% @doc Ensure that the network add data and have it mined into blocks.
tiny_blockweave_with_added_data_test() ->
	{timeout, 120, fun() ->
		ar_storage:clear(),
		TestData = ar_tx:new(<<"TEST DATA">>),
		ar_storage:write_tx(TestData),
		B0 = ar_weave:init([]),
		ar_storage:write_block(B0),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		timer:sleep(500),
		ar_node:add_tx(Node2, TestData),
		timer:sleep(1000),
		ar_node:mine(Node1),
		ar_util:do_until(
			fun() ->
				BI = ar_node:get_blocks(Node2),
				BL = ar_storage:read_block(BI, BI),
				BHead = hd(BL),
				TXs = BHead#block.txs,
				TestDataID = TestData#tx.id,
				[TestDataID] == TXs
			end,
			1000,
			60000
		)
	end}.

%% @doc Ensure that the network can mine multiple blocks correctly.
medium_blockweave_multi_mine_test_() ->
	{timeout, 120, fun() ->
		ar_storage:clear(),
		TestData1 = ar_tx:new(<<"TEST DATA1">>),
		ar_storage:write_tx(TestData1),
		TestData2 = ar_tx:new(<<"TEST DATA2">>),
		ar_storage:write_tx(TestData2),
		B0 = ar_weave:init([]),
		Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 50) ],
		[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
		ar_node:add_tx(ar_util:pick_random(Nodes), TestData1),
		timer:sleep(2000),
		ar_node:mine(ar_util:pick_random(Nodes)),
		timer:sleep(2000),
		B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
		ar_node:add_tx(ar_util:pick_random(Nodes), TestData2),
		timer:sleep(2000),
		ar_node:mine(ar_util:pick_random(Nodes)),
		?assert(ar_util:do_until(
			fun() ->
				B2 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
				TestDataID1 = TestData1#tx.id,
				TestDataID2 = TestData2#tx.id,
				BI = ar_node:get_block_index(ar_util:pick_random(Nodes)),
				[TestDataID1] == (hd(ar_storage:read_block(B1, BI)))#block.txs andalso
				[TestDataID2] == (hd(ar_storage:read_block(B2, BI)))#block.txs
			end,
			1000,
			30000
		))
	end}.

%% @doc Ensure that a 'claimed' block triggers a non-zero mining reward.
mining_reward_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Node1 = ar_node:start([], ar_weave:init([]), 0, ar_wallet:to_address(Pub1)),
	ar_node:mine(Node1),
	timer:sleep(1000),
	?assert(ar_node:get_balance(Node1, Pub1) > 0).

%% @doc Check that other nodes accept a new block and associated mining reward.
multi_node_mining_reward_test_() ->
	{timeout, 20, fun() ->
		ar_storage:clear(),
		{_Priv1, Pub1} = ar_wallet:new(),
		Node1 = ar_node:start([], B0 = ar_weave:init([])),
		Node2 = ar_node:start([Node1], B0, 0, ar_wallet:to_address(Pub1)),
		ar_node:mine(Node2),
		timer:sleep(2000),
		?assert(ar_node:get_balance(Node1, Pub1) > 0)
	end}.

%% @doc Ensure that TX replay attack mitigation works.
replay_attack_test_() ->
	{timeout, 120, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_storage:write_tx(SignedTX),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(500),
		ar_node:add_tx(Node1, SignedTX),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(500),
		?assertEqual(?AR(8999), ar_node:get_balance(Node2, Pub1)),
		?assertEqual(?AR(1000), ar_node:get_balance(Node2, Pub2))
	end}.

%% @doc Ensure last_tx functions after block mine.
last_tx_test_() ->
	{timeout, 20, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		ID = SignedTX#tx.id,
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		timer:sleep(500),
		ar_node:add_tx(Node1, SignedTX),
		timer:sleep(500),
		ar_storage:write_tx(SignedTX),
		timer:sleep(500),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(500),
		?assertEqual(?OK(ar_node:get_last_tx(Node2, Pub1)), ID)
	end}.

%%%
%%% Embedded tests.
%%%

%% @doc Create two new wallets and a blockweave with a wallet balance.
%% Create and verify execution of a signed exchange of value tx.
wallet_transaction_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		timer:sleep(300),
		ar_storage:write_tx(SignedTX),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(300),
		?AR(999) = ar_node:get_balance(Node2, Pub1),
		?AR(9000) = ar_node:get_balance(Node2, Pub2)
	end}.

%% @doc Test that a slightly larger network is able to receive data and
%% propogate data and blocks.
large_blockweave_with_data_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		TestData = ar_tx:new(<<"TEST DATA">>),
		ar_storage:write_tx(TestData),
		B0 = ar_weave:init([]),
		Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 200) ],
		[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 100)) || Node <- Nodes ],
		ar_node:add_tx(ar_util:pick_random(Nodes), TestData),
		receive after 2500 -> ok end,
		ar_node:mine(ar_util:pick_random(Nodes)),
		?assert(ar_util:do_until(
			fun() ->
				B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
				TestDataID = TestData#tx.id,
				[TestDataID] == (hd(ar_storage:read_block(B1, B1)))#block.txs
			end,
			1000,
			30000
		))
	end}.

%% @doc Test that large networks (500 nodes) with only 1% connectivity
%% still function correctly.
large_weakly_connected_blockweave_with_data_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		TestData = ar_tx:new(<<"TEST DATA">>),
		ar_storage:write_tx(TestData),
		B0 = ar_weave:init([]),
		Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 200) ],
		[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
		ar_node:add_tx(ar_util:pick_random(Nodes), TestData),
		receive after 2500 -> ok end,
		ar_node:mine(ar_util:pick_random(Nodes)),
		?assert(ar_util:do_until(
			fun() ->
				B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
				TestDataID = TestData#tx.id,
				[TestDataID] == (ar_storage:read_block(hd(B1), B1))#block.txs
			end,
			1000,
			30000
		))
	end}.

%% @doc Ensure that the network can add multiple peices of data and have
%% it mined into blocks.
medium_blockweave_mine_multiple_data_test_() ->
	{timeout, 60, fun() ->
		{Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
		SignedTX2 = ar_tx:sign_pre_fork_2_0(TX2, Priv2, Pub2),
		B0 = ar_weave:init([
			{ar_wallet:to_address(Pub1), ?AR(10000), <<>>},
			{ar_wallet:to_address(Pub2), ?AR(10000), <<>>},
			{ar_wallet:to_address(Pub3), ?AR(10000), <<>>}
		]),
		Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 50) ],
		[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
		ar_node:add_tx(ar_util:pick_random(Nodes), SignedTX),
		ar_node:add_tx(ar_util:pick_random(Nodes), SignedTX2),
		receive after 1500 -> ok end,
		ar_node:mine(ar_util:pick_random(Nodes)),
		?assert(ar_util:do_until(
			fun() ->
				B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
				true ==
					lists:member(
						SignedTX#tx.id,
						(hd(ar_storage:read_block(B1, B1)))#block.txs
					) andalso
				true ==
					lists:member(
						SignedTX2#tx.id,
						(hd(ar_storage:read_block(B1, B1)))#block.txs
					)
			end,
			1000,
			30000
		))
	end}.

%% @doc Wallet0 -> Wallet1 | mine | Wallet1 -> Wallet2 | mine | check
wallet_two_transaction_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
		SignedTX2 = ar_tx:sign_pre_fork_2_0(TX2, Priv2, Pub2),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_storage:write_tx([SignedTX]),
		timer:sleep(300),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(1000),
		ar_node:add_tx(Node2, SignedTX2),
		ar_storage:write_tx([SignedTX2]),
		timer:sleep(1000),
		ar_node:mine(Node2), % Mine B2
		timer:sleep(300),
		?AR(999) = ar_node:get_balance(Node1, Pub1),
		?AR(8499) = ar_node:get_balance(Node1, Pub2),
		?AR(500) = ar_node:get_balance(Node1, Pub3)
	end}.

%% @doc Wallet0 -> Wallet1 { with tags } | mine | check
mine_tx_with_key_val_tags_test_() ->
	{timeout, 10, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_storage:write_tx([SignedTX]),
		ar_node:add_tx(Node1, SignedTX),
		timer:sleep(300),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(1000),
		BI = [{B1Hash, _}|_] = ar_node:get_blocks(Node2),
		#block { txs = TXs } = ar_storage:read_block(B1Hash, BI),
		?assertEqual(ar_storage:read_tx(TXs), [SignedTX])
	end}.

%% @doc Verify the behaviour of out of order TX submission.
%% NOTE: The current behaviour (out of order TXs get dropped)
%% is not necessarily the behaviour we want, but we should keep
%% track of it.
single_wallet_double_tx_wrong_order_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(5000), <<>>),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(4000), TX#tx.id),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		SignedTX2 = ar_tx:sign_pre_fork_2_0(TX2, Priv1, Pub1),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX2),
		timer:sleep(500),
		ar_node:add_tx(Node1, SignedTX),
		ar_storage:write_tx([SignedTX]),
		timer:sleep(500),
		ar_node:mine(Node1), % Mine B1
		receive after 200 -> ok end,
		?AR(4999) = ar_node:get_balance(Node2, Pub1),
		?AR(5000) = ar_node:get_balance(Node2, Pub2),
		?AR(0) = ar_node:get_balance(Node2, Pub3),
		CurrentB = ar_node:get_current_block(whereis(http_entrypoint_node)),
		length(CurrentB#block.txs) == 1
	end}.

%% @doc Ensure that TX Id threading functions correctly (in the positive case).
tx_threading_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub2, ?AR(1), ?AR(1000), SignedTX#tx.id),
		SignedTX2 = ar_tx:sign_pre_fork_2_0(TX2, Priv1, Pub1),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_storage:write_tx([SignedTX,SignedTX2]),
		timer:sleep(500),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(300),
		ar_node:add_tx(Node1, SignedTX2),
		timer:sleep(500),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(1000),
		?AR(7998) = ar_node:get_balance(Node2, Pub1),
		?AR(2000) = ar_node:get_balance(Node2, Pub2)
	end}.

%% @doc Ensure that TX Id threading functions correctly (in the negative case).
bogus_tx_thread_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
		TX2 = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<"INCORRECT TX ID">>),
		SignedTX = ar_tx:sign_pre_fork_2_0(TX, Priv1, Pub1),
		SignedTX2 = ar_tx:sign_pre_fork_2_0(TX2, Priv1, Pub1),
		B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		ar_node:add_tx(Node1, SignedTX),
		ar_storage:write_tx([SignedTX,SignedTX2]),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(500),
		ar_node:add_tx(Node1, SignedTX2),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(500),
		?AR(8999) = ar_node:get_balance(Node2, Pub1),
		?AR(1000) = ar_node:get_balance(Node2, Pub2)
	end}.
