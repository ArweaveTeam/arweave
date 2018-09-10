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
		Msg1 when is_record(Msg1, gs_msg) ->
			{add_tx, TX1} = Msg1#gs_msg.data,
			?assertEqual(TX, TX1)
	end,
	% Send transaction second time.
	ar_gossip:send(InitGS, {add_tx, TX}),
	receive
		Msg2 when is_record(Msg2, gs_msg) ->
			{add_tx, TX2} = Msg2#gs_msg.data,
			?assertNotEqual(TX, TX2)
	after 1000 ->
		ok
	end.

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

%% @doc Check that blocks can be added (if valid) by external processes.
add_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node1 = ar_node:start([], [B0]),
	[B1 | _] = ar_weave:add([B0]),
	ar_node:add_block(Node1, B1, B0),
	Blocks = lists:map(fun(B) -> B#block.indep_hash end, [B1, B0]),
	timer:sleep(1000),
	?assertEqual(ar_node:get_blocks(Node1), Blocks).

%% @doc Ensure that bogus blocks are not accepted onto the network.
add_bogus_block_test() ->
	ar_storage:clear(),
	ar_storage:write_tx(
		[
			TX1 = ar_tx:new(<<"HELLO WORLD">>),
			TX2 = ar_tx:new(<<"NEXT BLOCK.">>)
		]
	),
	Node = ar_node:start(),
	GS0 = ar_gossip:init([Node]),
	B0 = ar_weave:init([]),
	ar_storage:write_block(B0),
	B1 = ar_weave:add(B0, [TX1]),
	LastB = hd(B1),
	ar_storage:write_block(hd(B1)),
	BL = [hd(B1), hd(B0)],
	Node ! {replace_block_list, BL},
	Bs = [B2|_] = ar_weave:add(B1, [TX2]),
	ar_storage:write_block(B2),
	ar_gossip:send(GS0,
		{
			new_block,
			self(),
			B2#block.height,
			B2#block { hash = <<"INCORRECT">> },
			ar_node_utils:find_recall_block(Bs)
		}),
	?assert(ar_util:do_until(
		fun() ->
			[RecvdB | _] = ar_node:get_blocks(Node),
			LastB == ar_storage:read_block(RecvdB, B2#block.hash_list)
		end,
		500,
		4000
	)).

%% @doc Ensure that blocks with incorrect nonces are not accepted onto
%% the network.
add_bogus_block_nonce_test() ->
	ar_storage:clear(),
	ar_storage:write_tx(
		[
			TX1 = ar_tx:new(<<"HELLO WORLD">>),
			TX2 = ar_tx:new(<<"NEXT BLOCK.">>)
		]
	),
	Node = ar_node:start(),
	GS0 = ar_gossip:init([Node]),
	B0 = ar_weave:init([]),
	ar_storage:write_block(B0),
	B1 = ar_weave:add(B0, [TX1]),
	LastB = hd(B1),
	ar_storage:write_block(hd(B1)),
	BL = [hd(B1), hd(B0)],
	Node ! {replace_block_list, BL},
	B2 = ar_weave:add(B1, [TX2]),
	ar_storage:write_block(hd(B2)),
	ar_gossip:send(GS0,
		{new_block,
			self(),
			(hd(B2))#block.height,
			(hd(B2))#block { nonce = <<"INCORRECT">> },
			ar_node_utils:find_recall_block(B2)
		}
	),
	?assert(ar_util:do_until(
		fun() ->
			[RecvdB | _] = ar_node:get_blocks(Node),
			LastB == ar_storage:read_block(RecvdB, (hd(B2))#block.hash_list)
		end,
		500,
		4000
	)).

%% @doc Ensure that blocks with bogus hash lists are not accepted by the network.
add_bogus_hash_list_test() ->
	ar_storage:clear(),
	ar_storage:write_tx(
		[
			TX1 = ar_tx:new(<<"HELLO WORLD">>),
			TX2 = ar_tx:new(<<"NEXT BLOCK.">>)
		]
	),
	Node = ar_node:start(),
	GS0 = ar_gossip:init([Node]),
	B0 = ar_weave:init([]),
	ar_storage:write_block(B0),
	B1 = ar_weave:add(B0, [TX1]),
	LastB = hd(B1),
	ar_storage:write_block(hd(B1)),
	BL = [hd(B1), hd(B0)],
	Node ! {replace_block_list, BL},
	B2 = ar_weave:add(B1, [TX2]),
	ar_storage:write_block(hd(B2)),
	ar_gossip:send(GS0,
		{new_block,
			self(),
			(hd(B2))#block.height,
			(hd(B2))#block {
				hash_list =
					[<<"INCORRECT HASH">> | tl((hd(B2))#block.hash_list)]
			},
			ar_node_utils:find_recall_block(B2)
		}),
	?assert(ar_util:do_until(
		fun() ->
			[RecvdB | _] = ar_node:get_blocks(Node),
			LastB == ar_storage:read_block(RecvdB, (hd(B2))#block.hash_list)
		end,
		500,
		4000
	)).

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
	timer:sleep(3000),
	Bs = ar_node:get_blocks(Node2),
	TestDataID	= TestData#tx.id,
	[TestDataID] = (hd(ar_storage:read_block(Bs, Bs)))#block.txs.

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
		% Test data 1.
		ar_node:add_tx(ar_util:pick_random(Nodes), TestData1),
		timer:sleep(1000),
		ar_node:mine(ar_util:pick_random(Nodes)),
		BNode = ar_util:pick_random(Nodes),
		{ok, B1} = ar_util:do_until(
			fun() ->
				Bs = ar_node:get_blocks(BNode),
				if
					length(Bs) == 2 -> {ok, Bs};
					true            -> false
				end
			end,
			500,
			30000
		),
		% Test data 2.
		ar_node:add_tx(ar_util:pick_random(Nodes), TestData2),
		timer:sleep(1000),
		ar_node:mine(ar_util:pick_random(Nodes)),
		{ok, B2} = ar_util:do_until(
			fun() ->
				Bs = ar_node:get_blocks(BNode),
				if
					length(Bs) == 3 -> {ok, Bs};
					true            -> false
				end
			end,
			500,
			30000
		),
		TestDataID1 = TestData1#tx.id,
		TestDataID2 = TestData2#tx.id,
		% BHL = ar_node:get_hash_list(ar_util:pick_random(Nodes)),
		BHL = ar_node:get_hash_list(BNode),
		?assertEqual([TestDataID1], (hd(ar_storage:read_block(B1, BHL)))#block.txs),
		?assertEqual([TestDataID2], (hd(ar_storage:read_block(B2, BHL)))#block.txs)
	end}.

%% @doc Setup a network, mine a block, cause one node to forget that block.
%% Ensure that the 'truncated' node can still verify and accept new blocks.
tiny_collaborative_blockweave_mining_test_() ->
	{timeout, 120, fun() ->
		ar_storage:clear(),
		B0 = ar_weave:init([]),
		Node1 = ar_node:start([], B0),
		Node2 = ar_node:start([Node1], B0),
		ar_node:add_peers(Node1, Node2),
		timer:sleep(500),
		ar_node:mine(Node1), % Mine B1
		timer:sleep(500),
		ar_node:mine(Node1), % Mine B2
		timer:sleep(500),
		ar_node:truncate(Node1),
		ar_node:mine(Node2), % Mine B3
		?assert(ar_util:do_until(
			fun() ->
				B3 = ar_node:get_blocks(Node1),
				RB3 = ar_storage:read_block(B3, B3),
				HdRB3 = hd(RB3),
				HdRB3#block.height == 3
			end,
			1000,
			60000
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
multi_node_mining_reward_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Node1 = ar_node:start([], B0 = ar_weave:init([])),
	Node2 = ar_node:start([Node1], B0, 0, ar_wallet:to_address(Pub1)),
	ar_node:mine(Node2),
	timer:sleep(2000),
	?assert(ar_node:get_balance(Node1, Pub1) > 0).

%% @doc Ensure that TX replay attack mitigation works.
replay_attack_test() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
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
	?assertEqual(?AR(1000), ar_node:get_balance(Node2, Pub2)).

%% @doc Ensure last_tx functions after block mine.
last_tx_test() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
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
	?assertEqual(ar_node:get_last_tx(Node2, Pub1), ID).

%%%
%%% Embedded tests.
%%%

%% @doc Ensure that a set of txs can be checked for serialization, those that
%% don't serialize disregarded.
filter_out_of_order_txs_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	RawTX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
	TX = RawTX#tx {owner = Pub1},
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	RawTX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
	TX2 = RawTX2#tx {owner = Pub1},
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
	WalletList =
		[
			{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
			{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
			{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
		],
	% TX1 applied, TX2 applied
	{_, [SignedTX2, SignedTX]} =
		ar_node_utils:filter_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX2]
		),
	% TX2 disregarded, TX1 applied
	{_, [SignedTX]} =
		ar_node_utils:filter_out_of_order_txs(
			WalletList,
			[SignedTX2, SignedTX]
		).

%% @doc Ensure that a large set of txs can be checked for serialization,
%% those that don't serialize disregarded.
filter_out_of_order_txs_large_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
	TX3 = ar_tx:new(Pub3, ?AR(1), ?AR(50), SignedTX2#tx.id),
	SignedTX3 = ar_tx:sign(TX3, Priv1, Pub1),
	WalletList =
		[
			{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
			{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
			{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
		],
	% TX1 applied, TX2 applied, TX3 applied
	{_, [SignedTX3, SignedTX2, SignedTX]} =
		ar_node_utils:filter_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX2, SignedTX3]
		),
	% TX2 disregarded, TX3 disregarded, TX1 applied
	{_, [SignedTX]} =
		ar_node_utils:filter_out_of_order_txs(
			WalletList,
			[SignedTX2, SignedTX3, SignedTX]
		),
	% TX1 applied, TX3 disregarded, TX2 applied.
	{_, [SignedTX2, SignedTX]} =
		ar_node_utils:filter_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX3, SignedTX2]
		).

%% @doc Ensure that a set of txs can be serialized in the best possible order.
filter_all_out_of_order_txs_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
	WalletList =
		[
			{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
			{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
			{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
		],
	% TX1 applied, TX2 applied
	[SignedTX, SignedTX2] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX2]
		),
	% TX2 applied, TX1 applied
	[SignedTX, SignedTX2] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX2, SignedTX]
		).

%% @doc Ensure that a large set of txs can be serialized in the best
%% possible order.
filter_all_out_of_order_txs_large_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
	TX3 = ar_tx:new(Pub3, ?AR(1), ?AR(50), SignedTX2#tx.id),
	SignedTX3 = ar_tx:sign(TX3, Priv1, Pub1),
	TX4 = ar_tx:new(Pub1, ?AR(1), ?AR(25), <<>>),
	SignedTX4 = ar_tx:sign(TX4, Priv2, Pub2),
	WalletList =
		[
			{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
			{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
			{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
		],
	% TX1 applied, TX2 applied, TX3 applied
	[SignedTX, SignedTX2, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX2, SignedTX3]
		),
	% TX1 applied, TX3 applied, TX2 applied
	[SignedTX, SignedTX2, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX3, SignedTX2]
		),
	% TX2 applied, TX1 applied, TX3 applied
	[SignedTX, SignedTX2, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX2, SignedTX, SignedTX3]
		),
	% TX2 applied, TX3 applied, TX1 applied
	[SignedTX, SignedTX2, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX2, SignedTX3, SignedTX]
		),
	% TX3 applied, TX1 applied, TX2 applied
	[SignedTX, SignedTX2, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX3, SignedTX, SignedTX2]
		),
	% TX3 applied, TX2 applied, TX1 applied
	[SignedTX, SignedTX2, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX3, SignedTX2, SignedTX]
		),
	% TX1 applied, TX1 duplicate, TX1 duplicate, TX2 applied, TX4 applied
	% TX1 duplicate, TX3 applied
	% NB: Consider moving into separate test.
	[SignedTX, SignedTX2, SignedTX4, SignedTX3] =
		ar_node_utils:filter_all_out_of_order_txs(
			WalletList,
			[SignedTX, SignedTX, SignedTX, SignedTX2, SignedTX4, SignedTX, SignedTX3]
		).

%% @doc Create two new wallets and a blockweave with a wallet balance.
%% Create and verify execution of a signed exchange of value tx.
wallet_transaction_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
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
	?AR(9000) = ar_node:get_balance(Node2, Pub2).

%% @doc Test that a slightly larger network is able to receive data and
%% propogate data and blocks.
large_blockweave_with_data_test_slow() ->
	ar_storage:clear(),
	TestData = ar_tx:new(<<"TEST DATA">>),
	ar_storage:write_tx(TestData),
	B0 = ar_weave:init([]),
	Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 200) ],
	[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 100)) || Node <- Nodes ],
	ar_node:add_tx(ar_util:pick_random(Nodes), TestData),
	receive after 2500 -> ok end,
	ar_node:mine(ar_util:pick_random(Nodes)),
	receive after 2500 -> ok end,
	B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
	TestDataID	= TestData#tx.id,
	[TestDataID] = (hd(ar_storage:read_block(B1, B1)))#block.txs.

%% @doc Test that large networks (500 nodes) with only 1% connectivity
%% still function correctly.
large_weakly_connected_blockweave_with_data_test_slow() ->
	ar_storage:clear(),
	TestData = ar_tx:new(<<"TEST DATA">>),
	ar_storage:write_tx(TestData),
	B0 = ar_weave:init([]),
	Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 200) ],
	[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
	ar_node:add_tx(ar_util:pick_random(Nodes), TestData),
	receive after 2500 -> ok end,
	ar_node:mine(ar_util:pick_random(Nodes)),
	receive after 2500 -> ok end,
	B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
	TestDataID	= TestData#tx.id,
	[TestDataID] = (hd(ar_storage:read_block(B1, B1)))#block.txs.

%% @doc Ensure that the network can add multiple peices of data and have
%% it mined into blocks.
medium_blockweave_mine_multiple_data_test_slow() ->
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
	B0 = ar_weave:init([]),
	Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, 50) ],
	[ ar_node:add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
	ar_node:add_tx(ar_util:pick_random(Nodes), SignedTX),
	ar_node:add_tx(ar_util:pick_random(Nodes), SignedTX2),
	receive after 1500 -> ok end,
	ar_node:mine(ar_util:pick_random(Nodes)),
	receive after 1250 -> ok end,
	B1 = ar_node:get_blocks(ar_util:pick_random(Nodes)),
	true =
		lists:member(
			SignedTX#tx.id,
			(hd(ar_storage:read_block(B1, B1)))#block.txs
		),
	true =
		lists:member(
			SignedTX2#tx.id,
			(hd(ar_storage:read_block(B1, B1)))#block.txs
		).

%% @doc Wallet0 -> Wallet1 | mine | Wallet1 -> Wallet2 | mine | check
wallet_two_transaction_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
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
	?AR(500) = ar_node:get_balance(Node1, Pub3).

%% @doc Wallet1 -> Wallet2 | Wallet1 -> Wallet3 | mine | check
%% @doc Wallet1 -> Wallet2 | Wallet1 -> Wallet3 | mine | check
single_wallet_double_tx_before_mine_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	OrphanedTX = ar_tx:new(Pub2, ?AR(1), ?AR(5000), <<>>),
	OrphanedTX2 = ar_tx:new(Pub3, ?AR(1), ?AR(4000), <<>>),
	TX = OrphanedTX#tx { owner = Pub1 },
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = OrphanedTX2#tx { owner = Pub1, last_tx = SignedTX#tx.id },
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
	B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node1 = ar_node:start([], B0),
	Node2 = ar_node:start([Node1], B0),
	ar_node:add_peers(Node1, Node2),
	ar_node:add_tx(Node1, SignedTX),
	ar_storage:write_tx([SignedTX]),
	timer:sleep(500),
	ar_node:add_tx(Node1, SignedTX2),
	ar_storage:write_tx([SignedTX2]),
	timer:sleep(500),
	ar_node:mine(Node1), % Mine B1
	timer:sleep(500),
	?AR(4999) = ar_node:get_balance(Node2, Pub1),
	?AR(5000) = ar_node:get_balance(Node2, Pub2),
	?AR(0) = ar_node:get_balance(Node2, Pub3).

%% @doc Verify the behaviour of out of order TX submission.
%% NOTE: The current behaviour (out of order TXs get dropped)
%% is not necessarily the behaviour we want, but we should keep
%% track of it.
single_wallet_double_tx_wrong_order_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(5000), <<>>),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(4000), TX#tx.id),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
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
	length(CurrentB#block.txs) == 1.

%% @doc Ensure that TX Id threading functions correctly (in the positive case).
tx_threading_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub2, ?AR(1), ?AR(1000), SignedTX#tx.id),
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
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
	?AR(2000) = ar_node:get_balance(Node2, Pub2).

%% @doc Ensure that TX Id threading functions correctly (in the negative case).
bogus_tx_thread_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<>>),
	TX2 = ar_tx:new(Pub2, ?AR(1), ?AR(1000), <<"INCORRECT TX ID">>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
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
	?AR(1000) = ar_node:get_balance(Node2, Pub2).

%%%
%%% EOF
%%%

