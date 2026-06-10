-module(ar_tx_anchor_tests).
-test_peers([peer1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").


accepts_at_most_one_wallet_list_anchored_tx_per_block_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_accepts_at_most_one_wallet_list_anchored_tx_per_block/0}.

rejects_txs_with_outdated_anchors_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun() ->
		%% A TX anchoring the block one past get_max_tx_anchor_depth() is rejected.
		Key = {_, Pub} = ar_wallet:new(),
		[B0] = ar_weave:init([
			{ar_wallet:to_address(Pub), ?AR(20), <<>>}
		]),
		_ = ar_test_node:start_peer(peer1, B0),
		ar_tx_test_utils:mine_blocks(peer1, ar_block:get_max_tx_anchor_depth()),
		?assertMatch({ok, _}, ar_test_await:node_height(peer1, ar_block:get_max_tx_anchor_depth())),
		TX1 = ar_test_node:sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }),
		{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
			ar_test_node:post_tx_to_peer(peer1, TX1)
	end}.

test_accepts_at_most_one_wallet_list_anchored_tx_per_block() ->
	%% Only one wallet-list-anchored TX is allowed per block: a TX chaining a
	%% still-in-mempool TX is rejected, but a block-anchored TX is accepted and mined.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	_ = ar_test_node:start_peer(peer1, B0),
	_ = ar_test_node:connect_to_peer(peer1),
	TX1 = ar_test_node:sign_v1_tx(Key),
	ar_test_node:assert_post_tx_to_peer(peer1, TX1),
	ar_test_node:mine(peer1),
	?assertMatch({ok, _}, ar_test_await:node_height(peer1, 1)),
	TX2 = ar_test_node:sign_v1_tx(Key, #{ last_tx => TX1#tx.id }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX2),
	TX3 = ar_test_node:sign_v1_tx(Key, #{ last_tx => TX2#tx.id }),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx from mempool).">>, _, _}} =
		ar_test_node:post_tx_to_peer(peer1, TX3),
	TX4 = ar_test_node:sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX4),
	ar_test_node:mine(peer1),
	{ok, PeerBI} = ar_test_await:node_height(peer1, 2),
	B2 = ar_test_node:remote_call(peer1, ar_test_await, block_stored, [hd(PeerBI)]),
	?assertEqual([TX2#tx.id, TX4#tx.id], B2#block.txs).
