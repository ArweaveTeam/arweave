-module(ar_tx_anchor_tests).
-test_peers([peer1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").


accepts_at_most_one_wallet_list_anchored_tx_per_block_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_accepts_at_most_one_wallet_list_anchored_tx_per_block/0}.

rejects_txs_with_outdated_anchors_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun() ->
		%% A TX anchoring the block one past get_max_tx_anchor_depth() is rejected.
		%%
		%% A TX anchoring the deepest still-valid block (the
		%% ar_block:get_max_tx_anchor_depth()-th block from the tip) is
		%% accepted. This pins both sides of the depth boundary and guards
		%% against an off-by-one in
		%% lists:sublist(BlockTXPairs, get_max_tx_anchor_depth()) inside
		%% ar_node_worker:get_block_anchors_and_recent_txs_map/1.
		Key = {_, Pub} = ar_wallet:new(),
		[B0] = ar_weave:init([
			{ar_wallet:to_address(Pub), ?AR(20), <<>>}
		]),
		_ = ar_test_node:start_peer(peer1, B0),
		ar_tx_test_utils:mine_blocks(peer1, ar_block:get_max_tx_anchor_depth()),
		{ok, BI} = ar_test_await:node_height(peer1, ar_block:get_max_tx_anchor_depth()),
		TX1 = ar_test_node:sign_v1_tx(Key, #{ last_tx => B0#block.indep_hash }),
		?assertEqual({invalid, tx_bad_anchor},
				ar_test_node:remote_call(peer1, ar_tx_validator, validate, [TX1])),
		{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
			ar_test_node:post_tx_to_peer(peer1, TX1),
		DeepestValidBH = element(1, lists:nth(ar_block:get_max_tx_anchor_depth(), BI)),
		TX2 = ar_test_node:sign_v1_tx(Key, #{ last_tx => DeepestValidBH,
				tags => [{<<"nonce">>, <<"depth_boundary">>}] }),
		?assertMatch({valid, _},
				ar_test_node:remote_call(peer1, ar_tx_validator, validate, [TX2])),
		ar_test_node:assert_post_tx_to_peer(peer1, TX2)
	end}.

rejects_replay_after_anchor_window_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun rejects_replay_after_anchor_window/0}.

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

rejects_replay_after_anchor_window() ->
	%% Exercise the transition from tx_already_in_weave to tx_bad_anchor for
	%% a block-anchored TX. While the TX's block and anchor are in the anchor
	%% window, a replay is caught by RecentTXMap as tx_already_in_weave. Once
	%% they age out, the replay must land in verify_block_anchor -> tx_bad_anchor.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	_ = ar_test_node:start_peer(peer1, B0),
	TX = ar_test_node:sign_v1_tx(Key, #{
		reward => ?AR(1),
		last_tx => B0#block.indep_hash
	}),
	ar_test_node:assert_post_tx_to_peer(peer1, TX),
	ar_test_node:mine(peer1),
	?assertMatch({ok, _}, ar_test_await:node_height(peer1, 1)),
	forget_txs(peer1, [TX]),
	{ok, {{<<"400">>, _}, _, <<"Transaction is already on the weave.">>, _, _}} =
		ar_tx_test_utils:post_tx_to_peer_once(peer1, TX),
	%% Mine enough empty blocks to push TX's block and anchor past the anchor
	%% window so its id leaves RecentTXMap and its last_tx leaves BlockAnchors.
	TargetHeight = 1 + ar_block:get_max_tx_anchor_depth(),
	lists:foreach(
		fun(H) ->
			ar_test_node:mine(peer1),
			?assertMatch({ok, _}, ar_test_await:node_height(peer1, H))
		end,
		lists:seq(2, TargetHeight)
	),
	forget_txs(peer1, [TX]),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		ar_tx_test_utils:post_tx_to_peer_once(peer1, TX).

forget_txs(Node, TXs) ->
	lists:foreach(
		fun(TX) ->
			ar_test_node:remote_call(Node, ets, delete, [ignored_ids, TX#tx.id])
		end,
		TXs
	).
