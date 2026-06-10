-module(ar_tx_joins_network_tests).
-test_peers([peer1]).


-include("ar.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

joins_network_successfully_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun joins_network_successfully/0}.

joins_network_successfully() ->
	%% peer1 mines get_max_tx_anchor_depth() blocks, some with TXs, then main
	%% joins it: main rejects a TX with an outdated anchor and ends up with all
	%% of peer1's TXs. The nodes are then isolated and each mines a competing
	%% chain anchored at the oldest still-valid block, and main fork-recovers
	%% onto peer1's branch on reconnect.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(200000000), <<>>},
		{Addr = crypto:strong_rand_bytes(32), ?AR(200000000), <<>>},
		{crypto:strong_rand_bytes(32), ?AR(200000000), <<>>}
	]),
	ar_test_node:start(B0),
	_ = ar_test_node:start_peer(peer1, B0),
	{TXs, _} = lists:foldl(
		fun(Height, {TXs, LastTX}) ->
			{TX, AnchorType} = case rand:uniform(4) of
				1 ->
					{ar_test_node:sign_v1_tx(Key, #{ last_tx => LastTX, reward => ?AR(10000) }), tx_anchor};
				2 ->
					{ar_test_node:sign_v1_tx(Key, #{ last_tx => ar_test_node:get_tx_anchor(peer1), reward => ?AR(10000),
							tags => [{<<"nonce">>, integer_to_binary(rand:uniform(100))}] }),
							block_anchor};
				3 ->
					{ar_test_node:sign_tx(Key, #{ last_tx => LastTX, target => Addr,
							reward => ?AR(10000) }), tx_anchor};
				4 ->
					{ar_test_node:sign_tx(Key, #{ last_tx => ar_test_node:get_tx_anchor(peer1), reward => ?AR(10000),
							tags => [{<<"nonce">>, integer_to_binary(rand:uniform(100))}]}),
							block_anchor}
			end,
			ar_test_node:assert_post_tx_to_peer(peer1, TX),
			ar_test_node:mine(peer1),
			?assertMatch({ok, _}, ar_test_await:node_height(peer1, Height)),
			ok = ar_test_await:mempool_drained(peer1),
			{TXs ++ [{TX, AnchorType}], TX#tx.id}
		end,
		{[], <<>>},
		lists:seq(1, ar_block:get_max_tx_anchor_depth())
	),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	BI = ar_test_node:remote_call(peer1, ar_node, get_block_index, []),
	?assertEqual(ok, ar_test_await:block_index_matches(main, BI)),
	TX1 = ar_test_node:sign_tx(Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth() + 1, BI)) }),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		ar_test_node:post_tx_to_peer(main, TX1),
	lists:foreach(
		fun({TX, _}) ->
			ok = ar_test_await:tx_confirmed(main, TX#tx.id)
		end,
		TXs
	),
	lists:foreach(
		fun({TX, AnchorType}) ->
			Reply = ar_test_node:post_tx_to_peer(main, TX),
			case AnchorType of
				tx_anchor ->
					?assertMatch({ok, {{<<"400">>, _}, _,
							<<"Invalid anchor (last_tx).">>, _, _}}, Reply);
				block_anchor ->
					RecentBHL = lists:sublist(?BI_TO_BHL(BI), ar_block:get_max_tx_anchor_depth()),
					case lists:member(TX#tx.last_tx, RecentBHL) of
						true ->
							?assertMatch({ok, {{<<"400">>, _}, _,
									<<"Transaction is already on the weave.">>, _, _}}, Reply);
						false ->
							?assertMatch({ok, {{<<"400">>, _}, _,
									<<"Invalid anchor (last_tx).">>, _, _}}, Reply)
					end
			end
		end,
		TXs
	),
	ar_test_node:disconnect_from(peer1),

	%% Mine on main first so its block can't be rebased once peer1's 2-block fork wins.
	TX2 = ar_test_node:sign_tx(main, Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth(), BI)) }),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:mine(),
	?assertMatch({ok, _}, ar_test_await:node_height(main, ar_block:get_max_tx_anchor_depth() + 1)),

	%% Mine two blocks on peer1 to orphan main's branch.
	ar_test_node:mine(peer1),
	?assertMatch({ok, _}, ar_test_await:node_height(peer1, ar_block:get_max_tx_anchor_depth() + 1)),

	%% Anchor at depth - 1 since this block lands at depth + 2.
	TX3 = ar_test_node:sign_tx(peer1, Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth() - 1, BI)) }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX3),
	ar_test_node:mine(peer1),
	{ok, BI2} = ar_test_await:node_height(peer1, ar_block:get_max_tx_anchor_depth() + 2),

	ar_test_node:connect_to_peer(peer1),

	?assertMatch({ok, _}, ar_test_await:node_height(main, ar_block:get_max_tx_anchor_depth() + 2)),

	TX4 = ar_test_node:sign_tx(peer1, Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth(), BI2)) }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX4),
	?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX4])),
	ar_test_node:mine(peer1),
	{ok, BI3} = ar_test_await:node_height(peer1, ar_block:get_max_tx_anchor_depth() + 3),
	{ok, BI3} = ar_test_await:node_height(main, ar_block:get_max_tx_anchor_depth() + 3),

	?assertEqual([TX4#tx.id], (ar_test_await:block_stored(hd(BI3)))#block.txs),
	?assertEqual([TX3#tx.id], (ar_test_await:block_stored(hd(BI2)))#block.txs).
