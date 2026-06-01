-module(ar_tx_joins_network_tests).


-include("ar.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [wait_until_height/2, assert_wait_until_height/2,
	read_block_when_stored/1]).

joins_network_successfully_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun joins_network_successfully/0}.

joins_network_successfully() ->
	%% Start a node and mine ar_block:get_max_tx_anchor_depth() blocks, some of them
	%% with transactions.
	%%
	%% Join this node by another node.
	%% Post a transaction with an outdated anchor to the new node.
	%% Expect it to be rejected.
	%%
	%% Expect all the transactions to be present on the new node.
	%%
	%% Isolate the nodes. Mine 1 block with a transaction anchoring the
	%% oldest block possible on peer1. Mine a block on main so that it stops
	%% tracking the block just referenced by peer1. Reconnect the nodes, mine another
	%% block with transactions anchoring the oldest block possible on peer1.
	%% Expect main to fork recover successfully.
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
			assert_wait_until_height(peer1, Height),
			ar_util:do_until(
				fun() ->
					ar_test_node:remote_call(peer1, ar_mempool, get_all_txids, []) == []
				end,
				200,
				1000
			),
			{TXs ++ [{TX, AnchorType}], TX#tx.id}
		end,
		{[], <<>>},
		lists:seq(1, ar_block:get_max_tx_anchor_depth())
	),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	BI = ar_test_node:remote_call(peer1, ar_node, get_block_index, []),
	?assertEqual(ok, ar_test_node:wait_until_block_index(BI)),
	TX1 = ar_test_node:sign_tx(Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth() + 1, BI)) }),
	{ok, {{<<"400">>, _}, _, <<"Invalid anchor (last_tx).">>, _, _}} =
		ar_test_node:post_tx_to_peer(main, TX1),
	%% Expect transactions to be on main.
	lists:foreach(
		fun({TX, _}) ->
			?assert(
				ar_util:do_until(
					fun() ->
						ar_test_node:get_tx_confirmations(main, TX#tx.id) > 0
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

	%% Mine the block on main first to ensure that it can't be rebased after the 2-block
	%% fork from peer1 wins.
	TX2 = ar_test_node:sign_tx(main, Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth(), BI)) }),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:mine(),
	wait_until_height(main, ar_block:get_max_tx_anchor_depth() + 1),

	%% mine two blocks on peer to ensure that the main branch is orphaned.
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, ar_block:get_max_tx_anchor_depth() + 1),

	%% lists:nth(ar_block:get_max_tx_anchor_depth() - 1, BI) since we'll be at at ar_block:get_max_tx_anchor_depth() + 2.
	TX3 = ar_test_node:sign_tx(peer1, Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth() - 1, BI)) }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX3),
	ar_test_node:mine(peer1),
	BI2 = assert_wait_until_height(peer1, ar_block:get_max_tx_anchor_depth() + 2),

	ar_test_node:connect_to_peer(peer1),

	wait_until_height(main, ar_block:get_max_tx_anchor_depth() + 2),

	TX4 = ar_test_node:sign_tx(peer1, Key, #{ last_tx => element(1, lists:nth(ar_block:get_max_tx_anchor_depth(), BI2)) }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX4),
	ar_test_node:assert_wait_until_receives_txs([TX4]),
	ar_test_node:mine(peer1),
	BI3 = assert_wait_until_height(peer1, ar_block:get_max_tx_anchor_depth() + 3),
	BI3 = wait_until_height(main, ar_block:get_max_tx_anchor_depth() + 3),

	?assertEqual([TX4#tx.id], (read_block_when_stored(hd(BI3)))#block.txs),
	?assertEqual([TX3#tx.id], (read_block_when_stored(hd(BI2)))#block.txs).
