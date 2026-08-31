-module(ar_fork_2_9_6_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% From the fork 2.9.6 activation height on, format-1 transactions are no
%%% longer accepted: nodes refuse them on arrival, miners leave them out of
%%% new blocks, and blocks carrying them are rejected. The test profile's
%%% FORKS_RESET sets the activation height to 0, so the tests mock it
%%% wherever a node must still accept format-1 transactions.

rejects_format_1_txs_after_fork_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun test_rejects_format_1_txs_after_fork/0}.

rejects_block_with_format_1_tx_after_fork_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT,
	 fun test_rejects_block_with_format_1_tx_after_fork/0}.

ignores_block_with_v1_denomination0_tx_after_fork_for_good_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT,
	 fun test_ignores_block_with_v1_denomination0_tx_after_fork_for_good/0}.

test_rejects_format_1_txs_after_fork() ->
	{Key, LeftoverKey, B0} = new_funded_wallets(),
	ar_test_node:start(B0),
	ar_test_node:run_with_mocked([main],
			[{ar_fork, height_2_9_6, fun() -> 2 end}], fun() ->
		%% The next block, at height 1, precedes the fork: format-1
		%% transactions are still accepted and mined.
		PreForkTX = ar_test_node:sign_v1_tx(main, Key, #{ denomination => 1 }),
		ar_test_node:assert_post_tx_to_peer(main, PreForkTX),
		ar_test_node:mine(main),
		[{H1, _, _} | _] = ar_test_node:wait_until_height(main, 1),
		?assertEqual([PreForkTX#tx.id], block_txids(H1)),
		%% The next block, at height 2, activates the fork: format-1
		%% transactions are refused on arrival.
		PostForkTX = ar_test_node:sign_v1_tx(main, Key, #{ denomination => 1 }),
		?assertMatch({ok, {{<<"400">>, _}, _,
				<<"Transaction verification failed.">>, _, _}},
				post_tx(main, PostForkTX)),
		?assertNot(ar_mempool:has_tx(PostForkTX#tx.id)),
		?assertEqual({ok, ["tx_format_1_not_supported"]},
				ar_tx_db:get_error_codes(PostForkTX#tx.id)),
		%% A format-1 transaction that entered the mempool before the fork
		%% (injected directly here, like a reorg returning orphaned
		%% transactions does) is left out of the next block and dropped.
		%% Format-2 transactions are mined as usual.
		LeftoverTX = ar_test_node:sign_v1_tx(main, LeftoverKey,
				#{ denomination => 1 }),
		ar_mempool:add_tx(LeftoverTX, ready_for_mining),
		V2TX = ar_test_node:sign_tx(main, Key, #{}),
		ar_test_node:assert_post_tx_to_peer(main, V2TX),
		ar_test_node:mine(main),
		[{H2, _, _} | _] = ar_test_node:wait_until_height(main, 2),
		?assertEqual([V2TX#tx.id], block_txids(H2)),
		true = ar_util:do_until(fun() ->
			not ar_mempool:has_tx(LeftoverTX#tx.id)
		end, 200, 10_000)
	end).

test_rejects_block_with_format_1_tx_after_fork() ->
	{Key, _LeftoverKey, B0} = new_funded_wallets(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	%% peer1 stands in for a miner that ignores the fork; main runs with the
	%% test profile's activation height of 0 and must reject peer1's block.
	ar_test_node:run_with_mocked([peer1],
			[{ar_fork, height_2_9_6, fun() -> infinity end}], fun() ->
		V1TX = ar_test_node:sign_v1_tx(peer1, Key, #{ denomination => 1 }),
		ar_test_node:assert_post_tx_to_peer(peer1, V1TX),
		ar_test_node:mine(peer1),
		[{H, _, _} | _] = ar_test_node:wait_until_height(peer1, 1),
		B = ar_test_node:remote_call(peer1, ar_block_cache, get,
				[block_cache, H]),
		?assertEqual([V1TX#tx.id], [TX#tx.id || TX <- B#block.txs]),
		ok = ar_events:subscribe(block),
		?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:send_new_block(ar_test_node:peer_ip(main), B)),
		receive
			{event, block, {rejected, Reason, H, _Peer}} ->
				?assertEqual(invalid_txs, Reason)
		after 60_000 ->
			?assert(false,
					"The block with a format-1 transaction was not rejected.")
		end,
		?assertEqual(0, ar_node:get_height())
	end).

test_ignores_block_with_v1_denomination0_tx_after_fork_for_good() ->
	{Key, _LeftoverKey, B0} = new_funded_wallets(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	%% peer1 stands in for a miner that ignores both the fork and the
	%% deprecation of format-1 transactions without a denomination.
	ar_test_node:run_with_mocked([peer1],
			[{ar_fork, height_2_9_6, fun() -> infinity end},
			 {ar_tx, is_v1_denomination0_tx, fun(_TX) -> false end}], fun() ->
		V1TX = ar_test_node:sign_v1_tx(peer1, Key, #{ denomination => 0 }),
		?assert(ar_tx:is_v1_denomination0_tx(V1TX)),
		ar_test_node:assert_post_tx_to_peer(peer1, V1TX),
		ar_test_node:mine(peer1),
		[{H, _, _} | _] = ar_test_node:wait_until_height(peer1, 1),
		B = ar_test_node:remote_call(peer1, ar_block_cache, get,
				[block_cache, H]),
		?assertEqual([V1TX#tx.id], [TX#tx.id || TX <- B#block.txs]),
		ok = ar_events:subscribe(block),
		?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:send_new_block(ar_test_node:peer_ip(main), B)),
		receive
			{event, block, {rejected, Reason, H, _Peer}} ->
				?assertEqual(invalid_txs, Reason)
		after 60_000 ->
			?assert(false,
					"The block with a format-1 transaction was not rejected.")
		end,
		%% Before the fork such a block is only refused from the peer that
		%% supplied it, and only for a while, so the other peers can deliver
		%% the honest copy. From the fork on it is invalid whoever delivers it
		%% and is ignored for good, so nobody can make the node validate it
		%% over and over.
		%% Dropping the block from the cache and recording the permanent
		%% ignore are separate steps the node can still revisit, so wait for
		%% both rather than asserting one from the other.
		true = ar_util:do_until(fun() ->
			ar_ignore_registry:permanent_member(H)
					andalso ar_block_cache:get(block_cache, H) == not_found
		end, 200, 10_000),
		?assertEqual(0, ar_node:get_height())
	end).

new_funded_wallets() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(1000), <<>>}
	]),
	{Key1, Key2, B0}.

block_txids(H) ->
	B = ar_block_cache:get(block_cache, H),
	[case TX of #tx{ id = TXID } -> TXID; TXID -> TXID end
			|| TX <- B#block.txs].

post_tx(Node, TX) ->
	ar_test_node:post_tx_json(Node,
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))).
