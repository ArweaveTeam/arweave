-module(ar_tx_fork_recovery_tests).
-test_peers([peer1]).


-include("ar.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").


recovers_from_forks_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun() -> recovers_from_forks(7) end}.

recovers_from_forks(ForkHeight) ->
	%% peer1 and main mine in sync, then diverge; an extra block on peer1 makes
	%% main fork-recover onto it. Afterwards, replaying any past TX on main is
	%% rejected, while the orphaned fork's TXs are accepted and mined again.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(20), <<>>}
	]),
	_ = ar_test_node:start(B0),
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	MainPort = arweave_config:get([port]),
	PreForkTXs = lists:foldl(
		fun(Height, TXs) ->
			TX = ar_test_node:sign_v1_tx(Key, #{ last_tx => ar_test_node:get_tx_anchor(peer1),
					tags => [{<<"nonce">>, random_nonce()}] }),
			ar_test_node:assert_post_tx_to_peer(peer1, TX),
			?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX])),
			ar_test_node:mine(peer1),
			{ok, BI} = ar_test_await:node_height(peer1, Height),
			{ok, BI} = ar_test_await:node_height(main, Height),
			assert_block_txs(peer1, [TX], BI),
			assert_block_txs(main, [TX], BI),
			TXs ++ [TX]
		end,
		[],
		lists:seq(1, ForkHeight)
	),
	PostTXToMain =
		fun() ->
			UnsignedTX = #{ last_tx => ar_test_node:get_tx_anchor(main),
					tags => [{<<"nonce">>, random_nonce()}], reward => ?AR(1) },
			TX = case rand:uniform(2) of
				1 ->
					ar_test_node:sign_tx(main, Key, UnsignedTX);
				2 ->
					ar_test_node:sign_v1_tx(main, Key, UnsignedTX)
			end,
			ar_test_node:assert_post_tx_to_peer(main, TX),
			[TX]
		end,
	PostTXToPeer =
		fun() ->
			UnsignedTX = #{ last_tx => ar_test_node:get_tx_anchor(peer1),
					tags => [{<<"nonce">>, random_nonce()}] },
			TX = case rand:uniform(2) of
				1 ->
					ar_test_node:sign_tx(Key, UnsignedTX);
				2 ->
					ar_test_node:sign_v1_tx(Key, UnsignedTX)
			end,
			ar_test_node:assert_post_tx_to_peer(peer1, TX),
			[TX]
		end,
	ar_test_node:disconnect_from(peer1),
	{MainPostForkTXs, PeerPostForkTXs} = lists:foldl(
		fun(Height, {MainTXs, PeerTXs}) ->
			UpdatedMainTXs = MainTXs ++ ([NewMainTX] = PostTXToMain()),
			ar_test_node:mine(),
			{ok, BI} = ar_test_await:node_height(main, Height),
			assert_block_txs(main, [NewMainTX], BI),
			UpdatedPeerTXs = PeerTXs ++ ([NewPeerTX] = PostTXToPeer()),
			ar_test_node:mine(peer1),
			{ok, PeerBI} = ar_test_await:node_height(peer1, Height),
			assert_block_txs(peer1, [NewPeerTX], PeerBI),
			{UpdatedMainTXs, UpdatedPeerTXs}
		end,
		{[], []},
		lists:seq(ForkHeight + 1, 9)
	),
	ar_test_node:connect_to_peer(peer1),
	TX2 = ar_test_node:sign_tx(Key, #{ last_tx => ar_test_node:get_tx_anchor(peer1),
			tags => [{<<"nonce">>, random_nonce()}] }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX2),
	?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [TX2])),
	ar_test_node:mine(peer1),
	?assertMatch({ok, _}, ar_test_await:node_height(peer1, 10)),
	?assertMatch({ok, _}, ar_test_await:node_height(main, 10)),
	forget_txs(
		PreForkTXs ++
		MainPostForkTXs ++
		PeerPostForkTXs ++
		[TX2]
	),
	%% The pre-fork, fork-recovery, and fresh TXs are all on the weave.
	lists:foreach(
		fun(TX) ->
			ok = ar_test_await:tx_confirmed(main, TX#tx.id),
			{ok, {{<<"400">>, _}, _, _, _, _}} =
				ar_test_node:post_tx_to_peer(main, TX)
		end,
		PreForkTXs ++ PeerPostForkTXs ++ [TX2]
	),
	%% The abandoned fork's block-anchored TXs are back in the mempool.
	lists:foreach(
		fun(TX) ->
			{ok, {{<<"208">>, _}, _, <<"Transaction already processed.">>, _, _}} =
				ar_http:req(#{
					method => post,
					peer => {127, 0, 0, 1, MainPort},
					path => "/tx",
					headers => [{<<"x-p2p-port">>, integer_to_binary(MainPort, 10)}],
					body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
				})
		end,
		MainPostForkTXs
	).

forget_txs(TXs) ->
	lists:foreach(
		fun(TX) ->
			ets:delete(ignored_ids, TX#tx.id)
		end,
		TXs
	).

assert_block_txs(Node, TXs, BI) ->
	TXIDs = lists:map(fun(TX) -> TX#tx.id end, TXs),
	B = ar_test_node:remote_call(Node, ar_test_await, block_stored, [hd(BI)]),
	?assertEqual(lists:sort(TXIDs), lists:sort(B#block.txs)).

random_nonce() ->
	integer_to_binary(rand:uniform(1000000)).
