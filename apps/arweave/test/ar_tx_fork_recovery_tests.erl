-module(ar_tx_fork_recovery_tests).


-include("ar.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [wait_until_height/2, assert_wait_until_height/2]).

recovers_from_forks_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun() -> recovers_from_forks(7) end}.

recovers_from_forks(ForkHeight) ->
	%% Mine a number of blocks with transactions on peer1 and main in sync,
	%% then mine another bunch independently.
	%%
	%% Mine an extra block on peer1 to make main fork recover to it.
	%% Expect the fork recovery to be successful.
	%%
	%% Try to replay all the past transactions on main. Expect the transactions to be rejected.
	%%
	%% Resubmit all the transactions from the orphaned fork. Expect them to be accepted
	%% and successfully mined into a block.
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
			ar_test_node:assert_wait_until_receives_txs([TX]),
			ar_test_node:mine(peer1),
			BI = assert_wait_until_height(peer1, Height),
			BI = wait_until_height(main, Height),
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
			BI = wait_until_height(main, Height),
			assert_block_txs(main, [NewMainTX], BI),
			UpdatedPeerTXs = PeerTXs ++ ([NewPeerTX] = PostTXToPeer()),
			ar_test_node:mine(peer1),
			PeerBI = assert_wait_until_height(peer1, Height),
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
	ar_test_node:assert_wait_until_receives_txs([TX2]),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 10),
	wait_until_height(main, 10),
	forget_txs(
		PreForkTXs ++
		MainPostForkTXs ++
		PeerPostForkTXs ++
		[TX2]
	),
	%% Assert pre-fork transactions, the transactions which came during
	%% fork recovery, and the freshly created transaction are in the
	%% weave.
	lists:foreach(
		fun(TX) ->
			?assert(
				ar_util:do_until(
					fun() ->
						ar_test_node:get_tx_confirmations(main, TX#tx.id) > 0
					end,
					100,
					1000
				)
			),
			{ok, {{<<"400">>, _}, _, _, _, _}} =
				ar_test_node:post_tx_to_peer(main, TX)
		end,
		PreForkTXs ++ PeerPostForkTXs ++ [TX2]
	),
	%% Assert the block anchored transactions from the abandoned fork are
	%% back in the memory pool.
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
	B = ar_test_node:remote_call(Node, ar_test_node, read_block_when_stored, [hd(BI)]),
	?assertEqual(lists:sort(TXIDs), lists:sort(B#block.txs)).

random_nonce() ->
	integer_to_binary(rand:uniform(1000000)).
