-module(ar_poller_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [slave_start/1, 
		assert_post_tx_to_slave/1,
		assert_wait_until_height/2, slave_wait_until_height/1, wait_until_height/1,
		read_block_when_stored/1]).

polling_test_() ->
	{timeout, 120, fun test_polling/0}.

test_polling() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	TXs =
		lists:map(
			fun(Height) ->
				SignedTX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(peer1) }),
				assert_post_tx_to_slave(SignedTX),
				ar_test_node:mine(peer1),
				assert_wait_until_height(peer1, Height),
				SignedTX
			end,
			lists:seq(1, 9)
		),
	ar_test_node:connect_to_peer(peer1),
	wait_until_height(9),
	lists:foreach(
		fun(Height) ->
			{H, _, _} = ar_node:get_block_index_entry(Height),
			B = read_block_when_stored(H),
			TX = lists:nth(Height, TXs),
			?assertEqual([TX#tx.id], B#block.txs)
		end,
		lists:seq(1, 9)
	),
	%% Make the nodes diverge. Expect one of them to fetch and apply the blocks
	%% from the winning fork.
	ar_test_node:disconnect_from(peer1),
	ar_test_node:mine(),
	ar_test_node:mine(peer1),
	[{MH11, _, _} | _] = wait_until_height(10),
	[{SH11, _, _} | _] = slave_wait_until_height(10),
	?assertNotEqual(SH11, MH11),
	ar_test_node:mine(),
	ar_test_node:mine(peer1),
	[{MH12, _, _} | _] = wait_until_height(11),
	[{SH12, _, _} | _] = slave_wait_until_height(11),
	?assertNotEqual(SH12, MH12),
	ar_test_node:mine(),
	ar_test_node:mine(peer1),
	[{MH13, _, _} | _] = MBI12 = wait_until_height(12),
	[{SH13, _, _} | _] = SBI12 = slave_wait_until_height(12),
	?assertNotEqual(SH13, MH13),
	BM13 = ar_block_cache:get(block_cache, MH13),
	BS13 = ar_test_node:remote_call(peer1, ar_block_cache, get, [block_cache, SH13]),
	CDiffM13 = BM13#block.cumulative_diff,
	CDiffS13 = BS13#block.cumulative_diff,
	ar_test_node:connect_to_peer(peer1),
	case CDiffM13 > CDiffS13 of
		true ->
			?debugFmt("Case 1.", []),
			?assertEqual(ok, ar_test_node:wait_until_block_index(peer1, MBI12)),
			?assertMatch([{MH13, _, _} | _], ar_node:get_block_index());
		false ->
			case CDiffM13 < CDiffS13 of
				true ->
					?debugFmt("Case 2.", []),
					?assertEqual(ok, ar_test_node:wait_until_block_index(SBI12)),
					?assertMatch([{SH13, _, _} | _],
							ar_test_node:remote_call(peer1, ar_node, get_block_index, []));
				false ->
					?debugFmt("Case 3.", []),
					ar_test_node:mine(peer1),
					[{MH14, _, _}, {MH13_1, _, _}, {MH12_1, _, _}, {MH11_1, _, _} | _]
					= wait_until_height(13),
					[{SH14, _, _} | _] = slave_wait_until_height(13),
					?assertEqual(MH14, SH14),
					?assertEqual(SH13, MH13_1),
					?assertEqual(SH12, MH12_1),
					?assertEqual(SH11, MH11_1)
			end
	end.
