-module(ar_tx_queue_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [assert_post_tx_to_slave/2]).
-import(ar_test_node, [assert_wait_until_receives_txs/2, wait_until_height/2]).
-import(ar_test_node, [sign_tx/2, sign_tx_pre_fork_2_0/2, get_tx_anchor/0]).
-import(ar_test_node, [get_tx_price/1, slave_mine/1, slave_call/3]).
-import(ar_test_node, [test_with_mocked_functions/2]).

txs_broadcast_order_test_() ->
	{timeout, 60, fun test_txs_broadcast_order/0}.

test_txs_broadcast_order() ->
	%% Set up two nodes with HTTP.
	{MasterNode, SlaveNode, _} = setup(),
	%% Create 4 transactions with the same size
	%% but different rewards.
	TX1 = ar_tx:new(<<"DATA1">>, ?AR(1)),
	TX2 = ar_tx:new(<<"DATA2">>, ?AR(10)),
	TX3 = ar_tx:new(<<"DATA3">>, ?AR(100)),
	TX4 = ar_tx:new(<<"DATA4">>, ?AR(1000)),
	Expected = encode_txs([TX4, TX3, TX2, TX1]),
	%% Pause the bridge to give time for txs
	%% to accumulate in the queue.
	ar_tx_queue:set_pause(true),
	%% Limit the number of emitters so that
	%% the order in which transactions are
	%% received by the node can be asserted.
	ar_tx_queue:set_max_emitters(1),
	assert_post_tx_to_slave(SlaveNode, TX1),
	assert_post_tx_to_slave(SlaveNode, TX2),
	assert_post_tx_to_slave(SlaveNode, TX3),
	assert_post_tx_to_slave(SlaveNode, TX4),
	ar_util:do_until(
		fun() ->
			case length(ar_tx_queue:show_queue()) of
				L when L == length(Expected) ->
					ok;
				_ ->
					continue
			end
		end,
		200,
		2000
	),
	%% Expect the transactions to be received in the order
	%% from the highest utility score to the lowest.
	ar_tx_queue:set_pause(false),
	ar_util:do_until(
		fun() ->
			TXs = encode_txs(ar_node:get_mined_txs(MasterNode)),
			case length(TXs) of
				4 ->
					?assertEqual(lists:sort(Expected), lists:sort(TXs)),
					ok;
				3 ->
					?assertEqual(lists:sort(encode_txs([TX4, TX3, TX2])), lists:sort(TXs)),
					continue;
				2 ->
					?assertEqual(lists:sort(encode_txs([TX4, TX3])), lists:sort(TXs)),
					continue;
				1 ->
					?assertEqual(encode_txs([TX4]), TXs),
					continue;
				0 ->
					continue
			end
		end,
		10,
		2000
	).

drop_lowest_priority_txs_test_() ->
	{timeout, 10, fun test_drop_lowest_priority_txs/0}.

test_drop_lowest_priority_txs() ->
	setup(),
	ar_tx_queue:set_pause(true),
	ar_tx_queue:set_max_size(6 * ?TX_SIZE_BASE),
	HigherPriorityTXs = import_4_txs(),
	LowerPriorityTXs = make_txs(4),
	lists:foreach(
		fun(TX) ->
			ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX)
		end,
		LowerPriorityTXs
	),
	Actual = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	?assertEqual(5, length(Actual)),
	[TX1, TX2, TX3, TX4, TX5] = Actual,
	?assert(lists:member(TX5, encode_txs(LowerPriorityTXs))),
	?assertEqual(HigherPriorityTXs, [TX1, TX2, TX3, TX4]),
	%% Post 2 transactions bigger than the queue size limit.
	%% Expect all transactions but these two to be dropped from the queue.
	HighestPriorityTXs = [
		ar_tx:new(<< <<0>> || _ <- lists:seq(1, 2 * ?TX_SIZE_BASE) >>, ?AR(2000)),
		ar_tx:new(<< <<0>> || _ <- lists:seq(1, 2 * ?TX_SIZE_BASE) >>, ?AR(1000))
	],
	lists:foreach(
		fun(TX) ->
			ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX)
		end,
		HighestPriorityTXs
	),
	Actual2 = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	?assertEqual(encode_txs(HighestPriorityTXs), Actual2).

get_queue_endpoint_test_() ->
	{timeout, 10, fun test_get_queue_endpoint/0}.

test_get_queue_endpoint() ->
	setup(),
	ar_tx_queue:set_pause(true),
	Expected = import_4_txs(),
	Actual = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	?assertEqual(Expected, Actual).

test_txs_are_included_in_blocks_sorted_by_utility_test() ->
	{MasterNode, SlaveNode, Wallet} = setup(),
	TXs = [
		%% Base size, extra reward.
		sign_tx_pre_fork_2_0(Wallet, #{ reward => get_tx_price(0) + ?AR(1), last_tx => get_tx_anchor() }),
		%% More data, same extra reward.
		sign_tx_pre_fork_2_0(
			Wallet,
			#{ data => <<"More data">>, reward => get_tx_price(9) + ?AR(1), last_tx => get_tx_anchor() }
		),
		%% Base size, default reward.
		sign_tx_pre_fork_2_0(Wallet, #{ last_tx => get_tx_anchor() })
	],
	lists:foldl(
		fun(_, ToPost) ->
			TX = ar_util:pick_random(ToPost),
			assert_post_tx_to_slave(SlaveNode, TX),
			ToPost -- [TX]
		end,
		TXs,
		TXs
	),
	assert_wait_until_receives_txs(MasterNode, TXs),
	slave_mine(SlaveNode),
	BI = wait_until_height(MasterNode, 1),
	B = ar_storage:read_block(hd(BI), BI),
	?assertEqual(
		lists:map(fun(TX) -> TX#tx.id end, TXs),
		B#block.txs
	),
	SlaveB = slave_call(ar_storage, read_block, [hd(BI), BI]),
	?assertEqual(
		lists:map(fun(TX) -> TX#tx.id end, TXs),
		SlaveB#block.txs
	).

format_2_txs_are_gossiped_test() ->
	test_with_mocked_functions(
		[
			{ar_fork, height_2_0, fun() -> 0 end}
		],
		fun format_2_txs_are_gossiped/0
	).

format_2_txs_are_gossiped() ->
	{MasterNode, SlaveNode, Wallet} = setup(),
	TXParams = #{format => 2, data => <<"TXDATA">>, reward => ?AR(1)},
	SignedTX = sign_tx(Wallet, TXParams),
	SignedTXNoData = ar_tx:strip_data(SignedTX),
	assert_post_tx_to_slave(SlaveNode, SignedTX),
	assert_wait_until_receives_txs(MasterNode, [SignedTXNoData]),
	slave_mine(SlaveNode),
	BI = wait_until_height(MasterNode, 1),

	#block{txs = [TX1]} = ar_storage:read_block(hd(BI), BI),
	?assertEqual(TX1, SignedTXNoData#tx.id),
	?assertEqual(ar_storage:read_tx(TX1), SignedTXNoData),

	#block{txs = [TX2]} = slave_call(ar_storage, read_block, [hd(BI), BI]),
	?assertEqual(slave_call(ar_storage, read_tx, [TX2]), SignedTX),
	?assertEqual(TX2, SignedTX#tx.id).

%%%% private

setup() ->
	{Pub, _} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(5000), <<>>}]),
	{MasterNode, _} = ar_test_node:start(B0),
	{SlaveNode, _} = ar_test_node:slave_start(B0),
	ar_test_node:connect_to_slave(),
	{MasterNode, SlaveNode, Wallet}.

http_get_queue() ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(<<"GET">>, {127, 0, 0, 1, 1984}, "/queue"),
	ar_serialize:dejsonify(Body).

import_4_txs() ->
	TX1 = ar_tx:new(<<"DATA1">>, ?AR(50)),
	TX2 = ar_tx:new(<<"DATA2">>, ?AR(10)),
	TX3 = ar_tx:new(<<"DATA3">>, ?AR(80)),
	TX4 = ar_tx:new(<<"DATA4data4">>, ?AR(80)),
	lists:foreach(
		fun(TX) ->
			ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX)
		end,
		[TX1, TX2, TX3, TX4]
	),
	[
		ar_util:encode(TX3#tx.id), % score = 80 / (base size + 5)  ~ 0.02488
		ar_util:encode(TX4#tx.id), % score = 80 / (base size + 10) ~ 0.02484
		ar_util:encode(TX1#tx.id), % score = 50 / (base size + 5)  ~ 0.15
		ar_util:encode(TX2#tx.id)  % score = 10 / (base size + 5   ~ 0.03
	].

make_txs(0) -> [];
make_txs(N) ->
	B = integer_to_binary(N),
	[ar_tx:new(<<"DATA", B/binary>>, ?AR(1)) | make_txs(N-1)].

encode_txs(TXs) ->
	lists:map(fun(TX) -> ar_util:encode(TX#tx.id) end, TXs).
