-module(ar_tx_queue_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
	assert_post_tx_to_slave/1, disconnect_from_slave/0,
	assert_wait_until_receives_txs/1, wait_until_height/1,
	sign_tx/2, get_tx_anchor/0,
	get_tx_price/1, slave_mine/0, slave_call/3, connect_to_slave/0,
	post_tx_to_master/2, read_block_when_stored/1
]).

txs_broadcast_order_test_() ->
	{timeout, 60, fun test_txs_broadcast_order/0}.

test_txs_broadcast_order() ->
	%% Set up two nodes with HTTP.
	{_MasterNode, _SlaveNode, _} = setup(),
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
	assert_post_tx_to_slave(TX1),
	assert_post_tx_to_slave(TX2),
	assert_post_tx_to_slave(TX3),
	assert_post_tx_to_slave(TX4),
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
			TXs = encode_txs(ar_node:get_ready_for_mining_txs()),
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
	ar_tx_queue:set_max_header_size(6 * ?TX_SIZE_BASE + 1000),
	HigherPriorityTXs = [
		import_tx(1, 200, ?AR(50)),
		import_tx(1, 250, ?AR(50)),
		import_tx(1, 200, ?AR(40)),
		import_tx(1, 250, ?AR(40))
	],
	LowerPriorityTXs = [
		import_tx(1, 200, ?AR(10)),
		import_tx(1, 250, ?AR(10))
	],
	Actual = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	%% Only 5 fit into 6 * ?TX_SIZE_BASE + 1000.
	?assertEqual(5, length(Actual)),
	[TXID1, TXID2, TXID3, TXID4, TXID5] = Actual,
	?assert(lists:member(TXID5, encode_txs(LowerPriorityTXs))),
	?assertEqual([TXID1, TXID2, TXID3, TXID4], encode_txs(HigherPriorityTXs)),
	%% Post 2 transactions bigger than the queue size limit.
	%% Expect all transactions but these two to be dropped from the queue.
	HighestPriorityTXs = [
		import_tx(1, 2 * ?TX_SIZE_BASE, ?AR(2000)),
		import_tx(1, 2 * ?TX_SIZE_BASE, ?AR(1000))
	],
	Actual2 = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	?assertEqual(encode_txs(HighestPriorityTXs), Actual2),
	%% Set max data size. Submit some format=2 txs.
	%% Expect those exceeding the new limit to be dropped.
	ar_tx_queue:set_max_header_size(9 * ?TX_SIZE_BASE + 1),
	ar_tx_queue:set_max_data_size(2),
	Format2TX1 = import_tx(2, 1, ?AR(3)),
	Format2TX2 = import_tx(2, 1, ?AR(2)),
	import_tx(2, 1, ?AR(1)), % This one does not fit into the limit.
	Format1TX = import_tx(1, 1, 10), % v1, does not contribute to the data limit.
	Actual3 = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	?assertEqual(encode_txs([Format2TX1, Format2TX2, Format1TX]), Actual3).

get_queue_endpoint_test_() ->
	{timeout, 10, fun test_get_queue_endpoint/0}.

test_get_queue_endpoint() ->
	setup(),
	ar_tx_queue:set_pause(true),
	Expected = encode_txs([
		import_tx(1, 0, ?AR(1)),
		import_tx(2, 200, ?AR(1)),
		import_tx(2, 400, ?AR(1)),
		import_tx(1, 10, 10),
		import_tx(1, 200, ?AR(100)) %% Big v1 txs are prioritized below all else.
	]),
	Actual = [TXID || {[{_, TXID}, _, _]} <- http_get_queue()],
	?assertEqual(Expected, Actual).

test_txs_are_included_in_blocks_sorted_by_utility_test_() ->
	{timeout, 20, fun test_txs_are_included_in_blocks_sorted_by_utility/0}.

test_txs_are_included_in_blocks_sorted_by_utility() ->
	{_MasterNode, _SlaveNode, Wallet} = setup(),
	TXs = [
		%% Base size, extra reward.
		sign_tx(Wallet, #{
			reward => get_tx_price(0) + 100000, last_tx => get_tx_anchor()
		}),
		%% More data, extra reward.
		sign_tx(
			Wallet,
			#{
				%% Make sure v1 tx is not malleable.
				data => << (crypto:strong_rand_bytes(1000))/binary, <<"a">>/binary >>,
				reward => get_tx_price(1000) + 100000,
				last_tx => get_tx_anchor()
			}),
		%% Base size, default reward.
		sign_tx(Wallet, #{ last_tx => get_tx_anchor() })
	],
	SortedTXs = lists:sort(
		fun(#tx{ reward = Reward1 }, #tx{ reward = Reward2 }) ->
			Reward1 > Reward2
		end,
		TXs
	),
	lists:foldl(
		fun(_, ToPost) ->
			TX = ar_util:pick_random(ToPost),
			assert_post_tx_to_slave(TX),
			ToPost -- [TX]
		end,
		TXs,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	slave_mine(),
	BI = wait_until_height(1),
	B = read_block_when_stored(hd(BI)),
	?assertEqual(
		lists:map(fun(TX) -> TX#tx.id end, SortedTXs),
		B#block.txs
	),
	SlaveB = slave_call(ar_storage, read_block, [hd(BI)]),
	?assertEqual(
		lists:map(fun(TX) -> TX#tx.id end, SortedTXs),
		SlaveB#block.txs
	).

format_2_txs_are_gossiped_test_() ->
	{timeout, 60, fun format_2_txs_are_gossiped/0}.

format_2_txs_are_gossiped() ->
	{_MasterNode, _SlaveNode, Wallet} = setup(),
	TXParams = #{format => 2, data => <<"TXDATA">>, reward => ?AR(1)},
	SignedTX = sign_tx(Wallet, TXParams),
	SignedTXHeader = SignedTX#tx{ data = <<>> },
	assert_post_tx_to_slave(SignedTX),
	assert_wait_until_receives_txs([SignedTXHeader]),
	slave_mine(),
	BI = wait_until_height(1),
	#block{ txs = [MasterTXID] } = ar_storage:read_block(hd(BI)),
	?assertEqual(SignedTXHeader#tx.id, MasterTXID),
	?assertEqual(SignedTXHeader, ar_storage:read_tx(MasterTXID)),
	#block{ txs = [SlaveTXID] } = slave_call(ar_storage, read_block, [hd(BI)]),
	?assertEqual(SignedTXHeader, slave_call(ar_storage, read_tx, [SlaveTXID])),
	?assertEqual(SignedTXHeader#tx.id, SlaveTXID),
	true = ar_util:do_until(
		fun() ->
			%% Wait until chunks are picked up from disk pool and prepared for serving.
			slave_call(ar_data_sync, get_tx_data, [SlaveTXID]) == {ok, <<"TXDATA">>}
		end,
		100,
		2000
	),
	ar_util:do_until(
		fun() ->
			%% Wait until downloader fetches data.
			{ok, <<"TXDATA">>} == ar_data_sync:get_tx_data(MasterTXID)
		end,
		100,
		5000
	).

tx_is_dropped_after_it_is_included_test_() ->
	{timeout, 60, fun test_tx_is_dropped_after_it_is_included/0}.

test_tx_is_dropped_after_it_is_included() ->
	{_Master, _Slave, _Wallet} = setup(),
	CommittedTXs = [
		ar_tx:new(<<"DATA1">>, ?AR(1)),
		(ar_tx:new(<<"DATA3">>, ?AR(10)))#tx{ format = 2, data_size = 5, data_root = <<"r">> },
		ar_tx:new(<<>>, ?AR(100)),
		(ar_tx:new(<<>>, ?AR(1000)))#tx{ format = 2, data_size = 0 }
	],
	ar_tx_queue:set_pause(true),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		CommittedTXs
	),
	disconnect_from_slave(),
	NotCommittedTXs = [
		ar_tx:new(<<"DATA2">>, ?AR(1)),
		(ar_tx:new(<<"DATA4">>, ?AR(10)))#tx{ format = 2, data_size = 5, data_root = <<"r">> },
		ar_tx:new(<<>>, ?AR(10)),
		(ar_tx:new(<<>>, ?AR(1)))#tx{ format = 2, data_size = 0 }
	],
	lists:foreach(
		fun(TX) ->
			post_tx_to_master(TX, false)
		end,
		NotCommittedTXs
	),
	TXIDs = [TX#tx.id || TX <- CommittedTXs ++ NotCommittedTXs],
	?assertEqual(
		lists:sort([ar_util:encode(TXID) || TXID <- TXIDs]),
		lists:sort([TXID || {[{_, TXID}, _, _]} <- http_get_queue()])
	),
	connect_to_slave(),
	slave_mine(),
	wait_until_height(1),
	NotCommittedTXIDs = [TX#tx.id || TX <- NotCommittedTXs],
	?assertEqual(
		lists:sort([ar_util:encode(TXID) || TXID <- NotCommittedTXIDs]),
		lists:sort([TXID || {[{_, TXID}, _, _]} <- http_get_queue()])
	).

setup() ->
	{Pub, _} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(5000), <<>>}]),
	{MasterNode, _} = ar_test_node:start(B0),
	{SlaveNode, _} = ar_test_node:slave_start(B0),
	ar_test_node:connect_to_slave(),
	{MasterNode, SlaveNode, Wallet}.

http_get_queue() ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984}, path => "/queue"}),
	ar_serialize:dejsonify(Body).

import_tx(Format, Size, Fee) ->
	TX = (ar_tx:new(crypto:strong_rand_bytes(Size), Fee))#tx{ format = Format },
	ar_http_iface_client:send_new_tx({127, 0, 0, 1, 1984}, TX),
	TX.

encode_txs(TXs) ->
	lists:map(fun(TX) -> ar_util:encode(TX#tx.id) end, TXs).
