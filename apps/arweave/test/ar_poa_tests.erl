-module(ar_poa_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0,
		assert_post_tx_to_slave/1, slave_mine/0, slave_call/3,
		wait_until_height/1, assert_slave_wait_until_height/1,
		assert_wait_until_receives_txs/1,
		get_tx_anchor/0, sign_tx/2, read_block_when_stored/1]).

v1_transactions_after_2_0_test_() ->
	{timeout, 240, fun test_v1_transactions_after_2_0/0}.

test_v1_transactions_after_2_0() ->
	Key = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	TXs = generate_txs(Key, fun ar_test_node:sign_v1_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				1 ->
					assert_txs_mined(TXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(1, 10)
	),
	MoreTXs = generate_txs(Key2, fun ar_test_node:sign_v1_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		MoreTXs
	),
	assert_wait_until_receives_txs(MoreTXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				11 ->
					assert_txs_mined(MoreTXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(11, 20)
	).

v2_transactions_after_2_0_test_() ->
	{timeout, 240, fun test_v2_transactions_after_2_0/0}.

test_v2_transactions_after_2_0() ->
	Key = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	TXs = generate_txs(Key, fun ar_test_node:sign_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				1 ->
					assert_txs_mined(TXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(1, 10)
	),
	MoreTXs = generate_txs(Key2, fun ar_test_node:sign_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		MoreTXs
	),
	assert_wait_until_receives_txs(MoreTXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				11 ->
					assert_txs_mined(MoreTXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(11, 20)
	).

recall_byte_on_the_border_test_() ->
	{timeout, 60, fun test_recall_byte_on_the_border/0}.

test_recall_byte_on_the_border() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(100), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	%% Generate one-byte transactions so that recall byte is often on the
	%% the border between two transactions.
	TXs = [
		sign_tx(Key, #{ data => <<"A">>, tags => [random_nonce()], last_tx => get_tx_anchor() }),
		sign_tx(Key, #{ data => <<"B">>, tags => [random_nonce()], last_tx => get_tx_anchor() }),
		sign_tx(Key, #{ data => <<"B">>, tags => [random_nonce()], last_tx => get_tx_anchor() }),
		sign_tx(Key, #{ data => <<"C">>, tags => [random_nonce()], last_tx => get_tx_anchor() })
	],
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				1 ->
					assert_txs_mined(TXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(1, 10)
	).

over_reported_tx_size_test_() ->
	{timeout, 60, fun test_over_reported_tx_size/0}.

test_over_reported_tx_size() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(100), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	TXs = [
		%% Post a transaction with some data and correctly reported size
		%% so that we do not get stuck picking a recall byte.
		sign_tx(Key, #{
			data => crypto:strong_rand_bytes(100),
			tags => [random_nonce()],
			last_tx => get_tx_anchor()
		}),
		sign_tx(Key, #{
			data => <<"A">>,
			tags => [random_nonce()],
			data_size => 2,
			last_tx => get_tx_anchor()
		})
	],
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				1 ->
					assert_txs_mined(TXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(1, 4)
	).

under_reported_tx_size_test_() ->
	{timeout, 60, fun test_under_reported_tx_size/0}.

test_under_reported_tx_size() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(100), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	TXs = [
		%% Post a transaction with some data and correctly reported size
		%% so that we do not get stuck picking a recall byte.
		sign_tx(Key, #{
			data => crypto:strong_rand_bytes(100),
			tags => [random_nonce()],
			last_tx => get_tx_anchor()
		}),
		sign_tx(Key, #{
			data => <<"ABCDE">>,
			tags => [random_nonce()],
			data_size => 1,
			last_tx => get_tx_anchor()
		})
	],
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				1 ->
					assert_txs_mined(TXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(1, 4)
	).

ignores_transactions_with_invalid_data_root_test_() ->
	{timeout, 60, fun test_ignores_transactions_with_invalid_data_root/0}.

test_ignores_transactions_with_invalid_data_root() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub), ?AR(100), <<>>}
	]),
	{_Master, _} = start(B0),
	{_Slave, _} = slave_start(B0),
	connect_to_slave(),
	Default = slave_call(ar_meta_db, get, [max_poa_option_depth]),
	slave_call(ar_meta_db, put, [max_poa_option_depth, 100]),
	%% Generate transactions where half of them are valid and the other
	%% half has an invalid data_root.
	GenerateTXParams =
		fun
			(valid) ->
				#{ data => <<"DATA">>, tags => [random_nonce()], last_tx => get_tx_anchor() };
			(invalid) ->
				#{ data_root => crypto:strong_rand_bytes(32),
					data => <<"DATA">>, tags => [random_nonce()], last_tx => get_tx_anchor() }
		end,
	TXs = [
		sign_tx(Key, GenerateTXParams(valid)),
		sign_tx(Key, GenerateTXParams(invalid)),
		sign_tx(Key, GenerateTXParams(valid)),
		sign_tx(Key, GenerateTXParams(invalid)),
		sign_tx(Key, GenerateTXParams(valid)),
		sign_tx(Key, GenerateTXParams(invalid)),
		sign_tx(Key, GenerateTXParams(valid)),
		sign_tx(Key, GenerateTXParams(invalid)),
		sign_tx(Key, GenerateTXParams(valid)),
		sign_tx(Key, GenerateTXParams(invalid))
	],
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(TX)
		end,
		TXs
	),
	assert_wait_until_receives_txs(TXs),
	lists:foreach(
		fun(Height) ->
			slave_mine(),
			BI = wait_until_height(Height),
			case Height of
				1 ->
					assert_txs_mined(TXs, BI);
				_ ->
					noop
			end,
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(1, 10)
	),
	slave_call(ar_meta_db, put, [max_poa_option_depth, Default]).

generate_txs(Key, SignFun) ->
	[
		SignFun(Key, #{ data => <<>>, tags => [random_nonce()], last_tx => get_tx_anchor() }),
		SignFun(Key, #{ data => <<"B">>, tags => [random_nonce()], last_tx => get_tx_anchor() }),
		SignFun(
			Key, #{
				data => <<"DATA">>,
				tags => [random_nonce()],
				last_tx => get_tx_anchor()
			}
		),
		SignFun(
			Key, #{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE) >>,
				tags => [random_nonce()],
				last_tx => get_tx_anchor()
			}
		),
		SignFun(
			Key,
			#{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE * 2) >>,
				tags => [random_nonce()],
				last_tx => get_tx_anchor()
			}
		),
		SignFun(
			Key, #{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE * 3) >>,
				tags => [random_nonce()],
				last_tx => get_tx_anchor()
			}
		),
		SignFun(
			Key, #{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE * 13) >>,
				tags => [random_nonce()],
				last_tx => get_tx_anchor()
			}
		)
	].

random_nonce() ->
	{<<"nonce">>, integer_to_binary(rand:uniform(1000000))}.

assert_txs_mined(TXs, [{H, _, _} | _]) ->
	B = read_block_when_stored(H),
	TXIDs = [TX#tx.id || TX <- TXs],
	?assertEqual(length(TXIDs), length(B#block.txs)),
	?assertEqual(lists:sort(TXIDs), lists:sort(B#block.txs)).
