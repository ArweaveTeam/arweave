-module(ar_poa_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_fork, [test_on_fork/3]).

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0]).
-import(ar_test_node, [assert_post_tx_to_slave/2]).
-import(ar_test_node, [slave_mine/1]).
-import(ar_test_node, [wait_until_height/2, assert_slave_wait_until_height/2]).

v1_transactions_after_2_0_test_() ->
	test_on_fork(height_2_0, 0, fun test_v1_transactions_after_2_0/0).

test_v1_transactions_after_2_0() ->
	Key = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	]),
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	TXs = generate_txs(Key, fun ar_test_node:sign_v1_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		TXs
	),
	lists:foreach(
		fun(Height) ->
			slave_mine(Slave),
			wait_until_height(Master, Height),
			assert_slave_wait_until_height(Slave, Height)
		end,
		lists:seq(1, 10)
	),
	MoreTXs = generate_txs(Key2, fun ar_test_node:sign_v1_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		MoreTXs
	),
	lists:foreach(
		fun(Height) ->
			slave_mine(Slave),
			wait_until_height(Master, Height),
			assert_slave_wait_until_height(Slave, Height)
		end,
		lists:seq(11, 20)
	).


v2_transactions_after_2_0_test_() ->
	test_on_fork(height_2_0, 0, fun test_v2_transactions_after_2_0/0).

test_v2_transactions_after_2_0() ->
	Key = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(100), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(100), <<>>}
	]),
	{Master, _} = start(B0),
	{Slave, _} = slave_start(B0),
	connect_to_slave(),
	TXs = generate_txs(Key, fun ar_test_node:sign_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		TXs
	),
	lists:foreach(
		fun(Height) ->
			slave_mine(Slave),
			wait_until_height(Master, Height),
			assert_slave_wait_until_height(Slave, Height)
		end,
		lists:seq(1, 10)
	),
	MoreTXs = generate_txs(Key2, fun ar_test_node:sign_tx/2),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_slave(Slave, TX)
		end,
		MoreTXs
	),
	lists:foreach(
		fun(Height) ->
			slave_mine(Slave),
			wait_until_height(Master, Height),
			assert_slave_wait_until_height(Slave, Height)
		end,
		lists:seq(11, 20)
	).

generate_txs(Key, SignFun) ->
	[
		SignFun(Key, #{ data => <<>>, tags => [random_nonce()] }),
		SignFun(Key, #{ data => <<"B">>, tags => [random_nonce()] }),
		SignFun(Key, #{ data => <<"DATA">>, tags => [random_nonce()] }),
		SignFun(
			Key, #{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE) >>,
				tags => [random_nonce()]
			}
		),
		SignFun(
			Key,
			#{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE * 2) >>,
				tags => [random_nonce()]
			}
			),
		SignFun(
			Key, #{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE * 3) >>,
				tags => [random_nonce()]
			}
		),
		SignFun(
			Key, #{
				data => << <<"B">> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE * 13) >>,
				tags => [random_nonce()]
			}
		)
	].

random_nonce() ->
	{<<"nonce">>, integer_to_binary(rand:uniform(1000000))}.
