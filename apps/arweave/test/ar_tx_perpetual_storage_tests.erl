-module(ar_tx_perpetual_storage_tests).

-include("src/ar.hrl").
-include("src/perpetual_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% These functions serve as mocks and are exported to overcome the meck's
%% issue with picking them up after the module is rebuilt (e.g. during debugging).
-export([zero_height/0, small_inflation/1, big_inflation/1, calculate_reward_pool_huge_weave_size/8]).

-import(ar_difficulty, [twice_smaller_diff/1]).
-import(ar_tx_perpetual_storage, [get_cost_per_year_at_datetime/1]).

-import(ar_test_node, [start/2, slave_start/2, connect_to_slave/0]).
-import(ar_test_node, [slave_mine/1]).
-import(ar_test_node, [sign_tx/1, sign_tx/2]).
-import(ar_test_node, [assert_post_tx_to_slave/2, assert_post_tx_to_master/2]).
-import(ar_test_node, [wait_until_height/2, assert_slave_wait_until_height/2]).
-import(ar_test_node, [get_tx_anchor/0, get_tx_anchor/1]).
-import(ar_test_node, [get_balance/1]).
-import(ar_test_node, [test_with_mocked_functions/2]).

-define(HUGE_WEAVE_SIZE, 1000000000000000).

updates_pool_and_assigns_rewards_correctly_before_burden_test_() ->
	%% Smaller burden is achieved by starting with a zero weave size
	%% and setting significant inflation rewards.
	test_with_mocked_functions(
		[
			{ar_fork, height_2_0, fun() -> 0 end},
			{ar_inflation, calculate, fun big_inflation/1}
		],
		fun updates_pool_and_assigns_rewards_correctly_before_burden/0
	).

updates_pool_and_assigns_rewards_correctly_after_burden_test_() ->
	%% Bigger burden is achieved by mocking ar_inflation:calculate to return
	%% an insignificant reward value and overriding ar_node_utils:calculate_reward_pool
	%% to always accept a huge weave size.
	test_with_mocked_functions(
		[
			{ar_fork, height_2_0, fun ar_tx_perpetual_storage_tests:zero_height/0},
			{ar_inflation, calculate, fun ar_tx_perpetual_storage_tests:small_inflation/1},
			{ar_node_utils, calculate_reward_pool,
				fun ar_tx_perpetual_storage_tests:calculate_reward_pool_huge_weave_size/8}
		],
		fun updates_pool_and_assigns_rewards_correctly_after_burden/0
	).

get_cost_per_year_at_datetime_is_monotone_test() ->
	InitialDT = {{2019, 1, 1}, {0, 0, 0}},
	FollowingDTs = [
		{{2019, 1, 1}, {10, 0, 0}},
		{{2019, 6, 15}, {0, 0, 0}},
		{{2019, 6, 29}, {23, 59, 59}},
		{{2019, 6, 30}, {0, 0, 0}},
		{{2019, 6, 30}, {23, 59, 59}},
		{{2019, 7, 1}, {0, 0, 0}},
		{{2019, 12, 31}, {23, 59, 59}},
		{{2020, 1, 1}, {0, 0, 0}},
		{{2020, 1, 2}, {0, 0, 0}},
		{{2020, 10, 1}, {0, 0, 0}},
		{{2020, 12, 31}, {23, 59, 59}},
		{{2021, 1, 1}, {0, 0, 0}},
		{{2021, 2, 1}, {0, 0, 0}},
		{{2021, 12, 31}, {23, 59, 59}},
		{{2022, 1, 1}, {0, 0, 0}},
		{{2022, 6, 29}, {23, 59, 59}},
		{{2022, 6, 30}, {0, 0, 0}},
		{{2050, 3, 1}, {10, 10, 10}},
		{{2100, 2, 1}, {0, 0, 0}}
	],
	lists:foldl(
		fun(CurrDT, PrevDT) ->
			CurrCost = get_cost_per_year_at_datetime(CurrDT),
			PrevCost = get_cost_per_year_at_datetime(PrevDT),
			?assert(CurrCost =< PrevCost),
			CurrDT
		end,
		InitialDT,
		FollowingDTs
	).

zero_height() ->
	0.

small_inflation(_) ->
	1.

big_inflation(_) ->
	10000000000000.

calculate_reward_pool_huge_weave_size(OldPool, TXs, RewardAddr, POA, _WeaveSize, Height, Diff, Timestamp) ->
	meck:passthrough([OldPool, TXs, RewardAddr, POA, ?HUGE_WEAVE_SIZE, Height, Diff, Timestamp]).

updates_pool_and_assigns_rewards_correctly_before_burden() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Key3 = {_, Pub3} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(2000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
		{ar_wallet:to_address(Pub3), ?AR(2000), <<>>}
	]),
	{_, RewardAddr} = ar_wallet:new(),
	{Master, _} = start(B0, ar_wallet:to_address(RewardAddr)),
	{Slave, _} = slave_start(B0, ar_wallet:to_address(RewardAddr)),
	connect_to_slave(),
	%% Mine a block without transactions. Expect an inflation reward.
	slave_mine(Slave),
	BI1 = wait_until_height(Master, 1),
	B1 = ar_storage:read_block(hd(BI1), BI1),
	?assertEqual(0, B1#block.reward_pool),
	?assertEqual(ar_wallet:to_address(RewardAddr), B1#block.reward_addr),
	Balance1 = get_balance(RewardAddr),
	?assertEqual(ar_inflation:calculate(1), Balance1),
	%% Mine a block with an empty transaction. Expect an inflation reward and
	%% data reward for the base tx size.
	TX1 = sign_tx(Key1),
	assert_post_tx_to_slave(Slave, TX1),
	slave_mine(Slave),
	BI2 = wait_until_height(Master, 2),
	B2 = ar_storage:read_block(hd(BI2), BI2),
	RewardPoolIncrement1 = ar_tx_perpetual_storage:calculate_tx_cost(
		0,
		twice_smaller_diff(B2#block.diff),
		2,
		B2#block.timestamp
	),
	assert_almost_equal(B1#block.reward_pool + RewardPoolIncrement1, B2#block.reward_pool, 1),
	?assertEqual(ar_wallet:to_address(RewardAddr), B2#block.reward_addr),
	Balance2 = get_balance(RewardAddr),
	Reward =
		ar_inflation:calculate(2) +
		erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement1),
	assert_reward_bigger_than_burden(
		Reward,
		B2#block.diff,
		2,
		B2#block.timestamp,
		0
	),
	assert_almost_equal(Balance1 + Reward, Balance2, 1),
	%% Mine a block with a transaction. Expect a size-prorated data reward
	%% and an inflation reward.
	Data = << <<1>> || _ <- lists:seq(1, 10) >>,
	TX2 = sign_tx(Key2, #{ data => Data }),
	assert_post_tx_to_slave(Slave, TX2),
	slave_mine(Slave),
	BI3 = wait_until_height(Master, 3),
	B3 = ar_storage:read_block(hd(BI3), BI3),
	RewardPoolIncrement2 = ar_tx_perpetual_storage:calculate_tx_cost(
		byte_size(Data),
		twice_smaller_diff(B3#block.diff),
		3,
		B3#block.timestamp
	),
	assert_almost_equal(
		B2#block.reward_pool + RewardPoolIncrement2,
		B3#block.reward_pool,
		1
	),
	?assertEqual(B2#block.weave_size + byte_size(TX2#tx.data), B3#block.weave_size),
	?assertEqual(ar_wallet:to_address(RewardAddr), B3#block.reward_addr),
	Balance3 = get_balance(RewardAddr),
	Reward2 =
		ar_inflation:calculate(3) +
		erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement2),
	assert_reward_bigger_than_burden(
		Reward2,
		B3#block.diff,
		3,
		B3#block.timestamp,
		B3#block.weave_size
	),
	assert_almost_equal(Balance2 + Reward2, Balance3, 1),
	%% Mine a block with four transactions from three different wallets.
	%% Expect the totals to be correct.
	Data2 = << <<2>> || _ <- lists:seq(1, 15) >>,
	Data3 = << <<2>> || _ <- lists:seq(1, 25) >>,
	Data4 = << <<2>> || _ <- lists:seq(1, 50) >>,
	Data5 = << <<2>> || _ <- lists:seq(1, 5) >>,
	TX3 = sign_tx(Key1, #{ data => Data2, last_tx => get_tx_anchor(master) }),
	TX4 = sign_tx(Key2, #{ data => Data3, last_tx => get_tx_anchor(master) }),
	TX5 = sign_tx(Key3, #{ data => Data4, last_tx => get_tx_anchor(master) }),
	TX6 = sign_tx(Key2, #{ data => Data5, last_tx => get_tx_anchor(master) }),
	lists:foreach(
		fun(TX) ->
			assert_post_tx_to_master(Master, TX)
		end,
		[TX3, TX4, TX5, TX6]
	),
	%% Store the previous transactions posted to slave on master
	%% so that it has PoA data to mine.
	ar_storage:write_tx([TX1, TX2]),
	ar_node:mine(Master),
	BI4 = assert_slave_wait_until_height(Slave, 4),
	B4 = ar_storage:read_block(hd(BI4), BI4),
	{RewardPoolIncrement3, WeaveSizeIncrement} = lists:foldl(
		fun(Chunk, {Sum, Size}) ->
			{Sum + ar_tx_perpetual_storage:calculate_tx_cost(
				byte_size(Chunk),
				twice_smaller_diff(B4#block.diff),
				4,
				B4#block.timestamp
			), Size + byte_size(Chunk)}
		end,
		{0, 0},
		[Data2, Data3, Data4, Data5]
	),
	assert_almost_equal(B3#block.reward_pool + RewardPoolIncrement3, B4#block.reward_pool, 1),
	?assertEqual(ar_wallet:to_address(RewardAddr), B4#block.reward_addr),
	?assertEqual(B3#block.weave_size + WeaveSizeIncrement, B4#block.weave_size),
	Balance4 = get_balance(RewardAddr),
	Reward3 =
		ar_inflation:calculate(4) +
		erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement3),
	assert_reward_bigger_than_burden(
		Reward3,
		B4#block.diff,
		4,
		B4#block.timestamp,
		B4#block.weave_size
	),
	assert_almost_equal(Balance3 + Reward3, Balance4, 5).

updates_pool_and_assigns_rewards_correctly_after_burden() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	[BNoPool] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(2000), <<>>}
	]),
	B0 = BNoPool#block{ reward_pool = ?AR(10000000000000) },
	{_, RewardAddr} = ar_wallet:new(),
	{Master, _} = start(B0, ar_wallet:to_address(RewardAddr)),
	{Slave, _} = slave_start(B0, ar_wallet:to_address(RewardAddr)),
	connect_to_slave(),
	%% Mine a block with a transaction. Expect the reward to contain inflation, data reward,
	%% and the correct portion of the pool.
	BigChunk = << <<3>> || _ <- lists:seq(1, 10000) >>,
	TX1 = sign_tx(Key1, #{ data => BigChunk, last_tx => get_tx_anchor() }),
	assert_post_tx_to_slave(Slave, TX1),
	slave_mine(Slave),
	BI1 = wait_until_height(Master, 1),
	B1 = ar_storage:read_block(hd(BI1), BI1),
	RewardPoolIncrement1 = ar_tx_perpetual_storage:calculate_tx_cost(
		byte_size(BigChunk),
		twice_smaller_diff(B1#block.diff),
		1,
		B1#block.timestamp
	),
	BaseReward1 = ar_inflation:calculate(1) + erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement1),
	PoolShare1 = get_miner_pool_share(
		B1#block.diff,
		B1#block.timestamp,
		?HUGE_WEAVE_SIZE,
		BaseReward1,
		1
	),
	assert_almost_equal(
		B0#block.reward_pool + RewardPoolIncrement1 - PoolShare1,
		B1#block.reward_pool,
		1
	),
	?assertEqual(byte_size(TX1#tx.data), B1#block.weave_size),
	?assertEqual(ar_wallet:to_address(RewardAddr), B1#block.reward_addr),
	Balance1 = get_balance(RewardAddr),
	assert_almost_equal(BaseReward1 + PoolShare1, Balance1, 1),
	%% Mine an empty block. Expect an inflation reward and a share of the pool.
	slave_mine(Slave),
	BI2 = wait_until_height(Master, 2),
	B2 = ar_storage:read_block(hd(BI2), BI2),
	BaseReward2 = ar_inflation:calculate(2),
	PoolShare2 = get_miner_pool_share(
		B2#block.diff,
		B2#block.timestamp,
		?HUGE_WEAVE_SIZE,
		BaseReward2,
		2
	),
	Balance2 = get_balance(RewardAddr),
	assert_almost_equal(Balance1 + BaseReward2 + PoolShare2, Balance2, 1).

assert_almost_equal(Num1, Num2, Precision) ->
	?assert(
		abs(Num2 - Num1) < Precision,
		io_lib:format("Expected ~B to be almost equal ~B with precision ~B", [Num1, Num2, Precision])
	).

assert_reward_bigger_than_burden(Reward, Diff, Height, Timestamp, WeaveSize) ->
	Cost = ar_tx_perpetual_storage:usd_to_ar(
		ar_tx_perpetual_storage:get_cost_per_block_at_timestamp(Timestamp),
		Diff,
		Height
	),
	Burden = erlang:trunc(WeaveSize * Cost / (1024 * 1024 * 1024)),
	?assert(Reward > Burden).

get_miner_pool_share(Diff, Timestamp, WeaveSize, BaseReward, Height) ->
	Cost = ar_tx_perpetual_storage:usd_to_ar(
		ar_tx_perpetual_storage:get_cost_per_block_at_timestamp(Timestamp),
		Diff,
		Height
	),
	Burden = erlang:trunc(WeaveSize * Cost / (1024 * 1024 * 1024)),
	AR = Burden - BaseReward,
	?assert(AR > 0),
	AR.
