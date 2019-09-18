-module(ar_tx_perpetual_storage_tests).

-include("src/ar.hrl").
-include("src/perpetual_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_difficulty, [twice_smaller_diff/1]).

-import(ar_test_node, [start/2, slave_start/2, connect_to_slave/0]).
-import(ar_test_node, [slave_mine/1]).
-import(ar_test_node, [sign_tx/1, sign_tx/2]).
-import(ar_test_node, [assert_post_tx_to_slave/2, assert_post_tx_to_master/2]).
-import(ar_test_node, [wait_until_height/2, assert_slave_wait_until_height/2]).
-import(ar_test_node, [get_tx_anchor/0, get_tx_anchor/1]).
-import(ar_test_node, [get_balance/1]).

-import(ar_test_fork, [test_on_fork/3]).

updates_pool_and_assigns_rewards_correctly_test_() ->
	test_on_fork(height_1_8, 0, fun updates_pool_and_assigns_rewards_correctly/0).

updates_pool_and_assigns_rewards_correctly() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Key3 = {_, Pub3} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(20), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(20), <<>>},
		{ar_wallet:to_address(Pub3), ?AR(20), <<>>}
	]),
	{_, RewardAddr} = ar_wallet:new(),
	{Master, _} = start(B0, ar_wallet:to_address(RewardAddr)),
	{Slave, _} = slave_start(B0, ar_wallet:to_address(RewardAddr)),
	connect_to_slave(),
	%% Mine a block without transactions. Expect an inflation reward.
	slave_mine(Slave),
	BHL1 = wait_until_height(Master, 1),
	B1 = ar_storage:read_block(hd(BHL1), BHL1),
	?assertEqual(0, B1#block.reward_pool),
	?assertEqual(ar_wallet:to_address(RewardAddr), B1#block.reward_addr),
	Balance1 = get_balance(RewardAddr),
	?assertEqual(ar_inflation:calculate(1), Balance1),
	%% Mine a block with an empty transaction. Expect an inflation reward and
	%% data reward for the base tx size.
	TX1 = sign_tx(Key1),
	assert_post_tx_to_slave(Slave, TX1),
	slave_mine(Slave),
	BHL2 = wait_until_height(Master, 2),
	B2 = ar_storage:read_block(hd(BHL2), BHL2),
	RewardPoolIncrement1 = ar_tx_perpetual_storage:calculate_tx_cost(
		0,
		twice_smaller_diff(B2#block.diff),
		2,
		B2#block.timestamp
	),
	assert_almost_equal(B1#block.reward_pool + RewardPoolIncrement1, B2#block.reward_pool, 10),
	?assertEqual(ar_wallet:to_address(RewardAddr), B2#block.reward_addr),
	Balance2 = get_balance(RewardAddr),
	assert_almost_equal(
		ar_inflation:calculate(2) + Balance1 + erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement1),
		Balance2,
		10
	),
	%% Mine a block with a transaction. Expect a size-prorated data reward
	%% and an inflation reward.
	Data = << <<1>> || _ <- lists:seq(1, 10) >>,
	TX2 = sign_tx(Key2, #{ data => Data }),
	assert_post_tx_to_slave(Slave, TX2),
	slave_mine(Slave),
	BHL3 = wait_until_height(Master, 3),
	B3 = ar_storage:read_block(hd(BHL3), BHL3),
	RewardPoolIncrement2 = ar_tx_perpetual_storage:calculate_tx_cost(
		byte_size(Data),
		twice_smaller_diff(B3#block.diff),
		3,
		B3#block.timestamp
	),
	assert_almost_equal(B2#block.reward_pool + RewardPoolIncrement2, B3#block.reward_pool, 10),
	?assertEqual(byte_size(TX2#tx.data), B3#block.weave_size),
	?assertEqual(ar_wallet:to_address(RewardAddr), B3#block.reward_addr),
	Balance3 = get_balance(RewardAddr),
	assert_almost_equal(
		ar_inflation:calculate(3) + Balance2 + erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement2),
		Balance3,
		10
	),
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
	ar_node:mine(Master),
	BHL4 = assert_slave_wait_until_height(Master, 4),
	B4 = ar_storage:read_block(hd(BHL4), BHL4),
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
	assert_almost_equal(B3#block.reward_pool + RewardPoolIncrement3, B4#block.reward_pool, 10),
	?assertEqual(ar_wallet:to_address(RewardAddr), B4#block.reward_addr),
	?assertEqual(B3#block.weave_size + WeaveSizeIncrement, B4#block.weave_size),
	Balance4 = get_balance(RewardAddr),
	assert_almost_equal(
		ar_inflation:calculate(4) + Balance3 + ?MINING_REWARD_MULTIPLIER * RewardPoolIncrement3,
		Balance4,
		10
	),
	%% Post a big transaction so that the burden becomes bigger than the reward. Expect
	%% the reward to contain inflation, data reward, and the correct portion of the pool.
	BigChunk = << <<3>> || _ <- lists:seq(1, 10000) >>,
	TX7 = sign_tx(Key1, #{ data => BigChunk, last_tx => get_tx_anchor() }),
	assert_post_tx_to_slave(Slave, TX7),
	slave_mine(Slave),
	BHL5 = assert_slave_wait_until_height(Slave, 5),
	B5 = ar_storage:read_block(hd(BHL5), BHL5),
	RecallB = ar_storage:read_block(ar_weave:calculate_recall_block(hd(BHL4), BHL4), BHL4),
	RewardPoolIncrement4 = ar_tx_perpetual_storage:calculate_tx_cost(
		byte_size(BigChunk),
		twice_smaller_diff(B5#block.diff),
		5,
		B5#block.timestamp
	),
	BaseReward = ar_inflation:calculate(5) + erlang:trunc(?MINING_REWARD_MULTIPLIER * RewardPoolIncrement4),
	PoolShare = get_miner_pool_share(
		B5#block.diff,
		B5#block.timestamp,
		B5#block.weave_size,
		BaseReward,
		5,
		RecallB#block.block_size
	),
	assert_almost_equal(B4#block.reward_pool + RewardPoolIncrement4 - PoolShare, B5#block.reward_pool, 10),
	?assertEqual(B4#block.weave_size + byte_size(TX7#tx.data), B5#block.weave_size),
	?assertEqual(ar_wallet:to_address(RewardAddr), B5#block.reward_addr),
	Balance5 = get_balance(RewardAddr),
	assert_almost_equal(Balance4 + BaseReward + PoolShare, Balance5, 10).

assert_almost_equal(Num1, Num2, Precision) ->
	?assert(
		abs(Num2 - Num1) < Precision,
		io_lib:format("Expected ~B to be almost equal ~B", [Num1, Num2])
	).

get_miner_pool_share(Diff, Timestamp, WeaveSize, BaseReward, Height, RecallSize) ->
	Cost = ar_tx_perpetual_storage:usd_to_ar(
		ar_tx_perpetual_storage:perpetual_cost_at_timestamp(Timestamp),
		Diff,
		Height
	),
	Burden = erlang:trunc(WeaveSize * Cost / (1024 * 1024 * 1024)),
	AR = Burden - BaseReward,
	erlang:trunc(AR * max(1, RecallSize) * Height / WeaveSize).
