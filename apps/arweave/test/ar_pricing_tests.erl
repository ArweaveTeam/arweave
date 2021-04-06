-module(ar_pricing_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("eunit/include/eunit.hrl").

%% These functions serve as mocks and are exported to overcome the meck's
%% issue with picking them up after the module is rebuilt (e.g. during debugging).
-export([get_miner_reward_and_endowment_pool/1]).

-import(ar_test_node, [
	start/1, start/2, slave_start/2, connect_to_slave/0, slave_call/3,
	slave_mine/0,
	sign_tx/1, sign_tx/2,
	assert_post_tx_to_slave/1, assert_post_tx_to_master/1,
	wait_until_height/1, assert_slave_wait_until_height/1,
	get_tx_anchor/0, get_tx_anchor/1,
	get_balance/1,
	test_with_mocked_functions/2,
	read_block_when_stored/1
]).

-import(ar_test_fork, [
	test_on_fork/3
]).

-define(HUGE_WEAVE_SIZE, 1000000000000000).

updates_pool_and_assigns_rewards_correctly_before_burden_test_() ->
	test_on_fork(height_2_5, 0, fun updates_pool_and_assigns_rewards_correctly_before_burden/0).

updates_pool_and_assigns_rewards_correctly_after_burden_test_() ->
	%% Bigger burden is achieved by mocking `ar_pricing:get_miner_reward_and_endowment_pool/1`
	%% so that it considers the weave size really big. Otherwise, we cannot start a big
	%% weave without storing a significant share of the data - test nodes won't be able to
	%% mine blocks.
	test_on_fork(height_2_5, 0,
		test_with_mocked_functions(
			[
				{ar_pricing, get_miner_reward_and_endowment_pool,
					fun ar_pricing_tests:get_miner_reward_and_endowment_pool/1}
			],
			fun updates_pool_and_assigns_rewards_correctly_after_burden/0
		)
	).

unclaimed_rewards_go_to_endowment_pool_test_() ->
	test_on_fork(height_2_5, 0, fun test_unclaimed_rewards_go_to_endowment_pool/0).

get_miner_reward_and_endowment_pool(Args) ->
	{Pool, TXs, Addr, _WeaveSize, Height, Timestamp, Rate} = Args,
	meck:passthrough([{Pool, TXs, Addr, ?HUGE_WEAVE_SIZE, Height, Timestamp, Rate}]).

updates_pool_and_assigns_rewards_correctly_before_burden() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Key3 = {_, Pub3} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(2000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
		{ar_wallet:to_address(Pub3), ?AR(2000), <<>>}
	]),
	RewardKey = {_, RewardAddr} = ar_wallet:new(),
	{_Master, _} = start(B0, ar_wallet:to_address(RewardAddr)),
	{_Slave, _} = slave_start(B0, ar_wallet:to_address(RewardAddr)),
	connect_to_slave(),
	%% Mine a block without transactions. Expect an inflation reward.
	slave_mine(),
	BI1 = wait_until_height(1),
	B1 = read_block_when_stored(hd(BI1)),
	?assertEqual(0, B1#block.reward_pool),
	?assertEqual(ar_wallet:to_address(RewardAddr), B1#block.reward_addr),
	Balance = get_balance(RewardAddr),
	?assertEqual(ar_inflation:calculate(1), Balance),
	%% Mine a block with an empty transaction. Expect an inflation reward and
	%% data reward for the base tx size.
	TX1 = sign_tx(Key1),
	assert_post_tx_to_slave(TX1),
	slave_mine(),
	BI2 = wait_until_height(2),
	B2 = read_block_when_stored(hd(BI2)),
	RewardPoolIncrement = B2#block.reward_pool - B1#block.reward_pool,
	TXFee = ar_pricing:get_tx_fee(0, B2#block.timestamp, B1#block.usd_to_ar_rate, 2),
	?assertEqual(ar_wallet:to_address(RewardAddr), B2#block.reward_addr),
	Balance2 = get_balance(RewardAddr),
	MinerReward = Balance2 - Balance,
	?assertEqual(TXFee + trunc(ar_inflation:calculate(2)), MinerReward + RewardPoolIncrement),
	{MultiplierDividend, MultiplierDivisor} = ?MINING_REWARD_MULTIPLIER,
	?assertEqual(
		MultiplierDividend * RewardPoolIncrement div MultiplierDivisor,
		MinerReward - trunc(ar_inflation:calculate(2))
	),
	%% Mine a block with a transaction. Expect a size-prorated data reward
	%% and an inflation reward.
	Data = << <<1>> || _ <- lists:seq(1, 10) >>,
	TX2 = sign_tx(Key2, #{ data => Data }),
	assert_post_tx_to_slave(TX2),
	slave_mine(),
	BI3 = wait_until_height(3),
	B3 = read_block_when_stored(hd(BI3)),
	RewardPoolIncrement2 = B3#block.reward_pool - B2#block.reward_pool,
	TXFee2 =
		ar_pricing:get_tx_fee(byte_size(Data), B2#block.timestamp, B1#block.usd_to_ar_rate, 2),
	?assertEqual(ar_wallet:to_address(RewardAddr), B3#block.reward_addr),
	Balance3 = get_balance(RewardAddr),
	MinerReward2 = Balance3 - Balance2,
	?assertEqual(
		TXFee2 + trunc(ar_inflation:calculate(3)),
		MinerReward2 + RewardPoolIncrement2
	),
	?assertEqual(
		MultiplierDividend * RewardPoolIncrement2 div MultiplierDivisor,
		MinerReward2 - trunc(ar_inflation:calculate(3))
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
			assert_post_tx_to_master(TX)
		end,
		[TX3, TX4, TX5, TX6]
	),
	%% Store the previous transactions posted to slave on master
	%% so that it has PoA data to mine.
	ar_storage:write_tx([TX1, TX2]),
	ar_node:mine(),
	BI4 = assert_slave_wait_until_height(4),
	B4 = read_block_when_stored(hd(BI4)),
	RewardPoolIncrement3 = B4#block.reward_pool - B3#block.reward_pool,
	TXFee3 =
		lists:foldl(
			fun(Chunk, Sum) ->
				Rate = B3#block.usd_to_ar_rate,
				Sum + ar_pricing:get_tx_fee(byte_size(Chunk), B4#block.timestamp, Rate, 4)
			end,
			0,
			[Data2, Data3, Data4, Data5]
		),
	?assertEqual(ar_wallet:to_address(RewardAddr), B4#block.reward_addr),
	Balance4 = get_balance(RewardAddr),
	MinerReward3 = Balance4 - Balance3,
	?assertEqual(
		TXFee3 + trunc(ar_inflation:calculate(4)),
		MinerReward3 + RewardPoolIncrement3
	),
	%% Post a transaction from the mining wallet. Expect an increase of the
	%% inflation reward minus what goes to the pool because the size-prorated
	%% data reward is paid to the same wallet.
	RewardWalletTX = sign_tx(RewardKey, #{ data => << <<1>> || _ <- lists:seq(1, 11) >> }),
	assert_post_tx_to_master(RewardWalletTX),
	ar_node:mine(),
	BI5 = assert_slave_wait_until_height(5),
	B5 = slave_call(ar_test_node, read_block_when_stored, [hd(BI5)]),
	RewardPoolIncrement4 = B5#block.reward_pool - B4#block.reward_pool,
	TXFee4 =
		ar_pricing:get_tx_fee(
			byte_size(RewardWalletTX#tx.data),
			B5#block.timestamp,
			B4#block.usd_to_ar_rate,
			5
		),
	?assertEqual(B4#block.weave_size + byte_size(RewardWalletTX#tx.data), B5#block.weave_size),
	?assertEqual(ar_wallet:to_address(RewardAddr), B5#block.reward_addr),
	Balance5 = get_balance(RewardAddr),
	?assertEqual(
		Balance4
			- TXFee4
			+ trunc(ar_inflation:calculate(5))
			+ MultiplierDividend * RewardPoolIncrement4 div MultiplierDivisor,
		Balance5
	).

updates_pool_and_assigns_rewards_correctly_after_burden() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	[BNoPool] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(2000), <<>>}
	]),
	B0 = BNoPool#block{ reward_pool = ?AR(10000000000000) },
	RewardKey = {_, RewardAddr} = ar_wallet:new(),
	{_Master, _} = start(B0, ar_wallet:to_address(RewardAddr)),
	{_Slave, _} = slave_start(B0, ar_wallet:to_address(RewardAddr)),
	connect_to_slave(),
	Balance = get_balance(RewardAddr),
	%% Mine a block with a transaction. Expect the reward to contain inflation, data reward,
	%% and some portion of the endowment pool.
	BigChunk = << <<3>> || _ <- lists:seq(1, 10000) >>,
	TX1 = sign_tx(Key1, #{ data => BigChunk, last_tx => get_tx_anchor() }),
	assert_post_tx_to_slave(TX1),
	slave_mine(),
	BI1 = wait_until_height(1),
	B1 = read_block_when_stored(hd(BI1)),
	RewardPoolIncrement = B1#block.reward_pool - B0#block.reward_pool,
	TXFee =
		ar_pricing:get_tx_fee(
			byte_size(BigChunk),
			B1#block.timestamp,
			B0#block.usd_to_ar_rate,
			1
		),
	Balance2 = get_balance(RewardAddr),
	MinerReward = Balance2 - Balance,
	?assertEqual(TXFee + trunc(ar_inflation:calculate(1)), MinerReward + RewardPoolIncrement),
	{MultiplierDividend, MultiplierDivisor} = ?MINING_REWARD_MULTIPLIER,
	?assert(
		MinerReward >
			trunc(ar_inflation:calculate(1))
			+ MultiplierDividend * RewardPoolIncrement div MultiplierDivisor
	),
	?assertEqual(byte_size(TX1#tx.data), B1#block.weave_size),
	%% Mine an empty block. Expect an inflation reward and a share of the endowment pool.
	slave_mine(),
	BI2 = wait_until_height(2),
	B2 = read_block_when_stored(hd(BI2)),
	RewardPoolIncrement2 = B2#block.reward_pool - B1#block.reward_pool,
	?assert(RewardPoolIncrement2 < 0),
	Balance3 = get_balance(RewardAddr),
	MinerReward2 = Balance3 - Balance2,
	?assertEqual(trunc(ar_inflation:calculate(2)), MinerReward2 + RewardPoolIncrement2),
	%% Post a transaction from the mining wallet. Expect an increase of the
	%% inflation reward minus what goes to the endowment pool because the size-prorated
	%% data reward is paid to the same wallet.
	RewardWalletTX = sign_tx(RewardKey, #{ data => << <<1>> || _ <- lists:seq(1, 11) >> }),
	assert_post_tx_to_slave(RewardWalletTX),
	slave_mine(),
	BI3 = wait_until_height(3),
	B3 = read_block_when_stored(hd(BI3)),
	RewardPoolIncrement3 = B3#block.reward_pool - B2#block.reward_pool,
	TXFee2 =
		ar_pricing:get_tx_fee(
			RewardWalletTX#tx.data_size,
			B3#block.timestamp,
			B2#block.usd_to_ar_rate,
			3
		),
	Balance4 = get_balance(RewardAddr),
	MinerReward3 = Balance4 - Balance3,
	?assertEqual(
		trunc(ar_inflation:calculate(3)),
		MinerReward3 + RewardPoolIncrement3
	),
	?assert(
		MinerReward3 >
			trunc(ar_inflation:calculate(3))
			+ MultiplierDividend * RewardPoolIncrement3 div MultiplierDivisor
			- TXFee2
	).

test_unclaimed_rewards_go_to_endowment_pool() ->
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(2000), <<>>}]),
	start(B0),
	%% Mine a block without transactions. Expect no endowment pool increase.
	ar_node:mine(),
	BI1 = wait_until_height(1),
	B1 = read_block_when_stored(hd(BI1)),
	?assertEqual(0, B1#block.reward_pool),
	%% Mine a block with an empty transaction. Expect the endowment pool
	%% to receive the data reward for the base tx size.
	TX1 = sign_tx(Key),
	assert_post_tx_to_master(TX1),
	ar_node:mine(),
	BI2 = wait_until_height(2),
	B2 = ar_storage:read_block(hd(BI2)),
	RewardPoolIncrement = B2#block.reward_pool - B1#block.reward_pool,
	TXFee = ar_pricing:get_tx_fee(0, B2#block.timestamp, B1#block.usd_to_ar_rate, 2),
	?assertEqual(TX1#tx.reward, TXFee),
	?assertEqual(TXFee, RewardPoolIncrement),
	%% Mine a block with four transactions. Expect the endowment pool
	%% to receive the total of the fees.
	Data = << <<2>> || _ <- lists:seq(1, 175) >>,
	Data2 = << <<2>> || _ <- lists:seq(1, 25) >>,
	Data3 = << <<2>> || _ <- lists:seq(1, 1) >>,
	Data4 = << <<2>> || _ <- lists:seq(1, 500 * 1024) >>,
	TX2 = sign_tx(Key, #{ data => Data, last_tx => get_tx_anchor(master) }),
	TX3 = sign_tx(Key, #{ data => Data2, last_tx => get_tx_anchor(master) }),
	TX4 = sign_tx(Key, #{ data => Data3, last_tx => get_tx_anchor(master) }),
	TX5 = sign_tx(Key, #{ data => Data4, last_tx => get_tx_anchor(master) }),
	TotalFee =
		lists:foldl(
			fun(TX, Sum) ->
				assert_post_tx_to_master(TX),
				Sum + TX#tx.reward
			end,
			0,
			[TX2, TX3, TX4, TX5]
		),
	ar_node:mine(),
	BI3 = wait_until_height(3),
	B3 = read_block_when_stored(hd(BI3)),
	RewardPoolIncrement2 = B3#block.reward_pool - B2#block.reward_pool,
	?assertEqual(TotalFee, RewardPoolIncrement2).
