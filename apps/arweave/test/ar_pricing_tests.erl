-module(ar_pricing_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% These functions serve as mocks and are exported to overcome the meck's
%% issue with picking them up after the module is rebuilt (e.g. during debugging).
-export([get_miner_reward_and_endowment_pool/1]).

-import(ar_test_node, [start/1, start/2, slave_start/1, slave_start/2, connect_to_slave/0,
		slave_call/3, slave_mine/0, sign_tx/1, sign_tx/2, sign_tx/3, sign_v1_tx/3,
		get_tx_price/3, get_tx_price/2, get_optimistic_tx_price/2, get_optimistic_tx_price/3,
		assert_post_tx_to_slave/1, assert_post_tx_to_master/1, post_tx_to_master/1,
		get_balance/2, get_reserved_balance/2, wait_until_height/1,
		assert_slave_wait_until_height/1, get_tx_anchor/0,
		get_tx_anchor/1, get_balance/1, test_with_mocked_functions/2,
		read_block_when_stored/1]).

-define(HUGE_WEAVE_SIZE, 1000000000000000).

auto_redenomination_and_endowment_debt_test_() ->
	{timeout, 120, fun test_auto_redenomination_and_endowment_debt/0}.

test_auto_redenomination_and_endowment_debt() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	{_, Pub2} = ar_wallet:new(),
	Key3 = {_, Pub3} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), 20000000000000, <<>>},
		{ar_wallet:to_address(Pub2), 2000000000, <<>>},
		{ar_wallet:to_address(Pub3), ?AR(1000000000000000000), <<>>}
	]),
	start(B0),
	slave_start(B0),
	connect_to_slave(),
	?assert(?PRICE_2_6_TRANSITION_START == 2),
	?assert(?PRICE_2_6_TRANSITION_BLOCKS == 2),
	?assert(?PRICE_ADJUSTMENT_FREQUENCY == 2),
	?assert(?REDENOMINATION_DELAY_BLOCKS == 2),
	?assert(?REWARD_HISTORY_BLOCKS == 3),
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assertEqual(262144 * 3, B0#block.weave_size),
	{ok, Config} = application:get_env(arweave, config),
	{_, MinerPub} = ar_wallet:load_key(Config#config.mining_addr),
	?assertEqual(0, get_balance(master, MinerPub)),
	?assertEqual(0, get_reserved_balance(master, Config#config.mining_addr)),
	ar_node:mine(),
	_BI1 = wait_until_height(1),
	assert_slave_wait_until_height(1),
	?assertEqual(0, get_balance(master, MinerPub)),
	B1 = ar_node:get_current_block(),
	?assertEqual(10, B1#block.reward),
	?assertEqual(10, get_reserved_balance(master, B1#block.reward_addr)),
	?assertEqual(1, hash_rate(B1)),
	MinerAddr = ar_wallet:to_address(MinerPub),
	?assertEqual([{MinerAddr, 1, 10, 1}, {B0#block.reward_addr, 1, 10, 1}],
			B1#block.reward_history),
	%% The price is recomputed every two blocks.
	?assertEqual(B1#block.price_per_gib_minute, B1#block.scheduled_price_per_gib_minute),
	ar_node:mine(),
	_BI2 = wait_until_height(2),
	assert_slave_wait_until_height(2),
	?assertEqual(0, get_balance(master, MinerPub)),
	B2 = ar_node:get_current_block(),
	?assertEqual(10, B2#block.reward),
	?assertEqual(20, get_reserved_balance(master, B2#block.reward_addr)),
	?assertEqual(B1#block.price_per_gib_minute, B2#block.price_per_gib_minute),
	?assertEqual(B2#block.scheduled_price_per_gib_minute, B2#block.price_per_gib_minute),
	?assertEqual([{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1},
			{B0#block.reward_addr, 1, 10, 1}], B2#block.reward_history),
	ar_node:mine(),
	_BI3 = wait_until_height(3),
	assert_slave_wait_until_height(3),
	?assertEqual(0, get_balance(master, MinerPub)),
	B3 = ar_node:get_current_block(),
	?assertEqual(10, B3#block.reward),
	?assertEqual(30, get_reserved_balance(master, B3#block.reward_addr)),
	?assertEqual(B3#block.price_per_gib_minute, B2#block.price_per_gib_minute),
	?assertEqual(B3#block.scheduled_price_per_gib_minute,
			B2#block.scheduled_price_per_gib_minute),
	?assertEqual(0, B3#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(1, B3#block.kryder_plus_rate_multiplier),
	?assertEqual([{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1},
			{B0#block.reward_addr, 1, 10, 1}], B3#block.reward_history),
	Fee = get_optimistic_tx_price(master, 1024),
	ar_node:mine(),
	_BI4 = wait_until_height(4),
	assert_slave_wait_until_height(4),
	B4 = ar_node:get_current_block(),
	%% We are at the height ?PRICE_2_6_TRANSITION_START + ?PRICE_2_6_TRANSITION_BLOCKS
	%% so the new algorithm kicks in which estimates the expected block reward and takes
	%% the missing amount from the endowment pool or takes on debt.
	ExpectedReward4 = B3#block.price_per_gib_minute
			* ?N_REPLICATIONS(B3#block.height)
			* 2 % minutes
			* 3 div (4 * 1024), % weave_size / GiB
	?assertEqual(ExpectedReward4, B4#block.reward),
	?assertEqual(ExpectedReward4 + 20, get_reserved_balance(master, B4#block.reward_addr)),
	?assertEqual(10, get_balance(master, MinerPub)),
	?assertEqual(ExpectedReward4 - 10, B4#block.debt_supply),
	?assertEqual(1, B4#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B4#block.kryder_plus_rate_multiplier),
	?assertEqual(B4#block.price_per_gib_minute, B3#block.scheduled_price_per_gib_minute),
	?assertEqual(ar_pricing:get_price_per_gib_minute(lists:sublist(B3#block.reward_history, 3),
			1), B4#block.scheduled_price_per_gib_minute),
	%% The Kryder+ rate multiplier is 2 now so the fees should have doubled.
	?assert(lists:member(get_optimistic_tx_price(master, 1024), [Fee * 2, Fee * 2 + 1])),
	?assertEqual([{MinerAddr, 1, ExpectedReward4, 1}, {MinerAddr, 1, 10, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {B0#block.reward_addr, 1, 10, 1}],
			B4#block.reward_history),
	?assertEqual(ar_block:reward_history_hash([{MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}]), B4#block.reward_history_hash),
	ar_node:mine(),
	_BI5 = wait_until_height(5),
	assert_slave_wait_until_height(5),
	B5 = ar_node:get_current_block(),
	?assertEqual(20, get_balance(master, MinerPub)),
	?assertEqual([{MinerAddr, 1, ExpectedReward4, 1}, {MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1},
			{B0#block.reward_addr, 1, 10, 1}], B5#block.reward_history),
	?assertEqual(ar_block:reward_history_hash([{MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}]), B5#block.reward_history_hash),
	?assertEqual(1, B5#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B5#block.kryder_plus_rate_multiplier),
	%% The price per GiB minute recalculation only happens every two blocks.
	?assertEqual(B5#block.scheduled_price_per_gib_minute, B5#block.price_per_gib_minute),
	?assertEqual(B4#block.price_per_gib_minute, B5#block.price_per_gib_minute),
	?assertEqual(20000000000000, get_balance(master, Pub1)),
	?assert(lists:member(get_optimistic_tx_price(master, 1024), [Fee * 2, Fee * 2 + 1])),
	HalfKryderLatchReset = ?RESET_KRYDER_PLUS_LATCH_THRESHOLD div 2,
	TX1 = sign_tx(master, Key1, #{ denomination => 0, reward => HalfKryderLatchReset }),
	assert_post_tx_to_master(TX1),
	ar_node:mine(),
	_BI6 = wait_until_height(6),
	assert_slave_wait_until_height(6),
	B6 = ar_node:get_current_block(),
	{MinerShareDividend, MinerShareDivisor} = ?MINER_FEE_SHARE,
	?assertEqual(30, get_balance(master, MinerPub)),
	?assertEqual(10 + % inflation
			HalfKryderLatchReset,
			B6#block.reward
			+ B6#block.reward_pool - B5#block.reward_pool
			- (B6#block.debt_supply - B5#block.debt_supply)),
	?assertEqual(20000000000000 - HalfKryderLatchReset, get_balance(master, Pub1)),
	?assertEqual(1, B6#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B6#block.kryder_plus_rate_multiplier),
	?assertEqual(B6#block.price_per_gib_minute, B5#block.price_per_gib_minute),
	?assertEqual(
		min(ar_pricing:get_price_per_gib_minute(lists:sublist(B5#block.reward_history, 3), 1),
				B6#block.price_per_gib_minute * 2),
			B6#block.scheduled_price_per_gib_minute),
	assert_new_account_fee(),
	?assertEqual(1, B6#block.denomination),
	?assertEqual(?TOTAL_SUPPLY + B6#block.debt_supply - B6#block.reward_pool,
			prometheus_gauge:value(available_supply)),
	?assertEqual([{MinerAddr, 1, B6#block.reward, 1}, {MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, ExpectedReward4, 1}], lists:sublist(B6#block.reward_history, 3)),
	TX2 = sign_tx(master, Key1, #{ denomination => 0, reward => HalfKryderLatchReset * 2 }),
	assert_post_tx_to_master(TX2),
	ar_node:mine(),
	_BI7 = wait_until_height(7),
	assert_slave_wait_until_height(7),
	B7 = ar_node:get_current_block(),
	?assertEqual(10 + % inflation
			HalfKryderLatchReset * 2,
			B7#block.reward
			+ B7#block.reward_pool - B6#block.reward_pool
			- (B7#block.debt_supply - B6#block.debt_supply)),
	?assertEqual(30 + ExpectedReward4, get_balance(master, MinerPub)),
	?assertEqual(20000000000000 - HalfKryderLatchReset * 3, get_balance(master, Pub1)),
	?assert(B7#block.reward_pool > ?RESET_KRYDER_PLUS_LATCH_THRESHOLD),
	?assertEqual(1, B7#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B7#block.kryder_plus_rate_multiplier),
	?assertEqual(B6#block.price_per_gib_minute, B7#block.price_per_gib_minute),
	?assert(get_optimistic_tx_price(master, 1024) > Fee),
	ar_node:mine(),
	_BI8 = wait_until_height(8),
	assert_slave_wait_until_height(8),
	B8 = ar_node:get_current_block(),
	?assertEqual(30 + ExpectedReward4 + ExpectedReward4, get_balance(master, MinerPub)),
	%% Release because at the previous block the endowment pool exceeded the threshold.
	?assert(B8#block.reward_pool < B7#block.reward_pool),
	?assertEqual(0, B8#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B8#block.kryder_plus_rate_multiplier),
	?assertEqual(1, B8#block.denomination),
	?assert(prometheus_gauge:value(available_supply) > ?REDENOMINATION_THRESHOLD),
	?assert(B8#block.price_per_gib_minute > B7#block.price_per_gib_minute),
	%% A transaction with explicitly set denomination.
	TX3 = sign_tx(master, Key3, #{ denomination => 1 }),
	{Reward3, _} = get_tx_price(master, 0, ar_wallet:to_address(Pub2)),
	{Reward4, _} = get_tx_price(master, 0),
	assert_post_tx_to_master(TX3),
	TX4 = sign_tx(master, Key3, #{ denomination => 2 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX4)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX4#tx.id)),
	TX5 = sign_tx(master, Key3, #{ denomination => 0, target => ar_wallet:to_address(Pub2),
			quantity => 10 }),
	assert_post_tx_to_master(TX5),
	ar_node:mine(),
	_BI9 = wait_until_height(9),
	assert_slave_wait_until_height(9),
	B9 = ar_node:get_current_block(),
	?assertEqual(0, B9#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B9#block.kryder_plus_rate_multiplier),
	?assertEqual(1, B9#block.denomination),
	?assertEqual(2, length(B9#block.txs)),
	?assertEqual(0, B9#block.redenomination_height),
	?assert(prometheus_gauge:value(available_supply) < ?REDENOMINATION_THRESHOLD),
	?assertEqual(?AR(1000000000000000000) - Reward3 - Reward4 - 10, get_balance(master, Pub3)),
	?assertEqual(2000000000 + 10, get_balance(master, Pub2)),
	ar_node:mine(),
	_BI10 = wait_until_height(10),
	assert_slave_wait_until_height(10),
	B10 = ar_node:get_current_block(),
	?assertEqual(9 + ?REDENOMINATION_DELAY_BLOCKS, B10#block.redenomination_height),
	?assertEqual(1, B10#block.denomination),
	TX6 = sign_tx(master, Key3, #{ denomination => 0 }),
	%% Transactions without explicit denomination are not accepted now until
	%% the redenomination height.
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX6)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX6#tx.id)),
	%% The redenomination did not start yet.
	TX7 = sign_tx(master, Key3, #{ denomination => 2 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX7)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX7#tx.id)),
	{_, Pub4} = ar_wallet:new(),
	TX8 = sign_tx(master, Key3, #{ denomination => 1, target => ar_wallet:to_address(Pub4),
			quantity => 3 }),
	assert_post_tx_to_master(TX8),
	ar_node:mine(),
	_BI11 = wait_until_height(11),
	assert_slave_wait_until_height(11),
	B11 = ar_node:get_current_block(),
	?assertEqual(1, length(B11#block.txs)),
	?assertEqual(1, B11#block.denomination),
	?assertEqual(3, get_balance(master, Pub4)),
	Balance11 = get_balance(master, Pub3),
	{_, Pub5} = ar_wallet:new(),
	TX9 = sign_v1_tx(master, Key3, #{ denomination => 0, target => ar_wallet:to_address(Pub5),
			quantity => 100 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX9)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX9#tx.id)),
	%% The redenomination did not start just yet.
	TX10 = sign_v1_tx(master, Key3, #{ denomination => 2 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX10)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX10#tx.id)),
	{Reward11, _} = get_tx_price(master, 0, ar_wallet:to_address(Pub5)),
	?assert(hash_rate(B11) > 1),
	?assertEqual(lists:sublist([{MinerAddr, hash_rate(B11), B11#block.reward, 1}
			| B10#block.reward_history],
			?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT),
			B11#block.reward_history),
	TX11 = sign_tx(master, Key3, #{ denomination => 1, target => ar_wallet:to_address(Pub5),
			quantity => 100 }),
	assert_post_tx_to_master(TX11),
	ar_node:mine(),
	_BI12 = wait_until_height(12),
	assert_slave_wait_until_height(12),
	?assertEqual(100 * 1000, get_balance(master, Pub5)),
	?assertEqual(3 * 1000, get_balance(master, Pub4)),
	?assertEqual((Balance11 - Reward11 - 100) * 1000, get_balance(master, Pub3)),
	B12 = ar_node:get_current_block(),
	?assertEqual(1, length(B12#block.txs)),
	?assertEqual(2, B12#block.denomination),
	?assertEqual(10 * 1000 + % inflation
			Reward11 * 1000, % fees
			B12#block.reward
			+ B12#block.reward_pool - B11#block.reward_pool * 1000
			- (B12#block.debt_supply - B11#block.debt_supply * 1000)),
	?assertEqual(B11#block.debt_supply * 1000, B12#block.debt_supply),
	%% Setting the price scheduled on height=10.
	?assertEqual(B11#block.scheduled_price_per_gib_minute * 1000,
			B12#block.price_per_gib_minute),
	?assertEqual([{MinerAddr, hash_rate(B12), B12#block.reward, 2} | B11#block.reward_history],
			B12#block.reward_history),
	TX12 = sign_tx(master, Key3, #{ denomination => 0, quantity => 10,
			target => ar_wallet:to_address(Pub5) }),
	assert_post_tx_to_master(TX12),
	TX13 = sign_v1_tx(master, Key3, #{ denomination => 0,
			reward => get_optimistic_tx_price(master, 0),
			target => ar_wallet:to_address(Pub4), quantity => 4 }),
	assert_post_tx_to_master(TX13),
	Reward14 = get_optimistic_tx_price(master, 0),
	TX14 = sign_v1_tx(master, Key3, #{ denomination => 2, reward => Reward14,
			quantity => 5, target => ar_wallet:to_address(Pub4) }),
	assert_post_tx_to_master(TX14),
	TX15 = sign_v1_tx(master, Key3, #{ denomination => 2,
			reward => erlang:ceil(Reward14 / 1000) }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX15)),
	?assertEqual({ok, ["tx_too_cheap"]}, ar_tx_db:get_error_codes(TX15#tx.id)),
	TX16 = sign_v1_tx(master, Key3, #{ denomination => 3 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, post_tx_to_master(TX16)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX16#tx.id)),
	{_, Pub6} = ar_wallet:new(),
	%% Divide the reward by 1000 and specify the previous denomination.
	Reward17 = get_optimistic_tx_price(master, 0, ar_wallet:to_address(Pub6)),
	TX17 = sign_tx(master, Key3, #{ denomination => 1,
			reward => erlang:ceil(Reward17 / 1000), target => ar_wallet:to_address(Pub6),
			quantity => 7 }),
	assert_post_tx_to_master(TX17),
	ar_node:mine(),
	_BI13 = wait_until_height(13),
	assert_slave_wait_until_height(13),
	B13 = ar_node:get_current_block(),
	Reward17_2 = erlang:ceil(Reward17 / 1000) * 1000,
	BaseReward13 =
		B12#block.price_per_gib_minute
		* ?N_REPLICATIONS(B13#block.height)
		* 2 % minutes
		* B13#block.weave_size div (1024 * 1024 * 1024), % weave_size / GiB
	FeeSum13 =
			Reward14 * MinerShareDividend div MinerShareDivisor % TX12
			+ Reward14 * MinerShareDividend div MinerShareDivisor % TX13
			+ Reward14 * MinerShareDividend div MinerShareDivisor % TX14
			+ Reward17_2 * MinerShareDividend div MinerShareDivisor, % TX17
	?assertEqual(B12#block.reward_pool + Reward14 * 3 + Reward17_2 - FeeSum13
			- max(0, (BaseReward13 - (10 * 1000 + FeeSum13))), B13#block.reward_pool),
	?assertEqual(4, length(B13#block.txs)),
	?assertEqual(2, B13#block.denomination),
	?assertEqual(100 * 1000 + 10, get_balance(master, Pub5)),
	?assertEqual(7 * 1000, get_balance(master, Pub6)),
	?assertEqual(3 * 1000 + 4 + 5, get_balance(master, Pub4)),
	assert_new_account_fee(),
	ar_node:mine(),
	_BI14 = wait_until_height(14),
	assert_slave_wait_until_height(14),
	B14 = ar_node:get_current_block(),
	?assertEqual(B13#block.price_per_gib_minute, B14#block.price_per_gib_minute),
	?assertEqual(
		min(
			ar_pricing:get_price_per_gib_minute(lists:sublist(B13#block.reward_history, 3), 2),
			B14#block.price_per_gib_minute * 2
		), B14#block.scheduled_price_per_gib_minute).

hash_rate(#block{ diff = Diff }) ->
	ar_difficulty:get_hash_rate(Diff).

assert_new_account_fee() ->
	?assert(get_optimistic_tx_price(master,
			262144 + ?NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT) >
			get_optimistic_tx_price(master, 0, <<"non-existent-address">>)),
	?assert(get_optimistic_tx_price(master,
			?NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT - 262144) <
			get_optimistic_tx_price(master, 0, <<"non-existent-address">>)).

updates_pool_and_assigns_rewards_correctly_before_burden_test_() ->
	test_with_mocked_functions(
		[{ar_fork, height_2_6, fun() -> infinity end}],
		fun updates_pool_and_assigns_rewards_correctly_before_burden/0
	).

updates_pool_and_assigns_rewards_correctly_after_burden_test_() ->
	%% Bigger burden is achieved by mocking `ar_pricing:get_miner_reward_and_endowment_pool/1`
	%% so that it thinks the weave size is very big. Otherwise, we cannot start a big
	%% weave without actually storing a significant share of the data - test nodes won't be
	%% able to mine blocks.
	test_with_mocked_functions(
		[
			{ar_pricing, get_miner_reward_and_endowment_pool,
				fun ar_pricing_tests:get_miner_reward_and_endowment_pool/1},
			{ar_inflation, calculate, fun(_Height) -> 1 end},
			{ar_fork, height_2_6, fun() -> infinity end}
		],
		fun updates_pool_and_assigns_rewards_correctly_after_burden/0
	).

unclaimed_rewards_go_to_endowment_pool_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> infinity end}],
		fun test_unclaimed_rewards_go_to_endowment_pool/0).

get_miner_reward_and_endowment_pool(Args) ->
	{Pool, TXs, Addr, _WeaveSize, Height, Timestamp, Rate} = Args,
	meck:passthrough([{Pool, TXs, Addr, ?HUGE_WEAVE_SIZE, Height, Timestamp, Rate}]).

updates_pool_and_assigns_rewards_correctly_before_burden() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Key2 = {_, Pub2} = ar_wallet:new(),
	Key3 = {_, Pub3} = ar_wallet:new(),
	RewardKey = {_, RewardAddr} = ar_wallet:new_keyfile(),
	WalletName = ar_util:encode(ar_wallet:to_address(RewardKey)),
	Path = ar_wallet:wallet_filepath(WalletName),
	SlavePath = slave_call(ar_wallet, wallet_filepath, [WalletName]),
	%% Copy the key because we mine blocks on both nodes using the same key in this test.
	{ok, _} = file:copy(Path, SlavePath),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(2000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
		{ar_wallet:to_address(Pub3), ?AR(2000), <<>>},
		{ar_wallet:to_address(RewardAddr), ?AR(1), <<>>}
	]),
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
	?assertEqual(?AR(1) + ar_inflation:calculate(1), Balance),
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
	Size3 = ar_tx:get_weave_size_increase(byte_size(Data), B3#block.height),
	TXFee2 = ar_pricing:get_tx_fee(Size3, B2#block.timestamp, B1#block.usd_to_ar_rate, 2),
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
				Size = ar_tx:get_weave_size_increase(byte_size(Chunk), B4#block.height),
				Sum + ar_pricing:get_tx_fee(Size, B4#block.timestamp, Rate, 4)
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
	Size5 = ar_tx:get_weave_size_increase(byte_size(RewardWalletTX#tx.data), B5#block.height),
	TXFee4 = ar_pricing:get_tx_fee(Size5, B5#block.timestamp, B4#block.usd_to_ar_rate, 5),
	?assertEqual(B4#block.weave_size + 262144, B5#block.weave_size),
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
	RewardKey = {_, RewardAddr} = slave_call(ar_wallet, new_keyfile, []),
	[BNoPool] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(2000), <<>>},
		{ar_wallet:to_address(RewardAddr), ?AR(1), <<>>}
	]),
	B0 = BNoPool#block{ reward_pool = ?AR(10000000000000) },
	{_Master, _} = start(B0),
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
	Size1 = ar_tx:get_weave_size_increase(byte_size(BigChunk), B1#block.height),
	TXFee = ar_pricing:get_tx_fee(Size1, B1#block.timestamp, B0#block.usd_to_ar_rate, 1),
	Balance2 = get_balance(RewardAddr),
	MinerReward = Balance2 - Balance,
	?assertEqual(TXFee + trunc(ar_inflation:calculate(1)), MinerReward + RewardPoolIncrement),
	{MultiplierDividend, MultiplierDivisor} = ?MINING_REWARD_MULTIPLIER,
	?assert(
		MinerReward >=
			trunc(ar_inflation:calculate(1))
			+ MultiplierDividend * RewardPoolIncrement div MultiplierDivisor
	),
	?assertEqual(Size1, B1#block.block_size),
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
	Size3 = ar_tx:get_weave_size_increase(RewardWalletTX#tx.data_size, B3#block.height),
	TXFee2 = ar_pricing:get_tx_fee(Size3, B3#block.timestamp, B2#block.usd_to_ar_rate, 3),
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
	ar_node_worker:set_reward_addr(unclaimed),
	%% Mine a block without transactions. Expect no endowment pool increase.
	ar_node:mine(),
	BI1 = wait_until_height(1),
	B1 = read_block_when_stored(hd(BI1)),
	?assertEqual(0, B1#block.reward_pool),
	%% Mine a block with an empty transaction. Expect the endowment pool
	%% to receive the data reward for the base tx size.
	TX1 = sign_tx(master, Key, #{}),
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
	TX2 = sign_tx(master, Key, #{ data => Data, last_tx => get_tx_anchor(master) }),
	TX3 = sign_tx(master, Key, #{ data => Data2, last_tx => get_tx_anchor(master) }),
	TX4 = sign_tx(master, Key, #{ data => Data3, last_tx => get_tx_anchor(master) }),
	TX5 = sign_tx(master, Key, #{ data => Data4, last_tx => get_tx_anchor(master) }),
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
