-module(ar_pricing_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0,
		sign_tx/3, sign_v1_tx/3,
		get_tx_price/3, get_tx_price/2, get_optimistic_tx_price/2, get_optimistic_tx_price/3,
		assert_post_tx_to_master/1, post_tx_to_master/1,
		get_balance/2, get_reserved_balance/2, wait_until_height/1,
		assert_slave_wait_until_height/1]).

-define(DISTANT_FUTURE_BLOCK_HEIGHT, 262800000). %% 1,000 years from genesis

auto_redenomination_and_endowment_debt_test_() ->
	{timeout, 180, fun test_auto_redenomination_and_endowment_debt/0}.

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
	?assert(?PRICE_2_6_8_TRANSITION_START == 2),
	?assert(?PRICE_2_6_8_TRANSITION_BLOCKS == 2),
	?assert(?PRICE_ADJUSTMENT_FREQUENCY == 2),
	?assert(?REDENOMINATION_DELAY_BLOCKS == 2),
	?assert(?REWARD_HISTORY_BLOCKS == 3),
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assertEqual(262144 * 3, B0#block.weave_size),
	{ok, Config} = application:get_env(arweave, config),
	{_, MinerPub} = ar_wallet:load_key(Config#config.mining_addr),
	?assertEqual(0, get_balance(master, MinerPub)),
	?assertEqual(0, get_reserved_balance(master, Config#config.mining_addr)),
	ar_test_node:mine(),
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
	?assertEqual([{time_diff(B1, B0), vdf_diff(B1, B0), chunk_count(B1)}, {1, 1, 1}],
			B1#block.block_time_history),
	%% The price is recomputed every two blocks.
	?assertEqual(B1#block.price_per_gib_minute, B1#block.scheduled_price_per_gib_minute),
	ar_test_node:mine(),
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
	?assertEqual([{time_diff(B2, B1), vdf_diff(B2, B1), chunk_count(B2)},
			{time_diff(B1, B0), vdf_diff(B1, B0), chunk_count(B1)}, {1, 1, 1}],
			B2#block.block_time_history),
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
	ar_test_node:mine(),
	_BI4 = wait_until_height(4),
	assert_slave_wait_until_height(4),
	B4 = ar_node:get_current_block(),
	%% We are at the height ?PRICE_2_6_8_TRANSITION_START + ?PRICE_2_6_8_TRANSITION_BLOCKS
	%% so the new algorithm kicks in which estimates the expected block reward and takes
	%% the missing amount from the endowment pool or takes on debt.
	AvgBlockTime4 = lists:sum([element(1, El)
			|| El <- lists:sublist(B3#block.block_time_history, 3)]) div 3,
	ExpectedReward4 = max(ar_inflation:calculate(4), B3#block.price_per_gib_minute
			* ?N_REPLICATIONS(B4#block.height)
			* AvgBlockTime4 div 60
			* 3 div (4 * 1024)), % weave_size / GiB
	?assertEqual(ExpectedReward4, B4#block.reward),
	?assertEqual(ExpectedReward4 + 20, get_reserved_balance(master, B4#block.reward_addr)),
	?assertEqual(10, get_balance(master, MinerPub)),
	?assertEqual(ExpectedReward4 - 10, B4#block.debt_supply),
	?assertEqual(1, B4#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B4#block.kryder_plus_rate_multiplier),
	?assertEqual(B4#block.price_per_gib_minute, B3#block.scheduled_price_per_gib_minute),
	PricePerGiBMinute3 = B3#block.price_per_gib_minute,
	?assertEqual(max(PricePerGiBMinute3 div 2, min(PricePerGiBMinute3 * 2,
			ar_pricing:get_price_per_gib_minute(B3#block.height,
				lists:sublist(B3#block.reward_history, 3),
				lists:sublist(B3#block.block_time_history, 3), 1))),
			B4#block.scheduled_price_per_gib_minute),
	%% The Kryder+ rate multiplier is 2 now so the fees should have doubled.
	?assert(lists:member(get_optimistic_tx_price(master, 1024), [Fee * 2, Fee * 2 + 1])),
	?assertEqual([{MinerAddr, 1, ExpectedReward4, 1}, {MinerAddr, 1, 10, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {B0#block.reward_addr, 1, 10, 1}],
			B4#block.reward_history),
	?assertEqual(ar_block:reward_history_hash([{MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}]), B4#block.reward_history_hash),
	ar_test_node:mine(),
	_BI5 = wait_until_height(5),
	assert_slave_wait_until_height(5),
	B5 = ar_node:get_current_block(),
	?assertEqual(20, get_balance(master, MinerPub)),
	AvgBlockTime5 = lists:sum([element(1, El)
			|| El <- lists:sublist(B4#block.block_time_history, 3)]) div 3,
	ExpectedReward5 = max(B4#block.price_per_gib_minute
			* ?N_REPLICATIONS(B5#block.height)
			* AvgBlockTime5 div 60
			* 3 div (4 * 1024), % weave_size / GiB
			ar_inflation:calculate(5)),
	?assertEqual(ExpectedReward5, B5#block.reward),
	?assertEqual([{MinerAddr, 1, ExpectedReward5, 1}, {MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1},
			{B0#block.reward_addr, 1, 10, 1}], B5#block.reward_history),
	?assertEqual(ar_block:reward_history_hash([{MinerAddr, 1, ExpectedReward5, 1},
			{MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}]), B5#block.reward_history_hash),
	?assertEqual(1, B5#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B5#block.kryder_plus_rate_multiplier),
	%% The price per GiB minute recalculation only happens every two blocks.
	?assertEqual(B5#block.scheduled_price_per_gib_minute,
			B4#block.scheduled_price_per_gib_minute),
	?assertEqual(B4#block.price_per_gib_minute, B5#block.price_per_gib_minute),
	?assertEqual(20000000000000, get_balance(master, Pub1)),
	?assert(lists:member(get_optimistic_tx_price(master, 1024), [Fee * 2, Fee * 2 + 1])),
	HalfKryderLatchReset = ?RESET_KRYDER_PLUS_LATCH_THRESHOLD div 2,
	TX1 = sign_tx(master, Key1, #{ denomination => 0, reward => HalfKryderLatchReset }),
	assert_post_tx_to_master(TX1),
	ar_test_node:mine(),
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
	?assertEqual(B6#block.price_per_gib_minute, B5#block.scheduled_price_per_gib_minute),
	PricePerGiBMinute5 = B5#block.price_per_gib_minute,
	?assertEqual(
		max(PricePerGiBMinute5 div 2,
			min(ar_pricing:get_price_per_gib_minute(B5#block.height,
					lists:sublist(B5#block.reward_history, 3),
					lists:sublist(B5#block.block_time_history, 3), 1),
				PricePerGiBMinute5 * 2)),
			B6#block.scheduled_price_per_gib_minute),
	assert_new_account_fee(),
	?assertEqual(1, B6#block.denomination),
	?assertEqual(?TOTAL_SUPPLY + B6#block.debt_supply - B6#block.reward_pool,
			prometheus_gauge:value(available_supply)),
	?assertEqual([{MinerAddr, 1, B6#block.reward, 1}, {MinerAddr, 1, ExpectedReward5, 1},
			{MinerAddr, 1, ExpectedReward4, 1}], lists:sublist(B6#block.reward_history, 3)),
	TX2 = sign_tx(master, Key1, #{ denomination => 0, reward => HalfKryderLatchReset * 2 }),
	assert_post_tx_to_master(TX2),
	ar_test_node:mine(),
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
	ar_test_node:mine(),
	_BI8 = wait_until_height(8),
	assert_slave_wait_until_height(8),
	B8 = ar_node:get_current_block(),
	?assertEqual(30 + ExpectedReward5 + ExpectedReward4, get_balance(master, MinerPub)),
	%% Release because at the previous block the endowment pool exceeded the threshold.
	?assert(B8#block.reward_pool < B7#block.reward_pool),
	?assertEqual(0, B8#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B8#block.kryder_plus_rate_multiplier),
	?assertEqual(1, B8#block.denomination),
	?assert(prometheus_gauge:value(available_supply) > ?REDENOMINATION_THRESHOLD),
	?assert(B8#block.scheduled_price_per_gib_minute > B8#block.price_per_gib_minute),
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
	ar_test_node:mine(),
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
	ar_test_node:mine(),
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
	ar_test_node:mine(),
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
	ar_test_node:mine(),
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
	{Reward12, 2} = get_tx_price(master, 0),
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
	ar_test_node:mine(),
	_BI13 = wait_until_height(13),
	assert_slave_wait_until_height(13),
	B13 = ar_node:get_current_block(),
	Reward17_2 = erlang:ceil(Reward17 / 1000) * 1000,
	AvgBlockTime13 = lists:sum([element(1, El)
			|| El <- lists:sublist(B12#block.block_time_history, 3)]) div 3,
	BaseReward13 =
		B12#block.price_per_gib_minute
		* ?N_REPLICATIONS(B13#block.height)
		* AvgBlockTime13 div 60 % minutes
		* B13#block.weave_size div (1024 * 1024 * 1024), % weave_size / GiB
	FeeSum13 =
			Reward12 * MinerShareDividend div MinerShareDivisor % TX12
			+ Reward14 * MinerShareDividend div MinerShareDivisor % TX13
			+ Reward14 * MinerShareDividend div MinerShareDivisor % TX14
			+ Reward17_2 * MinerShareDividend div MinerShareDivisor, % TX17
	?debugFmt("B12#block.reward_pool: ~B, fees: ~B, fees received by the reward pool:~B, "
			"expected reward: ~B, miner fee share: ~B~n", [B12#block.reward_pool,
				Reward12 + Reward14 * 2 + Reward17_2,
				Reward12 + Reward14 * 2 + Reward17_2 - FeeSum13,
				BaseReward13, FeeSum13]),
	?assertEqual(B12#block.reward_pool + Reward12 + Reward14 * 2 + Reward17_2 - FeeSum13
			- max(0, (BaseReward13 - (10 * 1000 + FeeSum13))), B13#block.reward_pool),
	?assertEqual(4, length(B13#block.txs)),
	?assertEqual(2, B13#block.denomination),
	?assertEqual(100 * 1000 + 10, get_balance(master, Pub5)),
	?assertEqual(7 * 1000, get_balance(master, Pub6)),
	?assertEqual(3 * 1000 + 4 + 5, get_balance(master, Pub4)),
	assert_new_account_fee(),
	ar_test_node:mine(),
	_BI14 = wait_until_height(14),
	assert_slave_wait_until_height(14),
	B14 = ar_node:get_current_block(),
	PricePerGiBMinute13 = B13#block.price_per_gib_minute,
	?assertEqual(
		max(PricePerGiBMinute13 div 2, min(
			ar_pricing:get_price_per_gib_minute(B13#block.height,
					lists:sublist(B13#block.reward_history, 3),
					lists:sublist(B13#block.block_time_history, 3), 2),
			PricePerGiBMinute13 * 2
		)), B14#block.scheduled_price_per_gib_minute).

time_diff(#block{ timestamp = TS }, #block{ timestamp = PrevTS }) ->
	TS - PrevTS.

vdf_diff(#block{ nonce_limiter_info = Info }, #block{ nonce_limiter_info = PrevInfo }) ->
	Info#nonce_limiter_info.global_step_number
			- PrevInfo#nonce_limiter_info.global_step_number.

chunk_count(#block{ recall_byte2 = undefined }) ->
	1;
chunk_count(_) ->
	2.

hash_rate(#block{ diff = Diff }) ->
	ar_difficulty:get_hash_rate(Diff).

assert_new_account_fee() ->
	?assert(get_optimistic_tx_price(master,
			262144 + ?NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT) >
			get_optimistic_tx_price(master, 0, <<"non-existent-address">>)),
	?assert(get_optimistic_tx_price(master,
			?NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT - 262144) <
			get_optimistic_tx_price(master, 0, <<"non-existent-address">>)).
