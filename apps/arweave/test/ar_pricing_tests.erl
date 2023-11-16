-module(ar_pricing_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DISTANT_FUTURE_BLOCK_HEIGHT, 262800000). %% 1,000 years from genesis

get_price_per_gib_minute_test_() ->
	[
		ar_test_node:test_with_mocked_functions(
			[{ar_fork, height_2_7, fun() -> 5 end}],
			fun test_price_per_gib_minute_pre_fork/0),
		{timeout, 30, fun test_price_per_gib_minute_pre_block_time_history/0},
		%% Need to set the 2.7 fork height to -1 since BLOCK_TIME_HISTORY_BLOCKS is 3 and
		%% PRICE_2_6_8_TRANSITION_START is 2. Without this workaround we can't start the
		%% price transition until it's half over.
		ar_test_node:test_with_mocked_functions(
			[{ar_fork, height_2_7, fun() -> -1 end}],
			fun test_price_per_gib_minute_in_transition/0),
		ar_test_node:test_with_mocked_functions(
			[{ar_fork, height_2_7, fun() -> -1 end}],
			fun test_price_per_gib_minute_pre_transition/0),
		{timeout, 30, fun test_price_per_gib_minute_post_transition/0}
	].

test_price_per_gib_minute_pre_fork() ->
	?assertEqual(122880,
		ar_pricing:get_price_per_gib_minute(0, reward_history(1, 1), vdf(1, 1), 0),
		"Before 2.7").

test_price_per_gib_minute_pre_block_time_history() ->
	%% This test verifies an edge case code path that probably shouldn't ver be triggered.
	%% ar_fork:height_2_7() and ar_fork:height_2_6_8() ar 0
	%% ?BLOCK_TIME_HISTORY_BLOCKS is 3
	%% ?PRICE_2_6_8_TRANSITION_START is 2
	%% So when the price transition starts we don't have enough block time history to apply the
	%% new algorithm.
	Start = ar_fork:height_2_6_8() + ?PRICE_2_6_8_TRANSITION_START,
	?assertEqual(?PRICE_PER_GIB_MINUTE_PRE_TRANSITION,
		ar_pricing:get_price_per_gib_minute(Start, reward_history(1, 1), vdf(1, 1), 0),
		"Before we have enough block time history").

test_price_per_gib_minute_pre_transition() ->
	Start = ar_fork:height_2_6_8() + ?PRICE_2_6_8_TRANSITION_START,
	?assertEqual(?PRICE_PER_GIB_MINUTE_PRE_TRANSITION,
		ar_pricing:get_price_per_gib_minute(Start-1, reward_history(1, 1), vdf(1, 1), 0),
		"Before transition").

test_price_per_gib_minute_in_transition() ->
	Start = ar_fork:height_2_6_8() + ?PRICE_2_6_8_TRANSITION_START,
	?assertEqual(?PRICE_PER_GIB_MINUTE_PRE_TRANSITION,
		ar_pricing:get_price_per_gib_minute(Start, reward_history(1, 1), vdf(1, 1), 0),
		"Transition start"),
	?assertEqual(34801,
		ar_pricing:get_price_per_gib_minute(Start + 1, reward_history(1, 1), vdf(1, 1), 0),
		"In transition"),
	?assertEqual(61440,
		ar_pricing:get_price_per_gib_minute(Start + ?PRICE_2_6_8_TRANSITION_BLOCKS,
			reward_history(1, 1), vdf(1, 1), 0),
		"Transition end").

test_price_per_gib_minute_post_transition() ->
	AtTransitionEnd = ar_fork:height_2_6_8() +
		?PRICE_2_6_8_TRANSITION_START +
		?PRICE_2_6_8_TRANSITION_BLOCKS,
	do_price_per_gib_minute_post_transition(AtTransitionEnd),
	BeyondTransition = AtTransitionEnd + 1000,
	do_price_per_gib_minute_post_transition(BeyondTransition).

do_price_per_gib_minute_post_transition(Height) ->
	Baseline = 61440,
	Baseline1_5 = 92160, %% 1.5x Baseline
	Baseline2 = 122880, %% 2x Baseline
	Baseline10 = 614400, %% 10x Baseline
	?assertEqual(Baseline,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(1, 1), vdf(1, 1), 0),
		"hash_rate: low, reward: low, vdf: perfect, chunks: all_one"),
	?assertEqual(Baseline10,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(1, 10), vdf(1, 1), 0),
		"hash_rate: low, reward: high, vdf: perfect, chunks: all_one"),
	?assertEqual(Baseline div 10,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(10, 1), vdf(1, 1), 0),
		"hash_rate: high, reward: low, vdf: perfect, chunks: all_one"),
	?assertEqual(Baseline,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(10, 10), vdf(1, 1), 0),
		"hash_rate: high, reward: high, vdf: perfect, chunks: all_one"),
	?assertEqual(Baseline div 10,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(1, 1), vdf(10, 1), 0),
		"hash_rate: low, reward: low, vdf: slow, chunks: all_one"),
	?assertEqual(Baseline10,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(1, 1), vdf(1, 10), 0),
		"hash_rate: low, reward: low, vdf: fast, chunks: all_one"),
	?assertEqual(Baseline2,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(1, 1), all_two_chunks(), 0),
		"hash_rate: low, reward: low, vdf: perfect, chunks: all_two"),
	?assertEqual(Baseline1_5,
		ar_pricing:get_price_per_gib_minute(Height, reward_history(1, 1), mix_chunks(), 0),
		"hash_rate: low, reward: low, vdf: perfect, chunks: mix").

reward_history(HashRate, Reward) ->
	[
		{crypto:strong_rand_bytes(32), 1*HashRate, 1*Reward, 0},
		{crypto:strong_rand_bytes(32), 2*HashRate, 2*Reward, 0},
		{crypto:strong_rand_bytes(32), 3*HashRate, 3*Reward, 0}
	].

vdf(BlockInterval, VDFSteps) ->
	[
		{BlockInterval*10, VDFSteps*10, 1},
		{BlockInterval*20, VDFSteps*20, 1},
		{BlockInterval*30, VDFSteps*30, 1}
	].

all_two_chunks() ->
	[
		{10, 10, 2},
		{20, 20, 2},
		{30, 30, 2}
	].

mix_chunks() ->
	[
		{10, 10, 1},
		{20, 20, 2},
		{30, 30, 1}
	].

recalculate_price_per_gib_minute_test_block() ->
	#block{
		height = ?PRICE_ADJUSTMENT_FREQUENCY-1,
		denomination = 1,
		reward_history = [
			{<<>>, 10000, 10, 1}
		],
		block_time_history = [
			{129, 135, 1}
		],
		price_per_gib_minute = 10000,
		scheduled_price_per_gib_minute = 15000
	}.

recalculate_price_per_gib_minute_2_7_test_() ->
	ar_test_node:test_with_mocked_functions(
		[{ar_fork, height_2_6, fun() -> -1 end},
		{ar_fork, height_2_7, fun() -> -1 end},
		{ar_fork, height_2_8, fun() -> infinity end}],
		fun() ->
			B = recalculate_price_per_gib_minute_test_block(),
			?assertEqual({15000, 8162}, ar_pricing:recalculate_price_per_gib_minute(B)),
			ok
		end).

recalculate_price_per_gib_minute_2_8_ema_test_() ->
	ar_test_node:test_with_mocked_functions(
		[{ar_fork, height_2_6, fun() -> -1 end},
		{ar_fork, height_2_7, fun() -> -1 end},
		{ar_fork, height_2_8, fun() -> -1 end}],
		fun() ->
			B = recalculate_price_per_gib_minute_test_block(),
			?assertEqual({15000, 14316}, ar_pricing:recalculate_price_per_gib_minute(B)),
			ok
		end).

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
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	?assert(?PRICE_2_6_8_TRANSITION_START == 2),
	?assert(?PRICE_2_6_8_TRANSITION_BLOCKS == 2),
	?assert(?PRICE_ADJUSTMENT_FREQUENCY == 2),
	?assert(?REDENOMINATION_DELAY_BLOCKS == 2),
	?assert(?REWARD_HISTORY_BLOCKS == 3),
	?assert(?DOUBLE_SIGNING_REWARD_SAMPLE_SIZE == 2),
	?assertEqual(262144 * 3, B0#block.weave_size),
	{ok, Config} = application:get_env(arweave, config),
	{_, MinerPub} = ar_wallet:load_key(Config#config.mining_addr),
	?assertEqual(0, get_balance(MinerPub)),
	?assertEqual(0, get_reserved_balance(Config#config.mining_addr)),
	ar_test_node:mine(),
	_BI1 = ar_test_node:wait_until_height(1),
	ar_test_node:assert_wait_until_height(peer1, 1),
	?assertEqual(0, get_balance(MinerPub)),
	B1 = ar_node:get_current_block(),
	?assertEqual(10, B1#block.reward),
	?assertEqual(10, get_reserved_balance(B1#block.reward_addr)),
	?assertEqual(1, hash_rate(B1)),
	MinerAddr = ar_wallet:to_address(MinerPub),
	?assertEqual([{MinerAddr, 1, 10, 1}, {B0#block.reward_addr, 1, 10, 1}],
			B1#block.reward_history),
	?assertEqual([{time_diff(B1, B0), vdf_diff(B1, B0), chunk_count(B1)}, {1, 1, 1}],
			B1#block.block_time_history),
	%% The price is recomputed every two blocks.
	?assertEqual(B1#block.price_per_gib_minute, B1#block.scheduled_price_per_gib_minute),
	ar_test_node:mine(),
	_BI2 = ar_test_node:wait_until_height(2),
	ar_test_node:assert_wait_until_height(peer1, 2),
	?assertEqual(0, get_balance(MinerPub)),
	B2 = ar_node:get_current_block(),
	?assertEqual(10, B2#block.reward),
	?assertEqual(20, get_reserved_balance(B2#block.reward_addr)),
	?assertEqual(B1#block.price_per_gib_minute, B2#block.price_per_gib_minute),
	?assertEqual(B2#block.scheduled_price_per_gib_minute, B2#block.price_per_gib_minute),
	?assertEqual([{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1},
			{B0#block.reward_addr, 1, 10, 1}], B2#block.reward_history),
	?assertEqual([{time_diff(B2, B1), vdf_diff(B2, B1), chunk_count(B2)},
			{time_diff(B1, B0), vdf_diff(B1, B0), chunk_count(B1)}, {1, 1, 1}],
			B2#block.block_time_history),
	ar_test_node:mine(),
	_BI3 = ar_test_node:wait_until_height(3),
	ar_test_node:assert_wait_until_height(peer1, 3),
	?assertEqual(0, get_balance(MinerPub)),
	B3 = ar_node:get_current_block(),
	?assertEqual(10, B3#block.reward),
	?assertEqual(30, get_reserved_balance(B3#block.reward_addr)),
	?assertEqual(B3#block.price_per_gib_minute, B2#block.price_per_gib_minute),
	?assertEqual(B3#block.scheduled_price_per_gib_minute,
			B2#block.scheduled_price_per_gib_minute),
	?assertEqual(0, B3#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(1, B3#block.kryder_plus_rate_multiplier),
	?assertEqual([{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1},
			{B0#block.reward_addr, 1, 10, 1}], B3#block.reward_history),
	Fee = ar_test_node:get_optimistic_tx_price(main, 1024),
	ar_test_node:mine(),
	_BI4 = ar_test_node:wait_until_height(4),
	ar_test_node:assert_wait_until_height(peer1, 4),
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
	?assertEqual(ExpectedReward4 + 20, get_reserved_balance(B4#block.reward_addr)),
	?assertEqual(10, get_balance(MinerPub)),
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
	?assert(lists:member(ar_test_node:get_optimistic_tx_price(main, 1024), [Fee * 2, Fee * 2 + 1])),
	?assertEqual([{MinerAddr, 1, ExpectedReward4, 1}, {MinerAddr, 1, 10, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}, {B0#block.reward_addr, 1, 10, 1}],
			B4#block.reward_history),
	?assertEqual(ar_block:reward_history_hash([{MinerAddr, 1, ExpectedReward4, 1},
			{MinerAddr, 1, 10, 1}, {MinerAddr, 1, 10, 1}]), B4#block.reward_history_hash),
	ar_test_node:mine(),
	_BI5 = ar_test_node:wait_until_height(5),
	ar_test_node:assert_wait_until_height(peer1, 5),
	B5 = ar_node:get_current_block(),
	?assertEqual(20, get_balance(MinerPub)),
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
	?assertEqual(20000000000000, get_balance(Pub1)),
	?assert(lists:member(ar_test_node:get_optimistic_tx_price(main, 1024), [Fee * 2, Fee * 2 + 1])),
	HalfKryderLatchReset = ?RESET_KRYDER_PLUS_LATCH_THRESHOLD div 2,
	TX1 = ar_test_node:sign_tx(main, Key1, #{ denomination => 0, reward => HalfKryderLatchReset }),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	ar_test_node:mine(),
	_BI6 = ar_test_node:wait_until_height(6),
	ar_test_node:assert_wait_until_height(peer1, 6),
	B6 = ar_node:get_current_block(),
	{MinerShareDividend, MinerShareDivisor} = ?MINER_FEE_SHARE,
	?assertEqual(30, get_balance(MinerPub)),
	?assertEqual(10 + % inflation
			HalfKryderLatchReset,
			B6#block.reward
			+ B6#block.reward_pool - B5#block.reward_pool
			- (B6#block.debt_supply - B5#block.debt_supply)),
	?assertEqual(20000000000000 - HalfKryderLatchReset, get_balance(Pub1)),
	?assertEqual(1, B6#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B6#block.kryder_plus_rate_multiplier),
	?assertEqual(B6#block.price_per_gib_minute, B5#block.scheduled_price_per_gib_minute),
	ScheduledPricePerGiBMinute5 = B5#block.scheduled_price_per_gib_minute,
	?assertEqual(
		max(ScheduledPricePerGiBMinute5 div 2,
			min(ar_pricing:get_price_per_gib_minute(B5#block.height,
					lists:sublist(B5#block.reward_history, 3),
					lists:sublist(B5#block.block_time_history, 3), 1),
				ScheduledPricePerGiBMinute5 * 2)),
			B6#block.scheduled_price_per_gib_minute),
	assert_new_account_fee(),
	?assertEqual(1, B6#block.denomination),
	?assertEqual(?TOTAL_SUPPLY + B6#block.debt_supply - B6#block.reward_pool,
			prometheus_gauge:value(available_supply)),
	?assertEqual([{MinerAddr, 1, B6#block.reward, 1}, {MinerAddr, 1, ExpectedReward5, 1},
			{MinerAddr, 1, ExpectedReward4, 1}], lists:sublist(B6#block.reward_history, 3)),
	TX2 = ar_test_node:sign_tx(main, Key1, #{ denomination => 0, reward => HalfKryderLatchReset * 2 }),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:mine(),
	_BI7 = ar_test_node:wait_until_height(7),
	ar_test_node:assert_wait_until_height(peer1, 7),
	B7 = ar_node:get_current_block(),
	?assertEqual(10 + % inflation
			HalfKryderLatchReset * 2,
			B7#block.reward
			+ B7#block.reward_pool - B6#block.reward_pool
			- (B7#block.debt_supply - B6#block.debt_supply)),
	?assertEqual(30 + ExpectedReward4, get_balance(MinerPub)),
	?assertEqual(20000000000000 - HalfKryderLatchReset * 3, get_balance(Pub1)),
	?assert(B7#block.reward_pool > ?RESET_KRYDER_PLUS_LATCH_THRESHOLD),
	?assertEqual(1, B7#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B7#block.kryder_plus_rate_multiplier),
	?assertEqual(B6#block.price_per_gib_minute, B7#block.price_per_gib_minute),
	?assert(ar_test_node:get_optimistic_tx_price(main, 1024) > Fee),
	ar_test_node:mine(),
	_BI8 = ar_test_node:wait_until_height(8),
	ar_test_node:assert_wait_until_height(peer1, 8),
	B8 = ar_node:get_current_block(),
	?assertEqual(30 + ExpectedReward5 + ExpectedReward4, get_balance(MinerPub)),
	%% Release because at the previous block the endowment pool exceeded the threshold.
	?assert(B8#block.reward_pool < B7#block.reward_pool),
	?assertEqual(0, B8#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B8#block.kryder_plus_rate_multiplier),
	?assertEqual(1, B8#block.denomination),
	?assert(prometheus_gauge:value(available_supply) > ?REDENOMINATION_THRESHOLD),
	?assert(B8#block.scheduled_price_per_gib_minute > B8#block.price_per_gib_minute),
	%% A transaction with explicitly set denomination.
	TX3 = ar_test_node:sign_tx(main, Key3, #{ denomination => 1 }),
	{Reward3, _} = ar_test_node:get_tx_price(main, 0, ar_wallet:to_address(Pub2)),
	{Reward4, _} = ar_test_node:get_tx_price(main, 0),
	ar_test_node:assert_post_tx_to_peer(main, TX3),
	TX4 = ar_test_node:sign_tx(main, Key3, #{ denomination => 2 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX4)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX4#tx.id)),
	TX5 = ar_test_node:sign_tx(main, Key3, #{ denomination => 0, target => ar_wallet:to_address(Pub2),
			quantity => 10 }),
	ar_test_node:assert_post_tx_to_peer(main, TX5),
	ar_test_node:mine(),
	_BI9 = ar_test_node:wait_until_height(9),
	ar_test_node:assert_wait_until_height(peer1, 9),
	B9 = ar_node:get_current_block(),
	?assertEqual(0, B9#block.kryder_plus_rate_multiplier_latch),
	?assertEqual(2, B9#block.kryder_plus_rate_multiplier),
	?assertEqual(1, B9#block.denomination),
	?assertEqual(2, length(B9#block.txs)),
	?assertEqual(0, B9#block.redenomination_height),
	?assert(prometheus_gauge:value(available_supply) < ?REDENOMINATION_THRESHOLD),
	?assertEqual(?AR(1000000000000000000) - Reward3 - Reward4 - 10, get_balance(Pub3)),
	?assertEqual(2000000000 + 10, get_balance(Pub2)),
	ar_test_node:mine(),
	_BI10 = ar_test_node:wait_until_height(10),
	ar_test_node:assert_wait_until_height(peer1, 10),
	B10 = ar_node:get_current_block(),
	?assertEqual(9 + ?REDENOMINATION_DELAY_BLOCKS, B10#block.redenomination_height),
	?assertEqual(1, B10#block.denomination),
	TX6 = ar_test_node:sign_tx(main, Key3, #{ denomination => 0 }),
	%% Transactions without explicit denomination are not accepted now until
	%% the redenomination height.
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX6)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX6#tx.id)),
	%% The redenomination did not start yet.
	TX7 = ar_test_node:sign_tx(main, Key3, #{ denomination => 2 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX7)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX7#tx.id)),
	{_, Pub4} = ar_wallet:new(),
	TX8 = ar_test_node:sign_tx(main, Key3, #{ denomination => 1, target => ar_wallet:to_address(Pub4),
			quantity => 3 }),
	ar_test_node:assert_post_tx_to_peer(main, TX8),
	ar_test_node:mine(),
	_BI11 = ar_test_node:wait_until_height(11),
	ar_test_node:assert_wait_until_height(peer1, 11),
	B11 = ar_node:get_current_block(),
	?assertEqual(1, length(B11#block.txs)),
	?assertEqual(1, B11#block.denomination),
	?assertEqual(3, get_balance(Pub4)),
	Balance11 = get_balance(Pub3),
	{_, Pub5} = ar_wallet:new(),
	TX9 = ar_test_node:sign_v1_tx(main, Key3, #{ denomination => 0, target => ar_wallet:to_address(Pub5),
			quantity => 100 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX9)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX9#tx.id)),
	%% The redenomination did not start just yet.
	TX10 = ar_test_node:sign_v1_tx(main, Key3, #{ denomination => 2 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX10)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX10#tx.id)),
	{Reward11, _} = ar_test_node:get_tx_price(main, 0, ar_wallet:to_address(Pub5)),
	?assert(hash_rate(B11) > 1),
	?assertEqual(lists:sublist([{MinerAddr, hash_rate(B11), B11#block.reward, 1}
			| B10#block.reward_history],
			?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT),
			B11#block.reward_history),
	TX11 = ar_test_node:sign_tx(main, Key3, #{ denomination => 1, target => ar_wallet:to_address(Pub5),
			quantity => 100 }),
	ar_test_node:assert_post_tx_to_peer(main, TX11),
	ar_test_node:mine(),
	_BI12 = ar_test_node:wait_until_height(12),
	ar_test_node:assert_wait_until_height(peer1, 12),
	?assertEqual(100 * 1000, get_balance(Pub5)),
	?assertEqual(3 * 1000, get_balance(Pub4)),
	?assertEqual((Balance11 - Reward11 - 100) * 1000, get_balance(Pub3)),
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
	TX12 = ar_test_node:sign_tx(main, Key3, #{ denomination => 0, quantity => 10,
			target => ar_wallet:to_address(Pub5) }),
	{Reward12, 2} = ar_test_node:get_tx_price(main, 0),
	ar_test_node:assert_post_tx_to_peer(main, TX12),
	TX13 = ar_test_node:sign_v1_tx(main, Key3, #{ denomination => 0,
			reward => ar_test_node:get_optimistic_tx_price(main, 0),
			target => ar_wallet:to_address(Pub4), quantity => 4 }),
	ar_test_node:assert_post_tx_to_peer(main, TX13),
	Reward14 = ar_test_node:get_optimistic_tx_price(main, 0),
	TX14 = ar_test_node:sign_v1_tx(main, Key3, #{ denomination => 2, reward => Reward14,
			quantity => 5, target => ar_wallet:to_address(Pub4) }),
	ar_test_node:assert_post_tx_to_peer(main, TX14),
	TX15 = ar_test_node:sign_v1_tx(main, Key3, #{ denomination => 2,
			reward => erlang:ceil(Reward14 / 1000) }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX15)),
	?assertEqual({ok, ["tx_too_cheap"]}, ar_tx_db:get_error_codes(TX15#tx.id)),
	TX16 = ar_test_node:sign_v1_tx(main, Key3, #{ denomination => 3 }),
	?assertMatch({ok, {{<<"400">>, _}, _, _, _, _}}, ar_test_node:post_tx_to_peer(main, TX16)),
	?assertEqual({ok, ["invalid_denomination"]}, ar_tx_db:get_error_codes(TX16#tx.id)),
	{_, Pub6} = ar_wallet:new(),
	%% Divide the reward by 1000 and specify the previous denomination.
	Reward17 = ar_test_node:get_optimistic_tx_price(main, 0, ar_wallet:to_address(Pub6)),
	TX17 = ar_test_node:sign_tx(main, Key3, #{ denomination => 1,
			reward => erlang:ceil(Reward17 / 1000), target => ar_wallet:to_address(Pub6),
			quantity => 7 }),
	ar_test_node:assert_post_tx_to_peer(main, TX17),
	ar_test_node:mine(),
	_BI13 = ar_test_node:wait_until_height(13),
	ar_test_node:assert_wait_until_height(peer1, 13),
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
	?assertEqual(100 * 1000 + 10, get_balance(Pub5)),
	?assertEqual(7 * 1000, get_balance(Pub6)),
	?assertEqual(3 * 1000 + 4 + 5, get_balance(Pub4)),
	assert_new_account_fee(),
	ar_test_node:mine(),
	_BI14 = ar_test_node:wait_until_height(14),
	ar_test_node:assert_wait_until_height(peer1, 14),
	B14 = ar_node:get_current_block(),
	ScheduledPricePerGiBMinute13 = B13#block.scheduled_price_per_gib_minute,
	?assertEqual(
		max(ScheduledPricePerGiBMinute13 div 2, min(
			ar_pricing:get_price_per_gib_minute(B13#block.height,
					lists:sublist(B13#block.reward_history, 3),
					lists:sublist(B13#block.block_time_history, 3), 2),
			ScheduledPricePerGiBMinute13 * 2
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
	?assert(ar_test_node:get_optimistic_tx_price(main,
			262144 + ?NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT) >
			ar_test_node:get_optimistic_tx_price(main, 0, <<"non-existent-address">>)),
	?assert(ar_test_node:get_optimistic_tx_price(main,
			?NEW_ACCOUNT_FEE_DATA_SIZE_EQUIVALENT - 262144) <
			ar_test_node:get_optimistic_tx_price(main, 0, <<"non-existent-address">>)).

%% @doc Return the current balance of the given account.
get_balance(Pub) ->
	Address = ar_wallet:to_address(Pub),
	Peer = ar_test_node:peer_ip(main),
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(ar_util:encode(Address)) ++ "/balance"
		}),
	Balance = binary_to_integer(Reply),
	B = ar_node:get_current_block(),
	{ok, {{<<"200">>, _}, _, Reply2, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet_list/" ++ binary_to_list(ar_util:encode(B#block.wallet_list))
					++ "/" ++ binary_to_list(ar_util:encode(Address)) ++ "/balance"
		}),
	case binary_to_integer(Reply2) of
		Balance ->
			Balance;
		Balance2 ->
			?assert(false, io_lib:format("Expected: ~B, got: ~B.~n", [Balance, Balance2]))
	end.


get_reserved_balance(Address) ->
	Peer = ar_test_node:peer_ip(main),
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(ar_util:encode(Address))
					++ "/reserved_rewards_total"
		}),
	binary_to_integer(Reply).