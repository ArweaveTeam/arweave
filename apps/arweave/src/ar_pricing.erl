-module(ar_pricing).

%% 2.6 exports.
-export([get_price_per_gib_minute/2, get_tx_fee/1,
		get_miner_reward_endowment_pool_debt_supply/1, recalculate_price_per_gib_minute/1,
		redenominate/3, may_be_redenominate/1]).

%% 2.5 exports.
-export([get_tx_fee/4, get_miner_reward_and_endowment_pool/1,
		usd_to_ar_rate/1, usd_to_ar/3, recalculate_usd_to_ar_rate/1,
		get_storage_cost/4, get_expected_min_decline_rate/6]).

%% For tests.
-export([get_v2_price_per_gib_minute/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_inflation.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Types.
%%%===================================================================

-type nonegint() :: non_neg_integer().
-type fraction() :: {integer(), integer()}.
-type usd() :: float() | fraction().
-type date() :: {nonegint(), nonegint(), nonegint()}.
-type time() :: {nonegint(), nonegint(), nonegint()}.
-type datetime() :: {date(), time()}.

%%%===================================================================
%%% Public interface 2.6+.
%%%===================================================================

%% @doc Return the price per gibibyte minute estimated from the given history of
%% network hash rates and block rewards. The total reward used in calculations
%% is at least 1 Winston, even if all block rewards from the given history are 0.
%% Also, the returned price is always at least 1 Winston.
get_price_per_gib_minute(Height, B) ->
	V2Price = get_v2_price_per_gib_minute(Height, B),
	ar_pricing_transition:get_transition_price(Height, V2Price).

get_v2_price_per_gib_minute(Height, B) ->
	OneDifficultyHeight = ar_fork:height_2_7() + ar_block_time_history:history_length(),
	TwoDifficultyHeight = ar_fork:height_2_7_2() + ar_block_time_history:history_length(),

	case Height of
		_ when Height >= TwoDifficultyHeight ->
			get_v2_price_per_gib_minute_two_difficulty(Height, B);
		_ when Height >= OneDifficultyHeight ->
			get_v2_price_per_gib_minute_one_difficulty(Height, B);
		_ ->
			get_v2_price_per_gib_minute_simple(B)
	end.

get_v2_price_per_gib_minute_two_difficulty(Height, B) ->
	{HashRateTotal, RewardTotal} = ar_rewards:get_locked_totals(B),
	{IntervalTotal, VDFIntervalTotal, OneChunkCount, TwoChunkCount} =
		ar_block_time_history:sum_history(B),
	%% The intent of the SolutionsPerPartitionPerVDFStep is to estimate network replica
	%% count (how many copies of the weave are stored across the network).
	%% The logic behind this is complex - an explanation from @vird:
	%%
	%% 1. Naive solution: If we assume that each miner stores 1 replica, then we
	%%    can trivially calculate the network replica count using the network hashrate
	%%    (which we have) and the weave size (which we also have). However what if on
	%%    average each miner only stores 50% of the weave? In that case each miner will
	%%    get fewer hashes per partition (because they will miss out on 2-chunk solutions
	%%    that fall on the partitions they don't store), and that will push *up* the
	%%    replica count for a given network hashrate. How much to scale up our replica
	%%    count is based on the average replica count per miner.
	%%
	%% 2. Estimate average replica count per miner. Start with this basic assumption:
	%%    the higher the percentage of the weave a miner stores, the more likely they are
	%%    to mine a 2-chunk solution. If a miner has 100% of the weave and if the PoA1 and
	%%    PoA2 difficulties are the same, then, on average, 50% of their solutions will be
	%%    1-chunk, and 50% will be 2-chunk.
	%%
	%%    With this we can use the ratio of observed 2-chunk to 1-chunk solutions to
	%%    estimate the average percentage of the weave each miner stores.
	%%
	%% 3. However, what happens if the PoA1 difficulty is higher than the PoA2 difficulty?
	%%    In that case, we'd expect a miner with 100% of the weave to have fewer 1-chunk
	%%    solutions than 2-chunk solutions. If the PoA1 difficulty is PoA1Mult times higher
	%%    than the PoA2 difficulty, we'd expect the maximum number of solutions to be:
	%%    
	%%    (PoA1Mult + 1) * ?RECALL_RANGE_SIZE div (?DATA_CHUNK_SIZE * PoA1Mult)
	%%    
	%%    Or basically 1 1-chunk solution for every PoA1Mult 2-chunk solutions in the
	%%    full-replica case.
	%%
	%% 4. Finally, what if the average miner is not mining a full replica? In that case we
	%%    need to arrive at an equation that weights the 1-chunk and 2-chunk solutions
	%%    differently - and use that to estimate the expected number of solutions per
	%%    partition:
	%%
	%%    EstimatedSolutionsPerPartition = 
	%%    (
	%%      ?RECALL_RANGE_SIZE div PoA1Mult +
	%%		?RECALL_RANGE_SIZE * TwoChunkCount div (OneChunkCount * PoA1Mult)
	%%    ) div (?DATA_CHUNK_SIZE) 
	%%
	%% The SolutionsPerPartitionPerVDFStep combines that average weave calculation
	%% with the expected number of solutions per partition per VDF step to arrive a single
	%% number that can be used in the PricePerGiBPerMinute calculation.
	PoA1Mult = ar_difficulty:poa1_diff_multiplier(Height),
	MaxSolutionsPerPartition =
		(PoA1Mult + 1) * ?RECALL_RANGE_SIZE div (?DATA_CHUNK_SIZE * PoA1Mult),
	SolutionsPerPartitionPerVDFStep =
		case OneChunkCount of
			0 ->
				MaxSolutionsPerPartition;
			_ ->
				%% The following is a version of the EstimatedSolutionsPerPartition
				%% equation mentioned above that has been simpplified to limit rounding
				%% errors:
				EstimatedSolutionsPerPartition =
					(OneChunkCount + TwoChunkCount) * ?RECALL_RANGE_SIZE
					div (?DATA_CHUNK_SIZE * OneChunkCount * PoA1Mult),
				min(MaxSolutionsPerPartition, EstimatedSolutionsPerPartition)
		end,
	%% The following walks through the math of calculating the price per GiB per minute.
	%% However to reduce rounding errors due to divs, the uncommented equation at the
	%% end is used instead. Logically they should be the same. Notably the '* ?TARGET_BLOCK_TIME' in
	%% SolutionsPerPartitionPerBlock and the 'div ?TARGET_BLOCK_TIME' in PricePerGiBPerSecond cancel
	%% each other out.
	%%
	%% SolutionsPerPartitionPerSecond =
	%%          (SolutionsPerPartitionPerVDFStep * VDFIntervalTotal) div IntervalTotal
	%% SolutionsPerPartitionPerBlock = SolutionsPerPartitionPerSecond * ?TARGET_BLOCK_TIME,
	%% EstimatedPartitionCount = max(1, HashRateTotal) div SolutionsPerPartitionPerBlock,
	%% EstimatedDataSizeInGiB = EstimatedPartitionCount * (?PARTITION_SIZE) div (?GiB),
	%% PricePerGiBPerBlock = max(1, RewardTotal) div EstimatedDataSizeInGiB,
	%% PricePerGiBPerSecond = PricePerGibPerBlock div ?TARGET_BLOCK_TIME
	%% PricePerGiBPerMinute = PricePerGiBPerSecond * 60,
	PricePerGiBPerMinute = 
		(
			(SolutionsPerPartitionPerVDFStep * VDFIntervalTotal) *
			max(1, RewardTotal) * (?GiB) * 60
		)
		div
		(
			IntervalTotal * max(1, HashRateTotal) * (?PARTITION_SIZE)
		),
	log_price_metrics(get_v2_price_per_gib_minute_two_difficulty,
		Height, HashRateTotal, RewardTotal, 
		IntervalTotal, VDFIntervalTotal, OneChunkCount, TwoChunkCount,
		SolutionsPerPartitionPerVDFStep, PricePerGiBPerMinute),
	PricePerGiBPerMinute.

get_v2_price_per_gib_minute_one_difficulty(Height, B) ->
	{HashRateTotal, RewardTotal} = ar_rewards:get_locked_totals(B),
	{IntervalTotal, VDFIntervalTotal, OneChunkCount, TwoChunkCount} =
		ar_block_time_history:sum_history(B),
	%% The intent of the SolutionsPerPartitionPerVDFStep is to estimate network replica
	%% count (how many copies of the weave are stored across the network).
	%% The logic behind this is complex - an explanation from @vird:
	%%
	%% 1. Naive solution: If we assume that each miner stores 1 replica, then we
	%%    can trivially calculate the network replica count using the network hashrate
	%%    (which we have) and the weave size (which we also have). However what if on
	%%    average each miner only stores 50% of the weave? In that case each miner will
	%%    get fewer hashes per partition (because they will miss out on 2-chunk solutions
	%%    that fall on the partitions they don't store), and that will push *up* the
	%%    replica count for a given network hashrate. How much to scale up our replica
	%%    count is based on the average replica count per miner.
	%% 2. Estimate average replica count per miner: Start with this basic assumption:
	%%    the higher the percentage of the weave a miner stores, the more likely they are
	%%    to mine a 2-chunk solution. If a miner has 100% of the weave, then, on average,
	%%    50% of their solutions will be 1-chunk, and 50% will be 2-chunk.
	%%
	%%    With this we can use the ratio of observed 2-chunk to 1-chunk solutions to
	%%    estimate the average percentage of the weave each miner stores.
	%%
	%% The SolutionsPerPartitionPerVDFStep combines that average weave % calculation
	%% with the expected number of solutions per partition per VDF step to arrive a single
	%% number that can be used in the PricePerGiBPerMinute calculation.
	SolutionsPerPartitionPerVDFStep =
		case OneChunkCount of
			0 ->
				2 * (?RECALL_RANGE_SIZE) div (?DATA_CHUNK_SIZE);
			_ ->
				min(2 * ?RECALL_RANGE_SIZE,
						?RECALL_RANGE_SIZE
							+ ?RECALL_RANGE_SIZE * TwoChunkCount div OneChunkCount)
					div ?DATA_CHUNK_SIZE
		end,
	%% The following walks through the math of calculating the price per GiB per minute.
	%% However to reduce rounding errors due to divs, the uncommented equation at the
	%% end is used instead. Logically they should be the same. Notably the '* ?TARGET_BLOCK_TIME' in
	%% SolutionsPerPartitionPerBlock and the 'div ?TARGET_BLOCK_TIME' in PricePerGiBPerSecond cancel
	%% each other out.
	%%
	%% SolutionsPerPartitionPerSecond =
	%%          (SolutionsPerPartitionPerVDFStep * VDFIntervalTotal) div IntervalTotal
	%% SolutionsPerPartitionPerBlock = SolutionsPerPartitionPerSecond * ?TARGET_BLOCK_TIME,
	%% EstimatedPartitionCount = max(1, HashRateTotal) div SolutionsPerPartitionPerBlock,
	%% EstimatedDataSizeInGiB = EstimatedPartitionCount * (?PARTITION_SIZE) div (?GiB),
	%% PricePerGiBPerBlock = max(1, RewardTotal) div EstimatedDataSizeInGiB,
	%% PricePerGiBPerSecond = PricePerGibPerBlock div ?TARGET_BLOCK_TIME
	%% PricePerGiBPerMinute = PricePerGiBPerSecond * 60,
	PricePerGiBPerMinute = 
		(
			(SolutionsPerPartitionPerVDFStep * VDFIntervalTotal) *
			max(1, RewardTotal) * (?GiB) * 60
		)
		div
		(
			IntervalTotal * max(1, HashRateTotal) * (?PARTITION_SIZE)
		),
	log_price_metrics(get_v2_price_per_gib_minute_one_difficulty,
		Height, HashRateTotal, RewardTotal, 
		IntervalTotal, VDFIntervalTotal, OneChunkCount, TwoChunkCount,
		SolutionsPerPartitionPerVDFStep, PricePerGiBPerMinute),
	PricePerGiBPerMinute.

get_v2_price_per_gib_minute_simple(B) ->
	{HashRateTotal, RewardTotal} = ar_rewards:get_locked_totals(B),
	%% 2 recall ranges per partition per second.
	SolutionsPerPartitionPerSecond = 2 * (?RECALL_RANGE_SIZE) div (?DATA_CHUNK_SIZE),
	SolutionsPerPartitionPerMinute = SolutionsPerPartitionPerSecond * 60,
	SolutionsPerPartitionPerBlock = SolutionsPerPartitionPerMinute * 2,
	%% Estimated partition count = hash rate / 2 / solutions per partition per minute.
	%% 2 minutes is the average block time.
	%% Estimated data size = estimated partition count * partition size.
	%% Estimated price per gib minute = total block reward / estimated data size
	%% in gibibytes.
	(max(1, RewardTotal) * (?GiB) * SolutionsPerPartitionPerBlock)
		div (max(1, HashRateTotal)
				* (?PARTITION_SIZE)
				* 2	% The reward is paid every two minutes whereas we are calculating
					% the minute rate here.
			).

%% @doc Return the minimum required transaction fee for the given number of
%% total bytes stored and gibibyte minute price.
get_tx_fee(Args) ->
	{DataSize, GiBMinutePrice, KryderPlusRateMultiplier, Height} = Args,
	FirstYearPrice = DataSize * GiBMinutePrice * 60 * 24 * 365,
	{LnDecayDividend, LnDecayDivisor} = ?LN_PRICE_DECAY_ANNUAL,
	PerpetualPrice = {-FirstYearPrice * LnDecayDivisor * KryderPlusRateMultiplier
			* (?N_REPLICATIONS(Height)), LnDecayDividend * (?GiB)},
	MinerShare = ar_fraction:multiply(PerpetualPrice,
			?MINER_MINIMUM_ENDOWMENT_CONTRIBUTION_SHARE),
	{Dividend, Divisor} = ar_fraction:add(PerpetualPrice, MinerShare),
	Dividend div Divisor.

%% @doc Return the block reward, the new endowment pool, and the new debt supply.
get_miner_reward_endowment_pool_debt_supply(Args) ->
	{EndowmentPool, DebtSupply, TXs, WeaveSize, Height, GiBMinutePrice,
			KryderPlusRateMultiplierLatch, KryderPlusRateMultiplier, Denomination,
			BlockInterval} = Args,
	Inflation = redenominate(ar_inflation:calculate(Height), 1, Denomination),
	ExpectedReward = (?N_REPLICATIONS(Height)) * WeaveSize * GiBMinutePrice
			* BlockInterval div (60 * ?GiB),
	{EndowmentPoolFeeShare, MinerFeeShare} = distribute_transaction_fees2(TXs, Denomination),
	BaseReward = Inflation + MinerFeeShare,
	EndowmentPool2 = EndowmentPool + EndowmentPoolFeeShare,
	case BaseReward >= ExpectedReward of
		true ->
			{BaseReward, EndowmentPool2, DebtSupply, KryderPlusRateMultiplierLatch,
					KryderPlusRateMultiplier};
		false ->
			Take = ExpectedReward - BaseReward,
			{EndowmentPool3, DebtSupply2} =
				case Take > EndowmentPool2 of
					true ->
						{0, DebtSupply + Take - EndowmentPool2};
					false ->
						{EndowmentPool2 - Take, DebtSupply}
				end,
			{KryderPlusRateMultiplierLatch2, KryderPlusRateMultiplier2} =
				case {Take > EndowmentPool2, KryderPlusRateMultiplierLatch} of
					{true, 0} ->
						{1, KryderPlusRateMultiplier * 2};
					{false, 1} ->
						Threshold = redenominate(?RESET_KRYDER_PLUS_LATCH_THRESHOLD, 1,
								Denomination),
						case EndowmentPool3 > Threshold of
							true ->
								{0, KryderPlusRateMultiplier};
							false ->
								{1, KryderPlusRateMultiplier}
						end;
					_ ->
						{KryderPlusRateMultiplierLatch, KryderPlusRateMultiplier}
				end,
			{BaseReward + Take, EndowmentPool3, DebtSupply2, KryderPlusRateMultiplierLatch2,
					KryderPlusRateMultiplier2}
	end.

%% @doc Return the denominated amount.
redenominate(Amount, 0, _Denomination) ->
	Amount;
redenominate(Amount, BaseDenomination, BaseDenomination) ->
	Amount;
redenominate(Amount, BaseDenomination, Denomination) when Denomination > BaseDenomination ->
	redenominate(Amount * 1000, BaseDenomination, Denomination - 1).

%% @doc	Increase the amount of base currency units in the system if
%% the available supply is too low.
may_be_redenominate(B) ->
	#block{ height = Height, denomination = Denomination,
			redenomination_height = RedenominationHeight } = B,
	case ar_pricing_transition:is_v2_pricing_height(Height + 1) of
		false ->
			{Denomination, RedenominationHeight};
		true ->
			may_be_redenominate2(B)
	end.

may_be_redenominate2(B) ->
	#block{ height = Height, denomination = Denomination,
			redenomination_height = RedenominationHeight } = B,
	case Height == RedenominationHeight of
		true ->
			{Denomination + 1, RedenominationHeight};
		false ->
			case Height < RedenominationHeight of
				true ->
					{Denomination, RedenominationHeight};
				false ->
					may_be_redenominate3(B)
			end
	end.

may_be_redenominate3(B) ->
	#block{ height = Height, debt_supply = DebtSupply, reward_pool = EndowmentPool,
			denomination = Denomination, redenomination_height = RedenominationHeight } = B,
	TotalSupply = get_total_supply(Denomination),
	case TotalSupply + DebtSupply - EndowmentPool < (?REDENOMINATION_THRESHOLD) of
		true ->
			{Denomination, Height + (?REDENOMINATION_DELAY_BLOCKS)};
		false ->
			{Denomination, RedenominationHeight}
	end.

%% @doc Return the new current and scheduled prices per byte minute.
recalculate_price_per_gib_minute(B) ->
	#block{ height = PrevHeight, block_time_history = BlockTimeHistory,
			denomination = Denomination, price_per_gib_minute = Price,
			scheduled_price_per_gib_minute = ScheduledPrice } = B,
	Height = PrevHeight + 1,
	Fork_2_7 = ar_fork:height_2_7(),
	Fork_2_7_1 = ar_fork:height_2_7_1(),
	case Height of
		Fork_2_7 ->
			{ar_pricing_transition:static_price(), ar_pricing_transition:static_price()};
		Height when Height < Fork_2_7_1 ->
			case is_price_adjustment_height(Height) of
				false ->
					{Price, ScheduledPrice};
				true ->
					%% price_per_gib_minute = scheduled_price_per_gib_minute
					%% scheduled_price_per_gib_minute = get_price_per_gib_minute() capped to
					%%                                  0.5x to 2x of old price_per_gib_minute
					Price2 = min(Price * 2, get_price_per_gib_minute(Height, B)),
					Price3 = max(Price div 2, Price2),
					{ScheduledPrice, Price3}
			end;
		_ ->
			case is_price_adjustment_height(Height) of
				false ->
					{Price, ScheduledPrice};
				true ->
					%% price_per_gib_minute = scheduled_price_per_gib_minute
					%% scheduled_price_per_gib_minute =
					%% 		get_price_per_gib_minute() 
					%%		EMA'ed with scheduled_price_per_gib_minute at 0.1 alpha
					%%		and then capped to 0.5x to 2x of scheduled_price_per_gib_minute
					TargetPrice = get_price_per_gib_minute(Height, B),
					EMAPrice = (9 * ScheduledPrice + TargetPrice) div 10,
					Price2 = min(ScheduledPrice * 2, EMAPrice),
					Price3 = max(ScheduledPrice div 2, Price2),
					?LOG_DEBUG([{event, recalculate_price_per_gib_minute},
						{height, Height},
						{old_price, Price},
						{scheduled_price, ScheduledPrice},
						{target_price, TargetPrice},
						{ema_price, EMAPrice},
						{capped_price, Price3}]),
					{ScheduledPrice, Price3}
			end
	end.

is_price_adjustment_height(Height) ->
	Height rem ?PRICE_ADJUSTMENT_FREQUENCY == 0.

distribute_transaction_fees2(TXs, Denomination) ->
	distribute_transaction_fees2(TXs, 0, 0, Denomination).

distribute_transaction_fees2([], EndowmentPoolTotal, MinerTotal, _Denomination) ->
	{EndowmentPoolTotal, MinerTotal};
distribute_transaction_fees2([TX | TXs], EndowmentPoolTotal, MinerTotal, Denomination) ->
	TXFee = redenominate(TX#tx.reward, TX#tx.denomination, Denomination),
	{Dividend, Divisor} = ?MINER_FEE_SHARE,
	MinerFee = TXFee * Dividend div Divisor,
	EndowmentPoolTotal2 = EndowmentPoolTotal + TXFee - MinerFee,
	MinerTotal2 = MinerTotal + MinerFee,
	distribute_transaction_fees2(TXs, EndowmentPoolTotal2, MinerTotal2, Denomination).

get_total_supply(Denomination) ->
	redenominate(?TOTAL_SUPPLY, 1, Denomination).

%%%===================================================================
%%% Public interface 2.5.
%%%===================================================================

%% @doc Return the perpetual cost of storing the given amount of data.
get_storage_cost(DataSize, Timestamp, Rate, Height) ->
	Size = ?TX_SIZE_BASE + DataSize,
	PerpetualGBStorageCost =
		usd_to_ar(
			get_perpetual_gb_cost_at_timestamp(Timestamp, Height),
			Rate,
			Height
		),
	StorageCost = max(1, PerpetualGBStorageCost div (1024 * 1024 * 1024)) * Size,
	HashingCost = StorageCost,
	StorageCost + HashingCost.

%% @doc Calculate the transaction fee.
get_tx_fee(DataSize, Timestamp, Rate, Height) ->
	MaintenanceCost = get_storage_cost(DataSize, Timestamp, Rate, Height),
	MinerFeeShare = get_miner_fee_share(MaintenanceCost, Height),
	MaintenanceCost + MinerFeeShare.

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool({Pool, TXs, unclaimed, _, _, _, _}) ->
	{0, Pool + lists:sum([TX#tx.reward || TX <- TXs])};
get_miner_reward_and_endowment_pool(Args) ->
	{Pool, TXs, _Addr, WeaveSize, Height, Timestamp, Rate} = Args,
	Inflation = trunc(ar_inflation:calculate(Height)),
	{PoolFeeShare, MinerFeeShare} = distribute_transaction_fees(TXs, Height),
	BaseReward = Inflation + MinerFeeShare,
	StorageCostPerGBPerBlock =
		usd_to_ar(
			get_gb_cost_per_block_at_timestamp(Timestamp, Height),
			Rate,
			Height
		),
	Burden = WeaveSize * StorageCostPerGBPerBlock div (1024 * 1024 * 1024),
	Pool2 = Pool + PoolFeeShare,
	case BaseReward >= Burden of
		true ->
			{BaseReward, Pool2};
		false ->
			Take = min(Pool2, Burden - BaseReward),
			{BaseReward + Take, Pool2 - Take}
	end.

%% @doc Return the effective USD to AR rate corresponding to the given block
%% considering its previous block.
usd_to_ar_rate(#block{ height = PrevHeight } = PrevB) ->
	Height_2_5 = ar_fork:height_2_5(),
	Height = PrevHeight + 1,
	case PrevHeight < Height_2_5 of
		true ->
			?INITIAL_USD_TO_AR(Height)();
		false ->
			PrevB#block.usd_to_ar_rate
	end.

%% @doc Return the amount of AR the given number of USD is worth.
usd_to_ar(USD, Rate, Height) when is_number(USD) ->
	usd_to_ar({USD, 1}, Rate, Height);
usd_to_ar({Dividend, Divisor}, Rate, Height) ->
	InitialInflation = trunc(ar_inflation:calculate(?INITIAL_USD_TO_AR_HEIGHT(Height)())),
	CurrentInflation = trunc(ar_inflation:calculate(Height)),
	{InitialRateDividend, InitialRateDivisor} = Rate,
	trunc(	Dividend
			* ?WINSTON_PER_AR
			* CurrentInflation
			* InitialRateDividend	)
		div Divisor
		div InitialInflation
		div InitialRateDivisor.

recalculate_usd_to_ar_rate(#block{ height = PrevHeight } = B) ->
	Height = PrevHeight + 1,
	Fork_2_5 = ar_fork:height_2_5(),
	true = Height >= Fork_2_5,
	case Height > Fork_2_5 of
		false ->
			Rate = ?INITIAL_USD_TO_AR(Height)(),
			{Rate, Rate};
		true ->
			Fork_2_6 = ar_fork:height_2_6(),
			case Height == Fork_2_6 of
				true ->
					{B#block.usd_to_ar_rate, ?FORK_2_6_PRE_TRANSITION_USD_TO_AR_RATE};
				false ->
					recalculate_usd_to_ar_rate2(B)
			end
	end.

%% @doc Return an estimation for the minimum required decline rate making the given
%% Amount (in Winston) sufficient to subsidize storage for Period seconds starting from
%% Timestamp and assuming the given USD to AR rate.
%% When computing the exponent, the function accounts for the first 16 summands in
%% the Taylor series. The fraction is reduced to the 1/1000000 precision.
get_expected_min_decline_rate(Timestamp, Period, Amount, Size, Rate, Height) ->
	{USDDiv1, USDDivisor1} = get_gb_cost_per_year_at_timestamp(Timestamp, Height),
	%% Multiply by 2 to account for hashing costs.
	Sum1 = 2 * usd_to_ar({USDDiv1, USDDivisor1}, Rate, Height),
	{USDDiv2, USDDivisor2} = get_gb_cost_per_year_at_timestamp(Timestamp + Period, Height),
	Sum2 = 2 * usd_to_ar({USDDiv2, USDDivisor2}, Rate, Height),
	%% Sum1 / -logRate - Sum2 / -logRate = Amount
	%% => -logRate = (Sum1 - Sum2) / Amount
	%% => 1 / Rate = exp((Sum1 - Sum2) / Amount)
	%% => Rate = 1 / exp((Sum1 - Sum2) / Amount)
	{ExpDiv, ExpDivisor} = ar_fraction:natural_exponent(
			{(Sum1 - Sum2) * Size, Amount * (1024 * 1024 * 1024)}, 16),
	ar_fraction:reduce({ExpDivisor, ExpDiv}, 1000000).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Get the share of the maintenance cost the miner receives for a transation.
get_miner_fee_share(MaintenanceCost, Height) ->
	{Dividend, Divisor} = ?MINING_REWARD_MULTIPLIER,
	case Height >= ar_fork:height_2_5() of
		false ->
			erlang:trunc(MaintenanceCost * (Dividend / Divisor));
		true ->
			MaintenanceCost * Dividend div Divisor
	end.

distribute_transaction_fees(TXs, Height) ->
	distribute_transaction_fees(TXs, 0, 0, Height).

distribute_transaction_fees([], EndowmentPool, Miner, _Height) ->
	{EndowmentPool, Miner};
distribute_transaction_fees([TX | TXs], EndowmentPool, Miner, Height) ->
	TXFee = TX#tx.reward,
	{Dividend, Divisor} = ?MINING_REWARD_MULTIPLIER,
	MinerFee =
		case Height >= ar_fork:height_2_5() of
			false ->
				erlang:trunc((Dividend / Divisor) * TXFee / ((Dividend / Divisor) + 1));
			true ->
				TXFee * Dividend div (Dividend + Divisor)
		end,
	distribute_transaction_fees(TXs, EndowmentPool + TXFee - MinerFee, Miner + MinerFee,
			Height).

%% @doc Return the cost of storing 1 GB in the network perpetually.
%% Integral of the exponential decay curve k*e^(-at), i.e. k/a.
%% @end
-spec get_perpetual_gb_cost_at_timestamp(Timestamp::integer(), Height::nonegint()) -> usd().
get_perpetual_gb_cost_at_timestamp(Timestamp, Height) ->
	K = get_gb_cost_per_year_at_timestamp(Timestamp, Height),
	get_perpetual_gb_cost(K, Height).

-spec get_perpetual_gb_cost(Init::usd(), Height::nonegint()) -> usd().
get_perpetual_gb_cost(Init, Height) ->
	case Height >= ar_fork:height_2_5() of
		true ->
			{LnDecayDividend, LnDecayDivisor} = ?LN_PRICE_DECAY_ANNUAL,
			{InitDividend, InitDivisor} = Init,
			{-InitDividend * LnDecayDivisor, InitDivisor * LnDecayDividend};
		false ->
			{Dividend, Divisor} = ?PRICE_DECAY_ANNUAL,
			Init / -math:log(Dividend / Divisor)
	end.

%% @doc Return the cost in USD of storing 1 GB per year at the given time.
-spec get_gb_cost_per_year_at_timestamp(Timestamp::integer(), Height::nonegint()) -> usd().
get_gb_cost_per_year_at_timestamp(Timestamp, Height) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_gb_cost_per_year_at_datetime(Datetime, Height).

%% @doc Return the cost in USD of storing 1 GB per average block time at the given time.
-spec get_gb_cost_per_block_at_timestamp(integer(), nonegint()) -> usd().
get_gb_cost_per_block_at_timestamp(Timestamp, Height) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_gb_cost_per_block_at_datetime(Datetime, Height).

%% @doc Return the cost in USD of storing 1 GB per year.
-spec get_gb_cost_per_year_at_datetime(DT::datetime(), Height::nonegint()) -> usd().
get_gb_cost_per_year_at_datetime({{Y, M, _}, _} = DT, Height) ->
	PrevY = prev_jun_30_year(Y, M),
	NextY = next_jun_30_year(Y, M),
	FracY = fraction_of_year(PrevY, NextY, DT, Height),
	PrevYCost = usd_p_gby(PrevY, Height),
	NextYCost = usd_p_gby(NextY, Height),
	case Height >= ar_fork:height_2_5() of
		true ->
			{FracYDividend, FracYDivisor} = FracY,
			{PrevYCostDividend, PrevYCostDivisor} = PrevYCost,
			{NextYCostDividend, NextYCostDivisor} = NextYCost,
			Dividend =
				(?N_REPLICATIONS(Height))
				* (
					PrevYCostDividend * NextYCostDivisor * FracYDivisor
					- FracYDividend
						* (
							PrevYCostDividend
								* NextYCostDivisor
							- NextYCostDividend
								* PrevYCostDivisor
						)
				),
			Divisor =
				PrevYCostDivisor
				* NextYCostDivisor
				* FracYDivisor,
			{Dividend, Divisor};
		false ->
			CY = PrevYCost - (FracY * (PrevYCost - NextYCost)),
			CY * (?N_REPLICATIONS(Height))
	end.

prev_jun_30_year(Y, M) when M < 7 ->
	Y - 1;
prev_jun_30_year(Y, _M) ->
	Y.

next_jun_30_year(Y, M) when M < 7 ->
	Y;
next_jun_30_year(Y, _M) ->
	Y + 1.

%% @doc Return the cost in USD of storing 1 GB per average block time.
-spec get_gb_cost_per_block_at_datetime(DT::datetime(), Height::nonegint()) -> usd().
get_gb_cost_per_block_at_datetime(DT, Height) ->
	case Height >= ar_fork:height_2_5() of
		true ->
			{Dividend, Divisor} = get_gb_cost_per_year_at_datetime(DT, Height),
			{Dividend, Divisor * ar_inflation:blocks_per_year(Height)};
		false ->
			get_gb_cost_per_year_at_datetime(DT, Height) / ar_inflation:blocks_per_year(Height)
	end.

%% @doc Return the cost in USD of storing 1 GB per year. Estmimated from empirical data.
%% Assumes a year after 2019 inclusive. Uses data figures for 2018 and 2019.
%% Extrapolates the exponential decay curve k*e^(-at) to future years.
%% @end
-spec usd_p_gby(nonegint(), nonegint()) -> usd().
usd_p_gby(2018, Height) ->
	{Dividend, Divisor} = ?USD_PER_GBY_2018,
	case Height >= ar_fork:height_2_5() of
		true ->
			{Dividend, Divisor};
		false ->
			Dividend / Divisor
	end;
usd_p_gby(2019, Height) ->
	{Dividend, Divisor} = ?USD_PER_GBY_2019,
	case Height >= ar_fork:height_2_5() of
		true ->
			{Dividend, Divisor};
		false ->
			Dividend / Divisor
	end;
usd_p_gby(Y, Height) ->
	case Height >= ar_fork:height_2_5() of
		true ->
			{KDividend, KDivisor} = ?USD_PER_GBY_2019,
			{ADividend, ADivisor} = ?LN_PRICE_DECAY_ANNUAL,
			T = Y - 2019,
			P = ?TX_PRICE_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION,
			{EDividend, EDivisor} = ar_fraction:natural_exponent({ADividend * T, ADivisor}, P),
			{EDividend * KDividend, EDivisor * KDivisor};	
		false ->
			{Dividend, Divisor} = ?USD_PER_GBY_2019,
			K = Dividend / Divisor,
			{DecayDividend, DecayDivisor} = ?PRICE_DECAY_ANNUAL,
			A = math:log(DecayDividend / DecayDivisor),
			T = Y - 2019,
			K * math:exp(A * T)
	end.

%% @doc Return elapsed time as the fraction of the year
%% between Jun 30th of PrevY and Jun 30th of NextY.
%% @end
-spec fraction_of_year(nonegint(), nonegint(), datetime(), nonegint()) -> float() | fraction().
fraction_of_year(PrevY, NextY, {{Y, Mo, D}, {H, Mi, S}}, Height) ->
	Start = calendar:datetime_to_gregorian_seconds({{PrevY, 6, 30}, {23, 59, 59}}),
	Now = calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, Mi, S}}),
	End = calendar:datetime_to_gregorian_seconds({{NextY, 6, 30}, {23, 59, 59}}),
	case Height >= ar_fork:height_2_5() of
		true ->
			{Now - Start, End - Start};
		false ->
			(Now - Start) / (End - Start)
	end.

%% TODO Use calendar:system_time_to_universal_time/2 in Erlang OTP-21.
system_time_to_universal_time(Time, TimeUnit) ->
	Seconds = erlang:convert_time_unit(Time, TimeUnit, seconds),
	DaysFrom0To1970 = 719528,
	SecondsPerDay = 86400,
	calendar:gregorian_seconds_to_datetime(Seconds + (DaysFrom0To1970 * SecondsPerDay)).

recalculate_usd_to_ar_rate2(#block{ height = PrevHeight } = B) ->
	case is_price_adjustment_height(PrevHeight + 1) of
		false ->
			{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
		true ->
			Fork_2_6 = ar_fork:height_2_6(),
			true = PrevHeight + 1 /= Fork_2_6,
			case PrevHeight + 1 > Fork_2_6 of
				true ->
					%% Keep the rate fixed after the 2.6 fork till the transition to the
					%% new pricing scheme ends. Then it won't be used any longer.
					{B#block.scheduled_usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
				false ->
					recalculate_usd_to_ar_rate3(B)
			end
	end.

recalculate_usd_to_ar_rate3(#block{ height = PrevHeight, diff = Diff } = B) ->
	Height = PrevHeight + 1,
	InitialDiff = ar_retarget:switch_to_linear_diff(?INITIAL_USD_TO_AR_DIFF(Height)()),
	MaxDiff = ?MAX_DIFF,
	InitialRate = ?INITIAL_USD_TO_AR(Height)(),
	{Dividend, Divisor} = InitialRate,
	ScheduledRate = {Dividend * (MaxDiff - Diff), Divisor * (MaxDiff - InitialDiff)},
	Rate = B#block.scheduled_usd_to_ar_rate,
	MaxAdjustmentUp = ar_fraction:multiply(Rate, ?USD_TO_AR_MAX_ADJUSTMENT_UP_MULTIPLIER),
	MaxAdjustmentDown = ar_fraction:multiply(Rate, ?USD_TO_AR_MAX_ADJUSTMENT_DOWN_MULTIPLIER),
	CappedScheduledRate = ar_fraction:reduce(ar_fraction:maximum(
			ar_fraction:minimum(ScheduledRate, MaxAdjustmentUp), MaxAdjustmentDown),
			?USD_TO_AR_FRACTION_REDUCTION_LIMIT),
	?LOG_DEBUG([{event, recalculated_rate},
			{new_rate, ar_util:safe_divide(element(1, Rate), element(2, Rate))},
			{new_scheduled_rate, ar_util:safe_divide(element(1, CappedScheduledRate),
					element(2, CappedScheduledRate))},
			{new_scheduled_rate_without_capping,
					ar_util:safe_divide(element(1, ScheduledRate), element(2, ScheduledRate))},
		{max_adjustment_up, ar_util:safe_divide(element(1, MaxAdjustmentUp),
				element(2,MaxAdjustmentUp))},
		{max_adjustment_down, ar_util:safe_divide(element(1, MaxAdjustmentDown),
				element(2,MaxAdjustmentDown))}]),
	{Rate, CappedScheduledRate}.

log_price_metrics(Event,
		Height, HashRateTotal, RewardTotal, IntervalTotal, VDFIntervalTotal,
		OneChunkCount, TwoChunkCount, SolutionsPerPartitionPerVDFStep, PricePerGiBPerMinute) ->

	RewardHistoryLength = ar_testnet:reward_history_blocks(Height),
	AverageHashRate = HashRateTotal div RewardHistoryLength,
	EstimatedDataSizeInBytes = network_data_size(Height,
			AverageHashRate, IntervalTotal, VDFIntervalTotal, SolutionsPerPartitionPerVDFStep),

	prometheus_gauge:set(poa_count, [1], OneChunkCount),
	prometheus_gauge:set(poa_count, [2], TwoChunkCount),
	prometheus_gauge:set(v2_price_per_gibibyte_minute, PricePerGiBPerMinute),
	prometheus_gauge:set(network_data_size, EstimatedDataSizeInBytes),

	?LOG_DEBUG([{event, Event}, {height, Height},
		{hash_rate_total, HashRateTotal}, {average_hash_rate, AverageHashRate},
		{reward_total, RewardTotal},
		{interval_total, IntervalTotal}, {vdf_interval_total, VDFIntervalTotal},
		{one_chunk_count, OneChunkCount}, {two_chunk_count, TwoChunkCount},
		{solutions_per_partition_per_vdf_step, SolutionsPerPartitionPerVDFStep},
		{data_size, EstimatedDataSizeInBytes}, {price, PricePerGiBPerMinute}]).

network_data_size(Height,
		AverageHashRate, IntervalTotal, VDFIntervalTotal, SolutionsPerPartitionPerVDFStep) ->
	TargetTime = ar_testnet:target_block_time(Height),
	SolutionsPerPartitionPerBlock =
		(SolutionsPerPartitionPerVDFStep * VDFIntervalTotal * TargetTime) div IntervalTotal,
	EstimatedPartitionCount = AverageHashRate div SolutionsPerPartitionPerBlock,
	EstimatedPartitionCount * (?PARTITION_SIZE).

%%%===================================================================
%%% Tests.
%%%===================================================================

get_gb_cost_per_year_at_datetime_is_monotone_test_() ->
	[
		ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> infinity end}],
			fun test_get_gb_cost_per_year_at_datetime_is_monotone/0, 120)
		| 
		[
			ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> Height end}],
				fun test_get_gb_cost_per_year_at_datetime_is_monotone/0, 120)
			|| Height <- lists:seq(0, 20)
		]
	].

test_get_gb_cost_per_year_at_datetime_is_monotone() ->
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
		fun(CurrDT, {PrevDT, PrevHeight}) ->
			CurrCost = get_gb_cost_per_year_at_datetime(CurrDT, PrevHeight + 1),
			PrevCost = get_gb_cost_per_year_at_datetime(PrevDT, PrevHeight),
			assert_less_than_or_equal_to(CurrCost, PrevCost),
			{CurrDT, PrevHeight + 1}
		end,
		{InitialDT, 0},
		FollowingDTs
	).

assert_less_than_or_equal_to(X1, X2) when is_number(X1), is_number(X2) ->
	?assert(X1 =< X2, io_lib:format("~p is bigger than ~p", [X1, X2]));
assert_less_than_or_equal_to({Dividend1, Divisor1} = X1, X2) when is_number(X2) ->
	?assert((Dividend1 div Divisor1) =< X2, io_lib:format("~p is bigger than ~p", [X1, X2]));
assert_less_than_or_equal_to({Dividend1, Divisor1} = X1, {Dividend2, Divisor2} = X2) ->
	?assert(Dividend1 * Divisor2 =< Dividend2 * Divisor1,
		io_lib:format("~p is bigger than ~p", [X1, X2])).
