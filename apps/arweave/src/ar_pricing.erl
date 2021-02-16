-module(ar_pricing).

-export([
	get_tx_fee/4,
	get_miner_reward_and_endowment_pool/1,
	get_tx_fee_pre_fork_2_4/4,
	usd_to_ar/3,
	recalculate_usd_to_ar_rate/1,
	usd_to_ar_pre_fork_2_4/3,
	get_miner_reward_and_endowment_pool_pre_fork_2_4/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_inflation.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Types.
%%%===================================================================

-type nonegint() :: non_neg_integer().
-type usd() :: float().
-type date() :: {nonegint(), nonegint(), nonegint()}.
-type time() :: {nonegint(), nonegint(), nonegint()}.
-type datetime() :: {date(), time()}.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Calculate the transaction fee.
get_tx_fee(DataSize, Timestamp, Rate, Height) ->
	Size = ?TX_SIZE_BASE + DataSize,
	PerpetualGBStorageCost =
		usd_to_ar(
			get_perpetual_gb_cost_at_timestamp(Timestamp),
			Rate,
			Height
		),
	StorageCost = max(1, PerpetualGBStorageCost div (1024 * 1024 * 1024)) * Size,
	HashingCost = StorageCost,
	MaintenanceCost = StorageCost + HashingCost,
	MinerFeeShare = get_miner_fee_share(MaintenanceCost),
	MaintenanceCost + MinerFeeShare.

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool({Pool, TXs, unclaimed, _, _, _, _}) ->
	{0, Pool + lists:sum([TX#tx.reward || TX <- TXs])};
get_miner_reward_and_endowment_pool(Args) ->
	{Pool, TXs, _Addr, WeaveSize, Height, Timestamp, Rate} = Args,
	Inflation = trunc(ar_inflation:calculate(Height)),
	{PoolFeeShare, MinerFeeShare} = distribute_transaction_fees(TXs),
	BaseReward = Inflation + MinerFeeShare,
	StorageCostPerGBPerBlock =
		usd_to_ar(
			get_gb_cost_per_block_at_timestamp(Timestamp),
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

%% @doc Calculate the transaction fee.
get_tx_fee_pre_fork_2_4(Size, Diff, Height, Timestamp) ->
	GBs = (?TX_SIZE_BASE + Size) / (1024 * 1024 * 1024),
	true = Height >= ar_fork:height_2_0(),
	PerGB = usd_to_ar_pre_fork_2_4(get_perpetual_gb_cost_at_timestamp(Timestamp), Diff, Height),
	StorageCost = PerGB * GBs,
	HashingCost = StorageCost,
	MaintenanceCost = erlang:trunc(StorageCost + HashingCost),
	MinerFeeShare = get_miner_fee_share(MaintenanceCost),
	MaintenanceCost + MinerFeeShare.

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool_pre_fork_2_4({Pool, TXs, unclaimed, _, _, _, _}) ->
	{0, Pool + lists:sum([TX#tx.reward || TX <- TXs])};
get_miner_reward_and_endowment_pool_pre_fork_2_4(Args) ->
	{Pool, TXs, _RewardAddr, WeaveSize, Height, Diff, Timestamp} = Args,
	true = Height >= ar_fork:height_2_0(),
	Inflation = trunc(ar_inflation:calculate(Height)),
	{PoolFeeShare, MinerFeeShare} = distribute_transaction_fees(TXs),
	BaseReward = Inflation + MinerFeeShare,
	StorageCostPerGBPerBlock =
		usd_to_ar_pre_fork_2_4(
			get_gb_cost_per_block_at_timestamp(Timestamp),
			Diff,
			Height
		),
	Burden = trunc(WeaveSize * StorageCostPerGBPerBlock / (1024 * 1024 * 1024)),
	Pool2 = Pool + PoolFeeShare,
	case BaseReward >= Burden of
		true ->
			{BaseReward, Pool2};
		false ->
			Take = min(Pool2, Burden - BaseReward),
			{BaseReward + Take, Pool2 - Take}
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
	case Height == Fork_2_5 of
		true ->
			Rate = ?INITIAL_USD_TO_AR(Height),
			{Rate, Rate};
		false ->
			recalculate_usd_to_ar_rate2(B)
	end.

%% @doc Return the amount of AR the given number of USD is worth.
usd_to_ar_pre_fork_2_4(USD, Diff, Height) ->
	InitialDiff = ar_retarget:switch_to_linear_diff(?INITIAL_USD_TO_AR_DIFF(Height)()),
	MaxDiff = ar_mine:max_difficulty(),
	DeltaP = (MaxDiff - InitialDiff) / (MaxDiff - Diff),
	InitialInflation = ar_inflation:calculate(?INITIAL_USD_TO_AR_HEIGHT(Height)()),
	DeltaInflation = ar_inflation:calculate(Height) / InitialInflation,
	erlang:trunc(
		(USD * ?WINSTON_PER_AR * DeltaInflation) / (?INITIAL_USD_PER_AR(Height)() * DeltaP)
	).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Get the share of the maintenance cost the miner receives for a transation.
get_miner_fee_share(MaintenanceCost) ->
	erlang:trunc(MaintenanceCost * ?MINING_REWARD_MULTIPLIER).

distribute_transaction_fees(TXs) ->
	distribute_transaction_fees(TXs, 0, 0).

distribute_transaction_fees([], EndowmentPool, Miner) ->
	{EndowmentPool, Miner};
distribute_transaction_fees([TX | TXs], EndowmentPool, Miner) ->
	TXFee = TX#tx.reward,
	MinerFee =
		erlang:trunc(
			(?MINING_REWARD_MULTIPLIER) * TXFee / ((?MINING_REWARD_MULTIPLIER) + 1)
		),
	distribute_transaction_fees(TXs, EndowmentPool + TXFee - MinerFee, Miner + MinerFee).

%% @doc Return the cost of storing 1 GB in the network perpetually.
%% Integral of the exponential decay curve k*e^(-at), i.e. k/a.
%% @end
-spec get_perpetual_gb_cost_at_timestamp(Timestamp::integer()) -> usd().
get_perpetual_gb_cost_at_timestamp(Timestamp) ->
	K = get_gb_cost_per_year_at_timestamp(Timestamp),
	get_perpetual_gb_cost(K, ?USD_PER_GBY_DECAY_ANNUAL).

-spec get_perpetual_gb_cost(Init::float(), Decay::float()) -> usd().
get_perpetual_gb_cost(Init, Decay) ->
	Init / - math:log(Decay).

%% @doc Return the cost in USD of storing 1 GB per year at the given time.
-spec get_gb_cost_per_year_at_timestamp(Timestamp::integer()) -> usd().
get_gb_cost_per_year_at_timestamp(Timestamp) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_gb_cost_per_year_at_datetime(Datetime).

%% @doc Return the cost in USD of storing 1 GB per average block time at the given time.
-spec get_gb_cost_per_block_at_timestamp(Timestamp::integer()) -> usd().
get_gb_cost_per_block_at_timestamp(Timestamp) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_gb_cost_per_block_at_datetime(Datetime).

%% @doc Return the cost in USD of storing 1 GB per year.
-spec get_gb_cost_per_year_at_datetime(datetime()) -> usd().
get_gb_cost_per_year_at_datetime({{Y, M, _}, _} = DT) ->
	PrevY = prev_jun_30_year(Y, M),
	NextY = next_jun_30_year(Y, M),
	FracY = fraction_of_year(PrevY, NextY, DT),
	PrevYCost = usd_p_gby(PrevY),
	NextYCost = usd_p_gby(NextY),
	CY = PrevYCost - (FracY * (PrevYCost - NextYCost)),
	CY * ?N_REPLICATIONS.

prev_jun_30_year(Y, M) when M < 7 ->
	Y - 1;
prev_jun_30_year(Y, _M) ->
	Y.

next_jun_30_year(Y, M) when M < 7 ->
	Y;
next_jun_30_year(Y, _M) ->
	Y + 1.

%% @doc Return the cost in USD of storing 1 GB per average block time.
-spec get_gb_cost_per_block_at_datetime(datetime()) -> usd().
get_gb_cost_per_block_at_datetime(DT) ->
	get_gb_cost_per_year_at_datetime(DT) / ?BLOCKS_PER_YEAR.

%% @doc Return the cost in USD of storing 1 GB per year. Estmimated from empirical data.
%% Assumes a year after 2019 inclusive. Uses data figures for 2018 and 2019.
%% Extrapolates the exponential decay curve k*e^(-at) to future years.
%% @end
-spec usd_p_gby(nonegint()) -> usd().
usd_p_gby(2018) -> ?USD_PER_GBY_2018;
usd_p_gby(2019) -> ?USD_PER_GBY_2019;
usd_p_gby(Y) ->
	K = ?USD_PER_GBY_2019,
	A = math:log(?USD_PER_GBY_DECAY_ANNUAL),
	T = Y - 2019,
	K * math:exp(A * T).

%% @doc Return elapsed time as the fraction of the year
%% between Jun 30th of PrevY and Jun 30th of NextY.
%% @end
-spec fraction_of_year(nonegint(), nonegint(), datetime()) -> float().
fraction_of_year(PrevY, NextY, {{Y, Mo, D},{H, Mi, S}}) ->
	Start = calendar:datetime_to_gregorian_seconds({{PrevY, 6, 30}, {23, 59, 59}}),
	Now = calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, Mi, S}}),
	End = calendar:datetime_to_gregorian_seconds({{NextY, 6, 30}, {23, 59, 59}}),
	(Now - Start) / (End - Start).

%% TODO Use calendar:system_time_to_universal_time/2 in Erlang OTP-21.
system_time_to_universal_time(Time, TimeUnit) ->
	Seconds = erlang:convert_time_unit(Time, TimeUnit, seconds),
	DaysFrom0To1970 = 719528,
	SecondsPerDay = 86400,
	calendar:gregorian_seconds_to_datetime(Seconds + (DaysFrom0To1970 * SecondsPerDay)).

recalculate_usd_to_ar_rate2(#block{ height = PrevHeight } = B) ->
	case is_usd_to_ar_rate_adjustment_height(PrevHeight + 1) of
		false ->
			{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
		true ->
			recalculate_usd_to_ar_rate3(B)
	end.

is_usd_to_ar_rate_adjustment_height(Height) ->
	Height rem ?USD_TO_AR_ADJUSTMENT_FREQUENCY == 0.

recalculate_usd_to_ar_rate3(#block{ height = PrevHeight, diff = Diff } = B) ->
	Height = PrevHeight + 1,
	InitialDiff = ar_retarget:switch_to_linear_diff(?INITIAL_USD_TO_AR_DIFF(Height)()),
	MaxDiff = ar_mine:max_difficulty(),
	InitialRate = ?INITIAL_USD_TO_AR(Height),
	{Dividend, Divisor} = InitialRate,
	ScheduledRate = {Dividend * (MaxDiff - Diff), Divisor * (MaxDiff - InitialDiff)},
	{B#block.scheduled_usd_to_ar_rate, ScheduledRate}.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_gb_cost_per_year_at_datetime_is_monotone_test() ->
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
			CurrCost = get_gb_cost_per_year_at_datetime(CurrDT),
			PrevCost = get_gb_cost_per_year_at_datetime(PrevDT),
			?assert(CurrCost =< PrevCost),
			CurrDT
		end,
		InitialDT,
		FollowingDTs
	).
