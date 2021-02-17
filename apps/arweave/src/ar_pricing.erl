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
-include_lib("arweave/include/ar_mine.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Types.
%%%===================================================================

-type nonegint() :: non_neg_integer().
-type decimal() :: {integer(), integer()}.
-type usd() :: float() | decimal().
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
			get_perpetual_gb_cost_at_timestamp(Timestamp, Height),
			Rate,
			Height
		),
	StorageCost = max(1, PerpetualGBStorageCost div (1024 * 1024 * 1024)) * Size,
	HashingCost = StorageCost,
	MaintenanceCost = StorageCost + HashingCost,
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

%% @doc Calculate the transaction fee.
get_tx_fee_pre_fork_2_4(Size, Diff, Height, Timestamp) ->
	GBs = (?TX_SIZE_BASE + Size) / (1024 * 1024 * 1024),
	true = Height >= ar_fork:height_2_0(),
	PerGB =
		usd_to_ar_pre_fork_2_4(
			get_perpetual_gb_cost_at_timestamp(Timestamp, Height),
			Diff,
			Height
		),
	StorageCost = PerGB * GBs,
	HashingCost = StorageCost,
	MaintenanceCost = erlang:trunc(StorageCost + HashingCost),
	MinerFeeShare = get_miner_fee_share(MaintenanceCost, Height),
	MaintenanceCost + MinerFeeShare.

%% @doc Return the miner reward and the new endowment pool.
get_miner_reward_and_endowment_pool_pre_fork_2_4({Pool, TXs, unclaimed, _, _, _, _}) ->
	{0, Pool + lists:sum([TX#tx.reward || TX <- TXs])};
get_miner_reward_and_endowment_pool_pre_fork_2_4(Args) ->
	{Pool, TXs, _RewardAddr, WeaveSize, Height, Diff, Timestamp} = Args,
	true = Height >= ar_fork:height_2_0(),
	Inflation = trunc(ar_inflation:calculate(Height)),
	{PoolFeeShare, MinerFeeShare} = distribute_transaction_fees(TXs, Height),
	BaseReward = Inflation + MinerFeeShare,
	StorageCostPerGBPerBlock =
		usd_to_ar_pre_fork_2_4(
			get_gb_cost_per_block_at_timestamp(Timestamp, Height),
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
	InitialDiff =
		ar_retarget:switch_to_linear_diff_pre_fork_2_4(?INITIAL_USD_TO_AR_DIFF(Height)()),
	MaxDiff = ?MAX_DIFF,
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
	distribute_transaction_fees(TXs, EndowmentPool + TXFee - MinerFee, Miner + MinerFee, Height).

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
			{LnDecayDividend, LnDecayDivisor} = ?LN_USD_PER_GBY_DECAY_ANNUAL,
			{InitDividend, InitDivisor} = Init,
			{-InitDividend * LnDecayDivisor, InitDivisor * LnDecayDividend};
		false ->
			{Dividend, Divisor} = ?USD_PER_GBY_DECAY_ANNUAL,
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
				?N_REPLICATIONS
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
			CY * ?N_REPLICATIONS
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
			{Dividend, Divisor * ?BLOCKS_PER_YEAR};
		false ->
			get_gb_cost_per_year_at_datetime(DT, Height) / ?BLOCKS_PER_YEAR
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
			{ADividend, ADivisor} = ?LN_USD_PER_GBY_DECAY_ANNUAL,
			T = Y - 2019,
			P = ?TX_PRICE_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION,
			{EDividend, EDivisor} = ar_decimal:natural_exponent({ADividend * T, ADivisor}, P),
			{EDividend * KDividend, EDivisor * KDivisor};	
		false ->
			{Dividend, Divisor} = ?USD_PER_GBY_2019,
			K = Dividend / Divisor,
			{DecayDividend, DecayDivisor} = ?USD_PER_GBY_DECAY_ANNUAL,
			A = math:log(DecayDividend / DecayDivisor),
			T = Y - 2019,
			K * math:exp(A * T)
	end.

%% @doc Return elapsed time as the fraction of the year
%% between Jun 30th of PrevY and Jun 30th of NextY.
%% @end
-spec fraction_of_year(nonegint(), nonegint(), datetime(), nonegint()) -> float() | decimal().
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
	MaxDiff = ?MAX_DIFF,
	InitialRate = ?INITIAL_USD_TO_AR(Height),
	{Dividend, Divisor} = InitialRate,
	ScheduledRate = {Dividend * (MaxDiff - Diff), Divisor * (MaxDiff - InitialDiff)},
	{B#block.scheduled_usd_to_ar_rate, ScheduledRate}.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_gb_cost_per_year_at_datetime_is_monotone_test_() ->
	[
		ar_test_fork:test_on_fork(
			height_2_5, infinity, fun test_get_gb_cost_per_year_at_datetime_is_monotone/0
		) | [
			ar_test_fork:test_on_fork(
				height_2_5, Height, fun test_get_gb_cost_per_year_at_datetime_is_monotone/0
			)
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
