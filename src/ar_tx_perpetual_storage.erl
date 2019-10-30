-module(ar_tx_perpetual_storage).
-export([
	calculate_tx_fee/4, calculate_tx_reward/1,
	perpetual_cost_at_timestamp/1,
	perpetual_cost_at_timestamp_pre_fork_1_9/1,
	get_cost_per_block_at_timestamp/1,
	calculate_tx_cost/4,
	usd_to_ar/3,
	get_cost_per_year_at_datetime/1
]).
-include("ar.hrl").
-include("ar_inflation.hrl").
-include("perpetual_storage.hrl").

%% types
-type nonegint() :: non_neg_integer().
-type winston() :: nonegint().
-type usd() :: float().
-type date() :: {nonegint(), nonegint(), nonegint()}.
-type time() :: {nonegint(), nonegint(), nonegint()}.
-type datetime() :: {date(), time()}.

%%%% Costs in Winston

%% @doc TX storage fee to the enduser.
calculate_tx_fee(DataSize, Diff, Height, Timestamp) ->
	TXCost = calculate_tx_cost(DataSize, Diff, Height, Timestamp),
	TXReward = calculate_tx_reward(TXCost),
	TXCost + TXReward.

%% @doc Perpetual storage cost to the network.
calculate_tx_cost(Bytes, Diff, Height, Timestamp) ->
	GBs = (?TX_SIZE_BASE + Bytes) / (1024 * 1024 * 1024),
	PerGB = case ar_fork:height_1_9() of
		ForkH when Height >= ForkH ->
			usd_to_ar(perpetual_cost_at_timestamp(Timestamp), Diff, Height);
		_ ->
			usd_to_ar(perpetual_cost_at_timestamp_pre_fork_1_9(Timestamp), Diff, Height)
	end,
	StorageCost = PerGB * GBs,
	HashingCost = StorageCost,
	erlang:trunc(StorageCost + HashingCost).

%% @doc Mining reward for a TX of given cost.
calculate_tx_reward(TXCost) ->
	erlang:trunc(TXCost * ?MINING_REWARD_MULTIPLIER).

-spec usd_to_ar(USD::usd(), Diff::nonegint(), Height::nonegint()) -> winston().
usd_to_ar(USD, Diff, Height) ->
	case ar_fork:height_1_9() of
		H when Height >= H ->
			usd_to_ar_post_fork_1_9(USD, Diff, Height);
		_ ->
			usd_to_ar_pre_fork_1_9(USD, Diff, Height)
	end.

usd_to_ar_post_fork_1_9(USD, Diff, Height) ->
	InitialDiff = ar_retarget:switch_to_linear_diff(?INITIAL_USD_PER_AR_DIFF(Height)()),
	MaxDiff = ar_mine:max_difficulty(),
	DeltaP = (MaxDiff - InitialDiff) / (MaxDiff - Diff),
	InitialInflation = ar_inflation:calculate(?INITIAL_USD_PER_AR_HEIGHT(Height)()),
	DeltaInflation = ar_inflation:calculate(Height) / InitialInflation,
	erlang:trunc(
		(USD * ?WINSTON_PER_AR * DeltaInflation) / (?INITIAL_USD_PER_AR(Height)() * DeltaP)
	).

%%%% Costs in USD

%% @doc Cost to store 1GB in the arweave perpetually.
%% Integral of the exponential decay curve k*e^(-at), i.e. k/a.
-spec perpetual_cost_at_timestamp(Timestamp::integer()) -> usd().
perpetual_cost_at_timestamp(Timestamp) ->
	K = get_cost_per_year_at_timestamp(Timestamp),
	perpetual_cost(K, ?USD_PER_GBY_DECAY_ANNUAL).

-spec perpetual_cost(Init::float(), Decay::float()) -> usd().
perpetual_cost(Init, Decay) ->
	Init / - math:log(Decay).

%% @doc Cost to store 1GB per year at the given time.
-spec get_cost_per_year_at_timestamp(Timestamp::integer()) -> usd().
get_cost_per_year_at_timestamp(Timestamp) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_cost_per_year_at_datetime(Datetime).

%% @doc Cost to store 1GB per block at the given time.
-spec get_cost_per_block_at_timestamp(Timestamp::integer()) -> usd().
get_cost_per_block_at_timestamp(Timestamp) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_cost_per_block_at_datetime(Datetime).

%% @doc $/GB-year, based on $/GB-year data.
-spec get_cost_per_year_at_datetime(datetime()) -> usd().
get_cost_per_year_at_datetime({{Y, M, _}, _} = DT) ->
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

%% @doc $/GB-block, based on $/GB-year data.
-spec get_cost_per_block_at_datetime(datetime()) -> usd().
get_cost_per_block_at_datetime(DT) ->
	get_cost_per_year_at_datetime(DT) / ?BLOCKS_PER_YEAR.

%% @doc $/GB-year taken (or estimated) from empirical data.
%% Assumes a year after 2019 inclusive. Uses data figures for 2018-2019.
%% Uses exponential decay curve k*e^(-at) for future years.
-spec usd_p_gby(nonegint()) -> usd().
usd_p_gby(2018) -> ?USD_PER_GBY_2018;
usd_p_gby(2019) -> ?USD_PER_GBY_2019;
usd_p_gby(Y) ->
	K = ?USD_PER_GBY_2019,
	A = math:log(?USD_PER_GBY_DECAY_ANNUAL),
	T = Y - 2019,
	K * math:exp(A * T).

%%%% Time functions

%% @doc Returns elapsed time as the fraction of the year
%% between Jun 30th of PrevY and Jun 30th of NextY.
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

%%%% Pre-fork 1.9 functions.

perpetual_cost_at_timestamp_pre_fork_1_9(Timestamp) ->
	K = get_cost_per_year_at_timestamp_pre_fork_1_9(Timestamp),
	perpetual_cost(K, ?USD_PER_GBY_DECAY_ANNUAL).

get_cost_per_year_at_timestamp_pre_fork_1_9(Timestamp) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_cost_per_year_at_datetime_pre_fork_1_9(Datetime).

get_cost_per_year_at_datetime_pre_fork_1_9({{Y, _, _}, _} = DT) ->
	FracY = fraction_of_year_pre_fork_1_9(DT),
	PrevY = usd_p_gby(Y - 1),
	NextY = usd_p_gby(Y + 1),
	CY = PrevY - (FracY * (PrevY - NextY)),
	CY * ?N_REPLICATIONS.

fraction_of_year_pre_fork_1_9({{Y, Mo, D},{H, Mi, S}}) ->
	Start = calendar:datetime_to_gregorian_seconds({{Y, 1, 1}, {0, 0, 0}}),
	Now = calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, Mi, S}}),
	End = calendar:datetime_to_gregorian_seconds({{Y + 1, 1, 1}, {0, 0, 0}}),
	(Now - Start) / (End - Start).

usd_to_ar_pre_fork_1_9(USD, Diff, Height) ->
	InitialDiff = ar_retarget:switch_to_linear_diff(?INITIAL_USD_PER_AR_DIFF(Height)()),
	DeltaP = Diff / InitialDiff,
	InitialInflation = ar_inflation:calculate(?INITIAL_USD_PER_AR_HEIGHT(Height)()),
	DeltaInflation = ar_inflation:calculate(Height) / InitialInflation,
	(USD * ?WINSTON_PER_AR * DeltaInflation) / (?INITIAL_USD_PER_AR(Height)() * DeltaP).
