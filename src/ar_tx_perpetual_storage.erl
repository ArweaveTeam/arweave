-module(ar_tx_perpetual_storage).
-export([
	calculate_tx_fee/4, calculate_tx_reward/1,
	perpetual_cost_at_timestamp/1,
	calculate_tx_cost/4,
	usd_to_ar/3
]).
-include("ar.hrl").
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
	PerGB = usd_to_ar(perpetual_cost_at_timestamp(Timestamp), Diff, Height),
	StorageCost = PerGB * GBs,
	HashingCost = StorageCost,
	erlang:trunc(StorageCost + HashingCost).

%% @doc Mining reward for a TX of given cost.
calculate_tx_reward(TXCost) ->
	erlang:trunc(TXCost * ?MINING_REWARD_MULTIPLIER).

-spec usd_to_ar(USD::usd(), Diff::nonegint(), Height::nonegint()) -> winston().
usd_to_ar(USD, Diff, Height) ->
	InitialDiff = ar_retarget:switch_to_linear_diff(?INITIAL_USD_PER_AR_DIFF),
	DeltaP = case ar_fork:height_1_9() of
		H when Height >= H ->
			MaxDiff = ar_mine:max_difficulty(),
			(MaxDiff - InitialDiff) / (MaxDiff - Diff);
		_ ->
			Diff / InitialDiff
	end,
	InitialInflation = ar_inflation:calculate(?INITIAL_USD_PER_AR_HEIGHT),
	DeltaInflation = ar_inflation:calculate(Height) / InitialInflation,
	(USD * ?WINSTON_PER_AR * DeltaInflation) / (?INITIAL_USD_PER_AR * DeltaP).

%%%% Costs in USD

%% @doc Cost to store 1GB in the arweave perpetually.
%% Integral of the exponential decay curve k*e^(-at), i.e. k/a.
-spec perpetual_cost_at_timestamp(Timestamp::integer()) -> usd().
perpetual_cost_at_timestamp(Timestamp) ->
	K = get_cost_at_timestamp(Timestamp),
	perpetual_cost(K, ?USD_PER_GBY_DECAY_ANNUAL).

-spec perpetual_cost(Init::float(), Decay::float()) -> usd().
perpetual_cost(Init, Decay) ->
	Init / - math:log(Decay).

%% @doc Cost to store 1GB at the given time.
-spec get_cost_at_timestamp(Timestamp::integer()) -> usd().
get_cost_at_timestamp(Timestamp) ->
	Datetime = system_time_to_universal_time(Timestamp, seconds),
	get_cost_at_datetime(Datetime).

%% @doc $/GB-block, based on $/GB-year data.
-spec get_cost_at_datetime(datetime()) -> usd().
get_cost_at_datetime({{Y, _, _}, _} = DT) ->
	FracY = fraction_of_year(DT),
	PrevY = usd_p_gby(Y - 1),
	NextY = usd_p_gby(Y + 1),
	CY = PrevY - (FracY * (PrevY - NextY)),
	CY * ?N_REPLICATIONS.

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

%% @doc Returns day of the year.
-spec fraction_of_year(datetime()) -> float().
fraction_of_year({{Y, Mo, D},{H, Mi, S}}) ->
	Start = calendar:datetime_to_gregorian_seconds({{Y, 1, 1}, {0, 0, 0}}),
	Now = calendar:datetime_to_gregorian_seconds({{Y, Mo, D}, {H, Mi, S}}),
	End = calendar:datetime_to_gregorian_seconds({{Y + 1, 1, 1}, {0, 0, 0}}),
	(Now - Start) / (End - Start).

%% TODO Use calendar:system_time_to_universal_time/2 in Erlang OTP-21.
system_time_to_universal_time(Time, TimeUnit) ->
	Seconds = erlang:convert_time_unit(Time, TimeUnit, seconds),
	DaysFrom0To1970 = 719528,
	SecondsPerDay = 86400,
	calendar:gregorian_seconds_to_datetime(Seconds + (DaysFrom0To1970 * SecondsPerDay)).
