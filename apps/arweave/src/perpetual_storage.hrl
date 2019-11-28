%% @doc Macros for perpetual storage calculations.

%% @doc Assumed number of replications in the long term.
-define(N_REPLICATIONS, 10).

%% @doc Decay rate of the storage cost in GB/year.
-define(USD_PER_GBY_DECAY_ANNUAL, 0.995). % i.e., 0.5% annual decay rate

%% @doc Figures taken from the date-GBh spreadsheet.
-define(USD_PER_GBY_2018, 0.001045).
-define(USD_PER_GBY_2019, 0.000925).

%% @doc Initial $/AR exchange rate.
-define(INITIAL_USD_PER_AR(Height), fun() ->
	Forks = {
		ar_fork:height_1_9()
	},
	case Forks of
		{Fork_1_9} when Height < Fork_1_9 ->
			1.5;
		_ ->
			1.2
	end
end).

%% @doc The difficulty of the network at the time when
%% the $/AR exchange rate was ?INITIAL_USD_PER_AR.
%% This is an old-style diff - needs to be converted
%% to the linear diff for price calculations.
-define(INITIAL_USD_PER_AR_DIFF(Height), fun() ->
	Forks = {
		ar_fork:height_1_9()
	},
	case Forks of
		{Fork_1_9} when Height < Fork_1_9 ->
			28;
		_ ->
			29
	end
end).

%% @doc The network height at the time when
%% the $/AR exchange rate was ?INITIAL_USD_PER_AR.
-define(INITIAL_USD_PER_AR_HEIGHT(Height), fun() ->
	Forks = {
		ar_fork:height_1_9()
	},
	case Forks of
		{Fork_1_9} when Height < Fork_1_9 ->
			ar_fork:height_1_8();
		{Fork_1_9} ->
			Fork_1_9
	end
end).

%% @doc Mining reward as a proportion of tx cost.
-define(MINING_REWARD_MULTIPLIER, 0.2).
