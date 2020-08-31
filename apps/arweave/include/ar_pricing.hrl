%%% @doc Pricing macros.

%% The original USD to AR conversion rate, defined as a fraction. Set up at fork 2.4.
-define(USD_TO_AR_INITIAL_RATE, {1, 5}).

%% @deprecated Base wallet generation fee. Used until fork 2.2.
-define(WALLET_GEN_FEE, 250000000000).

%% The base wallet generation fee in USD, defined as a fraction.
%% The amount in AR depends on the current difficulty and height.
-define(WALLET_GEN_FEE_USD, {1, 10}).

%% Assumed number of replications in the long term.
-define(N_REPLICATIONS, 10).

%% Decay rate of the storage cost in GB/year.
-define(USD_PER_GBY_DECAY_ANNUAL, 0.995). % I.e., 0.5% annual decay rate.

%% The estimated historical price of storing 1GB of data for a year.
-define(USD_PER_GBY_2018, 0.001045).
-define(USD_PER_GBY_2019, 0.000925).

%% Initial $/AR exchange rate. Used until the fork 2.4.
-define(INITIAL_USD_PER_AR(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2()
	},
	case Forks of
		{Fork_1_9, _Fork_2_2} when Height < Fork_1_9 ->
			1.5;
		{_Fork_1_9, Fork_2_2} when Height >= Fork_2_2 ->
			4;
		_ ->
			1.2
	end
end).

%% The difficulty of the network at the time when
%% the $/AR exchange rate was ?INITIAL_USD_PER_AR.
%% This is an old-style diff - needs to be converted
%% to the linear diff in price calculations.
-define(INITIAL_USD_PER_AR_DIFF(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2()
	},
	case Forks of
		{Fork_1_9, _Fork_2_2} when Height < Fork_1_9 ->
			28;
		{_Fork_1_9, Fork_2_2} when Height >= Fork_2_2 ->
			34;
		_ ->
			29
	end
end).

%% @doc The network height at the time when
%% the $/AR exchange rate was ?INITIAL_USD_PER_AR.
-define(INITIAL_USD_PER_AR_HEIGHT(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2()
	},
	case Forks of
		{Fork_1_9, _Fork_2_2} when Height < Fork_1_9 ->
			ar_fork:height_1_8();
		{_Fork_1_9, Fork_2_2} when Height >= Fork_2_2 ->
			Fork_2_2;
		{Fork_1_9, _Fork_2_2} ->
			Fork_1_9
	end
end).

%% @doc Mining reward as a proportion of tx cost.
-define(MINING_REWARD_MULTIPLIER, 0.2).

%% @doc How much harder it should be to mine each
%% subsequent alternative POA option.
-define(ALTERNATIVE_POA_DIFF_MULTIPLIER, 2).
