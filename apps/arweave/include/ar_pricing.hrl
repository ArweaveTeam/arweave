%%% @doc Pricing macros.

%% The base wallet generation fee in USD, defined as a fraction.
%% The amount in AR depends on the current difficulty and height.
-define(WALLET_GEN_FEE_USD, {1, 10}).

%% The target number of replications.
-define(N_REPLICATIONS, fun(MACRO_Height) ->
	MACRO_Forks = {
		ar_fork:height_2_5(),
		ar_fork:height_2_6()
	},
	case MACRO_Forks of
		{_MACRO_Fork_2_5, MACRO_Fork_2_6} when MACRO_Height >= MACRO_Fork_2_6 ->
			15;
		{MACRO_Fork_2_5, _MACRO_Fork_2_6} when MACRO_Height >= MACRO_Fork_2_5 ->
			45;
		_ ->
			10
	end
end).

%% An approximation of the natural logarithm of ?USD_PER_GBY_DECAY_ANNUAL (0.995),
%% expressed as a decimal fraction, with the precision of math:log.
-define(LN_USD_PER_GBY_DECAY_ANNUAL, {-5012541823544286, 1000000000000000000}).

%% Decay rate of the storage cost in GB/year, expressed as a decimal fraction.
-define(USD_PER_GBY_DECAY_ANNUAL, {995, 1000}). % 0.995, i.e., 0.5% annual decay rate.

%% The estimated historical price of storing 1 GB of data for the year 2018,
%% expressed as a decimal fraction.
-define(USD_PER_GBY_2018, {1045, 1000000}). % 0.001045

%% The estimated historical price of storing 1 GB of data for the year 2019,
%% expressed as a decimal fraction.
-define(USD_PER_GBY_2019, {925, 1000000}). % 0.000925

%% The precision of computing the natural exponent as a decimal fraction,
%% expressed as the maximal power of the argument in the Taylor series.
-define(TX_PRICE_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION, 9).

%% USD to AR exchange rates by height defined together with INITIAL_USD_TO_AR_HEIGHT
%% and INITIAL_USD_TO_AR_DIFF. The protocol uses these constants to estimate the
%% USD to AR rate at any block based on the change in the network difficulty and inflation
%% rewards.
-define(INITIAL_USD_TO_AR(Height), fun() ->
	Forks = {
		ar_fork:height_2_4(),
		ar_fork:height_2_5(),
		ar_fork:height_2_6(),
		ar_fork:height_2_7()
	},
	case Forks of
		{_Fork_2_4, _Fork_2_5, _Fork_2_6, Fork_2_7} when Height >= Fork_2_7 ->
			{1, 10};
		{_Fork_2_4, _Fork_2_5, Fork_2_6, _Fork_2_7} when Height >= Fork_2_6 ->
			{1, 10};
		{_Fork_2_4, Fork_2_5, _Fork_2_6, _Fork_2_7} when Height >= Fork_2_5 ->
			{1, 65};
		{Fork_2_4, _Fork_2_5, _Fork_2_6, _Fork_2_7} when Height >= Fork_2_4 ->
			?INITIAL_USD_TO_AR_PRE_FORK_2_5
	end
end).

%% The original USD to AR conversion rate, defined as a fraction. Set up at fork 2.4.
%% Used until the fork 2.5.
-define(INITIAL_USD_TO_AR_PRE_FORK_2_5, {1, 5}).

%% The network difficulty at the time when the USD to AR exchange rate was
%% ?INITIAL_USD_TO_AR(Height). Used to account for the change in the network
%% difficulty when estimating the new USD to AR rate.
-define(INITIAL_USD_TO_AR_DIFF(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2(),
		ar_fork:height_2_5(),
		ar_fork:height_2_6(),
		ar_fork:height_2_7()
	},
	case Forks of
		{_Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6, Fork_2_7} when Height >= Fork_2_7 ->
			32; % TODO
		{_Fork_1_9, _Fork_2_2, _Fork_2_5, Fork_2_6, _Fork_2_7} when Height >= Fork_2_6 ->
			32;
		{_Fork_1_9, _Fork_2_2, Fork_2_5, _Fork_2_6, _Fork_2_7} when Height >= Fork_2_5 ->
			32;
		{_Fork_1_9, Fork_2_2, _Fork_2_5, _Fork_2_6, _Fork_2_7} when Height >= Fork_2_2 ->
			34;
		{Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6, _Fork_2_7} when Height < Fork_1_9 ->
			28;
		_ ->
			29
	end
end).

%% The network height at the time when the USD to AR exchange rate was
%% ?INITIAL_USD_TO_AR(Height). Used to account for the change in inflation
%% rewards when estimating the new USD to AR rate.
-define(INITIAL_USD_TO_AR_HEIGHT(Height), fun() ->
	Forks = {
		ar_fork:height_1_9(),
		ar_fork:height_2_2(),
		ar_fork:height_2_5(),
		ar_fork:height_2_6(),
		ar_fork:height_2_7()
	},
	%% In case the fork heights are reset to 0 (e.g. on testnets),
	%% set the initial height to 1 - the height where the inflation
	%% emission essentially begins.
	case Forks of
		{_Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6, Fork_2_7} when Height >= Fork_2_7 ->
			max(Fork_2_7, 1);
		{_Fork_1_9, _Fork_2_2, _Fork_2_5, Fork_2_6, _Fork_2_7} when Height >= Fork_2_6 ->
			max(Fork_2_6, 1);
		{_Fork_1_9, _Fork_2_2, Fork_2_5, _Fork_2_6, _Fork_2_7} when Height >= Fork_2_5 ->
			max(Fork_2_5, 1);
		{_Fork_1_9, Fork_2_2, _Fork_2_5, _Fork_2_6, _Fork_2_7} when Height >= Fork_2_2 ->
			max(Fork_2_2, 1);
		{Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6, _Fork_2_7} when Height < Fork_1_9 ->
			max(ar_fork:height_1_8(), 1);
		{Fork_1_9, _Fork_2_2, _Fork_2_5, _Fork_2_6, _Fork_2_7} ->
			max(Fork_1_9, 1)
	end
end).

%% The USD to AR rate is re-estimated every so many blocks.
-ifdef(DEBUG).
-define(USD_TO_AR_ADJUSTMENT_FREQUENCY, 10).
-else.
-define(USD_TO_AR_ADJUSTMENT_FREQUENCY, 50).
-endif.

%% The largest possible multiplier for a one-step increase of the USD to AR Rate.
-define(USD_TO_AR_MAX_ADJUSTMENT_UP_MULTIPLIER, {1005, 1000}).

%% The largest possible multiplier for a one-step decrease of the USD to AR Rate.
-define(USD_TO_AR_MAX_ADJUSTMENT_DOWN_MULTIPLIER, {995, 1000}).

%% Reduce the USD to AR fraction if both the dividend and the devisor become bigger than this.
-ifdef(DEBUG).
-define(USD_TO_AR_FRACTION_REDUCTION_LIMIT, 100).
-else.
-define(USD_TO_AR_FRACTION_REDUCTION_LIMIT, 1000000).
-endif.

%% Mining reward as a proportion of the estimated transaction storage costs,
%% defined as a fraction.
-define(MINING_REWARD_MULTIPLIER, {2, 10}).

%% The USD to AR exchange rate for a new chain, e.g. a testnet.
-define(NEW_WEAVE_USD_TO_AR_RATE, ?INITIAL_USD_TO_AR_PRE_FORK_2_5).

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

%% Base wallet generation fee. Used until fork 2.2.
-define(WALLET_GEN_FEE, 250000000000).
