%% @doc Macros for perpetual storage calculations.

%% @doc Assumed number of replications in the long term.
-define(N_REPLICATIONS, 10).

%% @doc Decay rate of the storage cost in GB/year.
-define(USD_PER_GBY_DECAY_ANNUAL, 0.995). % i.e., 0.5% annual decay rate

%% @doc Figures taken from the date-GBh spreadsheet.
-define(USD_PER_GBY_2018, 0.001045).
-define(USD_PER_GBY_2019, 0.000925).

%% @doc Initial $/AR exchange rate.
-ifdef(DEBUG).
%% Use a smaller rate in tests to decrease burden so that
%% there is space for testing the pooling mechanics before
%% reward becomes bigger than the burden.
-define(INITIAL_USD_PER_AR, 0.1).
-else.
-define(INITIAL_USD_PER_AR, 1.5).
-endif.

%% @doc The difficulty of the network at the time when
%% the $/AR exchange rate was ?INITIAL_USD_PER_AR.
%% This is an old-style diff - needs to be converted
%% to the linear diff for price calculations.
-define(INITIAL_USD_PER_AR_DIFF, 28).

%% @doc The network height at the time when
%% the $/AR exchange rate was ?INITIAL_USD_PER_AR.
-define(INITIAL_USD_PER_AR_HEIGHT, ar_fork:height_1_8()).

%% @doc Mining reward as a proportion of tx cost.
-define(MINING_REWARD_MULTIPLIER, 0.2).

%% @doc Byte size of the TX headers, tags allowance, etc.
-define(TX_SIZE_BASE, 3210).
