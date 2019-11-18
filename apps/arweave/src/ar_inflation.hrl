-ifndef(AR_INFLATION_HRL).
-define(AR_INFLATION_HRL, true).

-include("ar.hrl").

%% How many blocks are created in a year?
-define(BLOCKS_PER_YEAR, (30 * 24 * 365)).
%% Default test error tolerance in percent.
-define(DEFAULT_TOLERANCE_PERCENT, 0.001).
%% Height at which the 1.5.0.0 fork takes affect.
-ifdef(DEBUG).
%% The inflation tests serve as a documentation of how rewards are computed. Therefore, we keep the
%% mainnet value in these tests. Other tests have FORK 1.6 height set to zero from now on.
-define(FORK_15_HEIGHT, 95000).
-else.
-define(FORK_15_HEIGHT, ?FORK_1_6).
-endif.
%% Height at which block rewards began.
-define(REWARD_START_HEIGHT, 65700).
%% @doc BLOCK_PER_YEAR macro prior to 1.5.0.0 release.
-define(PRE_15_BLOCK_PER_YEAR, 525600 / (?TARGET_TIME/60) ).

%% @doc The number of extra tokens to grant for blocks between the 1.5.0.0 release
%% and the end of year one.
-define(POST_15_Y1_EXTRA, 13275279633337).

-endif.
