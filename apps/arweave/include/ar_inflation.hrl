-ifndef(AR_INFLATION_HRL).
-define(AR_INFLATION_HRL, true).

-include_lib("arweave/include/ar.hrl").

%% An estimation for the number of blocks produced in a year.
%% Note: I've confirmed that when TARGET_TIME = 120 the following equation is
%% exactly equal to `30 * 24 * 365` when executed within an Erlang shell (i.e. 262800).
-define(BLOCKS_PER_YEAR, ((60 * 60 * 24 * 365) div ?TARGET_TIME)).

%% An approximation of the natural logarithm of 2,
%% expressed as a decimal fraction, with the precision of math:log.
-define(LN2, {6931471805599453, 10000000000000000}).

%% The precision of computing the natural exponent as a decimal fraction,
%% expressed as the maximal power of the argument in the Taylor series.
-define(INFLATION_NATURAL_EXPONENT_DECIMAL_FRACTION_PRECISION, 24).

%% The tolerance used in the inflation schedule tests.
-define(DEFAULT_TOLERANCE_PERCENT, 0.001).

%% Height at which the 1.5.0.0 fork takes effect.
-ifdef(DEBUG).
%% The inflation tests serve as a documentation of how rewards are computed.
%% Therefore, we keep the mainnet value in these tests. Other tests have
%% FORK 1.6 height set to zero from now on.
-define(FORK_15_HEIGHT, 95000).
-else.
-define(FORK_15_HEIGHT, ?FORK_1_6).
-endif.

%% BLOCK_PER_YEAR macro prior to 1.5.0.0 release.
-define(PRE_15_BLOCK_PER_YEAR, 525600 / (?TARGET_TIME / 60) ).

%% The number of extra tokens to grant for blocks between the 1.5.0.0 release
%% and the end of year one.
-define(POST_15_Y1_EXTRA, 13275279633337).

-endif.
