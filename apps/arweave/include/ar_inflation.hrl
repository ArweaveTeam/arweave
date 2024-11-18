-ifndef(AR_INFLATION_HRL).
-define(AR_INFLATION_HRL, true).

-include_lib("arweave/include/ar.hrl").

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

%% Blocks per year prior to 1.5.0.0 release.
-define(PRE_15_BLOCKS_PER_YEAR, 525600 / (120 / 60)).

%% Blocks per year prior to 2.5.0.0 release.
-define(PRE_25_BLOCKS_PER_YEAR, (525600 / (120 / 60))).

%% The number of extra tokens to grant for blocks between the 1.5.0.0 release
%% and the end of year one.
%% 
%% calculate_post_15_y1_extra() ->
%%     Pre15 = erlang:trunc(sum_rewards(fun calculate/1, 0, ?FORK_15_HEIGHT)),
%%     Base = erlang:trunc(sum_rewards(fun calculate_base/1, 0, ?FORK_15_HEIGHT)),
%%     Post15Diff = Base - Pre15,
%%     erlang:trunc(Post15Diff / (?BLOCKS_PER_YEAR - ?FORK_15_HEIGHT)).
-define(POST_15_Y1_EXTRA, 13275279633337).

-endif.
