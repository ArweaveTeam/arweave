-module(ar_pricing_transition).

-export([get_transition_price/2, static_price/0, static_pricing_height/0, is_v2_pricing_height/1,
	transition_start_2_6_8/0, transition_start_2_7_2/0, transition_length_2_6_8/0,
	transition_length_2_7_2/0, transition_length/1
	]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_inflation.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

%% @doc This module encapsulates most of the complexity of our multi-phased pricing transition.
%%                                                     __
%%                                                   /    \__
%% V2 Pricing..................................__   /
%%                                           /|  \_/  
%%                                         /  |
%%                                       /    |
%%                                     /      |
%%                                   /        |
%%                                 /          |
%% 520 (cap).............________/            |
%%                     / |       |            |
%% 400 ______________/   |       |            |
%%       |           |   |       |            |
%%     today   2/20/24   3/7/24  11/20/24     11/20/26
%%               2.6.8   2.7.2   2.7.2        2.7.2
%%          Transition   HF      Transition   Transition
%%               Start           Start        End

%%%===================================================================
%%% Constants
%%%===================================================================

%% The number of blocks which have to pass since the 2.6.8 fork before we
%% start mixing in the new fee calculation method.
-ifdef(DEBUG).
	-define(PRICE_2_6_8_TRANSITION_START, 2).
-else.
	-ifndef(PRICE_2_6_8_TRANSITION_START).
		-ifdef(FORKS_RESET).
			-define(PRICE_2_6_8_TRANSITION_START, 0).
		-else.
			%% Target: February 20, 2024 at 2p UTC
			%% Fork 2.6.8 was published at: May 30, 2023 at 3:35p UTC
			%% Time between dates: 265 days, 22 hours, 25 minutes
			%% https://www.timeanddate.com/date/durationresult.html?m1=05&d1=30&y1=2023&m2=02&d2=20&y2=2024&h1=15&i1=35&s1=&h2=14&i2=&s2=
			%% In seconds: 22,976,700
			%% In blocks: 22,976,700 / 128s average block time = 179505
			%% Target block: 1189560 + 179505 = 1369065
			-define(PRICE_2_6_8_TRANSITION_START, 179505).
		-endif.
	-endif.
-endif.

%% The number of blocks following the 2.6.8 + ?PRICE_2_6_8_TRANSITION_START block
%% where the tx fee computation is transitioned to the new calculation method.
%% Let TransitionStart = fork 2.6.8 height + ?PRICE_2_6_8_TRANSITION_START.
%% Let A = height - TransitionStart + 1.
%% Let B = TransitionStart + ?PRICE_2_6_8_TRANSITION_BLOCKS - (height + 1).
%% Then price per GiB-minute = price old * B / (A + B) + price new * A / (A + B).
-ifdef(DEBUG).
	-define(PRICE_2_6_8_TRANSITION_BLOCKS, 2).
-else.
	-ifndef(PRICE_2_6_8_TRANSITION_BLOCKS).
		-ifdef(FORKS_RESET).
			-define(PRICE_2_6_8_TRANSITION_BLOCKS, 0).
		-else.
			-ifndef(PRICE_2_6_8_TRANSITION_BLOCKS).
				-define(PRICE_2_6_8_TRANSITION_BLOCKS, (30 * 24 * 30 * 18)). % ~18 months.
			-endif.
		-endif.
	-endif.
-endif.

%% The number of blocks which have to pass since the 2.6.8 fork before we
%% remove the price transition cap.
%%
%% Note: Even though this constant is related to the *2.7.2* fork we count the blocks
%% since the *2.6.8* fork for easier comparison with ?PRICE_2_6_8_TRANSITION_START
-ifdef(DEBUG).
	-define(PRICE_2_7_2_TRANSITION_START, 4).
-else.
	-ifndef(PRICE_2_7_2_TRANSITION_START).
		-ifdef(FORKS_RESET).
			-define(PRICE_2_7_2_TRANSITION_START, 0).
		-else.
			%% Target: November 20, 2024 at 2p UTC
			%% Fork 2.6.8 was published at: May 30, 2023 at 3:35p UTC
			%% Time between dates: 539 days, 22 hours, 25 minutes
			%% https://www.timeanddate.com/date/durationresult.html?m1=5&d1=30&y1=2023&m2=11&d2=20&y2=2024&h1=15&i1=35&s1=0&h2=14&i2=0&s2=0
			%% In seconds: 46,650,300
			%% In blocks: 46,650,300 / 128.9s average block time = 361910
			%% Target block: 1189560 + 361910 = 1551470
			-define(PRICE_2_7_2_TRANSITION_START, 361910).
		-endif.
	-endif.
-endif.

%% The number of blocks following the 2.6.8 + ?PRICE_2_7_2_TRANSITION_START block
%% where the tx fee computation is transitioned to the new calculation method.
%% Let TransitionStart = fork 2.6.8 height + ?PRICE_2_7_2_TRANSITION_START.
%% Let A = height - TransitionStart + 1.
%% Let B = TransitionStart + ?PRICE_2_7_2_TRANSITION_START - (height + 1).
%% Then price per GiB-minute = price cap * B / (A + B) + price new * A / (A + B).
-ifdef(DEBUG).
	-define(PRICE_2_7_2_TRANSITION_BLOCKS, 2).
-else.
	-ifndef(PRICE_2_7_2_TRANSITION_BLOCKS).
		-ifdef(FORKS_RESET).
			-define(PRICE_2_7_2_TRANSITION_BLOCKS, 0).
		-else.
			-ifndef(PRICE_2_7_2_TRANSITION_BLOCKS).
				-define(PRICE_2_7_2_TRANSITION_BLOCKS, (30 * 24 * 30 * 24)). % ~24 months.
			-endif.
		-endif.
	-endif.
-endif.

-ifdef(DEBUG).
	-define(PRICE_PER_GIB_MINUTE_PRE_TRANSITION, 8162).
-else.
	%% STATIC_2_6_8_FEE_WINSTON / (200 (years) * 365 (days) * 24 * 60) / 20 (replicas)
	%% = ~400 Winston per GiB per minute.
	-define(PRICE_PER_GIB_MINUTE_PRE_TRANSITION, 400).
-endif.

-ifdef(DEBUG).
	-define(PRICE_2_7_2_PER_GIB_MINUTE_CAP, 30000).
-else.
	%% 1_100_000_000_000 / (200 (years) * 365 (days) * 24 * 60) / 20 (replicas)
	%% = ~520 Winston per GiB per minute.
	-define(PRICE_2_7_2_PER_GIB_MINUTE_CAP, 520).
-endif.


%%%===================================================================
%%% Public Interface
%%%===================================================================

%% @doc There's a complex series of transition phases that we pass through as we move from
%% static pricing to dynamic pricing (aka v2 pricing). This function handles those phases.
get_transition_price(Height, V2Price) ->
	StaticPricingHeight = ar_pricing_transition:static_pricing_height(),
	PriceTransitionStart = transition_start(Height),
	PriceTransitionEnd = PriceTransitionStart + transition_length(Height),

	StartPrice = transition_start_price(Height),
	CapPrice = transition_cap(Height),

	case Height of
		_ when Height < StaticPricingHeight ->
			ar_pricing_transition:static_price();
		_ when Height < PriceTransitionEnd ->
			%% Interpolate between the pre-transition price and the new price.
			Interval1 = Height - PriceTransitionStart,
			Interval2 = PriceTransitionEnd - Height,
			InterpolatedPrice =
				(StartPrice * Interval2 + V2Price * Interval1) div (Interval1 + Interval2),
			PricePerGiBPerMinute = ar_util:between(InterpolatedPrice, 0, CapPrice),
			?LOG_DEBUG([{event, get_price_per_gib_minute},
				{height, Height}, {price1, StartPrice}, {price2, V2Price}, {cap, CapPrice},
				{transition_start, PriceTransitionStart}, {transition_end, PriceTransitionEnd},
				{interval1, Interval1}, {interval2, Interval2},
				{interpolated_price, InterpolatedPrice}, {price, PricePerGiBPerMinute}]),
			PricePerGiBPerMinute;
		_ ->
			V2Price
	end.

static_price() ->
	?PRICE_PER_GIB_MINUTE_PRE_TRANSITION.

%% @doc Height before which we use the hardcoded static price - no phase
%% of the pricing transition has started.
static_pricing_height() ->
	ar_pricing_transition:transition_start_2_6_8().

%% @doc Return true if the given height is a height where the transition to the
%% new pricing algorithm is complete.
is_v2_pricing_height(Height) ->
	Height >=
		ar_pricing_transition:transition_start_2_7_2() +
			ar_pricing_transition:transition_length_2_7_2().

transition_start_2_6_8() ->
	ar_fork:height_2_6_8() + ?PRICE_2_6_8_TRANSITION_START.

transition_start_2_7_2() ->
	%% Note: Even though this constant is related to the *2.7.2* fork we count the blocks
	%% since the *2.6.8* fork for easier comparison with ?PRICE_2_6_8_TRANSITION_START
	ar_fork:height_2_6_8() + ?PRICE_2_7_2_TRANSITION_START.

transition_length_2_6_8() ->
	?PRICE_2_6_8_TRANSITION_BLOCKS.

transition_length_2_7_2() ->
	?PRICE_2_7_2_TRANSITION_BLOCKS.

transition_length(Height) ->
	TransitionStart_2_7_2 = ar_pricing_transition:transition_start_2_7_2(),

	case Height of
		_ when Height >= TransitionStart_2_7_2 ->
			ar_pricing_transition:transition_length_2_7_2();
		_ ->
			ar_pricing_transition:transition_length_2_6_8()
	end.


%%%===================================================================
%%% Private functions
%%%===================================================================

transition_start(Height) ->
	TransitionStart_2_6_8 = ar_pricing_transition:transition_start_2_6_8(),
	TransitionStart_2_7_2 = ar_pricing_transition:transition_start_2_7_2(),
	
	%% There are 2 overlapping transition periods:
	%% 2.6.8 Transition Period:
	%% - Start: 2.6.8 + ?PRICE_2_6_8_TRANSITION_START
	%% - Length: 18 months
	%%
	%% 2.7.2 Transition Period:
	%% - Start: 2.6.8 + ?PRICE_2_7_2_TRANSITION_START
	%% - Length: 24 months
	%%
	%% The 2.7.2 transition period starts in the middle of the 2.6.8 transition period and 
	%% replaces it.
	case Height of
		_ when Height >= TransitionStart_2_7_2 ->
			TransitionStart_2_7_2;
		_ ->
			TransitionStart_2_6_8
	end.

transition_start_price(Height) ->
	TransitionStart_2_7_2 = ar_pricing_transition:transition_start_2_7_2(),

	case Height of
		_ when Height >= TransitionStart_2_7_2 ->
			?PRICE_2_7_2_PER_GIB_MINUTE_CAP;
		_ ->
			?PRICE_PER_GIB_MINUTE_PRE_TRANSITION
	end.

transition_cap(Height) ->
	TransitionStart_2_7_2 = ar_pricing_transition:transition_start_2_7_2(),
	Fork_2_7_2 = ar_fork:height_2_7_2(),
	
	case Height of
		_ when Height >= TransitionStart_2_7_2 ->
			infinity;
		_ when Height >= Fork_2_7_2 ->
			?PRICE_2_7_2_PER_GIB_MINUTE_CAP;
		_ ->
			infinity
	end.
