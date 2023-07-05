-ifndef(AR_PEERS_HRL).
-define(AR_PEERS_HRL, true).

-include_lib("ar.hrl").

-define(STARTING_LATENCY_EMA, 1000). %% initial value to avoid over-weighting the first response
-define(RATE_SUCCESS, 1).
-define(RATE_ERROR, 0).
-define(RATE_PENALTY, -1).

-define(AVAILABLE_METRICS, [overall, data_sync]). %% the performance metrics currently tracked
-define(AVAILABLE_SUCCESS_RATINGS, [?RATE_PENALTY, ?RATE_ERROR, ?RATE_SUCCESS]).

-record(performance, {
	version = 3,
	release = -1,
	bytes = 0,
	latency = ?STARTING_LATENCY_EMA,
	transfers = 0,
	success = 1.0,
	rating = 0
}).

-endif.