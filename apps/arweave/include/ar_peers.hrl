-ifndef(AR_PEERS_HRL).
-define(AR_PEERS_HRL, true).

-include_lib("ar.hrl").

-define(STARTING_LATENCY_EMA, 1000). %% initial value to avoid over-weighting the first response

-record(overall_performance, {
	bytes = 0,
	latency = ?STARTING_LATENCY_EMA,
	transfers = 0,
	success = 1.0,
	rating = 0
}).

-record(sync_performance, {
	bytes = 0,
	latency = ?STARTING_LATENCY_EMA,
	transfers = 0,
	success = 1.0,
	max_active = 0,
	rating = 0
}).

-record(performance, {
	version = 3,
	release = -1,
	overall = #overall_performance{},
	sync = #sync_performance{}
}).

-endif.