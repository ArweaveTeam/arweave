-ifndef(AR_PEERS_HRL).
-define(AR_PEERS_HRL, true).

-include_lib("ar.hrl").

-record(performance, {
	version = 3,
	release = -1,
	total_bytes = 0,
	total_throughput = 0.0, %% bytes per millisecond
	total_transfers = 0,
	average_latency = 0.0, %% milliseconds
	average_throughput = 0.0, %% bytes per millisecond
	average_success = 1.0,
	lifetime_rating = 0, %% longer time window
	current_rating = 0 %% shorter time window
}).

-endif.