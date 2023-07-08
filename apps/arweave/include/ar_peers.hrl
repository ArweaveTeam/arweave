-ifndef(AR_PEERS_HRL).
-define(AR_PEERS_HRL, true).

-include_lib("ar.hrl").

%% factor to scale the average throughput by when rating gossiped data - lower is better
-define(GOSSIP_ADVANTAGE, 0.5). 

-record(performance, {
	version = 3,
	release = -1,
	average_bytes = 0.0,
	total_bytes = 0,
	average_latency = 0.0, %% milliseconds
	total_latency = 0.0, %% milliseconds
	transfers = 0,
	average_success = 1.0,
	rating = 0
}).

-endif.