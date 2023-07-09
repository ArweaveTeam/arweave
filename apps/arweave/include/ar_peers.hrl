-ifndef(AR_PEERS_HRL).
-define(AR_PEERS_HRL, true).

-include_lib("ar.hrl").

%% When rating gossiped data we don't have a latency value to use, so we'll credit the peer at
%% this rate - which is (currently) roughly equal to the latency of 20ms to fetch a 1250 byte
%% transaction. This is on the fast end so should generally be a boost to the peer's rating. We
%% can adjust this value to increase or decrease the incentive for gossiping data.
-define(GOSSIP_THROUGHPUT, 60). %% bytes per millisecond

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