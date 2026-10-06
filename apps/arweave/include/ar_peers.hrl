-ifndef(AR_PEERS_HRL).
-define(AR_PEERS_HRL, true).

-include_lib("ar.hrl").

%% The maximum number of peers to return from get_peers/0.
-define(MAX_PEER_DISCOVERY_LIST_LEN, 1000).

-record(performance, {
                     version = 5,
                     release = -1,
                     total_bytes = 0,
                     total_throughput = 0.0, %% bytes per millisecond
                     total_transfers = 0,
                     average_throughput = 0.0, %% bytes per millisecond
                     average_success = 1.0
                    }).

-endif.
