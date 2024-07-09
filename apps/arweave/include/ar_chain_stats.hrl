-ifndef(AR_CHAIN_STATS_HRL).
-define(AR_CHAIN_STATS_HRL, true).

-record(fork, {
    id,
    height,
    timestamp,
    block_ids
}).

-endif.
