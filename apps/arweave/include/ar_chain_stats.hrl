-ifndef(AR_CHAIN_STATS_HRL).
-define(AR_CHAIN_STATS_HRL, true).

-define(RECENT_FORKS_AGE, 60 * 60 * 24 * 30). %% last 30 days of forks
-define(RECENT_FORKS_LENGTH, 20). %% only return the last 20 fork

-record(fork, {
    id,
    height,
    timestamp,
    block_ids
}).

-endif.
