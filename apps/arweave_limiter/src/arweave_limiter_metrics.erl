-module(arweave_limiter_metrics).

-export([register/0]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Declare Arweave Rate Limiter metrics.
register() ->
    ok = prometheus_histogram:new([
                                   {name, ar_limiter_response_time_microseconds},
                                   {help, "Time it took for the limiter to respond to requests"},
                                   %% buckets might be reduced for production
                                   {buckets, [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50]},
                                   {labels, [limiter_id]}]),

    ok = prometheus_counter:new([
                                 {name, ar_limiter_requests_total},
                                 {help, "The number of requests the limiter has processed"},
                                 {labels, [limiter_id]}]),
    ok = prometheus_counter:new([{name, ar_limiter_rejected_total},
                                 {help, "The number of request were rejected by the limiter"},
                                 {labels, [limiter_id, reason]}
                                ]),
    ok = prometheus_counter:new([{name, ar_limiter_reduce_requests_total},
                                 {help, "The number of reduce request by peer in total"},
                                 {labels, [limiter_id]}
                                ]),
    ok = prometheus_gauge:new([
                               {name, ar_limiter_peers},
                               {help, "The number of peers the limiter is monitoring currently"},
                               %% limiting type:
                               %% sliding_window -> baseline, leaky_bucket -> burst, concurrency -> concurrency
                               {labels, [limiter_id, limiting_type]}]),
    ok = prometheus_gauge:new([
                               {name, ar_limiter_tracked_items_total},
                               {help, "The number of timestamps, leaky tokens, concurrent processes are tracked"},
                               %% limiting type:
                               %% sliding_window -> baseline, leaky_bucket -> burst, concurrency -> concurrency
                               {labels, [limiter_id, limiting_type]}]),
    ok = prometheus_counter:new([
                                 {name, ar_limiter_leaky_ticks},
                                 {help, "The number of leaky bucket ticks the limiter has processed"},
                                 {labels, [limiter_id]}]),
    ok = prometheus_counter:new([
                                 {name, ar_limiter_leaky_tick_delete_peer_total},
                                 {help, "The number of times a peer has been dropped from the leaky bucket token register"},
                                 {labels, [limiter_id]}]),
    ok = prometheus_counter:new([
                                 {name, ar_limiter_cleanup_tick_expired_sliding_peers_deleted_total},
                                 {help, "The number of times a peer has been dropped from the sliding window timestamp register"},
                                 {labels, [limiter_id]}]),
    ok = prometheus_counter:new([
                                 %% To show how much tokens clients are burning for bursts.
                                 {name, ar_limiter_leaky_tick_token_reductions_total},
                                 {help, "All the consumed leaky bucket tokens that were reduced for all peers in total"},
                                 {labels, [limiter_id]}]),
    ok = prometheus_counter:new([
                                 %% To see how many peers bite into their burst tokens. (in a period)
                                 {name, ar_limiter_leaky_tick_reductions_peer},
                                 {help, "The times a leaky bucket token reduction had have to be performed for a peer"},
                                 {labels, [limiter_id]}]),
    ok.
