-module(arweave_client_throttling_metrics).

-export([register/0]).

%%% Public interface.

%% @doc Declare Arweave Client Throttling metrics.
register() ->
    ok = prometheus_histogram:new(
           [{name, arweave_client_throttling_response_time_microseconds},
            {help, "Time it took for the limiter to respond to requests"},
            %% buckets might be reduced for production
            {buckets, [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100, 500, 1000]},
            {labels, [group_id]}]),

    ok = prometheus_counter:new(
           [{name, arweave_client_throttling_requests_total},
            {help, "The number of requests the limiter has processed"},
            {labels, [group_id]}]),
    ok = prometheus_counter:new(
           [{name, arweave_client_throttling_queued_total},
            {help, "The number of request were rejected by the limiter"},
            {labels, [group_id, reason]}
           ]),
    ok = prometheus_counter:new(
           [{name, arweave_client_throttling_requests_error},
            {help, "The number of request were rejected by the limiter"},
            {labels, [group_id, reason]}
           ]),

    ok = prometheus_gauge:new(
           [{name, arweave_client_throttling_peers},
            {help, "The number of peers the limiter is monitoring currently"},
            %% limiting type:
            %% sliding_window -> baseline, leaky_bucket -> burst, concurrency -> concurrency
            {labels, [group_id]}]),
    ok.
