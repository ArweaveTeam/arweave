-module(arweave_throttling_metrics).

-export([register/0]).

-ifdef(AR_TEST).
-export([cleanup/0]).
-endif.

-define(LONG_LATENCY_BUCKETS, [1, 5, 10, 25, 50, 100, 250, 500,
                               1_000, 5_000, 10_000, 50_000,
                               100_000, 500_000, 1_000_000,
                               5_000_000, 10_000_000, 50_000_000,
                               100_000_000, 500_000_000, 1_000_000_000]).
-define(SHORT_LATENCY_BUCKETS, [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50, 100, 500, 1000]).


%%% Public interface.

%% @doc Declare Arweave Client Throttling metrics.
register() ->
    %% Practical use: We want to monitor the performance of requests going through
    %%                the throttling logic.
    %%                We need to measure to latencies: time spent on calling the
    %%                worker process, and time spent queued. This provides a good
    %%                understanding where the requests spend their time if latency
    %%                increases.
    %%                We chose to measure the worker+queue latency, because that's
    %%                our #1 priority: how much latency we "introduce" in total.
    ok = prometheus_histogram:new(
        [{name, arweave_throttling_request_response_time_microseconds},
            {help, "Time request spent being throttled, including being routed "
                "to and calling the throttling group, and being queued."},
            %% buckets might be reduced for production
            {buckets, ?LONG_LATENCY_BUCKETS},
            {labels, [group_id]}]),
    ok = prometheus_histogram:new(
        [{name, arweave_throttling_worker_response_time_microseconds},
            {help, "Time it took for the throttling group worker process to "
                "respond to requests whether they can be executed right away or "
                "need to be queued"},
            %% buckets might be reduced for production
            {buckets, ?SHORT_LATENCY_BUCKETS},
            {labels, [group_id]}]),


    %% Practical use: is_throttled is a blocking call, as it needs an response about
    %%                the current internal state of the throttling process to determine
    %%                it's load.
    %%                We want to measure the performance, to see if it slows down peer sync.
    ok = prometheus_histogram:new(
        [{name, arweave_throttling_is_throttled_response_time_microseconds},
            {help, "Time it took for the throttling group worker to respond "
                "to is_throttled requests"},
            %% buckets might be reduced for production
            {buckets, ?SHORT_LATENCY_BUCKETS},
            {labels, [group_id]}]),

    %% Practical use: We count the requests, how many of them are queued,
    %%                and how many are timing out. This gives us an indication
    %%                of how over- or under-provisioned limiter/throttling quotas,
    %%                and whether that's the reason for performance degradation.
    ok = prometheus_counter:new(
        [{name, arweave_throttling_requests_total},
            {help, "The number of requests the throttling workers have processed"},
            {labels, [group_id]}]),
    ok = prometheus_counter:new(
        [{name, arweave_throttling_queued_total},
            {help, "The number of request were queued by throttling group workers"},
            {labels, [group_id]}
        ]),
    ok = prometheus_counter:new(
        [{name, arweave_throttling_requests_error},
            {help, "The number of request returned an error when calling the "
                "throttling logic. e.g.: timeouts"},
            {labels, [group_id, reason]}
        ]),

    %% Practical user: We monitor the rate of which we fail to update the
    %%                 throttling quotas with the information gained from
    %%                 response headers.
    %%                 This allows us to monitor how many peers with incompatible
    %%                 limiters is the arweave node connected to.
    %%                 It's not necessarily a problem to have update errors.
    ok = prometheus_counter:new(
        [{name, arweave_throttling_quota_update_requests},
            {help, "The number of quota_update requests made"},
            {labels, [group_id]}
        ]),
    ok = prometheus_counter:new(
        [{name, arweave_throttling_quota_update_error},
            {help, "The number of quota_update request that were rejected by "
                "the throttling logic. e.g.: missing headers, incompatible headers"},
            {labels, [group_id, reason]}
        ]),

    %% Practical use: To determine load profile: many peers, few request vs few peers
    %%                with high number of requests
    ok = prometheus_gauge:new(
        [{name, arweave_throttling_peers},
            {help, "The number of peers the throttling is monitoring currently"},
            {labels, [group_id]}]),

    %% Practical use: Beside the rate and ratio of requests queued, we want to monitor the
    %%                number of queued (hanging) requests to determine load profile.
    %% Examples: - large payload requests might increase memory use.
    %%           - a lot of small but usually high frequency requests might exhaust handler
    %%             process pool.
    ok = prometheus_gauge:new(
        [{name, arweave_throttling_queued_requests},
            {help, "The number of requests throttling groups have queued currently"},
            {labels, [group_id]}]),
    ok.

cleanup() ->
    prometheus_histogram:deregister(arweave_throttling_request_response_time_microseconds),
    prometheus_histogram:deregister(arweave_throttling_worker_response_time_microseconds),
    prometheus_histogram:deregister(arweave_throttling_is_throttled_response_time_microseconds),
    prometheus_counter:deregister(arweave_throttling_requests_total),
    prometheus_counter:deregister(arweave_throttling_queued_total),
    prometheus_counter:deregister(arweave_throttling_requests_error),
    prometheus_counter:deregister(arweave_throttling_quota_update_error),
    prometheus_counter:deregister(arweave_throttling_quota_update_requests),
    prometheus_gauge:deregister(arweave_throttling_peers),
    prometheus_gauge:deregister(arweave_throttling_queued_requests),
    ok.
