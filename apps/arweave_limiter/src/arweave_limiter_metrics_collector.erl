-module(arweave_limiter_metrics_collector).

-behaviour(prometheus_collector).

-export([
	deregister_cleanup/1,
	collect_mf/2
]).

-ifdef(AR_TEST).
-export([
         metrics/0,
         tracked_items/1,
         peers/1
        ]).
-endif.

-import(prometheus_model_helpers, [create_mf/4]).

-include_lib("prometheus/include/prometheus.hrl").
-define(METRIC_NAME_PREFIX, "arweave_").

%% ===================================================================
%% API
%% ===================================================================

%% called to collect Metric Families
-spec collect_mf(_Registry, Callback) -> ok when
	_Registry :: prometheus_registry:registry(),
	Callback :: prometheus_collector:callback().
collect_mf(_Registry, Callback) ->
	Metrics = metrics(),
	[add_metric_family(Metric, Callback) || Metric <- Metrics],
	ok.

%% called when collector deregistered
deregister_cleanup(_Registry) -> ok.

%% ===================================================================
%% Private functions
%% ===================================================================

add_metric_family({Name, Type, Help, Metrics}, Callback) ->
	Callback(create_mf(?METRIC_NAME(Name), Help, Type, Metrics)).

metrics() ->
    AllInfo = arweave_limiter_sup:all_info(),
    [
     {ar_limiter_tracked_items_total, gauge, "tracked requests, timestamps, leaky tokens", tracked_items(AllInfo)},
     {ar_limiter_peers, gauge, "The number of peers the limiter is monitoring currently", peers(AllInfo)}
    ].

tracked_items(AllInfo) ->
    lists:foldl(fun tracked_items_info/2, [], AllInfo).

tracked_items_info({Id, Info}, Acc) ->
    SlidingTimestamps = count_sliding_timestamps(Info),
    Monitors = maps:get(concurrent_monitors, Info),
    LeakyPeers = maps:get(leaky_tokens, Info),
    Items = [
             {[{limiter_id, Id}, {limiting_type, concurrency}], maps:size(Monitors)},
             {[{limiter_id, Id}, {limiting_type, leaky_bucket_tokens}], maps:size(LeakyPeers)},
             {[{limiter_id, Id}, {limiting_type, sliding_window_timestamps}], SlidingTimestamps}
            ],
    Items ++ Acc.

count_sliding_timestamps(Info) ->
    SlidingTimestamps = maps:get(sliding_timestamps, Info),
    maps:fold(fun(_Peer, TimestampList, Acc) ->
                        length(TimestampList) + Acc
                end, 0, SlidingTimestamps).

peers(AllInfo) ->
    lists:foldl(fun peers_info/2, [], AllInfo).

peers_info({Id, Info}, Acc) ->
    ConcurrentRequests = maps:get(concurrent_requests, Info),
    LeakyPeers = maps:get(leaky_tokens, Info),
    SlidingPeers = maps:get(sliding_timestamps, Info),       
    Items = [
             {[{limiter_id, Id}, {limiting_type, concurrency}], maps:size(ConcurrentRequests)},
             {[{limiter_id, Id}, {limiting_type, leaky_bucket_tokens}], maps:size(LeakyPeers)},
             {[{limiter_id, Id}, {limiting_type, sliding_window_timestamps}], maps:size(SlidingPeers)}
            ],
    Items ++ Acc.
