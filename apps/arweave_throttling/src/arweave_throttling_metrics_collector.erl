-module(arweave_throttling_metrics_collector).

-behaviour(prometheus_collector).

-export([
	deregister_cleanup/1,
	collect_mf/2
]).

-ifdef(AR_TEST).
-export([
         metrics/0,
         peers/1
        ]).
-endif.

-import(prometheus_model_helpers, [create_mf/4]).

-include_lib("prometheus/include/prometheus.hrl").
-define(METRIC_NAME_PREFIX, "arweave_").

%% API

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

%% Private functions
add_metric_family({Name, Type, Help, Metrics}, Callback) ->
	Callback(create_mf(?METRIC_NAME(Name), Help, Type, Metrics)).

metrics() ->
    AllInfo = arweave_throttling_sup:all_info(),
    [
     {arweave_throttling_peers, gauge, "The number of peers the limiter is monitoring currently", peers(AllInfo)},
     {arweave_throttling_queued_requests, gauge, "The number of peers the limiter is monitoring currently", queued_requests(AllInfo)}
    ].

peers(AllInfo) ->
    lists:foldl(fun peers_info/2, [], AllInfo).

peers_info({Id, Info}, Acc) ->
    Peers = maps:get(peers, Info),
    [{[{group_id, Id}], Peers} | Acc].


queued_requests(AllInfo) ->
    lists:foldl(fun queued_requests_info/2, [], AllInfo).

queued_requests_info({Id, Info}, Acc) ->
    QueuedRequests = maps:get(queued, Info),
    [{[{group_id, Id}], QueuedRequests} | Acc].
