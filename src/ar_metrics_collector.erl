-module(ar_metrics_collector).
-behaviour(prometheus_collector).
-export([deregister_cleanup/1,
         collect_mf/2]).


-import(prometheus_model_helpers, [create_mf/4]).


-include_lib("prometheus.hrl").
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
	[add_metric_family(Metric, Callback)
		|| Metric <- Metrics],
	ok.


%% called when collector deregistered
deregister_cleanup(_Registry) -> ok.

%% ===================================================================
%% Private functions
%% ===================================================================

add_metric_family({Name, Type, Help, Metrics}, Callback) ->
	Callback(create_mf(?METRIC_NAME(Name), Help, Type, Metrics)).

metrics() ->
	[
	 {block_height, gauge,
		"Block height.",
		% FIXME This metric should be updated by ar_node itself, not pulled like this
		length(ar_node:get_hash_list(whereis(http_entrypoint_node))) - 1 },
	 {peer_count, gauge,
		"peer count",
		% FIXME This metric should be updated by arb_ridge itself, not pulled like this
		length(ar_bridge:get_remote_peers(whereis(http_bridge_node)))},
	 {storage_blocks_stored, gauge,
		"Blocks stroed",
		ar_storage:blocks_on_disk()},
	 {arnode_queue_len, gauge,
		"Size of message queuee on ar_node",
		element(2, erlang:process_info(whereis(http_entrypoint_node), message_queue_len))},
	 {arbridge_queue_len, gauge,
		"Size of message queuee on ar_bridge",
		element(2, erlang:process_info(whereis(http_bridge_node), message_queue_len))},
	 {ignored_ids_len, gauge,
		"Size of table of Ignored/already seen IDs:", 
		ets:info(ignored_ids, size)}
	].
