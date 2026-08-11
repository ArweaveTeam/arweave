-module(ar_metrics_collector).

-behaviour(prometheus_collector).

-export([
         deregister_cleanup/1,
         collect_mf/2
        ]).

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

%% @doc ar_header_sync owns this table and creates it after the metrics
%% endpoint is already up: a scrape during that boot window (or while
%% header sync restarts) must degrade to 0, not crash the whole render.
%% The lookup's own default only covers a missing key, not a missing
%% table.
synced_blocks() ->
    try
        ets:lookup_element(ar_header_sync, synced_blocks, 2, 0)
    catch error:badarg ->
        0
    end.

metrics() ->
    RanchInfo = ranch:info(),
    [
     {storage_blocks_stored, gauge,
      "Blocks stored",
      synced_blocks()},
     {arnode_queue_len, gauge,
      "Size of message queuee on ar_node_worker",
      arweave_util:message_queue_len(ar_node_worker)},
     {arbridge_queue_len, gauge,
      "Size of message queuee on ar_bridge",
      arweave_util:message_queue_len(ar_bridge)},
     {ar_storage_queue_len, gauge,
      "Size of message queue on ar_storage",
      arweave_util:message_queue_len(ar_storage)},
     {ignored_ids_len, gauge,
      "Size of table of Ignored/already seen IDs:",
      ets:info(ignored_ids, size)},
     {ar_data_discovery_bytes_total, gauge, "ar_data_discovery process memory",
      get_process_memory(ar_data_discovery)},
     {ar_node_worker_bytes_total, gauge, "ar_node_worker process memory",
      get_process_memory(ar_node_worker)},
     {ar_header_sync_bytes_total, gauge, "ar_header_sync process memory",
      get_process_memory(ar_header_sync)},
     {ar_wallets_bytes_total, gauge,
      "Total account tree memory: the ar_account_tree process heap (the diff DAG) plus "
      "the ETS table holding the tree itself.",
      get_process_memory(ar_account_tree) + account_tree_ets_bytes()},
     {account_tree_ets_bytes, gauge,
      "Memory of the ETS table holding the account tree",
      account_tree_ets_bytes()},
     {ar_http_iface_listener_ranch_max_connections, gauge, "Maximum number of Ranch connections",
      get_ranch_max_connections(RanchInfo, ar_http_iface_listener)},
     {ar_http_iface_listener_ranch_active_connections, gauge, "Currently active Ranch connections",
      get_ranch_active_connections(RanchInfo, ar_http_iface_listener)}
    ].

account_tree_ets_bytes() ->
    case ets:info(ar_patricia_tree, memory) of
        undefined ->
            0;
        Words ->
            Words * erlang:system_info(wordsize)
    end.

get_process_memory(Name) ->
    case whereis(Name) of
        undefined ->
            0;
        PID ->
            {memory, Memory} = erlang:process_info(PID, memory),
            Memory
    end.

get_ranch_max_connections(RInfo, Name) ->
    get_ranch_info_value(RInfo, Name, max_connections).

get_ranch_active_connections(RInfo, Name) ->
    get_ranch_info_value(RInfo, Name, active_connections).

get_ranch_info_value(RInfo, Name, Key) ->
    PoolDetails = proplists:get_value(Name, RInfo, []),
    %% Signal error condition with -1
    proplists:get_value(Key, PoolDetails, -1).
