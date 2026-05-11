-module(arweave_limiter_config).

-export([get_config/0, get_config/1, get_config/2, get_number_of_workers/1]).

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% These are static low traffic endpoints, always operate on a single instance.
-define(LOCAL_PEER_WORKERS, 1).
-define(METRICS_WORKERS, 1).

get_config() ->
    {ok, Config} = arweave_config:get_env(),
    [
     #{id => chunk,
       sliding_window_limit => Config#config.'http_api.limiter.chunk.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.chunk.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.chunk.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.chunk.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.chunk.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.chunk.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.chunk.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.chunk.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.chunk.is_manual_reduction_disabled'},

     #{id => data_sync_record,
       sliding_window_limit => Config#config.'http_api.limiter.data_sync_record.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.data_sync_record.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.data_sync_record.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.data_sync_record.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.data_sync_record.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.data_sync_record.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.data_sync_record.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.data_sync_record.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.data_sync_record.is_manual_reduction_disabled'},

     #{id => recent_hash_list_diff,
       sliding_window_limit => Config#config.'http_api.limiter.recent_hash_list_diff.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.recent_hash_list_diff.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.recent_hash_list_diff.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.recent_hash_list_diff.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.recent_hash_list_diff.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.recent_hash_list_diff.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.recent_hash_list_diff.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.recent_hash_list_diff.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.recent_hash_list_diff.is_manual_reduction_disabled'},

     #{id => block_index,
       sliding_window_limit => Config#config.'http_api.limiter.block_index.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.block_index.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.block_index.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.block_index.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.block_index.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.block_index.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.block_index.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.block_index.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.block_index.is_manual_reduction_disabled'},

     #{id => wallet_list,
       number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
       sliding_window_limit => Config#config.'http_api.limiter.wallet_list.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.wallet_list.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.wallet_list.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.wallet_list.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.wallet_list.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.wallet_list.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.wallet_list.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.wallet_list.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.wallet_list.is_manual_reduction_disabled'},

     #{id => get_vdf,
       sliding_window_limit => Config#config.'http_api.limiter.get_vdf.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.get_vdf.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.get_vdf.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.get_vdf.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.get_vdf.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.get_vdf.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.get_vdf.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.get_vdf.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.get_vdf.is_manual_reduction_disabled'},

     #{id => get_vdf_session,
       number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
       sliding_window_limit => Config#config.'http_api.limiter.get_vdf_session.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.get_vdf_session.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.get_vdf_session.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.get_vdf_session.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.get_vdf_session.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.get_vdf_session.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.get_vdf_session.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.get_vdf_session.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.get_vdf_session.is_manual_reduction_disabled'},

     #{id => get_previous_vdf_session,
       sliding_window_limit => Config#config.'http_api.limiter.get_previous_vdf_session.sliding_window_limit',
       sliding_window_duration =>
           Config#config.'http_api.limiter.get_previous_vdf_session.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.get_previous_vdf_session.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.get_previous_vdf_session.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.get_previous_vdf_session.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.get_previous_vdf_session.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.get_previous_vdf_session.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.get_previous_vdf_session.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.get_previous_vdf_session.is_manual_reduction_disabled'},

     #{id => general,
       sliding_window_limit => Config#config.'http_api.limiter.general.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.general.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.general.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.general.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.general.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.general.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.general.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.general.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.general.is_manual_reduction_disabled'},

     #{id => metrics,
       sliding_window_limit => Config#config.'http_api.limiter.metrics.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.metrics.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.metrics.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.metrics.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.metrics.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.metrics.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.metrics.leaky_tick_reduction',
       concurrency_limit => Config#config.'http_api.limiter.metrics.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.metrics.is_manual_reduction_disabled'},
     %% Local peers
     #{id => local_peers,
       number_of_workers => ?LOCAL_PEER_WORKERS,
       no_limit => true}
    ].

get_config(Id) when is_atom(Id) ->
    %% Removing the ?MODULE macro will break some tests.
    case lists:search(fun(#{id := ConfigId}) -> Id == ConfigId end, ?MODULE:get_config()) of
        {value, Config} -> Config;
        false -> {error, {limiter_config, not_found, Id}}
    end.

get_config(Id, Key) ->
    case get_config(Id) of 
        {error, _} = E ->
            E;
        Config ->
            try maps:get(Key, Config) of
                Value ->
                    Value
            catch _E:_R ->
                    %% Config is always a map, we construct it above.
                    {error, {limiter_config, key_not_found, Id, Key}}
            end
    end.

get_number_of_workers(local_peers) -> ?LOCAL_PEER_WORKERS;
get_number_of_workers(metrics) -> ?METRICS_WORKERS;
get_number_of_workers(_) -> ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS.

%% TESTS
get_id_test() ->
    ?assertMatch(#{id := metrics}, get_config(metrics)),
    ?assertMatch(#{id := general}, get_config(general)),
    ?assertMatch(#{id := chunk}, get_config(chunk)),
    ?assertMatch(#{id := data_sync_record}, get_config(data_sync_record)),
    ?assertMatch({error, {limiter_config, not_found, doesnt_exist}},
                 get_config(doesnt_exist)),
    ok.

get_id_key_test() ->
    {ok, Config} = arweave_config:get_env(),
    ChunkSlidingLimit = Config#config.'http_api.limiter.chunk.sliding_window_limit',
    ?assertMatch(ChunkSlidingLimit,
                 get_config(chunk, sliding_window_limit)),
    ?assertMatch({error, {limiter_config, not_found, doesnt_exist}},
                 get_config(doesnt_exist, some_key)),
    ?assertMatch({error, {limiter_config, key_not_found, metrics, some_key}},
                 get_config(metrics, some_key)),
    ok.
