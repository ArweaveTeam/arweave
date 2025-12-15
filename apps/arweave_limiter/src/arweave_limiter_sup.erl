-module(arweave_limiter_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, all_info/0]).

-ifdef(AR_TEST).
-export([start_link/1, child_spec/1, reset_all/0]).
-endif.

%% Supervisor callbacks
-export([init/1]).

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_sup.hrl").

-include_lib("kernel/include/logger.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
    start_link(get_limiter_config()).

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Config]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([Config]) ->
    ok = arweave_limiter_metrics:register(),
    {ok, {supervisor_spec(Config), children_spec(Config)}}.

supervisor_spec(_Config) ->
    #{ strategy => one_for_all,
       intensity => 5,
       period => 10 }.

%%--------------------------------------------------------------------
%% Child spec generation based on Config.
%%--------------------------------------------------------------------
children_spec(Configs) ->
    [child_spec(Config) || Config <- Configs].

child_spec(#{id := Id} = Config) ->
    #{ id => Id,
       start => {arweave_limiter_group, start_link, [Id, Config]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT}.

get_limiter_config() ->
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
       tick_reduction => Config#config.'http_api.limiter.chunk.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.data_sync_record.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.recent_hash_list_diff.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.block_index.leaky_tick_interval',
       concurrency_limit => Config#config.'http_api.limiter.block_index.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.block_index.is_manual_reduction_disabled'},

     #{id => wallet_list,
       sliding_window_limit => Config#config.'http_api.limiter.wallet_list.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.wallet_list.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.wallet_list.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.wallet_list.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.wallet_list.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.wallet_list.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.wallet_list.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.get_vdf.leaky_tick_interval',
       concurrency_limit => Config#config.'http_api.limiter.get_vdf.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.get_vdf.is_manual_reduction_disabled'},

     #{id => get_vdf_session,
       sliding_window_limit => Config#config.'http_api.limiter.get_vdf_session.sliding_window_limit',
       sliding_window_duration => Config#config.'http_api.limiter.get_vdf_session.sliding_window_duration',
       timestamp_cleanup_tick_ms =>
           Config#config.'http_api.limiter.get_vdf_session.sliding_window_timestamp_cleanup_interval',
       timestamp_cleanup_expiry =>
           Config#config.'http_api.limiter.get_vdf_session.sliding_window_timestamp_cleanup_expiry',
       leaky_rate_limit => Config#config.'http_api.limiter.get_vdf_session.leaky_limit',
       leaky_tick_ms => Config#config.'http_api.limiter.get_vdf_session.leaky_tick_interval',
       tick_reduction => Config#config.'http_api.limiter.get_vdf_session.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.get_previous_vdf_session.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.general.leaky_tick_interval',
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
       tick_reduction => Config#config.'http_api.limiter.metrics.leaky_tick_interval',
       concurrency_limit => Config#config.'http_api.limiter.metrics.concurrency_limit',
       is_manual_reduction_disabled => Config#config.'http_api.limiter.metrics.is_manual_reduction_disabled'},
     %% Local peers
     #{id => local_peers,
       no_limit => true}
    ].

all_info() ->
    Children = supervisor:which_children(?MODULE),
    [{Id, arweave_limiter_group:info(Id)}  || {Id, _Child, _Type, _Modules} <- Children].

reset_all() ->
    Children = supervisor:which_children(?MODULE),
    [{Id, arweave_limiter_group:reset_all(Id)}  || {Id, _Child, _Type, _Modules} <- Children].
