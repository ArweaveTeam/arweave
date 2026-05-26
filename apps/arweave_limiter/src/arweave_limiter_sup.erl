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
    [
     #{id => chunk,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_CHUNK_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_CHUNK_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit => ?DEFAULT_HTTP_API_LIMITER_CHUNK_LEAKY_LIMIT,
       leaky_tick_ms => ?DEFAULT_HTTP_API_LIMITER_CHUNK_LEAKY_TICK_INTERVAL,
       tick_reduction => ?DEFAULT_HTTP_API_LIMITER_CHUNK_LEAKY_TICK_REDUCTION,
       concurrency_limit => ?DEFAULT_HTTP_API_LIMITER_CHUNK_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => data_sync_record,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_DATA_SYNC_RECORD_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_DATA_SYNC_RECORD_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit =>
           ?DEFAULT_HTTP_API_LIMITER_DATA_SYNC_RECORD_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_DATA_SYNC_RECORD_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_DATA_SYNC_RECORD_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => recent_hash_list_diff,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_RECENT_HASH_LIST_DIFF_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_RECENT_HASH_LIST_DIFF_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit =>
           ?DEFAULT_HTTP_API_LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_RECENT_HASH_LIST_DIFF_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_RECENT_HASH_LIST_DIFF_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => block_index,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_BLOCK_INDEX_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_BLOCK_INDEX_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit =>
           ?DEFAULT_HTTP_API_LIMITER_BLOCK_INDEX_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_BLOCK_INDEX_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_BLOCK_INDEX_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_BLOCK_INDEX_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => wallet_list,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_WALLET_LIST_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_WALLET_LIST_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit =>
           ?DEFAULT_HTTP_API_LIMITER_WALLET_LIST_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_WALLET_LIST_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_WALLET_LIST_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_WALLET_LIST_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => get_vdf,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit => ?DEFAULT_HTTP_API_LIMITER_GET_VDF_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => get_vdf_session,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SESSION_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SESSION_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SESSION_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SESSION_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SESSION_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_VDF_SESSION_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => get_previous_vdf_session,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_PREVIOUS_VDF_SESSION_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_GET_PREVIOUS_VDF_SESSION_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_GET_PREVIOUS_VDF_SESSION_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GET_PREVIOUS_VDF_SESSION_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => general,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GENERAL_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_GENERAL_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit => ?DEFAULT_HTTP_API_LIMITER_GENERAL_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_GENERAL_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_GENERAL_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_GENERAL_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},

     #{id => metrics,
       sliding_window_limit =>
           ?DEFAULT_HTTP_API_LIMITER_METRICS_SLIDING_WINDOW_LIMIT,
       sliding_window_duration =>
           ?DEFAULT_HTTP_API_LIMITER_METRICS_SLIDING_WINDOW_DURATION,
       timestamp_cleanup_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_INTERVAL,
       timestamp_cleanup_expiry =>
           ?DEFAULT_HTTP_API_LIMITER_TIMESTAMP_CLEANUP_EXPIRY,
       leaky_rate_limit => ?DEFAULT_HTTP_API_LIMITER_METRICS_LEAKY_LIMIT,
       leaky_tick_ms =>
           ?DEFAULT_HTTP_API_LIMITER_METRICS_LEAKY_TICK_INTERVAL,
       tick_reduction =>
           ?DEFAULT_HTTP_API_LIMITER_METRICS_LEAKY_TICK_REDUCTION,
       concurrency_limit =>
           ?DEFAULT_HTTP_API_LIMITER_METRICS_CONCURRENCY_LIMIT,
       is_manual_reduction_disabled =>
           ?DEFAULT_HTTP_API_LIMITER_IS_MANUAL_REDUCTION_DISABLED},
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
