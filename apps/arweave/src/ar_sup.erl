%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

-module(ar_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    %% These ETS tables should belong to the supervisor.
    ets:new(ar_shutdown_manager, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_timer, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_peers, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_http, [set, public, named_table]),
    ets:new(ar_http_inflight,
            [set, public, named_table, {write_concurrency, true}, {read_concurrency, true}]),
    ets:new(ar_blacklist_middleware, [set, public, named_table]),
    ets:new(blacklist, [set, public, named_table]),
    ets:new(ignored_ids, [bag, public, named_table]),
    ets:new(ar_tx_emitter_recently_emitted, [set, public, named_table]),
    ets:new(ar_tx_db, [set, public, named_table]),
    ets:new(ar_nonce_limiter, [set, public, named_table]),
    ets:new(ar_nonce_limiter_server, [set, public, named_table]),
    ets:new(ar_header_sync, [set, public, named_table, {read_concurrency, true}]),
    %% ar_sync_discovery* tables moved to ar_data_sync_sup (the sup that
    %% owns the gen_server that uses them).
    ets:new(ar_data_sync_state, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_chunk_storage, [set, public, named_table]),
    ets:new(ar_entropy_storage, [set, public, named_table]),
    ets:new(ar_mining_stats, [set, public, named_table]),
    ets:new(entropy_generation_stats, [ordered_set, public, named_table]),
    ets:new(ar_global_sync_record, [set, public, named_table]),
    ets:new(ar_disk_pool_data_roots, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_disk_pool_chunks_cache, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_disk_pool_chunks_cache_reverse, [bag, public, named_table]),
    ets:new(ar_tx_blacklist, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_tx_blacklist_pending_headers,
            [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_tx_blacklist_pending_data,
            [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_tx_blacklist_offsets,
            [ordered_set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_tx_blacklist_pending_restore_headers,
            [ordered_set, public, named_table, {read_concurrency, true}]),
    ets:new(block_cache, [set, public, named_table]),
    ets:new(tx_prefixes, [bag, public, named_table]),
    ets:new(block_index, [ordered_set, public, named_table]),
    ets:new(node_state, [set, public, named_table]),
    ets:new(mining_state, [set, public, named_table, {read_concurrency, true}]),
    ets:new(ar_total_supply_cache, [set, public, named_table, {read_concurrency, true}]),
    %% `ar_process_sampler' sits right after `ar_shutdown_manager' so it is
    %% one of the last processes terminated on shutdown:. It keeps
    %% recording process metrics (e.g. long message queues) while the rest
    %% of the tree shuts down.
    Debug = arweave_config:get([debug]),
    DebugChildren = case Debug of
        true -> [?CHILD(ar_process_sampler, worker)];
        false -> []
    end,
    Children = [
        ?CHILD(ar_shutdown_manager, worker)
    ] ++ DebugChildren ++ [
        ?CHILD(ar_disksup, worker),
        ?CHILD_SUP(ar_events_sup, supervisor),
        ?CHILD_SUP(ar_http_sup, supervisor),
        ?CHILD_SUP(ar_kv_sup, supervisor),
        ?CHILD_SUP(ar_storage_sup, supervisor),
        ?CHILD(ar_peers, worker),
        ?CHILD(ar_disk_cache, worker),
        ?CHILD(ar_watchdog, worker),
        ?CHILD(ar_tx_blacklist, worker),
        ?CHILD_SUP(ar_bridge_sup, supervisor),
        ?CHILD_SUP(ar_packing_sup, supervisor),
        ?CHILD_SUP(ar_sync_record_sup, supervisor),
        ?CHILD(ar_header_sync, worker),
        %% ar_chunk_storage_sup -> ar_data_sync_sup -> ar_repack_sup start order
        %% is intentional.
        %% ar_chunk_storage_sup: no init-time dependencies on the other two;
        %% ar_data_sync_sup: opens the per-store RocksDB databases
        %% (`chunk_data_db', `tx_index', ...) in init. Its workers also call
        %% the ar_chunk_storage workers at runtime (put/get/cut/delete);
        %% starting ar_chunk_storage_sup first means it stops last (children
        %% stop in reverse start order), so during shutdown those calls
        %% target live workers instead of blocking against terminating ones;
        %% ar_repack_sup: reads those databases in init.
        ?CHILD_SUP(ar_chunk_storage_sup, supervisor),
        ?CHILD_SUP(ar_data_sync_sup, supervisor),
        ?CHILD_SUP(ar_repack_sup, supervisor),
        ?CHILD_SUP(ar_data_root_sync_sup, supervisor),
        ?CHILD_SUP(ar_verify_chunks_sup, supervisor),
        ?CHILD(ar_global_sync_record, worker),
        ?CHILD_SUP(ar_nonce_limiter_sup, supervisor),
        mining_sup(),
        ?CHILD(ar_coordination, worker),
        ?CHILD_SUP(ar_tx_emitter_sup, supervisor),
        ?CHILD(ar_tx_poller, worker),
        ?CHILD_SUP(ar_block_pre_validator_sup, supervisor),
        ?CHILD_SUP(ar_poller_sup, supervisor),
        ?CHILD_SUP(ar_webhook_sup, supervisor),
        ?CHILD(ar_pool, worker),
        ?CHILD(ar_pool_job_poller, worker),
        ?CHILD(ar_pool_cm_job_poller, worker),
        ?CHILD(ar_chain_stats, worker),
        ?CHILD_SUP(ar_node_sup, supervisor)
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.

-ifdef(LOCALNET).
mining_sup() ->
    ?CHILD_SUP(ar_localnet_mining_sup, supervisor).
-else.
mining_sup() ->
    ?CHILD_SUP(ar_mining_sup, supervisor).
-endif.
