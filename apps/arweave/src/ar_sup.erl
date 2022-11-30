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

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 30000, Type, [I]}).

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
	ets:new(ar_peers, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_storage, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_randomx_state_key_blocks, [set, public, named_table]),
	ets:new(ar_randomx_state_key_heights, [ordered_set, public, named_table]),
	ets:new(blacklist, [set, public, named_table]),
	ets:new(ignored_ids, [bag, public, named_table]),
	ets:new(ar_tx_emitter_recently_emitted, [set, public, named_table]),
	ets:new(ar_tx_db, [set, public, named_table]),
	ets:new(ar_packing_server, [set, public, named_table]),
	ets:new(ar_header_sync, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_data_discovery, [ordered_set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_data_sync_state, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_mining_server, [set, public, named_table]),
	ets:new(ar_global_sync_record, [set, public, named_table]),
	ets:new(ar_disk_pool_data_roots, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_data_sync_skip_intervals, [ordered_set, public, named_table,
			{read_concurrency, true}]),
	ets:new(sync_records, [set, public, named_table, {read_concurrency, true}]),
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
	ets:new(chunk_storage_file_index, [set, public, named_table, {read_concurrency, true}]),
	ets:new(mining_state, [set, public, named_table, {read_concurrency, true}]),
	{ok, {{one_for_one, 5, 10}, [
		?CHILD(ar_rate_limiter, worker),
		?CHILD(ar_disksup, worker),
		?CHILD(ar_events_sup, supervisor),
		?CHILD(ar_arql_db, worker),
		?CHILD(ar_storage, worker),
		?CHILD(ar_peers, worker),
		?CHILD(ar_disk_cache, worker),
		?CHILD(ar_watchdog, worker),
		?CHILD(ar_tx_blacklist, worker),
		?CHILD(ar_bridge_sup, supervisor),
		?CHILD(ar_packing_server, worker),
		?CHILD(ar_sync_record_sup, supervisor),
		?CHILD(ar_chunk_storage_sup, supervisor),
		?CHILD(ar_data_discovery, worker),
		?CHILD(ar_header_sync, worker),
		?CHILD(ar_data_sync_sup, supervisor),
		?CHILD(ar_global_sync_record, worker),
		?CHILD(ar_nonce_limiter, worker),
		?CHILD(ar_mining_server, worker),
		?CHILD(ar_tx_emitter_sup, supervisor),
		?CHILD(ar_block_pre_validator_sup, supervisor),
		?CHILD(ar_poller_sup, supervisor),
		?CHILD(ar_node_sup, supervisor),
		?CHILD(ar_webhook_sup, supervisor)
	]}}.
