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
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

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
	ets:new(ar_meta_db, [set, public, named_table, {read_concurrency, true}]),
	ets:new(blacklist, [set, public, named_table]),
	ets:new(ignored_ids, [bag, public, named_table]),
	ets:new(ar_tx_db, [set, public, named_table]),
	ets:new(ar_data_sync_state, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_data_sync, [ordered_set, public, named_table, {read_concurrency, true}]),
	ets:new(sync_records, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_tx_blacklist,
		[set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_tx_blacklist_pending_headers,
		[set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_tx_blacklist_pending_data,
		[set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_tx_blacklist_offsets,
		[ordered_set, public, named_table, {read_concurrency, true}]),
	ets:new(block_cache, [set, public, named_table]),
	ets:new(node_state, [set, public, named_table]),
	ets:new(ar_chunk_storage, [ordered_set, public, named_table, {read_concurrency, true}]),
	ets:new(chunk_storage_file_index, [set, public, named_table, {read_concurrency, true}]),
	ets:new(mining_state, [set, public, named_table, {read_concurrency, true}]),
	{ok, {{one_for_one, 5, 10}, [
		?CHILD(ar_disksup, worker),
		?CHILD(ar_meta_db, worker),
		?CHILD(ar_arql_db, worker),
		?CHILD(ar_events_sup, supervisor),
		?CHILD(ar_watchdog, worker),
		?CHILD(ar_tx_blacklist, worker),
		?CHILD(ar_bridge, worker),
		?CHILD(ar_tx_queue, worker),
		?CHILD(ar_sync_record, worker),
		?CHILD(ar_chunk_storage, worker),
		?CHILD(ar_header_sync, worker),
		?CHILD(ar_data_sync, worker),
		?CHILD(ar_node_sup, supervisor),
		?CHILD(ar_webhook_sup, supervisor),
		?CHILD(ar_poller, worker)
	]}}.
