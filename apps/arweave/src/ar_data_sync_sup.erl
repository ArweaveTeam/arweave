-module(ar_data_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_sup.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
	%% ETS tables owned by this subtree. Created here (sup convention) so they
	%% survive child restarts.
	ets:new(?WORKER_LOAD_TABLE,
		[named_table, public, set,
			{read_concurrency, true}, {write_concurrency, true}]),
	%% ar_data_discovery's tables. Created unconditionally so callers
	%% that read them (e.g. ar_peer_sync's get_bucket_peers) don't crash
	%% on missing-table when sync_jobs=0 — they'll just see empty tables.
	ets:new(ar_data_discovery,
		[ordered_set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_data_discovery_footprint_buckets,
		[ordered_set, public, named_table, {read_concurrency, true}]),
	%% ordered_set (not set) so wipe_peer_cache_rows/1 and
	%% peer_last_cache_write/1 can range-scan by Peer prefix instead of
	%% scanning the whole table. The compound key is {Peer, CacheKey, Mode},
	%% Peer leading, so {Peer, '_', '_'} match-specs get O(log N + k).
	ets:new(ar_data_discovery_peer_intervals,
		[ordered_set, public, named_table,
			{read_concurrency, true}, {write_concurrency, true}]),
	%% Peer worker supervisor must start before worker master
	PeerWorkerSup = #{
		id => ar_peer_worker_sup,
		start => {ar_peer_worker_sup, start_link, []},
		restart => permanent,
		shutdown => infinity,
		type => supervisor,
		modules => [ar_peer_worker_sup]
	},
	%% ar_data_roots must start before any ar_data_sync_<StoreID> instance
	%% so the cast/call API is available when ar_data_sync's join/cut/
	%% add_tip_block handlers fire during early init.
	DataRoots = ?CHILD(ar_data_roots, worker),
	%% ar_disk_pool starts LAST so the disk-pool KV (opened by
	%% ar_data_sync_default's init_kv) is available during ar_disk_pool's init.
	DiskPool = ?CHILD(ar_disk_pool, worker),
	%% ar_data_discovery is the peer-coverage scanner that feeds
	%% ar_peer_sync's enqueue decisions. Self-opts-out via `ignore' from
	%% init/1 when sync_jobs=0. Must start BEFORE ar_peer_sync.
	DataDiscovery = ?CHILD(ar_data_discovery, worker),
	Children =
		[PeerWorkerSup, DataRoots, DataDiscovery] ++
		ar_data_sync_coordinator:register_workers() ++
		ar_chunk_copy:register_workers() ++
		%% ar_peer_sync is the per-StoreID network-sync gen_server (owns
		%% the task queue, the enqueue pass state, and the device lock).
		%% Must start BEFORE ar_data_sync's per-StoreID gen_servers so
		%% the API is callable from their init and from the chunk_copy
		%% completion handler.
		ar_peer_sync:register_workers() ++
		ar_data_sync:register_workers() ++
		[DiskPool],
	{ok, {{one_for_one, 5, 10}, Children}}.
