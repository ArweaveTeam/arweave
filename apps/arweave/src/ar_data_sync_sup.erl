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
    %% Sync tables are owned by this supervisor so they survive child restarts.
    ok = ar_sync:create_ets(),
    %% ar_data_roots must start before any ar_data_sync_<StoreID> instance
    %% so the cast/call API is available when ar_data_sync's join/cut/
    %% add_tip_block handlers fire during early init.
    DataRoots = ?CHILD(ar_data_roots, worker),
    %% ar_disk_pool starts LAST so the disk-pool KV (opened by
    %% ar_data_sync_default's init_kv) is available during ar_disk_pool's init.
    DiskPool = ?CHILD(ar_disk_pool, worker),
    %% The network-sync subtree precedes the per-store data-sync processes
    %% that call its public API.
    SyncChildren = ar_sync:child_specs(),
    Children =
        [DataRoots]
        ++ SyncChildren
        ++ ar_data_sync:register_workers()
        ++ [DiskPool],
    {ok, {{one_for_one, 5, 10}, Children}}.
