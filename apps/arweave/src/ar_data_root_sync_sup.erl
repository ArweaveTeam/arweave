-module(ar_data_root_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% internal
-export([register_workers/0]).

-include("ar_sup.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
    Workers = register_workers(),
    {ok, {{one_for_one, 5, 10}, Workers}}.

register_workers() ->
    lists:map(
        fun(StorageModule) ->
            #store_info{id = StoreID} =
                arweave_storage:store_info(StorageModule),
            Name = ar_data_root_sync:name(StoreID),
            ?CHILD_WITH_ARGS(ar_data_root_sync, worker, Name, [StoreID])
        end,
        arweave_config:storage_modules()
    ).
