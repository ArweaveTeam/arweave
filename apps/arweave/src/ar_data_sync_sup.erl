-module(ar_data_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	Workers = lists:map(
		fun(Number) ->
			Name = list_to_atom("ar_data_sync_worker_" ++ integer_to_list(Number)),
			{Name, {ar_data_sync_worker, start_link, [Name]}, permanent, ?SHUTDOWN_TIMEOUT,
					worker, [Name]}
		end,
		lists:seq(1, Config#config.sync_jobs)
	),
	SyncWorkerNames = [element(1, El) || El <- Workers],
	SyncWorkerMaster = {ar_data_sync_worker_master, {ar_data_sync_worker_master, start_link,
			[ar_data_sync_worker_master, SyncWorkerNames]}, permanent, ?SHUTDOWN_TIMEOUT,
			worker, [ar_data_sync_worker_master]},
	Workers2 = Workers ++ [SyncWorkerMaster],
	StorageModuleWorkers = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			Name = list_to_atom("ar_data_sync_" ++ StoreID),
			{Name, {ar_data_sync, start_link, [Name, StoreID]}, permanent, ?SHUTDOWN_TIMEOUT,
					worker, [Name]}
		end,
		Config#config.storage_modules
	),
	DefaultStorageModuleWorker = {ar_data_sync_default, {ar_data_sync, start_link,
			[ar_data_sync_default, "default"]}, permanent, ?SHUTDOWN_TIMEOUT, worker,
			[ar_data_sync_default]},
	Workers3 = Workers2 ++ (StorageModuleWorkers ++ [DefaultStorageModuleWorker]),
	{ok, {{one_for_one, 5, 10}, Workers3}}.
