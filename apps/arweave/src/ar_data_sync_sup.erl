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
	SyncWorkers = case ar_data_sync_worker_master:is_syncing_enabled() of
		true ->
			Workers = lists:map(
				fun(Number) ->
					Name = list_to_atom("ar_data_sync_worker_" ++ integer_to_list(Number)),
					?CHILD_WITH_ARGS(ar_data_sync_worker, worker, Name, [Name])
				end,
				lists:seq(1, Config#config.sync_jobs)
			),
			SyncWorkerNames = [element(1, El) || El <- Workers],
			SyncWorkerMaster = ?CHILD_WITH_ARGS(
				ar_data_sync_worker_master, worker, ar_data_sync_worker_master,
				[SyncWorkerNames]),
			Workers ++ [SyncWorkerMaster];
		false ->
			[]
	end,
	StorageModuleWorkers = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			Name = list_to_atom("ar_data_sync_" ++ StoreID),
			?CHILD_WITH_ARGS(ar_data_sync, worker, Name, [Name, {StoreID, none}])
		end,
		Config#config.storage_modules
	),
	DefaultStorageModuleWorker = ?CHILD_WITH_ARGS(ar_data_sync, worker,
		ar_data_sync_default, [ar_data_sync_default, {"default", none}]),
	RepackInPlaceWorkers = lists:map(
		fun({StorageModule, TargetPacking}) ->
			StoreID = ar_storage_module:id(StorageModule),
			Name = list_to_atom("ar_data_sync_" ++ StoreID),
			?CHILD_WITH_ARGS(ar_data_sync, worker, Name, [Name, {StoreID, TargetPacking}])
		end,
		Config#config.repack_in_place_storage_modules
	),
	Children = SyncWorkers ++ StorageModuleWorkers ++ [DefaultStorageModuleWorker]
			++ RepackInPlaceWorkers,
	{ok, {{one_for_one, 5, 10}, Children}}.
