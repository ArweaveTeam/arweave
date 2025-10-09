-module(ar_sync_record_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
	ets:new(sync_records, [set, public, named_table, {read_concurrency, true}]),
	{ok, Config} = arweave_config:get_env(),
	ConfiguredWorkers = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			Label = ar_storage_module:label(StoreID),
			Name = list_to_atom("ar_sync_record_" ++ Label),
			?CHILD_WITH_ARGS(ar_sync_record, worker, Name, [Name, StoreID])
		end,
		Config#config.storage_modules
	),
	DefaultSyncRecordWorker = ?CHILD_WITH_ARGS(ar_sync_record, worker, ar_sync_record_default,
		[ar_sync_record_default, ?DEFAULT_MODULE]),
	RepackInPlaceWorkers = lists:map(
		fun({StorageModule, _Packing}) ->
			StoreID = ar_storage_module:id(StorageModule),
			Label = ar_storage_module:label(StoreID),
			Name = list_to_atom("ar_sync_record_" ++ Label),
			?CHILD_WITH_ARGS(ar_sync_record, worker, Name, [Name, StoreID])
		end,
		Config#config.repack_in_place_storage_modules
	),
	Workers = [DefaultSyncRecordWorker] ++ ConfiguredWorkers ++ RepackInPlaceWorkers,
	{ok, {{one_for_one, 5, 10}, Workers}}.
