-module(ar_data_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

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
	ConfiguredWorkers = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			Name = list_to_atom("ar_data_sync_" ++ StoreID),
			{Name, {ar_data_sync, start_link, [Name, StoreID]}, permanent, 30000, worker,
					[Name]}
		end,
		Config#config.storage_modules
	),
	Workers = [{ar_data_sync_default, {ar_data_sync, start_link,
			[ar_data_sync_default, "default"]}, permanent, 30000, worker,
			[ar_data_sync_default]} | ConfiguredWorkers],
	{ok, {{one_for_one, 5, 10}, Workers}}.
