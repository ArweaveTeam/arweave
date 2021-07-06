-module(ar_sync_record_sup).

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
			Name = list_to_atom("ar_sync_record_" ++ StoreID),
			{Name, {ar_sync_record, start_link, [Name, StoreID]}, permanent, 30000, worker,
					[Name]}
		end,
		Config#config.storage_modules
	),
	Workers = [{ar_sync_record_default, {ar_sync_record, start_link,
			[ar_sync_record_default, "default"]}, permanent, 30000, worker,
			[ar_sync_record_default]} | ConfiguredWorkers],
	{ok, {{one_for_one, 5, 10}, Workers}}.
