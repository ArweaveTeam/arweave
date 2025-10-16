-module(ar_data_root_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% internal
-export([register_workers/0]).

-include("ar_sup.hrl").
-include("ar_config.hrl").

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
	{ok, Config} = application:get_env(arweave, config),
	lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),
			Name = list_to_atom("ar_data_root_sync_" ++ ar_storage_module:label(StoreID)),
			?CHILD_WITH_ARGS(ar_data_root_sync, worker, Name, [StoreID])
		end,
		Config#config.storage_modules
	).