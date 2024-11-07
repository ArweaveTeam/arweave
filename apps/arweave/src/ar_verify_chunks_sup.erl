-module(ar_verify_chunks_sup).

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
	case Config#config.verify of
		false ->
			ignore;
		true ->
			Workers = lists:map(
				fun(StorageModule) ->
					StoreID = ar_storage_module:id(StorageModule),
					Label = ar_storage_module:label(StorageModule),
					Name = list_to_atom("ar_verify_chunks_" ++ Label),
					?CHILD_WITH_ARGS(ar_verify_chunks, worker, Name, [Name, StoreID])
				end,
				Config#config.storage_modules
			),
			{ok, {{one_for_one, 5, 10}, Workers}}
	end.
	
