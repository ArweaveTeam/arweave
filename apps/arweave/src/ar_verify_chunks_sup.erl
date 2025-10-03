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
	{ok, Config} = arweave_config:get_env(),
	case Config#config.verify of
		false ->
			ignore;
		_ ->
			Workers = lists:map(
				fun(StorageModule) ->
					StoreID = ar_storage_module:id(StorageModule),
					Name = ar_verify_chunks:name(StoreID),
					?CHILD_WITH_ARGS(ar_verify_chunks, worker, Name, [Name, StoreID])
				end,
				Config#config.storage_modules
			),
			Reporter = ?CHILD(ar_verify_chunks_reporter, worker),
			{ok, {{one_for_one, 5, 10}, [Reporter | Workers]}}
	end.
	
