-module(ar_storage_sup).

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
    ets:new(ar_storage, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_storage_module, [set, public, named_table]),
	{ok, {{one_for_one, 5, 10}, [
		?CHILD(ar_storage, worker),
		?CHILD(ar_device_lock, worker)
	]}}.
