-module(ar_data_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
	Children = 
		ar_data_sync_worker_master:register_workers() ++
		ar_chunk_copy:register_workers() ++
		ar_data_sync:register_workers(),
	{ok, {{one_for_one, 5, 10}, Children}}.
