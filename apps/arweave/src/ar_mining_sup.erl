-module(ar_mining_sup).

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
	NumStorageModules = length(Config#config.storage_modules),
	MaxWorkers = max(2, 2 * trunc((erlang:system_info(schedulers_online) - 1) / 2)),
	%% Ideally we have 2 workers per storage module - 1 for the current VDF session and one
	%% for the previous VDF session. However we don't want more workers than there are schedulers
	%% so limit to largest multiple of 2 less than the number of schedulers (but no less than 2).
	NumMiningWorkers = min(MaxWorkers, 2*NumStorageModules),
	?LOG_INFO([{event, ar_mining_sup}, {num_mining_workers, NumMiningWorkers}]),
	MiningWorkers = lists:map(
		fun(Number) ->
			Name = list_to_atom("ar_mining_worker_" ++ integer_to_list(Number)),
			?CHILD_WITH_ARGS(ar_mining_worker, worker, Name, [Name])
		end,
		lists:seq(1, NumMiningWorkers)
	),
	MiningWorkerNames = [element(1, El) || El <- MiningWorkers],
	Children = MiningWorkers ++ [
		?CHILD_WITH_ARGS(ar_mining_server, worker, ar_mining_server, [MiningWorkerNames]),
		?CHILD(ar_mining_hash, worker),
		?CHILD(ar_mining_io, worker),
		?CHILD(ar_mining_stats, worker)
	],
	{ok, {{one_for_one, 5, 10}, Children}}.
