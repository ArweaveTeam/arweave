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
	%% Largest multiple of 2 less than the number of schedulers (but no less than 2).
	%% The ar_mining_server will divide the workers into 2 groups - one for the current
	%% VDF session and one for the previous VDF session.
	NumMiningWorkers = max(2, 2 * trunc((erlang:system_info(schedulers_online) - 1) / 2)),
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
