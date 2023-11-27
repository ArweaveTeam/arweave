-module(ar_tx_emitter_sup).

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
	Workers = lists:map(
		fun(Num) ->
			Name = list_to_atom("ar_tx_emitter_worker_" ++ integer_to_list(Num)),
			{Name, {ar_tx_emitter_worker, start_link, [Name]}, permanent, ?SHUTDOWN_TIMEOUT,
					worker, [ar_tx_emitter_worker]}
		end,
		lists:seq(1, Config#config.max_emitters)
	),
	WorkerNames = [element(1, El) || El <- Workers],
	Children = [
		?CHILD_WITH_ARGS(ar_tx_emitter, worker, ar_tx_emitter, [ar_tx_emitter, WorkerNames]) | 
		Workers
	],
	{ok, {{one_for_one, 5, 10}, Children}}.
