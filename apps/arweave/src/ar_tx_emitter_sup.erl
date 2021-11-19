-module(ar_tx_emitter_sup).

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
	Children = lists:map(
		fun(Num) ->
			Name = list_to_atom("ar_tx_emitter_worker_" ++ integer_to_list(Num)),
			{Name, {ar_tx_emitter_worker, start_link, [Name]}, permanent, 5000, worker,
					[ar_tx_emitter_worker]}
		end,
		lists:seq(1, Config#config.max_emitters)
	),
	Workers = [element(1, El) || El <- Children],
	Children2 = [{ar_tx_emitter, {ar_tx_emitter, start_link, [ar_tx_emitter, Workers]},
			permanent, 5000, worker, [ar_tx_emitter]} | Children],
	{ok, {{one_for_one, 5, 10}, Children2}}.
