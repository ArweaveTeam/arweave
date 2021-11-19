-module(ar_tx_validator_sup).

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
			Name = list_to_atom("ar_tx_validator_worker_" ++ integer_to_list(Num)),
			{Name, {ar_tx_validator_worker, start_link, [Name]}, permanent, 5000, worker,
					[ar_tx_validator_worker]}
		end,
		lists:seq(1, Config#config.tx_validators)
	),
	Workers = [element(1, El) || El <- Children],
	Children2 = [{ar_tx_validator, {ar_tx_validator, start_link, [ar_tx_validator, Workers]},
			permanent, 5000, worker, [ar_tx_validator]} | Children],
	{ok, {{one_for_one, 5, 10}, Children2}}.
