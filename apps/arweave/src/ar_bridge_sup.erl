-module(ar_bridge_sup).

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
	Children = lists:map(
		fun(Num) ->
			Name = list_to_atom("ar_block_propagation_worker" ++ integer_to_list(Num)),
			{Name, {ar_block_propagation_worker, start_link, [Name]}, permanent, 5000, worker,
					[ar_block_propagation_worker]}
		end,
		lists:seq(1, ?BLOCK_PROPAGATION_PARALLELIZATION)
	),
	Workers = [element(1, El) || El <- Children],
	Children2 = [{ar_bridge, {ar_bridge, start_link, [ar_bridge, Workers]},
			permanent, 5000, worker, [ar_bridge]} | Children],
	{ok, {{one_for_one, 5, 10}, Children2}}.
