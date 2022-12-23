-module(ar_poller_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_config.hrl").

%%%===================================================================
%%% Public API.
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
			Name = list_to_atom("ar_poller_worker_" ++ integer_to_list(Num)),
			{Name, {ar_poller_worker, start_link, [Name]}, permanent, ?SHUTDOWN_TIMEOUT,
					worker, [ar_poller_worker]}
		end,
		lists:seq(1, Config#config.block_pollers)
	),
	Workers = [element(1, El) || El <- Children],
	Children2 = [{ar_poller, {ar_poller, start_link, [ar_poller, Workers]},
			permanent, ?SHUTDOWN_TIMEOUT, worker, [ar_poller]} | Children],
	{ok, {{one_for_one, 5, 10}, Children2}}.
