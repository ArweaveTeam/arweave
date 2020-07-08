-module(ar_data_sync_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

%%%===================================================================
%%% Public API.
%%%===================================================================

start_link(Args) ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%%%===================================================================
%%% Supervisor callbacks.
%%%===================================================================

init(Args) ->
	SupFlags = #{strategy => one_for_one, intensity => 100, period => 60},
	ChildSpec = #{
		id => ar_data_sync,
		start => {ar_data_sync, start_link, [Args]},
		shutdown => infinity
	},
	{ok, {SupFlags, [ChildSpec]}}.
