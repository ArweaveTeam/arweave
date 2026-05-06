%%% @doc Supervisor for ar_peer_worker processes.
%%% Uses simple_one_for_one to dynamically spawn peer workers on demand.
-module(ar_peer_worker_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks.
%%%===================================================================

init([]) ->
	%% Per-peer admission control is purely a sync concept; opt out of
	%% the supervision tree entirely when sync_jobs=0.
	case ar_data_sync_coordinator:is_syncing_enabled() of
		false ->
			ignore;
		true ->
			ets:new(ar_peer_worker,
					[set, public, named_table, {read_concurrency, true}]),
			ChildSpec = #{
				id => ar_peer_worker,
				start => {ar_peer_worker, start_link, []},
				restart => temporary,  %% Don't restart - recreated on demand
				shutdown => 5000,
				type => worker,
				modules => [ar_peer_worker]
			},
			{ok, {#{strategy => simple_one_for_one, intensity => 10,
					period => 60}, [ChildSpec]}}
	end.

