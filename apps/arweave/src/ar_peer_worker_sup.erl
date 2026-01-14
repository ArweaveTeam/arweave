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
	ets:new(ar_peer_worker, [set, public, named_table, {read_concurrency, true}]),
	ChildSpec = #{
		id => ar_peer_worker,
		start => {ar_peer_worker, start_link, []},
		restart => temporary,  %% Don't restart - will be recreated on demand
		shutdown => 5000,
		type => worker,
		modules => [ar_peer_worker]
	},
	{ok, {#{strategy => simple_one_for_one, intensity => 10, period => 60}, [ChildSpec]}}.

