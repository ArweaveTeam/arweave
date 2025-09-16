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
	MaxEmitters = arweave_config:get(max_emitters),
	Workers = lists:map(fun tx_workers/1, lists:seq(1, MaxEmitters)),
	WorkerNames = [ Name || #{ id := Name } <- Workers],
	Emitter = tx_emitter([ar_tx_emitter, WorkerNames]),
	ChildrenSpec = [Emitter|Workers],
	{ok, {supervisor_spec(), ChildrenSpec}}.

supervisor_spec() ->
	#{ strategy => one_for_one
	 , intensity => 5
	 , period => 10
	 }.

% helper to create ar_tx_emitter process, in charge
% of sending chunk to propagate to ar_tx_emitter_worker.
tx_emitter(Args) ->
	#{ id => ar_tx_emitter
	 , type => worker
	 , start => {ar_tx_emitter, start_link, Args}
	 , shutdown => ?SHUTDOWN_TIMEOUT
	 , modules => [ar_tx_emitter]
	 , restart => permanent
	 }.

% helper function to create ar_tx_workers processes.
tx_workers(Num) ->
	Name = "ar_tx_emitter_worker_" ++ integer_to_list(Num),
	Atom = list_to_atom(Name),
	#{ id => Atom
	 , start => {ar_tx_emitter_worker, start_link, [Atom]}
	 , restart => permanent
	 , type => worker
	 , timeout => ?SHUTDOWN_TIMEOUT
	 , modules => [ar_tx_emitter_worker]
	 }.
