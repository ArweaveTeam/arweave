-module(ar_data_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-include_lib("ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks.
%% ===================================================================

init([]) ->
	%% Peer worker supervisor must start before worker master
	PeerWorkerSup = #{
		id => ar_peer_worker_sup,
		start => {ar_peer_worker_sup, start_link, []},
		restart => permanent,
		shutdown => infinity,
		type => supervisor,
		modules => [ar_peer_worker_sup]
	},
	Children = 
		[PeerWorkerSup] ++
		ar_data_sync_coordinator:register_workers() ++
		ar_chunk_copy:register_workers() ++
		ar_data_sync:register_workers(),
	{ok, {{one_for_one, 5, 10}, Children}}.
