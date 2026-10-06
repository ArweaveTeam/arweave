%%% @doc The supervisor for the processes that run while sync is active:
%%% discovery, the scheduler, and one chunk writer and one sweeper per storage
%%% module. arweave_sync:activate/0 starts it and arweave_sync:deactivate/0
%%% stops it. A crash in any of them restarts them all: the scheduler starts
%%% over with no claims, and later sweeps find the chunks that were in flight.
-module(arweave_sync_runtime_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children =
        [?CHILD(arweave_sync_discovery, worker)] ++
            arweave_sync_chunk_writer:register_workers() ++
            arweave_sync_scheduler:register_workers() ++
            arweave_sync_sweeper:register_workers(),
    {ok, {{one_for_all, 5, 10}, Children}}.
