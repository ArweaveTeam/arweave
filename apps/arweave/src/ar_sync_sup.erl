-module(ar_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include_lib("arweave/include/ar_sup.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [?CHILD(ar_sync_discovery, worker)]
        ++ ar_sync_scheduler:register_workers()
        ++ [?CHILD(ar_chunk_copy, worker)]
        ++ ar_sync_store_sweeper:register_workers(),
    {ok, {{one_for_all, 5, 10}, Children}}.
