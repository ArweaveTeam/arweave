-module(arweave_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include_lib("arweave_sync/include/arweave_sync.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% The application root outlives all pipeline and ingestion workers.
    arweave_sync_peer:create_ets(),
    %% Both discovery tables use {Mode, Location, Peer} keys.
    ets:new(
        ?SYNC_BUCKET_CACHE_TABLE,
        [ordered_set, public, named_table, {read_concurrency, true}]
    ),
    ets:new(
        ?CHUNK_INTERVAL_CACHE_TABLE,
        [ordered_set, public, named_table, {read_concurrency, true}]
    ),
    ets:new(arweave_sync_state, [named_table, public, set]),
    Children = [
        #{
            id => arweave_sync_runtime,
            start => {arweave_sync_runtime, start_link, []}
        }
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.
