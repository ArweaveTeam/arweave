%%% @doc The sync application's root supervisor. It creates the discovery
%%% caches (SYNC_BUCKET_CACHE_TABLE, CHUNK_INTERVAL_CACHE_TABLE) and the
%%% arweave_sync_state table, which live as long as the application.
%%% arweave_sync:activate/0 adds arweave_sync_runtime_sup as its only child.
-module(arweave_sync_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include_lib("arweave_sync/include/arweave_sync.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% The tables live here because this supervisor outlives the runtime
    %% workers.
    ets:new(
        ?SYNC_BUCKET_CACHE_TABLE,
        [ordered_set, public, named_table, {read_concurrency, true}]
    ),
    ets:new(
        ?CHUNK_INTERVAL_CACHE_TABLE,
        [ordered_set, public, named_table, {read_concurrency, true}]
    ),
    ets:new(arweave_sync_state, [named_table, public, set]),
    {ok, {{one_for_one, 5, 10}, []}}.
