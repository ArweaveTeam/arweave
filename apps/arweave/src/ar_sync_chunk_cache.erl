%%% Cache reservations owned by one incarnation of a data-sync process.
-module(ar_sync_chunk_cache).

-export([init/1, reserve/1, release/0, reset/1, reset_store/1,
        size/0, size/1, sizes/0]).

%% @doc Reclaim the previous process's reservations before accepting new chunks.
init(StoreID) ->
    reset_store(StoreID),
    ets:insert(ar_data_sync_state, {{chunk_cache_size, self()}, 0}),
    ets:insert(ar_data_sync_state, {{chunk_cache_owner, StoreID}, self()}),
    ok.

%% @doc Stop handoffs to the previous process and discard its reservations.
reset_store(StoreID) ->
    %% Keep the owner mapping until init publishes its replacement. If init is
    %% interrupted, the next attempt can still identify the obsolete counter.
    case ets:lookup(ar_data_sync_state, {chunk_cache_owner, StoreID}) of
        [{_, OldPID}] -> reset(OldPID);
        [] -> ok
    end.

%% @doc Reserve a chunk and return the exact process that must receive it.
reserve(StoreID) ->
    case ets:lookup(ar_data_sync_state, {chunk_cache_owner, StoreID}) of
        [{_, PID}] -> do_reserve(PID);
        [] -> {error, not_initialized}
    end.

do_reserve(PID) ->
    %% Do not recreate a counter removed by restart cleanup.
    try ets:update_counter(ar_data_sync_state,
            {chunk_cache_size, PID}, {2, 1}) of
        _ -> {ok, PID}
    catch error:badarg ->
        {error, not_initialized}
    end.

%% @doc Release one reservation held by the calling data-sync process.
release() ->
    try ets:update_counter(ar_data_sync_state, {chunk_cache_size, self()},
            {2, -1, 0, 0}) of
        _ -> ok
    catch error:badarg ->
        %% A restart already reclaimed this process's reservations.
        ok
    end.

%% @doc Reclaim a stopped process's reservations, leaving its replacement alone.
reset(PID) ->
    ets:delete(ar_data_sync_state, {chunk_cache_size, PID}),
    ok.

%% @doc Sum the authoritative owner counters without a second mutable total.
size() ->
    lists:sum(ets:select(ar_data_sync_state,
        [{{{chunk_cache_size, '_'}, '$1'}, [], ['$1']}])).

%% @doc Return the reservations held by the current process for StoreID.
size(StoreID) ->
    case ets:lookup(ar_data_sync_state, {chunk_cache_owner, StoreID}) of
        [{_, PID}] -> owner_size(PID);
        [] -> 0
    end.

%% @doc Return the current reservation count for every initialized store.
sizes() ->
    [{StoreID, owner_size(PID)} || [StoreID, PID] <-
        ets:match(ar_data_sync_state, {{chunk_cache_owner, '$1'}, '$2'})].

owner_size(PID) ->
    case ets:lookup(ar_data_sync_state, {chunk_cache_size, PID}) of
        [{_, Size}] -> Size;
        [] -> 0
    end.
