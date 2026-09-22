%%% @doc Transient network-sync fetch worker.
%%%
%%% One process per dispatched task, `spawn_monitor`'d by `arweave_sync_scheduler`.
%%% It makes at most one chunk request and hands the result to sync ingestion
%%% for validation and host storage. The scheduler keeps the task claim active until that handoff
%%% reaches a terminal state.
%%%
%%% Exit contract (read by the dispatcher's `'DOWN'` handler): `normal` covers
%%% both a successful request and a fetch failure; only a genuine crash exits
%%% abnormally. Unfilled bytes in the chunk-sized claim are rediscovered by a later
%%% store sweep.
%%%
%%% `arweave_sync_scheduler' checks chunk-cache and disk capacity before spawn. The
%%% worker atomically reserves shared memory before its request because another
%%% producer may consume capacity after selection.
-module(arweave_sync_fetch_worker).

-export([run/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

%%%===================================================================
%%% Entry point.
%%%===================================================================

%% @doc Fetch from the task's offset and rate the peer by the bytes it delivered.
run(#task{peer = Peer} = Task) ->
    %% Fetch timing uses the scheduler's clock. timer:tc remains
    %% the wall-clock input to ar_peers.
    {ElapsedUs, {Result, BytesFetched, FetchTiming}} =
        timer:tc(fun() -> fetch_task(Task) end),
    arweave_sync_scheduler:report_fetch_completed(
        Task#task.task_ref,
        BytesFetched,
        FetchTiming
    ),
    rate_peer(Peer, Result, ElapsedUs, BytesFetched),
    case Result of
        {worker_crash, Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace);
        _ ->
            ok
    end,
    ok.

%%%===================================================================
%%% Internal.
%%%===================================================================

%% @doc Report the fetch outcome to ar_peers. A local crash is not booked
%% against the peer. Cache pressure also ends the task without rating it.
rate_peer(Peer, ok, ElapsedUs, BytesFetched) ->
    (arweave_sync_deps:peers()):rate_fetched_data(Peer, chunk, ok, ElapsedUs, BytesFetched);
rate_peer(_Peer, {worker_crash, _, _, _}, _ElapsedUs, _BytesFetched) ->
    ok;
%% A 429 is the peer correctly enforcing its rate policy, not bad data or ill
%% health, and its worker time already feeds the scheduler's concurrency logic,
%% so it is not booked as an invalid_data failure.
%% Booking it let the politeness equilibrium (high reject ratios at small
%% caps) sink average_success below ?MINIMUM_SUCCESS and remove healthy
%% rate-limited peers (measured 2026-07-13: average_success 0.33 on a peer
%% at cap ~9, one warning from removal).
rate_peer(
    _Peer,
    {error, {ok, {{<<"429">>, _}, _, _, _, _}}},
    _ElapsedUs,
    _BytesFetched
) ->
    ok;
rate_peer(_Peer, cache_full, _ElapsedUs, _BytesFetched) ->
    ok;
rate_peer(Peer, Result, ElapsedUs, _BytesFetched) ->
    (arweave_sync_deps:peers()):rate_fetched_data(Peer, chunk, Result, ElapsedUs, 0).

%% @doc Make at most one chunk request within the task's chunk-sized claim.
%% Returns {Result, BytesFetched, FetchTiming}.
fetch_task(Task) ->
    try
        do_fetch_task(Task)
    catch
        Class:Reason:Stacktrace ->
            {{worker_crash, Class, Reason, Stacktrace}, 0, #fetch_timing{}}
    end.

do_fetch_task(Task) ->
    #task{offset = Offset, store_id = StoreID} = Task,
    case next_fetch_action(Offset, StoreID) of
        complete ->
            {ok, 0, #fetch_timing{}};
        cache_full ->
            {cache_full, 0, #fetch_timing{}};
        {fetch, FetchOffset} ->
            case (arweave_sync_deps:chunk_cache()):reserve(StoreID) of
                full ->
                    {cache_full, 0, #fetch_timing{}};
                {ok, CacheRef} ->
                    try
                        fetch_reserved(Task, FetchOffset, CacheRef)
                    after
                        (arweave_sync_deps:chunk_cache()):release(CacheRef)
                    end
            end
    end.

fetch_reserved(
    #task{peer = Peer, store_id = StoreID, offset = Offset} = Task,
    FetchOffset,
    CacheRef
) ->
    Byte = FetchOffset - 1,
    Packing = get_target_packing(StoreID),
    {FetchResult, FetchTiming} = timed_fetch(
        Peer, FetchOffset, Packing
    ),
    case FetchResult of
        {ok, #{chunk := Chunk} = Proof, _Time, _TransferSize} ->
            TaskRef = Task#task.task_ref,
            arweave_sync_ingest:store_fetched_chunk(
                StoreID, Peer, Byte, Proof, TaskRef, CacheRef
            ),
            {ok, byte_size(Chunk), FetchTiming};
        {error, {ok, {{<<"404">>, _}, _, _, _, _}} = Reason} ->
            {{error, Reason}, 0, FetchTiming};
        {error, Reason} ->
            (arweave_sync_deps:http()):log_failed_request({error, Reason}, [
                {event, failed_to_fetch_chunk},
                {peer, arweave_util:format_peer(Peer)},
                {start_offset, FetchOffset},
                {end_offset, Offset + ?DATA_CHUNK_SIZE},
                {reason, io_lib:format("~p", [Reason])}
            ]),
            {{error, Reason}, 0, FetchTiming}
    end.

timed_fetch(Peer, FetchOffset, Packing) ->
    StartMs = (arweave_sync_deps:clock()):monotonic_ms(),
    Result = (arweave_sync_deps:http()):get_chunk_binary(Peer, FetchOffset, Packing),
    ElapsedMs = max(1, (arweave_sync_deps:clock()):monotonic_ms() - StartMs),
    FetchTiming =
        case Result of
            {ok, _, _, _} ->
                #fetch_timing{productive_ms = ElapsedMs};
            {error, {ok, {{<<"429">>, _}, _, _, _, _}}} ->
                #fetch_timing{reject_ms = ElapsedMs};
            {error, timeout} ->
                #fetch_timing{timeout_ms = ElapsedMs};
            {error, client_error} ->
                #fetch_timing{client_error_ms = ElapsedMs};
            _ ->
                #fetch_timing{}
        end,
    {Result, FetchTiming}.

next_fetch_action(Offset, StoreID) ->
    End = Offset + ?DATA_CHUNK_SIZE,
    FindAction = fun Scan(Start) ->
        FetchOffset = (arweave_sync_deps:blacklist()):get_next_not_blacklisted_byte(Start + 1),
        Byte = FetchOffset - 1,
        case Byte >= End of
            true ->
                complete;
            false ->
                case
                    (arweave_sync_deps:storage()):get_next_interval(
                        synced, Byte, End, any_packing, {ar_data_sync, byte}, StoreID
                    )
                of
                    {RecordedEnd, RecordedStart} when RecordedStart =< Byte ->
                        Scan(RecordedEnd);
                    _ ->
                        case (arweave_sync_deps:chunk_cache()):is_full() of
                            true -> cache_full;
                            false -> {fetch, FetchOffset}
                        end
                end
        end
    end,
    FindAction(Offset).

%% @doc Read the target packing for this store, gated by the
%% [sync, request_packed_chunks] config (a cheap ETS read).
get_target_packing(StoreID) ->
    case arweave_config:get([sync, request_packed_chunks]) of
        true ->
            case arweave_storage:store_info(StoreID) of
                #store_info{packing = Packing} -> Packing;
                not_found -> not_found
            end;
        false ->
            any
    end.
