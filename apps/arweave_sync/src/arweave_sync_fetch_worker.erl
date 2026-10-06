%%% @doc A short-lived process that fetches one chunk from one peer.
%%% arweave_sync_scheduler spawns and monitors one worker (run/1) for
%%% each task the scheduler starts. Each worker goes through these steps once:
%%%
%%% 1. Find (next_fetch_action/2): find the first byte in the task's chunk
%%%    that is neither synced nor blacklisted. The worker stops when no such
%%%    byte exists or the chunk cache is full.
%%% 2. Fetch (do_fetch_task/1): reserve room in the chunk cache, request the
%%%    chunk from the peer, and hand the fetched chunk and its cache reference
%%%    to arweave_sync_chunk_writer.
%%% 3. Report (run/1): send the bytes fetched and the fetch timing to the
%%%    scheduler, and rate the peer in ar_peers.
%%%
%%% The worker exits normally even when the fetch fails. Only a crash in the
%%% worker ends the process abnormally, and the scheduler logs the crash.
-module(arweave_sync_fetch_worker).

-export([run/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Fetch the task's chunk, report the result to the scheduler and rate the
%% peer.
run(#task{peer = Peer} = Task) ->
    %% FetchTiming is measured with the scheduler's clock, while ar_peers rates
    %% the peer by the wall-clock time from timer:tc.
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
%%% Find.
%%%===================================================================

next_fetch_action(Offset, StoreID) ->
    End = Offset + ?DATA_CHUNK_SIZE,
    FindAction = fun Scan(Start) ->
        FetchOffset = ?DEP(blacklist):get_next_not_blacklisted_byte(Start + 1),
        Byte = FetchOffset - 1,
        case Byte >= End of
            true ->
                complete;
            false ->
                case
                    ?DEP(storage):get_next_interval(
                        synced, Byte, End, any_packing, {ar_data_sync, byte}, StoreID
                    )
                of
                    {RecordedEnd, RecordedStart} when RecordedStart =< Byte ->
                        Scan(RecordedEnd);
                    _ ->
                        case ?DEP(chunk_cache):is_full() of
                            true -> cache_full;
                            false -> {fetch, FetchOffset}
                        end
                end
        end
    end,
    FindAction(Offset).

%%%===================================================================
%%% Fetch.
%%%===================================================================

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
            case ?DEP(chunk_cache):reserve(StoreID) of
                full ->
                    {cache_full, 0, #fetch_timing{}};
                {ok, CacheRef} ->
                    try
                        fetch_reserved(Task, FetchOffset, CacheRef)
                    after
                        ?DEP(chunk_cache):release(CacheRef)
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
            arweave_sync_chunk_writer:store_fetched_chunk(
                StoreID, Peer, Byte, Proof, TaskRef, CacheRef
            ),
            {ok, byte_size(Chunk), FetchTiming};
        {error, {ok, {{<<"404">>, _}, _, _, _, _}} = Reason} ->
            {{error, Reason}, 0, FetchTiming};
        {error, Reason} ->
            ?DEP(http):log_failed_request({error, Reason}, [
                {event, failed_to_fetch_chunk},
                {peer, arweave_lib_util:format_peer(Peer)},
                {start_offset, FetchOffset},
                {end_offset, Offset + ?DATA_CHUNK_SIZE},
                {reason, io_lib:format("~p", [Reason])}
            ]),
            {{error, Reason}, 0, FetchTiming}
    end.

timed_fetch(Peer, FetchOffset, Packing) ->
    StartMs = ?DEP(clock):monotonic_ms(),
    Result = ?DEP(http):get_chunk_binary(Peer, FetchOffset, Packing),
    ElapsedMs = max(1, ?DEP(clock):monotonic_ms() - StartMs),
    FetchTiming =
        case Result of
            {ok, _, _, _} ->
                #fetch_timing{productive_ms = ElapsedMs};
            {error, timeout} ->
                #fetch_timing{timeout_ms = ElapsedMs};
            {error, client_error} ->
                #fetch_timing{client_error_ms = ElapsedMs};
            _ ->
                case is_rejection(Result) of
                    true -> #fetch_timing{reject_ms = ElapsedMs};
                    false -> #fetch_timing{}
                end
        end,
    {Result, FetchTiming}.

get_target_packing(StoreID) ->
    case ?DEP(config):get([sync, request_packed_chunks]) of
        true ->
            case ?DEP(storage):store_info(StoreID) of
                #store_info{packing = Packing} -> Packing;
                not_found -> not_found
            end;
        false ->
            any
    end.

%%%===================================================================
%%% Report.
%%%===================================================================

%% @doc Report the fetch result to ar_peers, except for a worker crash or a
%% full chunk cache, which are not the peer's fault.
rate_peer(Peer, ok, ElapsedUs, BytesFetched) ->
    ?DEP(peers):rate_fetched_data(Peer, chunk, ok, ElapsedUs, BytesFetched);
rate_peer(_Peer, {worker_crash, _, _, _}, _ElapsedUs, _BytesFetched) ->
    ok;
rate_peer(_Peer, cache_full, _ElapsedUs, _BytesFetched) ->
    ok;
rate_peer(Peer, Result, ElapsedUs, _BytesFetched) ->
    %% A rejection means the peer is enforcing its rate or load limits, not
    %% that it sent bad data or is unhealthy, and the scheduler already counts
    %% the time a rejection costs. Healthy peers can reject many requests even
    %% at small caps, so rating rejections as failures would push their
    %% average_success below ?MINIMUM_SUCCESS and get them removed.
    case is_rejection(Result) of
        true ->
            ok;
        false ->
            ?DEP(peers):rate_fetched_data(
                Peer, chunk, Result, ElapsedUs, 0)
    end.

%% @doc Return whether the peer declined the request under its rate limit
%% (429) or its load limit (503).
is_rejection({error, {ok, {{Status, _}, _, _, _, _}}}) ->
    lists:member(Status, [<<"429">>, <<"503">>]);
is_rejection(_Result) ->
    false.
