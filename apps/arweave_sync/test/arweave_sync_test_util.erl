%%% App-owned fixtures behind arweave_sync's test-only interface.
-module(arweave_sync_test_util).
-export([enqueue_chunk/3, with_fetch_result/4, deliver_chunk/4, store_ready/2]).
-export([with_mocks/2]).
-include_lib("arweave_sync/include/arweave_sync.hrl").

%% @doc Run a test callback with local dependency mocks, then restore the modules.
with_mocks([], Test) ->
    Test();
with_mocks([{Module, _, _} | _] = Mocks, Test) ->
    {ModuleMocks, Rest} = lists:partition(
        fun({M, _, _}) -> M =:= Module end, Mocks
    ),
    meck:new(Module, [passthrough]),
    try
        lists:foreach(
            fun({M, F, Impl}) -> meck:expect(M, F, Impl) end,
            ModuleMocks
        ),
        with_mocks(Rest, Test)
    after
        meck:unload(Module)
    end.

%% @doc Claim a chunk range through the real scheduler.
enqueue_chunk(StoreID, Peer, Offset) ->
    Task = #task{
        store_id = StoreID,
        offset = Offset,
        sources = [#task_source{peer = Peer}]
    },
    arweave_sync_scheduler:claim_and_enqueue(StoreID, [Task]).

%% @doc Replace fetches during Test, sending {sync_chunk_fetched, StoreID, Result}.
with_fetch_result(Peer, Byte, Proof, Test) ->
    Parent = self(),
    Fetch = fun(#task{store_id = StoreID, task_ref = TaskRef}) ->
        Result = do_deliver_chunk(StoreID, Peer, Byte, Proof, TaskRef),
        Parent ! {sync_chunk_fetched, StoreID, Result},
        arweave_sync_scheduler:report_fetch_completed(
            TaskRef, byte_size(maps:get(chunk, Proof)), #fetch_timing{}
        )
    end,
    meck:new(arweave_sync_fetch_worker, [passthrough, no_link]),
    try
        meck:expect(arweave_sync_fetch_worker, run, Fetch),
        Test()
    after
        meck:unload(arweave_sync_fetch_worker)
    end.

%% @doc Deliver an unscheduled proof through real cache admission and ingestion.
deliver_chunk(StoreID, Peer, Byte, Proof) ->
    do_deliver_chunk(StoreID, Peer, Byte, Proof, undefined).

do_deliver_chunk(StoreID, Peer, Byte, Proof, TaskRef) ->
    Cache = arweave_sync_deps:chunk_cache(),
    case Cache:reserve(StoreID) of
        {ok, CacheRef} ->
            try
                arweave_sync_ingest:store_fetched_chunk(
                    StoreID, Peer, Byte, Proof, TaskRef, CacheRef
                )
            after
                Cache:release(CacheRef)
            end;
        Error ->
            Error
    end.

%% @doc Confirm that the replacement writer's ingestion process is initialized.
store_ready(StoreID, WriterPID) ->
    case arweave_sync_ingest:writer(StoreID) of
        {WriterPID, IngestPID} ->
            (catch gen_server:call(IngestPID, ping)) =:= pong;
        _ ->
            false
    end.
