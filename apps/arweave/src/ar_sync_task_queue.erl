%% @doc Per-storage-module stateless helper library for network sync.
%% Operates on the opaque `#sync_task_queue{}', owned by
%% `ar_peer_sync's per-StoreID gen_server: it slices fetchable peer-intervals
%% into chunk-sized tasks, deduped against `inflight_intervals' (an ar_intervals
%% set of queued + currently-fetching ranges).
%%
%% These library only transforms the sync_task_queue record; `ar_peer_sync' owns it
%% and drives the range lifecycle.
%% 
%% The contract `ar_peer_sync' must uphold: every range recorded by
%% insert_batch is eventually released by release_task_range. A range that is
%% never released leaks the dedup overlay (watched by the
%% sync_task_queue_inflight_bytes metric).
-module(ar_sync_task_queue).
-test_category([fast]).

-export([new/0, size/1, inflight_bytes/1, insert_batch/3, drain/1,
        release_task_range/3]).

-include("ar.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(sync_task_queue, {
    %% gb_set of sliced-but-not-yet-drained chunk tasks ({FK, Start, End, Peer}).
    q = gb_sets:new(),
    %% ar_intervals set of queued + currently-fetching ranges, used to dedup.
    inflight_intervals = ar_intervals:new()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

new() ->
    #sync_task_queue{}.

size(#sync_task_queue{ q = Q }) ->
    gb_sets:size(Q).

%% @doc Total bytes in `inflight_intervals' (queued + currently
%% fetching). Drives `sync_task_queue_inflight_bytes'.
inflight_bytes(#sync_task_queue{ inflight_intervals = I }) ->
    ar_intervals:sum(I).

%% @doc Insert a list of {Peer, Intervals, FootprintKey} entries into the
%% queue, capping each peer at ChunksPerPeer chunk-sized slices. Byte ranges
%% already in flight are skipped (dedup via `inflight_intervals').
insert_batch(PeerEntries, ChunksPerPeer, Queue) ->
    lists:foldl(
        fun({Peer, Intervals, FootprintKey}, Acc) ->
            insert_peer(Peer, Intervals, FootprintKey, ChunksPerPeer, Acc)
        end,
        Queue,
        PeerEntries
    ).

%% @doc Remove and return every queued task ({FootprintKey, Start, End, Peer}),
%% leaving `inflight_intervals' intact so the drained ranges still dedup future
%% insert_batch/3 calls until released. Used by ar_peer_sync to hand newly-sliced
%% tasks to ar_sync_dispatcher on each ar_peer_sync step.
drain(#sync_task_queue{ q = Q } = Queue) ->
    {gb_sets:to_list(Q), Queue#sync_task_queue{ q = gb_sets:new() }}.

%% Remove a terminal task range from `inflight_intervals'. Future insert_batch/3
%% calls covering [Start, End) may enqueue tasks for it again.
release_task_range(Start, End,
        #sync_task_queue{ inflight_intervals = InflightIntervals } = Queue) ->
    Queue#sync_task_queue{
        inflight_intervals = ar_intervals:delete(InflightIntervals, End, Start) }.

%%%===================================================================
%%% Private helpers.
%%%===================================================================

insert_peer(Peer, Intervals, FootprintKey, ChunksToEnqueue,
        #sync_task_queue{ q = Q, inflight_intervals = InflightIntervals } = Queue) ->
    %% Drop intervals already in flight so two peers seeding the same range
    %% do not each get enqueued for it.
    OuterJoin = ar_intervals:outerjoin(InflightIntervals, Intervals),
    {_, {Q2, InflightIntervals2}} = ar_intervals:fold(
        fun (_, {0, Acc}) ->
                {0, Acc};
            ({End, Start}, {Remaining, {QAcc, IFAcc}}) ->
                RangeEnd = min(End, Start + (Remaining * ?DATA_CHUNK_SIZE)),
                ChunkOffsets = lists:seq(Start, RangeEnd - 1, ?DATA_CHUNK_SIZE),
                ChunksEnqueued = length(ChunkOffsets),
                {Q3, IF2} = insert_range(Peer, FootprintKey, Start, RangeEnd,
                        ChunkOffsets, {QAcc, IFAcc}),
                {Remaining - ChunksEnqueued, {Q3, IF2}}
        end,
        {ChunksToEnqueue, {Q, InflightIntervals}},
        OuterJoin
    ),
    Queue#sync_task_queue{ q = Q2, inflight_intervals = InflightIntervals2 }.

insert_range(Peer, FootprintKey, RangeStart, RangeEnd, ChunkOffsets,
        {Q, InflightIntervals}) ->
    Q2 = lists:foldl(
        fun(ChunkStart, QAcc) ->
            gb_sets:add_element(
                {FootprintKey, ChunkStart,
                        min(ChunkStart + ?DATA_CHUNK_SIZE, RangeEnd), Peer},
                QAcc)
        end,
        Q,
        ChunkOffsets
    ),
    InflightIntervals2 = ar_intervals:add(InflightIntervals, RangeEnd, RangeStart),
    {Q2, InflightIntervals2}.

-ifdef(AR_TEST).

enqueue_intervals_test() ->
    test_enqueue_intervals([], 2, [], [], [], "Empty Intervals"),
    Peer1 = {1, 2, 3, 4, 1984},
    Peer2 = {101, 102, 103, 104, 1984},
    Peer3 = {201, 202, 203, 204, 1984},

    test_enqueue_intervals(
        [
            {Peer1, ar_intervals:from_list([
                    {4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
                    {9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
                ]), none}
        ],
        5,
        [{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
        [
            {4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
            {9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
        ],
        [
            {none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
            {none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
            {none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1},
            {none, 7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer1},
            {none, 8*?DATA_CHUNK_SIZE, 9*?DATA_CHUNK_SIZE, Peer1}
        ],
        "Single peer, full intervals, all chunks. Non-overlapping seed."),

    test_enqueue_intervals(
        [
            {Peer1, ar_intervals:from_list([
                {4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
                {9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
            ]), none},
            {Peer2, ar_intervals:from_list([
                {4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
                {7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
            ]), none},
            {Peer3, ar_intervals:from_list([
                {8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
            ]), none}
        ],
        2,
        [{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
        [
            {4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
            {8*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
        ],
        [
            {none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
            {none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
            {none, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
            {none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer2},
            {none, 7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer3}
        ],
        "Multiple peers, overlapping, full intervals, 2 chunks."),

    ok.

test_enqueue_intervals(Intervals, ChunksPerPeer, SeedRanges, ExpectedAddedRanges,
        ExpectedChunks, Label) ->
    SeedInflight = ar_intervals:from_list(SeedRanges),
    Seeded = #sync_task_queue{ inflight_intervals = SeedInflight },
    Result = insert_batch(Intervals, ChunksPerPeer, Seeded),
    #sync_task_queue{ q = QResult, inflight_intervals = ResultInflight } = Result,
    ExpectedInflight = lists:foldl(fun({End, Start}, Acc) ->
            ar_intervals:add(Acc, End, Start)
        end, SeedInflight, ExpectedAddedRanges),
    ?assertEqual(ar_intervals:to_list(ExpectedInflight),
        ar_intervals:to_list(ResultInflight), Label),
    ?assertEqual(ExpectedChunks, gb_sets:to_list(QResult), Label).

-endif.
