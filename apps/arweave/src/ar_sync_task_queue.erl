%% @doc Per-storage-module sync task queue (stateless library).
%%
%% Operates on the opaque `#sync_task_queue{}' record, which is owned by
%% `ar_peer_sync's per-StoreID gen_server. This module exposes the
%% operations that mutate the queue's `q' (a gb_set ordered for dispatch)
%% and `in_flight_intervals' (an ar_intervals overlay used to dedup
%% queued + currently-fetching ranges).
%%
%% Task lifecycle invariants ar_peer_sync must uphold:
%%
%%   insert_batch/3   - producer adds new tasks. Adds byte ranges to
%%                      `in_flight_intervals'.
%%   take_smallest/1  - consumer pops the next task. Byte range stays
%%                      in `in_flight_intervals' (still in flight).
%%   task_completed/3 - any path where a task definitively exits the
%%                      pipeline (success, failure, drop, reap, cut).
%%                      Removes the byte range from `in_flight_intervals'.
%%
%% Tasks are ordered by `{FootprintKey, Start, End, Peer}' so
%% replica-2.9 footprint work stays grouped, preserving entropy
%% amortization in the 2.9 replica mode.
-module(ar_sync_task_queue).

-export([new/0, size/1, size_by_mode/1, inflight_bytes/1, is_empty/1,
		in_flight_intervals/1, insert_batch/3, take_smallest/1, task_completed/3]).

-include("ar.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% normal_count and footprint_count partition `q' by mode (mode = normal if
%% FootprintKey == none, else footprint). They're maintained alongside the
%% gb_set so the per-mode `sync_task_queue_size{store_id, mode}` gauge can
%% be emitted in O(1) rather than walking the set.
-record(sync_task_queue, {
	q = gb_sets:new(),
	in_flight_intervals = ar_intervals:new(),
	normal_count = 0,
	footprint_count = 0
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

new() ->
	#sync_task_queue{}.

size(#sync_task_queue{ q = Q }) ->
	gb_sets:size(Q).

%% @doc Return {NormalCount, FootprintCount} — per-mode count of tasks
%% currently in the gb_set (not-yet-popped). Drives `sync_task_queue_size'.
size_by_mode(#sync_task_queue{ normal_count = N, footprint_count = F }) ->
	{N, F}.

%% @doc Total bytes in the in_flight_intervals overlay (queued + currently
%% fetching). Drives `sync_task_queue_inflight_bytes'.
inflight_bytes(#sync_task_queue{ in_flight_intervals = I }) ->
	ar_intervals:sum(I).

is_empty(#sync_task_queue{ q = Q }) ->
	gb_sets:is_empty(Q).

in_flight_intervals(#sync_task_queue{ in_flight_intervals = I }) ->
	I.

%% @doc Insert a list of {Peer, Intervals, FootprintKey} entries into the
%% queue, capping each peer at ChunksPerPeer chunk-sized slices. Byte ranges
%% already in flight are skipped (dedup via `in_flight_intervals' overlay).
insert_batch(PeerEntries, ChunksPerPeer, Queue) ->
	lists:foldl(
		fun({Peer, Intervals, FootprintKey}, Acc) ->
			insert_peer(Peer, Intervals, FootprintKey, ChunksPerPeer, Acc)
		end,
		Queue,
		PeerEntries
	).

%% @doc Remove the smallest task from the queue. The task's byte range
%% stays in `in_flight_intervals' so subsequent insert_batch/3 calls
%% dedup against the still-in-flight range. Caller must invoke
%% task_completed/3 once the sync_range definitively finishes to release
%% the dedup.
take_smallest(#sync_task_queue{ q = Q,
		normal_count = N, footprint_count = F } = Queue) ->
	{{FootprintKey, Start, End, Peer}, Q2} = gb_sets:take_smallest(Q),
	Task = {FootprintKey, Start, End, Peer},
	{N2, F2} = decrement_mode_count(FootprintKey, N, F),
	{Task, Queue#sync_task_queue{ q = Q2,
			normal_count = N2, footprint_count = F2 }}.

%% @doc Release the dedup overlay for a byte range whose sync_range has
%% definitively completed (success, failure, drop, reap, cut). Future
%% insert_batch/3 calls covering [Start, End) may enqueue tasks for it
%% again.
task_completed(End, Start,
		#sync_task_queue{ in_flight_intervals = InFlightIntervals } = Queue) ->
	Queue#sync_task_queue{
		in_flight_intervals = ar_intervals:delete(InFlightIntervals, End, Start) }.

%%%===================================================================
%%% Private helpers.
%%%===================================================================

insert_peer(Peer, Intervals, FootprintKey, ChunksToEnqueue,
		#sync_task_queue{ q = Q,
				in_flight_intervals = InFlightIntervals,
				normal_count = N, footprint_count = F } = Queue) ->
	%% Drop intervals already in flight so two peers seeding the same range
	%% do not each get enqueued for it.
	OuterJoin = ar_intervals:outerjoin(InFlightIntervals, Intervals),
	{_, {Q2, InFlightIntervals2, N2, F2}} = ar_intervals:fold(
		fun	(_, {0, Acc}) ->
				{0, Acc};
			({End, Start}, {Remaining, {QAcc, IFAcc, NAcc, FAcc}}) ->
				RangeEnd = min(End, Start + (Remaining * ?DATA_CHUNK_SIZE)),
				ChunkOffsets = lists:seq(Start, RangeEnd - 1, ?DATA_CHUNK_SIZE),
				ChunksEnqueued = length(ChunkOffsets),
				{Q3, IF2} = insert_range(Peer, FootprintKey, Start, RangeEnd,
						ChunkOffsets, {QAcc, IFAcc}),
				{NAcc2, FAcc2} = case FootprintKey of
					none -> {NAcc + ChunksEnqueued, FAcc};
					_ -> {NAcc, FAcc + ChunksEnqueued}
				end,
				{Remaining - ChunksEnqueued, {Q3, IF2, NAcc2, FAcc2}}
		end,
		{ChunksToEnqueue, {Q, InFlightIntervals, N, F}},
		OuterJoin
	),
	Queue#sync_task_queue{ q = Q2, in_flight_intervals = InFlightIntervals2,
			normal_count = N2, footprint_count = F2 }.

insert_range(Peer, FootprintKey, RangeStart, RangeEnd, ChunkOffsets,
		{Q, InFlightIntervals}) ->
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
	InFlightIntervals2 = ar_intervals:add(InFlightIntervals, RangeEnd, RangeStart),
	{Q2, InFlightIntervals2}.

%% Mode = normal if FootprintKey == none, else footprint.
decrement_mode_count(none, Normal, Footprint) ->
	{max(0, Normal - 1), Footprint};
decrement_mode_count(_FootprintKey, Normal, Footprint) ->
	{Normal, max(0, Footprint - 1)}.

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
	SeedInFlight = ar_intervals:from_list(SeedRanges),
	Seeded = #sync_task_queue{ in_flight_intervals = SeedInFlight },
	Result = insert_batch(Intervals, ChunksPerPeer, Seeded),
	#sync_task_queue{ q = QResult, in_flight_intervals = ResultInFlight } = Result,
	ExpectedInFlight = lists:foldl(fun({End, Start}, Acc) ->
			ar_intervals:add(Acc, End, Start)
		end, SeedInFlight, ExpectedAddedRanges),
	?assertEqual(ar_intervals:to_list(ExpectedInFlight),
		ar_intervals:to_list(ResultInFlight), Label),
	?assertEqual(ExpectedChunks, gb_sets:to_list(QResult), Label).

-endif.
