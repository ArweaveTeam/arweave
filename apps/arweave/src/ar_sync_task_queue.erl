%% @doc Per-storage-module queue of sync tasks produced by ar_peer_sync's
%% discover loop and consumed by ar_peer_sync's sync loop.
-module(ar_sync_task_queue).

-export([new/0, size/1, is_empty/1, intervals/1,
		insert_batch/3, take_smallest/1, task_completed/3]).

-include("ar.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(sync_task_queue, {
	%% A gb_set of {FootprintKey, Start, End, Peer} tuples ordered for dispatch.
	%% Tasks belonging to the same footprint sort together, so the consumer pops
	%% one footprint's chunks before moving on, preserving entropy amortization
	%% in the 2.9 replica mode.
	q = gb_sets:new(),
	%% A compact ar_intervals set holding byte ranges already present in `q'.
	%% Used for O(log n) dedup so two peers seeding the same range do not each
	%% get enqueued for that range.
	intervals = ar_intervals:new()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

new() ->
	#sync_task_queue{}.

size(#sync_task_queue{ q = Q }) ->
	gb_sets:size(Q).

is_empty(#sync_task_queue{ q = Q }) ->
	gb_sets:is_empty(Q).

intervals(#sync_task_queue{ intervals = I }) ->
	I.

%% @doc Insert a list of {Peer, Intervals, FootprintKey} entries into the
%% queue, capping each peer at ChunksPerPeer chunk-sized slices. Byte ranges
%% already present in the queue are skipped (dedup via `intervals' overlay).
insert_batch(PeerEntries, ChunksPerPeer, Queue) ->
	lists:foldl(
		fun({Peer, Intervals, FootprintKey}, Acc) ->
			insert_peer(Peer, Intervals, FootprintKey, ChunksPerPeer, Acc)
		end,
		Queue,
		PeerEntries
	).

%% @doc Remove the smallest task from the queue. The task's byte range
%% stays in `intervals' so subsequent insert_batch/3 calls dedup against
%% the still-in-flight range. Caller must invoke task_completed/3 once
%% the sync_range definitively finishes to release the dedup.
take_smallest(#sync_task_queue{ q = Q } = Queue) ->
	{{FootprintKey, Start, End, Peer}, Q2} = gb_sets:take_smallest(Q),
	Task = {FootprintKey, Start, End, Peer},
	{Task, Queue#sync_task_queue{ q = Q2 }}.

%% @doc Release the dedup overlay for a byte range whose sync_range has
%% definitively completed (success or non-recast failure). Future
%% insert_batch/3 calls covering [Start, End) may enqueue tasks for it
%% again.
task_completed(End, Start, #sync_task_queue{ intervals = QIntervals } = Queue) ->
	Queue#sync_task_queue{ intervals = ar_intervals:delete(QIntervals, End, Start) }.

%%%===================================================================
%%% Private helpers.
%%%===================================================================

insert_peer(Peer, Intervals, FootprintKey, ChunksToEnqueue,
		#sync_task_queue{ q = Q, intervals = QIntervals } = Queue) ->
	%% Drop intervals already present in the queue so two peers seeding the
	%% same range do not each get enqueued for it.
	OuterJoin = ar_intervals:outerjoin(QIntervals, Intervals),
	{_, {Q2, QIntervals2}} = ar_intervals:fold(
		fun	(_, {0, Acc}) ->
				{0, Acc};
			({End, Start}, {Remaining, {QAcc, QIAcc}}) ->
				RangeEnd = min(End, Start + (Remaining * ?DATA_CHUNK_SIZE)),
				ChunkOffsets = lists:seq(Start, RangeEnd - 1, ?DATA_CHUNK_SIZE),
				ChunksEnqueued = length(ChunkOffsets),
				{Remaining - ChunksEnqueued,
					insert_range(Peer, FootprintKey, Start, RangeEnd, ChunkOffsets,
							{QAcc, QIAcc})}
		end,
		{ChunksToEnqueue, {Q, QIntervals}},
		OuterJoin
	),
	Queue#sync_task_queue{ q = Q2, intervals = QIntervals2 }.

insert_range(Peer, FootprintKey, RangeStart, RangeEnd, ChunkOffsets, {Q, QIntervals}) ->
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
	QIntervals2 = ar_intervals:add(QIntervals, RangeEnd, RangeStart),
	{Q2, QIntervals2}.

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
		"Single peer, full intervals, all chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
				]), none}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, 2 chunks. Non-overlapping QIntervals."),

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
		"Multiple peers, overlapping, full intervals, 2 chunks. Non-overlapping QIntervals."),

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
		3,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1},
			{none, 7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer3}
		],
		"Multiple peers, overlapping, full intervals, 3 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none}
		],
		5,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}, {9*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{7*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, all chunks. Overlapping QIntervals."),

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
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}, {9*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{none, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer2}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, trunc(5.75*?DATA_CHUNK_SIZE)}
			]), none}
		],
		2,
		[
			{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE},
			{trunc(8.5*?DATA_CHUNK_SIZE), trunc(6.5*?DATA_CHUNK_SIZE)}
		],
		[
			{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, trunc(3.25*?DATA_CHUNK_SIZE), Peer1}
		],
		"Single peer, partial intervals, 2 chunks. Overlapping partial QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, trunc(5.75*?DATA_CHUNK_SIZE)}
			]), none},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			]), none},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			]), none}
		],
		2,
		[
			{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE},
			{trunc(8.5*?DATA_CHUNK_SIZE), trunc(6.5*?DATA_CHUNK_SIZE)}
		],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{none, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{none, 3*?DATA_CHUNK_SIZE, trunc(3.25*?DATA_CHUNK_SIZE), Peer1},
			{none, trunc(3.25*?DATA_CHUNK_SIZE), 4*?DATA_CHUNK_SIZE, Peer2},
			{none, 6*?DATA_CHUNK_SIZE, trunc(6.5*?DATA_CHUNK_SIZE), Peer2}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Overlapping QIntervals.").

test_enqueue_intervals(Intervals, ChunksPerPeer, QIntervalsRanges, ExpectedQIntervalRanges, ExpectedChunks, Label) ->
	QIntervals = ar_intervals:from_list(QIntervalsRanges),
	Seeded = #sync_task_queue{ intervals = QIntervals },
	Result = insert_batch(Intervals, ChunksPerPeer, Seeded),
	#sync_task_queue{ q = QResult, intervals = QIntervalsResult } = Result,
	ExpectedQIntervals = lists:foldl(fun({End, Start}, Acc) ->
			ar_intervals:add(Acc, End, Start)
		end, QIntervals, ExpectedQIntervalRanges),
	?assertEqual(ar_intervals:to_list(ExpectedQIntervals), ar_intervals:to_list(QIntervalsResult), Label),
	?assertEqual(ExpectedChunks, gb_sets:to_list(QResult), Label).
-endif.
