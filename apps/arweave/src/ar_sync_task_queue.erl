%% @doc Per-storage-module queue of sync tasks produced by ar_peer_sync's
%% discover loop and consumed by ar_peer_sync's sync loop.
%%
%% The queue has two complementary structures maintained as one invariant:
%%
%%   * `q'         - a gb_set of {FootprintKey, Start, End, Peer} tuples,
%%                   ordered for dispatch. Tasks belonging to the same
%%                   footprint sort together, so the consumer pops one
%%                   footprint's chunks before moving on - preserving
%%                   entropy amortization in the 2.9 replica mode.
%%   * `intervals' - a compact ar_intervals set holding the byte ranges
%%                   already present in `q'. Used for O(log n) dedup so
%%                   two peers seeding the same range do not each get
%%                   enqueued for that range.
%%
%% External callers treat the record as opaque.
-module(ar_sync_task_queue).

-export([new/0, size/1, is_empty/1, intervals/1,
		insert_batch/3, take_smallest/1,
		from_raw/2, to_raw/1]).

-include("ar.hrl").

-record(sync_task_queue, {
	q = gb_sets:new(),
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

%% @doc Remove the smallest task from the queue. Also removes the task's
%% byte range from the dedup intervals so future inserts may re-enqueue
%% the range if discovery re-surfaces it.
take_smallest(#sync_task_queue{ q = Q, intervals = QIntervals } = Queue) ->
	{{FootprintKey, Start, End, Peer}, Q2} = gb_sets:take_smallest(Q),
	QIntervals2 = ar_intervals:delete(QIntervals, End, Start),
	Task = {FootprintKey, Start, End, Peer},
	{Task, Queue#sync_task_queue{ q = Q2, intervals = QIntervals2 }}.

%% @doc Build a queue from a raw {gb_set, ar_intervals} pair. Only used by
%% the transitional shim in ar_data_sync; remove once state migration completes.
from_raw(Q, Intervals) ->
	#sync_task_queue{ q = Q, intervals = Intervals }.

%% @doc Unwrap to a raw {gb_set, ar_intervals} pair. Only used by the
%% transitional shim in ar_data_sync.
to_raw(#sync_task_queue{ q = Q, intervals = I }) ->
	{Q, I}.

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
