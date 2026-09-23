-module(arweave_storage_footprint_record).

-export([
    add/3,
    add_async/4,
    delete/2,
    start_initialization/1,
    mark_initialized/1,
    is_step_current/2,
    initialization_walk/2,
    initialization_step_written/3,
    is_initialized/1,
    get_offset/1,
    get_padded_offset_from_footprint_offset/1,
    get_footprint/1,
    footprint_range/2,
    get_location/1,
    get_footprint_bucket/1,
    get_intervals/3,
    get_intervals/4,
    get_unsynced_intervals/3,
    footprint_intervals_to_byte_intervals/1,
    footprint_intervals_to_byte_intervals/3,
    byte_intervals_to_footprint_intervals/3,
    max_offset/1,
    is_recorded/2,
    get_next_sector_start/1,
    get_sector_bucket_start/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

%% The key of the store's own database holding the footprint offset the
%% initialization resumes from, or <<"complete">> once it is done.
-define(INITIALIZATION_CURSOR_KEY, <<"footprint_record_init_cursor">>).

%% The initialization advances in steps. A step walks for at most
%% ?INITIALIZATION_STEP_BUDGET_MS, which bounds how long the store's sync
%% record server spends in one message, and then sleeps for at least as long,
%% so the initialization never takes more than half that server's time. A step
%% that wrote many intervals sleeps longer still: a store adds no more than
%% ?INITIALIZATION_ADDS_PER_SECOND intervals per second, the rate at which the
%% per-chunk initialization of earlier releases fed the global sync record.
-ifdef(AR_TEST).
-define(INITIALIZATION_STEP_BUDGET_MS, 20).
-else.
-define(INITIALIZATION_STEP_BUDGET_MS, 100).
-endif.
-define(INITIALIZATION_ADDS_PER_SECOND, 200).

-moduledoc """
    This module exports functions for maintaining
a replica 2.9 entropy-aligned record of the synced chunks.
It differs from the normal record (ar_data_sync) in that it only registers
the bucket numbers of the synced chunks and records chunks with the same footprint
next to each other. For example, a record may contain intervals 0-10, 1000-1024,
1028-2048. This means the node has the first 10 chunks of the first entropy footprint,
the last 24 chunks of the first entropy footprint and chunks 4-44 of the second
entropy footprint. These chunks are from the first partition. The offset of the chunks
from the second partition is shifted by the number of chunks in the replica 2.9
entropy generated per partition (which is slightly bigger than the number of chunks
                                 that can fit in the 3.6 TB partition).

Note that Packing does not have to be replica_2_9. We maintain this record
for any packing so that it is convenient to serve the data to any client.
""".

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Add a chunk to the footprint record.
add(Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage:add_sync_record(
        FootprintOffset, FootprintOffset - 1, Packing, {ar_data_sync, footprint}, StoreID
    ).

%% @doc Add a chunk to the footprint record asynchronously.
add_async(Tag, Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage_sync_record:add_async(
        Tag,
        FootprintOffset,
        FootprintOffset - 1,
        Packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% @doc Start building the store's footprint record, unless it is built
%% already.
start_initialization(StoreID) ->
    case read_initialization_cursor(state_db(StoreID)) of
        complete ->
            ok;
        Cursor ->
            {RangeStart, RangeEnd} = store_range(StoreID),
            ?LOG_INFO([
                {event, initializing_footprint_record},
                {store_id, StoreID},
                {cursor, Cursor},
                {range_start, RangeStart},
                {range_end, RangeEnd},
                {start_pct, initialization_pct(Cursor, StoreID)}
            ]),
            %% The console line waits for the first step to show how much
            %% ground a step covers on this node.
            schedule_initialization_step(0, StoreID, Cursor, false)
    end.

%% @doc Whether a step scheduled at Cursor is still the one to take: the
%% persisted cursor has not moved past it, and the record is not built. Two
%% chains of steps can overlap when the initialization is started again while
%% one runs; of steps at the same cursor the first wins, and the rest stop.
is_step_current(StateDB, Cursor) ->
    read_initialization_cursor(StateDB) == Cursor.

%% @doc Record that the store's footprint record is built, for a caller that
%% knows it was built by other means.
mark_initialized(StoreID) ->
    write_initialization_cursor(state_db(StoreID), <<"complete">>).

%% @doc Record the outcome of a step whose intervals the caller has written:
%% persist where the walk stopped, or mark the record built, report the
%% estimate once and schedule the next step.
initialization_step_written(StateDB, StoreID, Step) ->
    #{
        status := Status,
        cursor := Cursor,
        next := Next,
        added := Added,
        estimate_reported := EstimateReported
    } = Step,
    case Status of
        complete ->
            ok = write_initialization_cursor(StateDB, <<"complete">>),
            ?LOG_INFO([
                {event, footprint_record_initialized}, {store_id, StoreID}
            ]),
            arweave_storage_deps:console(
                "~nThe storage module ~s finished building its footprint "
                "index and can now sync data from peers.~n",
                [StoreID]
            );
        continue ->
            ok = write_initialization_cursor(
                StateDB, binary:encode_unsigned(Next)
            ),
            Delay = next_initialization_step_ms(Added),
            EstimateReported2 =
                EstimateReported orelse
                    report_initialization_estimate(
                        Cursor, Next, Delay, StoreID
                    ),
            schedule_initialization_step(
                Delay, StoreID, Next, EstimateReported2
            )
    end.

%% @doc Whether the store's footprint record has been built from its
%% {ar_data_sync, byte} one. A store without an on-disk record - the default
%% module - has nothing to build.
is_initialized(StoreID) ->
    case arweave_storage_module:get_by_id(StoreID) of
        Module when is_atom(Module) ->
            true;
        _ ->
            read_initialization_cursor(state_db(StoreID)) == complete
    end.

%% @doc Walk one step of the store's {ar_data_sync, byte} record, from Cursor
%% (a footprint offset, or start) for at most one step's budget, and return
%% {complete | continue, NextCursor, Intervals}: the {ar_data_sync, footprint}
%% intervals covering the chunks it found, as [{Packing, {End, Start}}].
%% Contiguous chunks make a single interval, and a chunk belongs to every
%% packing it is recorded with except the transitional unpacked_padded. The
%% caller writes them - only the store's own server can, since they go through
%% its write-ahead log - and repeating a step is harmless.
initialization_walk(StoreID, Cursor) ->
    initialization_walk(
        StoreID, store_range(StoreID), Cursor, ?INITIALIZATION_STEP_BUDGET_MS
    ).

initialization_walk(StoreID, {RangeStart, RangeEnd}, start, BudgetMs) ->
    {First, _} = span(RangeStart, RangeEnd),
    initialization_walk(StoreID, {RangeStart, RangeEnd}, First, BudgetMs);
initialization_walk(StoreID, {RangeStart, RangeEnd}, Cursor, BudgetMs) ->
    Packings = [
        Packing
     || Packing <- arweave_storage_sync_record:get_packings(
            {ar_data_sync, byte}, StoreID
        ),
        Packing =/= unpacked_padded
    ],
    {_, End} = span(RangeStart, RangeEnd),
    Ctx = #{
        range => {RangeStart, RangeEnd},
        end_offset => End,
        packings => Packings,
        store_id => StoreID,
        deadline => erlang:monotonic_time(millisecond) + BudgetMs
    },
    {Next, Runs} = walk_footprints(Cursor, Ctx, #{}),
    Status =
        case Next >= End of
            true -> complete;
            false -> continue
        end,
    {Status, Next, intervals(Runs)}.

%% Flatten the per-packing runs into the intervals to write, oldest first.
intervals(Runs) ->
    [
        {Packing, Run}
     || {Packing, PackingRuns} <- maps:to_list(Runs),
        Run <- lists:reverse(PackingRuns)
    ].

%% @doc Return {First, End}: the start of the first footprint a chunk of the
%% byte range can map to and the end of the last one.
span(RangeStart, RangeEnd) ->
    ChunksPerPartition = get_chunks_per_partition(),
    FirstPartition = arweave_storage_deps:get_entropy_partition(RangeStart + 1),
    LastPartition = arweave_storage_deps:get_entropy_partition(RangeEnd),
    {FirstPartition * ChunksPerPartition,
        (LastPartition + 1) * ChunksPerPartition}.

%% @doc Get the offset of a chunk in the footprint record.
get_offset(Offset) ->
    PaddedOffset = arweave_constants:get_chunk_padded_offset(Offset),
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    FootprintsPerPartition = arweave_storage_deps:get_footprints_per_partition(),

    ChunksPerPartition = get_chunks_per_partition(),
    Partition = arweave_storage_deps:get_entropy_partition(PaddedOffset),
    PartitionOffset =
        (PaddedOffset - Partition * arweave_constants:partition_size()) div ?DATA_CHUNK_SIZE - 1,

    %% Which footprint within the partition
    Footprint = PartitionOffset rem FootprintsPerPartition,
    %% Position within the footprint
    FootprintOffset = PartitionOffset div FootprintsPerPartition,
    Partition * ChunksPerPartition + Footprint * FootprintSize + FootprintOffset + 1.

%% @doc Return the largest end offset of the chunk that maps to the given footprint offset.
get_padded_offset_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    Partition = Start div ChunksPerPartition,
    FootprintsPerPartition = arweave_storage_deps:get_footprints_per_partition(),
    PartitionStart = Partition * ChunksPerPartition,
    Footprint = (Start - PartitionStart) div FootprintSize,
    InFootprintOffset = (Start - PartitionStart) rem FootprintSize,
    EndOffset =
        Partition * arweave_constants:partition_size() +
            (InFootprintOffset * FootprintsPerPartition + (Footprint + 1)) * ?DATA_CHUNK_SIZE,
    arweave_constants:get_chunk_padded_offset(EndOffset).

%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
get_footprint(Offset) ->
    EntropyIndex = arweave_storage_deps:get_entropy_index(Offset, 0),
    EntropyIndex div ?SUB_CHUNK_COUNT.

%% @doc Return the bucket end offset of the first chunk of the sector after
%% the one holding the given chunk.
get_next_sector_start(AbsoluteChunkEndOffset) ->
    get_sector_bucket_start(AbsoluteChunkEndOffset, 1) + ?DATA_CHUNK_SIZE.

%% @doc Get the replica 2.9 partition and footprint containing a chunk offset.
get_location(Offset) ->
    {arweave_storage_deps:get_entropy_partition(Offset), get_footprint(Offset)}.

%% @doc The footprint location of a footprint offset: the
%% {Partition, Footprint} pair get_location/1 derives from a byte offset,
%% computed in footprint space. It cannot go through get_location/1: a
%% partition's entropy covers a few offsets more than the partition has
%% chunks, those map to chunks of the next partition, and telling them apart
%% is what comparing the two answers is for.
get_location_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    ChunksPerPartition = get_chunks_per_partition(),
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    Partition = Start div ChunksPerPartition,
    {Partition, (Start - Partition * ChunksPerPartition) div FootprintSize}.

%% @doc Get the footprint bucket number of a chunk.
get_footprint_bucket(Offset) ->
    get_offset(Offset) div ?NETWORK_FOOTPRINT_BUCKET_SIZE.

%% @doc Get the synced footprint intervals of a chunk.
get_intervals(Partition, Footprint, StoreID) ->
    get_intervals(Partition, Footprint, any, StoreID).

%% @doc Get the synced footprint intervals of a chunk.
get_intervals(Partition, Footprint, any, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    arweave_storage_sync_record:get_intervals(
        synced, Start, End, any_packing, {ar_data_sync, footprint}, StoreID
    );
get_intervals(Partition, Footprint, Packing, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    arweave_storage_sync_record:get_intervals(
        synced,
        Start,
        End,
        Packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% @doc Get the unsynced footprint intervals of a chunk.
get_unsynced_intervals(Partition, Footprint, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    arweave_storage:get_intervals(
        unsynced,
        Start,
        End,
        any_packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% @doc Delete a chunk from the footprint record.
delete(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage:delete_sync_record(
        FootprintOffset, FootprintOffset - 1, {ar_data_sync, footprint}, StoreID
    ).

%% @doc Convert footprint intervals to byte intervals.
footprint_intervals_to_byte_intervals(FootprintIntervals) ->
    do_footprint_intervals_to_byte_intervals(
        ar_intervals:to_list(FootprintIntervals), ar_intervals:new()
    ).

%% @doc Convert footprint intervals to byte intervals, cut at End, and drop
%% bytes before the chunk containing Start.
footprint_intervals_to_byte_intervals(FootprintIntervals, Start, End) ->
    ByteIntervals = footprint_intervals_to_byte_intervals(FootprintIntervals),
    ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
    PaddedStart =
        case arweave_constants:get_chunk_padded_offset(Start) of
            Start -> Start;
            PaddedOffset -> PaddedOffset - ?DATA_CHUNK_SIZE
        end,
    ar_intervals:outerjoin(
        ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2
    ).

%% @doc Return the offsets of the footprint whose chunks overlap ByteIntervals,
%% the inverse of footprint_intervals_to_byte_intervals/1.
byte_intervals_to_footprint_intervals(ByteIntervals, Partition, Footprint) ->
    {Start, End} = footprint_range(Partition, Footprint),
    ar_intervals:fold(
        fun({ByteEnd, ByteStart}, Acc) ->
            %% Each offset maps to one chunk in byte space
            %% (chunk_byte_interval/1), and a footprint's chunks follow the
            %% order of its offsets, so the ones overlapping
            %% (ByteStart, ByteEnd] are the offsets from First up to, not
            %% including, Next.
            First = first_chunk_ending_after(ByteStart, Start, End),
            Next = first_chunk_starting_from(ByteEnd, Start, End),
            case Next > First of
                true -> ar_intervals:add(Acc, Next - 1, First - 1);
                false -> Acc
            end
        end,
        ar_intervals:new(),
        ByteIntervals
    ).

%% @doc Return the first offset in (Start, End] whose chunk ends after Byte,
%% or End + 1 when none does.
first_chunk_ending_after(Byte, Start, End) ->
    first_footprint_offset(
        fun(Offset) ->
            {ChunkEnd, _ChunkStart} = chunk_byte_interval(Offset),
            ChunkEnd > Byte
        end,
        Start + 1,
        End + 1
    ).

%% @doc Return the first offset in (Start, End] whose chunk starts at or after
%% Byte, or End + 1 when none does.
first_chunk_starting_from(Byte, Start, End) ->
    first_footprint_offset(
        fun(Offset) ->
            {_ChunkEnd, ChunkStart} = chunk_byte_interval(Offset),
            ChunkStart >= Byte
        end,
        Start + 1,
        End + 1
    ).

%% @doc Return the first offset in [Low, High) satisfying Pred, or High; Pred
%% must stay true once true, as it does for a footprint's chunks.
first_footprint_offset(_Pred, Low, High) when Low >= High ->
    High;
first_footprint_offset(Pred, Low, High) ->
    Mid = (Low + High) div 2,
    case Pred(Mid) of
        true -> first_footprint_offset(Pred, Low, Mid);
        false -> first_footprint_offset(Pred, Mid + 1, High)
    end.

%% @doc Return the byte interval {End, Start} of the chunk a footprint offset
%% maps to: the 256 KiB bucket ending at
%% get_padded_offset_from_footprint_offset/1.
chunk_byte_interval(FootprintOffset) ->
    End = get_padded_offset_from_footprint_offset(FootprintOffset),
    {End, End - ?DATA_CHUNK_SIZE}.

%% @doc Return an upper bound on the footprint offsets reachable by a weave of
%% the given byte size: the per-partition footprint capacity times the number
%% of partitions touched by the weave.
max_offset(WeaveSize) when WeaveSize > 0 ->
    NumPartitions =
        (WeaveSize + arweave_constants:partition_size() - 1) div arweave_constants:partition_size(),
    NumPartitions * get_chunks_per_partition();
max_offset(_) ->
    0.

%% @doc Return true if a chunk containing the given Offset (=< EndOffset, > StartOffset)
%% is found in the footprint record.
is_recorded(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage:is_recorded(
        FootprintOffset,
        any_packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%%%===================================================================
%%% Private functions.
%%%===================================================================

state_db(StoreID) ->
    arweave_storage_sync_record:state_db(StoreID).

%% @doc The store's byte range, as the sync paths use it.
store_range(StoreID) ->
    case arweave_storage:store_info(StoreID) of
        #store_info{padded_range = Range} -> Range;
        not_found -> {-1, -1}
    end.

read_initialization_cursor(StateDB) ->
    case arweave_storage_deps:db_get(StateDB, ?INITIALIZATION_CURSOR_KEY) of
        {ok, <<"complete">>} -> complete;
        {ok, CursorBin} -> binary:decode_unsigned(CursorBin);
        not_found -> start
    end.

write_initialization_cursor(StateDB, Value) ->
    arweave_storage_deps:db_put(
        StateDB, ?INITIALIZATION_CURSOR_KEY, Value
    ).

schedule_initialization_step(Delay, StoreID, Cursor, EstimateReported) ->
    Step = {footprint_record_initialization_step, Cursor, EstimateReported},
    {ok, _} = arweave_storage_deps:apply_after(
        Delay,
        gen_server,
        cast,
        [arweave_storage_sync_record:name(StoreID), Step],
        #{skip_on_shutdown => false}
    ),
    ok.

%% @doc How long to wait before the next step: as long as a step may run, and
%% longer when this one wrote enough intervals to exceed
%% ?INITIALIZATION_ADDS_PER_SECOND.
next_initialization_step_ms(Added) ->
    max(
        ?INITIALIZATION_STEP_BUDGET_MS,
        Added * 1000 div ?INITIALIZATION_ADDS_PER_SECOND
    ).

%% @doc Tell the operator that the module is building its footprint index and
%% how long it is expected to take, projecting the span left from the ground
%% the step that just ran covered. Returns whether the estimate was reported;
%% a step that covered nothing reports nothing and leaves it to the next one.
report_initialization_estimate(Cursor, NewCursor, Delay, StoreID) ->
    {RangeStart, RangeEnd} = store_range(StoreID),
    {First, End} = span(RangeStart, RangeEnd),
    Covered =
        NewCursor -
            case Cursor of
                start -> First;
                _ -> Cursor
            end,
    case Covered > 0 of
        false ->
            false;
        true ->
            Steps = (End - NewCursor + Covered - 1) div Covered,
            RemainingMs = Steps * (?INITIALIZATION_STEP_BUDGET_MS + Delay),
            Pct = initialization_pct(NewCursor, StoreID),
            ?LOG_INFO([
                {event, footprint_record_initialization_estimate},
                {store_id, StoreID},
                {percent_migrated, Pct},
                {estimated_duration_ms, RemainingMs}
            ]),
            arweave_storage_deps:console(
                "~nThe storage module ~s is building its footprint index "
                "(~B% done at ~s, about ~s to go). It will not sync data "
                "from peers until this completes; mining and copying data "
                "from the other local modules are not affected.~n",
                [
                    StoreID,
                    Pct,
                    arweave_util:utc_timestamp(),
                    arweave_util:format_duration_ms(RemainingMs)
                ]
            ),
            true
    end.

%% @doc Percentage (0-100) of the store's footprint span the initialization
%% cursor has passed.
initialization_pct(start, _StoreID) ->
    0;
initialization_pct(Cursor, StoreID) ->
    {RangeStart, RangeEnd} = store_range(StoreID),
    {First, End} = span(RangeStart, RangeEnd),
    case Cursor > First andalso End > First of
        true -> min(100, (Cursor - First) * 100 div (End - First));
        false -> 0
    end.

%% @doc Walk footprints from Cursor until the step's deadline passes or the
%% span ends, returning {NextCursor, Runs}: per-packing lists of merged
%% {End, Start} runs, latest first. The deadline is read after each footprint,
%% a thousand-odd lookups apart, so the check costs nothing measurable and a
%% step always covers at least one footprint.
walk_footprints(Cursor, #{end_offset := End} = Ctx, Runs) when Cursor >= End ->
    walk_done(Cursor, Ctx, Runs);
walk_footprints(Cursor, #{deadline := Deadline} = Ctx, Runs) ->
    {Next, Runs2} = walk_footprint(Cursor, Ctx, Runs),
    case erlang:monotonic_time(millisecond) >= Deadline of
        true -> walk_done(Next, Ctx, Runs2);
        false -> walk_footprints(Next, Ctx, Runs2)
    end.

%% @doc Take one footprint: collect its recorded offsets, or step over a
%% partition the {ar_data_sync, byte} record has nothing in. Returns the next
%% cursor and the runs it extended.
walk_footprint(Cursor, Ctx, Runs) ->
    {Partition, Footprint} = get_location_from_footprint_offset(Cursor + 1),
    case Footprint == 0 andalso is_partition_empty(Partition, Ctx) of
        true ->
            %% Skip to the next partition.
            {(Partition + 1) * get_chunks_per_partition(), Runs};
        false ->
            {Start, FootprintEnd} = footprint_range(Partition, Footprint),
            Runs2 = collect_footprint_runs(Start + 1, FootprintEnd, Ctx, Runs),
            {FootprintEnd, Runs2}
    end.

walk_done(Cursor, #{end_offset := End}, Runs) ->
    {min(Cursor, End), Runs}.

%% @doc Whether the {ar_data_sync, byte} record holds nothing in the
%% partition, with a chunk of slack either side since a chunk may map to a
%% neighbouring partition.
is_partition_empty(Partition, Ctx) ->
    #{range := {RangeStart, RangeEnd}, store_id := StoreID} = Ctx,
    PartitionSize = arweave_constants:partition_size(),
    Start = max(RangeStart, Partition * PartitionSize - ?DATA_CHUNK_SIZE),
    End = min(RangeEnd, (Partition + 1) * PartitionSize + ?DATA_CHUNK_SIZE),
    Start >= End orelse
        arweave_storage_sync_record:get_next_interval(
            synced, Start, End, any_packing, {ar_data_sync, byte}, StoreID
        ) == not_found.

%% @doc Look up in the {ar_data_sync, byte} record each footprint offset in
%% [Offset, FootprintEnd] and extend the packing runs with the recorded ones.
%% A partition's entropy covers a few more offsets than the partition has
%% chunks; those map to chunks of the next partition and are skipped.
collect_footprint_runs(Offset, FootprintEnd, _Ctx, Runs) when
    Offset > FootprintEnd
->
    Runs;
collect_footprint_runs(Offset, FootprintEnd, Ctx, Runs) ->
    #{range := {RangeStart, RangeEnd}, packings := Packings} = Ctx,
    PaddedOffset = get_padded_offset_from_footprint_offset(Offset),
    {Partition, _} = get_location_from_footprint_offset(Offset),
    InRange = PaddedOffset > RangeStart andalso PaddedOffset =< RangeEnd,
    Runs2 =
        case
            InRange andalso
                arweave_storage_deps:get_entropy_partition(PaddedOffset) ==
                    Partition
        of
            false ->
                Runs;
            true ->
                lists:foldl(
                    fun(Packing, Acc) ->
                        case is_byte_recorded(PaddedOffset, Packing, Ctx) of
                            true -> add_run(Packing, Offset, Acc);
                            false -> Acc
                        end
                    end,
                    Runs,
                    Packings
                )
        end,
    collect_footprint_runs(Offset + 1, FootprintEnd, Ctx, Runs2).

is_byte_recorded(PaddedOffset, Packing, #{store_id := StoreID}) ->
    arweave_storage_sync_record:is_recorded(
        PaddedOffset, Packing, {ar_data_sync, byte}, StoreID
    ).

%% @doc Record footprint offset Offset for Packing, extending the latest run
%% when it ends right before it.
add_run(Packing, Offset, Runs) ->
    case maps:get(Packing, Runs, []) of
        [{Offset0, Start} | Rest] when Offset0 == Offset - 1 ->
            Runs#{Packing => [{Offset, Start} | Rest]};
        PackingRuns ->
            Runs#{Packing => [{Offset, Offset - 1} | PackingRuns]}
    end.

get_chunks_per_partition() ->
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    arweave_util:pad_to_closest_multiple_equal_or_above(
        arweave_constants:partition_size(), ?DATA_CHUNK_SIZE * FootprintSize
    ) div ?DATA_CHUNK_SIZE.

footprint_range(Partition, Footprint) ->
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    PartitionStartOffset = Partition * ChunksPerPartition,
    Start = PartitionStartOffset + Footprint * FootprintSize,
    End = min(Start + FootprintSize, PartitionStartOffset + ChunksPerPartition),
    {Start, End}.

do_footprint_intervals_to_byte_intervals([], Intervals) ->
    Intervals;
do_footprint_intervals_to_byte_intervals([{End, Start} | Rest], Intervals) ->
    Intervals2 = do_footprint_intervals_to_byte_intervals(Start, End, Intervals),
    do_footprint_intervals_to_byte_intervals(Rest, Intervals2).

do_footprint_intervals_to_byte_intervals(Start, End, Intervals) when Start >= End ->
    Intervals;
do_footprint_intervals_to_byte_intervals(Start, End, Intervals) ->
    {ChunkEnd, ChunkStart} = chunk_byte_interval(Start + 1),
    Intervals2 = ar_intervals:add(Intervals, ChunkEnd, ChunkStart),
    do_footprint_intervals_to_byte_intervals(Start + 1, End, Intervals2).

%% @doc Return the start offset of the first bucket of the sector that is
%% SectorShift sectors after the one holding the given chunk. The last
%% sector of a partition overhangs the next one, so a shift past it lands
%% on the next partition's first bucket.
get_sector_bucket_start(AbsoluteChunkEndOffset, SectorShift) ->
    SectorSize = arweave_storage_deps:get_entropy_sector_size(),
    PartitionSize = arweave_constants:partition_size(),
    PartitionRelativeOffset =
        arweave_storage_deps:get_partition_offset(AbsoluteChunkEndOffset),
    Partition = arweave_storage_deps:get_entropy_partition(AbsoluteChunkEndOffset),
    Sector = PartitionRelativeOffset div SectorSize + SectorShift,
    SectorStart = min(
        Partition * PartitionSize + Sector * SectorSize,
        (Partition + 1) * PartitionSize
    ),
    arweave_util:floor_int(
        SectorStart + ?DATA_CHUNK_SIZE - 1, ?DATA_CHUNK_SIZE
    ).
