-module(arweave_storage_footprint_record).

-export([
    add/3,
    add_async/4,
    delete/2,
    initialize/1,
    continue_initialization/5,
    resume_initialization/1,
    mark_initialized/1,
    is_initialized/1,
    get_intervals/3,
    get_intervals/4,
    get_unsynced_intervals/3,
    is_recorded/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_storage/include/arweave_storage_deps.hrl").

%% The key of the store's own database holding the footprint offset the
%% initialization resumes from, <<"start">> before the first step, or
%% <<"complete">> once it is done. An absent key means it was not requested.
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
-define(INITIALIZATION_RETRY_MS, 1000).

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
    FootprintOffset = arweave_lib_footprint:get_footprint_offset(Offset),
    arweave_storage:add_sync_record(
        FootprintOffset, FootprintOffset - 1, Packing, {ar_data_sync, footprint}, StoreID
    ).

%% @doc Add a chunk to the footprint record asynchronously.
add_async(Tag, Offset, Packing, StoreID) ->
    FootprintOffset = arweave_lib_footprint:get_footprint_offset(Offset),
    arweave_storage_sync_record:add_async(
        Tag,
        FootprintOffset,
        FootprintOffset - 1,
        Packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% Initialization mutators execute in the store's sync record server so
%% cursor writes and interval/WAL updates remain serialized.
%% @doc Persist the initialization request before scheduling its first step.
initialize(StoreID) ->
    StateDB = state_db(StoreID),
    case read_initialization_cursor(StateDB) of
        not_found ->
            case write_initialization_cursor(StateDB, <<"start">>) of
                ok -> do_initialize(StoreID, start);
                Error -> Error
            end;
        complete ->
            ok;
        Cursor ->
            do_initialize(StoreID, Cursor)
    end.

%% @doc Run a bounded initialization step using the server's interval writer.
continue_initialization(
    StoreID, Cursor, EstimateReported, WriteIntervals, State
) ->
    case read_initialization_cursor(state_db(StoreID)) == Cursor of
        false ->
            %% A newer chain advanced the cursor or marked the record complete.
            State;
        true ->
            {Status, Next, Intervals} = walk_initialization(StoreID, Cursor),
            {Added, State2} = WriteIntervals(Intervals, State),
            ok = initialization_step_written(StoreID, #{
                status => Status,
                cursor => Cursor,
                next => Next,
                added => Added,
                estimate_reported => EstimateReported
            }),
            State2
    end.

%% @doc Resume a requested migration after the store's sync records load.
resume_initialization(StoreID) ->
    case read_initialization_cursor(state_db(StoreID)) of
        not_found -> ok;
        complete -> ok;
        Cursor -> do_initialize(StoreID, Cursor)
    end.

%% @doc Mark the footprint record complete from the store's sync record server.
mark_initialized(StoreID) ->
    write_initialization_cursor(state_db(StoreID), <<"complete">>).

%% @doc Return whether the store's footprint record is built.
is_initialized(StoreID) ->
    case arweave_storage_module:get_by_id(StoreID) of
        Module when is_atom(Module) ->
            %% In-memory stores have no migration to run.
            true;
        _ ->
            read_initialization_cursor(state_db(StoreID)) == complete
    end.

%% @doc Commit a step's cursor and schedule the next step or a retry.
initialization_step_written(StoreID, Step) ->
    #{
        status := Status,
        cursor := Cursor,
        next := Next,
        added := Added,
        estimate_reported := EstimateReported
    } = Step,
    Value = case Status of
        complete -> <<"complete">>;
        continue -> binary:encode_unsigned(Next)
    end,
    case write_initialization_cursor(state_db(StoreID), Value) of
        ok ->
            do_initialization_step_written(StoreID, Step);
        {error, Reason} ->
            ?LOG_WARNING([
                {event, footprint_record_initialization_write_failed},
                {store_id, StoreID}, {cursor, Cursor}, {reason, Reason}
            ]),
            %% The intervals are already in the WAL. Repeating them is safe;
            %% keep the persisted cursor until all of this step is committed.
            schedule_initialization_step(
                max(
                    ?INITIALIZATION_RETRY_MS,
                    next_initialization_step_ms(Added)
                ),
                Cursor,
                EstimateReported
            )
    end.

%% @doc Return {Status, NextCursor, Intervals} for a bounded footprint walk.
walk_initialization(StoreID, Cursor) ->
    do_walk_initialization(
        StoreID, store_range(StoreID), Cursor, ?INITIALIZATION_STEP_BUDGET_MS
    ).

do_walk_initialization(StoreID, {RangeStart, RangeEnd}, start, BudgetMs) ->
    {First, _} = do_get_span(RangeStart, RangeEnd),
    do_walk_initialization(
        StoreID, {RangeStart, RangeEnd}, First, BudgetMs
    );
do_walk_initialization(
    StoreID, {RangeStart, RangeEnd}, Cursor, BudgetMs
) ->
    Packings = [
        Packing
     || Packing <- arweave_storage_sync_record:get_packings(
            {ar_data_sync, byte}, StoreID
        ),
        Packing =/= unpacked_padded
    ],
    {_, End} = do_get_span(RangeStart, RangeEnd),
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

%% @doc Return the footprint bounds covering the store's padded byte range.
get_span(StoreID) ->
    {RangeStart, RangeEnd} = store_range(StoreID),
    do_get_span(RangeStart, RangeEnd).

%% @doc Return {First, End}: the start of the first footprint a chunk of the
%% byte range can map to and the end of the last one.
do_get_span(RangeStart, RangeEnd) ->
    ChunksPerPartition = arweave_lib_footprint:get_chunks_per_partition(),
    FirstPartition =
        arweave_lib_replica_2_9:get_entropy_partition(RangeStart + 1),
    LastPartition =
        arweave_lib_replica_2_9:get_entropy_partition(RangeEnd),
    {FirstPartition * ChunksPerPartition,
        (LastPartition + 1) * ChunksPerPartition}.

%% @doc Get the synced footprint intervals of a chunk.
get_intervals(Partition, Footprint, StoreID) ->
    get_intervals(Partition, Footprint, any, StoreID).

%% @doc Get the synced footprint intervals of a chunk.
get_intervals(Partition, Footprint, any, StoreID) ->
    {Start, End} =
        arweave_lib_footprint:get_footprint_range(Partition, Footprint),
    arweave_storage_sync_record:get_intervals(
        synced, Start, End, any_packing, {ar_data_sync, footprint}, StoreID
    );
get_intervals(Partition, Footprint, Packing, StoreID) ->
    {Start, End} =
        arweave_lib_footprint:get_footprint_range(Partition, Footprint),
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
    {Start, End} =
        arweave_lib_footprint:get_footprint_range(Partition, Footprint),
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
    FootprintOffset = arweave_lib_footprint:get_footprint_offset(Offset),
    arweave_storage:delete_sync_record(
        FootprintOffset, FootprintOffset - 1, {ar_data_sync, footprint}, StoreID
    ).

%% @doc Return true if a chunk containing the given Offset (=< EndOffset, > StartOffset)
%% is found in the footprint record.
is_recorded(Offset, StoreID) ->
    FootprintOffset = arweave_lib_footprint:get_footprint_offset(Offset),
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

do_initialization_step_written(StoreID, #{status := complete}) ->
    ?LOG_INFO([
        {event, footprint_record_initialized}, {store_id, StoreID}
    ]),
    ?DEP(console):console(
        "~nThe storage module ~s finished building its footprint "
        "index and can now sync data from peers.~n",
        [StoreID]
    );
do_initialization_step_written(StoreID, Step) ->
    #{cursor := Cursor, next := Next, added := Added,
        estimate_reported := EstimateReported} = Step,
    Delay = next_initialization_step_ms(Added),
    EstimateReported2 = EstimateReported orelse
        report_initialization_estimate(Cursor, Next, Delay, StoreID),
    schedule_initialization_step(
        Delay, Next, EstimateReported2
    ).

read_initialization_cursor(StateDB) ->
    case ?DEP(kv):get(StateDB, ?INITIALIZATION_CURSOR_KEY) of
        {ok, <<"start">>} -> start;
        {ok, <<"complete">>} -> complete;
        {ok, CursorBin} -> binary:decode_unsigned(CursorBin);
        not_found -> not_found
    end.

do_initialize(StoreID, Cursor) ->
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
    schedule_initialization_step(0, Cursor, false).

write_initialization_cursor(StateDB, Value) ->
    ?DEP(kv):put(StateDB, ?INITIALIZATION_CURSOR_KEY, Value).

schedule_initialization_step(Delay, Cursor, EstimateReported) ->
    Step = {continue_footprint_record_initialization, Cursor, EstimateReported},
    {ok, _} = ?DEP(clock):apply_after(
        Delay,
        gen_server,
        cast,
        [self(), Step],
        #{skip_on_shutdown => false}
    ),
    ok.

%% @doc Pace each step by its time budget and interval write rate.
next_initialization_step_ms(Added) ->
    max(
        ?INITIALIZATION_STEP_BUDGET_MS,
        Added * 1000 div ?INITIALIZATION_ADDS_PER_SECOND
    ).

%% @doc Report an initialization estimate once a step makes measurable progress.
report_initialization_estimate(Cursor, NewCursor, Delay, StoreID) ->
    {First, End} = get_span(StoreID),
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
            ?DEP(console):console(
                "~nThe storage module ~s is building its footprint index "
                "(~B% done at ~s, about ~s to go). It will not sync data "
                "from peers until this completes; mining and copying data "
                "from the other local modules are not affected.~n",
                [
                    StoreID,
                    Pct,
                    calendar:system_time_to_rfc3339(
                        ?DEP(clock):system_ms() div 1000, [{offset, "Z"}]),
                    arweave_lib_util:format_duration_ms(RemainingMs)
                ]
            ),
            true
    end.

%% @doc Return the percentage of the store's footprint span already initialized.
initialization_pct(start, _StoreID) ->
    0;
initialization_pct(Cursor, StoreID) ->
    {First, End} = get_span(StoreID),
    case Cursor > First andalso End > First of
        true -> min(100, (Cursor - First) * 100 div (End - First));
        false -> 0
    end.

%% @doc The store's byte range, as the sync paths use it.
store_range(StoreID) ->
    case arweave_storage:store_info(StoreID) of
        #store_info{padded_range = Range} -> Range;
        not_found -> {-1, -1}
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
    {Partition, Footprint} =
        arweave_lib_footprint:get_location_from_footprint_offset(Cursor + 1),
    case Footprint == 0 andalso is_partition_empty(Partition, Ctx) of
        true ->
            %% Skip to the next partition.
            {(Partition + 1) * arweave_lib_footprint:get_chunks_per_partition(), Runs};
        false ->
            {Start, FootprintEnd} = arweave_lib_footprint:get_footprint_range(Partition, Footprint),
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
    PartitionSize = arweave_lib_constants:partition_size(),
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
    PaddedOffset =
        arweave_lib_footprint:get_padded_offset_from_footprint_offset(Offset),
    {Partition, _} =
        arweave_lib_footprint:get_location_from_footprint_offset(Offset),
    InRange = PaddedOffset > RangeStart andalso PaddedOffset =< RangeEnd,
    Runs2 =
        case
            InRange andalso
                arweave_lib_replica_2_9:get_entropy_partition(
                    PaddedOffset
                ) == Partition
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

