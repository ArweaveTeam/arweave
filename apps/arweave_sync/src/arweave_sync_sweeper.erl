%%% @doc One sweeper per store: walks the store range, finds the chunks the
%%% store lacks, and offers them to arweave_sync_scheduler as work.
%%% ar_data_sync starts the sweep loop (start/1) and sends each new weave size
%%% (set_weave_size/2); the loop then schedules its own sweep steps.
%%%
%%% A sweep (initialize_sweep/1) starts two modes at the store start, each
%%% with its own cursor offset. The first sweep after the sweeper starts
%%% resumes instead at the offsets last saved (maybe_save_cursor/1), so a
%%% node restart does not send the sweep back over ranges it had passed. The
%%% saved offsets stay at the first parked range, so a restart sweeps the
%%% parked ranges again. The modes are:
%%% - byte: one QUERY_RANGE_STEP_SIZE grid step at a time, up to the weave
%%%   size;
%%% - footprint: one footprint at a time, up to the lower of the weave size
%%%   and the disk pool threshold.
%%%
%%% Each sweep step runs:
%%% 1. Gate (do_sweep/1, can_sweep/1): continue only when sync is enabled,
%%%    the store holds the device lock in sync mode, the node has joined, the
%%%    footprint record is initialized and the disk has space. Otherwise try
%%%    again later; when the device lock reports off or complete, stop until
%%%    the sweeper restarts.
%%% 2. Readahead (fill_sweep_queues/1): in each mode, find the next unsynced
%%%    range past the readahead cursor, ask arweave_sync_discovery to start
%%%    fetching peer metadata for the range, and add the range to the mode's
%%%    sweep queue.
%%% 3. Claim (process_sweep_queues/1): take the head of one sweep queue,
%%%    alternating modes, once discovery has had SWEEP_RANGE_WARM_WAIT_MS to
%%%    fetch the metadata. Check again what the store still lacks, and pass
%%%    the range and the cached peer ranges to
%%%    arweave_sync_chunk_picker:claim_ranges/3, which builds the work and
%%%    offers it to the scheduler. Then move the cursor past the range. If the
%%%    store's claim limit stops the claim, keep what is left of the range at
%%%    the head of the queue and retry after BLOCKED_RETRY_DELAY_MS. If the
%%%    claim skipped chunks because their sources had no room
%%%    (arweave_sync_store:full_peers/2), park the range (park_sweep_range/3),
%%%    or, when the mode already holds its sweep queue length of parked
%%%    ranges and the range is not one of them, keep the range at the head
%%%    and retry after BLOCKED_RETRY_DELAY_MS.
%%%    Each step first claims again from the parked range due first
%%%    (retry_parked_range/1), every PARKED_RANGE_RETRY_DELAY_MS, so a peer
%%%    whose data the sweep has passed stays busy.
%%% 4. Complete (complete_sweep/1): once both modes reach their live ends,
%%%    save the store start as the cursor offsets and start a new sweep after
%%%    SWEEP_RESTART_DELAY_MS.
%%%
%%% The sweeper's state:
%%% - cursor: the offset each mode has processed up to;
%%% - readahead_cursor: the end of the ranges already in the sweep queues;
%%% - sweep_queues: the queued ranges of each mode, up to
%%%   BYTE_SWEEP_QUEUE_MAX_LENGTH and FOOTPRINT_SWEEP_QUEUE_MAX_LENGTH;
%%% - parked_ranges: the parked ranges of each mode, up to the same lengths,
%%%   kept across sweeps;
%%% - weave_size, disk_pool_threshold: the bounds that set the live ends.
-module(arweave_sync_sweeper).

-ifdef(AR_TEST).
-export([
    byte_range_end/2,
    can_sweep/1,
    completed_range_delay/2,
    do_enqueue_sweep_range/3,
    frontier_offset/2,
    initialize_sweep/1,
    maybe_save_cursor/1,
    park_sweep_range/3,
    process_next_sweep_range/2,
    process_sweep_queues/1,
    process_sweep_range/2,
    read_resume_offsets/3,
    sweep_queue/2,
    unsynced_intervals/3,
    unsynced_footprint_intervals/3
]).
-endif.

-behaviour(gen_server).

-export([start_link/1, name/1, register_workers/0, store_ids/0]).
-export([start/1, set_weave_size/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include("arweave_sync_sweeper.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(StoreID) ->
    gen_server:start_link({local, name(StoreID)}, ?MODULE, StoreID, []).

name(?DEFAULT_MODULE) ->
    arweave_sync_sweeper_default;
name(StoreID) ->
    list_to_atom(
        "arweave_sync_sweeper_" ++
            (?DEP(storage):store_info(StoreID))#store_info.label
    ).

register_workers() ->
    [?CHILD_WITH_ARGS(?MODULE, worker, name(SID), [SID]) || SID <- store_ids()].

%% @doc Return the IDs of the stores that have a sweeper: every configured
%% storage module, but not the default store, which holds only transient data.
store_ids() ->
    [
        (?DEP(storage):store_info(Module))#store_info.id
     || Module <- ?DEP(config):storage_modules()
    ].

%% @doc Schedule an immediate sweep of StoreID.
start(StoreID) ->
    gen_server:cast(name(StoreID), start).

%% @doc Send the new weave size to the sweeper, which also reads the current
%% disk pool threshold when it handles the update.
set_weave_size(StoreID, WeaveSize) ->
    gen_server:cast(name(StoreID), {set_weave_size, WeaveSize}).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
    arweave_sync:restore_store(StoreID),
    {RangeStart, RangeEnd} =
        case ?DEP(storage):store_info(StoreID) of
            #store_info{padded_range = Range} -> Range;
            not_found -> {-1, -1}
        end,
    ?LOG_INFO([
        {event, init},
        {module, ?MODULE},
        {store_id, StoreID},
        {range_start, RangeStart},
        {range_end, RangeEnd}
    ]),
    {ok, #state{
        store_id = StoreID,
        range_start = RangeStart,
        range_end = RangeEnd,
        sync_status = ?DEP(data_sync):init_sync_status(StoreID),
        resume_offsets = read_resume_offsets(StoreID, RangeStart, RangeEnd)
    }}.

handle_call(ping, _From, State) ->
    {reply, pong, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

handle_cast(start, State) ->
    %% Sweeping early is safe: a queued range is not claimed until its
    %% SWEEP_RANGE_WARM_WAIT_MS has passed.
    {noreply, schedule_sweep(0, State)};
%% A smaller weave size after a reorg needs no special handling: the cursors'
%% live ends shrink with it. Work already submitted past the new bounds can
%% still fetch from a peer on the old fork. The chunk writer then skips the
%% chunk, whose proof no longer matches the block index, and its claim is
%% released as usual.
handle_cast({set_weave_size, WeaveSize}, State) ->
    DiskPoolThreshold = ?DEP(disk_pool):get_threshold(),
    {noreply, State#state{
        weave_size = WeaveSize,
        disk_pool_threshold = DiskPoolThreshold
    }};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(
    {sweep, Token},
    #state{next_sweep = {_DueMs, Token}} = State
) ->
    do_sweep(State#state{next_sweep = undefined});
handle_info({sweep, _StaleToken}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([
        {event, terminate},
        {module, ?MODULE},
        {reason, io_lib:format("~p", [Reason])}
    ]),
    ok.

%%%===================================================================
%%% Gate.
%%%===================================================================

do_sweep(State) ->
    case arweave_sync:enabled() of
        false ->
            {noreply, schedule_sweep(?NODE_JOIN_RETRY_DELAY_MS, State)};
        true ->
            sweep_with_device_lock(State)
    end.

sweep_with_device_lock(State) ->
    Status = ?DEP(device_lock):acquire_lock(
        sync, State#state.store_id, State#state.sync_status
    ),
    State2 = State#state{sync_status = Status},
    case Status of
        active ->
            sweep(State2);
        paused ->
            {noreply, schedule_sweep(?DEVICE_LOCK_WAIT, State2)};
        _ ->
            %% off or complete: acquire_lock/3 keeps returning the same
            %% status, so the sweep loop stops until the sweeper restarts.
            {noreply, State2}
    end.

sweep(#state{cursor = undefined} = State) ->
    State2 = initialize_sweep(State),
    case can_sweep(State2) of
        {blocked, Delay} ->
            {noreply, schedule_sweep(Delay, State)};
        complete ->
            %% Either the store starts above both live ends, so there is
            %% nothing to sweep yet, or the saved offsets had reached them.
            %% The next sweep starts at the store start.
            {noreply, schedule_sweep(
                ?NODE_JOIN_RETRY_DELAY_MS,
                State#state{resume_offsets = undefined}
            )};
        ready ->
            ?LOG_DEBUG([
                {event, sync_network},
                {stage, sweep_started},
                {store_id, State#state.store_id},
                {range_start, State#state.range_start},
                {range_end, State#state.range_end}
            ]),
            sweep_ready(State2)
    end;
sweep(State) ->
    publish_sweep_metrics(State),
    State2 = maybe_save_cursor(State),
    case can_sweep(State2) of
        {blocked, Delay} ->
            {noreply, schedule_sweep(Delay, State2)};
        complete ->
            complete_sweep(State2);
        ready ->
            sweep_ready(State2)
    end.

initialize_sweep(State) ->
    Cursor = resume_cursor(
        State#state.resume_offsets,
        arweave_sync_cursor:new(State#state.range_start, State#state.range_end)
    ),
    State#state{
        cursor = Cursor,
        readahead_cursor = Cursor,
        sweep_queues = new_sweep_queues(),
        next_mode = byte,
        resume_offsets = undefined
    }.

resume_cursor(undefined, Cursor) ->
    Cursor;
resume_cursor({Byte, Footprint}, Cursor) ->
    arweave_sync_cursor:set(
        footprint, Footprint, arweave_sync_cursor:set(byte, Byte, Cursor)
    ).

%% @doc Return the byte and footprint offsets saved for the store if both lie
%% in the store range, or undefined.
read_resume_offsets(StoreID, RangeStart, RangeEnd) ->
    case ?DEP(storage):read_sweep_cursor(StoreID) of
        {ok, {Byte, Footprint} = Offsets}
                when is_integer(Byte), is_integer(Footprint),
                    Byte >= RangeStart, Byte =< RangeEnd,
                    Footprint >= RangeStart, Footprint =< RangeEnd ->
            Offsets;
        _ ->
            undefined
    end.

%% @doc Save the cursor offsets if they have changed and the last save is at
%% least SWEEP_CURSOR_SAVE_INTERVAL_MS old.
maybe_save_cursor(#state{cursor = undefined} = State) ->
    State;
maybe_save_cursor(State) ->
    #state{saved_at_ms = SavedAtMs} = State,
    Offsets = {resume_offset(byte, State), resume_offset(footprint, State)},
    NowMs = ?DEP(clock):monotonic_ms(),
    IsDue =
        SavedAtMs == undefined orelse
            NowMs - SavedAtMs >= ?SWEEP_CURSOR_SAVE_INTERVAL_MS,
    case Offsets =/= State#state.saved_offsets andalso IsDue of
        true -> save_offsets(Offsets, NowMs, State);
        false -> State
    end.

%% @doc Return the offset a restart resumes the mode from: the cursor, or the
%% first parked range before it. Parked ranges are not saved, so a restart
%% sweeps them again instead of waiting for the next sweep.
resume_offset(Mode, State) ->
    lists:foldl(
        fun(#sweep_range{offset = Offset}, Min) -> min(Offset, Min) end,
        arweave_sync_cursor:current(Mode, State#state.cursor),
        parked_ranges(Mode, State)
    ).

save_offsets(Offsets, NowMs, State) ->
    StoreID = State#state.store_id,
    case ?DEP(storage):write_sweep_cursor(Offsets, StoreID) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING([
                {event, failed_to_save_sweep_cursor},
                {store_id, StoreID},
                {reason, io_lib:format("~p", [Reason])}
            ])
    end,
    State#state{saved_offsets = Offsets, saved_at_ms = NowMs}.

%% @doc Check whether the node, store and disk are ready to sweep, and whether
%% the sweep has reached its live ends.
can_sweep(State) ->
    #state{
        store_id = StoreID,
        weave_size = WeaveSize,
        disk_pool_threshold = DiskPoolThreshold,
        cursor = Cursor
    } = State,
    Ready =
        WeaveSize =/= undefined andalso
            DiskPoolThreshold =/= undefined andalso
            ?DEP(node):is_joined() andalso
            ?DEP(storage):is_footprint_record_initialized(StoreID),
    case Ready of
        false ->
            {blocked, ?NODE_JOIN_RETRY_DELAY_MS};
        true ->
            case ?DEP(data_sync):is_disk_space_sufficient(StoreID) of
                false ->
                    {blocked, 30_000};
                not_initialized ->
                    {blocked, 1_000};
                true ->
                    case
                        arweave_sync_cursor:is_complete(
                            Cursor, WeaveSize, DiskPoolThreshold
                        )
                    of
                        true -> complete;
                        false -> ready
                    end
            end
    end.

sweep_ready(State) ->
    State2 = retry_parked_range(fill_sweep_queues(State)),
    case process_sweep_queues(State2) of
        {ok, State3} ->
            Delay = completed_range_delay(State2, State3),
            {noreply, schedule_sweep(Delay, State3)};
        {blocked, Delay, State3} ->
            NextDelay =
                case can_extend_sweep_queue(State3) of
                    true -> 0;
                    false -> min(Delay, parked_range_delay(State3))
                end,
            {noreply, schedule_sweep(NextDelay, State3)};
        done ->
            %% Both sweep queues are empty. The next step either refills them
            %% or finds that the sweep is complete.
            {noreply, schedule_sweep(0, State2)}
    end.

%%%===================================================================
%%% Readahead.
%%%===================================================================

fill_sweep_queues(State) ->
    State2 = fill_sweep_queue(byte, State),
    fill_sweep_queue(footprint, State2).

fill_sweep_queue(Mode, State) ->
    Queue = sweep_queue(Mode, State),
    ReadaheadCursor = State#state.readahead_cursor,
    LiveEnd = arweave_sync_cursor:live_end(
        Mode,
        ReadaheadCursor,
        State#state.weave_size,
        State#state.disk_pool_threshold
    ),
    Offset = arweave_sync_cursor:current(Mode, ReadaheadCursor),
    case
        queue:len(Queue) >= sweep_queue_max_length(Mode) orelse
            Offset >= LiveEnd
    of
        true ->
            State;
        false ->
            {UnsyncedRange, ReadaheadCursor2} = next_unsynced_range(
                Mode,
                State#state.store_id,
                ReadaheadCursor,
                State#state.weave_size,
                State#state.disk_pool_threshold
            ),
            enqueue_sweep_range(
                Mode, Offset, UnsyncedRange, ReadaheadCursor2, State
            )
    end.

next_unsynced_range(Mode, StoreID, Cursor, WeaveSize, DiskPoolThreshold) ->
    Offset = arweave_sync_cursor:current(Mode, Cursor),
    case
        find_unsynced_range(
            Mode,
            Offset,
            StoreID,
            Cursor,
            WeaveSize,
            DiskPoolThreshold
        )
    of
        done ->
            {none, Cursor};
        {no_need, NextOffset} ->
            {none, arweave_sync_cursor:set(Mode, NextOffset, Cursor)};
        {need, UnsyncedRange} ->
            {UnsyncedRange, Cursor}
    end.

enqueue_sweep_range(Mode, Offset, none, ReadaheadCursor, State) ->
    %% Queue a placeholder anyway, so that the cursor moves past this step
    %% only after the earlier queued ranges are processed.
    do_enqueue_sweep_range(
        #sweep_range{
            mode = Mode,
            offset = Offset,
            next_offset = arweave_sync_cursor:current(Mode, ReadaheadCursor),
            requested_at = ?DEP(clock):monotonic_ms()
        },
        ReadaheadCursor,
        State
    );
enqueue_sweep_range(Mode, Offset, UnsyncedRange, ReadaheadCursor, State) ->
    #unsynced_range{query_offset = QueryOffset} = UnsyncedRange,
    Peers = candidate_peers(QueryOffset),
    %% Ask discovery to start fetching peer metadata now, so that the metadata
    %% is likely cached by the time the sweeper processes this range.
    ok = arweave_sync_discovery:warm_peer_ranges(
        State#state.store_id, Peers, QueryOffset
    ),
    %% Only the readahead cursor moves past the range now; the main cursor
    %% moves once the range is processed.
    ReadaheadCursor2 = arweave_sync_cursor:advance(UnsyncedRange, ReadaheadCursor),
    do_enqueue_sweep_range(
        #sweep_range{
            mode = Mode,
            offset = Offset,
            next_offset = arweave_sync_cursor:current(Mode, ReadaheadCursor2),
            unsynced_range = UnsyncedRange,
            requested_at = ?DEP(clock):monotonic_ms()
        },
        ReadaheadCursor2,
        State
    ).

do_enqueue_sweep_range(SweepRange, ReadaheadCursor, State) ->
    Mode = SweepRange#sweep_range.mode,
    Queue = sweep_queue(Mode, State),
    Queue2 = queue:in(SweepRange, Queue),
    set_sweep_queue(
        Mode,
        Queue2,
        State#state{readahead_cursor = ReadaheadCursor}
    ).

can_extend_sweep_queue(Mode, State) ->
    ReadaheadCursor = State#state.readahead_cursor,
    queue:len(sweep_queue(Mode, State)) < sweep_queue_max_length(Mode) andalso
        arweave_sync_cursor:current(Mode, ReadaheadCursor) <
            arweave_sync_cursor:live_end(
                Mode,
                ReadaheadCursor,
                State#state.weave_size,
                State#state.disk_pool_threshold
            ).

can_extend_sweep_queue(State) ->
    can_extend_sweep_queue(byte, State) orelse
        can_extend_sweep_queue(footprint, State).

%%%===================================================================
%%% Claim.
%%%===================================================================

%% @doc Process at most one queued range, taking turns between the byte and
%% footprint queues and trying the other queue when the first is blocked or
%% empty.
process_sweep_queues(State) ->
    FirstMode = State#state.next_mode,
    SecondMode = other_mode(FirstMode),
    case process_next_sweep_range(FirstMode, State) of
        {ok, State2} ->
            {ok, State2#state{next_mode = SecondMode}};
        {FirstResult, StateAfterFirst} ->
            case process_next_sweep_range(SecondMode, StateAfterFirst) of
                {ok, State2} ->
                    {ok, State2#state{next_mode = FirstMode}};
                {SecondResult, State2} ->
                    block_if_either_mode_blocked(
                        FirstResult, SecondResult, State2
                    )
            end
    end.

other_mode(byte) -> footprint;
other_mode(footprint) -> byte.

block_if_either_mode_blocked(done, done, _State) ->
    done;
block_if_either_mode_blocked({blocked, Delay}, done, State) ->
    {blocked, Delay, State};
block_if_either_mode_blocked(done, {blocked, Delay}, State) ->
    {blocked, Delay, State};
block_if_either_mode_blocked(
    {blocked, Delay1}, {blocked, Delay2}, State
) ->
    {blocked, min(Delay1, Delay2), State}.

process_next_sweep_range(Mode, State) ->
    case queue:peek(sweep_queue(Mode, State)) of
        empty ->
            {done, State};
        {value, #sweep_range{unsynced_range = none} = SweepRange} ->
            {ok, finish_sweep_range(SweepRange, State)};
        {value, SweepRange} ->
            process_sweep_range(SweepRange, State)
    end.

%% @doc Update the head of the sweep queue with the outcome of its claim.
apply_claim_outcome(SweepRange, finished, State) ->
    finish_sweep_range(SweepRange, State);
apply_claim_outcome(_SweepRange, {continue, SweepRange2}, State) ->
    replace_sweep_queue_head(SweepRange2, State);
apply_claim_outcome(SweepRange, {parked, SweepRange2}, State) ->
    RetryAtMs = ?DEP(clock):monotonic_ms() + ?PARKED_RANGE_RETRY_DELAY_MS,
    park_sweep_range(
        SweepRange2, RetryAtMs, finish_sweep_range(SweepRange, State)
    ).

process_sweep_range(
    #sweep_range{requested_at = RequestedAt} = SweepRange, State
) ->
    WarmDelay = max(
        0,
        RequestedAt + ?SWEEP_RANGE_WARM_WAIT_MS -
            ?DEP(clock):monotonic_ms()
    ),
    case WarmDelay of
        0 ->
            case claim_sweep_range(SweepRange, State) of
                {{blocked, _Delay}, _State} = Blocked ->
                    Blocked;
                {parked, SweepRange2} = Outcome ->
                    Mode = SweepRange#sweep_range.mode,
                    case has_parking_room(Mode, State)
                            orelse is_parked(SweepRange, State) of
                        true ->
                            {ok, apply_claim_outcome(
                                SweepRange, Outcome, State)};
                        false ->
                            %% Keep the range at the head so the sweep does
                            %% not run further ahead of the peers with no room.
                            {{blocked, ?BLOCKED_RETRY_DELAY_MS},
                                replace_sweep_queue_head(
                                    resume_at_skipped(SweepRange2), State)}
                    end;
                Outcome ->
                    {ok, apply_claim_outcome(SweepRange, Outcome, State)}
            end;
        _ ->
            {{blocked, WarmDelay}, State}
    end.

%% @doc Claim the chunks of the range that its sources have room for, and
%% return the outcome:
%% - {{blocked, Delay}, State}: the claim limit refused the claim;
%% - finished: the store needs nothing more from the range;
%% - {continue, SweepRange2}: the claim stopped at the claim limit, and
%%   SweepRange2 resumes after it;
%% - {parked, SweepRange2}: the claim reached the end of the range but skipped
%%   chunks whose sources had no room, from SweepRange2's skipped_from on.
claim_sweep_range(SweepRange, State) ->
    #sweep_range{unsynced_range = UnsyncedRange} = SweepRange,
    #unsynced_range{
        query_offset = QueryOffset,
        range_start = RangeStart,
        range_end = RangeEnd
    } = UnsyncedRange,
    Peers = candidate_peers(QueryOffset),
    %% Ask discovery again, for peers that appeared since the range was queued
    %% and for requests that discovery could not queue before. The claim below
    %% does not wait for these; it uses only the metadata already cached.
    ok = arweave_sync_discovery:warm_peer_ranges(
        State#state.store_id, Peers, QueryOffset
    ),
    {PeerRanges, _CacheStatus} = arweave_sync_discovery:cached_peer_ranges(
        State#state.store_id, Peers, QueryOffset, RangeStart, RangeEnd
    ),
    revalidate_and_claim(SweepRange, PeerRanges, State).

revalidate_and_claim(SweepRange, PeerRanges, State) ->
    #sweep_range{mode = Mode, offset = Offset} = SweepRange,
    case
        find_unsynced_range(
            Mode,
            Offset,
            State#state.store_id,
            State#state.cursor,
            State#state.weave_size,
            State#state.disk_pool_threshold
        )
    of
        {need, UnsyncedRange} ->
            UnsyncedRange2 = resume_unsynced_range(SweepRange, UnsyncedRange),
            case
                arweave_lib_intervals:is_empty(
                    UnsyncedRange2#unsynced_range.intervals
                )
            of
                true ->
                    range_end_outcome(SweepRange);
                false ->
                    claim_range(SweepRange, UnsyncedRange2, PeerRanges, State)
            end;
        _NoLongerNeeded ->
            finished
    end.

claim_range(SweepRange, UnsyncedRange, PeerRanges, State) ->
    case
        arweave_sync_chunk_picker:claim_ranges(
            State#state.store_id, [UnsyncedRange], PeerRanges
        )
    of
        blocked ->
            {{blocked, ?BLOCKED_RETRY_DELAY_MS}, State};
        {ok, _ClaimedChunks, NextClaimOffset, FirstSkipped} ->
            SweepRange2 = add_skipped(FirstSkipped, SweepRange),
            case NextClaimOffset of
                none ->
                    range_end_outcome(SweepRange2);
                _ ->
                    {continue, SweepRange2#sweep_range{
                        next_claim_offset = NextClaimOffset
                    }}
            end
    end.

add_skipped(none, SweepRange) ->
    SweepRange;
add_skipped(Offset, #sweep_range{skipped_from = undefined} = SweepRange) ->
    SweepRange#sweep_range{skipped_from = Offset};
add_skipped(Offset, #sweep_range{skipped_from = SkippedFrom} = SweepRange) ->
    SweepRange#sweep_range{skipped_from = min(Offset, SkippedFrom)}.

range_end_outcome(#sweep_range{skipped_from = undefined}) ->
    finished;
range_end_outcome(SweepRange) ->
    {parked, SweepRange}.

resume_unsynced_range(
    #sweep_range{next_claim_offset = undefined}, UnsyncedRange
) ->
    UnsyncedRange;
resume_unsynced_range(
    #sweep_range{next_claim_offset = NextClaimOffset},
    #unsynced_range{range_end = RangeEnd} = UnsyncedRange
) when NextClaimOffset >= RangeEnd ->
    %% The last claim ended at the live end, or the live end has since moved
    %% below it, so nothing is left to claim.
    UnsyncedRange#unsynced_range{intervals = arweave_lib_intervals:new()};
resume_unsynced_range(
    #sweep_range{next_claim_offset = NextClaimOffset},
    UnsyncedRange
) ->
    #unsynced_range{intervals = Intervals, range_end = RangeEnd} =
        UnsyncedRange,
    Remaining = arweave_lib_intervals:from_list([{RangeEnd, NextClaimOffset}]),
    UnsyncedRange#unsynced_range{
        intervals = arweave_lib_intervals:intersection(Intervals, Remaining)
    }.

replace_sweep_queue_head(SweepRange, State) ->
    Mode = SweepRange#sweep_range.mode,
    {{value, _Head}, Queue2} = queue:out(sweep_queue(Mode, State)),
    set_sweep_queue(Mode, queue:in_r(SweepRange, Queue2), State).

finish_sweep_range(SweepRange, State) ->
    #sweep_range{mode = Mode, next_offset = NextOffset} = SweepRange,
    {{value, SweepRange}, Queue2} = queue:out(sweep_queue(Mode, State)),
    State2 = set_sweep_queue(Mode, Queue2, State),
    State3 =
        case NextOffset of
            undefined ->
                State2;
            _ ->
                Cursor2 = arweave_sync_cursor:set(
                    Mode, NextOffset, State#state.cursor
                ),
                State2#state{cursor = Cursor2}
        end,
    State3.

%%%===================================================================
%%% Claim: parked ranges.
%%%===================================================================

%% @doc Return whether the mode has fewer parked ranges than its sweep queue
%% length. The sweep stops at a range it cannot park.
has_parking_room(Mode, State) ->
    length(parked_ranges(Mode, State)) < sweep_queue_max_length(Mode).

%% @doc Return whether a range at the same offset is parked. Parking the range
%% again only merges it into the parked one, so it needs no room.
is_parked(#sweep_range{mode = Mode, offset = Offset}, State) ->
    lists:keymember(Offset, #sweep_range.offset, parked_ranges(Mode, State)).

%% @doc Return the range set to claim from its first skipped chunk.
resume_at_skipped(#sweep_range{skipped_from = undefined} = SweepRange) ->
    SweepRange;
resume_at_skipped(#sweep_range{skipped_from = SkippedFrom} = SweepRange) ->
    SweepRange#sweep_range{
        next_claim_offset = SkippedFrom,
        skipped_from = undefined
    }.

%% @doc Add the range to its mode's parked ranges, to claim again from its
%% first skipped chunk at RetryAtMs. A range already parked keeps the lower
%% offset to claim from. When the mode has no parking room, a new range is
%% dropped and waits for the next sweep; process_sweep_range/2 checks the room
%% first, so this happens only in tests.
park_sweep_range(SweepRange, RetryAtMs, State) ->
    #sweep_range{mode = Mode, offset = Offset} = SweepRange,
    Entry = (resume_at_skipped(SweepRange))#sweep_range{
        retry_at_ms = RetryAtMs
    },
    ClaimFrom = Entry#sweep_range.next_claim_offset,
    Parked = parked_ranges(Mode, State),
    Parked2 =
        case lists:keyfind(Offset, #sweep_range.offset, Parked) of
            #sweep_range{next_claim_offset = ParkedFrom} = ParkedRange ->
                lists:keyreplace(Offset, #sweep_range.offset, Parked,
                    ParkedRange#sweep_range{
                        next_claim_offset = min_claim_offset(
                            ParkedFrom, ClaimFrom),
                        retry_at_ms = min(
                            ParkedRange#sweep_range.retry_at_ms, RetryAtMs)
                    });
            false ->
                case has_parking_room(Mode, State) of
                    true -> Parked ++ [Entry];
                    false -> Parked
                end
        end,
    set_parked_ranges(Mode, Parked2, State).

min_claim_offset(undefined, _Offset) -> undefined;
min_claim_offset(_Offset, undefined) -> undefined;
min_claim_offset(Offset1, Offset2) -> min(Offset1, Offset2).

%% @doc Claim again from the parked range due first, if one is due. A retry
%% that skips chunks parks the range again from the first skipped chunk; one
%% that stops at the claim limit continues on the next sweep step.
retry_parked_range(State) ->
    NowMs = ?DEP(clock):monotonic_ms(),
    case take_due_parked_range(NowMs, State) of
        none ->
            State;
        {SweepRange, State2} ->
            case claim_sweep_range(SweepRange, State2) of
                {{blocked, Delay}, _State} ->
                    park_sweep_range(SweepRange, NowMs + Delay, State2);
                finished ->
                    State2;
                {continue, #sweep_range{skipped_from = undefined} = Next} ->
                    park_sweep_range(Next, NowMs, State2);
                {_ContinueOrParked, Next} ->
                    park_sweep_range(
                        Next, NowMs + ?PARKED_RANGE_RETRY_DELAY_MS, State2
                    )
            end
    end.

take_due_parked_range(NowMs, State) ->
    Due = [
        {RetryAtMs, Mode, SweepRange}
     || Mode <- [byte, footprint],
        #sweep_range{retry_at_ms = RetryAtMs} = SweepRange
            <- parked_ranges(Mode, State),
        RetryAtMs =< NowMs
    ],
    case lists:sort(Due) of
        [] ->
            none;
        [{_RetryAtMs, Mode, SweepRange} | _] ->
            Parked = lists:delete(SweepRange, parked_ranges(Mode, State)),
            {SweepRange, set_parked_ranges(Mode, Parked, State)}
    end.

%% @doc Return the time until the next parked range is due, or infinity.
parked_range_delay(State) ->
    NowMs = ?DEP(clock):monotonic_ms(),
    lists:foldl(
        fun(#sweep_range{retry_at_ms = RetryAtMs}, Delay) ->
            min(Delay, max(0, RetryAtMs - NowMs))
        end,
        infinity,
        parked_ranges(byte, State) ++ parked_ranges(footprint, State)
    ).

parked_ranges(Mode, State) ->
    maps:get(Mode, State#state.parked_ranges).

set_parked_ranges(Mode, Parked, State) ->
    State#state{
        parked_ranges = maps:put(Mode, Parked, State#state.parked_ranges)
    }.

%%%===================================================================
%%% Claim: pacing.
%%%===================================================================

completed_range_delay(StateBefore, StateAfter) ->
    CompletedFootprint = completed_nonempty_head(
        footprint, StateBefore, StateAfter
    ),
    case
        CompletedFootprint andalso
            next_head_metadata_missing(footprint, StateAfter)
    of
        true -> ?SWEEP_RANGE_CADENCE_MS;
        false -> 0
    end.

next_head_metadata_missing(Mode, State) ->
    case queue:peek(sweep_queue(Mode, State)) of
        {value, #sweep_range{
            unsynced_range = #unsynced_range{
                query_offset = QueryOffset,
                range_start = RangeStart,
                range_end = RangeEnd
            }
        }} ->
            Peers = candidate_peers(QueryOffset),
            {_PeerRanges, CacheStatus} = arweave_sync_discovery:cached_peer_ranges(
                State#state.store_id, Peers, QueryOffset, RangeStart, RangeEnd
            ),
            CacheStatus =:= cache_miss;
        _ ->
            false
    end.

completed_nonempty_head(Mode, StateBefore, StateAfter) ->
    case queue:peek(sweep_queue(Mode, StateBefore)) of
        {value, #sweep_range{unsynced_range = none}} ->
            false;
        {value, #sweep_range{offset = Offset}} ->
            case queue:peek(sweep_queue(Mode, StateAfter)) of
                {value, #sweep_range{offset = Offset}} -> false;
                _ -> true
            end;
        empty ->
            false
    end.

%%%===================================================================
%%% Complete.
%%%===================================================================

complete_sweep(#state{store_id = StoreID} = State) ->
    ?LOG_DEBUG([
        {event, sync_network},
        {stage, sweep_complete},
        {store_id, StoreID}
    ]),
    %% A restart before the next save starts the next sweep at the store start
    %% too.
    Start = State#state.range_start,
    State2 = save_offsets(
        {Start, Start}, ?DEP(clock):monotonic_ms(), State
    ),
    State3 = State2#state{
        cursor = undefined,
        readahead_cursor = undefined,
        sweep_queues = new_sweep_queues()
    },
    {noreply, schedule_sweep(?SWEEP_RESTART_DELAY_MS, State3)}.

%%%===================================================================
%%% Metrics.
%%%===================================================================

%% @doc Publish how many bytes each mode has swept, from 0 up to the store's
%% range size, so that both modes and all stores share one scale.
publish_sweep_metrics(State) ->
    #state{
        store_id = StoreID,
        range_start = RangeStart,
        range_end = RangeEnd,
        cursor = Cursor
    } = State,
    case Cursor of
        undefined ->
            ok;
        _ ->
            RangeSize = max(1, RangeEnd - RangeStart),
            ByteSwept = max(0, arweave_sync_cursor:current(byte, Cursor) - RangeStart),
            FootprintSteps =
                max(
                    0,
                    arweave_sync_cursor:current(footprint, Cursor) - RangeStart
                ) div
                    ?DATA_CHUNK_SIZE,
            %% The footprint cursor moves one chunk per footprint swept, so
            %% the bytes swept are the steps times the footprint size.
            FootprintSwept = FootprintSteps
                * arweave_lib_constants:get_replica_2_9_footprint_size(),
            arweave_sync_metrics:publish_sweep_offset(
                StoreID,
                min(RangeSize, ByteSwept),
                min(RangeSize, FootprintSwept)
            )
    end.

%%%===================================================================
%%% Unsynced ranges.
%%%===================================================================

find_unsynced_range(
    byte,
    Offset,
    StoreID,
    Cursor,
    WeaveSize,
    DiskPoolThreshold
) ->
    RangeStart = arweave_sync_cursor:start(byte, Cursor),
    LiveEnd = arweave_sync_cursor:live_end(byte, Cursor, WeaveSize, DiskPoolThreshold),
    case Offset >= LiveEnd of
        true ->
            done;
        false ->
            End2 = byte_range_end(Offset, LiveEnd),
            UnsyncedIntervals = unsynced_intervals(Offset, End2, StoreID),
            case arweave_lib_intervals:is_empty(UnsyncedIntervals) of
                true ->
                    {no_need, End2};
                false ->
                    %% Look up and warm peer ranges from the first unsynced
                    %% byte, not the sweep offset. Discovery rounds byte
                    %% cache keys down to the QUERY_RANGE_STEP_SIZE grid, so
                    %% a lookup from the sweep offset could hit an earlier,
                    %% fully synced step and find nothing to fetch.
                    QueryOffset = frontier_offset(Offset, UnsyncedIntervals),
                    UnsyncedFootprintBytes = unsynced_bytes_in_footprint(
                        StoreID, QueryOffset, RangeStart, LiveEnd
                    ),
                    CombinedUnsyncedIntervals = arweave_lib_intervals:union(
                        UnsyncedIntervals, UnsyncedFootprintBytes
                    ),
                    {need, #unsynced_range{
                        kind = byte,
                        query_offset = QueryOffset,
                        intervals = CombinedUnsyncedIntervals,
                        range_start = RangeStart,
                        range_end = LiveEnd,
                        advance = End2
                    }}
            end
    end;
find_unsynced_range(
    footprint,
    Offset,
    StoreID,
    Cursor,
    WeaveSize,
    DiskPoolThreshold
) ->
    Start = arweave_sync_cursor:start(footprint, Cursor),
    LiveEnd = arweave_sync_cursor:live_end(footprint, Cursor, WeaveSize, DiskPoolThreshold),
    case Offset >= LiveEnd of
        true ->
            done;
        false ->
            UnsyncedIntervals = unsynced_bytes_in_footprint(
                StoreID, Offset, Start, LiveEnd
            ),
            NextOffset = arweave_sync_cursor:next_footprint_offset(
                Offset, Start, LiveEnd
            ),
            case arweave_lib_intervals:is_empty(UnsyncedIntervals) of
                true ->
                    {no_need, NextOffset};
                false ->
                    {need, #unsynced_range{
                        kind = footprint,
                        query_offset = Offset,
                        intervals = UnsyncedIntervals,
                        range_start = Start,
                        range_end = LiveEnd,
                        advance = {Offset, Start, LiveEnd}
                    }}
            end
    end.

%% @doc Return the next QUERY_RANGE_STEP_SIZE boundary after Offset, capped at
%% LiveEnd; store starts are not aligned to that grid, so stepping a fixed size
%% from the start would skip the beginning of every later grid step.
byte_range_end(Offset, LiveEnd) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    min(((Offset div Step) + 1) * Step, LiveEnd).

%% @doc Return the unsynced byte intervals between Start and End, leaving out
%% blacklisted bytes and the footprints the store does not keep.
unsynced_intervals(Start, End, StoreID) ->
    Storage = ?DEP(storage),
    Blacklist = ?DEP(blacklist),
    FootprintLimit = ?DEP(footprint_limit),
    Unsynced = Storage:get_intervals(
        unsynced, Start, End, any_packing, {ar_data_sync, byte}, StoreID
    ),
    Blacklisted = Blacklist:get_blacklisted_intervals(Start, End),
    Kept = FootprintLimit:kept_intervals(
        Start, End, FootprintLimit:get(StoreID)
    ),
    arweave_lib_intervals:intersection(
        Kept, arweave_lib_intervals:outerjoin(Blacklisted, Unsynced)
    ).

%% @doc Return a footprint's unsynced intervals in footprint-record space, or
%% no intervals when the store does not keep that footprint.
unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    FootprintLimit = ?DEP(footprint_limit),
    case Footprint < FootprintLimit:get(StoreID) of
        true ->
            Storage = ?DEP(storage),
            {Start, End} = arweave_lib_footprint:get_footprint_range(Partition, Footprint),
            Storage:get_intervals(
                unsynced, Start, End, any_packing, {ar_data_sync, footprint}, StoreID
            );
        false ->
            arweave_lib_intervals:new()
    end.

%% @doc Return the chunks the store lacks in the footprint at Offset, as byte
%% intervals clipped to RangeStart and RangeEnd.
unsynced_bytes_in_footprint(StoreID, Offset, RangeStart, RangeEnd) ->
    {Partition, Footprint} = arweave_lib_footprint:get_footprint_location(
        Offset + ?DATA_CHUNK_SIZE
    ),
    FootprintIntervals = unsynced_footprint_intervals(
        Partition, Footprint, StoreID
    ),
    arweave_lib_footprint:footprint_intervals_to_byte_intervals(
        FootprintIntervals, RangeStart, RangeEnd
    ).

%% @doc Return the first unsynced byte at or after the sweep offset.
frontier_offset(Offset, UnsyncedIntervals) ->
    case arweave_lib_intervals:is_empty(UnsyncedIntervals) of
        true ->
            Offset;
        false ->
            {_End, Start} = arweave_lib_intervals:smallest(UnsyncedIntervals),
            max(Offset, Start)
    end.

%%%===================================================================
%%% Sweep queues.
%%%===================================================================

new_sweep_queues() ->
    #{byte => queue:new(), footprint => queue:new()}.

sweep_queue(Mode, State) ->
    maps:get(Mode, State#state.sweep_queues).

set_sweep_queue(Mode, Queue, State) ->
    SweepQueues = maps:update(Mode, Queue, State#state.sweep_queues),
    State#state{sweep_queues = SweepQueues}.

sweep_queue_max_length(byte) ->
    ?BYTE_SWEEP_QUEUE_MAX_LENGTH;
sweep_queue_max_length(footprint) ->
    ?FOOTPRINT_SWEEP_QUEUE_MAX_LENGTH.

%%%===================================================================
%%% Private functions.
%%%===================================================================

candidate_peers(Offset) ->
    AllPeers =
        case ?DEP(config):get([sync, local_peers_only]) of
            true -> ?DEP(config):get([peers, local]);
            false -> arweave_sync_discovery:get_peers_for_offset(Offset)
        end,
    UnthrottledPeers = lists:filter(
        fun(Peer) ->
            not ?DEP(throttling):is_throttled(Peer, ?CHUNK_PATH)
        end,
        AllPeers
    ),
    HotPeers =
        case UnthrottledPeers of
            [] -> AllPeers;
            _ -> UnthrottledPeers
        end,
    ?DEP(peers):pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT).

%% @doc Schedule a sweep Delay milliseconds from now unless an earlier sweep is
%% already scheduled, so that only one sweep is scheduled at a time.
schedule_sweep(Delay, State) ->
    #state{next_sweep = NextSweep} = State,
    DueMs = ?DEP(clock):monotonic_ms() + Delay,
    case NextSweep of
        {ScheduledDueMs, _Token} when ScheduledDueMs =< DueMs ->
            State;
        _ ->
            Token = make_ref(),
            {ok, _} = ?DEP(clock):send_after(Delay, self(), {sweep, Token}),
            State#state{next_sweep = {DueMs, Token}}
    end.
