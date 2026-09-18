%%% @doc Per-store network-sync need identification (gen_server).
%%%
%%% The `sweep' loop repeatedly examines the store range. Each sweep keeps a
%%% byte-range cursor and a replica.2.9 footprint-cadence cursor. The sweeper
%%% identifies locally unsynced ranges, turns them into candidate tasks,
%%% then applies only the cursor progress returned by that process.
-module(arweave_sync_store_sweeper).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    byte_range_end/2,
    can_sweep/1,
    completed_range_delay/2,
    do_enqueue_sweep_range/3,
    frontier_offset/2,
    initialize_sweep/1,
    process_next_sweep_range/2,
    process_sweep_queues/1,
    process_sweep_range/2,
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

%% A locally needed unsynced range to resolve against peer-advertised intervals.
%% It may not be fetchable yet if every matching peer is throttled or still needs
%% a chunk interval job, but it is not speculative local work.
%% The shared #unsynced_range{} contract is defined in arweave_sync.hrl.

%% Fixed delay between sweeps. The sweep loop does not issue HTTP; discovery's
%% HTTP workers have their own pacing. This delay only prevents tight-loop log
%% spam and CPU spin on fully-synced modules.
-include("arweave_sync_store_sweeper.hrl").

%%%===================================================================
%%% Supervisor wiring.
%%%===================================================================

name(?DEFAULT_MODULE) ->
    arweave_sync_store_sweeper_default;
name(StoreID) ->
    list_to_atom(
        "arweave_sync_store_sweeper_" ++
            (arweave_storage:store_info(StoreID))#store_info.label
    ).

register_workers() ->
    [?CHILD_WITH_ARGS(?MODULE, worker, name(SID), [SID]) || SID <- store_ids()].

start_link(StoreID) ->
    gen_server:start_link({local, name(StoreID)}, ?MODULE, StoreID, []).

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Start (or re-kick) the sweep loop for StoreID. Invoked by
%% ar_data_sync once chunk_copy completes and whenever the store re-enters sync.
start(StoreID) ->
    gen_server:cast(name(StoreID), start).

%% @doc The configured store IDs that have a network-sync sweeper.
%% The default store handles transient data and does not participate in network
%% sync admission, dispatch, or metrics.
store_ids() ->
    [
        (arweave_storage:store_info(Module))#store_info.id
     || Module <- arweave_config:storage_modules()
    ].

%% @doc Update the chain-tip snapshot. The sweeper reads the corresponding
%% disk-pool threshold when it processes this asynchronous update.
set_weave_size(StoreID, WeaveSize) ->
    gen_server:cast(name(StoreID), {set_weave_size, WeaveSize}).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
    arweave_sync:restore_store(StoreID),
    {RangeStart, RangeEnd} =
        case arweave_storage:store_info(StoreID) of
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
        sync_status = (arweave_sync_deps:data_sync()):init_sync_status(StoreID)
    }}.

handle_call(ping, _From, State) ->
    %% A synchronous no-op: replying proves every earlier mailbox message was
    %% processed, allowing callers to observe when this stage has drained.
    {reply, pong, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

%% Request an immediate sweep. Kicked by ar_data_sync after the chunk-copy
%% phase; subsequent sweeps are self-perpetuating.
handle_cast(start, State) ->
    %% An early wake is harmless: a queued range recomputes its remaining warm
    %% wait before it is used.
    {noreply, schedule_sweep(0, State)};
%% A decrease (reorg) needs no special handling: the smaller bounds take
%% effect through the live cursor ends. Work already submitted past them fails
%% to fetch and releases its ranges normally.
handle_cast({set_weave_size, WeaveSize}, State) ->
    DiskPoolThreshold = (arweave_sync_deps:disk_pool()):get_threshold(),
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
%%% Sweep loop.
%%%===================================================================

do_sweep(State) ->
    case arweave_sync:enabled() of
        false ->
            {noreply, schedule_sweep(?NODE_JOIN_RETRY_DELAY_MS, State)};
        true ->
            sweep_with_device_lock(State)
    end.

sweep_with_device_lock(State) ->
    Status = (arweave_sync_deps:device_lock()):acquire_lock(
        sync, State#state.store_id, State#state.sync_status
    ),
    State2 = State#state{sync_status = Status},
    case Status of
        active ->
            sweep(State2);
        paused ->
            {noreply, schedule_sweep(?DEVICE_LOCK_WAIT, State2)};
        _ ->
            %% off / complete — not in sync mode. The loop is re-kicked by
            %% ar_data_sync when the store re-enters sync.
            {noreply, State2}
    end.

sweep(#state{cursor = undefined} = State) ->
    State2 = initialize_sweep(State),
    case can_sweep(State2) of
        {blocked, Delay} ->
            {noreply, schedule_sweep(Delay, State)};
        complete ->
            %% The module begins above both live bounds, so there is no active
            %% sweep to complete. Retry when chain state may have advanced.
            {noreply, schedule_sweep(?NODE_JOIN_RETRY_DELAY_MS, State)};
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
    case can_sweep(State) of
        {blocked, Delay} ->
            {noreply, schedule_sweep(Delay, State)};
        complete ->
            complete_sweep(State);
        ready ->
            sweep_ready(State)
    end.

%% @doc Initialize the traversal state for a new sweep.
initialize_sweep(State) ->
    Cursor = arweave_sync_cursor:new(State#state.range_start, State#state.range_end),
    State#state{
        cursor = Cursor,
        readahead_cursor = Cursor,
        sweep_queues = new_sweep_queues(),
        next_mode = byte
    }.

%% @doc Gate an initialized sweep on node, store, disk, and live-bound checks.
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
            (arweave_sync_deps:node()):is_joined() andalso
            (arweave_sync_deps:data_sync()):is_footprint_record_initialized(StoreID),
    case Ready of
        false ->
            {blocked, ?NODE_JOIN_RETRY_DELAY_MS};
        true ->
            case (arweave_sync_deps:data_sync()):is_disk_space_sufficient(StoreID) of
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
    State2 = fill_sweep_queues(State),
    case process_sweep_queues(State2) of
        {ok, State3} ->
            Delay = completed_range_delay(State2, State3),
            {noreply, schedule_sweep(Delay, State3)};
        {blocked, Delay, State3} ->
            NextDelay =
                case can_extend_sweep_queue(State3) of
                    true -> 0;
                    false -> Delay
                end,
            {noreply, schedule_sweep(NextDelay, State3)};
        done ->
            %% Both mode queues reached the end of their queued readahead.
            %% Completion is observed after the final queued range is processed.
            {noreply, schedule_sweep(0, State2)}
    end.

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

%% @doc Start the next sweep once both cursors reached their live bounds.
complete_sweep(#state{store_id = StoreID} = State) ->
    ?LOG_DEBUG([
        {event, sync_network},
        {stage, sweep_complete},
        {store_id, StoreID}
    ]),
    State2 = State#state{
        cursor = undefined,
        readahead_cursor = undefined,
        sweep_queues = new_sweep_queues()
    },
    {noreply, schedule_sweep(?SWEEP_RESTART_DELAY_MS, State2)}.

%% @doc Schedule a sweep for Delay milliseconds from now - unless
%% there's already an earlier sweep scheduled. This ensures there's always
%% only a single scheduled sweep at a time.
schedule_sweep(Delay, State) ->
    #state{next_sweep = NextSweep} = State,
    DueMs = (arweave_sync_deps:clock()):monotonic_ms() + Delay,
    case NextSweep of
        {ScheduledDueMs, _Token} when ScheduledDueMs =< DueMs ->
            State;
        _ ->
            Token = make_ref(),
            {ok, _} = (arweave_sync_deps:clock()):send_after(Delay, self(), {sweep, Token}),
            State#state{next_sweep = {DueMs, Token}}
    end.

%%%===================================================================
%%% Unsynced range discovery.
%%%===================================================================

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
            case ar_intervals:is_empty(UnsyncedIntervals) of
                true ->
                    {no_need, End2};
                false ->
                    %% Align the peer-interval lookup + warming to the first UNSYNCED
                    %% byte, not the raw sweep offset. The offset can lag in the synced
                    %% tail of a QUERY_RANGE_STEP_SIZE step while the unsynced data
                    %% (and its cached metadata) live in the following step;
                    %% Discovery aligns byte cache keys down to the grid, so a
                    %% lookup from the raw offset checks
                    %% the wrong synced, empty step and nothing is ever fetchable.
                    QueryOffset = frontier_offset(Offset, UnsyncedIntervals),
                    UnsyncedFootprintBytes = unsynced_bytes_in_footprint(
                        StoreID, QueryOffset, RangeStart, LiveEnd
                    ),
                    CombinedUnsyncedIntervals = ar_intervals:union(
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
            NextOffset = (arweave_sync_deps:replica()):get_next_fetch_offset(
                Offset, Start, LiveEnd
            ),
            case ar_intervals:is_empty(UnsyncedIntervals) of
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

%% @doc End the byte sweep at the next global metadata-query boundary. Storage
%% module padded starts are not query-grid aligned; adding one step to such a
%% start would permanently skip the beginning of every following grid range.
byte_range_end(Offset, LiveEnd) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    min(((Offset div Step) + 1) * Step, LiveEnd).

%% @doc Exclude blacklisted bytes and footprints the store does not retain.
unsynced_intervals(Start, End, StoreID) ->
    Storage = arweave_sync_deps:storage(),
    Blacklist = arweave_sync_deps:blacklist(),
    FootprintLimit = arweave_sync_deps:footprint_limit(),
    Unsynced = Storage:get_intervals(
        unsynced, Start, End, any_packing, {ar_data_sync, byte}, StoreID
    ),
    Blacklisted = Blacklist:get_blacklisted_intervals(Start, End),
    Kept = FootprintLimit:kept_intervals(
        Start, End, FootprintLimit:get(StoreID)
    ),
    ar_intervals:intersection(
        Kept, ar_intervals:outerjoin(Blacklisted, Unsynced)
    ).

%% @doc Read missing footprint entries only for footprints the store retains.
unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    FootprintLimit = arweave_sync_deps:footprint_limit(),
    case Footprint < FootprintLimit:get(StoreID) of
        true ->
            Storage = arweave_sync_deps:storage(),
            {Start, End} = arweave_storage:get_footprint_range(Partition, Footprint),
            Storage:get_intervals(
                unsynced, Start, End, any_packing, {ar_data_sync, footprint}, StoreID
            );
        false ->
            ar_intervals:new()
    end.

%% @doc Return locally missing chunks from the footprint at Offset as byte
%% intervals, clipped to the store's active range.
unsynced_bytes_in_footprint(StoreID, Offset, RangeStart, RangeEnd) ->
    {Partition, Footprint} = arweave_storage:get_footprint_location(
        Offset + ?DATA_CHUNK_SIZE
    ),
    FootprintIntervals = unsynced_footprint_intervals(
        Partition, Footprint, StoreID
    ),
    arweave_storage:footprint_intervals_to_byte_intervals(
        FootprintIntervals, RangeStart, RangeEnd
    ).

%% @doc Return the first needed byte at or after the sweep offset. Using the raw
%% offset can align the peer-interval lookup to an earlier, fully synced grid cell.
frontier_offset(Offset, UnsyncedIntervals) ->
    case ar_intervals:is_empty(UnsyncedIntervals) of
        true ->
            Offset;
        false ->
            {_End, Start} = ar_intervals:smallest(UnsyncedIntervals),
            max(Offset, Start)
    end.

%%%===================================================================
%%% Sweep queues.
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

enqueue_sweep_range(Mode, Offset, none, ReadaheadCursor, State) ->
    %% `none' means the complete byte query-range or footprint-cadence step
    %% contains no unsynced range. Keep a queue placeholder so its cursor
    %% advance remains ordered behind any earlier pending ranges.
    do_enqueue_sweep_range(
        #sweep_range{
            mode = Mode,
            offset = Offset,
            next_offset = arweave_sync_cursor:current(Mode, ReadaheadCursor),
            requested_at = (arweave_sync_deps:clock()):monotonic_ms()
        },
        ReadaheadCursor,
        State
    );
enqueue_sweep_range(Mode, Offset, UnsyncedRange, ReadaheadCursor, State) ->
    #unsynced_range{query_offset = QueryOffset} = UnsyncedRange,
    Peers = candidate_peers(QueryOffset),
    %% Tell discovery to begin fetching availability so it is likely cached
    %% before this sweep range is processed.
    ok = arweave_sync_discovery:warm_peer_ranges(
        State#state.store_id, Peers, QueryOffset
    ),
    %% Reserve this range as readahead; the processed cursor advances later.
    ReadaheadCursor2 = arweave_sync_cursor:advance(UnsyncedRange, ReadaheadCursor),
    do_enqueue_sweep_range(
        #sweep_range{
            mode = Mode,
            offset = Offset,
            next_offset = arweave_sync_cursor:current(Mode, ReadaheadCursor2),
            unsynced_range = UnsyncedRange,
            requested_at = (arweave_sync_deps:clock()):monotonic_ms()
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

candidate_peers(Offset) ->
    AllPeers =
        case arweave_config:get([sync, local_peers_only]) of
            true -> arweave_config:get([peers, local]);
            false -> arweave_sync_discovery:get_peers_for_offset(Offset)
        end,
    UnthrottledPeers = lists:filter(
        fun(Peer) ->
            not (arweave_sync_deps:throttling()):is_throttled(Peer, ?CHUNK_PATH)
        end,
        AllPeers
    ),
    HotPeers =
        case UnthrottledPeers of
            [] -> AllPeers;
            _ -> UnthrottledPeers
        end,
    (arweave_sync_deps:peers()):pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT).

%% @doc Maintain separate byte and footprint sweep-range queues. Give the
%% round-robin preferred head the first chance to advance, falling back to the
%% other when it is blocked or its queue is done. Advance at most one head per
%% call, and delay only when neither can advance.
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

process_sweep_range(
    #sweep_range{requested_at = RequestedAt} = SweepRange, State
) ->
    %% Wait until discovery has had the full warming interval for this range.
    WarmDelay = max(
        0,
        RequestedAt + ?SWEEP_RANGE_WARM_WAIT_MS -
            (arweave_sync_deps:clock()):monotonic_ms()
    ),
    maybe
        0 ?= WarmDelay,
        do_process_sweep_range(SweepRange, State)
    else
        _ -> {{blocked, WarmDelay}, State}
    end.

do_process_sweep_range(SweepRange, State) ->
    #sweep_range{unsynced_range = UnsyncedRange} = SweepRange,
    #unsynced_range{
        query_offset = QueryOffset,
        range_start = RangeStart,
        range_end = RangeEnd
    } = UnsyncedRange,
    Peers = candidate_peers(QueryOffset),
    %% Start warming candidates discovered since enqueue and retry demand that
    %% discovery could not previously queue, but do not extend this range's
    %% fixed wait; the claim below uses only metadata already cached.
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
                ar_intervals:is_empty(
                    UnsyncedRange2#unsynced_range.intervals
                )
            of
                true ->
                    {ok, finish_sweep_range(SweepRange, State)};
                false ->
                    claim_range(SweepRange, UnsyncedRange2, PeerRanges, State)
            end;
        _NoLongerNeeded ->
            {ok, finish_sweep_range(SweepRange, State)}
    end.

claim_range(SweepRange, UnsyncedRange, PeerRanges, State) ->
    case
        arweave_sync_chunk_picker:claim_ranges(
            State#state.store_id, [UnsyncedRange], PeerRanges
        )
    of
        blocked ->
            {{blocked, ?BLOCKED_RETRY_DELAY_MS}, State};
        {ok, _TasksProduced, NextClaimOffset} ->
            {ok, resume_sweep_range(SweepRange, NextClaimOffset, State)};
        {ok, _TasksProduced} ->
            {ok, finish_sweep_range(SweepRange, State)}
    end.

resume_unsynced_range(
    #sweep_range{next_claim_offset = undefined}, UnsyncedRange
) ->
    UnsyncedRange;
resume_unsynced_range(
    #sweep_range{next_claim_offset = NextClaimOffset},
    UnsyncedRange
) ->
    #unsynced_range{intervals = Intervals, range_end = RangeEnd} =
        UnsyncedRange,
    Remaining = ar_intervals:from_list([{RangeEnd, NextClaimOffset}]),
    UnsyncedRange#unsynced_range{
        intervals = ar_intervals:intersection(Intervals, Remaining)
    }.

resume_sweep_range(SweepRange, NextClaimOffset, State) ->
    Mode = SweepRange#sweep_range.mode,
    {{value, SweepRange}, Queue2} = queue:out(sweep_queue(Mode, State)),
    SweepRange2 = SweepRange#sweep_range{
        next_claim_offset = NextClaimOffset
    },
    set_sweep_queue(Mode, queue:in_r(SweepRange2, Queue2), State).

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
%%% Metrics.
%%%===================================================================

%% @doc Publish bytes swept per mode, normalized to 0..range size so both
%% modes and every store share one scale. The byte mode's cursor offset IS its
%% bytes swept; the footprint mode's cursor steps one chunk per footprint
%% (each step covering a whole footprint scattered across the partition), so
%% its progress is chunk-steps scaled by the footprint size. Flat while the
%% store has unsynced data = a stalled sweep; repeating passes = a sawtooth.
publish_sweep_metrics(State) ->
    #state{
        store_id = StoreID,
        range_start = RangeStart,
        range_end = RangeEnd,
        cursor = Cursor
    } = State,
    #store_info{label = Label} = arweave_storage:store_info(StoreID),
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
            %% Sweep progress counts bytes, not the footprint's sub-chunks.
            Constants = arweave_sync_deps:constants(),
            FootprintSwept = FootprintSteps * Constants:get_replica_2_9_footprint_size(),
            arweave_metrics:gauge_set(
                sync_sweep_offset,
                [Label, byte],
                min(RangeSize, ByteSwept)
            ),
            arweave_metrics:gauge_set(
                sync_sweep_offset,
                [Label, footprint],
                min(RangeSize, FootprintSwept)
            )
    end.
