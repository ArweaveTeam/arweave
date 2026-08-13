%%% @doc Per-store network-sync need identification (gen_server).
%%%
%%% The `sweep' loop repeatedly examines the store range. Each sweep keeps a
%%% byte-range cursor and a replica.2.9 footprint-cadence cursor. The sweeper
%%% identifies locally unsynced ranges, turns them into candidate tasks,
%%% then applies only the cursor progress returned by that process.
-module(ar_sync_store_sweeper).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/1, name/1, register_workers/0, store_ids/0]).
-export([start/1, set_weave_size/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_sync.hrl").
-include_lib("arweave/include/ar_sup.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% A locally needed unsynced range to resolve against peer-advertised intervals.
%% It may not be fetchable yet if every matching peer is throttled or still needs
%% a chunk interval job, but it is not speculative local work.
%% The shared #unsynced_range{} contract is defined in ar_sync.hrl.

-record(state, {
    %% Storage module identifier this state belongs to.
    store_id,
    %% Start offset of the storage module's range.
    range_start = -1 :: integer(),
    %% End offset of the storage module's range.
    range_end = -1 :: integer(),
    %% Latest known weave size (chain tip). Updated via set_weave_size/2.
    weave_size :: undefined | non_neg_integer(),
    %% Mirror of ar_device_lock's view of this module's sync-mode lock.
    sync_status = undefined,
    %% Cursor for the in-progress sweep. `undefined' means the loop hasn't
    %% started its first sweep yet.
    cursor = undefined :: undefined | ar_sync_cursor:t(),
    %% Offsets whose newly cached peer ranges still need to be offered to the
    %% task generator. A map deduplicates concurrent peer discoveries.
    pending_revisits = gb_sets:new(),
    %% Due time and token for the next scheduled sweep. An earlier request
    %% replaces the token; the superseded timer is ignored when it arrives.
    next_sweep = undefined :: undefined | {integer(), reference()}
}).

%% Peer selection (get_hot_peers / the chunk2 throttle) moved to ar_sync_chunk_picker.

%% Delay before retrying a range claim that could not be processed.
-define(BLOCKED_RETRY_DELAY_MS, 200).
%% Pace cursor progress when local need produced no tasks. Two seconds keeps
%% metadata discovery moving without letting a cold store race across its range.
-define(NO_TASK_ADVANCE_DELAY_MS, 2_000).
%% Fixed delay between sweeps. The sweep loop does not issue HTTP (that
%% lives in ar_sync_discovery's metadata job queues, which have their
%% own pacing), so this only prevents tight-loop log spam and CPU spin
%% on fully-synced modules.
-ifdef(AR_TEST).
-define(SWEEP_RESTART_DELAY_MS, 1_000).
-else.
-define(SWEEP_RESTART_DELAY_MS, 10_000).
-endif.

%%%===================================================================
%%% Supervisor wiring.
%%%===================================================================

name(?DEFAULT_MODULE) ->
    ar_sync_store_sweeper_default;
name(StoreID) ->
    list_to_atom("ar_sync_store_sweeper_" ++ ar_storage_module:label(StoreID)).

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

%% @doc The configured storage-module IDs that have a network-sync sweeper.
%% The default store handles transient data and does not participate in network
%% sync admission, dispatch, or metrics.
store_ids() ->
    StorageModules = [arweave_config:config_to_storage_module(M)
        || M <- arweave_config:get([storage_modules])],
    [ar_storage_module:id(SM) || SM <- StorageModules].

%% @doc Update the weave-size snapshot. Called by ar_data_sync on chain-tip
%% moves so the sweep loop's range clamp follows the tip.
set_weave_size(StoreID, WeaveSize) ->
    gen_server:cast(name(StoreID), {set_weave_size, WeaveSize}).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
    {RangeStart, RangeEnd} = ar_storage_module:get_padded_range(StoreID),
    ok = ar_events:subscribe(sync_discovery),
    ?LOG_INFO([{event, init}, {module, ?MODULE}, {store_id, StoreID},
        {range_start, RangeStart}, {range_end, RangeEnd}]),
    {ok, #state{
        store_id = StoreID,
        range_start = RangeStart,
        range_end = RangeEnd,
        sync_status = ar_data_sync:init_sync_status(StoreID)
    }}.

handle_call(ping, _From, State) ->
    %% A synchronous no-op: replying proves every earlier mailbox message was
    %% processed, allowing simulator settling and teardown to drain this stage.
    {reply, pong, State};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

%% Request an immediate sweep. Kicked by ar_data_sync after the chunk-copy
%% phase; subsequent sweeps are self-perpetuating.
handle_cast(start, State) ->
    {noreply, schedule_sweep(0, State)};

%% Chain-tip update from ar_data_sync. A decrease (reorg) needs no special
%% handling: the smaller tip takes effect via the live cursor bounds in the
%% sweep loop; tasks already submitted past the shrunk tip simply fail to fetch
%% and have their ranges released.
handle_cast({set_weave_size, WeaveSize}, State) ->
    {noreply, State#state{ weave_size = WeaveSize }};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({sweep, Token},
        #state{ next_sweep = {_DueMs, Token} } = State) ->
    do_sweep(State#state{ next_sweep = undefined });
handle_info({sweep, _StaleToken}, State) ->
    {noreply, State};

handle_info({event, sync_discovery,
        {chunk_intervals_updated, StoreID, Offset}},
        #state{ store_id = StoreID, cursor = Cursor } = State)
        when Cursor =/= undefined ->
    %% Byte and footprint describe peer source representations, not the local
    %% need lane that requested them. Recheck both lanes that have already
    %% reached this offset; the revisit map deduplicates notifications.
    State2 = maybe_queue_revisit(byte, Offset, Cursor, State),
    State3 = maybe_queue_revisit(footprint, Offset, Cursor, State2),
    {noreply, State3};

handle_info({event, sync_discovery,
        {footprint_reservation_released, StoreID, Offset}},
        #state{ store_id = StoreID } = State) ->
    %% The released reservation proves this footprint was already offered.
    %% Revisit it even if a new sweep pass has since restarted behind it.
    {noreply, queue_revisit(footprint, Offset, State)};

handle_info({event, sync_discovery, _Event}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([{event, terminate}, {module, ?MODULE},
        {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%%===================================================================
%%% Pending revisit queue.
%%%===================================================================

maybe_queue_revisit(Mode, Offset, Cursor, State) ->
    case Offset =< ar_sync_cursor:current(Mode, Cursor) of
        true -> queue_revisit(Mode, Offset, State);
        false -> State
    end.

queue_revisit(Mode, Offset, State) ->
    PendingRevisits = State#state.pending_revisits,
    State2 = State#state{
        pending_revisits = gb_sets:add_element({Offset, Mode}, PendingRevisits)
    },
    schedule_sweep(0, State2).

sweep_pending_revisit(State, DiskPoolThreshold) ->
    {Offset, Mode} = gb_sets:smallest(State#state.pending_revisits),
    {Delay, State2} =
        retry_pending_revisit(Mode, Offset, DiskPoolThreshold, State),
    {noreply, schedule_sweep(Delay, State2)}.

retry_pending_revisit(Mode, Offset, DiskPoolThreshold, State) ->
    case revisit_range(Mode, Offset, DiskPoolThreshold, State) of
        blocked ->
            {?BLOCKED_RETRY_DELAY_MS, State};
        {ok, _TasksProduced} ->
            PendingRevisits = gb_sets:del_element(
                {Offset, Mode}, State#state.pending_revisits),
            {0, State#state{ pending_revisits = PendingRevisits }}
    end.

%% @doc Recompute local need at Offset after peer ranges are discovered and
%% offer it for admission without moving the active sweep cursor.
revisit_range(Mode, Offset, DiskPoolThreshold, State) ->
    #state{ cursor = Cursor } = State,
    Start = ar_sync_cursor:start(Mode, Cursor),
    RevisitCursor = ar_sync_cursor:set(Mode, max(Offset, Start), Cursor),
    {Result, _UnsyncedRanges, _Cursor2} = find_and_claim_unsynced_ranges(
        [Mode], RevisitCursor, DiskPoolThreshold, State),
    Result.

%%%===================================================================
%%% Sweep loop.
%%%===================================================================

do_sweep(State) ->
    Status = ar_device_lock:acquire_lock(
        sync, State#state.store_id, State#state.sync_status),
    State2 = State#state{ sync_status = Status },
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

sweep(#state{ cursor = undefined } = State) ->
    case start_sweep(State) of
        {ok, Cursor} ->
            State2 = State#state{ cursor = Cursor },
            {noreply, schedule_sweep(0, State2)};
        blocked ->
            %% Node not joined yet, or footprint migration in flight.
            {noreply, schedule_sweep(?NODE_JOIN_RETRY_DELAY_MS, State)}
    end;
sweep(State) ->
    publish_sweep_metrics(State),
    DiskPoolThreshold = ar_disk_pool:get_threshold(),
    PendingRevisits = State#state.pending_revisits,
    case {can_sweep(State, DiskPoolThreshold),
            gb_sets:is_empty(PendingRevisits)} of
        {{blocked, Delay}, _} ->
            {noreply, schedule_sweep(Delay, State)};
        {_, false} ->
            sweep_pending_revisit(State, DiskPoolThreshold);
        {complete, true} ->
            %% Both cursors reached their live bounds. This is the sole
            %% completion trigger, so the sweep tracks a shrinking or
            %% growing weave tip without rewriting its static bounds.
            complete_sweep(State);
        {ready, true} ->
            {Delay, NewState} = sweep_next_range(State, DiskPoolThreshold),
            {noreply, schedule_sweep(Delay, NewState)}
    end.

%% @doc Build a new sweep.
start_sweep(State) ->
    #state{
        store_id = StoreID,
        range_start = Start,
        range_end = End,
        weave_size = WeaveSize
    } = State,
    case ready_to_start(StoreID, WeaveSize) of
        false ->
            blocked;
        true ->
            %% The live weave tip and disk-pool threshold are applied at each
            %% sweep/completion check, so the sweep tracks them without
            %% rewriting its static bounds.
            Cursor = ar_sync_cursor:new(Start, End),
            DiskPoolThreshold = ar_disk_pool:get_threshold(),
            case ar_sync_cursor:is_complete(Cursor, WeaveSize, DiskPoolThreshold) of
                true ->
                    %% Storage module's range is entirely above the current weave
                    %% tip; nothing to sync yet. The caller retries after a delay.
                    blocked;
                false ->
                    ?LOG_DEBUG([{event, sync_network}, {stage, sweep_started},
                        {store_id, StoreID}, {range_start, Start}, {range_end, End}]),
                    {ok, Cursor}
            end
    end.

ready_to_start(_StoreID, undefined) ->
    false;
ready_to_start(StoreID, _WeaveSize) ->
    case ar_sync_deps:is_joined() of
        false -> false;
        true -> ar_sync_deps:is_footprint_record_initialized(StoreID)
    end.

can_sweep(State, DiskPoolThreshold) ->
    #state{
        store_id = StoreID,
        weave_size = WeaveSize,
        cursor = Cursor
    } = State,
    case ar_sync_deps:is_disk_space_sufficient(StoreID) of
        false ->
            {blocked, 30_000};
        not_initialized ->
            {blocked, 1_000};
        true ->
            case ar_sync_cursor:is_complete(Cursor, WeaveSize, DiskPoolThreshold) of
                true ->
                    complete;
                false ->
                    ready
            end
    end.

%% @doc Find the next locally-needed window on each lane, offer it to
%% ar_sync_chunk_picker:claim_ranges/2, and advance past it. The cursor stays
%% put while the store has no claim headroom. A range that produces no tasks
%% advances at a fixed pace; discovery queues a revisit when newly fetched
%% chunk intervals become available. Only a step that produced tasks is
%% unpaced.
sweep_next_range(State, DiskPoolThreshold) ->
    #state{ cursor = Cursor } = State,
    {Result, UnsyncedRanges, Cursor2} = find_and_claim_unsynced_ranges(
        ar_sync_cursor:kinds(), Cursor, DiskPoolThreshold, State),
    {Delay, Cursor3} =
        case Result of
            blocked ->
                {?BLOCKED_RETRY_DELAY_MS, Cursor2};
            {ok, 0} when UnsyncedRanges =/= [] ->
                {?NO_TASK_ADVANCE_DELAY_MS,
                    advance_cursor(Cursor2, UnsyncedRanges)};
            {ok, _TasksProduced} ->
                {0, advance_cursor(Cursor2, UnsyncedRanges)}
        end,
    {Delay, State#state{ cursor = Cursor3 }}.

find_and_claim_unsynced_ranges(Modes, Cursor, DiskPoolThreshold, State) ->
    #state{ store_id = StoreID, weave_size = WeaveSize } = State,
    {UnsyncedRanges, Cursor2} = next_unsynced_ranges(
        Modes, StoreID, Cursor, WeaveSize, DiskPoolThreshold),
    Result = ar_sync_chunk_picker:claim_ranges(StoreID, UnsyncedRanges),
    {Result, UnsyncedRanges, Cursor2}.

advance_cursor(Cursor, UnsyncedRanges) ->
    lists:foldl(
        fun(UnsyncedRange, CursorAcc) ->
            ar_sync_cursor:advance(UnsyncedRange, CursorAcc)
        end,
        Cursor,
        UnsyncedRanges).

%% @doc Start the next sweep once both cursors reached their live bounds.
complete_sweep(#state{ store_id = StoreID } = State) ->
    ?LOG_DEBUG([{event, sync_network}, {stage, sweep_complete},
        {store_id, StoreID}]),
    case start_sweep(State) of
        {ok, Cursor2} ->
            State2 = State#state{ cursor = Cursor2 },
            {noreply, schedule_sweep(?SWEEP_RESTART_DELAY_MS, State2)};
        blocked ->
            %% Clear the cursor so later sweep casts start a new sweep
            %% (silent retry) instead of re-logging sweep_complete every second.
            State2 = State#state{ cursor = undefined },
            {noreply, schedule_sweep(?NODE_JOIN_RETRY_DELAY_MS, State2)}
    end.

schedule_sweep(Delay, State) ->
    #state{ next_sweep = NextSweep } = State,
    DueMs = ar_timer:monotonic_ms() + Delay,
    case NextSweep of
        {ScheduledDueMs, _Token} when ScheduledDueMs =< DueMs ->
            State;
        _ ->
            Token = make_ref(),
            {ok, _} = ar_timer:send_after(Delay, self(), {sweep, Token}),
            State#state{ next_sweep = {DueMs, Token} }
    end.

%%%===================================================================
%%% Unsynced range discovery.
%%%===================================================================

next_unsynced_ranges(Modes, StoreID, Cursor, WeaveSize, DiskPoolThreshold) ->
    {UnsyncedRanges, Cursor3} = lists:foldl(
        fun(Kind, {Acc, CursorAcc}) ->
            case next_unsynced_range(Kind, StoreID, CursorAcc, WeaveSize,
                    DiskPoolThreshold) of
                {none, Cursor2Acc} -> {Acc, Cursor2Acc};
                {UnsyncedRange, Cursor2Acc} ->
                    {[UnsyncedRange | Acc], Cursor2Acc}
            end
        end,
        {[], Cursor},
        Modes),
    {lists:reverse(UnsyncedRanges), Cursor3}.

next_unsynced_range(Kind, StoreID, Cursor, WeaveSize, DiskPoolThreshold) ->
    Offset = ar_sync_cursor:current(Kind, Cursor),
    case find_unsynced_range(Kind, Offset, StoreID, Cursor, WeaveSize,
            DiskPoolThreshold) of
        done ->
            {none, Cursor};
        {no_need, NextOffset} ->
            {none, ar_sync_cursor:set(Kind, NextOffset, Cursor)};
        {need, UnsyncedRange} ->
            {UnsyncedRange, Cursor}
    end.

find_unsynced_range(byte, Offset, StoreID, Cursor, WeaveSize,
        DiskPoolThreshold) ->
    RangeStart = ar_sync_cursor:start(byte, Cursor),
    LiveEnd = ar_sync_cursor:live_end(byte, Cursor, WeaveSize, DiskPoolThreshold),
    case Offset >= LiveEnd of
        true ->
            done;
        false ->
            End2 = min(Offset + ar_sync_cursor:query_range_step_size(), LiveEnd),
            UnsyncedIntervals = ar_sync_deps:unsynced_intervals(Offset, End2, StoreID),
            case ar_intervals:is_empty(UnsyncedIntervals) of
                true ->
                    {no_need, End2};
                false ->
                    %% Align the peer-interval lookup + warming to the first UNSYNCED
                    %% byte, not the raw sweep offset. The offset can lag in the synced
                    %% tail of a QUERY_RANGE_STEP_SIZE window while the unsynced data
                    %% (and its warmed cache) live in the NEXT window;
                    %% get_peer_ranges_for_peers
                    %% aligns DOWN to the grid, so a lookup from the raw offset checks
                    %% the wrong (synced, empty) window and nothing is ever fetchable.
                    QueryOffset = frontier_offset(Offset, UnsyncedIntervals),
                    UnsyncedFootprintBytes = unsynced_bytes_in_footprint(
                        StoreID, QueryOffset, RangeStart, LiveEnd),
                    CombinedUnsyncedIntervals = ar_intervals:union(
                        UnsyncedIntervals, UnsyncedFootprintBytes),
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

find_unsynced_range(footprint, Offset, StoreID, Cursor, WeaveSize,
        DiskPoolThreshold) ->
    Start = ar_sync_cursor:start(footprint, Cursor),
    LiveEnd = ar_sync_cursor:live_end(footprint, Cursor, WeaveSize, DiskPoolThreshold),
    case Offset >= LiveEnd of
        true ->
            done;
        false ->
            UnsyncedIntervals = unsynced_bytes_in_footprint(
                StoreID, Offset, Start, LiveEnd),
            NextOffset = ar_replica_2_9:get_next_fetch_offset(Offset, Start, LiveEnd),
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

%% @doc Return locally missing chunks from the footprint at Offset as byte
%% intervals, clipped to the store's active range.
unsynced_bytes_in_footprint(StoreID, Offset, RangeStart, RangeEnd) ->
    {Partition, Footprint} = ar_footprint_record:get_location(
        Offset + ?DATA_CHUNK_SIZE),
    FootprintIntervals = ar_sync_deps:unsynced_footprint_intervals(
        Partition, Footprint, StoreID),
    ar_footprint_record:footprint_intervals_to_byte_intervals(
        FootprintIntervals, RangeStart, RangeEnd).

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
%%% Metrics.
%%%===================================================================

%% @doc Publish bytes swept per lane, normalized to 0..range size so both
%% lanes and every store share one scale. The byte lane's cursor offset IS its
%% bytes swept; the footprint lane's cursor steps one chunk per footprint
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
    Label = ar_storage_module:label(StoreID),
    case Cursor of
        undefined ->
            ok;
        _ ->
            RangeSize = max(1, RangeEnd - RangeStart),
            ByteSwept = max(0, ar_sync_cursor:current(byte, Cursor) - RangeStart),
            FootprintSteps = max(0,
                ar_sync_cursor:current(footprint, Cursor) - RangeStart)
                    div ?DATA_CHUNK_SIZE,
            %% ar_block:get_replica_2_9_footprint_size() is the footprint's BYTE
            %% size; ar_replica_2_9:get_footprint_size() is a sub-chunk COUNT.
            FootprintSwept = FootprintSteps * ar_block:get_replica_2_9_footprint_size(),
            arweave_metrics:gauge_set(sync_sweep_offset, [Label, byte],
                min(RangeSize, ByteSwept)),
            arweave_metrics:gauge_set(sync_sweep_offset, [Label, footprint],
                min(RangeSize, FootprintSwept))
    end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

frontier_offset_test() ->
    ?assertEqual(100, frontier_offset(100, ar_intervals:new())),
    Intervals = ar_intervals:from_list([{300, 250}, {500, 400}]),
    ?assertEqual(250, frontier_offset(100, Intervals)),
    ?assertEqual(260, frontier_offset(260, Intervals)),
    ?assertEqual(450, frontier_offset(450, Intervals)).

byte_cursor_advances_to_end_of_successful_range_test() ->
    Cursor = ar_sync_cursor:new(0, 100),
    UnsyncedRange = #unsynced_range{ kind = byte, advance = 50 },
    Cursor2 = advance_cursor(Cursor, [UnsyncedRange]),
    ?assertEqual(50, ar_sync_cursor:current(byte, Cursor2)).

zero_task_step_advances_after_metadata_pace_test_() ->
    RangeEnd = ?DATA_CHUNK_SIZE,
    ar_test_util:with_mocked([
        {ar_sync_deps, unsynced_intervals,
            fun(0, RangeEnd2, test_store) ->
                ar_intervals:from_list([{RangeEnd2, 0}])
            end},
        {ar_sync_deps, unsynced_footprint_intervals,
            fun(_Partition, _Footprint, test_store) -> ar_intervals:new() end},
        {ar_footprint_record, get_location, fun(_) -> {0, 0} end},
        {ar_sync_chunk_picker, claim_ranges,
            fun(test_store, [_UnsyncedRange]) -> {ok, 0} end}
    ], fun() ->
        State = #state{
            store_id = test_store,
            range_start = 0,
            range_end = RangeEnd,
            weave_size = RangeEnd,
            cursor = ar_sync_cursor:new(0, RangeEnd)
        },
        {Delay, State2} = sweep_next_range(State, RangeEnd),
        %% A zero-task step advances after the configured two-second pace.
        ?assertEqual(2_000, Delay),
        ?assertEqual(RangeEnd,
            ar_sync_cursor:current(byte, State2#state.cursor))
    end, 30).

set_weave_size_decrease_keeps_sweep_test() ->
    Cursor = ar_sync_cursor:set(footprint, ?DATA_CHUNK_SIZE,
        ar_sync_cursor:set(byte, ?DATA_CHUNK_SIZE,
            ar_sync_cursor:new(0, 2 * ?DATA_CHUNK_SIZE))),
    State = #state{
        store_id = test_store,
        weave_size = 2 * ?DATA_CHUNK_SIZE,
        cursor = Cursor
    },
    {noreply, State2} = handle_cast({set_weave_size, ?DATA_CHUNK_SIZE}, State),
    %% The decrease updates the cached size and leaves the sweep intact; the shrunk
    %% tip takes effect via live cursor bounds downstream.
    ?assertEqual(?DATA_CHUNK_SIZE, State2#state.weave_size),
    ?assertEqual(2 * ?DATA_CHUNK_SIZE,
        ar_sync_cursor:live_end(byte, State2#state.cursor,
            2 * ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE)),
    ?assertEqual(2 * ?DATA_CHUNK_SIZE,
        ar_sync_cursor:live_end(footprint, State2#state.cursor,
            2 * ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE)).

chunk_intervals_updated_revisits_without_moving_cursor_test_() ->
    Offset = ?DATA_CHUNK_SIZE,
    %% Four chunks keep the test range small while leaving both cursors ahead
    %% of the completed metadata range.
    RangeEnd = 4 * Offset,
    ar_test_util:with_mocked([
        {ar_disk_pool, get_threshold, fun() -> RangeEnd end},
        {ar_sync_deps, is_disk_space_sufficient, fun(test_store) -> true end},
        {ar_sync_deps, unsynced_intervals,
            fun(Start, End, test_store) ->
                ar_intervals:from_list([{End, Start}])
            end},
        {ar_footprint_record, get_location, fun(_) -> {0, 0} end},
        {ar_footprint_record, get_unsynced_intervals,
            fun(0, 0, test_store) -> ar_intervals:new() end},
        {ar_sync_chunk_picker, claim_ranges,
            fun(test_store, UnsyncedRanges) ->
                put(revisited_ranges, UnsyncedRanges),
                {ok, 0}
            end}
    ], fun() ->
        erase(revisited_ranges),
        Cursor = ar_sync_cursor:set(footprint, 3 * Offset,
            ar_sync_cursor:set(byte, 2 * Offset,
                ar_sync_cursor:new(0, RangeEnd))),
        NextSweep = {Offset, make_ref()},
        State = #state{
            store_id = test_store,
            range_start = 0,
            range_end = RangeEnd,
            weave_size = RangeEnd,
            cursor = Cursor,
            next_sweep = NextSweep
        },
        {noreply, State2} = handle_info(
            {event, sync_discovery,
                {chunk_intervals_updated, test_store, Offset}}, State),
        ?assertEqual(Cursor, State2#state.cursor),
        ?assert(gb_sets:is_element(
            {Offset, byte}, State2#state.pending_revisits)),
        ?assert(gb_sets:is_element(
            {Offset, footprint}, State2#state.pending_revisits)),
        {0, State3} = retry_pending_revisit(
            byte, Offset, RangeEnd, State2),
        ?assertNot(gb_sets:is_element(
            {Offset, byte}, State3#state.pending_revisits)),
        ?assert(gb_sets:is_element(
            {Offset, footprint}, State3#state.pending_revisits)),
        [UnsyncedRange] = get(revisited_ranges),
        ?assertEqual(byte, UnsyncedRange#unsynced_range.kind),
        ?assertEqual(Offset, UnsyncedRange#unsynced_range.query_offset),
        {0, State4} = retry_pending_revisit(
            footprint, Offset, RangeEnd, State3),
        ?assert(gb_sets:is_empty(State4#state.pending_revisits)),
        {noreply, State5} = handle_info(
            {event, sync_discovery,
                {chunk_intervals_updated, test_store, RangeEnd}}, State4),
        ?assert(gb_sets:is_empty(State5#state.pending_revisits))
    end, 30).

%% can_sweep gates range generation on disk space and the tip.
can_sweep_test_() ->
    ar_test_util:with_mocked([
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ], fun test_can_sweep/0, 30).

test_can_sweep() ->
    Cursor = ar_sync_cursor:new(0, 1000),
    State = #state{ store_id = store1, range_end = 1000, weave_size = 1000,
        cursor = Cursor },
    %% Disk ok and at least one cursor below the tip -> ready.
    ?assertEqual(ready, can_sweep(State, 1000)),
    %% Both cursors reached their live bounds -> sweep done.
    ?assertEqual(complete,
        can_sweep(State#state{ cursor = set_test_cursors(Cursor, 1000, 1000) }, 1000)),
    %% One cursor can finish early while the other keeps the sweep active.
    ?assertEqual(ready,
        can_sweep(State#state{ cursor = set_test_cursors(Cursor, 1000, 0) }, 1000)),
    %% The footprint cursor is complete at the disk-pool threshold even when the
    %% storage module range extends further.
    ?assertEqual(complete,
        can_sweep(State#state{ cursor = set_test_cursors(Cursor, 1000, 500) }, 500)),
    %% Weave tip below both cursors -> sweep done.
    ?assertEqual(complete, can_sweep(State#state{ weave_size = 0 }, 500)),
    %% Disk state gates need identification.
    meck:expect(ar_data_sync, is_disk_space_sufficient, fun(_) -> false end),
    ?assertEqual({blocked, 30_000}, can_sweep(State, 500)),
    meck:expect(ar_data_sync, is_disk_space_sufficient, fun(_) -> not_initialized end),
    ?assertEqual({blocked, 1_000}, can_sweep(State, 500)).

%% start_sweep builds a sweep over the module range once the node is joined and
%% the migration complete.
start_sweep_test_() ->
    ar_test_util:with_mocked([
        {ar_node, is_joined, fun() -> true end},
        {ar_data_sync, is_footprint_record_initialized, fun(_) -> true end},
        {ar_disk_pool, get_threshold, fun() -> 1000 end}
    ], fun test_start_sweep/0, 30).

test_start_sweep() ->
    State = #state{ store_id = store1, range_start = 0, range_end = 1000,
        weave_size = 1000 },
    {ok, Cursor} = start_sweep(State),
    ?assertEqual(0, ar_sync_cursor:current(byte, Cursor)),
    ?assertEqual(0, ar_sync_cursor:current(footprint, Cursor)),
    ?assertEqual(1000, ar_sync_cursor:live_end(byte, Cursor, 1000, 1000)),
    ?assertEqual(1000, ar_sync_cursor:live_end(footprint, Cursor, 1000, 1000)),
    %% Range entirely above the weave tip -> blocked.
    ?assertEqual(blocked, start_sweep(State#state{ range_start = 1000 })),
    %% Weave size not known yet -> blocked.
    ?assertEqual(blocked, start_sweep(State#state{ weave_size = undefined })).

set_test_cursors(Cursor, Byte, Footprint) ->
    ar_sync_cursor:set(footprint, Footprint,
        ar_sync_cursor:set(byte, Byte, Cursor)).

-endif.
