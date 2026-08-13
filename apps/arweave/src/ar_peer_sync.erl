%%% @doc Per-storage-module network-sync work discovery (gen_server).
%%%
%%% The `enqueue' loop repeatedly sweeps the module range — one #sweep{} at a
%%% time, alternating normal and footprint modes. Each sweep intersects
%%% ar_data_discovery's cached peer offers with this module's unsynced gaps
%%% (via ar_sync_record) and pushes chunk-sized #sync_task{} records to
%%% ar_sync_dispatcher (gated by ar_sync_dispatcher:ready_for_work/0 so the
%%% dispatcher's buffer stays bounded). No peer HTTP discovery happens here
%%% (that is all in ar_data_discovery).
%%%
%%% **Sole owner of `inflight_intervals'.** A range is added when
%%% the task is pushed and removed when ar_sync_dispatcher reports the task done
%%% (via release_task_range/3 from its single 'DOWN' handler), or wholesale via
%%% reset_inflight/1 when the dispatcher restarts.
-module(ar_peer_sync).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/1, name/1, register_workers/0]).
-export([start/1, release_task_range/3, reset_inflight/0, set_weave_size/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").
-include_lib("arweave/include/ar_sup.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([cut_peer_footprint_intervals/3]).
-endif.

%% One in-progress sweep over the module range. All sweep state lives in this
%% record; it moves through the enqueue state machine and is replaced when the
%% sweep completes and a new one starts.
-record(sweep, {
                %% Left bound of the sweep range. For footprint mode, an inclusive
                %% boundary used when cutting per-peer footprint intervals to the
                %% module.
                start :: non_neg_integer(),
                %% Right bound of the sweep range (clamped to WeaveSize /
                %% DiskPoolThreshold when the sweep is built).
                end_ :: non_neg_integer(),
                %% Current position inside [start, end_). Advances on each step.
                offset :: non_neg_integer(),
                %% Which protocol we're querying peers with on this sweep.
                mode :: normal | footprint,
                %% Count of tasks produced in the current sweep. Resets each sweep.
                %% Logged at sweep_complete and used to gate the chunk_sync_started
                %% log to fire only on the first productive step.
                tasks_produced = 0 :: non_neg_integer()
               }).

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
                %% Per-module task queue (ar_sync_task_queue; record
                %% mutated only by handlers in this gen_server).
                queue = ar_sync_task_queue:new(),
                %% In-progress sweep. `undefined' means the loop hasn't started its
                %% first sweep yet.
                sweep = undefined :: undefined | #sweep{}
               }).

-define(GET_SYNC_RECORD_PATH, [<<"data_sync_record">>]).
-define(GET_FOOTPRINT_RECORD_PATH, [<<"footprints">>]).
-define(FOOTPRINT_MIGRATION_CURSOR_KEY, <<"footprint_migration_cursor">>).

%% Fixed delay between sweeps. The enqueue loop no longer issues HTTP (that
%% lives in ar_data_discovery's per-peer scanner pool, which has its
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
    ar_peer_sync_default;
name(StoreID) ->
    list_to_atom("ar_peer_sync_" ++ ar_storage_module:label(StoreID)).

register_workers() ->
    [?CHILD_WITH_ARGS(?MODULE, worker, name(SID), [SID]) || SID <- store_ids()].

start_link(StoreID) ->
    gen_server:start_link({local, name(StoreID)}, ?MODULE, StoreID, []).

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Start (or re-kick) the work-discovery loop for the StoreID. Invoked by
%% ar_data_sync once chunk_copy completes and whenever the store re-enters sync.
start(StoreID) ->
    gen_server:cast(name(StoreID), enqueue).

%% @doc Remove a byte range from the in-flight intervals. Called by
%% ar_sync_dispatcher's 'DOWN' handler once a task's worker exits (the single
%% terminal path), so ar_peer_sync can re-discover the range if needed.
release_task_range(StoreID, Start, End) ->
    gen_server:cast(name(StoreID), {release_task_range, Start, End}).

%% @doc Clear every ar_peer_sync instance's in-flight intervals. Called by
%% ar_sync_dispatcher on its init so that, after a dispatcher restart, ranges it
%% had in flight (now lost) become re-discoverable. A cast to a not-yet-started
%% instance is silently dropped, so this is a no-op on first boot.
reset_inflight() ->
    [gen_server:cast(name(StoreID), reset_inflight) || StoreID <- store_ids()],
    ok.

%% @doc The StoreIDs that have an ar_peer_sync instance (one per storage module
%% plus the default module).
store_ids() ->
    [ar_storage_module:id(SM) || SM <- arweave_config:storage_modules()]
        ++ [?DEFAULT_MODULE].

%% @doc Update the weave-size snapshot. Called by ar_data_sync on chain-tip
%% moves so the enqueue loop's range clamp follows the tip.
set_weave_size(StoreID, WeaveSize) ->
    gen_server:cast(name(StoreID), {set_weave_size, WeaveSize}).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
    {RangeStart, RangeEnd} = init_range(StoreID),
    ?LOG_INFO([{event, init}, {module, ?MODULE}, {store_id, StoreID},
               {range_start, RangeStart}, {range_end, RangeEnd}]),
    {ok, #state{
            store_id = StoreID,
            range_start = RangeStart,
            range_end = RangeEnd,
            sync_status = ar_data_sync:init_sync_status(StoreID)
           }}.

init_range(?DEFAULT_MODULE) ->
    {-1, -1};
init_range(StoreID) ->
    case (catch ar_storage_module:get_range(StoreID)) of
        {'EXIT', _} -> {-1, -1};
        {RangeStart, RangeEnd} ->
            {max(0, ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
             ar_block:get_chunk_padded_offset(RangeEnd)}
    end.

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

%% Enqueue (work-discovery) step. Kicked by ar_data_sync after the chunk_copy
%% phase and then self-perpetuating. Hold the sync-mode device lock while the
%% loop runs; ar_sync_dispatcher's fetch workers only write to a store in sync mode.
handle_cast(enqueue, State) ->
    Status = ar_device_lock:acquire_lock(
               sync, State#state.store_id, State#state.sync_status),
    State2 = State#state{ sync_status = Status },
    case Status of
        active ->
            enqueue(State2);
        paused ->
            arweave_util:cast_after(?DEVICE_LOCK_WAIT, self(), enqueue),
            {noreply, State2};
        _ ->
            %% off / complete — not in sync mode. The loop is re-kicked by
            %% ar_data_sync when the store re-enters sync.
            {noreply, State2}
    end;

%% Byte-range release from the dispatcher's terminal 'DOWN' handler.
handle_cast({release_task_range, Start, End}, State) ->
    NewQ = ar_sync_task_queue:release_task_range(Start, End, State#state.queue),
    State2 = State#state{ queue = NewQ },
    publish_queue_metrics(State2),
    {noreply, State2};

%% Wholesale reset of the in-flight intervals after a dispatcher restart: drop
%% every in-flight range so it can be re-discovered.
handle_cast(reset_inflight, State) ->
    State2 = State#state{ queue = ar_sync_task_queue:new() },
    publish_queue_metrics(State2),
    {noreply, State2};

%% Chain-tip update from ar_data_sync. A decrease (reorg) needs no special
%% handling: the smaller tip takes effect via min(end_, WeaveSize) in the
%% enqueue loop; tasks already pushed past the shrunk tip simply fail to fetch
%% and have their ranges released.
handle_cast({set_weave_size, WeaveSize}, State) ->
    {noreply, State#state{ weave_size = WeaveSize }};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, _State) ->
    ?LOG_INFO([{event, terminate}, {module, ?MODULE},
               {reason, io_lib:format("~p", [Reason])}]),
    ok.

%%%===================================================================
%%% Step implementation (operate on #state{}).
%%%===================================================================

enqueue(#state{ sweep = undefined } = State) ->
    case start_sweep(State, normal) of
        {ok, Sweep} ->
            gen_server:cast(self(), enqueue),
            {noreply, State#state{ sweep = Sweep }};
        not_ready ->
            %% Node not joined yet, or footprint migration in flight.
            arweave_util:cast_after(1000, self(), enqueue),
            {noreply, State}
    end;
enqueue(#state{ sweep = #sweep{} } = State) ->
    publish_queue_metrics(State),
    case can_enqueue(State) of
        {wait, offset_past_end, _Delay} ->
            %% Offset reached min(end_, WeaveSize) — the sweep is done. This is the
            %% sole completion trigger, so the sweep tracks a shrinking or growing
            %% weave tip without ever rewriting end_.
            complete_sweep(State);
        {wait, _Reason, Delay} ->
            arweave_util:cast_after(Delay, self(), enqueue),
            {noreply, State};
        ready ->
            {Action, NewState} = do_enqueue(State),
            case Action of
                cast_now -> gen_server:cast(self(), enqueue);
                {cast_after, Ms} -> arweave_util:cast_after(Ms, self(), enqueue)
            end,
            {noreply, NewState}
    end.

do_enqueue(#state{ sweep = #sweep{ mode = normal } } = State) ->
    do_enqueue_normal(State);
do_enqueue(#state{ sweep = #sweep{ mode = footprint } } = State) ->
    do_enqueue_footprint(State).

do_enqueue_normal(State) ->
    #state{ store_id = StoreID, weave_size = WeaveSize, queue = Q,
            sweep = #sweep{ offset = Offset, end_ = End } = Sweep } = State,
    End2 = min(min(Offset + ?QUERY_RANGE_STEP_SIZE, End), WeaveSize),
    UnsyncedIntervals = get_unsynced_intervals(Offset, End2, StoreID),
    case ar_intervals:is_empty(UnsyncedIntervals) of
        true ->
            NewSweep = Sweep#sweep{ offset = End2 },
            {cast_now, State#state{ sweep = NewSweep }};
        false ->
            case get_hot_peers(Offset, normal) of
                wait ->
                    {{cast_after, 1000}, State};
                Peers ->
                    {PeerCoverageEnd, FetchableEntries} =
                        determine_fetchable_intervals_normal(
                          Offset, Peers, UnsyncedIntervals),
                    {Tasks, NewQ} = claim_tasks(StoreID, FetchableEntries, Q),
                    ar_sync_dispatcher:enqueue(Tasks),
                    Produced = length(Tasks),
                    maybe_log_chunk_sync_started(StoreID, normal, Sweep, Produced),
                    %% If peers don't advertise data past Offset for this window,
                    %% skip the whole window to avoid spinning at the same cursor.
                    NewOffset =
                        case PeerCoverageEnd > Offset of
                            true -> min(End2, PeerCoverageEnd);
                            false -> End2
                        end,
                    NewSweep = Sweep#sweep{
                                 offset = NewOffset,
                                 tasks_produced = Sweep#sweep.tasks_produced
                                 + max(0, Produced)
                                },
                    {cast_now, State#state{ queue = NewQ, sweep = NewSweep }}
            end
    end.

do_enqueue_footprint(State) ->
    #state{ store_id = StoreID, queue = Q,
            sweep = #sweep{ start = Start, end_ = End, offset = Offset }
            = Sweep } = State,
    Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
    Footprint = ar_footprint_record:get_footprint(Offset + ?DATA_CHUNK_SIZE),
    UnsyncedIntervals =
        ar_footprint_record:get_unsynced_intervals(Partition, Footprint, StoreID),
    case ar_intervals:is_empty(UnsyncedIntervals) of
        true ->
            Offset2 = ar_replica_2_9:get_next_fetch_offset(Offset, Start, End),
            NewSweep = Sweep#sweep{ offset = Offset2 },
            {cast_now, State#state{ sweep = NewSweep }};
        false ->
            case get_hot_peers(Offset, footprint) of
                wait ->
                    {{cast_after, 1000}, State};
                Peers ->
                    FetchableEntries = determine_fetchable_intervals_footprint(
                                         Partition, Footprint, Start, End, Peers, UnsyncedIntervals),
                    {Tasks, NewQ} = claim_tasks(StoreID, FetchableEntries, Q),
                    ar_sync_dispatcher:enqueue(Tasks),
                    Produced = length(Tasks),
                    maybe_log_chunk_sync_started(StoreID, footprint, Sweep, Produced),
                    Offset2 = ar_replica_2_9:get_next_fetch_offset(Offset, Start, End),
                    NewSweep = Sweep#sweep{
                                 offset = Offset2,
                                 tasks_produced = Sweep#sweep.tasks_produced
                                 + max(0, Produced)
                                },
                    {cast_now, State#state{ queue = NewQ, sweep = NewSweep }}
            end
    end.

%% Log once per sweep.
maybe_log_chunk_sync_started(StoreID, Mode,
                             #sweep{ tasks_produced = 0 }, Produced) when Produced > 0 ->
    ?LOG_DEBUG([{event, sync_network}, {stage, chunk_sync_started},
                {store_id, StoreID}, {mode, Mode}, {tasks_enqueued, Produced}]);
maybe_log_chunk_sync_started(_StoreID, _Mode, _Pass, _Produced) ->
    ok.

can_enqueue(#state{ store_id = StoreID, weave_size = WeaveSize,
                    sweep = #sweep{ offset = Offset, end_ = End } }) ->
    case ar_data_sync:is_disk_space_sufficient(StoreID) of
        false ->
            {wait, disk_full, 30_000};
        not_initialized ->
            {wait, disk_info_missing, 1_000};
        true ->
            %% Backpressure is owned by the dispatcher: it stops accepting once
            %% its buffer + in-flight reach max_tasks, which keeps the buffer
            %% bounded without a second queue here.
            case ar_sync_dispatcher:ready_for_work() of
                false ->
                    {wait, dispatcher_full, 200};
                true ->
                    End2 = min(End, WeaveSize),
                    case Offset >= End2 of
                        true ->
                            {wait, offset_past_end, 500};
                        false ->
                            ready
                    end
            end
    end.

%% Flip mode and start the next sweep; the current sweep is finished (offset
%% reached the live tip).
complete_sweep(#state{ store_id = StoreID,
                       sweep = #sweep{ mode = Mode, tasks_produced = TasksProduced } } = State) ->
    NextMode = flip_mode(Mode),
    ?LOG_DEBUG([{event, sync_network}, {stage, sweep_complete},
                {store_id, StoreID}, {mode, Mode}, {tasks_produced, TasksProduced},
                {next_mode, NextMode}]),
    case start_sweep(State, NextMode) of
        {ok, Sweep2} ->
            arweave_util:cast_after(?SWEEP_RESTART_DELAY_MS, self(), enqueue),
            {noreply, State#state{ sweep = Sweep2 }};
        not_ready ->
            %% Clear the sweep so later enqueue casts hit the sweep=undefined clause
            %% (silent retry) instead of re-logging sweep_complete every second.
            arweave_util:cast_after(1000, self(), enqueue),
            {noreply, State#state{ sweep = undefined }}
    end.

%% Build a new sweep in the given mode.
start_sweep(#state{ store_id = StoreID, range_start = Start, range_end = End,
                    weave_size = WeaveSize }, Mode) ->
    case ready_to_start(StoreID, WeaveSize) of
        false ->
            not_ready;
        true ->
            %% end_ is the sweep's static target: the storage-module range, capped
            %% by the disk-pool threshold in footprint mode. The live weave tip is
            %% applied as min(end_, WeaveSize) at each enqueue/completion check, so
            %% the sweep tracks the tip up and down without rewriting end_.
            %% ready_to_start guarantees WeaveSize is bound here.
            SweepEnd = case Mode of
                           footprint ->
                               min(End, ar_disk_pool:get_threshold());
                           normal ->
                               End
                       end,
            case Start >= min(SweepEnd, WeaveSize) of
                true ->
                    %% Storage module's range is entirely above the current weave
                    %% tip (or disk-pool threshold for footprint mode); nothing to
                    %% sync yet. Caller cast_afters on not_ready.
                    not_ready;
                false ->
                    ?LOG_DEBUG([{event, sync_network}, {stage, sweep_started},
                                {store_id, StoreID}, {mode, Mode},
                                {start, Start}, {end_, SweepEnd}]),
                    {ok, #sweep{
                            start = Start, end_ = SweepEnd,
                            offset = Start, mode = Mode }}
            end
    end.

%%%===================================================================
%%% Per-peer fetchable-interval computation from the ar_data_discovery cache.
%%%===================================================================

%% @doc For each peer, intersect its cached advertised intervals with
%% our UnsyncedIntervals to determine what we can fetch. Returns
%% {PeerCoverageEnd, FetchableEntries}, where PeerCoverageEnd is the min right
%% bound across peers (used to bound cursor advance) and FetchableEntries
%% is a list of {Peer, FetchableIntervals, FootprintKey} entries.
determine_fetchable_intervals_normal(Left, Peers, UnsyncedIntervals) ->
    lists:foldl(
      fun(Peer, {RightAcc, Acc}) ->
              case ar_data_discovery:get_peer_intervals(Peer, Left, infinity) of
                  {ok, PeerIntervals, PeerRight} ->
                      FetchableIntervals = ar_intervals:intersection(
                                             PeerIntervals, UnsyncedIntervals),
                      case ar_intervals:is_empty(FetchableIntervals) of
                          true -> {min(RightAcc, PeerRight), Acc};
                          false ->
                              {min(RightAcc, PeerRight),
                               [{Peer, FetchableIntervals, none} | Acc]}
                      end;
                  {error, _} ->
                      {RightAcc, Acc}
              end
      end,
      {infinity, []},
      Peers
     ).

determine_fetchable_intervals_footprint(
  Partition, Footprint, Start, End, Peers, UnsyncedIntervals) ->
    lists:foldl(
      fun(Peer, Acc) ->
              case ar_data_discovery:get_peer_footprint_intervals(Peer, Partition, Footprint) of
                  {ok, PeerIntervals} ->
                      FetchableIntervals = ar_intervals:intersection(
                                             PeerIntervals, UnsyncedIntervals),
                      case ar_intervals:is_empty(FetchableIntervals) of
                          true -> Acc;
                          false ->
                              ByteIntervals = cut_peer_footprint_intervals(
                                                FetchableIntervals, Start, End),
                              FootprintKey = {Partition, Footprint, Peer},
                              [{Peer, ByteIntervals, FootprintKey} | Acc]
                      end;
                  {error, _} ->
                      Acc
              end
      end,
      [],
      Peers
     ).

%% @doc Build the new chunk-fetch #sync_task{}s from this step's fetchable
%% peer-interval offers: cap each peer's share, drop ranges already in flight,
%% slice the rest into chunks, and claim their ranges in the in-flight intervals
%% (so they aren't produced again until released). Returns {Tasks, NewQueue};
%% the caller pushes the tasks to the dispatcher.
claim_tasks(_StoreID, [], Queue) ->
    {[], Queue};
claim_tasks(StoreID, PeerEntries, Queue) ->
    TotalChunksToEnqueue = ?DEFAULT_SYNC_BUCKET_SIZE div ?DATA_CHUNK_SIZE,
    NumPeers = length(PeerEntries),
    ScalingFactor = 1.5,
    ChunksPerPeer = trunc(((TotalChunksToEnqueue + NumPeers - 1) div NumPeers) * ScalingFactor),
    Queue2 = ar_sync_task_queue:insert_batch(
               arweave_util:shuffle_list(PeerEntries), ChunksPerPeer, Queue),
    {Drained, Queue3} = ar_sync_task_queue:drain(Queue2),
    Tasks = [#sync_task{ start_offset = Start, end_offset = End, peer = Peer,
                         store_id = StoreID, footprint_key = FootprintKey }
             || {FootprintKey, Start, End, Peer} <- Drained],
    {Tasks, Queue3}.

%% @doc Publish this store's in-flight-interval byte total (queued + currently
%% fetching, i.e. ranges pushed to the dispatcher and not yet released). A
%% climbing-without-bound value flags a dedup-overlay leak.
publish_queue_metrics(#state{ store_id = StoreID, queue = Queue }) ->
    arweave_metrics:gauge_set(sync_task_queue_inflight_bytes,
                         [ar_storage_module:label(StoreID)],
                         ar_sync_task_queue:inflight_bytes(Queue)).

%%%===================================================================
%%% Peer picking.
%%%===================================================================

get_hot_peers(Offset, normal) ->
    Bucket = Offset div ?NETWORK_DATA_BUCKET_SIZE,
    get_hot_peers_for_bucket(
      fun() -> ar_data_discovery:get_bucket_peers(Bucket) end,
      ?GET_SYNC_RECORD_PATH);
get_hot_peers(Offset, footprint) ->
    FootprintBucket = ar_footprint_record:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE),
    get_hot_peers_for_bucket(
      fun() -> ar_data_discovery:get_footprint_bucket_peers(FootprintBucket) end,
      ?GET_FOOTPRINT_RECORD_PATH).

get_hot_peers_for_bucket(GetAllFun, Path) ->
    LocalOnly = arweave_config:get([sync, local_peers_only]),
    AllPeers =
        case LocalOnly of
            true -> arweave_config:get([peers, local]);
            false -> GetAllFun()
        end,
    HotPeers = [
                Peer || Peer <- AllPeers, not arweave_throttling:is_throttled(Peer, Path)
               ],
    case length(AllPeers) > 0 andalso length(HotPeers) == 0 of
        true ->
            wait;
        false ->
            ar_data_discovery:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT)
    end.

%%%===================================================================
%%% Unsynced interval gathering.
%%%===================================================================

get_unsynced_intervals(Start, End, StoreID) ->
    UnsyncedIntervals = get_unsynced_intervals(Start, End, ar_intervals:new(), StoreID),
    BlacklistedIntervals = ar_tx_blacklist:get_blacklisted_intervals(Start, End),
    ar_intervals:outerjoin(BlacklistedIntervals, UnsyncedIntervals).

get_unsynced_intervals(Start, End, Intervals, _StoreID) when Start >= End ->
    Intervals;
get_unsynced_intervals(Start, End, Intervals, StoreID) ->
    case ar_sync_record:get_next_synced_interval(Start, End, ar_data_sync, StoreID) of
        not_found ->
            ar_intervals:add(Intervals, End, Start);
        {End2, Start2} ->
            case Start2 > Start of
                true ->
                    End3 = min(Start2, End),
                    get_unsynced_intervals(End2, End,
                                           ar_intervals:add(Intervals, End3, Start), StoreID);
                _ ->
                    get_unsynced_intervals(End2, End, Intervals, StoreID)
            end
    end.

%%%===================================================================
%%% Sweep lifecycle.
%%%===================================================================

%% @doc The intervals returned by a peer may include intervals beyond the
%% storage module boundaries. This is because we end up querying all seeded
%% intervals belonging to a footprint that intersects this node's unsynced
%% intervals. Remove everything outside [Start, End].
cut_peer_footprint_intervals(FootprintIntervals, Start, End) ->
    ByteIntervals =
        ar_footprint_record:get_intervals_from_footprint_intervals(FootprintIntervals),
    ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
    PaddedStart =
        case ar_block:get_chunk_padded_offset(Start) of
            Start -> Start;
            PaddedOffset -> PaddedOffset - ?DATA_CHUNK_SIZE
        end,
    ar_intervals:outerjoin(
      ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2).

ready_to_start(_StoreID, undefined) ->
    false;
ready_to_start(StoreID, _WeaveSize) ->
    case ar_node:is_joined() of
        false -> false;
        true ->
            case ar_kv:get(ar_data_sync:migration_db(StoreID),
                           ?FOOTPRINT_MIGRATION_CURSOR_KEY) of
                {ok, <<"complete">>} -> true;
                _ -> false
            end
    end.

flip_mode(normal) -> footprint;
flip_mode(footprint) -> normal;
flip_mode(undefined) -> normal.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

cut_peer_footprint_intervals_test() ->
    ?assertEqual(
       ar_intervals:from_list([{786432, 524288}, {1310720, 1048576}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{4, 0}]), 262144, 1572864),
       "Full Footprint 0, aligned boundaries"),

    ?assertEqual(
       ar_intervals:from_list([{524288,262144}, {1048576,786432}, {1572864,1310720}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{8, 4}]), 262144, 1572864),
       "Full Footprint 1 cut to aligned boundaries"),

    ?assertEqual(
       ar_intervals:from_list([
                               {262144,200000}, {786432, 524288}, {1310720, 1048576}, {1600000,1572864}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{4, 0}]), 200000, 1600000),
       "Full Footprint 0, unaligned boundaries, pre-strict"),

    ?assertEqual(
       ar_intervals:from_list([{524288,262144}, {1048576, 786432}, {1572864, 1310720}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{8, 4}]), 200000, 1600000),
       "Full Footprint 1, unaligned boundaries, pre-strict"),

    ?assertEqual(
       ar_intervals:from_list([{2883584,2621440}, {3407872,3145728}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{12, 8}]), 2400000, 3500000),
       "Full Footprint 2, unaligned boundaries, post-strict"),

    ?assertEqual(
       ar_intervals:from_list([{2621440,2359296}, {3145728,2883584}, {3500000,3407872}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{16, 12}]), 2400000, 3500000),
       "Full Footprint 3, unaligned boundaries, post-strict"),

    ?assertEqual(
       ar_intervals:from_list([{2621440,2359296}, {3500000,3407872}]),
       cut_peer_footprint_intervals(
         ar_intervals:from_list([{16, 14}, {13, 12}]), 2400000, 3500000),
       "Partial Footprint 3, unaligned boundaries, post-strict"),

    ok.

set_weave_size_decrease_keeps_sweep_and_queue_test() ->
    Queue = ar_sync_task_queue:insert_batch(
              [{{127, 0, 0, 1, 1984},
                ar_intervals:from_list([{2 * ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}]), none}],
              1,
              ar_sync_task_queue:new()),
    Sweep = #sweep{
               start = 0,
               end_ = 2 * ?DATA_CHUNK_SIZE,
               offset = ?DATA_CHUNK_SIZE,
               mode = normal
              },
    State = #state{
               store_id = test_store,
               weave_size = 2 * ?DATA_CHUNK_SIZE,
               queue = Queue,
               sweep = Sweep
              },
    {noreply, State2} = handle_cast({set_weave_size, ?DATA_CHUNK_SIZE}, State),
    %% The decrease updates the cached size and leaves the sweep and queue intact;
    %% the shrunk tip takes effect via min(end_, WeaveSize) downstream.
    ?assertEqual(?DATA_CHUNK_SIZE, State2#state.weave_size),
    ?assertEqual(2 * ?DATA_CHUNK_SIZE, (State2#state.sweep)#sweep.end_),
    ?assertEqual(1, ar_sync_task_queue:size(State2#state.queue)).

flip_mode_test() ->
    ?assertEqual(footprint, flip_mode(normal)),
    ?assertEqual(normal, flip_mode(footprint)),
    ?assertEqual(normal, flip_mode(undefined)).

%% claim_tasks builds chunk #sync_task{}s from peer-interval offers, claims their
%% ranges in the in-flight intervals (deduping), and is a no-op on empty input.
claim_tasks_test() ->
    Peer = {1, 2, 3, 4, 1984},
    Entries = [{Peer, ar_intervals:from_list([{2 * ?DATA_CHUNK_SIZE, 0}]), none}],
    Q0 = ar_sync_task_queue:new(),
    %% Empty input -> no tasks, queue unchanged.
    ?assertEqual({[], Q0}, claim_tasks(store1, [], Q0)),
    %% The 2-chunk interval becomes two tasks (store_id + footprint_key carried).
    {Tasks, Q1} = claim_tasks(store1, Entries, Q0),
    ?assertEqual(2, length(Tasks)),
    ?assert(lists:all(fun(#sync_task{ store_id = S, footprint_key = FK }) ->
                              S =:= store1 andalso FK =:= none
                      end, Tasks)),
    %% Re-claiming the same entries (now in flight) yields nothing.
    ?assertEqual({[], Q1}, claim_tasks(store1, Entries, Q1)).

%% can_enqueue gates production on disk space, dispatcher capacity, and the tip.
can_enqueue_test_() ->
    ar_test_util:with_mocked([
                              {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
                              {ar_sync_dispatcher, ready_for_work, fun() -> true end}
                             ], fun test_can_enqueue/0, 30).

test_can_enqueue() ->
    Sweep = #sweep{ start = 0, end_ = 1000, offset = 0, mode = normal },
    State = #state{ store_id = store1, weave_size = 1000, sweep = Sweep },
    %% Disk ok, dispatcher ready, offset below the tip -> ready.
    ?assertEqual(ready, can_enqueue(State)),
    %% Offset reached end_ -> sweep done.
    ?assertMatch({wait, offset_past_end, _},
                 can_enqueue(State#state{ sweep = Sweep#sweep{ offset = 1000 } })),
    %% Weave tip below the offset (min(end_, WeaveSize)) -> sweep done.
    ?assertMatch({wait, offset_past_end, _}, can_enqueue(State#state{ weave_size = 0 })),
    %% Dispatcher at capacity -> wait (disk still ok).
    meck:expect(ar_sync_dispatcher, ready_for_work, fun() -> false end),
    ?assertMatch({wait, dispatcher_full, _}, can_enqueue(State)),
    %% Disk gates ahead of the dispatcher check.
    meck:expect(ar_data_sync, is_disk_space_sufficient, fun(_) -> false end),
    ?assertMatch({wait, disk_full, _}, can_enqueue(State)),
    meck:expect(ar_data_sync, is_disk_space_sufficient, fun(_) -> not_initialized end),
    ?assertMatch({wait, disk_info_missing, _}, can_enqueue(State)).

%% start_sweep builds a sweep over the module range (footprint mode capped at
%% the disk-pool threshold) once the node is joined and the migration complete.
start_sweep_test_() ->
    ar_test_util:with_mocked([
                              {ar_node, is_joined, fun() -> true end},
                              {ar_data_sync, migration_db, fun(_) -> migration_db end},
                              {ar_kv, get, fun(migration_db, _) -> {ok, <<"complete">>} end},
                              {ar_disk_pool, get_threshold, fun() -> 500 end}
                             ], fun test_start_sweep/0, 30).

test_start_sweep() ->
    State = #state{ store_id = store1, range_start = 0, range_end = 1000,
                    weave_size = 1000 },
    %% Normal sweep spans the whole range from its start.
    {ok, Normal} = start_sweep(State, normal),
    ?assertEqual({0, 1000, normal}, {Normal#sweep.offset, Normal#sweep.end_, Normal#sweep.mode}),
    %% Footprint sweep's end_ is clamped to the disk-pool threshold (500 < 1000).
    {ok, Footprint} = start_sweep(State, footprint),
    ?assertEqual(500, Footprint#sweep.end_),
    %% Range entirely above the weave tip -> not_ready.
    ?assertEqual(not_ready, start_sweep(State#state{ range_start = 1000 }, normal)),
    %% Weave size not known yet -> not_ready.
    ?assertEqual(not_ready, start_sweep(State#state{ weave_size = undefined }, normal)).

-endif.
