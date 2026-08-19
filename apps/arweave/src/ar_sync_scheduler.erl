%%% @doc Global scheduler for the network-sync subsystem.
%%%
%%% ar_sync_store_sweeper resolves local need through ar_sync_chunk_picker,
%%% and pushes tasks and footprint work here, bounded by the
%%% store's measured queue target.
%%% This server owns task selection and dispatch. Tasks first bind into a
%%% bounded runnable queue for one peer, then enter active fetching up to that
%%% peer's concurrency cap. It spawn_monitors one transient
%%% `ar_sync_fetch_worker' per active task and tracks the task through fetch
%%% and asynchronous storage completion.
%%%
%%% Worker liveness is network-concurrency accounting: inflight worker count is
%%% `map_size(monitor_index)'. Task state is backpressure accounting: claimed
%%% ranges and footprint entropy slots are released when a failed fetch is
%%% terminal or the one handed-off chunk reaches a terminal storage state.
-module(ar_sync_scheduler).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, register_workers/0,
        admission_headroom/1, claim_and_enqueue/2,
        tick_interval_ms/0,
        task_fetch_completed/3, task_write_completed/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(AR_TEST).
-export([inflight_count/0,
        override_tick_interval_ms/1, reset_all_overrides/0]).
-endif.

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_sync.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%% Cadence for the periodic re-dispatch tick (a cheap re-dispatch of freed
%% capacity, and the metric-emit cadence).
-define(TICK_INTERVAL_MS, 10_000).

-record(state, {
    %% Opaque indexed store state.
    stores = ar_sync_store:new(),
    %% TaskRef => #task{}: dispatched tasks in the fetching, writing, or
    %% write_complete state. A task leaves when its failed fetch or one async
    %% storage handoff reaches a terminal state.
    tasks = #{},
    %% MonitorRef => {TaskRef, WorkerPID}: routes the worker monitor's 'DOWN' to
    %% its task entry and lets the scheduler terminate the workers it owns.
    %% map_size is the inflight worker count.
    monitor_index = #{},
    %% Opaque footprint state.
    footprints = #{},
    %% Peer => FIFO of peer-bound tasks waiting for an active fetch slot.
    peer_queues = #{},
    %% Peer => true once any dispatch in the current scheduler interval sees
    %% runnable work held behind that peer's full active cap.
    driven_peers = #{},
    %% ar_sync_peer's opaque state, evolved on the scheduler tick.
    peer_state = ar_sync_peer:new(),
    %% Debounce flag: a dispatch self-cast is already queued. enqueue and
    %% worker-DOWN only mark that a dispatch is needed rather than each running a
    %% full dispatch pass; the single queued dispatch then drains all freed capacity
    %% in one pass. Without this, a burst of DOWN/enqueue messages (multi-peer churn)
    %% runs one expensive selection pass per message, the gen_server falls behind, and
    %% the task map goes stale so the dispatcher wrongly believes it is full.
    dispatch_scheduled = false,
    %% Opaque global download-rate limiter, including its refill and wakeup
    %% state. ar_sync_download_limit owns the token-bucket behavior.
    download_limit = ar_sync_download_limit:new()
}).

%% Mutable snapshot built while selecting one dispatch batch;
%% stores, peers, and footprints are updated as queued tasks are selected.
-record(dispatch, {
    %% Opaque indexed store dispatches.
    stores,
    worker_count,        %% live plus selected worker count for this pass
    peers,               %% ar_sync_peer:dispatch()
    footprints,          %% ar_sync_footprint:dispatch()
    peer_queues,          %% Peer => queue of bound runnable #task{}
    tasks_to_start = []  %% [#task{}] selected this pass (task_ref minted at spawn)
}).

%% A resolved dispatch candidate. Item retains the task or footprint's
%% lifecycle state while the remaining fields present one common selection
%% contract to the scheduler.
-record(work, {
    item,
    store_id,
    sources = []
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Supervisor child spec for the dispatcher. Caller gates on
%% ar_sync_download_limit:enabled/0.
register_workers() ->
    [?CHILD(?MODULE, worker)].

%% @doc Advisory chunk headroom for a store: how many more chunks the picker
%% should bother building. Exact admission is decided in claim_and_enqueue/2,
%% which stays authoritative if capacity moved in between.
admission_headroom(StoreID) ->
    case catch gen_server:call(?MODULE, {admission_headroom, StoreID}, infinity) of
        {'EXIT', _Reason} -> 0;
        Headroom -> Headroom
    end.

%% @doc Claim and queue as many candidate tasks as still fit. Synchronous so the
%% claimed set reflects this batch before overlapping ranges are offered.
claim_and_enqueue(_StoreID, []) ->
    {ok, 0};
claim_and_enqueue(StoreID, Tasks) ->
    case catch gen_server:call(?MODULE,
            {claim_and_enqueue, StoreID, Tasks}, infinity) of
        {'EXIT', _Reason} -> blocked;
        Reply -> Reply
    end.

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
    ?LOG_INFO([{event, init}, {module, ?MODULE}]),
    %% A restarted scheduler must not expose a cap from its previous state.
    ar_sync_peer:reset_rows(),
    {ok, _} = ar_timer:send_after(tick_interval_ms(),
        self(), scheduler_tick),
    {ok, #state{}}.

handle_call(get_state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(ping, _From, State) ->
    %% A synchronous no-op: replying proves every cast queued before the
    %% ping has been processed (tests use it to flush the pipeline).
    {reply, pong, State};
handle_call(inflight_count, _From, State) ->
    {reply, map_size(State#state.monitor_index), State};
handle_call({admission_headroom, StoreID}, _From, State) ->
    HeadroomChunks = ar_sync_store:admission_headroom(
        StoreID, State#state.stores),
    {reply, HeadroomChunks, State};
handle_call({claim_and_enqueue, StoreID, Tasks}, _From, State) ->
    {Result, State2} = admit_tasks(StoreID, Tasks, State),
    {reply, Result, schedule_dispatch(State2)};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

handle_cast(dispatch, State) ->
    State2 = dispatch(State#state{ dispatch_scheduled = false }),
    {noreply, State2};

handle_cast({task_fetch_completed, TaskRef, BytesFetched, FetchTiming}, State) ->
    {noreply, schedule_dispatch(
        on_task_fetch_completed(TaskRef, BytesFetched, FetchTiming, State))};

handle_cast({task_write_completed, TaskRef}, State) ->
    {noreply, schedule_dispatch(on_task_write_completed(TaskRef, State))};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, Reason}, State) ->
    {noreply, schedule_dispatch(worker_exited(Ref, Reason, State))};

handle_info(scheduler_tick, State) ->
    {ok, _} = ar_timer:send_after(tick_interval_ms(),
        self(), scheduler_tick),
    State2 = dispatch(tick(State, ar_timer:monotonic_ms())),
    emit_sync_metrics(State2),
    {noreply, State2};

handle_info({ar_sync_download_limit, wakeup}, State) ->
    {noreply, schedule_dispatch(State#state{ download_limit =
        ar_sync_download_limit:wakeup_fired(State#state.download_limit) })};

handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, #state{ monitor_index = MonitorIndex }) ->
    terminate_workers(MonitorIndex),
    ?LOG_INFO([{event, terminate}, {module, ?MODULE},
        {reason, io_lib:format("~p", [Reason])}]),
    ok.

terminate_workers(MonitorIndex) ->
    Monitors = lists:map(
        fun({_TaskRef, WorkerPID}) ->
            {WorkerPID, erlang:monitor(process, WorkerPID)}
        end,
        maps:values(MonitorIndex)),
    lists:foreach(
        fun({WorkerPID, _MonitorRef}) -> exit(WorkerPID, kill) end,
        Monitors),
    lists:foreach(
        fun({WorkerPID, MonitorRef}) ->
            receive
                {'DOWN', MonitorRef, process, WorkerPID, _Reason} -> ok
            end
        end,
        Monitors),
    ok.

%%%===================================================================
%%% Admission.
%%%===================================================================

%% Admit candidates in order until the store queue limit is reached. A chunk already
%% claimed is dropped because another peer's copy is already queued; the picker
%% will offer it again if that claim is released.
admit_tasks(StoreID, Candidates, State) ->
    admit_tasks(StoreID, Candidates, 0, false, State).

admit_tasks(_StoreID, [], Admitted, Blocked, State) ->
    Result = case Blocked of
        true -> blocked;
        false -> {ok, Admitted}
    end,
    {Result, State};
admit_tasks(StoreID, [Candidate | Rest], Admitted, Blocked, State) ->
    case admit_candidate(StoreID, Candidate, State) of
        {blocked, State2} ->
            %% Keep scanning because a later candidate may have an eligible
            %% source even when this candidate cannot currently bind.
            admit_tasks(StoreID, Rest, Admitted, true, State2);
        {ok, ClaimedChunks, State2} ->
            admit_tasks(StoreID, Rest, Admitted + ClaimedChunks,
                Blocked, State2)
    end.

admit_candidate(StoreID, #task{} = Task, State) ->
    case admit_task(StoreID, Task, State) of
        {ok, ClaimedChunks, State2} ->
            {ok, ClaimedChunks, State2};
        blocked ->
            {blocked, State}
    end;
admit_candidate(StoreID, Reservation, State) ->
    #state{ stores = StoreStates, footprints = Footprints } = State,
    case ar_sync_footprint:admit(Reservation, Footprints) of
        {ok, 0, Footprints2} ->
            {ok, 0, State#state{ footprints = Footprints2 }};
        {ok, ClaimedChunks, Footprints2} ->
            case ar_sync_store:admit(StoreID, Reservation, StoreStates) of
                {ok, ClaimedChunks, StoreStates2} ->
                    {ok, ClaimedChunks, State#state{
                        stores = StoreStates2,
                        footprints = Footprints2 }};
                blocked ->
                    %% A reservation is speculative. Skipping it lets the
                    %% sweep reach existing reservations whose later snapshots
                    %% may have better sources; concrete-task backpressure
                    %% still retains the warm-window head.
                    {ok, 0, State}
            end
    end.

admit_task(StoreID, Task, State) ->
    StoreStates = State#state.stores,
    case ar_sync_store:admit(
            StoreID, Task#task{ store_id = StoreID }, StoreStates) of
        {ok, ClaimedChunks, StoreStates2} ->
            {ok, ClaimedChunks, State#state{ stores = StoreStates2 }};
        blocked ->
            blocked
    end.

%%%===================================================================
%%% Dispatch.
%%%===================================================================

%% @doc Queue one dispatch pass unless one is already pending. The scheduler
%% tick invokes dispatch directly as a fallback.
schedule_dispatch(#state{ dispatch_scheduled = true } = State) ->
    State;
schedule_dispatch(State) ->
    gen_server:cast(self(), dispatch),
    State#state{ dispatch_scheduled = true }.

%% @doc One dispatch pass: fill bounded peer queues, activate queued tasks up
%% to peer/global/store limits, refill the vacated queue slots, and spawn the
%% selected fetch workers.
dispatch(State0) ->
    DownloadLimit = ar_sync_download_limit:refill(State0#state.download_limit),
    State1 = State0#state{ download_limit = DownloadLimit },
    Dispatch0 = start_dispatch(State1),
    {State2, Dispatch1} = bind_tasks(State1, Dispatch0),
    State2A = record_driven_peers(State2, Dispatch1),
    {State3, Dispatch2} = activate_tasks(State2A, Dispatch1),
    Dispatch2A = Dispatch2#dispatch{ stores = ar_sync_store:refresh_work(
        Dispatch2#dispatch.footprints, Dispatch2#dispatch.stores) },
    {State4, Dispatch3} = bind_tasks(State3, Dispatch2A),
    State4A = record_driven_peers(State4, Dispatch3),
    State5 = start_tasks(State4A, Dispatch3),
    maybe_schedule_download_limit_wakeup(
        finish_dispatch(State5, Dispatch3)).

%% @doc Return whether starting another fetch would fill the chunk cache. The
%% projected size includes chunks currently held by in-flight fetch workers.
is_chunk_cache_full(#dispatch{ worker_count = WorkerCount }) ->
    ar_sync_deps:chunk_cache_size() + WorkerCount
        >= ar_sync_deps:chunk_cache_size_limit().

start_tasks(State, Dispatch) ->
    #dispatch{ tasks_to_start = TasksToStart } = Dispatch,
    {Started, Tasks2} = register_started(
        lists:reverse(TasksToStart), [], State#state.tasks),
    State2 = State#state{ tasks = Tasks2 },
    lists:foldl(
        fun(Task, StateAcc) ->
            {WorkerPID, MonitorRef} = spawn_worker(Task),
            register_worker(Task#task.task_ref, WorkerPID, MonitorRef,
                StateAcc)
        end,
        State2,
        Started).

finish_dispatch(State, Dispatch) ->
    #dispatch{
        stores = StoreDispatches,
        footprints = FootprintDispatch,
        peer_queues = PeerQueues
    } = Dispatch,
    State#state{
        stores = ar_sync_store:finish_dispatch(StoreDispatches),
        footprints = ar_sync_footprint:finish_dispatch(FootprintDispatch),
        peer_queues = PeerQueues
    }.

%% @doc Arm the limiter's short retry when queued work is held only by the
%% download-rate limit.
maybe_schedule_download_limit_wakeup(State) ->
    #state{
        stores = StoreStates,
        footprints = Footprints,
        peer_queues = PeerQueues,
        download_limit = DownloadLimit
    } = State,
    HasWork = not ar_sync_store:queues_empty(StoreStates)
        orelse ar_sync_footprint:has_bound_work(Footprints)
        orelse not peer_queues_empty(PeerQueues),
    DownloadLimit2 = ar_sync_download_limit:maybe_schedule_wakeup(
        DownloadLimit, HasWork, tick_interval_ms()),
    State#state{ download_limit = DownloadLimit2 }.

%% @doc Record a started worker under its spawn monitor.
register_worker(TaskRef, WorkerPID, MonitorRef, State) ->
    State#state{ monitor_index =
        maps:put(MonitorRef, {TaskRef, WorkerPID}, State#state.monitor_index) }.

spawn_worker(Task) ->
    spawn_monitor(ar_sync_fetch_worker, run, [Task]).

%% @doc Build a fresh dispatch snapshot from the current server state.
%% The peer dispatch snapshots scheduler caps and accumulates fetches selected
%% during this pass. Store snapshots and footprint state are also
%% updated as tasks are selected.
start_dispatch(State) ->
    BoundTasks = peer_queued_tasks(State#state.peer_queues),
    Dispatch0 = #dispatch{
            stores = ar_sync_store:start_dispatch(
                State#state.footprints, State#state.tasks,
                BoundTasks, State#state.stores),
            worker_count = map_size(State#state.monitor_index),
            footprints = ar_sync_footprint:start_dispatch(State#state.footprints),
            peer_queues = State#state.peer_queues,
            peers = ar_sync_peer:start_dispatch(
                State#state.tasks, BoundTasks,
                State#state.peer_state) },
    StoresByPeer = ar_sync_store:stores_by_peer(Dispatch0#dispatch.stores),
    PeerDispatches = ar_sync_peer:set_store_task_targets(
        StoresByPeer, Dispatch0#dispatch.peers),
    Dispatch0#dispatch{ peers = PeerDispatches }.

%% @doc Fill peer queues from the least-loaded eligible stores. Concrete tasks
%% bind to one source; footprint reservations materialize one finite bound
%% batch. Neither operation starts network work or consumes download tokens.
bind_tasks(State, Dispatch) ->
    maybe
        {ok, StoreID} ?= ar_sync_store:best_store(Dispatch#dispatch.stores),
        {QueuedWork, StoreDispatches} ?= ar_sync_store:pop_work(
            StoreID, Dispatch#dispatch.stores),
        Dispatch2 = Dispatch#dispatch{ stores = StoreDispatches },
        Work = resolve_work(QueuedWork, Dispatch2),
        {State2, Dispatch3} = dispatch_work(Work, State, Dispatch2),
        bind_tasks(State2, Dispatch3)
    else
        none -> {State, Dispatch}
    end.

resolve_work(#task{} = Task, _Dispatch) ->
    #work{
        item = Task,
        store_id = Task#task.store_id,
        sources = Task#task.sources
    };
resolve_work(#footprint{} = Footprint, Dispatch) ->
    FootprintDispatch = Dispatch#dispatch.footprints,
    case ar_sync_footprint:reservation(Footprint, FootprintDispatch) of
        none ->
            none;
        Reservation ->
            #work{
                item = Reservation,
                store_id = ar_sync_footprint:store_id(Reservation),
                sources = ar_sync_footprint:sources(Reservation)
            }
    end.

dispatch_work(none, State, Dispatch) ->
    {State, Dispatch};
dispatch_work(Work, State, Dispatch) ->
    case best_source(Work, Dispatch) of
        {ok, Source} ->
            dispatch_selected_work(Work, Source, State, Dispatch);
        none ->
            {State, Dispatch}
    end.

best_source(Work, Dispatch) ->
    #work{ item = Item, store_id = StoreID,
        sources = Sources } = Work,
    #dispatch{ footprints = FootprintDispatch,
        peers = PeerDispatches } = Dispatch,
    ReadySources = lists:filter(
        fun(#task_source{ peer = Peer } = Source) ->
            ar_sync_footprint:is_source_compatible(
                Item, Source, FootprintDispatch)
            andalso ar_sync_peer:has_capacity(Item, Source, PeerDispatches)
            andalso peer_has_store_capacity(Item, Peer, StoreID, Dispatch)
        end,
        Sources),
    ar_sync_peer:best_source(StoreID, ReadySources, PeerDispatches).

%% @doc Concrete tasks have already passed admission; only footprint
%% reservations need peer/store capacity before materializing child tasks.
peer_has_store_capacity(#task{}, _Peer, _StoreID, _Dispatch) ->
    true;
peer_has_store_capacity(#footprint_reservation{}, Peer, StoreID, Dispatch) ->
    ar_sync_peer:store_capacity(Peer, StoreID, Dispatch#dispatch.peers) > 0.

dispatch_selected_work(#work{ item = #task{} = Task },
        Source, State, Dispatch) ->
    dispatch_task(Task, Source, State, Dispatch);
dispatch_selected_work(#work{
        item = #footprint_reservation{} = Reservation },
        Source, State, Dispatch) ->
    dispatch_footprint_tasks(Reservation, Source, State, Dispatch).

dispatch_footprint_tasks(Reservation, Source, State, Dispatch) ->
    Footprint = ar_sync_footprint:key(Reservation),
    case ensure_entropy_capacity(Footprint, Source, Dispatch) of
        {deferred, Dispatch2} ->
            {State, Dispatch2};
        {ready, Dispatch2} ->
            {State, build_footprint_batch(Reservation, Source, Dispatch2)}
    end.

%% @doc Ensure entropy capacity before fetching from a footprint source.
%% Each peer/footprint pair needs cached entropy, so exceeding the cache would
%% repeatedly evict and regenerate entropy instead of sustaining chunk fetches.
ensure_entropy_capacity(Footprint, Source, Dispatch) ->
    FootprintDispatch = Dispatch#dispatch.footprints,
    case ar_sync_footprint:has_entropy_capacity(
            Footprint, Source, FootprintDispatch) of
        true ->
            {ready, Dispatch};
        false ->
            Peer = Source#task_source.peer,
            StoreID = Footprint#footprint.store_id,
            CandidatePriority = peer_priority(Peer, StoreID, Dispatch),
            BoundPriorities = bound_footprint_priorities(Dispatch),
            {Result, FootprintDispatch2} =
                ar_sync_footprint:compete_for_entropy_capacity(
                    Footprint, CandidatePriority,
                    BoundPriorities, FootprintDispatch),
            {Result, Dispatch#dispatch{ footprints = FootprintDispatch2 }}
    end.

bound_footprint_priorities(Dispatch) ->
    lists:map(
        fun({Footprint, Peer, StoreID}) ->
            Priority = peer_priority(Peer, StoreID, Dispatch),
            {Priority, Footprint}
        end,
        ar_sync_footprint:bound_candidates(Dispatch#dispatch.footprints)).

peer_priority(Peer, StoreID, Dispatch) ->
    Runnable = ar_sync_store:has_capacity(StoreID, Dispatch#dispatch.stores)
        andalso ar_sync_peer:store_capacity(
            Peer, StoreID, Dispatch#dispatch.peers) > 0,
    {AvailabilityRank, PeerLoad, NegativeCap} = ar_sync_peer:priority(
        Peer, Runnable, Dispatch#dispatch.peers),
    StoreFootprintCount = ar_sync_footprint:bound_count(
        StoreID, Dispatch#dispatch.footprints),
    {StoreFootprintCount, AvailabilityRank, PeerLoad, NegativeCap}.

build_footprint_batch(Reservation, Source, Dispatch) ->
    StoreID = ar_sync_footprint:store_id(Reservation),
    #task_source{ peer = Peer, intervals = Intervals } = Source,
    Limit = ar_sync_peer:store_capacity(
        Peer, StoreID, Dispatch#dispatch.peers),
    AvailableIntervals = ar_sync_store:unclaimed_intervals(
        StoreID, Intervals, Dispatch#dispatch.stores),
    {FootprintDispatch2, Tasks, BoundReservation} =
        ar_sync_footprint:build_batch(
            Reservation, Source, AvailableIntervals,
            Limit, Dispatch#dispatch.footprints),
    StoreDispatches2 = case Reservation#footprint_reservation.state of
        queued ->
            ar_sync_store:bind_footprint(
                Reservation, Dispatch#dispatch.stores);
        bound ->
            Dispatch#dispatch.stores
    end,
    BoundTasks = [Task#task{ peer = Peer } || Task <- Tasks],
    StoreDispatches3 = ar_sync_store:enqueue_bound_tasks(
        BoundTasks, StoreDispatches2),
    StoreDispatches4 = ar_sync_store:add_reservations(
        [BoundReservation], StoreDispatches3),
    PeerDispatches = ar_sync_peer:enqueue_tasks(
        Peer, BoundTasks, Dispatch#dispatch.peers),
    PeerQueues = enqueue_peer_tasks(
        Peer, BoundTasks, Dispatch#dispatch.peer_queues),
    Dispatch#dispatch{ footprints = FootprintDispatch2,
        stores = StoreDispatches4, peers = PeerDispatches,
        peer_queues = PeerQueues }.

%% @doc Commit a selected task to its peer queue without starting its fetch.
dispatch_task(Task0, Source, State, Dispatch) ->
    #task_source{ peer = Peer, footprint = Footprint } = Source,
    Task = Task0#task{ peer = Peer, footprint = Footprint },
    StoreDispatches = ar_sync_store:bind_task(
        Task0, Dispatch#dispatch.stores),
    PeerDispatches = ar_sync_peer:enqueue_tasks(
        Peer, [Task], Dispatch#dispatch.peers),
    PeerQueues = enqueue_peer_tasks(
        Peer, [Task], Dispatch#dispatch.peer_queues),
    Dispatch2 = Dispatch#dispatch{
        stores = StoreDispatches,
        peers = PeerDispatches,
        peer_queues = PeerQueues },
    {State, Dispatch2}.

%% @doc Start peer-bound tasks while peer caps and shared resource gates allow.
activate_tasks(State, Dispatch) ->
    #state{ download_limit = DownloadLimit } = State,
    maybe
        false ?= is_chunk_cache_full(Dispatch),
        true ?= ar_sync_download_limit:has_capacity(DownloadLimit),
        {Task, Dispatch2} ?= pop_startable_peer_task(Dispatch),
        {State2, Dispatch3} = activate_task(Task, State, Dispatch2),
        activate_tasks(State2, Dispatch3)
    else
        _ -> {State, Dispatch}
    end.

activate_task(#task{ peer = Peer } = Task, State, Dispatch) ->
    #dispatch{
        worker_count = WorkerCount,
        tasks_to_start = TasksToStart
    } = Dispatch,
    StoreDispatches = ar_sync_store:start_bound_task(
        Task, Dispatch#dispatch.stores),
    PeerDispatches = ar_sync_peer:start_task(
        Peer, Task#task{ state = fetching }, Dispatch#dispatch.peers),
    Dispatch2 = Dispatch#dispatch{
        stores = StoreDispatches,
        peers = PeerDispatches,
        worker_count = WorkerCount + 1,
        tasks_to_start = [Task | TasksToStart]
    },
    State2 = State#state{ download_limit =
        ar_sync_download_limit:consume(
            State#state.download_limit, ?DATA_CHUNK_SIZE) },
    {State2, Dispatch2}.

pop_startable_peer_task(#dispatch{
        peer_queues = PeerQueues,
        stores = StoreDispatches,
        peers = PeerDispatches
    } = Dispatch) ->
    Candidates = maps:fold(
        fun(Peer, Queue, Acc) ->
            case ar_sync_peer:has_capacity(Peer, PeerDispatches) of
                false ->
                    Acc;
                true ->
                    case take_startable_task(Queue, StoreDispatches) of
                        none ->
                            Acc;
                        {Task, Queue2} ->
                            Priority = {
                                ar_sync_peer:fetching_count(
                                    Peer, PeerDispatches),
                                Peer
                            },
                            [{Priority, Peer, Task, Queue2} | Acc]
                    end
            end
        end,
        [],
        PeerQueues),
    case Candidates of
        [] ->
            none;
        _ ->
            {_Priority, Peer, Task, Queue2} = lists:min(Candidates),
            PeerQueues2 = put_peer_queue(Peer, Queue2, PeerQueues),
            {Task, Dispatch#dispatch{ peer_queues = PeerQueues2 }}
    end.

take_startable_task(Queue, StoreDispatches) ->
    Tasks = queue:to_list(Queue),
    case best_startable_task(Tasks, StoreDispatches, 0, none) of
        none ->
            none;
        {_Priority, Index} ->
            {Task, Remaining} = take_list_task(Index, Tasks, []),
            {Task, queue:from_list(Remaining)}
    end.

best_startable_task([], _StoreDispatches, _Index, Best) ->
    Best;
best_startable_task([#task{ store_id = StoreID } | Rest],
        StoreDispatches, Index, Best) ->
    Best2 = case ar_sync_store:can_start_bound_task(
            StoreID, StoreDispatches) of
        false ->
            Best;
        true ->
            Priority = {ar_sync_store:fetching_count(
                StoreID, StoreDispatches), Index},
            case Best of
                none -> {Priority, Index};
                {BestPriority, _BestIndex} when Priority < BestPriority ->
                    {Priority, Index};
                _ -> Best
            end
    end,
    best_startable_task(Rest, StoreDispatches, Index + 1, Best2).

take_list_task(0, [Task | Rest], Prefix) ->
    {Task, lists:reverse(Prefix, Rest)};
take_list_task(Index, [Task | Rest], Prefix) ->
    take_list_task(Index - 1, Rest, [Task | Prefix]).

enqueue_peer_tasks(Peer, Tasks, PeerQueues) ->
    Queue = maps:get(Peer, PeerQueues, queue:new()),
    Queue2 = lists:foldl(
        fun(Task, Acc) -> queue:in(Task, Acc) end,
        Queue,
        Tasks),
    maps:put(Peer, Queue2, PeerQueues).

put_peer_queue(Peer, Queue, PeerQueues) ->
    case queue:is_empty(Queue) of
        true -> maps:remove(Peer, PeerQueues);
        false -> maps:put(Peer, Queue, PeerQueues)
    end.

peer_queued_tasks(PeerQueues) ->
    maps:fold(
        fun(_Peer, Queue, Acc) -> queue:to_list(Queue) ++ Acc end,
        [],
        PeerQueues).

peer_queue_counts(PeerQueues) ->
    maps:map(fun(_Peer, Queue) -> queue:len(Queue) end, PeerQueues).

peer_queues_empty(PeerQueues) ->
    maps:size(PeerQueues) =:= 0.

%% @doc Latch peers whose runnable queues are held behind full active caps while
%% shared download and cache gates are open. Sampling every dispatch avoids
%% classifying completion waves by their phase at the scheduler tick.
record_driven_peers(State, Dispatch) ->
    SharedCapacity = ar_sync_download_limit:has_capacity(
        State#state.download_limit)
        andalso not is_chunk_cache_full(Dispatch),
    case SharedCapacity of
        false ->
            State;
        true ->
            DrivenPeers = maps:fold(
                fun(Peer, Queue, Acc) ->
                    case not queue:is_empty(Queue)
                            andalso not ar_sync_peer:has_capacity(
                                Peer, Dispatch#dispatch.peers) of
                        true -> maps:put(Peer, true, Acc);
                        false -> Acc
                    end
                end,
                State#state.driven_peers,
                Dispatch#dispatch.peer_queues),
            State#state{ driven_peers = DrivenPeers }
    end.

%% @doc Register the bytes fetched by the worker and the network-attempt time
%% consumed by the task.
task_fetch_completed({Pid, _Ref} = TaskRef, BytesFetched, FetchTiming)
        when is_pid(Pid) ->
    catch gen_server:cast(Pid,
        {task_fetch_completed, TaskRef, BytesFetched, FetchTiming}),
    ok.

%% @doc Register that ar_data_sync finished processing one handed-off chunk.
task_write_completed(undefined) ->
    ok;
task_write_completed({Pid, _Ref} = TaskRef) when is_pid(Pid) ->
    catch gen_server:cast(Pid, {task_write_completed, TaskRef}),
    ok;
task_write_completed(TaskRef) ->
    catch gen_server:cast(?MODULE, {task_write_completed, TaskRef}),
    ok.

%% @doc Evolve peer control from the scheduler's active, inflight, and driven
%% views. ar_sync_peer owns delivery observations and cap publication.
tick(State, NowMs) ->
    State2 = sample_store_drain_rates(State, NowMs),
    #state{ tasks = Tasks,
        peer_state = PeerState, stores = StoreStates,
        footprints = Footprints,
        peer_queues = PeerQueues,
        driven_peers = DrivenPeers } = State2,
    InflightCounts = inflight_counts(Tasks),
    arweave_metrics:gauge_set(sync_scheduler_driven_peers,
        map_size(DrivenPeers)),
    Peers = lists:usort(
        active_peers(StoreStates, Tasks, Footprints, PeerQueues)
            ++ maps:keys(DrivenPeers)),
    PeerState2 = ar_sync_peer:tick(
        Peers, InflightCounts, DrivenPeers, NowMs, PeerState),
    State2#state{ peer_state = PeerState2, driven_peers = #{} }.

%% @doc Sample each store's drain capacity. ar_sync_store owns the actual
%% chunk-cache backlog signal, sampling window, and capacity estimate.
sample_store_drain_rates(State, NowMs) ->
    State#state{ stores = ar_sync_store:sample_drain_rates(
        NowMs, State#state.stores) }.

%% @doc Fetching-task count per peer. Local writes remain in the per-store
%% backlog but no longer consume network concurrency after their fetch completes.
inflight_counts(Tasks) ->
    maps:fold(
        fun(_TaskRef, #task{ state = fetching, peer = Peer }, Acc) ->
                arweave_util:increment_map_value(Peer, Acc);
            (_TaskRef, _Task, Acc) ->
                Acc
        end, #{}, Tasks).

register_started([], Started, Tasks) ->
    {lists:reverse(Started), Tasks};
register_started([Task | Rest], Started, Tasks) ->
    TaskRef = {self(), make_ref()},
    Task2 = Task#task{ task_ref = TaskRef, state = fetching },
    register_started(Rest, [Task2 | Started],
        maps:put(TaskRef, Task2, Tasks)).

%% @doc Remove the worker monitor. A task still in `fetching` did not publish a
%% terminal result, so release it; tasks in a storage state remain until their
%% write completes.
worker_exited(MonitorRef, Reason, State) ->
    case maps:take(MonitorRef, State#state.monitor_index) of
        error ->
            State;
        {{TaskRef, _WorkerPID}, MonitorIndex2} ->
            State2 = State#state{ monitor_index = MonitorIndex2 },
            case maps:get(TaskRef, State2#state.tasks, not_found) of
                #task{} = Task ->
                    log_if_crash(Task, Reason),
                    finish_task(Task, State2);
                not_found ->
                    State2
            end
    end.

on_task_fetch_completed(TaskRef, BytesFetched, FetchTiming, State) ->
    #state{ tasks = Tasks } = State,
    case maps:get(TaskRef, Tasks, not_found) of
        #task{ state = writing } ->
            %% A successful fetch result has already been accounted. Ignore a
            %% duplicate completion rather than crediting it twice.
            State;
        #task{} = Task ->
            %% Refund the download-rate bucket for chunks the task was
            %% debited for but did not deliver (404s, 429s, timeouts):
            %% the budget prices bytes fetched, and a failed attempt
            %% carries zero bytes — without the refund, goodput under the limit
            %% would be rate x the success ratio instead of the rate.
            Refund = ?DATA_CHUNK_SIZE - BytesFetched,
            PeerState2 = ar_sync_peer:record_result(
                Task#task.peer, BytesFetched, FetchTiming, State#state.peer_state),
            State2 = State#state{ peer_state = PeerState2 },
            State3 = State2#state{ download_limit =
                ar_sync_download_limit:restore(
                    State2#state.download_limit, Refund) },
            case {BytesFetched, Task#task.state} of
                {0, _} ->
                    finish_task(Task, State3);
                {_, fetching} ->
                    %% Entropy is no longer needed after the fetch worker hands
                    %% off the chunk. Keep the exact store claim through the
                    %% asynchronous write, but release its footprint slot now.
                    Footprints2 = ar_sync_footprint:task_completed(
                        Task, State3#state.footprints),
                    put_task(Task#task{ state = writing, footprint = none },
                        State3#state{ footprints = Footprints2 });
                {_, write_complete} ->
                    finish_task(Task, State3);
                {_, writing} ->
                    State3
            end;
        not_found ->
            State
    end.

on_task_write_completed(TaskRef, State) ->
    #state{ tasks = Tasks } = State,
    case maps:get(TaskRef, Tasks, not_found) of
        #task{ state = fetching } = Task ->
            State2 = record_store_drain(Task, State),
            put_task(Task#task{ state = write_complete }, State2);
        #task{ state = writing } = Task ->
            finish_task(Task#task{ state = write_complete },
                record_store_drain(Task, State));
        #task{ state = write_complete } ->
            State;
        not_found ->
            State
    end.

record_store_drain(#task{ store_id = StoreID }, State) ->
    State#state{ stores = ar_sync_store:record_write_completed(
        StoreID, State#state.stores) }.

put_task(Task, State) ->
    #task{ task_ref = TaskRef } = Task,
    #state{ tasks = Tasks } = State,
    State#state{ tasks = maps:put(TaskRef, Task, Tasks) }.

finish_task(#task{ state = writing }, State) ->
    State;
finish_task(Task, State) ->
    #task{ task_ref = TaskRef } = Task,
    #state{
        tasks = Tasks,
        footprints = Footprints
    } = State,
    Footprints2 = ar_sync_footprint:task_completed(Task, Footprints),
    State2 = release_claim(Task#task.store_id, Task#task.offset, State),
    State2#state{ tasks = maps:remove(TaskRef, Tasks),
        footprints = Footprints2 }.

release_claim(StoreID, Offset, State) ->
    State#state{ stores = ar_sync_store:release_claim(
        StoreID, Offset, State#state.stores) }.

%%%===================================================================
%%% Helpers.
%%%===================================================================

log_if_crash(_Task, normal) ->
    ok;
log_if_crash(#task{ peer = Peer } = Task, Reason) ->
    ?LOG_WARNING([{event, sync_worker_crash}, {module, ?MODULE},
        %% A task carries no peer until dispatch binds one, and a crash can
        %% reach here before that.
        {peer, format_task_peer(Peer)},
        {start_offset, Task#task.offset},
        {end_offset, Task#task.offset + ?DATA_CHUNK_SIZE},
        {reason, io_lib:format("~p", [Reason])}]).

format_task_peer(undefined) ->
    unbound;
format_task_peer(Peer) ->
    arweave_util:format_peer(Peer).

%% @doc The control-tick cadence; every internal reader goes through this
%% function so the whole control loop scales together. Test builds may
%% shrink it at runtime — a persistent_term, not a mock, because the
%% cadence is read on hot fetch paths and meck serializes every call
%% through the meck_proc server.
-ifdef(AR_TEST).
tick_interval_ms() ->
    persistent_term:get({?MODULE, tick_interval_ms}, ?TICK_INTERVAL_MS).
override_tick_interval_ms(Ms) ->
    persistent_term:put({?MODULE, tick_interval_ms}, Ms).
reset_all_overrides() ->
    persistent_term:erase({?MODULE, tick_interval_ms}),
    ok.
-else.
tick_interval_ms() -> ?TICK_INTERVAL_MS.
-endif.

%%%===================================================================
%%% Metrics.
%%%===================================================================

%% @doc Emit scheduler-owned peer, store, task, and footprint gauges. Called
%% on the scheduler tick (~10s) so the cost of enumerating state is bounded. The
%% task gauges expose queued, fetching, and writing stages where each dimension
%% applies; sync_total_inflight separately counts live worker monitors. These are
%% distinct from sync_claimed_bytes_by_store, which can be large while zero
%% workers run for a starved store. All store_ids are emitted so a starved store
%% reports an explicit 0 rather than a missing series; peer series are removed
%% when the peer drops out.
emit_sync_metrics(State) ->
    #state{
        tasks = Tasks,
        monitor_index = MonitorIndex,
        stores = StoreStates,
        footprints = Footprints,
        peer_queues = PeerQueues
    } = State,
    StoreIDs = ar_sync_store_sweeper:store_ids(),
    {TasksByStore, TasksByPeer} = task_counts_by_stage(Tasks),
    QueuedByPeer = peer_queue_counts(PeerQueues),
    lists:foreach(
        fun(StoreID) ->
            emit_claim_metrics(StoreID, State),
            arweave_metrics:gauge_set(sync_tasks_by_store,
                [queued, StoreID], queued_task_count_by_store(StoreID, State)),
            arweave_metrics:gauge_set(sync_tasks_by_store,
                [fetching, StoreID],
                maps:get({fetching, StoreID}, TasksByStore, 0)),
            arweave_metrics:gauge_set(sync_tasks_by_store,
                [writing, StoreID],
                maps:get({writing, StoreID}, TasksByStore, 0))
        end,
        StoreIDs),
    emit_store_capacity_metrics(StoreIDs, State),
    ar_sync_footprint:emit_metrics(Footprints),
    ClaimedByPeerStore = claimed_chunks_by_peer_store(Tasks, PeerQueues),
    emit_claim_ownership_metrics(ClaimedByPeerStore, StoreIDs),
    Peers = active_peers(StoreStates, Tasks, Footprints, PeerQueues),
    CurLabels = lists:flatmap(
        fun(Peer) ->
            Label = arweave_util:format_peer(Peer),
            QueuedLabels = [queued, Label],
            FetchingLabels = [fetching, Label],
            WritingLabels = [writing, Label],
            arweave_metrics:gauge_set(sync_tasks_by_peer, QueuedLabels,
                maps:get(Peer, QueuedByPeer, 0)),
            arweave_metrics:gauge_set(sync_tasks_by_peer, FetchingLabels,
                maps:get({fetching, Peer}, TasksByPeer, 0)),
            arweave_metrics:gauge_set(sync_tasks_by_peer, WritingLabels,
                maps:get({writing, Peer}, TasksByPeer, 0)),
            [QueuedLabels, FetchingLabels, WritingLabels]
        end,
        Peers),
    arweave_metrics:gauge_set(sync_active_peers, length(Peers)),
    arweave_metrics:gauge_set(sync_total_inflight, map_size(MonitorIndex)),
    %% Drop stage/peer series for peers that have dropped out. The previously
    %% emitted label set is read back from Prometheus rather than tracked in state.
    prune_stale_labels(sync_tasks_by_peer, CurLabels),
    ok.

emit_store_capacity_metrics(StoreIDs, State) ->
    StoreStates = State#state.stores,
    lists:foreach(
        fun(StoreID) ->
            arweave_metrics:gauge_set(sync_store_pipeline_limit_chunks,
                [StoreID], ar_sync_store:pipeline_limit(StoreID, StoreStates)),
            case ar_sync_store:drain_rate(StoreID, StoreStates) of
                undefined ->
                    ok;
                ChunksPerSecond ->
                    arweave_metrics:gauge_set(
                        store_drain_rate_bytes_per_second, [StoreID],
                        ChunksPerSecond * ?DATA_CHUNK_SIZE)
            end
        end,
        StoreIDs).

emit_claim_metrics(StoreID, State) ->
    StoreStates = State#state.stores,
    Label = ar_storage_module:label(StoreID),
    arweave_metrics:gauge_set(sync_claimed_bytes_by_store, [Label],
        ar_sync_store:claimed_chunks(StoreID, StoreStates)
            * ?DATA_CHUNK_SIZE),
    arweave_metrics:gauge_set(sync_claim_headroom_bytes_by_store, [Label],
        ar_sync_store:admission_headroom(StoreID, StoreStates)
            * ?DATA_CHUNK_SIZE).

queued_task_count_by_store(StoreID, State) ->
    ar_sync_store:queued_task_count(StoreID, State#state.stores).

task_counts_by_stage(Tasks) ->
    maps:fold(
        fun(_TaskRef, #task{ state = Stage, peer = Peer,
            store_id = StoreID }, {ByStore, ByPeer})
                when Stage =:= fetching; Stage =:= writing ->
                {arweave_util:increment_map_value({Stage, StoreID}, ByStore),
                    arweave_util:increment_map_value({Stage, Peer}, ByPeer)};
            (_TaskRef, _Task, Counts) ->
                Counts
        end,
        {#{}, #{}},
        Tasks).

%% Include both runnable peer-queue claims and nonterminal active tasks.
claimed_chunks_by_peer_store(Tasks, PeerQueues) ->
    Active = maps:fold(
        fun(_TaskRef, Task, Acc) ->
            Key = {Task#task.peer, Task#task.store_id},
            arweave_util:increment_map_value(Key, Acc)
        end,
        #{},
        Tasks),
    lists:foldl(
        fun(Task, Acc) ->
            Key = {Task#task.peer, Task#task.store_id},
            arweave_util:increment_map_value(Key, Acc)
        end,
        Active,
        peer_queued_tasks(PeerQueues)).

%% @doc Distinct peers across queued, nonterminal, and bound footprint work.
%% This is the active-peer set used to allocate caps and emit per-peer metrics.
active_peers(StoreStates, Tasks, Footprints, PeerQueues) ->
    FromQueues = ar_sync_store:peers(StoreStates),
    FromTasks = [Task#task.peer || Task <- maps:values(Tasks)],
    FromFootprints = ar_sync_footprint:peers(Footprints),
    lists:usort(FromQueues ++ FromTasks ++ FromFootprints
        ++ maps:keys(PeerQueues)).

emit_claim_ownership_metrics(ClaimedByPeerStore, StoreIDs) ->
    PeersByStore = maps:fold(
        fun({Peer, StoreID}, _Chunks, Acc) ->
            maps:update_with(StoreID,
                fun(Peers) -> sets:add_element(Peer, Peers) end,
                sets:from_list([Peer]), Acc)
        end,
        #{},
        ClaimedByPeerStore),
    lists:foreach(
        fun(StoreID) ->
            arweave_metrics:gauge_set(sync_claimed_peers_by_store,
                [StoreID], sets:size(maps:get(StoreID, PeersByStore, sets:new())))
        end,
        StoreIDs),
    CurLabels = maps:fold(
        fun({Peer, StoreID}, Chunks, Acc) ->
            Labels = [arweave_util:format_peer(Peer),
                ar_storage_module:label(StoreID)],
            arweave_metrics:gauge_set(sync_claimed_chunks_by_peer_store,
                Labels, Chunks),
            [Labels | Acc]
        end,
        [],
        ClaimedByPeerStore),
    prune_stale_labels(sync_claimed_chunks_by_peer_store, CurLabels).

prune_stale_labels(Name, CurLabels) ->
    Existing = lists:map(
        fun({Labels, _MetricValue}) ->
            lists:map(
                fun({_LabelName, Value}) -> Value end,
                Labels)
        end,
        arweave_metrics:gauge_values(Name)),
    lists:foreach(
        fun(Labels) -> arweave_metrics:gauge_remove(Name, Labels) end,
        Existing -- CurLabels).


%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

%% Focused selection tests exercise binding and activation without spawning
%% workers; production registers and spawns Dispatch#dispatch.tasks_to_start.
dispatch_tasks(State, Dispatch) ->
    {State2, Dispatch2} = bind_tasks(State, Dispatch),
    activate_tasks(State2, Dispatch2).

%% @doc The number of running fetch workers.
inflight_count() ->
    gen_server:call(?MODULE, inflight_count).

terminate_workers_test() ->
    WorkerPID = spawn(fun() -> receive stop -> ok end end),
    MonitorRef = erlang:monitor(process, WorkerPID),
    terminate_workers(#{ MonitorRef => {make_ref(), WorkerPID} }),
    ?assertNot(is_process_alive(WorkerPID)).

local_writes_do_not_consume_peer_capacity_test() ->
    Peer = peer1,
    StoreID = store1,
    FetchingRef = make_ref(),
    WritingRef = make_ref(),
    Tasks = #{
        FetchingRef => #task{ state = fetching, peer = Peer,
            store_id = StoreID },
        WritingRef => #task{ state = writing, peer = Peer,
            store_id = StoreID }
    },
    StoreDispatches = ar_sync_store:start_dispatch(
        ar_sync_footprint:new(), Tasks, [], ar_sync_store:new()),
    PeerDispatches0 = ar_sync_peer:start_dispatch(
        Tasks, [], ar_sync_peer:new()),
    PeerDispatches = ar_sync_peer:set_store_task_targets(
        Peer, [StoreID], PeerDispatches0),
    Dispatch = #dispatch{ stores = StoreDispatches, peers = PeerDispatches },
    %% One task exists in each local stage, but only the fetching task occupies
    %% the peer's single network slot. The default peer has one active slot and
    %% an eight-task queued-work bootstrap, so one of nine assignments is used.
    ?assertEqual(1 / 9,
        ar_sync_peer:load(Peer, Dispatch#dispatch.peers)),
    ?assertNot(ar_sync_peer:has_capacity(Peer, Dispatch#dispatch.peers)),
    %% Local writes are accounted only by the destination store.
    ?assertEqual(1 / 9,
        ar_sync_peer:store_load(Peer, StoreID, Dispatch#dispatch.peers)),
    ?assertEqual(#{Peer => 1}, inflight_counts(Tasks)).

startable_task_prefers_less_active_store_test_() ->
    ar_test_util:with_mocked([
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ], fun test_startable_task_prefers_less_active_store/0, 30).

test_startable_task_prefers_less_active_store() ->
    StoreA = store_a,
    StoreB = store_b,
    FetchingTasks = maps:from_list([
        {make_ref(), #task{ state = fetching, store_id = StoreA }},
        {make_ref(), #task{ state = fetching, store_id = StoreA }}
    ]),
    TaskA = #task{ offset = 0, store_id = StoreA },
    TaskB = #task{ offset = ?DATA_CHUNK_SIZE, store_id = StoreB },
    StoreDispatches = ar_sync_store:start_dispatch(
        ar_sync_footprint:new(), FetchingTasks,
        [TaskA, TaskB], ar_sync_store:new()),
    %% Store A has two active fetches and appears first in the peer queue;
    %% store B has none, so activation selects B without reordering A.
    {TaskB, Remaining} = take_startable_task(
        queue:from_list([TaskA, TaskB]), StoreDispatches),
    ?assertEqual([TaskA], queue:to_list(Remaining)).

peer_queue_stays_full_behind_active_cap_test_() ->
    ar_test_util:with_mocked([
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ], fun test_peer_queue_stays_full_behind_active_cap/0, 30).

test_peer_queue_stays_full_behind_active_cap() ->
    Peer = peer1,
    StoreID = store1,
    %% Ten candidates exceed the two active plus four queued tasks that this
    %% test dispatch may hold, leaving work in the unbound store queue.
    Tasks = [#task{ store_id = StoreID,
            sources = [#task_source{ peer = Peer }],
            offset = Index * ?DATA_CHUNK_SIZE }
        || Index <- lists:seq(0, 9)],
    PeerDispatches = ar_sync_peer:set_store_task_targets(
        Peer, [StoreID], ar_sync_peer:test_dispatch(
            #{Peer => 2}, #{Peer => 4})),
    Dispatch0 = (seed(Tasks))#dispatch{ peers = PeerDispatches },
    {State1, Dispatch1} = bind_tasks(#state{}, Dispatch0),
    ?assertEqual(4, queue:len(maps:get(
        Peer, Dispatch1#dispatch.peer_queues))),
    ?assertEqual([], Dispatch1#dispatch.tasks_to_start),
    {State2, Dispatch2} = activate_tasks(State1, Dispatch1),
    %% The active cap starts two tasks and temporarily leaves two queued.
    ?assertEqual(2, length(Dispatch2#dispatch.tasks_to_start)),
    ?assertEqual(2, queue:len(maps:get(
        Peer, Dispatch2#dispatch.peer_queues))),
    State2A = record_driven_peers(State2, Dispatch2),
    ?assertEqual(#{Peer => true}, State2A#state.driven_peers),
    Dispatch2A = Dispatch2#dispatch{ stores = ar_sync_store:refresh_work(
        Dispatch2#dispatch.footprints, Dispatch2#dispatch.stores) },
    {_State3, Dispatch3} = bind_tasks(State2A, Dispatch2A),
    %% Refilling the two vacated places restores the four-task peer queue.
    ?assertEqual(4, queue:len(maps:get(
        Peer, Dispatch3#dispatch.peer_queues))).

bound_footprint_owner_is_an_active_peer_test() ->
    Footprint = #footprint{ store_id = store1, partition = 0, footprint = 1 },
    Reservation = ar_sync_footprint:test_reservation(
        store1, Footprint, [], peer1, 0, bound),
    Footprints = ar_sync_footprint:test_state([Reservation]),
    ?assertEqual([peer1], active_peers(#{}, #{}, Footprints, #{})).

reservation_binding_claims_only_enqueued_tasks_test() ->
    Peer = {1, 1, 1, 1, 9},
    Footprint = #footprint{ store_id = store1, partition = 0, footprint = 1 },
    %% The selected source advertises sixteen chunks from the complete
    %% footprint reservation.
    SparseIntervals = ar_intervals:from_list([
        {16 * ?DATA_CHUNK_SIZE, 0}
    ]),
    Reservation = ar_sync_footprint:new_reservation(store1, Footprint,
        [#task_source{ peer = Peer, footprint = Footprint,
            intervals = SparseIntervals }]),
    Dispatch0 = (seed([Reservation]))#dispatch{
        peers = peer_dispatches_for_stores(
            #{Peer => 8}, #{Peer => [store1]})
    },
    StoreStates0 = ar_sync_store:finish_dispatch(Dispatch0#dispatch.stores),
    ?assertEqual(ar_sync_store:chunks_in_claim(Reservation),
        ar_sync_store:claimed_chunks(store1, StoreStates0)),
    {Footprint, StoreDispatches} = ar_sync_store:pop_work(
        store1, Dispatch0#dispatch.stores),
    DispatchA = Dispatch0#dispatch{ stores = StoreDispatches },
    Work = resolve_work(Footprint, DispatchA),
    {_State, Dispatch1} = dispatch_work(Work, #state{}, DispatchA),
    StoreStates1 = ar_sync_store:finish_dispatch(Dispatch1#dispatch.stores),
    %% Binding releases the whole-footprint reservation. The unmeasured peer's
    %% eight-task queue limit remains claimed; retained intervals are future
    %% work.
    ?assertEqual(8, ar_sync_store:claimed_chunks(store1, StoreStates1)),
    ?assertEqual(8, ar_sync_store:queued_task_count(store1, StoreStates1)),
    ?assertEqual(8, queue:len(maps:get(Peer,
        Dispatch1#dispatch.peer_queues))).

fragmented_partial_intervals_claim_each_request_test() ->
    Peer = {1, 1, 1, 1, 9},
    Footprint = #footprint{ store_id = store1, partition = 0, footprint = 1 },
    %% Two disjoint half-chunk advertisements still produce two /chunk2
    %% requests, one from the start of each interval.
    HalfChunk = ?DATA_CHUNK_SIZE div 2,
    PartialIntervals = ar_intervals:from_list([
        {HalfChunk, 0},
        {2 * ?DATA_CHUNK_SIZE + HalfChunk, 2 * ?DATA_CHUNK_SIZE}
    ]),
    Reservation = ar_sync_footprint:new_reservation(store1, Footprint,
        [#task_source{ peer = Peer, footprint = Footprint,
            intervals = PartialIntervals }]),
    Dispatch0 = (seed([Reservation]))#dispatch{
        peers = peer_dispatches_for_stores(
            #{Peer => 8}, #{Peer => [store1]})
    },
    {Footprint, StoreDispatches} = ar_sync_store:pop_work(
        store1, Dispatch0#dispatch.stores),
    DispatchA = Dispatch0#dispatch{ stores = StoreDispatches },
    Work = resolve_work(Footprint, DispatchA),
    {_State, Dispatch1} = dispatch_work(Work, #state{}, DispatchA),
    StoreStates1 = ar_sync_store:finish_dispatch(Dispatch1#dispatch.stores),
    ?assertEqual(2, ar_sync_store:claimed_chunks(store1, StoreStates1)),
    ?assertEqual(2, ar_sync_store:queued_task_count(store1, StoreStates1)),
    ?assertEqual(2, queue:len(maps:get(Peer,
        Dispatch1#dispatch.peer_queues))).

overlapping_partial_intervals_defer_duplicate_request_test() ->
    Peer = {1, 1, 1, 1, 9},
    Footprint = #footprint{ store_id = store1, partition = 0, footprint = 1 },
    QuarterChunk = ?DATA_CHUNK_SIZE div 4,
    %% Both advertised fragments fall inside the first fixed-size request span.
    %% One request is claimed now; the sweeper may rediscover any uncovered
    %% pre-strict-split data after that request completes.
    PartialIntervals = ar_intervals:from_list([
        {QuarterChunk, 0},
        {3 * QuarterChunk, 2 * QuarterChunk}
    ]),
    Reservation = ar_sync_footprint:new_reservation(store1, Footprint,
        [#task_source{ peer = Peer, footprint = Footprint,
            intervals = PartialIntervals }]),
    Dispatch0 = (seed([Reservation]))#dispatch{
        peers = peer_dispatches_for_stores(
            #{Peer => 8}, #{Peer => [store1]})
    },
    {Footprint, StoreDispatches} = ar_sync_store:pop_work(
        store1, Dispatch0#dispatch.stores),
    DispatchA = Dispatch0#dispatch{ stores = StoreDispatches },
    Work = resolve_work(Footprint, DispatchA),
    {_State, Dispatch1} = dispatch_work(Work, #state{}, DispatchA),
    StoreStates1 = ar_sync_store:finish_dispatch(Dispatch1#dispatch.stores),
    ?assertEqual(1, ar_sync_store:claimed_chunks(store1, StoreStates1)),
    ?assertEqual(1, ar_sync_store:queued_task_count(store1, StoreStates1)),
    [Child] = queue:to_list(maps:get(Peer,
        Dispatch1#dispatch.peer_queues)),
    ?assertEqual(0, Child#task.offset).

%% Dispatch selection is pure (dispatch_tasks/2 produces a start list with no
%% side effects), so store balancing, footprint limits, and per-peer-cap invariants
%% are tested directly, without the gen_server or real workers.
decision_test_() ->
    ar_test_util:with_mocked([
        {ar_data_sync, is_disk_space_sufficient,
            fun(no_disk) -> false; (_) -> true end}
    ], fun() ->
        test_footprint_budget(),
        test_footprint_piggyback(),
        test_bound_footprint_refills_through_dispatch(),
        test_draining_footprint_tasks_finish_dispatching(),
        test_peer_store_queue_limit_bounds_footprint_bindings(),
        test_peer_store_queue_limit_is_work_conserving(),
        test_byte_source_bypasses_full_footprint_pool(),
        test_bound_footprint_keeps_its_peer(),
        test_per_peer_concurrency_cap(),
        test_store_balance(),
        test_disk_gate(),
        test_head_of_line_blocked_peers()
    end, 30).

%% Under a byte budget, different peer cap classes remain independently
%% eligible: the budget limits total tasks, not a latency-weighted slot pool.
mixed_peer_classes_rate_limited_test_() ->
    ar_test_util:with_mocked([
        %% Three rounds across four peer classes permit exactly 12 chunks.
        {arweave_config, get,
            fun([sync, max_download_rate]) -> 12 * ?DATA_CHUNK_SIZE;
                (Key) -> meck:passthrough([Key]) end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ], fun test_mixed_peer_classes_rate_limited/0, 30).

test_mixed_peer_classes_rate_limited() ->
    Fast = {1, 1, 1, 1, 9},
    Slow = {2, 2, 2, 2, 9},
    Limited = {3, 3, 3, 3, 9},
    Flaky = {4, 4, 4, 4, 9},
    Peers = [Fast, Slow, Limited, Flaky],
    %% Three rounds across four classes make a 12-chunk byte budget. A
    %% fourth queued round proves selection stops on tokens, not empty queues.
    Rounds = 3,
    ClassCount = length(Peers),
    QueuedRounds = Rounds + 1,
    IndexedPeers = lists:zip(Peers, lists:seq(0, ClassCount - 1)),
    Tasks = lists:flatmap(
        fun(Round) ->
            [#task{ store_id = store1,
                    sources = [#task_source{ peer = Peer }],
                    offset = (Round * ClassCount + Index) * ?DATA_CHUNK_SIZE }
                || {Peer, Index} <- IndexedPeers]
        end,
        lists:seq(0, QueuedRounds - 1)),
    BudgetChunks = Rounds * ClassCount,
    DownloadLimit = ar_sync_download_limit:refill(ar_sync_download_limit:new()),
    Dispatch0 = (seed(Tasks))#dispatch{
        %% Caps model deep fast, exact slow/flaky, and one-slot-headroom
        %% limited pipelines; every class can serve all three budget rounds.
        peers = ar_sync_peer:test_dispatch(#{Fast => 2 * Rounds, Slow => Rounds,
            Limited => Rounds + 1, Flaky => Rounds}) },
    {SelectedState, Dispatch} = dispatch_tasks(
        #state{ download_limit = DownloadLimit }, Dispatch0),
    StartedByPeer = lists:foldl(
        fun(#task{ peer = Peer }, Acc) ->
            arweave_util:increment_map_value(Peer, Acc)
        end, #{}, Dispatch#dispatch.tasks_to_start),
    ?assertEqual(BudgetChunks, length(Dispatch#dispatch.tasks_to_start)),
    ?assertNot(ar_sync_download_limit:has_capacity(
        SelectedState#state.download_limit)),
    ?assertEqual(maps:from_list([{Peer, Rounds}
        || Peer <- Peers]), StartedByPeer).

%% Unfetched bytes are restored through the actual task-completion transition.
bandwidth_cap_with_failures_test_() ->
    %% Two chunks/s leaves room to observe full, partial, and zero refunds.
    Rate = 2 * ?DATA_CHUNK_SIZE,
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, max_download_rate]) -> Rate;
                (Key) -> meck:passthrough([Key]) end}
    ], fun test_bandwidth_cap_with_failures/0, 30).

test_bandwidth_cap_with_failures() ->
    Peer = {5, 5, 5, 5, 9},
    FailedRef = make_ref(),
    PartialRef = make_ref(),
    FullRef = make_ref(),
    FailedTask = #task{ task_ref = FailedRef, peer = Peer,
        store_id = store1, footprint = none,
        offset = 0 },
    PartialTask = #task{ task_ref = PartialRef, peer = Peer,
        store_id = store1, footprint = none,
        offset = ?DATA_CHUNK_SIZE },
    FullTask = #task{ task_ref = FullRef, peer = Peer,
        store_id = store1, footprint = none,
        offset = 2 * ?DATA_CHUNK_SIZE },
    FullRate = ar_sync_download_limit:refill(ar_sync_download_limit:new()),
    ExhaustedRate = ar_sync_download_limit:consume(
        FullRate, 2 * ?DATA_CHUNK_SIZE),
    State0 = #state{ download_limit = ExhaustedRate,
        tasks = #{
            FailedRef => FailedTask,
            PartialRef => PartialTask,
            FullRef => FullTask
        } },
    FetchTiming = #fetch_timing{
        productive_ms = 250,
        timeout_ms = 500
    },
    State1 = on_task_fetch_completed(FailedRef, 0, FetchTiming, State0),
    ?assert(ar_sync_download_limit:has_capacity(State1#state.download_limit)),
    State1Exhausted = State1#state{ download_limit =
        ar_sync_download_limit:consume(
            State1#state.download_limit, ?DATA_CHUNK_SIZE) },
    %% A half-sized legacy chunk restores the unfilled half of its reservation.
    State2 = on_task_fetch_completed(PartialRef,
        ?DATA_CHUNK_SIZE div 2, FetchTiming, State1Exhausted),
    ?assert(ar_sync_download_limit:has_capacity(State2#state.download_limit)),
    State2Exhausted = State2#state{ download_limit =
        ar_sync_download_limit:consume(
            State2#state.download_limit, ?DATA_CHUNK_SIZE div 2) },
    State3 = on_task_fetch_completed(
        FullRef, ?DATA_CHUNK_SIZE, FetchTiming, State2Exhausted),
    ?assertNot(ar_sync_download_limit:has_capacity(
        State3#state.download_limit)).

task_lifecycle_test_() ->
    ar_test_util:with_mocked([
    ], fun test_task_lifecycle/0, 30).

test_task_lifecycle() ->
    FetchTiming = #fetch_timing{},
    WriteFirstRef = make_ref(),
    WriteFirstTask = #task{ store_id = store1,
        sources = [#task_source{ peer = peer1 }], offset = 0,
        task_ref = WriteFirstRef },
    WriteFirstState = #state{ tasks = #{ WriteFirstRef => WriteFirstTask } },
    WriteCompleted = on_task_write_completed(WriteFirstRef, WriteFirstState),
    ?assertEqual(write_complete,
        (maps:get(WriteFirstRef, WriteCompleted#state.tasks))#task.state),
    WriteFirstDone = on_task_fetch_completed(
        WriteFirstRef, ?DATA_CHUNK_SIZE, FetchTiming, WriteCompleted),
    ?assertNot(maps:is_key(WriteFirstRef, WriteFirstDone#state.tasks)),

    FetchFirstRef = make_ref(),
    FetchFirstTask = #task{ store_id = store1,
        sources = [#task_source{ peer = peer2 }], offset = ?DATA_CHUNK_SIZE,
        task_ref = FetchFirstRef },
    FetchFirstState = #state{ tasks = #{ FetchFirstRef => FetchFirstTask } },
    Writing = on_task_fetch_completed(
        FetchFirstRef, ?DATA_CHUNK_SIZE, FetchTiming, FetchFirstState),
    ?assertEqual(writing,
        (maps:get(FetchFirstRef, Writing#state.tasks))#task.state),
    FetchFirstDone = on_task_write_completed(FetchFirstRef, Writing),
    ?assertNot(maps:is_key(FetchFirstRef, FetchFirstDone#state.tasks)),

    FootprintRef = make_ref(),
    Footprint = #footprint{ store_id = store1, partition = 1, footprint = 1 },
    FootprintTask = #task{ store_id = store1,
        sources = [#task_source{ peer = peer3, footprint = Footprint }],
        peer = peer3, footprint = Footprint,
        offset = 2 * ?DATA_CHUNK_SIZE, task_ref = FootprintRef },
    BoundReservation = ar_sync_footprint:test_reservation(
        store1, Footprint,
        [#task_source{ peer = peer3, footprint = Footprint,
            intervals = ar_intervals:new() }],
        peer3, 1, bound),
    FootprintState = #state{
        tasks = #{FootprintRef => FootprintTask},
        footprints = ar_sync_footprint:test_state([BoundReservation])
    },
    FootprintWriting = on_task_fetch_completed(
        FootprintRef, ?DATA_CHUNK_SIZE, FetchTiming, FootprintState),
    ?assertEqual(0, ar_sync_footprint:bound_count(
        FootprintWriting#state.footprints)),
    ?assertEqual(none,
        (maps:get(FootprintRef, FootprintWriting#state.tasks))#task.footprint),
    FootprintDone = on_task_write_completed(FootprintRef, FootprintWriting),
    ?assertNot(maps:is_key(FootprintRef, FootprintDone#state.tasks)),
    ?assertEqual(0, ar_sync_footprint:bound_count(
        FootprintDone#state.footprints)),
    ?assert(ar_sync_footprint:is_empty(FootprintDone#state.footprints)),

    FailedRef = make_ref(),
    FailedTask = #task{ store_id = store1,
        sources = [#task_source{ peer = peer3 }],
        offset = 2 * ?DATA_CHUNK_SIZE,
        task_ref = FailedRef },
    FailedState = #state{ tasks = #{ FailedRef => FailedTask } },
    FailedDone = on_task_fetch_completed(
        FailedRef, 0, FetchTiming, FailedState),
    ?assertNot(maps:is_key(FailedRef, FailedDone#state.tasks)),

    DuplicateRef = make_ref(),
    DuplicateTask = #task{ store_id = store1,
        sources = [#task_source{ peer = peer4 }], offset = 0,
        task_ref = DuplicateRef },
    DuplicateState = #state{ tasks = #{ DuplicateRef => DuplicateTask } },
    Accounted = on_task_fetch_completed(
        DuplicateRef, ?DATA_CHUNK_SIZE, FetchTiming, DuplicateState),
    Duplicate = on_task_fetch_completed(
        DuplicateRef, ?DATA_CHUNK_SIZE, FetchTiming, Accounted),
    ?assertEqual(Accounted#state.peer_state, Duplicate#state.peer_state),

    CrashedRef = make_ref(),
    CrashedMonitorRef = make_ref(),
    CrashedTask = #task{ store_id = store1,
        sources = [#task_source{ peer = {5, 5, 5, 5, 9} }], offset = 0,
        task_ref = CrashedRef, state = write_complete },
    CrashedState = #state{
        tasks = #{ CrashedRef => CrashedTask },
        monitor_index = #{
            CrashedMonitorRef => {CrashedRef, self()}
        }
    },
    CrashedDone = worker_exited(
        CrashedMonitorRef, simulated_crash, CrashedState),
    ?assertNot(maps:is_key(CrashedRef, CrashedDone#state.tasks)),
    ?assertNot(maps:is_key(CrashedMonitorRef, CrashedDone#state.monitor_index)).

%% At two chunks/s, half a second refills exactly one task. A download-rate
%% wakeup must drive a real scheduler process before the coarser scheduler tick.
bandwidth_cap_trickle_test_() ->
    Rate = 2 * ?DATA_CHUNK_SIZE,
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([sync, max_download_rate]) -> Rate;
                %% One MiB is sufficient for init's positive footprint ceiling.
                ([packing, entropy, cache_size]) -> 1;
                (Key) -> meck:passthrough([Key]) end},
        {ar_data_sync, is_chunk_cache_full, fun() -> false end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {ar_sync_fetch_worker, run,
            fun(Task) ->
                ar_sync_scheduler:task_fetch_completed(
                    Task#task.task_ref, 0, #fetch_timing{})
            end}
    ], fun test_bandwidth_cap_trickle/0, 30).

test_bandwidth_cap_trickle() ->
    ensure_table(),
    {ok, PID} = gen_server:start(?MODULE, [], []),
    try
        Rate = 2 * ?DATA_CHUNK_SIZE,
        Peer = {6, 6, 6, 6, 9},
        Task = #task{ store_id = store1,
            sources = [#task_source{ peer = Peer }],
            offset = 0 },
        Now = ar_timer:monotonic_ms(),
        %% The configured two chunks/s earns one task in 1000/2 ms.
        RefillMs = 1000 div 2,
        FullRate = ar_sync_download_limit:refill(
            ar_sync_download_limit:new(Now - RefillMs), Now - RefillMs),
        ExhaustedRate = ar_sync_download_limit:consume(FullRate, Rate),
        _ = sys:replace_state(PID,
            fun(State) ->
                State0 = State#state{
                    download_limit = ExhaustedRate,
                    dispatch_scheduled = false },
                {ok, 1, State2} = admit_candidate(store1, Task, State0),
                State2
            end),
        PID ! {ar_sync_download_limit, wakeup},
        ?assertEqual(ok, ar_test_await:until(bandwidth_cap_trickle_dispatched, fun() ->
            {ok, State} = gen_server:call(PID, get_state),
            ar_sync_store:queues_empty(State#state.stores)
                andalso map_size(State#state.monitor_index) == 0
                andalso map_size(State#state.tasks) == 0
        end)),
        ?assertEqual(1, meck:num_calls(ar_sync_fetch_worker, run, 1))
    after
        gen_server:stop(PID)
    end.

%% Build a sync task the way ar_sync_chunk_picker does - peer-neutral, with the
%% offering peer as its one source. seed/1 routes each candidate through the
%% matching queue function so tests cover the same store queues as production.
base_dispatch() ->
    start_dispatch(#state{}).

seed(Tasks) ->
    State = lists:foldl(
        fun seed_candidate/2,
        #state{},
        Tasks),
    start_dispatch(State).

seed_candidate(#task{ store_id = StoreID } = Task, State) ->
    {ok, _ClaimedChunks, StoreStates} = ar_sync_store:admit(
        StoreID, Task, State#state.stores),
    State#state{ stores = StoreStates };
seed_candidate(#footprint_reservation{} = Reservation, State) ->
    StoreID = ar_sync_footprint:store_id(Reservation),
    {ok, _FootprintClaim, Footprints} = ar_sync_footprint:admit(
        Reservation, State#state.footprints),
    {ok, _StoreClaim, StoreStates} = ar_sync_store:admit(
        StoreID, Reservation, State#state.stores),
    State#state{ stores = StoreStates, footprints = Footprints }.

set_max_active_footprints(MaxActive, Dispatch) ->
    Dispatch#dispatch{ footprints = ar_sync_footprint:set_max_active(
        MaxActive, Dispatch#dispatch.footprints) }.

peer_dispatches(InflightCounts, PeerCaps) ->
    maps:fold(
        fun(Peer, InflightCount, Acc) ->
            lists:foldl(
                fun(_, Dispatches) ->
                    ar_sync_peer:start_task(Peer,
                        #task{ state = fetching,
                            store_id = existing_inflight }, Dispatches)
                end,
                Acc,
                lists:seq(1, InflightCount))
        end,
        ar_sync_peer:test_dispatch(PeerCaps),
        InflightCounts).

peer_dispatches_for_stores(PeerCaps, StoresByPeer) ->
    maps:fold(
        fun(Peer, StoreIDs, Acc) ->
            ar_sync_peer:set_store_task_targets(Peer, StoreIDs, Acc)
        end,
        ar_sync_peer:test_dispatch(PeerCaps),
        StoresByPeer).

%% Head-of-line: tasks whose only peer is at cap move behind unexamined work for
%% this pass, leaving another peer's tasks reachable in the same work queue.
test_head_of_line_blocked_peers() ->
    Slow = {1, 1, 1, 1, 9}, Fast = {2, 2, 2, 2, 9},
    SlowTasks = [#task{ store_id = store1,
            sources = [#task_source{ peer = Slow }],
            offset = I * ?DATA_CHUNK_SIZE }
        || I <- lists:seq(1, 20)],
    FastTasks = [#task{ store_id = store1,
            sources = [#task_source{ peer = Fast }],
            offset = (100 + I) * ?DATA_CHUNK_SIZE }
        || I <- lists:seq(1, 5)],
    {_State, Dispatch} = dispatch_tasks(#state{},
        (seed(SlowTasks ++ FastTasks))#dispatch{
            peers = peer_dispatches(
                #{Slow => 1000}, #{Slow => 1, Fast => 1000}) }),
    Peers = [T#task.peer || T <- Dispatch#dispatch.tasks_to_start],
    ?assertEqual(5, length(Peers)),
    ?assert(lists:all(fun(Pr) -> Pr =:= Fast end, Peers)).

%% At most the configured number of distinct footprint batches may be active.
test_footprint_budget() ->
    Tasks = lists:map(
        fun(I) ->
            Peer = {1, 1, 1, I, 9},
            Footprint = #footprint{ store_id = store1, partition = 1,
                footprint = I },
            footprint_reservation(store1, Peer, Footprint,
                [I * ?DATA_CHUNK_SIZE])
        end,
        lists:seq(1, 5)),
    Dispatch0 = set_max_active_footprints(2, seed(Tasks)),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{
            peers = ar_sync_peer:test_dispatch(maps:from_list([
                {{1, 1, 1, I, 9}, 1000} || I <- lists:seq(1, 5)])) }),
    ?assertEqual(2, length(Dispatch#dispatch.tasks_to_start)),
    ?assertEqual(2, ar_sync_footprint:bound_count(
        Dispatch#dispatch.footprints)).

%% Chunks generated from one footprint task share its entropy slot.
test_footprint_piggyback() ->
    P = {1, 1, 1, 1, 9},
    Footprint = #footprint{ store_id = store1, partition = 1, footprint = 1 },
    Tasks = [footprint_reservation(store1, P, Footprint,
        [I * ?DATA_CHUNK_SIZE || I <- lists:seq(1, 3)])],
    Dispatch0 = set_max_active_footprints(1, seed(Tasks)),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{
            peers = peer_dispatches_for_stores(
                #{P => 3}, #{P => [store1]}) }),
    ?assertEqual(3, length(Dispatch#dispatch.tasks_to_start)),
    Reservation = ar_sync_footprint:test_get(Footprint, Dispatch#dispatch.footprints),
    ?assertEqual(bound, ar_sync_footprint:reservation_state(Reservation)),
    ?assertEqual(3, ar_sync_footprint:active_tasks(Reservation)),
    ?assertEqual(P, ar_sync_footprint:reservation_peer(Reservation)).

%% A bound reservation with retained intervals remains normal store work. The
%% store-first dispatch loop enqueues one queue-sized batch and starts it even
%% when the ordinary store queue was initially empty.
test_bound_footprint_refills_through_dispatch() ->
    Peer = {1, 1, 1, 1, 9},
    StoreID = store1,
    Footprint = #footprint{ store_id = StoreID, partition = 1,
        footprint = 1 },
    %% A one-task active cap plus a two-task queue limit lets one fetch start
    %% while one waits, leaving one retained chunk for the next dispatch pass.
    Intervals = ar_intervals:from_list([{3 * ?DATA_CHUNK_SIZE, 0}]),
    Reservation = ar_sync_footprint:test_reservation(
        StoreID, Footprint,
        [#task_source{ peer = Peer, footprint = Footprint,
            intervals = Intervals }],
        Peer, 0, bound),
    State = #state{
        stores = ar_sync_store:new(),
        footprints = ar_sync_footprint:test_state([Reservation])
    },
    Dispatch0 = start_dispatch(State),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{ peers = ar_sync_peer:set_store_task_targets(
            Peer, [StoreID], ar_sync_peer:test_dispatch(
                #{Peer => 1}, #{Peer => 2})) }),
    [Started] = Dispatch#dispatch.tasks_to_start,
    ?assertEqual(Peer, Started#task.peer),
    Reservation2 = ar_sync_footprint:test_get(Footprint, Dispatch#dispatch.footprints),
    ?assertEqual(2, ar_sync_footprint:active_tasks(Reservation2)),
    [#task_source{ intervals = RemainingIntervals }] =
        ar_sync_footprint:sources(Reservation2),
    ?assertEqual(?DATA_CHUNK_SIZE,
        ar_intervals:sum(RemainingIntervals)).

%% Marking a reservation draining stops refills, but its already-enqueued
%% children remain eligible so the reservation can reach zero and release.
test_draining_footprint_tasks_finish_dispatching() ->
    Peer = {2, 2, 2, 2, 9},
    StoreID = store1,
    Footprint = #footprint{ store_id = StoreID, partition = 1,
        footprint = 2 },
    Child = #task{
        offset = 0,
        store_id = StoreID,
        footprint = Footprint,
        sources = [#task_source{ peer = Peer, footprint = Footprint }],
        state = queued
    },
    Reservation = ar_sync_footprint:test_reservation(
        StoreID, Footprint,
        [#task_source{ peer = Peer, footprint = Footprint,
            intervals = ar_intervals:new() }],
        Peer, 1, draining),
    {ok, 1, StoreStates} = ar_sync_store:admit(
        StoreID, Child, ar_sync_store:new()),
    Dispatch0 = start_dispatch(#state{
        stores = StoreStates,
        footprints = ar_sync_footprint:test_state([Reservation])
    }),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{ peers = ar_sync_peer:test_dispatch(#{Peer => 1}) }),
    [Started] = Dispatch#dispatch.tasks_to_start,
    ?assertEqual(Peer, Started#task.peer),
    ?assertEqual(Footprint, Started#task.footprint).

%% A peer/store pair binds one partial footprint at a time. Enqueued tasks
%% consume that pair's queue capacity before another footprint can bind.
test_peer_store_queue_limit_bounds_footprint_bindings() ->
    Peer = {1, 1, 1, 1, 9},
    AlternatePeer = {2, 2, 2, 2, 9},
    FootprintA = #footprint{ store_id = store1, partition = 1, footprint = 1 },
    FootprintB = #footprint{ store_id = store1, partition = 1, footprint = 2 },
    ReservationB = footprint_reservation(store1, Peer, FootprintB,
        [I * ?DATA_CHUNK_SIZE || I <- lists:seq(4, 6)]),
    [PeerSource] = ReservationB#footprint_reservation.sources,
    Tasks = [
        footprint_reservation(store1, Peer, FootprintA,
            [I * ?DATA_CHUNK_SIZE || I <- lists:seq(1, 3)]),
        ReservationB#footprint_reservation{ sources = [PeerSource,
            PeerSource#task_source{ peer = AlternatePeer }] }
    ],
    Dispatch0 = set_max_active_footprints(10, seed(Tasks)),
    PeerDispatches0 = peer_dispatches_for_stores(
        #{Peer => 3, AlternatePeer => 1},
        #{Peer => [store1], AlternatePeer => [store1]}),
    PeerDispatches = ar_sync_peer:start_task(AlternatePeer,
        #task{ state = fetching, store_id = store1 }, PeerDispatches0),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{ peers = PeerDispatches }),
    Started = Dispatch#dispatch.tasks_to_start,
    ?assertEqual(3, length(Started)),
    ?assertEqual(1, length(lists:usort([
        Footprint || #task{ footprint = Footprint } <- Started
    ]))).

%% Peer/store load is a selection bias rather than a second hard gate, so a
%% store with remaining work uses capacity that another store cannot use.
test_peer_store_queue_limit_is_work_conserving() ->
    Peer = {1, 1, 1, 1, 9},
    TasksA = [#task{ store_id = store_a,
        sources = [#task_source{ peer = Peer }], offset = 0 }],
    TasksB = [#task{ store_id = store_b,
            sources = [#task_source{ peer = Peer }],
            offset = I * ?DATA_CHUNK_SIZE }
        || I <- lists:seq(1, 10)],
    Dispatch1 = (seed(TasksA ++ TasksB))#dispatch{
        peers = peer_dispatches_for_stores(
            #{Peer => 4}, #{Peer => [store_a, store_b]}) },
    {_State, Dispatch} = dispatch_tasks(#state{}, Dispatch1),
    Started = Dispatch#dispatch.tasks_to_start,
    %% The four-task peer limit starts the only store_a task and lets store_b
    %% use the otherwise-unused second half of its nominal two-task share.
    ?assertEqual(4, length(Started)),
    ?assertEqual(1, count_tasks_by_store(store_a, Started)),
    ?assertEqual(3, count_tasks_by_store(store_b, Started)).

%% Byte availability remains startable when another footprint consumes the
%% only entropy slot.
test_byte_source_bypasses_full_footprint_pool() ->
    BytePeer = {1, 1, 1, 1, 9},
    FootprintPeer = {2, 2, 2, 2, 9},
    ActiveFootprint = #footprint{ store_id = store1, partition = 1,
        footprint = 1 },
    QueuedFootprint = #footprint{ store_id = store1, partition = 1,
        footprint = 2 },
    Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
    Task = ar_sync_footprint:new_reservation(store1, QueuedFootprint,
        [#task_source{ peer = BytePeer, intervals = Intervals },
            #task_source{ peer = FootprintPeer, footprint = QueuedFootprint,
                intervals = Intervals }]),
    BoundReservation = ar_sync_footprint:test_reservation(
        store1, ActiveFootprint,
        [#task_source{ peer = FootprintPeer,
            footprint = ActiveFootprint, intervals = Intervals }],
        FootprintPeer, 1, bound),
    Dispatch0 = seed([Task]),
    Footprints = ar_sync_footprint:test_state([Task, BoundReservation]),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{
            footprints = ar_sync_footprint:test_dispatch(Footprints, 1),
            peers = ar_sync_peer:test_dispatch(#{BytePeer => 1, FootprintPeer => 1})
        }),
    [Started] = Dispatch#dispatch.tasks_to_start,
    ?assertEqual(BytePeer, Started#task.peer),
    ?assertEqual(none, Started#task.footprint).

%% A footprint reservation chooses one source, and every enqueued task is bound
%% to that source even when the reservation carried other alternatives.
test_bound_footprint_keeps_its_peer() ->
    BoundPeer = {1, 1, 1, 1, 9},
    BytePeer = {2, 2, 2, 2, 9},
    OtherFootprintPeer = {3, 3, 3, 3, 9},
    Footprint = #footprint{ store_id = store1, partition = 1, footprint = 1 },
    Intervals = ar_intervals:from_list([
        {2 * ?DATA_CHUNK_SIZE, 0}]),
    Task = ar_sync_footprint:new_reservation(store1, Footprint,
        [#task_source{ peer = BoundPeer, footprint = Footprint,
                intervals = Intervals },
            #task_source{ peer = BytePeer, intervals = Intervals },
            #task_source{ peer = OtherFootprintPeer, footprint = Footprint,
                intervals = Intervals }]),
    Dispatch0 = seed([Task]),
    {_State, Dispatch} = dispatch_tasks(#state{},
        Dispatch0#dispatch{
            %% BoundPeer has the lowest normalized load, so it wins the one-time
            %% source choice despite the byte and footprint alternatives.
            peers = peer_dispatches(
                #{BoundPeer => 1, BytePeer => 8, OtherFootprintPeer => 6},
                #{BoundPeer => 10, BytePeer => 10,
                    OtherFootprintPeer => 10})
        }),
    ?assertEqual(2, length(Dispatch#dispatch.tasks_to_start)),
    ?assert(lists:all(
        fun(#task{ peer = Peer, sources = Sources }) ->
            Peer =:= BoundPeer andalso Sources =:= [#task_source{
                peer = BoundPeer, footprint = Footprint }]
        end,
        Dispatch#dispatch.tasks_to_start)).

%% The per-peer cap bounds how many workers each peer may run.
test_per_peer_concurrency_cap() ->
    Good = {1, 1, 1, 1, 25},
    Bad = {2, 2, 2, 2, 100},
    Tasks = [#task{ store_id = store1,
            sources = [#task_source{ peer = Good }],
            offset = I * ?DATA_CHUNK_SIZE }
        || I <- lists:seq(1, 50)]
        ++ [#task{ store_id = store1,
            sources = [#task_source{ peer = Bad }],
            offset = (100 + I) * ?DATA_CHUNK_SIZE }
        || I <- lists:seq(1, 50)],
    %% The sixteen- and eight-task caps total twenty-four, below the unmeasured
    %% store's twenty-five-task probe, so this test isolates peer caps.
    GoodCap = 16,
    {_State, Dispatch} = dispatch_tasks(#state{},
        (seed(Tasks))#dispatch{
            %% Explicit queue limits keep enough runnable work behind each active
            %% cap; the default eight-task bootstrap is for unmeasured peers.
            peers = ar_sync_peer:test_dispatch(
                #{Good => GoodCap, Bad => 8},
                #{Good => GoodCap, Bad => 8}) }),
    TasksToStart = Dispatch#dispatch.tasks_to_start,
    GoodCount = count_tasks_by_peer(Good, TasksToStart),
    BadCount = count_tasks_by_peer(Bad, TasksToStart),
    ?assertEqual(GoodCap, GoodCount),
    ?assertEqual(8, BadCount).

%% Two stores at different footprint positions must both get capacity through
%% least-loaded selection rather than let the lower-position store monopolize.
test_store_balance() ->
    P = {1, 1, 1, 1, 9},
    TasksA = [#task{ store_id = store_a,
            sources = [#task_source{ peer = P }],
            offset = I * ?DATA_CHUNK_SIZE } || I <- lists:seq(1, 20)],
    TasksB = [#task{ store_id = store_b,
            sources = [#task_source{ peer = P }],
            offset = I * ?DATA_CHUNK_SIZE } || I <- lists:seq(1, 20)],
    {_State, Dispatch} = dispatch_tasks(#state{},
        (seed(TasksA ++ TasksB))#dispatch{
            %% Ten runnable tasks make the queue deep enough to fill the
            %% explicit ten-request active cap in this synthetic first pass.
            peers = ar_sync_peer:test_dispatch(#{P => 10}, #{P => 10}) }),
    TasksToStart = Dispatch#dispatch.tasks_to_start,
    ACount = count_tasks_by_store(store_a, TasksToStart),
    BCount = count_tasks_by_store(store_b, TasksToStart),
    ?assertEqual(10, length(TasksToStart)),
    ?assertEqual(5, ACount),
    ?assertEqual(5, BCount).

%% A store without sufficient disk space starts no work.
test_disk_gate() ->
    P = {1, 1, 1, 1, 9},
    %% store_id = no_disk -> is_disk_space_sufficient/1 returns false (see mock).
    Tasks = [#task{ store_id = no_disk,
            sources = [#task_source{ peer = P }],
            offset = I * ?DATA_CHUNK_SIZE }
        || I <- lists:seq(1, 5)],
    {_State, Dispatch} = dispatch_tasks(#state{}, seed(Tasks)),
    ?assertEqual([], Dispatch#dispatch.tasks_to_start).

%% Per-peer cap = derived control (ar_sync_peer); this test pins the scheduler
%% wiring end to end: unmeasured peers hold the exploration cap, productive
%% driven work applies the exploration-window probe, and inactive peers drop
%% from the cap map.
cap_test_() ->
    PeerA = {1, 1, 1, 1, 25},
    PeerB = {2, 2, 2, 2, 75},
    {timeout, 30, fun() ->
        %% A genuinely unseen peer retains one exploration request until the
        %% next control tick measures it.
        PeerDispatches = ar_sync_peer:test_dispatch(#{known => 1}),
        ?assertEqual(1,
            ar_sync_peer:concurrency_cap(unknown, PeerDispatches)),
        ?assertEqual(1,
            ar_sync_peer:concurrency_cap(known, PeerDispatches)),
        %% First tick has no productive request time, so both peers remain at
        %% the eight-request exploration bound despite each having a queued
        %% task behind eight active fetches.
        InitialPeerState = record_peer_results([PeerA, PeerB],
            100_000_000, #fetch_timing{}, ar_sync_peer:new()),
        State0 = driven_peer_state(
            #{PeerA => 8, PeerB => 8}, #state{
                peer_state = InitialPeerState }),
        State1 = tick(State0, 1000),
        PeerDispatches1 = ar_sync_peer:start_dispatch(
            #{}, [], State1#state.peer_state),
        ?assertEqual(8, ar_sync_peer:concurrency_cap(PeerA, PeerDispatches1)),
        ?assertEqual(8, ar_sync_peer:concurrency_cap(PeerB, PeerDispatches1)),
        %% The first productive driven observation establishes an eight-request
        %% baseline and starts its sixteen-request probe.
        ProductiveTiming = #fetch_timing{ productive_ms = 1000 },
        PeerState2 = record_peer_results([PeerA, PeerB],
            100_000_000, ProductiveTiming, State1#state.peer_state),
        State2 = tick(driven_peer_state(
            #{PeerA => 8, PeerB => 8}, State1#state{
                peer_state = PeerState2 }), 2000),
        PeerDispatches2 = ar_sync_peer:start_dispatch(
            #{}, [], State2#state.peer_state),
        ?assertEqual(16, ar_sync_peer:concurrency_cap(PeerA, PeerDispatches2)),
        ?assertEqual(16, ar_sync_peer:concurrency_cap(PeerB, PeerDispatches2)),
        %% Third tick with PeerB's queue drained and nothing inflight:
        %% PeerB drops from the caps map (its budget memory is kept in the
        %% scheduler state); PeerA records the first flat-goodput observation
        %% and advances its additive probe while waiting for repeated evidence.
        PeerState3 = record_peer_results([PeerA, PeerB],
            100_000_000, ProductiveTiming, State2#state.peer_state),
        State3 = tick(driven_peer_state(#{PeerA => 16}, State2#state{
            peer_state = PeerState3 }), 3000),
        PeerDispatches3 = ar_sync_peer:start_dispatch(
            #{}, [], State3#state.peer_state),
        ?assertEqual(24, ar_sync_peer:concurrency_cap(PeerA, PeerDispatches3)),
        ?assertEqual(1, ar_sync_peer:concurrency_cap(PeerB, PeerDispatches3)),
        %% Dispatch uses the recomputed cap; an unknown peer retains one probe.
        Dispatch = (base_dispatch())#dispatch{
            peers = ar_sync_peer:test_dispatch(#{p => 40}) },
        ?assertEqual(40,
            ar_sync_peer:concurrency_cap(p, Dispatch#dispatch.peers)),
        ?assertEqual(1,
            ar_sync_peer:concurrency_cap(unknown, Dispatch#dispatch.peers))
    end}.

record_peer_results(Peers, DeliveredBytes, FetchTiming, PeerState) ->
    lists:foldl(
        fun(Peer, Acc) ->
            ar_sync_peer:record_result(
                Peer, DeliveredBytes, FetchTiming, Acc)
        end,
        PeerState,
        Peers).

driven_peer_state(InflightByPeer, State) ->
    Tasks = maps:fold(
        fun(Peer, Count, Acc) ->
            lists:foldl(
                fun(Index, TasksAcc) ->
                    Ref = {Peer, Index},
                    maps:put(Ref, #task{ task_ref = Ref, state = fetching,
                        peer = Peer, store_id = s }, TasksAcc)
                end,
                Acc,
                lists:seq(1, Count))
        end,
        #{},
        InflightByPeer),
    PeerQueues = maps:map(
        fun(Peer, _Count) ->
            queue:from_list([#task{ state = queued, peer = Peer,
                store_id = s }])
        end,
        InflightByPeer),
    State#state{ tasks = Tasks, peer_queues = PeerQueues,
        driven_peers = maps:map(fun(_Peer, _Count) -> true end,
            InflightByPeer) }.

%% End-to-end: enqueue a mix of byte + footprint tasks; immediate-exit workers
%% drive the spawn -> 'DOWN' -> release -> refill cycle to completion. Asserts no
%% residue (no leak): every structure is empty once the work drains.
no_leak_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([packing, entropy, cache_size]) -> 1000000;
                (K) -> meck:passthrough([K]) end},
        {ar_data_sync, is_chunk_cache_full, fun() -> false end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {ar_sync_fetch_worker, run,
            fun(Task) ->
                ar_sync_scheduler:task_fetch_completed(
                    Task#task.task_ref, 0, #fetch_timing{})
            end}
    ], fun test_no_leak/0, 30).

test_no_leak() ->
    drains_clean().

%% Same no-residue guarantee when workers CRASH (exit abnormally) rather than
%% complete: the monitor 'DOWN' must still release the slot, the per-peer count,
%% and the byte range. This is the core premise of the monitor-based design.
crash_no_leak_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([packing, entropy, cache_size]) -> 1000000;
                (K) -> meck:passthrough([K]) end},
        {ar_data_sync, is_chunk_cache_full, fun() -> false end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {ar_sync_fetch_worker, run, fun(_Task) -> exit(simulated_crash) end}
    ], fun drains_clean/0, 30).

%% Drive a mix of byte + footprint tasks through the gen_server (worker
%% behaviour set by the caller's mock) and assert no residue once work drains.
drains_clean() ->
    ensure_table(),
    %% Unregistered (the node under test already owns the registered name);
    %% drive it directly by pid.
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    try
        Peers = [{1, 1, 1, 1, 9}, {2, 2, 2, 2, 9}],
        ByteTasks = [#task{ store_id = store1,
                sources = [#task_source{ peer = Peer } || Peer <- Peers],
                offset = I * ?DATA_CHUNK_SIZE }
            || I <- lists:seq(1, 10)],
        Reservations = lists:map(
            fun(I) ->
                Footprint = #footprint{ store_id = store1, partition = 1,
                    footprint = I },
                footprint_reservation(store1, lists:nth(I, Peers), Footprint,
                    [(10 + I) * ?DATA_CHUNK_SIZE])
            end,
            lists:seq(1, 2)),
        Tasks = ByteTasks ++ Reservations,
        ExpectedClaims = lists:sum([
            ar_sync_store:chunks_in_claim(Task) || Task <- Tasks
        ]),
        ?assertEqual({ok, ExpectedClaims},
            gen_server:call(Pid, {claim_and_enqueue, store1, Tasks})),
        ?assertEqual(ok, ar_test_await:until(dispatcher_drained, fun() ->
            {ok, S} = gen_server:call(Pid, get_state),
            ar_sync_store:queues_empty(S#state.stores)
                andalso map_size(S#state.monitor_index) == 0
                andalso map_size(S#state.tasks) == 0
        end)),
        {ok, State} = gen_server:call(Pid, get_state),
        ?assertEqual(0, map_size(State#state.monitor_index)),
        ?assertEqual(0, map_size(State#state.tasks)),
        ?assert(ar_sync_footprint:is_empty(State#state.footprints)),
        ?assert(ar_sync_store:queues_empty(State#state.stores)),
        ?assert(ar_sync_store:claims_empty(State#state.stores))
    after
        gen_server:stop(Pid)
    end.

%% A full chunk cache stalls dispatch (nothing spawned, tasks stay queued), and
%% re-pushing the same tasks is deduped rather than doubling the queue.
cache_full_test_() ->
    ar_test_util:with_mocked([
        {arweave_config, get,
            fun([packing, entropy, cache_size]) -> 1000000;
                (K) -> meck:passthrough([K]) end},
        {ar_data_sync, chunk_cache_size, fun() -> 1 end},
        {ar_data_sync, chunk_cache_size_limit, fun() -> 1 end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ], fun test_cache_full/0, 30).

test_cache_full() ->
    ensure_table(),
    {ok, Pid} = gen_server:start(?MODULE, [], []),
    try
        Peers = [{1, 1, 1, 1, 9}, {2, 2, 2, 2, 9}],
        Tasks = [#task{
                offset = I * ?DATA_CHUNK_SIZE,
                sources = [#task_source{ peer = Peer } || Peer <- Peers],
                store_id = store1 }
            || I <- lists:seq(1, 5)],
        ?assertEqual({ok, length(Tasks)},
            gen_server:call(Pid, {claim_and_enqueue, store1, Tasks})),
        ?assertEqual({ok, 0},
            gen_server:call(Pid, {claim_and_enqueue, store1, Tasks})),
        {ok, State} = gen_server:call(Pid, get_state),   %% syncs both casts
        ?assertEqual(0, map_size(State#state.monitor_index)),  %% cache full -> nothing spawned
        ?assertEqual(5, queued_task_count_by_store(store1, State))  %% deduped, not 10
    after
        gen_server:stop(Pid)
    end.

ensure_table() ->
    ar_sync_peer:create_ets().

count_tasks_by_peer(Peer, Tasks) ->
    lists:foldl(
        fun(Task, Count) ->
            case Task#task.peer =:= Peer of
                true -> Count + 1;
                false -> Count
            end
        end,
        0,
        Tasks).

count_tasks_by_store(StoreID, Tasks) ->
    lists:foldl(
        fun(Task, Count) ->
            case Task#task.store_id =:= StoreID of
                true -> Count + 1;
                false -> Count
            end
        end,
        0,
        Tasks).

footprint_reservation(StoreID, Peer, Footprint, Offsets) ->
    Intervals = ar_intervals:from_list([
        {Offset + ?DATA_CHUNK_SIZE, Offset} || Offset <- Offsets
    ]),
    ar_sync_footprint:new_reservation(StoreID, Footprint,
        [#task_source{ peer = Peer, footprint = Footprint,
            intervals = Intervals }]).

-endif.
