%%% @doc Global scheduler for the network-sync subsystem.
%%%
%%% arweave_sync_store_sweeper resolves local need through arweave_sync_chunk_picker,
%%% and pushes tasks and footprint work here, bounded by the
%%% store's measured queue target.
%%% This server owns task selection and dispatch. Tasks first bind into a
%%% bounded runnable queue for one peer, then enter active fetching up to that
%%% peer's concurrency cap. It spawn_monitors one transient
%%% `arweave_sync_fetch_worker' per active task and tracks the task through fetch
%%% and asynchronous storage completion.
%%%
%%% Worker liveness is network-concurrency accounting: inflight worker count is
%%% `map_size(monitor_index)'. Task state is backpressure accounting: claimed
%%% ranges and footprint entropy slots are released when a failed fetch is
%%% terminal or the one handed-off chunk reaches a terminal storage state.
-module(arweave_sync_scheduler).

-ifdef(AR_TEST).
%% Focused tests live outside the production module.
-export([
    activate_tasks/2,
    active_peers/4,
    admit_candidate/3,
    bind_tasks/2,
    dispatch_work/3,
    inflight_counts/1,
    on_task_fetch_completed/4,
    on_task_unpacked/2,
    on_task_write_completed/2,
    queued_task_count_by_store/2,
    record_driven_peers/2,
    resolve_work/2,
    start_dispatch/1,
    take_startable_task/2,
    terminate_workers/1,
    tick/2,
    worker_exited/3
]).
-endif.

-behaviour(gen_server).

-export([
    start_link/0,
    register_workers/0,
    admission_headroom/1,
    claim_and_enqueue/2,
    tick_interval_ms/0,
    report_fetch_completed/3,
    reset_store/1,
    report_unpacked/1,
    report_write_completed/1, report_write_completed/2,
    report_write_failed/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(AR_TEST).
-export([
    inflight_count/0,
    override_tick_interval_ms/1,
    reset_all_overrides/0
]).
-endif.

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

-include("arweave_sync_scheduler.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Supervisor child spec for the dispatcher.
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
    case
        catch gen_server:call(
            ?MODULE,
            {claim_and_enqueue, StoreID, Tasks},
            infinity
        )
    of
        {'EXIT', _Reason} -> blocked;
        Reply -> Reply
    end.

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
    ?LOG_INFO([{event, init}, {module, ?MODULE}]),
    %% A restarted scheduler must not expose a cap from its previous state.
    arweave_sync_peer:reset_rows(),
    {ok, _} = (arweave_sync_deps:clock()):send_after(
        tick_interval_ms(),
        self(),
        scheduler_tick
    ),
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
    HeadroomChunks = arweave_sync_store:admission_headroom(
        StoreID, State#state.stores
    ),
    {reply, HeadroomChunks, State};
handle_call({claim_and_enqueue, StoreID, Tasks}, _From, State) ->
    {Result, State2} = admit_tasks(StoreID, Tasks, State),
    {reply, Result, schedule_dispatch(State2)};
handle_call({reset_store, StoreID}, _From, State) ->
    {reply, ok, schedule_dispatch(do_reset_store(StoreID, State))};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, {error, unhandled}, State}.

handle_cast(dispatch, State) ->
    State2 = dispatch(State#state{dispatch_scheduled = false}),
    {noreply, State2};
handle_cast({task_fetch_completed, TaskRef, BytesFetched, FetchTiming}, State) ->
    {noreply,
        schedule_dispatch(
            on_task_fetch_completed(TaskRef, BytesFetched, FetchTiming, State)
        )};
handle_cast({task_unpacked, TaskRef}, State) ->
    {noreply, schedule_dispatch(on_task_unpacked(TaskRef, State))};
handle_cast({task_write_completed, TaskRef}, State) ->
    {noreply, schedule_dispatch(on_task_write_completed(TaskRef, State))};
handle_cast({task_write_failed, TaskRef}, State) ->
    {noreply, schedule_dispatch(on_task_write_finished(TaskRef, State))};
handle_cast({store_write_completed, StoreID}, State) ->
    {noreply, schedule_dispatch(record_store_drain(StoreID, State))};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, _PID, Reason}, State) ->
    {noreply, schedule_dispatch(worker_exited(Ref, Reason, State))};
handle_info(scheduler_tick, State) ->
    {ok, _} = (arweave_sync_deps:clock()):send_after(
        tick_interval_ms(),
        self(),
        scheduler_tick
    ),
    State2 = dispatch(tick(State, (arweave_sync_deps:clock()):monotonic_ms())),
    emit_sync_metrics(State2),
    {noreply, State2};
handle_info({arweave_sync_download_limit, wakeup}, State) ->
    {noreply,
        schedule_dispatch(State#state{
            download_limit =
                arweave_sync_download_limit:wakeup_fired(State#state.download_limit)
        })};
handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(Reason, #state{monitor_index = MonitorIndex}) ->
    terminate_workers(MonitorIndex),
    ?LOG_INFO([
        {event, terminate},
        {module, ?MODULE},
        {reason, io_lib:format("~p", [Reason])}
    ]),
    ok.

terminate_workers(MonitorIndex) ->
    Monitors = lists:map(
        fun({_TaskRef, WorkerPID}) ->
            {WorkerPID, erlang:monitor(process, WorkerPID)}
        end,
        maps:values(MonitorIndex)
    ),
    lists:foreach(
        fun({WorkerPID, _MonitorRef}) -> exit(WorkerPID, kill) end,
        Monitors
    ),
    lists:foreach(
        fun({WorkerPID, MonitorRef}) ->
            receive
                {'DOWN', MonitorRef, process, WorkerPID, _Reason} -> ok
            end
        end,
        Monitors
    ),
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
    Result =
        case Blocked of
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
            admit_tasks(
                StoreID,
                Rest,
                Admitted + ClaimedChunks,
                Blocked,
                State2
            )
    end.

admit_candidate(StoreID, #task{} = Task, State) ->
    case admit_task(StoreID, Task, State) of
        {ok, ClaimedChunks, State2} ->
            {ok, ClaimedChunks, State2};
        blocked ->
            {blocked, State}
    end;
admit_candidate(StoreID, Reservation, State) ->
    #state{stores = StoreStates, footprints = Footprints} = State,
    case arweave_sync_footprint:admit(Reservation, Footprints) of
        {ok, 0, Footprints2} ->
            {ok, 0, State#state{footprints = Footprints2}};
        {ok, ClaimedChunks, Footprints2} ->
            case arweave_sync_store:admit(StoreID, Reservation, StoreStates) of
                {ok, ClaimedChunks, StoreStates2} ->
                    {ok, ClaimedChunks, State#state{
                        stores = StoreStates2,
                        footprints = Footprints2
                    }};
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
    case
        arweave_sync_store:admit(
            StoreID, Task#task{store_id = StoreID}, StoreStates
        )
    of
        {ok, ClaimedChunks, StoreStates2} ->
            {ok, ClaimedChunks, State#state{stores = StoreStates2}};
        blocked ->
            blocked
    end.

%%%===================================================================
%%% Dispatch.
%%%===================================================================

%% @doc Queue one dispatch pass unless one is already pending. The scheduler
%% tick invokes dispatch directly as a fallback.
schedule_dispatch(#state{dispatch_scheduled = true} = State) ->
    State;
schedule_dispatch(State) ->
    gen_server:cast(self(), dispatch),
    State#state{dispatch_scheduled = true}.

%% @doc One dispatch pass: fill bounded peer queues, activate queued tasks up
%% to peer/global/store limits, refill the vacated queue slots, and spawn the
%% selected fetch workers.
dispatch(State0) ->
    DownloadLimit = arweave_sync_download_limit:refill(State0#state.download_limit),
    State1 = State0#state{download_limit = DownloadLimit},
    Dispatch0 = start_dispatch(State1),
    {State2, Dispatch1} = bind_tasks(State1, Dispatch0),
    State2A = record_driven_peers(State2, Dispatch1),
    {State3, Dispatch2} = activate_tasks(State2A, Dispatch1),
    Dispatch2A = Dispatch2#dispatch{
        stores = arweave_sync_store:refresh_work(
            Dispatch2#dispatch.footprints, Dispatch2#dispatch.stores
        )
    },
    {State4, Dispatch3} = bind_tasks(State3, Dispatch2A),
    State4A = record_driven_peers(State4, Dispatch3),
    State5 = start_tasks(State4A, Dispatch3),
    maybe_schedule_download_limit_wakeup(
        finish_dispatch(State5, Dispatch3)
    ).

%% @doc Return whether starting another fetch would fill the chunk cache. The
%% projected size includes chunks currently held by in-flight fetch workers.
is_chunk_cache_full(#dispatch{worker_count = WorkerCount}) ->
    (arweave_sync_deps:chunk_cache()):cached_size() + WorkerCount >=
        (arweave_sync_deps:chunk_cache()):limit().

start_tasks(State, Dispatch) ->
    #dispatch{tasks_to_start = TasksToStart} = Dispatch,
    {Started, Tasks2} = register_started(
        lists:reverse(TasksToStart), [], State#state.tasks
    ),
    State2 = State#state{tasks = Tasks2},
    lists:foldl(
        fun(Task, StateAcc) ->
            {WorkerPID, MonitorRef} = spawn_worker(Task),
            register_worker(
                Task#task.task_ref,
                WorkerPID,
                MonitorRef,
                StateAcc
            )
        end,
        State2,
        Started
    ).

finish_dispatch(State, Dispatch) ->
    #dispatch{
        stores = StoreDispatches,
        footprints = FootprintDispatch,
        peer_queues = PeerQueues
    } = Dispatch,
    State#state{
        stores = arweave_sync_store:finish_dispatch(StoreDispatches),
        footprints = arweave_sync_footprint:finish_dispatch(FootprintDispatch),
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
    HasWork =
        not arweave_sync_store:queues_empty(StoreStates) orelse
            arweave_sync_footprint:has_bound_work(Footprints) orelse
            not peer_queues_empty(PeerQueues),
    DownloadLimit2 = arweave_sync_download_limit:maybe_schedule_wakeup(
        DownloadLimit, HasWork, tick_interval_ms()
    ),
    State#state{download_limit = DownloadLimit2}.

%% @doc Record a started worker under its spawn monitor.
register_worker(TaskRef, WorkerPID, MonitorRef, State) ->
    State#state{
        monitor_index =
            maps:put(MonitorRef, {TaskRef, WorkerPID}, State#state.monitor_index)
    }.

spawn_worker(Task) ->
    spawn_monitor(arweave_sync_fetch_worker, run, [Task]).

%% @doc Build a fresh dispatch snapshot from the current server state.
%% The peer dispatch snapshots scheduler caps and accumulates fetches selected
%% during this pass. Store snapshots and footprint state are also
%% updated as tasks are selected.
start_dispatch(State) ->
    BoundTasks = peer_queued_tasks(State#state.peer_queues),
    Dispatch0 = #dispatch{
        stores = arweave_sync_store:start_dispatch(
            State#state.footprints,
            State#state.tasks,
            BoundTasks,
            State#state.stores
        ),
        worker_count = map_size(State#state.monitor_index),
        footprints = arweave_sync_footprint:start_dispatch(State#state.footprints),
        peer_queues = State#state.peer_queues,
        peers = arweave_sync_peer:start_dispatch(
            State#state.tasks,
            BoundTasks,
            State#state.peer_state
        )
    },
    StoresByPeer = arweave_sync_store:stores_by_peer(Dispatch0#dispatch.stores),
    PeerDispatches = arweave_sync_peer:set_store_task_targets(
        StoresByPeer, Dispatch0#dispatch.peers
    ),
    Dispatch0#dispatch{peers = PeerDispatches}.

%% @doc Fill peer queues from the least-loaded eligible stores. Concrete tasks
%% bind to one source; footprint reservations materialize one finite bound
%% batch. Neither operation starts network work or consumes download tokens.
bind_tasks(State, Dispatch) ->
    maybe
        {ok, StoreID} ?= arweave_sync_store:best_store(Dispatch#dispatch.stores),
        {QueuedWork, StoreDispatches} ?=
            arweave_sync_store:pop_work(
                StoreID, Dispatch#dispatch.stores
            ),
        Dispatch2 = Dispatch#dispatch{stores = StoreDispatches},
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
    case arweave_sync_footprint:reservation(Footprint, FootprintDispatch) of
        none ->
            none;
        Reservation ->
            #work{
                item = Reservation,
                store_id = arweave_sync_footprint:store_id(Reservation),
                sources = arweave_sync_footprint:sources(Reservation)
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
    #work{
        item = Item,
        store_id = StoreID,
        sources = Sources
    } = Work,
    #dispatch{
        footprints = FootprintDispatch,
        peers = PeerDispatches
    } = Dispatch,
    ReadySources = lists:filter(
        fun(Source) ->
            arweave_sync_footprint:is_source_compatible(
                Item, Source, FootprintDispatch
            ) andalso
                arweave_sync_peer:has_capacity(Item, Source, PeerDispatches)
        end,
        Sources
    ),
    arweave_sync_peer:best_source(StoreID, ReadySources, PeerDispatches).

dispatch_selected_work(
    #work{item = #task{} = Task},
    Source,
    State,
    Dispatch
) ->
    dispatch_task(Task, Source, State, Dispatch);
dispatch_selected_work(
    #work{
        item = #footprint_reservation{} = Reservation
    },
    Source,
    State,
    Dispatch
) ->
    dispatch_footprint_tasks(Reservation, Source, State, Dispatch).

dispatch_footprint_tasks(Reservation, Source, State, Dispatch) ->
    case ensure_entropy_capacity(Reservation, Source, Dispatch) of
        {deferred, Dispatch2} ->
            {State, Dispatch2};
        {ready, Dispatch2} ->
            {State, build_footprint_batch(Reservation, Source, Dispatch2)}
    end.

%% @doc Ensure entropy capacity before fetching from a footprint source.
%% Each peer/footprint pair needs cached entropy, so exceeding the cache would
%% repeatedly evict and regenerate entropy instead of sustaining chunk fetches.
ensure_entropy_capacity(Reservation, Source, Dispatch) ->
    Footprint = arweave_sync_footprint:key(Reservation),
    FootprintDispatch = Dispatch#dispatch.footprints,
    case
        arweave_sync_footprint:has_entropy_capacity(
            Footprint, Source, FootprintDispatch
        )
    of
        true ->
            {ready, Dispatch};
        false ->
            CandidatePriority =
                footprint_priority(Reservation, Source, Dispatch),
            BoundPriorities = bound_footprint_priorities(Dispatch),
            {Result, FootprintDispatch2} =
                arweave_sync_footprint:compete_for_entropy_capacity(
                    Footprint,
                    CandidatePriority,
                    BoundPriorities,
                    FootprintDispatch
                ),
            {Result, Dispatch#dispatch{footprints = FootprintDispatch2}}
    end.

bound_footprint_priorities(Dispatch) ->
    lists:map(
        fun({Reservation, Source}) ->
            Priority = footprint_priority(Reservation, Source, Dispatch),
            {Priority, arweave_sync_footprint:key(Reservation)}
        end,
        arweave_sync_footprint:bound_candidates(Dispatch#dispatch.footprints)
    ).

%% @doc Rank a footprint's claim on an entropy slot as {slots its store
%% holds, whether its store can take its work, its peer's priority}; lower is
%% stronger. A footprint with fetches in flight is busy, not blocked, even when
%% those fetches fill its store's pipeline.
footprint_priority(Reservation, Source, Dispatch) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    StoreAvailable =
        arweave_sync_footprint:is_busy(Reservation) orelse
            arweave_sync_store:has_capacity(StoreID, Dispatch#dispatch.stores),
    arweave_sync_footprint:slot_priority(
        arweave_sync_footprint:bound_count(
            StoreID, Dispatch#dispatch.footprints
        ),
        StoreAvailable,
        arweave_sync_peer:priority(Reservation, Source, Dispatch#dispatch.peers)
    ).

build_footprint_batch(Reservation, Source, Dispatch) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    #task_source{peer = Peer, intervals = Intervals} = Source,
    Limit = arweave_sync_peer:store_capacity(
        Peer, StoreID, Dispatch#dispatch.peers
    ),
    AvailableIntervals = arweave_sync_store:unclaimed_intervals(
        StoreID, Intervals, Dispatch#dispatch.stores
    ),
    {FootprintDispatch2, Tasks, BoundReservation} =
        arweave_sync_footprint:build_batch(
            Reservation,
            Source,
            AvailableIntervals,
            Limit,
            Dispatch#dispatch.footprints
        ),
    StoreDispatches2 =
        case Reservation#footprint_reservation.state of
            queued ->
                arweave_sync_store:bind_footprint(
                    Reservation, Dispatch#dispatch.stores
                );
            bound ->
                Dispatch#dispatch.stores
        end,
    BoundTasks = [Task#task{peer = Peer} || Task <- Tasks],
    StoreDispatches3 = arweave_sync_store:enqueue_bound_tasks(
        BoundTasks, StoreDispatches2
    ),
    StoreDispatches4 = arweave_sync_store:add_reservations(
        [BoundReservation], StoreDispatches3
    ),
    PeerDispatches = arweave_sync_peer:enqueue_tasks(
        Peer, BoundTasks, Dispatch#dispatch.peers
    ),
    PeerQueues = enqueue_peer_tasks(
        Peer, BoundTasks, Dispatch#dispatch.peer_queues
    ),
    Dispatch#dispatch{
        footprints = FootprintDispatch2,
        stores = StoreDispatches4,
        peers = PeerDispatches,
        peer_queues = PeerQueues
    }.

%% @doc Commit a selected task to its peer queue without starting its fetch.
dispatch_task(Task0, Source, State, Dispatch) ->
    #task_source{peer = Peer, footprint = Footprint} = Source,
    Task = Task0#task{peer = Peer, footprint = Footprint},
    StoreDispatches = arweave_sync_store:bind_task(
        Task0, Dispatch#dispatch.stores
    ),
    PeerDispatches = arweave_sync_peer:enqueue_tasks(
        Peer, [Task], Dispatch#dispatch.peers
    ),
    PeerQueues = enqueue_peer_tasks(
        Peer, [Task], Dispatch#dispatch.peer_queues
    ),
    Dispatch2 = Dispatch#dispatch{
        stores = StoreDispatches,
        peers = PeerDispatches,
        peer_queues = PeerQueues
    },
    {State, Dispatch2}.

%% @doc Start peer-bound tasks while peer caps and shared resource gates allow.
activate_tasks(State, Dispatch) ->
    #state{download_limit = DownloadLimit} = State,
    maybe
        false ?= is_chunk_cache_full(Dispatch),
        true ?= arweave_sync_download_limit:has_capacity(DownloadLimit),
        {Task, Dispatch2} ?= pop_startable_peer_task(Dispatch),
        {State2, Dispatch3} = activate_task(Task, State, Dispatch2),
        activate_tasks(State2, Dispatch3)
    else
        _ -> {State, Dispatch}
    end.

activate_task(#task{peer = Peer} = Task, State, Dispatch) ->
    #dispatch{
        worker_count = WorkerCount,
        tasks_to_start = TasksToStart
    } = Dispatch,
    StoreDispatches = arweave_sync_store:start_bound_task(
        Task, Dispatch#dispatch.stores
    ),
    PeerDispatches = arweave_sync_peer:start_task(
        Peer, Task#task{state = fetching}, Dispatch#dispatch.peers
    ),
    Dispatch2 = Dispatch#dispatch{
        stores = StoreDispatches,
        peers = PeerDispatches,
        worker_count = WorkerCount + 1,
        tasks_to_start = [Task | TasksToStart]
    },
    State2 = State#state{
        download_limit =
            arweave_sync_download_limit:consume(
                State#state.download_limit, ?DATA_CHUNK_SIZE
            )
    },
    {State2, Dispatch2}.

pop_startable_peer_task(
    #dispatch{
        peer_queues = PeerQueues,
        stores = StoreDispatches,
        peers = PeerDispatches
    } = Dispatch
) ->
    Candidates = maps:fold(
        fun(Peer, Queue, Acc) ->
            case arweave_sync_peer:has_capacity(Peer, PeerDispatches) of
                false ->
                    Acc;
                true ->
                    case take_startable_task(Queue, StoreDispatches) of
                        none ->
                            Acc;
                        {Task, Queue2} ->
                            Priority = {
                                arweave_sync_peer:fetching_count(
                                    Peer, PeerDispatches
                                ),
                                Peer
                            },
                            [{Priority, Peer, Task, Queue2} | Acc]
                    end
            end
        end,
        [],
        PeerQueues
    ),
    case Candidates of
        [] ->
            none;
        _ ->
            {_Priority, Peer, Task, Queue2} = lists:min(Candidates),
            PeerQueues2 = put_peer_queue(Peer, Queue2, PeerQueues),
            {Task, Dispatch#dispatch{peer_queues = PeerQueues2}}
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
best_startable_task(
    [#task{store_id = StoreID} | Rest],
    StoreDispatches,
    Index,
    Best
) ->
    Best2 =
        case
            arweave_sync_store:can_start_bound_task(
                StoreID, StoreDispatches
            )
        of
            false ->
                Best;
            true ->
                Priority = {
                    arweave_sync_store:fetching_count(
                        StoreID, StoreDispatches
                    ),
                    Index
                },
                case Best of
                    none ->
                        {Priority, Index};
                    {BestPriority, _BestIndex} when Priority < BestPriority ->
                        {Priority, Index};
                    _ ->
                        Best
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
        Tasks
    ),
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
        PeerQueues
    ).

peer_queue_counts(PeerQueues) ->
    maps:map(fun(_Peer, Queue) -> queue:len(Queue) end, PeerQueues).

peer_queues_empty(PeerQueues) ->
    maps:size(PeerQueues) =:= 0.

%% @doc Latch peers whose runnable queues are held behind full active caps while
%% shared download and cache gates are open. Sampling every dispatch avoids
%% classifying completion waves by their phase at the scheduler tick.
record_driven_peers(State, Dispatch) ->
    SharedCapacity =
        arweave_sync_download_limit:has_capacity(
            State#state.download_limit
        ) andalso
            not is_chunk_cache_full(Dispatch),
    case SharedCapacity of
        false ->
            State;
        true ->
            DrivenPeers = maps:fold(
                fun(Peer, Queue, Acc) ->
                    case
                        not queue:is_empty(Queue) andalso
                            not arweave_sync_peer:has_capacity(
                                Peer, Dispatch#dispatch.peers
                            )
                    of
                        true -> maps:put(Peer, true, Acc);
                        false -> Acc
                    end
                end,
                State#state.driven_peers,
                Dispatch#dispatch.peer_queues
            ),
            State#state{driven_peers = DrivenPeers}
    end.

%% @doc Report the bytes fetched by the worker and the network-attempt time
%% consumed by the task.
report_fetch_completed({Pid, _Ref} = TaskRef, BytesFetched, FetchTiming) when
    is_pid(Pid)
->
    catch gen_server:cast(
        Pid,
        {task_fetch_completed, TaskRef, BytesFetched, FetchTiming}
    ),
    ok.

%% @doc Report that ar_data_sync finished processing one handed-off chunk.
report_write_completed(StoreID, undefined) ->
    gen_server:cast(?MODULE, {store_write_completed, StoreID});
report_write_completed(_StoreID, TaskRef) ->
    report_write_completed(TaskRef).

%% @doc Report that a handed-off chunk is unpacked, so its footprint's
%% entropy is no longer needed.
report_unpacked(undefined) ->
    ok;
report_unpacked({Pid, _Ref} = TaskRef) when is_pid(Pid) ->
    catch gen_server:cast(Pid, {task_unpacked, TaskRef}),
    ok;
report_unpacked(TaskRef) ->
    catch gen_server:cast(?MODULE, {task_unpacked, TaskRef}),
    ok.

%% @doc Report that a scheduler-owned chunk reached a terminal write result.
report_write_completed(undefined) ->
    ok;
report_write_completed({Pid, _Ref} = TaskRef) when is_pid(Pid) ->
    catch gen_server:cast(Pid, {task_write_completed, TaskRef}),
    ok;
report_write_completed(TaskRef) ->
    catch gen_server:cast(?MODULE, {task_write_completed, TaskRef}),
    ok.

%% @doc Cancel old active work before a replacement store accepts handoffs.
reset_store(StoreID) ->
    case whereis(?MODULE) of
        undefined -> ok;
        PID -> gen_server:call(PID, {reset_store, StoreID}, infinity)
    end.

do_reset_store(StoreID, State) ->
    StoreTasks = maps:filter(
        fun(_, #task{store_id = SID}) -> SID =:= StoreID end,
        State#state.tasks
    ),
    StoreMonitors = maps:filter(
        fun(_, {TaskRef, _}) -> maps:is_key(TaskRef, StoreTasks) end,
        State#state.monitor_index
    ),
    %% A fetching task may already have handed its chunk to the old writer.
    %% Wait for its worker to stop before allowing a new owner to accept data.
    terminate_workers(StoreMonitors),
    maps:foreach(
        fun(Ref, _) -> erlang:demonitor(Ref, [flush]) end,
        StoreMonitors
    ),
    State2 = State#state{
        monitor_index = maps:without(
            maps:keys(StoreMonitors), State#state.monitor_index
        )
    },
    maps:fold(
        fun(_, Task, Acc) ->
            finish_task(Task#task{state = write_complete}, Acc)
        end,
        State2,
        StoreTasks
    ).

%% @doc Report a failed handoff; it is released without crediting the store's
%% drain rate.
report_write_failed({PID, _Ref} = TaskRef) when is_pid(PID) ->
    gen_server:cast(PID, {task_write_failed, TaskRef}).

%% @doc Evolve peer control from the scheduler's active, inflight, and driven
%% views. arweave_sync_peer owns delivery observations and cap publication.
tick(State, NowMs) ->
    State2 = sample_store_drain_rates(State, NowMs),
    #state{
        tasks = Tasks,
        peer_state = PeerState,
        stores = StoreStates,
        footprints = Footprints,
        peer_queues = PeerQueues,
        driven_peers = DrivenPeers
    } = State2,
    InflightCounts = inflight_counts(Tasks),
    arweave_metrics:gauge_set(
        sync_scheduler_driven_peers,
        map_size(DrivenPeers)
    ),
    Peers = lists:usort(
        active_peers(StoreStates, Tasks, Footprints, PeerQueues) ++
            maps:keys(DrivenPeers)
    ),
    PeerState2 = arweave_sync_peer:tick(
        Peers, InflightCounts, DrivenPeers, NowMs, PeerState
    ),
    State2#state{peer_state = PeerState2, driven_peers = #{}}.

%% @doc Sample each store's drain capacity. arweave_sync_store owns the actual
%% chunk-cache backlog signal, sampling window, and capacity estimate.
sample_store_drain_rates(State, NowMs) ->
    State#state{
        stores = arweave_sync_store:sample_drain_rates(
            NowMs, State#state.stores
        )
    }.

%% @doc Fetching-task count per peer. Local writes remain in the per-store
%% backlog but no longer consume network concurrency after their fetch completes.
inflight_counts(Tasks) ->
    maps:fold(
        fun
            (_TaskRef, #task{state = fetching, peer = Peer}, Acc) ->
                arweave_util:increment_map_value(Peer, Acc);
            (_TaskRef, _Task, Acc) ->
                Acc
        end,
        #{},
        Tasks
    ).

register_started([], Started, Tasks) ->
    {lists:reverse(Started), Tasks};
register_started([Task | Rest], Started, Tasks) ->
    TaskRef = {self(), make_ref()},
    Task2 = Task#task{task_ref = TaskRef, state = fetching},
    register_started(
        Rest,
        [Task2 | Started],
        maps:put(TaskRef, Task2, Tasks)
    ).

%% @doc Remove the worker monitor. A task still in `fetching` did not publish a
%% terminal result, so release it; tasks in a storage state remain until their
%% write completes.
worker_exited(MonitorRef, Reason, State) ->
    case maps:take(MonitorRef, State#state.monitor_index) of
        error ->
            State;
        {{TaskRef, _WorkerPID}, MonitorIndex2} ->
            State2 = State#state{monitor_index = MonitorIndex2},
            case maps:get(TaskRef, State2#state.tasks, not_found) of
                #task{} = Task ->
                    log_if_crash(Task, Reason),
                    finish_task(Task, State2);
                not_found ->
                    State2
            end
    end.

on_task_fetch_completed(TaskRef, BytesFetched, FetchTiming, State) ->
    #state{tasks = Tasks} = State,
    case maps:get(TaskRef, Tasks, not_found) of
        #task{state = writing} ->
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
            PeerState2 = arweave_sync_peer:record_result(
                Task#task.peer, BytesFetched, FetchTiming, State#state.peer_state
            ),
            State2 = State#state{peer_state = PeerState2},
            State3 = State2#state{
                download_limit =
                    arweave_sync_download_limit:restore(
                        State2#state.download_limit, Refund
                    )
            },
            case {BytesFetched, Task#task.state} of
                {0, _} ->
                    finish_task(Task, State3);
                {_, fetching} ->
                    %% Keep the store claim through the asynchronous write.
                    %% The footprint's entropy slot stays held too: a
                    %% peer-packed chunk is unpacked with those entropies
                    %% after the handoff, and on_task_unpacked releases it.
                    put_task(Task#task{state = writing}, State3);
                {_, write_complete} ->
                    finish_task(Task, State3);
                {_, writing} ->
                    State3
            end;
        not_found ->
            State
    end.

%% @doc Release the footprint's entropy slot once the chunk is in unpacked
%% form; the store claim stays until the write completes.
on_task_unpacked(TaskRef, State) ->
    case maps:get(TaskRef, State#state.tasks, not_found) of
        #task{footprint = none} ->
            State;
        #task{} = Task ->
            Footprints2 = arweave_sync_footprint:task_completed(
                Task, State#state.footprints
            ),
            put_task(
                Task#task{footprint = none},
                State#state{footprints = Footprints2}
            );
        not_found ->
            State
    end.

on_task_write_completed(TaskRef, State) ->
    case maps:get(TaskRef, State#state.tasks, not_found) of
        #task{state = Status, store_id = StoreID} when
            Status =:= fetching; Status =:= writing
        ->
            on_task_write_finished(TaskRef, record_store_drain(StoreID, State));
        _ ->
            State
    end.

on_task_write_finished(TaskRef, State) ->
    #state{tasks = Tasks} = State,
    case maps:get(TaskRef, Tasks, not_found) of
        #task{state = fetching} = Task ->
            put_task(Task#task{state = write_complete}, State);
        #task{state = writing} = Task ->
            finish_task(Task#task{state = write_complete}, State);
        #task{state = write_complete} ->
            State;
        not_found ->
            State
    end.

record_store_drain(StoreID, State) ->
    State#state{
        stores = arweave_sync_store:record_write_completed(
            StoreID, State#state.stores
        )
    }.

put_task(Task, State) ->
    #task{task_ref = TaskRef} = Task,
    #state{tasks = Tasks} = State,
    State#state{tasks = maps:put(TaskRef, Task, Tasks)}.

finish_task(#task{state = writing}, State) ->
    State;
finish_task(Task, State) ->
    #task{task_ref = TaskRef} = Task,
    #state{
        tasks = Tasks,
        footprints = Footprints
    } = State,
    Footprints2 = arweave_sync_footprint:task_completed(Task, Footprints),
    State2 = release_claim(Task#task.store_id, Task#task.offset, State),
    State2#state{
        tasks = maps:remove(TaskRef, Tasks),
        footprints = Footprints2
    }.

release_claim(StoreID, Offset, State) ->
    State#state{
        stores = arweave_sync_store:release_claim(
            StoreID, Offset, State#state.stores
        )
    }.

%%%===================================================================
%%% Helpers.
%%%===================================================================

log_if_crash(_Task, normal) ->
    ok;
log_if_crash(#task{peer = Peer} = Task, Reason) ->
    ?LOG_WARNING([
        {event, sync_worker_crash},
        {module, ?MODULE},
        %% A task carries no peer until dispatch binds one, and a crash can
        %% reach here before that.
        {peer, format_task_peer(Peer)},
        {start_offset, Task#task.offset},
        {end_offset, Task#task.offset + ?DATA_CHUNK_SIZE},
        {reason, io_lib:format("~p", [Reason])}
    ]).

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
    StoreIDs = arweave_sync_store_sweeper:store_ids(),
    {TasksByStore, TasksByPeer} = task_counts_by_stage(Tasks),
    QueuedByPeer = peer_queue_counts(PeerQueues),
    lists:foreach(
        fun(StoreID) ->
            emit_claim_metrics(StoreID, State),
            arweave_metrics:gauge_set(
                sync_tasks_by_store,
                [queued, StoreID],
                queued_task_count_by_store(StoreID, State)
            ),
            arweave_metrics:gauge_set(
                sync_tasks_by_store,
                [fetching, StoreID],
                maps:get({fetching, StoreID}, TasksByStore, 0)
            ),
            arweave_metrics:gauge_set(
                sync_tasks_by_store,
                [writing, StoreID],
                maps:get({writing, StoreID}, TasksByStore, 0)
            )
        end,
        StoreIDs
    ),
    emit_store_capacity_metrics(StoreIDs, State),
    arweave_sync_footprint:emit_metrics(Footprints),
    ClaimedByPeerStore = claimed_chunks_by_peer_store(Tasks, PeerQueues),
    emit_claim_ownership_metrics(ClaimedByPeerStore, StoreIDs),
    Peers = active_peers(StoreStates, Tasks, Footprints, PeerQueues),
    CurLabels = lists:flatmap(
        fun(Peer) ->
            Label = arweave_util:format_peer(Peer),
            QueuedLabels = [queued, Label],
            FetchingLabels = [fetching, Label],
            WritingLabels = [writing, Label],
            arweave_metrics:gauge_set(
                sync_tasks_by_peer,
                QueuedLabels,
                maps:get(Peer, QueuedByPeer, 0)
            ),
            arweave_metrics:gauge_set(
                sync_tasks_by_peer,
                FetchingLabels,
                maps:get({fetching, Peer}, TasksByPeer, 0)
            ),
            arweave_metrics:gauge_set(
                sync_tasks_by_peer,
                WritingLabels,
                maps:get({writing, Peer}, TasksByPeer, 0)
            ),
            [QueuedLabels, FetchingLabels, WritingLabels]
        end,
        Peers
    ),
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
            arweave_metrics:gauge_set(
                sync_store_pipeline_limit_chunks,
                [StoreID],
                arweave_sync_store:pipeline_limit(StoreID, StoreStates)
            ),
            case arweave_sync_store:drain_rate(StoreID, StoreStates) of
                undefined ->
                    ok;
                ChunksPerSecond ->
                    arweave_metrics:gauge_set(
                        store_drain_rate_bytes_per_second,
                        [StoreID],
                        ChunksPerSecond * ?DATA_CHUNK_SIZE
                    )
            end
        end,
        StoreIDs
    ).

emit_claim_metrics(StoreID, State) ->
    StoreStates = State#state.stores,
    #store_info{label = Label} = arweave_storage:store_info(StoreID),
    arweave_metrics:gauge_set(
        sync_claimed_bytes_by_store,
        [Label],
        arweave_sync_store:claimed_chunks(StoreID, StoreStates) *
            ?DATA_CHUNK_SIZE
    ),
    arweave_metrics:gauge_set(
        sync_claim_headroom_bytes_by_store,
        [Label],
        arweave_sync_store:admission_headroom(StoreID, StoreStates) *
            ?DATA_CHUNK_SIZE
    ).

queued_task_count_by_store(StoreID, State) ->
    arweave_sync_store:queued_task_count(StoreID, State#state.stores).

task_counts_by_stage(Tasks) ->
    maps:fold(
        fun
            (
                _TaskRef,
                #task{
                    state = Stage,
                    peer = Peer,
                    store_id = StoreID
                },
                {ByStore, ByPeer}
            ) when
                Stage =:= fetching; Stage =:= writing
            ->
                {
                    arweave_util:increment_map_value({Stage, StoreID}, ByStore),
                    arweave_util:increment_map_value({Stage, Peer}, ByPeer)
                };
            (_TaskRef, _Task, Counts) ->
                Counts
        end,
        {#{}, #{}},
        Tasks
    ).

%% Include both runnable peer-queue claims and nonterminal active tasks.
claimed_chunks_by_peer_store(Tasks, PeerQueues) ->
    Active = maps:fold(
        fun(_TaskRef, Task, Acc) ->
            Key = {Task#task.peer, Task#task.store_id},
            arweave_util:increment_map_value(Key, Acc)
        end,
        #{},
        Tasks
    ),
    lists:foldl(
        fun(Task, Acc) ->
            Key = {Task#task.peer, Task#task.store_id},
            arweave_util:increment_map_value(Key, Acc)
        end,
        Active,
        peer_queued_tasks(PeerQueues)
    ).

%% @doc Distinct peers across queued, nonterminal, and bound footprint work.
%% This is the active-peer set used to allocate caps and emit per-peer metrics.
active_peers(StoreStates, Tasks, Footprints, PeerQueues) ->
    FromQueues = arweave_sync_store:peers(StoreStates),
    FromTasks = [Task#task.peer || Task <- maps:values(Tasks)],
    FromFootprints = arweave_sync_footprint:peers(Footprints),
    lists:usort(
        FromQueues ++ FromTasks ++ FromFootprints ++
            maps:keys(PeerQueues)
    ).

emit_claim_ownership_metrics(ClaimedByPeerStore, StoreIDs) ->
    PeersByStore = maps:fold(
        fun({Peer, StoreID}, _Chunks, Acc) ->
            maps:update_with(
                StoreID,
                fun(Peers) -> sets:add_element(Peer, Peers) end,
                sets:from_list([Peer]),
                Acc
            )
        end,
        #{},
        ClaimedByPeerStore
    ),
    lists:foreach(
        fun(StoreID) ->
            arweave_metrics:gauge_set(
                sync_claimed_peers_by_store,
                [StoreID],
                sets:size(maps:get(StoreID, PeersByStore, sets:new()))
            )
        end,
        StoreIDs
    ),
    CurLabels = maps:fold(
        fun({Peer, StoreID}, Chunks, Acc) ->
            Labels = [
                arweave_util:format_peer(Peer),
                (arweave_storage:store_info(StoreID))#store_info.label
            ],
            arweave_metrics:gauge_set(
                sync_claimed_chunks_by_peer_store,
                Labels,
                Chunks
            ),
            [Labels | Acc]
        end,
        [],
        ClaimedByPeerStore
    ),
    prune_stale_labels(sync_claimed_chunks_by_peer_store, CurLabels).

prune_stale_labels(Name, CurLabels) ->
    Existing = lists:map(
        fun({Labels, _MetricValue}) ->
            lists:map(
                fun({_LabelName, Value}) -> Value end,
                Labels
            )
        end,
        arweave_metrics:gauge_values(Name)
    ),
    lists:foreach(
        fun(Labels) -> arweave_metrics:gauge_remove(Name, Labels) end,
        Existing -- CurLabels
    ).

%%%===================================================================
%%% Test support.
%%%===================================================================

-ifdef(AR_TEST).

%% Focused selection tests exercise binding and activation without spawning
%% workers; production registers and spawns Dispatch#dispatch.tasks_to_start.

%% @doc The number of running fetch workers.
inflight_count() ->
    gen_server:call(?MODULE, inflight_count).

-endif.
