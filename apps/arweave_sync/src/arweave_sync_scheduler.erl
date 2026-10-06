%%% @doc The sync scheduler: one process that decides which chunks to fetch
%%% from which peers, starts those fetches, and follows each one until its
%%% store writes the chunk.
%%%
%%% Terms:
%%% - unit of work: a task (one chunk) or a footprint reservation (the
%%%   unsynced chunks of one footprint, which share entropy);
%%% - claim: a store's hold on the chunks a unit of work covers, so that no
%%%   one else fetches them for the store;
%%% - peer queue: the tasks bound to one peer, waiting to be fetched;
%%% - entropy slot: room for one footprint's entropy in the entropy cache; a
%%%   footprint reservation holds one while its chunks are fetched;
%%% - dispatch plan: the copies of the store, footprint and peer state that
%%%   one dispatch pass changes and then commits.
%%%
%%% The scheduler runs in a cycle:
%%% 1. Admission (claim_and_enqueue/2), when arweave_sync_sweeper offers the
%%%    units of work that arweave_sync_chunk_picker built for one store. For
%%%    each unit, up to the store's claim limit:
%%%    - claim its chunks in the store;
%%%    - add it to the store's work queue.
%%% 2. Dispatch (dispatch/1), after each event that adds work or frees up
%%%    room, and on each tick. Each pass builds a dispatch plan:
%%%    - Bind: take units of work from the least loaded stores and bind each
%%%      one to the least loaded peer that offers it. A footprint reservation
%%%      first takes an entropy slot, then moves a batch of its chunks into
%%%      the peer's queue as tasks.
%%%    - Start: take tasks from the peer queues while the peer, store and
%%%      download limits allow.
%%%    - Bind again, into the room those fetches freed in the peer queues.
%%%    - Commit: spawn an arweave_sync_fetch_worker for each started task and
%%%      write the plan back.
%%% 3. Results (the report_* functions), as each fetch worker and store write
%%%    reports back: add the fetch result to the peer, and release the task's
%%%    claim and entropy slot once the fetch fails or the write finishes.
%%% 4. Tick (every TICK_INTERVAL_MS): sample the store write rates, update the
%%%    peer limits and publish metrics.
-module(arweave_sync_scheduler).

-ifdef(AR_TEST).
-export([
    active_peers/1,
    admit_work/3,
    bind_to_peers/1,
    bind_work/2,
    on_task_fetch_completed/4,
    on_task_unpacked/2,
    on_task_write_completed/2,
    update_driven/2,
    resolve_work/2,
    snapshot/1,
    start_fetches/2,
    terminate_workers/1,
    tick/2,
    worker_exited/3
]).
-endif.

-behaviour(gen_server).

-export([
    start_link/0,
    register_workers/0,
    %% Admission.
    admission_headroom/1,
    claim_and_enqueue/2,
    %% Task lifecycle.
    report_fetch_completed/3,
    report_unpacked/1,
    report_write_completed/1, report_write_completed/2,
    report_write_failed/1,
    %% Tick.
    tick_interval_ms/0
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
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Supervisor child spec for the scheduler.
register_workers() ->
    [?CHILD(?MODULE, worker)].

%%%===================================================================
%%% Public interface: admission.
%%%===================================================================

%% @doc Return how many more chunks the store can claim now, so the picker
%% avoids building work that claim_and_enqueue/2 would refuse.
admission_headroom(StoreID) ->
    case catch gen_server:call(?MODULE, {admission_headroom, StoreID}, infinity) of
        {'EXIT', _Reason} -> 0;
        Headroom -> Headroom
    end.

%% @doc Claim and queue as much of the offered work as fits. The call is
%% synchronous, so the claims cover this batch before the sweeper offers any
%% overlapping range.
claim_and_enqueue(_StoreID, []) ->
    {ok, 0};
claim_and_enqueue(StoreID, Work) ->
    case
        catch gen_server:call(
            ?MODULE,
            {claim_and_enqueue, StoreID, Work},
            infinity
        )
    of
        {'EXIT', _Reason} -> blocked;
        Reply -> Reply
    end.

%%%===================================================================
%%% Public interface: task lifecycle.
%%%===================================================================

%% @doc Report how many bytes the worker fetched and how long the task's
%% network attempts took.
report_fetch_completed({Pid, _Ref} = TaskRef, BytesFetched, FetchTiming) when
    is_pid(Pid)
->
    catch gen_server:cast(
        Pid,
        {task_fetch_completed, TaskRef, BytesFetched, FetchTiming}
    ),
    ok.

%% @doc Report that a handed-off chunk is unpacked, so the chunk no longer needs
%% its footprint's entropy.
report_unpacked(undefined) ->
    ok;
report_unpacked({Pid, _Ref} = TaskRef) when is_pid(Pid) ->
    catch gen_server:cast(Pid, {task_unpacked, TaskRef}),
    ok;
report_unpacked(TaskRef) ->
    catch gen_server:cast(?MODULE, {task_unpacked, TaskRef}),
    ok.

%% @doc Report that the write of a chunk fetched by a scheduler task has
%% finished.
report_write_completed(undefined) ->
    ok;
report_write_completed({Pid, _Ref} = TaskRef) when is_pid(Pid) ->
    catch gen_server:cast(Pid, {task_write_completed, TaskRef}),
    ok;
report_write_completed(TaskRef) ->
    catch gen_server:cast(?MODULE, {task_write_completed, TaskRef}),
    ok.

%% @doc Report that ar_data_sync finished processing one handed-off chunk.
report_write_completed(StoreID, undefined) ->
    gen_server:cast(?MODULE, {store_write_completed, StoreID});
report_write_completed(_StoreID, TaskRef) ->
    report_write_completed(TaskRef).

%% @doc Report a failed handoff, so the scheduler releases the task without
%% counting it toward the store's write rate.
report_write_failed({PID, _Ref} = TaskRef) when is_pid(PID) ->
    gen_server:cast(PID, {task_write_failed, TaskRef}).

%%%===================================================================
%%% Public interface: tick.
%%%===================================================================

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
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
    ?LOG_INFO([{event, init}, {module, ?MODULE}]),
    {ok, _} = ?DEP(clock):send_after(
        tick_interval_ms(),
        self(),
        scheduler_tick
    ),
    {ok, #state{}}.

handle_call(get_state, _From, State) ->
    {reply, {ok, State}, State};
handle_call(ping, _From, State) ->
    %% The reply shows that the scheduler has handled every cast sent before
    %% the ping, which tests use to flush the pipeline.
    {reply, pong, State};
handle_call(inflight_count, _From, State) ->
    {reply, map_size(State#state.monitor_index), State};
handle_call({admission_headroom, StoreID}, _From, State) ->
    HeadroomChunks = arweave_sync_store:admission_headroom(
        StoreID, State#state.stores
    ),
    {reply, HeadroomChunks, State};
handle_call({claim_and_enqueue, StoreID, Work}, _From, State) ->
    {Result, State2} = admit(StoreID, Work, State),
    {reply, Result, schedule_dispatch(State2)};
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
    {noreply, schedule_dispatch(record_store_write(StoreID, State))};
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, _PID, Reason}, State) ->
    {noreply, schedule_dispatch(worker_exited(Ref, Reason, State))};
handle_info(scheduler_tick, State) ->
    {ok, _} = ?DEP(clock):send_after(
        tick_interval_ms(),
        self(),
        scheduler_tick
    ),
    State2 = dispatch(tick(State, ?DEP(clock):monotonic_ms())),
    emit_metrics(State2),
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

%% @doc Admit the work the sweeper offers for one store, in order. Return
%% {ok, ClaimedChunks}, or blocked when the store's claim limit refused any
%% unit of work.
admit(StoreID, Work, State) ->
    admit(StoreID, Work, 0, false, State).

admit(_StoreID, [], Admitted, Blocked, State) ->
    Result =
        case Blocked of
            true -> blocked;
            false -> {ok, Admitted}
        end,
    {Result, State};
admit(StoreID, [Unit | Rest], Admitted, Blocked, State) ->
    case admit_work(StoreID, Unit, State) of
        {blocked, State2} ->
            %% Keep going: a later unit may need no new claim, such as a chunk
            %% already claimed or a footprint already reserved.
            admit(StoreID, Rest, Admitted, true, State2);
        {ok, ClaimedChunks, State2} ->
            admit(StoreID, Rest, Admitted + ClaimedChunks, Blocked, State2)
    end.

%% @doc Claim the chunks of one unit of work in the store and add the unit to
%% the store's work queue, within the store's claim limit.
admit_work(StoreID, #task{} = Task, State) ->
    case
        arweave_sync_store:admit(
            StoreID, Task#task{store_id = StoreID}, State#state.stores
        )
    of
        {ok, ClaimedChunks, Stores} ->
            {ok, ClaimedChunks, State#state{stores = Stores}};
        blocked ->
            {blocked, State}
    end;
admit_work(StoreID, Reservation, State) ->
    #state{stores = Stores, footprints = Footprints} = State,
    case arweave_sync_footprint:admit(Reservation, Footprints) of
        {ok, 0, Footprints2} ->
            {ok, 0, State#state{footprints = Footprints2}};
        {ok, ClaimedChunks, Footprints2} ->
            case arweave_sync_store:admit(StoreID, Reservation, Stores) of
                {ok, ClaimedChunks, Stores2} ->
                    {ok, ClaimedChunks, State#state{
                        stores = Stores2,
                        footprints = Footprints2
                    }};
                blocked ->
                    %% A reservation is speculative, so skip it instead of
                    %% blocking. The sweep can then move on to existing
                    %% reservations, whose later offers may bring better
                    %% sources. A blocked task still holds the sweep at the
                    %% head of its queue.
                    {ok, 0, State}
            end
    end.

%%%===================================================================
%%% Dispatch: pass.
%%%===================================================================

schedule_dispatch(#state{dispatch_scheduled = true} = State) ->
    State;
schedule_dispatch(State) ->
    gen_server:cast(self(), dispatch),
    State#state{dispatch_scheduled = true}.

%% @doc Run one dispatch pass: bind work to peer queues, start fetches from
%% them, bind more work into the room those fetches freed, and spawn a worker
%% for each fetch.
dispatch(State0) ->
    %% Grow the download limit's byte balance for the time since the last
    %% pass.
    DownloadLimit = arweave_sync_download_limit:refill(State0#state.download_limit),
    State1 = State0#state{download_limit = DownloadLimit},
    %% Copy the store, footprint and peer state that this pass changes.
    Plan0 = snapshot(State1),
    %% Bind work from the work queues to peer queues, up to each peer's queue
    %% length.
    Plan1 = bind_to_peers(Plan0),
    %% Note which peers are already at their cap while the download limit
    %% and the chunk cache have room.
    State2 = update_driven(State1, Plan1),
    %% Start fetches from the peer queues within the peer caps, the store cache
    %% limits, the download limit and the chunk cache.
    {State3, Plan2} = start_fetches(State2, Plan1),
    %% Return the work that found no peer with room to the work queues, then
    %% try to bind it again, now that the started fetches have freed room in
    %% the peer queues.
    Plan3 = bind_to_peers(Plan2#dispatch_plan{
        stores = arweave_sync_store:refresh_work(
            Plan2#dispatch_plan.footprints, Plan2#dispatch_plan.stores
        )
    }),
    %% Note the peers that the just-started fetches filled to their cap.
    State4 = update_driven(State3, Plan3),
    %% Spawn the fetch workers and write the pass's changes back.
    State5 = spawn_workers(State4, Plan3),
    %% Schedule a pass for when the download limit refills, if it holds back
    %% queued work.
    maybe_schedule_download_limit_wakeup(
        commit_plan(State5, Plan3)
    ).

%% @doc Snapshot the stores, footprints and peers for one dispatch pass, and
%% split each peer's queue among the stores it serves.
snapshot(State) ->
    Plan0 = #dispatch_plan{
        stores = arweave_sync_store:snapshot(
            State#state.footprints,
            State#state.tasks,
            arweave_sync_peer:queued_tasks(State#state.peer_state),
            State#state.stores
        ),
        worker_count = map_size(State#state.monitor_index),
        footprints = arweave_sync_footprint:snapshot(State#state.footprints),
        peers = arweave_sync_peer:snapshot(
            State#state.tasks,
            State#state.peer_state
        )
    },
    StoresByPeer =
        arweave_sync_store:stores_with_room_by_peer(Plan0#dispatch_plan.stores),
    PeerPlan = arweave_sync_peer:set_store_task_targets(
        StoresByPeer, Plan0#dispatch_plan.peers
    ),
    Plan0#dispatch_plan{peers = PeerPlan}.

%% @doc Record which peers are driven: at their cap with tasks still queued,
%% while the download limit and the chunk cache have room.
update_driven(State, Plan) ->
    GatesOpen =
        arweave_sync_download_limit:has_capacity(
            State#state.download_limit
        ) andalso
            not is_chunk_cache_full(Plan),
    State#state{
        peer_state = arweave_sync_peer:update_driven(
            Plan#dispatch_plan.peers, GatesOpen, State#state.peer_state
        )
    }.

%% @doc Return whether the chunk cache is full, counting one chunk for each
%% fetch in flight.
is_chunk_cache_full(#dispatch_plan{worker_count = WorkerCount}) ->
    ?DEP(chunk_cache):cached_size() + WorkerCount >= ?DEP(chunk_cache):limit().

commit_plan(State, Plan) ->
    #dispatch_plan{
        stores = StorePlan,
        footprints = FootprintPlan,
        peers = PeerPlan
    } = Plan,
    State#state{
        stores = arweave_sync_store:commit_plan(StorePlan),
        footprints = arweave_sync_footprint:commit_plan(FootprintPlan),
        peer_state = arweave_sync_peer:commit_plan(
            PeerPlan, State#state.peer_state
        )
    }.

maybe_schedule_download_limit_wakeup(State) ->
    #state{
        stores = StoreStates,
        footprints = Footprints,
        peer_state = PeerState,
        download_limit = DownloadLimit
    } = State,
    HasWork =
        not arweave_sync_store:queues_empty(StoreStates) orelse
            arweave_sync_footprint:has_bound_work(Footprints) orelse
            arweave_sync_peer:has_queued_tasks(PeerState),
    DownloadLimit2 = arweave_sync_download_limit:maybe_schedule_wakeup(
        DownloadLimit, HasWork, tick_interval_ms()
    ),
    State#state{download_limit = DownloadLimit2}.

%%%===================================================================
%%% Dispatch: binding.
%%%===================================================================

%% @doc Bind work to peer queues, taking each unit from the least-loaded store
%% that has work and room, until no such store is left.
bind_to_peers(Plan) ->
    maybe
        {ok, StoreID} ?=
            arweave_sync_store:best_store(Plan#dispatch_plan.stores),
        {QueuedWork, StorePlan} ?=
            arweave_sync_store:pop_work(
                StoreID, Plan#dispatch_plan.stores
            ),
        Plan2 = Plan#dispatch_plan{stores = StorePlan},
        Work = resolve_work(QueuedWork, Plan2),
        bind_to_peers(bind_work(Work, Plan2))
    else
        none -> Plan
    end.

%% @doc Return the work for a work queue entry: the task itself, the
%% footprint's reservation, or none if this pass has put the footprint aside.
resolve_work(#task{} = Task, _Plan) ->
    #work{
        unit = Task,
        store_id = Task#task.store_id,
        sources = Task#task.sources
    };
resolve_work(#footprint{} = Footprint, Plan) ->
    FootprintPlan = Plan#dispatch_plan.footprints,
    case arweave_sync_footprint:reservation(Footprint, FootprintPlan) of
        none ->
            none;
        Reservation ->
            #work{
                unit = Reservation,
                store_id = arweave_sync_footprint:store_id(Reservation),
                sources = arweave_sync_footprint:sources(Reservation)
            }
    end.

%% @doc Bind one unit of work to its best source peer, if one has room.
bind_work(none, Plan) ->
    Plan;
bind_work(Work, Plan) ->
    case best_source(Work, Plan) of
        none ->
            Plan;
        {ok, Source} ->
            case Work#work.unit of
                #task{} = Task ->
                    bind_task(Task, Source, Plan);
                #footprint_reservation{} = Reservation ->
                    bind_footprint(Reservation, Source, Plan)
            end
    end.

%% @doc Return the least-loaded peer that offers the work and has room in its
%% queue for the work's store; a bound footprint reservation can only use its
%% own peer.
best_source(Work, Plan) ->
    #work{
        unit = Unit,
        store_id = StoreID,
        sources = Sources
    } = Work,
    #dispatch_plan{
        footprints = FootprintPlan,
        peers = PeerPlan
    } = Plan,
    ReadySources = lists:filter(
        fun(Source) ->
            arweave_sync_footprint:is_source_compatible(
                Unit, Source, FootprintPlan
            ) andalso
                arweave_sync_peer:has_capacity(Unit, Source, PeerPlan)
        end,
        Sources
    ),
    arweave_sync_peer:best_source(StoreID, ReadySources, PeerPlan).

bind_task(Task0, Source, Plan) ->
    #task_source{peer = Peer, footprint = Footprint} = Source,
    Task = Task0#task{peer = Peer, footprint = Footprint},
    StorePlan = arweave_sync_store:bind_task(
        Task0, Plan#dispatch_plan.stores
    ),
    PeerPlan = arweave_sync_peer:enqueue_tasks(
        Peer, [Task], Plan#dispatch_plan.peers
    ),
    Plan#dispatch_plan{
        stores = StorePlan,
        peers = PeerPlan
    }.

%%%===================================================================
%%% Dispatch: footprints.
%%%===================================================================

%% @doc Bind a batch of the footprint's chunks to the source peer's queue once
%% the footprint holds an entropy slot.
bind_footprint(Reservation, Source, Plan) ->
    case ensure_entropy_capacity(Reservation, Source, Plan) of
        {deferred, Plan2} ->
            Plan2;
        {ready, Plan2} ->
            build_footprint_batch(Reservation, Source, Plan2)
    end.

%% @doc Make sure the footprint has an entropy slot before fetching from the
%% source, competing with the bound footprints for one if needed. Each
%% footprint and peer pair needs cached entropy, and more pairs than the cache
%% can hold would keep evicting and regenerating entropy instead of fetching
%% chunks.
ensure_entropy_capacity(Reservation, Source, Plan) ->
    Footprint = arweave_sync_footprint:key(Reservation),
    FootprintPlan = Plan#dispatch_plan.footprints,
    case
        arweave_sync_footprint:has_entropy_capacity(
            Footprint, Source, FootprintPlan
        )
    of
        true ->
            {ready, Plan};
        false ->
            CandidatePriority =
                footprint_priority(Reservation, Source, Plan),
            BoundPriorities = bound_footprint_priorities(Plan),
            {Result, FootprintPlan2} =
                arweave_sync_footprint:compete_for_entropy_capacity(
                    Footprint,
                    CandidatePriority,
                    BoundPriorities,
                    FootprintPlan
                ),
            {Result, Plan#dispatch_plan{footprints = FootprintPlan2}}
    end.

bound_footprint_priorities(Plan) ->
    lists:map(
        fun({Reservation, Source}) ->
            Priority = footprint_priority(Reservation, Source, Plan),
            {Priority, arweave_sync_footprint:key(Reservation)}
        end,
        arweave_sync_footprint:bound_candidates(Plan#dispatch_plan.footprints)
    ).

%% @doc Rank a footprint competing for an entropy slot; the lowest rank wins.
%% Footprints compare by, in order:
%% - the entropy slots their store already holds, fewest first;
%% - whether their store can take more work. A footprint with fetches in
%%   flight counts as able to, even when those fetches fill its store's
%%   pipeline;
%% - their peer's priority.
footprint_priority(Reservation, Source, Plan) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    StoreAvailable =
        arweave_sync_footprint:is_busy(Reservation) orelse
            arweave_sync_store:has_capacity(StoreID, Plan#dispatch_plan.stores),
    arweave_sync_footprint:slot_priority(
        arweave_sync_footprint:bound_count(
            StoreID, Plan#dispatch_plan.footprints
        ),
        StoreAvailable,
        arweave_sync_peer:priority(
            Reservation, Source, Plan#dispatch_plan.peers
        )
    ).

%% @doc Bind a batch of the footprint's chunks to the peer, up to the store's
%% pipeline limit and the room in the peer's queue.
build_footprint_batch(Reservation, Source, Plan) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    %% The footprint's chunks that the peer advertises and that the store
    %% lacked when the sweeper offered the reservation.
    #task_source{peer = Peer, intervals = SourceIntervals} = Source,
    %% A large batch uses much of the footprint's cached entropy at once.
    %% Keeping it within the store's pipeline limit lets the store write the
    %% chunks as they arrive.
    Limit = min(
        arweave_sync_peer:queue_capacity(Peer, Plan#dispatch_plan.peers),
        arweave_sync_store:pipeline_limit(StoreID, Plan#dispatch_plan.stores)
    ),
    %% Leave out the chunks the store has claimed, such as those that
    %% earlier batches already turned into tasks.
    AvailableIntervals = arweave_sync_store:unclaimed_intervals(
        StoreID, SourceIntervals, Plan#dispatch_plan.stores
    ),
    {FootprintPlan2, Tasks, BoundReservation} =
        arweave_sync_footprint:build_batch(
            Reservation,
            Source,
            AvailableIntervals,
            Limit,
            Plan#dispatch_plan.footprints
        ),
    StorePlan2 =
        case Reservation#footprint_reservation.state of
            queued ->
                arweave_sync_store:bind_footprint(
                    Reservation, Plan#dispatch_plan.stores
                );
            bound ->
                Plan#dispatch_plan.stores
        end,
    BoundTasks = [Task#task{peer = Peer} || Task <- Tasks],
    StorePlan3 = arweave_sync_store:enqueue_bound_tasks(
        BoundTasks, StorePlan2
    ),
    StorePlan4 = arweave_sync_store:add_reservations(
        [BoundReservation], StorePlan3
    ),
    PeerPlan = arweave_sync_peer:enqueue_tasks(
        Peer, BoundTasks, Plan#dispatch_plan.peers
    ),
    Plan#dispatch_plan{
        footprints = FootprintPlan2,
        stores = StorePlan4,
        peers = PeerPlan
    }.

%%%===================================================================
%%% Dispatch: starting fetches.
%%%===================================================================

start_fetches(State, Plan) ->
    #state{download_limit = DownloadLimit} = State,
    #dispatch_plan{stores = Stores, peers = PeerPlan} = Plan,
    StoreRank =
        fun(StoreID) -> arweave_sync_store:rank(StoreID, Stores) end,
    maybe
        false ?= is_chunk_cache_full(Plan),
        true ?= arweave_sync_download_limit:has_capacity(DownloadLimit),
        {Task, PeerPlan2} ?=
            arweave_sync_peer:take_fetch(StoreRank, PeerPlan),
        {State2, Plan2} =
            start_fetch(Task, State, Plan#dispatch_plan{peers = PeerPlan2}),
        start_fetches(State2, Plan2)
    else
        _ -> {State, Plan}
    end.

start_fetch(Task, State, Plan) ->
    #dispatch_plan{
        worker_count = WorkerCount,
        tasks_to_start = TasksToStart
    } = Plan,
    StorePlan = arweave_sync_store:start_bound_task(
        Task, Plan#dispatch_plan.stores
    ),
    Plan2 = Plan#dispatch_plan{
        stores = StorePlan,
        worker_count = WorkerCount + 1,
        tasks_to_start = [Task | TasksToStart]
    },
    State2 = State#state{
        download_limit =
            arweave_sync_download_limit:consume(
                State#state.download_limit, ?DATA_CHUNK_SIZE
            )
    },
    {State2, Plan2}.

spawn_workers(State, Plan) ->
    #dispatch_plan{tasks_to_start = TasksToStart} = Plan,
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

register_worker(TaskRef, WorkerPID, MonitorRef, State) ->
    State#state{
        monitor_index =
            maps:put(MonitorRef, {TaskRef, WorkerPID}, State#state.monitor_index)
    }.

spawn_worker(Task) ->
    spawn_monitor(arweave_sync_fetch_worker, run, [Task]).

%%%===================================================================
%%% Tick.
%%%===================================================================

%% @doc Sample the store write rates and update the limits of the peers with
%% work.
tick(State, NowMs) ->
    State2 = State#state{
        stores =
            arweave_sync_store:sample_write_rates(NowMs, State#state.stores)
    },
    State2#state{
        peer_state = arweave_sync_peer:tick(
            active_peers(State2), NowMs, State2#state.peer_state
        )
    }.

%% @doc Return the peers with work.
active_peers(State) ->
    #state{
        stores = Stores,
        tasks = Tasks,
        footprints = Footprints,
        peer_state = PeerState
    } = State,
    lists:usort(
        arweave_sync_store:source_peers(Stores) ++
            [Task#task.peer || Task <- maps:values(Tasks)] ++
            arweave_sync_footprint:peers(Footprints) ++
            maps:keys(arweave_sync_peer:queue_lengths(PeerState))
    ).

%% @doc Publish the scheduler's metrics; only the tick calls this, because it
%% walks every task.
emit_metrics(State) ->
    #state{
        tasks = Tasks,
        monitor_index = MonitorIndex,
        stores = Stores,
        footprints = Footprints,
        peer_state = PeerState
    } = State,
    arweave_sync_metrics:publish_stores(
        arweave_sync_sweeper:store_ids(),
        Stores,
        Tasks,
        arweave_sync_peer:queued_tasks(PeerState)
    ),
    arweave_sync_metrics:publish_peers(
        active_peers(State),
        Tasks,
        arweave_sync_peer:queue_lengths(PeerState),
        map_size(MonitorIndex)
    ),
    arweave_sync_metrics:publish_footprints(
        arweave_sync_footprint:bound_count(Footprints),
        arweave_sync_footprint:max_active()
    ).

%%%===================================================================
%%% Task lifecycle.
%%%===================================================================

%% @doc Remove the worker's monitor. A task still in `fetching` will never get
%% a fetch result, so it is released; a task in `writing` stays until its
%% write finishes.
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
    arweave_lib_util:format_peer(Peer).

%% @doc Record a fetch result: credit the peer, return the undelivered bytes to
%% the download limit, and move the task to writing, or finish it if the fetch
%% failed or the write has already finished.
on_task_fetch_completed(TaskRef, BytesFetched, FetchTiming, State) ->
    #state{tasks = Tasks} = State,
    case maps:get(TaskRef, Tasks, not_found) of
        #task{state = writing} ->
            %% The fetch result is already recorded, so ignore this duplicate
            %% rather than credit the peer twice.
            State;
        #task{} = Task ->
            %% Give the bytes the fetch did not deliver (404s, 429s,
            %% timeouts) back to the download limit, so that the limit caps
            %% the bytes actually fetched. Otherwise goodput under the limit
            %% would only be the limit times the success ratio.
            Refund = ?DATA_CHUNK_SIZE - BytesFetched,
            PeerState2 = arweave_sync_peer:add_fetch_result(
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
                    %% The store claim stays until the asynchronous write
                    %% finishes. The footprint's entropy slot stays too,
                    %% because the packing server uses those entropies to
                    %% unpack a peer-packed chunk after the handoff;
                    %% on_task_unpacked/2 releases the slot.
                    put_task(Task#task{state = writing}, State3);
                {_, write_complete} ->
                    finish_task(Task, State3);
                {_, writing} ->
                    State3
            end;
        not_found ->
            State
    end.

%% @doc Release the footprint's entropy slot once the chunk is unpacked.
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

%% @doc Count a finished write toward the store's write rate, then mark the
%% task's write as done.
on_task_write_completed(TaskRef, State) ->
    case maps:get(TaskRef, State#state.tasks, not_found) of
        #task{state = Status, store_id = StoreID} when
            Status =:= fetching; Status =:= writing
        ->
            on_task_write_finished(TaskRef, record_store_write(StoreID, State));
        _ ->
            State
    end.

%% @doc Mark the task's write as done, and finish the task if its fetch result
%% has also arrived.
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

record_store_write(StoreID, State) ->
    State#state{
        stores = arweave_sync_store:record_write_completed(
            StoreID, State#state.stores
        )
    }.

put_task(Task, State) ->
    #task{task_ref = TaskRef} = Task,
    #state{tasks = Tasks} = State,
    State#state{tasks = maps:put(TaskRef, Task, Tasks)}.

%% @doc Drop the task and release its store claim and entropy slot, unless
%% storage is still writing its chunk.
finish_task(#task{state = writing}, State) ->
    State;
finish_task(Task, State) ->
    #task{task_ref = TaskRef, store_id = StoreID, offset = Offset} = Task,
    #state{tasks = Tasks, stores = Stores, footprints = Footprints} = State,
    State#state{
        tasks = maps:remove(TaskRef, Tasks),
        stores = arweave_sync_store:release_claim(StoreID, Offset, Stores),
        footprints = arweave_sync_footprint:task_completed(Task, Footprints)
    }.

%%%===================================================================
%%% Test support.
%%%===================================================================

-ifdef(AR_TEST).
%% @doc Return the number of running fetch workers.
inflight_count() ->
    gen_server:call(?MODULE, inflight_count).
-endif.
