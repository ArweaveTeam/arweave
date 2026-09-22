-module(arweave_sync_scheduler_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_sync/include/arweave_sync_scheduler.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        shutdown_terminates_workers,
        local_writes_do_not_consume_peer_capacity,
        bound_footprint_owner_is_an_active_peer,
        reservation_binding_claims_only_enqueued_tasks,
        fragmented_partial_intervals_claim_each_request,
        overlapping_partial_intervals_defer_duplicate_request,
        startable_task_prefers_less_active_store,
        peer_queue_stays_full_behind_active_cap,
        mixed_peer_classes_rate_limited,
        bandwidth_cap_with_failures,
        task_lifecycle,
        bandwidth_cap_trickle,
        cache_full,
        footprint_budget,
        footprint_piggyback,
        bound_footprint_refills_through_dispatch,
        draining_footprint_tasks_finish_dispatching,
        peer_store_queue_limit_bounds_footprint_bindings,
        peer_store_queue_limit_is_work_conserving,
        byte_source_bypasses_full_footprint_pool,
        bound_footprint_keeps_its_peer,
        per_peer_concurrency_cap,
        store_balance,
        disk_gate,
        head_of_line_blocked_peers,
        cap,
        {group, no_leak},
        {group, crash_no_leak}
    ].

groups() ->
    [{no_leak, [], [drains_clean]}, {crash_no_leak, [], [drains_clean]}].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_sync),
    ok = ar_metrics:register(),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = ar_metrics:cleanup(),
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_group(Group, Config) ->
    [{completion_mode, Group} | Config].

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    arweave_sync_deps:override_module(arweave_sync_deps_mainnet),
    arweave_sync_peer:reset_rows(),
    %% Workers are mocked: only read-side cache accounting is needed here.
    ar_chunk_cache:create_ets(),
    ets:insert(ar_chunk_cache, [{limit, 2000}, {used, 0}]),
    Config.

end_per_testcase(_, _) ->
    ets:delete(ar_chunk_cache),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Scheduler shutdown terminates every worker in its monitor index.
shutdown_terminates_workers(_Config) ->
    WorkerPID = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    MonitorRef = erlang:monitor(process, WorkerPID),
    arweave_sync_scheduler:terminate_workers(#{MonitorRef => {make_ref(), WorkerPID}}),
    ?assertNot(is_process_alive(WorkerPID)).

%% @doc Pending local writes count toward store load but do not consume peer
%% fetch capacity.
local_writes_do_not_consume_peer_capacity(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
        ],
        fun() ->
            Peer = peer1,
            StoreID = store1,
            FetchingRef = make_ref(),
            WritingRef = make_ref(),
            Tasks = #{
                FetchingRef => #task{
                    state = fetching,
                    peer = Peer,
                    store_id = StoreID
                },
                WritingRef => #task{
                    state = writing,
                    peer = Peer,
                    store_id = StoreID
                }
            },
            StoreDispatches = arweave_sync_store:start_dispatch(
                arweave_sync_footprint:new(),
                Tasks,
                [],
                arweave_sync_store:new()
            ),
            PeerDispatches0 = arweave_sync_peer:start_dispatch(
                Tasks, [], arweave_sync_peer:new()
            ),
            PeerDispatches = arweave_sync_peer:set_store_task_targets(
                Peer, [StoreID], PeerDispatches0
            ),
            Dispatch = #dispatch{
                stores = StoreDispatches, peers = PeerDispatches
            },
            %% One task exists in each local stage, but only the fetching task occupies
            %% the peer's single network slot. The default peer has one active slot and
            %% an eight-task queued-work bootstrap, so one of nine assignments is used.
            ?assertEqual(
                1 / 9,
                arweave_sync_peer:load(Peer, Dispatch#dispatch.peers)
            ),
            ?assertNot(
                arweave_sync_peer:has_capacity(Peer, Dispatch#dispatch.peers)
            ),
            %% Local writes are accounted only by the destination store.
            ?assertEqual(
                1 / 9,
                arweave_sync_peer:store_load(
                    Peer, StoreID, Dispatch#dispatch.peers
                )
            ),
            ?assertEqual(#{Peer => 1}, arweave_sync_scheduler:inflight_counts(Tasks))
        end
    ).

%% @doc A bound footprint keeps its peer active even without queued or inflight
%% tasks.
bound_footprint_owner_is_an_active_peer(_Config) ->
    Footprint = #footprint{store_id = store1, partition = 0, footprint = 1},
    Reservation = arweave_sync_footprint:test_reservation(
        store1, Footprint, [], peer1, 0, bound
    ),
    Footprints = arweave_sync_footprint:test_state([Reservation]),
    ?assertEqual([peer1], arweave_sync_scheduler:active_peers(#{}, #{}, Footprints, #{})).

%% @doc Binding a footprint replaces its speculative claim with claims for
%% enqueued tasks only.
reservation_binding_claims_only_enqueued_tasks(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
        ],
        fun() ->
            Peer = {1, 1, 1, 1, 9},
            Footprint = #footprint{
                store_id = store1, partition = 0, footprint = 1
            },
            %% The selected source advertises sixteen chunks from the complete
            %% footprint reservation.
            SparseIntervals = ar_intervals:from_list([
                {16 * ?DATA_CHUNK_SIZE, 0}
            ]),
            Reservation = arweave_sync_footprint:new_reservation(
                store1,
                Footprint,
                [
                    #task_source{
                        peer = Peer,
                        footprint = Footprint,
                        intervals = SparseIntervals
                    }
                ]
            ),
            Dispatch0 = (seed([Reservation]))#dispatch{
                peers = peer_dispatches_for_stores(
                    #{Peer => 8}, #{Peer => [store1]}
                )
            },
            StoreStates0 = arweave_sync_store:finish_dispatch(
                Dispatch0#dispatch.stores
            ),
            ?assertEqual(
                arweave_sync_store:chunks_in_claim(Reservation),
                arweave_sync_store:claimed_chunks(store1, StoreStates0)
            ),
            {Footprint, StoreDispatches} = arweave_sync_store:pop_work(
                store1, Dispatch0#dispatch.stores
            ),
            DispatchA = Dispatch0#dispatch{stores = StoreDispatches},
            Work = arweave_sync_scheduler:resolve_work(Footprint, DispatchA),
            {_State, Dispatch1} = arweave_sync_scheduler:dispatch_work(Work, #state{}, DispatchA),
            StoreStates1 = arweave_sync_store:finish_dispatch(
                Dispatch1#dispatch.stores
            ),
            %% Binding releases the whole-footprint reservation. The unmeasured peer's
            %% eight-task queue limit remains claimed; retained intervals are future
            %% work.
            ?assertEqual(
                8, arweave_sync_store:claimed_chunks(store1, StoreStates1)
            ),
            ?assertEqual(
                8, arweave_sync_store:queued_task_count(store1, StoreStates1)
            ),
            ?assertEqual(
                8,
                queue:len(
                    maps:get(
                        Peer,
                        Dispatch1#dispatch.peer_queues
                    )
                )
            )
        end
    ).

%% @doc Disjoint partial-chunk ranges claim one full request each.
fragmented_partial_intervals_claim_each_request(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
        ],
        fun() ->
            Peer = {1, 1, 1, 1, 9},
            Footprint = #footprint{
                store_id = store1, partition = 0, footprint = 1
            },
            %% Two disjoint half-chunk advertisements still produce two /chunk2
            %% requests, one from the start of each interval.
            HalfChunk = ?DATA_CHUNK_SIZE div 2,
            PartialIntervals = ar_intervals:from_list([
                {HalfChunk, 0},
                {2 * ?DATA_CHUNK_SIZE + HalfChunk, 2 * ?DATA_CHUNK_SIZE}
            ]),
            Reservation = arweave_sync_footprint:new_reservation(
                store1,
                Footprint,
                [
                    #task_source{
                        peer = Peer,
                        footprint = Footprint,
                        intervals = PartialIntervals
                    }
                ]
            ),
            Dispatch0 = (seed([Reservation]))#dispatch{
                peers = peer_dispatches_for_stores(
                    #{Peer => 8}, #{Peer => [store1]}
                )
            },
            {Footprint, StoreDispatches} = arweave_sync_store:pop_work(
                store1, Dispatch0#dispatch.stores
            ),
            DispatchA = Dispatch0#dispatch{stores = StoreDispatches},
            Work = arweave_sync_scheduler:resolve_work(Footprint, DispatchA),
            {_State, Dispatch1} = arweave_sync_scheduler:dispatch_work(Work, #state{}, DispatchA),
            StoreStates1 = arweave_sync_store:finish_dispatch(
                Dispatch1#dispatch.stores
            ),
            ?assertEqual(
                2, arweave_sync_store:claimed_chunks(store1, StoreStates1)
            ),
            ?assertEqual(
                2, arweave_sync_store:queued_task_count(store1, StoreStates1)
            ),
            ?assertEqual(
                2,
                queue:len(
                    maps:get(
                        Peer,
                        Dispatch1#dispatch.peer_queues
                    )
                )
            )
        end
    ).

%% @doc Fragments covered by one request span produce only one immediate claim.
overlapping_partial_intervals_defer_duplicate_request(_Config) ->
    arweave_sync_test_util:with_mocks(
        [
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
        ],
        fun() ->
            Peer = {1, 1, 1, 1, 9},
            Footprint = #footprint{
                store_id = store1, partition = 0, footprint = 1
            },
            QuarterChunk = ?DATA_CHUNK_SIZE div 4,
            %% Both advertised fragments fall inside the first fixed-size request span.
            %% One request is claimed now; the sweeper may rediscover any uncovered
            %% pre-strict-split data after that request completes.
            PartialIntervals = ar_intervals:from_list([
                {QuarterChunk, 0},
                {3 * QuarterChunk, 2 * QuarterChunk}
            ]),
            Reservation = arweave_sync_footprint:new_reservation(
                store1,
                Footprint,
                [
                    #task_source{
                        peer = Peer,
                        footprint = Footprint,
                        intervals = PartialIntervals
                    }
                ]
            ),
            Dispatch0 = (seed([Reservation]))#dispatch{
                peers = peer_dispatches_for_stores(
                    #{Peer => 8}, #{Peer => [store1]}
                )
            },
            {Footprint, StoreDispatches} = arweave_sync_store:pop_work(
                store1, Dispatch0#dispatch.stores
            ),
            DispatchA = Dispatch0#dispatch{stores = StoreDispatches},
            Work = arweave_sync_scheduler:resolve_work(Footprint, DispatchA),
            {_State, Dispatch1} = arweave_sync_scheduler:dispatch_work(Work, #state{}, DispatchA),
            StoreStates1 = arweave_sync_store:finish_dispatch(
                Dispatch1#dispatch.stores
            ),
            ?assertEqual(
                1, arweave_sync_store:claimed_chunks(store1, StoreStates1)
            ),
            ?assertEqual(
                1, arweave_sync_store:queued_task_count(store1, StoreStates1)
            ),
            [Child] = queue:to_list(
                maps:get(
                    Peer,
                    Dispatch1#dispatch.peer_queues
                )
            ),
            ?assertEqual(0, Child#task.offset)
        end
    ).

%% @doc An idle store starts before a busier store even when its task appears
%% later in the queue.
startable_task_prefers_less_active_store(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        StoreA = store_a,
        StoreB = store_b,
        FetchingTasks = maps:from_list([
            {make_ref(), #task{state = fetching, store_id = StoreA}},
            {make_ref(), #task{state = fetching, store_id = StoreA}}
        ]),
        TaskA = #task{offset = 0, store_id = StoreA},
        TaskB = #task{offset = ?DATA_CHUNK_SIZE, store_id = StoreB},
        StoreDispatches = arweave_sync_store:start_dispatch(
            arweave_sync_footprint:new(),
            FetchingTasks,
            [TaskA, TaskB],
            arweave_sync_store:new()
        ),
        %% Store A has two active fetches and appears first in the peer queue;
        %% store B has none, so activation selects B without reordering A.
        {TaskB, Remaining} = arweave_sync_scheduler:take_startable_task(
            queue:from_list([TaskA, TaskB]), StoreDispatches
        ),
        ?assertEqual([TaskA], queue:to_list(Remaining))
    end).

%% @doc Starting capped fetches refills vacated queue positions without
%% exceeding queue capacity.
peer_queue_stays_full_behind_active_cap(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Peer = peer1,
        StoreID = store1,
        %% Ten candidates exceed the two active plus four queued tasks that this
        %% test dispatch may hold, leaving work in the unbound store queue.
        Tasks = [
            #task{
                store_id = StoreID,
                sources = [#task_source{peer = Peer}],
                offset = Index * ?DATA_CHUNK_SIZE
            }
         || Index <- lists:seq(0, 9)
        ],
        PeerDispatches = arweave_sync_peer:set_store_task_targets(
            Peer,
            [StoreID],
            arweave_sync_peer:test_dispatch(
                #{Peer => 2}, #{Peer => 4}
            )
        ),
        Dispatch0 = (seed(Tasks))#dispatch{peers = PeerDispatches},
        {State1, Dispatch1} = arweave_sync_scheduler:bind_tasks(#state{}, Dispatch0),
        ?assertEqual(
            4,
            queue:len(
                maps:get(
                    Peer, Dispatch1#dispatch.peer_queues
                )
            )
        ),
        ?assertEqual([], Dispatch1#dispatch.tasks_to_start),
        {State2, Dispatch2} = arweave_sync_scheduler:activate_tasks(State1, Dispatch1),
        %% The active cap starts two tasks and temporarily leaves two queued.
        ?assertEqual(2, length(Dispatch2#dispatch.tasks_to_start)),
        ?assertEqual(
            2,
            queue:len(
                maps:get(
                    Peer, Dispatch2#dispatch.peer_queues
                )
            )
        ),
        State2A = arweave_sync_scheduler:record_driven_peers(State2, Dispatch2),
        ?assertEqual(#{Peer => true}, State2A#state.driven_peers),
        Dispatch2A = Dispatch2#dispatch{
            stores = arweave_sync_store:refresh_work(
                Dispatch2#dispatch.footprints, Dispatch2#dispatch.stores
            )
        },
        {_State3, Dispatch3} = arweave_sync_scheduler:bind_tasks(State2A, Dispatch2A),
        %% Refilling the two vacated places restores the four-task peer queue.
        ?assertEqual(
            4,
            queue:len(
                maps:get(
                    Peer, Dispatch3#dispatch.peer_queues
                )
            )
        )
    end).

%% @doc The byte budget bounds total starts while leaving each peer-cap class
%% eligible.
mixed_peer_classes_rate_limited(_Config) ->
    Mocks = [
        %% Three rounds across four peer classes permit exactly 12 chunks.
        {arweave_config, get, fun
            ([sync, max_download_rate]) -> 12 * ?DATA_CHUNK_SIZE;
            (Key) -> meck:passthrough([Key])
        end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
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
                [
                    #task{
                        store_id = store1,
                        sources = [#task_source{peer = Peer}],
                        offset = (Round * ClassCount + Index) * ?DATA_CHUNK_SIZE
                    }
                 || {Peer, Index} <- IndexedPeers
                ]
            end,
            lists:seq(0, QueuedRounds - 1)
        ),
        BudgetChunks = Rounds * ClassCount,
        DownloadLimit = arweave_sync_download_limit:refill(
            arweave_sync_download_limit:new()
        ),
        Dispatch0 = (seed(Tasks))#dispatch{
            %% Caps model deep fast, exact slow/flaky, and one-slot-headroom
            %% limited pipelines; every class can serve all three budget rounds.
            peers = arweave_sync_peer:test_dispatch(#{
                Fast => 2 * Rounds,
                Slow => Rounds,
                Limited => Rounds + 1,
                Flaky => Rounds
            })
        },
        {SelectedState, Dispatch} = dispatch_tasks(
            #state{download_limit = DownloadLimit}, Dispatch0
        ),
        StartedByPeer = lists:foldl(
            fun(#task{peer = Peer}, Acc) ->
                arweave_util:increment_map_value(Peer, Acc)
            end,
            #{},
            Dispatch#dispatch.tasks_to_start
        ),
        ?assertEqual(BudgetChunks, length(Dispatch#dispatch.tasks_to_start)),
        ?assertNot(
            arweave_sync_download_limit:has_capacity(
                SelectedState#state.download_limit
            )
        ),
        ?assertEqual(
            maps:from_list([
                {Peer, Rounds}
             || Peer <- Peers
            ]),
            StartedByPeer
        )
    end).

%% @doc Failed or short fetches return unused reserved bytes to the download
%% budget.
bandwidth_cap_with_failures(_Config) ->
    %% Two chunks/s leaves room to observe full, partial, and zero refunds.
    Rate = 2 * ?DATA_CHUNK_SIZE,
    Mocks = [
        {arweave_config, get, fun
            ([sync, max_download_rate]) -> Rate;
            (Key) -> meck:passthrough([Key])
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Peer = {5, 5, 5, 5, 9},
        FailedRef = make_ref(),
        PartialRef = make_ref(),
        FullRef = make_ref(),
        FailedTask = #task{
            task_ref = FailedRef,
            peer = Peer,
            store_id = store1,
            footprint = none,
            offset = 0
        },
        PartialTask = #task{
            task_ref = PartialRef,
            peer = Peer,
            store_id = store1,
            footprint = none,
            offset = ?DATA_CHUNK_SIZE
        },
        FullTask = #task{
            task_ref = FullRef,
            peer = Peer,
            store_id = store1,
            footprint = none,
            offset = 2 * ?DATA_CHUNK_SIZE
        },
        FullRate = arweave_sync_download_limit:refill(
            arweave_sync_download_limit:new()
        ),
        ExhaustedRate = arweave_sync_download_limit:consume(
            FullRate, 2 * ?DATA_CHUNK_SIZE
        ),
        State0 = #state{
            download_limit = ExhaustedRate,
            tasks = #{
                FailedRef => FailedTask,
                PartialRef => PartialTask,
                FullRef => FullTask
            }
        },
        FetchTiming = #fetch_timing{
            productive_ms = 250,
            timeout_ms = 500
        },
        State1 = arweave_sync_scheduler:on_task_fetch_completed(FailedRef, 0, FetchTiming, State0),
        ?assert(
            arweave_sync_download_limit:has_capacity(
                State1#state.download_limit
            )
        ),
        State1Exhausted = State1#state{
            download_limit =
                arweave_sync_download_limit:consume(
                    State1#state.download_limit, ?DATA_CHUNK_SIZE
                )
        },
        %% A half-sized legacy chunk restores the unfilled half of its reservation.
        State2 = arweave_sync_scheduler:on_task_fetch_completed(
            PartialRef,
            ?DATA_CHUNK_SIZE div 2,
            FetchTiming,
            State1Exhausted
        ),
        ?assert(
            arweave_sync_download_limit:has_capacity(
                State2#state.download_limit
            )
        ),
        State2Exhausted = State2#state{
            download_limit =
                arweave_sync_download_limit:consume(
                    State2#state.download_limit, ?DATA_CHUNK_SIZE div 2
                )
        },
        State3 = arweave_sync_scheduler:on_task_fetch_completed(
            FullRef, ?DATA_CHUNK_SIZE, FetchTiming, State2Exhausted
        ),
        ?assertNot(
            arweave_sync_download_limit:has_capacity(
                State3#state.download_limit
            )
        )
    end).

%% @doc Fetch and write completion transitions release task and footprint state
%% exactly once.
task_lifecycle(_Config) ->
    Mocks = [],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        FetchTiming = #fetch_timing{},
        WriteFirstRef = make_ref(),
        WriteFirstTask = #task{
            store_id = store1,
            sources = [#task_source{peer = peer1}],
            offset = 0,
            task_ref = WriteFirstRef
        },
        WriteFirstState = #state{tasks = #{WriteFirstRef => WriteFirstTask}},
        WriteCompleted = arweave_sync_scheduler:on_task_write_completed(
            WriteFirstRef, WriteFirstState
        ),
        ?assertEqual(
            write_complete,
            (maps:get(WriteFirstRef, WriteCompleted#state.tasks))#task.state
        ),
        WriteFirstDone = arweave_sync_scheduler:on_task_fetch_completed(
            WriteFirstRef, ?DATA_CHUNK_SIZE, FetchTiming, WriteCompleted
        ),
        ?assertNot(maps:is_key(WriteFirstRef, WriteFirstDone#state.tasks)),

        FetchFirstRef = make_ref(),
        FetchFirstTask = #task{
            store_id = store1,
            sources = [#task_source{peer = peer2}],
            offset = ?DATA_CHUNK_SIZE,
            task_ref = FetchFirstRef
        },
        FetchFirstState = #state{tasks = #{FetchFirstRef => FetchFirstTask}},
        Writing = arweave_sync_scheduler:on_task_fetch_completed(
            FetchFirstRef, ?DATA_CHUNK_SIZE, FetchTiming, FetchFirstState
        ),
        ?assertEqual(
            writing,
            (maps:get(FetchFirstRef, Writing#state.tasks))#task.state
        ),
        FetchFirstDone = arweave_sync_scheduler:on_task_write_completed(FetchFirstRef, Writing),
        ?assertNot(maps:is_key(FetchFirstRef, FetchFirstDone#state.tasks)),

        FootprintRef = make_ref(),
        Footprint = #footprint{store_id = store1, partition = 1, footprint = 1},
        FootprintTask = #task{
            store_id = store1,
            sources = [#task_source{peer = peer3, footprint = Footprint}],
            peer = peer3,
            footprint = Footprint,
            offset = 2 * ?DATA_CHUNK_SIZE,
            task_ref = FootprintRef
        },
        BoundReservation = arweave_sync_footprint:test_reservation(
            store1,
            Footprint,
            [
                #task_source{
                    peer = peer3,
                    footprint = Footprint,
                    intervals = ar_intervals:new()
                }
            ],
            peer3,
            1,
            bound
        ),
        FootprintState = #state{
            tasks = #{FootprintRef => FootprintTask},
            footprints = arweave_sync_footprint:test_state([BoundReservation])
        },
        FootprintWriting = arweave_sync_scheduler:on_task_fetch_completed(
            FootprintRef, ?DATA_CHUNK_SIZE, FetchTiming, FootprintState
        ),
        %% The handed-off chunk is still unpacked with the footprint's
        %% entropies, so the entropy slot outlives the fetch.
        ?assertEqual(
            1,
            arweave_sync_footprint:bound_count(
                FootprintWriting#state.footprints
            )
        ),
        ?assertEqual(
            Footprint,
            (maps:get(FootprintRef, FootprintWriting#state.tasks))#task.footprint
        ),
        FootprintUnpacked = arweave_sync_scheduler:on_task_unpacked(FootprintRef, FootprintWriting),
        ?assertEqual(
            0,
            arweave_sync_footprint:bound_count(
                FootprintUnpacked#state.footprints
            )
        ),
        ?assertEqual(
            none,
            (maps:get(FootprintRef, FootprintUnpacked#state.tasks))#task.footprint
        ),
        %% A repeated signal is harmless.
        ?assertEqual(
            FootprintUnpacked,
            arweave_sync_scheduler:on_task_unpacked(FootprintRef, FootprintUnpacked)
        ),
        FootprintDone = arweave_sync_scheduler:on_task_write_completed(
            FootprintRef, FootprintUnpacked
        ),
        ?assertNot(maps:is_key(FootprintRef, FootprintDone#state.tasks)),
        ?assertEqual(
            0,
            arweave_sync_footprint:bound_count(
                FootprintDone#state.footprints
            )
        ),
        ?assert(
            arweave_sync_footprint:is_empty(FootprintDone#state.footprints)
        ),

        FailedRef = make_ref(),
        FailedTask = #task{
            store_id = store1,
            sources = [#task_source{peer = peer3}],
            offset = 2 * ?DATA_CHUNK_SIZE,
            task_ref = FailedRef
        },
        FailedState = #state{tasks = #{FailedRef => FailedTask}},
        FailedDone = arweave_sync_scheduler:on_task_fetch_completed(
            FailedRef, 0, FetchTiming, FailedState
        ),
        ?assertNot(maps:is_key(FailedRef, FailedDone#state.tasks)),

        DuplicateRef = make_ref(),
        DuplicateTask = #task{
            store_id = store1,
            sources = [#task_source{peer = peer4}],
            offset = 0,
            task_ref = DuplicateRef
        },
        DuplicateState = #state{tasks = #{DuplicateRef => DuplicateTask}},
        Accounted = arweave_sync_scheduler:on_task_fetch_completed(
            DuplicateRef, ?DATA_CHUNK_SIZE, FetchTiming, DuplicateState
        ),
        Duplicate = arweave_sync_scheduler:on_task_fetch_completed(
            DuplicateRef, ?DATA_CHUNK_SIZE, FetchTiming, Accounted
        ),
        ?assertEqual(Accounted#state.peer_state, Duplicate#state.peer_state),

        CrashedRef = make_ref(),
        CrashedMonitorRef = make_ref(),
        CrashedTask = #task{
            store_id = store1,
            sources = [#task_source{peer = {5, 5, 5, 5, 9}}],
            offset = 0,
            task_ref = CrashedRef,
            state = write_complete
        },
        CrashedState = #state{
            tasks = #{CrashedRef => CrashedTask},
            monitor_index = #{
                CrashedMonitorRef => {CrashedRef, self()}
            }
        },
        CrashedDone = arweave_sync_scheduler:worker_exited(
            CrashedMonitorRef, simulated_crash, CrashedState
        ),
        ?assertNot(maps:is_key(CrashedRef, CrashedDone#state.tasks)),
        ?assertNot(
            maps:is_key(CrashedMonitorRef, CrashedDone#state.monitor_index)
        )
    end).

%% @doc A download-budget wakeup starts work before the next coarse scheduler
%% tick.
bandwidth_cap_trickle(_Config) ->
    Rate = 2 * ?DATA_CHUNK_SIZE,
    Mocks = [
        {arweave_config, get, fun
            ([sync, max_download_rate]) -> Rate;
            %% One MiB is sufficient for init's positive footprint ceiling.
            ([packing, entropy, cache_size]) -> 1;
            (Key) -> meck:passthrough([Key])
        end},
        {ar_chunk_cache, is_full, fun() -> false end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
        {arweave_sync_fetch_worker, run, fun(Task) ->
            arweave_sync_scheduler:report_fetch_completed(
                Task#task.task_ref, 0, #fetch_timing{}
            )
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        ensure_table(),
        {ok, PID} = gen_server:start(arweave_sync_scheduler, [], []),
        try
            Rate = 2 * ?DATA_CHUNK_SIZE,
            Peer = {6, 6, 6, 6, 9},
            Task = #task{
                store_id = store1,
                sources = [#task_source{peer = Peer}],
                offset = 0
            },
            Now = ar_timer:monotonic_ms(),
            %% The configured two chunks/s earns one task in 1000/2 ms.
            RefillMs = 1000 div 2,
            FullRate = arweave_sync_download_limit:refill(
                arweave_sync_download_limit:new(Now - RefillMs), Now - RefillMs
            ),
            ExhaustedRate = arweave_sync_download_limit:consume(FullRate, Rate),
            _ = sys:replace_state(
                PID,
                fun(State) ->
                    State0 = State#state{
                        download_limit = ExhaustedRate,
                        dispatch_scheduled = false
                    },
                    {ok, 1, State2} = arweave_sync_scheduler:admit_candidate(store1, Task, State0),
                    State2
                end
            ),
            PID ! {arweave_sync_download_limit, wakeup},
            ?assertEqual(
                ok,
                ar_test_await:until(bandwidth_cap_trickle_dispatched, fun() ->
                    {ok, State} = gen_server:call(PID, get_state),
                    arweave_sync_store:queues_empty(State#state.stores) andalso
                        map_size(State#state.monitor_index) == 0 andalso
                        map_size(State#state.tasks) == 0
                end)
            ),
            ?assertEqual(1, meck:num_calls(arweave_sync_fetch_worker, run, 1))
        after
            gen_server:stop(PID)
        end
    end).

%% @doc A full cache prevents dispatch while repeated admissions remain
%% deduplicated.
cache_full(_Config) ->
    Mocks = [
        {arweave_config, get, fun
            ([packing, entropy, cache_size]) -> 1000000;
            (K) -> meck:passthrough([K])
        end},
        {ar_chunk_cache, cached_size, fun() -> 1 end},
        {ar_chunk_cache, limit, fun() -> 1 end},
        {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        ensure_table(),
        {ok, PID} = gen_server:start(arweave_sync_scheduler, [], []),
        try
            Peers = [{1, 1, 1, 1, 9}, {2, 2, 2, 2, 9}],
            Tasks = [
                #task{
                    offset = I * ?DATA_CHUNK_SIZE,
                    sources = [#task_source{peer = Peer} || Peer <- Peers],
                    store_id = store1
                }
             || I <- lists:seq(1, 5)
            ],
            ?assertEqual(
                {ok, length(Tasks)},
                gen_server:call(PID, {claim_and_enqueue, store1, Tasks})
            ),
            ?assertEqual(
                {ok, 0},
                gen_server:call(PID, {claim_and_enqueue, store1, Tasks})
            ),
            %% syncs both casts
            {ok, State} = gen_server:call(PID, get_state),
            %% cache full -> nothing spawned
            ?assertEqual(0, map_size(State#state.monitor_index)),
            %% deduped, not 10
            ?assertEqual(5, arweave_sync_scheduler:queued_task_count_by_store(store1, State))
        after
            gen_server:stop(PID)
        end
    end).

%% At most the configured number of distinct footprint batches may be active.
footprint_budget(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Tasks = lists:map(
            fun(I) ->
                Peer = {1, 1, 1, I, 9},
                Footprint = #footprint{
                    store_id = store1,
                    partition = 1,
                    footprint = I
                },
                footprint_reservation(
                    store1,
                    Peer,
                    Footprint,
                    [I * ?DATA_CHUNK_SIZE]
                )
            end,
            lists:seq(1, 5)
        ),
        Dispatch0 = set_max_active_footprints(2, seed(Tasks)),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{
                peers = arweave_sync_peer:test_dispatch(
                    maps:from_list([
                        {{1, 1, 1, I, 9}, 1000}
                     || I <- lists:seq(1, 5)
                    ])
                )
            }
        ),
        ?assertEqual(2, length(Dispatch#dispatch.tasks_to_start)),
        ?assertEqual(
            2,
            arweave_sync_footprint:bound_count(
                Dispatch#dispatch.footprints
            )
        )
    end).

%% Chunks generated from one footprint task share its entropy slot.
footprint_piggyback(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        P = {1, 1, 1, 1, 9},
        Footprint = #footprint{store_id = store1, partition = 1, footprint = 1},
        Tasks = [
            footprint_reservation(
                store1,
                P,
                Footprint,
                [I * ?DATA_CHUNK_SIZE || I <- lists:seq(1, 3)]
            )
        ],
        Dispatch0 = set_max_active_footprints(1, seed(Tasks)),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{
                peers = peer_dispatches_for_stores(
                    #{P => 3}, #{P => [store1]}
                )
            }
        ),
        ?assertEqual(3, length(Dispatch#dispatch.tasks_to_start)),
        Reservation = arweave_sync_footprint:test_get(
            Footprint, Dispatch#dispatch.footprints
        ),
        ?assertEqual(
            bound, arweave_sync_footprint:reservation_state(Reservation)
        ),
        ?assertEqual(3, arweave_sync_footprint:active_tasks(Reservation)),
        ?assertEqual(P, arweave_sync_footprint:reservation_peer(Reservation))
    end).

%% @doc A bound footprint's remaining intervals refill and start tasks even with
%% an empty store queue.
bound_footprint_refills_through_dispatch(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Peer = {1, 1, 1, 1, 9},
        StoreID = store1,
        Footprint = #footprint{
            store_id = StoreID,
            partition = 1,
            footprint = 1
        },
        %% A one-task active cap plus a two-task queue limit lets one fetch start
        %% while one waits, leaving one retained chunk for the next dispatch pass.
        Intervals = ar_intervals:from_list([{3 * ?DATA_CHUNK_SIZE, 0}]),
        Reservation = arweave_sync_footprint:test_reservation(
            StoreID,
            Footprint,
            [
                #task_source{
                    peer = Peer,
                    footprint = Footprint,
                    intervals = Intervals
                }
            ],
            Peer,
            0,
            bound
        ),
        State = #state{
            stores = arweave_sync_store:new(),
            footprints = arweave_sync_footprint:test_state([Reservation])
        },
        Dispatch0 = arweave_sync_scheduler:start_dispatch(State),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{
                peers = arweave_sync_peer:set_store_task_targets(
                    Peer,
                    [StoreID],
                    arweave_sync_peer:test_dispatch(
                        #{Peer => 1}, #{Peer => 2}
                    )
                )
            }
        ),
        [Started] = Dispatch#dispatch.tasks_to_start,
        ?assertEqual(Peer, Started#task.peer),
        Reservation2 = arweave_sync_footprint:test_get(
            Footprint, Dispatch#dispatch.footprints
        ),
        ?assertEqual(2, arweave_sync_footprint:active_tasks(Reservation2)),
        [#task_source{intervals = RemainingIntervals}] =
            arweave_sync_footprint:sources(Reservation2),
        ?assertEqual(
            ?DATA_CHUNK_SIZE,
            ar_intervals:sum(RemainingIntervals)
        )
    end).

%% Marking a reservation draining stops refills, but its already-enqueued
%% children remain eligible so the reservation can reach zero and release.
draining_footprint_tasks_finish_dispatching(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Peer = {2, 2, 2, 2, 9},
        StoreID = store1,
        Footprint = #footprint{
            store_id = StoreID,
            partition = 1,
            footprint = 2
        },
        Child = #task{
            offset = 0,
            store_id = StoreID,
            footprint = Footprint,
            sources = [#task_source{peer = Peer, footprint = Footprint}],
            state = queued
        },
        Reservation = arweave_sync_footprint:test_reservation(
            StoreID,
            Footprint,
            [
                #task_source{
                    peer = Peer,
                    footprint = Footprint,
                    intervals = ar_intervals:new()
                }
            ],
            Peer,
            1,
            draining
        ),
        {ok, 1, StoreStates} = arweave_sync_store:admit(
            StoreID, Child, arweave_sync_store:new()
        ),
        Dispatch0 = arweave_sync_scheduler:start_dispatch(#state{
            stores = StoreStates,
            footprints = arweave_sync_footprint:test_state([Reservation])
        }),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{
                peers = arweave_sync_peer:test_dispatch(#{Peer => 1})
            }
        ),
        [Started] = Dispatch#dispatch.tasks_to_start,
        ?assertEqual(Peer, Started#task.peer),
        ?assertEqual(Footprint, Started#task.footprint)
    end).

%% A peer/store pair binds one partial footprint at a time. Enqueued tasks
%% consume that pair's queue capacity before another footprint can bind.
peer_store_queue_limit_bounds_footprint_bindings(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Peer = {1, 1, 1, 1, 9},
        AlternatePeer = {2, 2, 2, 2, 9},
        FootprintA = #footprint{
            store_id = store1, partition = 1, footprint = 1
        },
        FootprintB = #footprint{
            store_id = store1, partition = 1, footprint = 2
        },
        ReservationB = footprint_reservation(
            store1,
            Peer,
            FootprintB,
            [I * ?DATA_CHUNK_SIZE || I <- lists:seq(4, 6)]
        ),
        [PeerSource] = ReservationB#footprint_reservation.sources,
        Tasks = [
            footprint_reservation(
                store1,
                Peer,
                FootprintA,
                [I * ?DATA_CHUNK_SIZE || I <- lists:seq(1, 3)]
            ),
            ReservationB#footprint_reservation{
                sources = [
                    PeerSource,
                    PeerSource#task_source{peer = AlternatePeer}
                ]
            }
        ],
        Dispatch0 = set_max_active_footprints(10, seed(Tasks)),
        PeerDispatches0 = peer_dispatches_for_stores(
            #{Peer => 3, AlternatePeer => 1},
            #{Peer => [store1], AlternatePeer => [store1]}
        ),
        PeerDispatches = arweave_sync_peer:start_task(
            AlternatePeer,
            #task{state = fetching, store_id = store1},
            PeerDispatches0
        ),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{peers = PeerDispatches}
        ),
        Started = Dispatch#dispatch.tasks_to_start,
        ?assertEqual(3, length(Started)),
        ?assertEqual(
            1,
            length(
                lists:usort([
                    Footprint
                 || #task{footprint = Footprint} <- Started
                ])
            )
        )
    end).

%% Peer/store load is a selection bias rather than a second hard gate, so a
%% store with remaining work uses capacity that another store cannot use.
peer_store_queue_limit_is_work_conserving(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Peer = {1, 1, 1, 1, 9},
        TasksA = [
            #task{
                store_id = store_a,
                sources = [#task_source{peer = Peer}],
                offset = 0
            }
        ],
        TasksB = [
            #task{
                store_id = store_b,
                sources = [#task_source{peer = Peer}],
                offset = I * ?DATA_CHUNK_SIZE
            }
         || I <- lists:seq(1, 10)
        ],
        Dispatch1 = (seed(TasksA ++ TasksB))#dispatch{
            peers = peer_dispatches_for_stores(
                #{Peer => 4}, #{Peer => [store_a, store_b]}
            )
        },
        {_State, Dispatch} = dispatch_tasks(#state{}, Dispatch1),
        Started = Dispatch#dispatch.tasks_to_start,
        %% The four-task peer limit starts the only store_a task and lets store_b
        %% use the otherwise-unused second half of its nominal two-task share.
        ?assertEqual(4, length(Started)),
        ?assertEqual(1, count_tasks_by_store(store_a, Started)),
        ?assertEqual(3, count_tasks_by_store(store_b, Started))
    end).

%% Byte availability remains startable when another footprint consumes the
%% only entropy slot.
byte_source_bypasses_full_footprint_pool(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        BytePeer = {1, 1, 1, 1, 9},
        FootprintPeer = {2, 2, 2, 2, 9},
        ActiveFootprint = #footprint{
            store_id = store1,
            partition = 1,
            footprint = 1
        },
        QueuedFootprint = #footprint{
            store_id = store1,
            partition = 1,
            footprint = 2
        },
        Intervals = ar_intervals:from_list([{?DATA_CHUNK_SIZE, 0}]),
        Task = arweave_sync_footprint:new_reservation(
            store1,
            QueuedFootprint,
            [
                #task_source{peer = BytePeer, intervals = Intervals},
                #task_source{
                    peer = FootprintPeer,
                    footprint = QueuedFootprint,
                    intervals = Intervals
                }
            ]
        ),
        BoundReservation = arweave_sync_footprint:test_reservation(
            store1,
            ActiveFootprint,
            [
                #task_source{
                    peer = FootprintPeer,
                    footprint = ActiveFootprint,
                    intervals = Intervals
                }
            ],
            FootprintPeer,
            1,
            bound
        ),
        Dispatch0 = seed([Task]),
        Footprints = arweave_sync_footprint:test_state([Task, BoundReservation]),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{
                footprints = arweave_sync_footprint:test_dispatch(
                    Footprints, 1
                ),
                peers = arweave_sync_peer:test_dispatch(#{
                    BytePeer => 1, FootprintPeer => 1
                })
            }
        ),
        [Started] = Dispatch#dispatch.tasks_to_start,
        ?assertEqual(BytePeer, Started#task.peer),
        ?assertEqual(none, Started#task.footprint)
    end).

%% A footprint reservation chooses one source, and every enqueued task is bound
%% to that source even when the reservation carried other alternatives.
bound_footprint_keeps_its_peer(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        BoundPeer = {1, 1, 1, 1, 9},
        BytePeer = {2, 2, 2, 2, 9},
        OtherFootprintPeer = {3, 3, 3, 3, 9},
        Footprint = #footprint{store_id = store1, partition = 1, footprint = 1},
        Intervals = ar_intervals:from_list([
            {2 * ?DATA_CHUNK_SIZE, 0}
        ]),
        Task = arweave_sync_footprint:new_reservation(
            store1,
            Footprint,
            [
                #task_source{
                    peer = BoundPeer,
                    footprint = Footprint,
                    intervals = Intervals
                },
                #task_source{peer = BytePeer, intervals = Intervals},
                #task_source{
                    peer = OtherFootprintPeer,
                    footprint = Footprint,
                    intervals = Intervals
                }
            ]
        ),
        Dispatch0 = seed([Task]),
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            Dispatch0#dispatch{
                %% BoundPeer has the lowest normalized load, so it wins the one-time
                %% source choice despite the byte and footprint alternatives.
                peers = peer_dispatches(
                    #{BoundPeer => 1, BytePeer => 8, OtherFootprintPeer => 6},
                    #{
                        BoundPeer => 10,
                        BytePeer => 10,
                        OtherFootprintPeer => 10
                    }
                )
            }
        ),
        ?assertEqual(2, length(Dispatch#dispatch.tasks_to_start)),
        ?assert(
            lists:all(
                fun(#task{peer = Peer, sources = Sources}) ->
                    Peer =:= BoundPeer andalso
                        Sources =:=
                            [
                                #task_source{
                                    peer = BoundPeer, footprint = Footprint
                                }
                            ]
                end,
                Dispatch#dispatch.tasks_to_start
            )
        )
    end).

%% The per-peer cap bounds how many workers each peer may run.
per_peer_concurrency_cap(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Good = {1, 1, 1, 1, 25},
        Bad = {2, 2, 2, 2, 100},
        Tasks =
            [
                #task{
                    store_id = store1,
                    sources = [#task_source{peer = Good}],
                    offset = I * ?DATA_CHUNK_SIZE
                }
             || I <- lists:seq(1, 50)
            ] ++
                [
                    #task{
                        store_id = store1,
                        sources = [#task_source{peer = Bad}],
                        offset = (100 + I) * ?DATA_CHUNK_SIZE
                    }
                 || I <- lists:seq(1, 50)
                ],
        %% The sixteen- and eight-task caps total twenty-four, below the unmeasured
        %% store's twenty-five-task probe, so this test isolates peer caps.
        GoodCap = 16,
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            (seed(Tasks))#dispatch{
                %% Explicit queue limits keep enough runnable work behind each active
                %% cap; the default eight-task bootstrap is for unmeasured peers.
                peers = arweave_sync_peer:test_dispatch(
                    #{Good => GoodCap, Bad => 8},
                    #{Good => GoodCap, Bad => 8}
                )
            }
        ),
        TasksToStart = Dispatch#dispatch.tasks_to_start,
        GoodCount = count_tasks_by_peer(Good, TasksToStart),
        BadCount = count_tasks_by_peer(Bad, TasksToStart),
        ?assertEqual(GoodCap, GoodCount),
        ?assertEqual(8, BadCount)
    end).

%% Two stores at different footprint positions must both get capacity through
%% least-loaded selection rather than let the lower-position store monopolize.
store_balance(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        P = {1, 1, 1, 1, 9},
        TasksA = [
            #task{
                store_id = store_a,
                sources = [#task_source{peer = P}],
                offset = I * ?DATA_CHUNK_SIZE
            }
         || I <- lists:seq(1, 20)
        ],
        TasksB = [
            #task{
                store_id = store_b,
                sources = [#task_source{peer = P}],
                offset = I * ?DATA_CHUNK_SIZE
            }
         || I <- lists:seq(1, 20)
        ],
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            (seed(TasksA ++ TasksB))#dispatch{
                %% Ten runnable tasks make the queue deep enough to fill the
                %% explicit ten-request active cap in this synthetic first pass.
                peers = arweave_sync_peer:test_dispatch(#{P => 10}, #{P => 10})
            }
        ),
        TasksToStart = Dispatch#dispatch.tasks_to_start,
        ACount = count_tasks_by_store(store_a, TasksToStart),
        BCount = count_tasks_by_store(store_b, TasksToStart),
        ?assertEqual(10, length(TasksToStart)),
        ?assertEqual(5, ACount),
        ?assertEqual(5, BCount)
    end).

%% A store without sufficient disk space starts no work.
disk_gate(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        P = {1, 1, 1, 1, 9},
        %% store_id = no_disk -> is_disk_space_sufficient/1 returns false (see mock).
        Tasks = [
            #task{
                store_id = no_disk,
                sources = [#task_source{peer = P}],
                offset = I * ?DATA_CHUNK_SIZE
            }
         || I <- lists:seq(1, 5)
        ],
        {_State, Dispatch} = dispatch_tasks(#state{}, seed(Tasks)),
        ?assertEqual([], Dispatch#dispatch.tasks_to_start)
    end).

%% Head-of-line: tasks whose only peer is at cap move behind unexamined work for
%% this pass, leaving another peer's tasks reachable in the same work queue.
head_of_line_blocked_peers(_Config) ->
    Mocks = [
        {ar_data_sync, is_disk_space_sufficient, fun
            (no_disk) -> false;
            (_) -> true
        end}
    ],
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        Slow = {1, 1, 1, 1, 9},
        Fast = {2, 2, 2, 2, 9},
        SlowTasks = [
            #task{
                store_id = store1,
                sources = [#task_source{peer = Slow}],
                offset = I * ?DATA_CHUNK_SIZE
            }
         || I <- lists:seq(1, 20)
        ],
        FastTasks = [
            #task{
                store_id = store1,
                sources = [#task_source{peer = Fast}],
                offset = (100 + I) * ?DATA_CHUNK_SIZE
            }
         || I <- lists:seq(1, 5)
        ],
        {_State, Dispatch} = dispatch_tasks(
            #state{},
            (seed(SlowTasks ++ FastTasks))#dispatch{
                peers = peer_dispatches(
                    #{Slow => 1000}, #{Slow => 1, Fast => 1000}
                )
            }
        ),
        Peers = [T#task.peer || T <- Dispatch#dispatch.tasks_to_start],
        ?assertEqual(5, length(Peers)),
        ?assert(lists:all(fun(Pr) -> Pr =:= Fast end, Peers))
    end).

%% @doc Scheduler ticks retain exploration caps, probe productive peers and
%% remove inactive peer caps.
cap(_Config) ->
    PeerA = {1, 1, 1, 1, 25},
    PeerB = {2, 2, 2, 2, 75},

    %% A genuinely unseen peer retains one exploration request until the
    %% next control tick measures it.
    PeerDispatches = arweave_sync_peer:test_dispatch(#{known => 1}),
    ?assertEqual(
        1,
        arweave_sync_peer:concurrency_cap(unknown, PeerDispatches)
    ),
    ?assertEqual(
        1,
        arweave_sync_peer:concurrency_cap(known, PeerDispatches)
    ),
    %% First tick has no productive request time, so both peers remain at
    %% the eight-request exploration bound despite each having a queued
    %% task behind eight active fetches.
    InitialPeerState = record_peer_results(
        [PeerA, PeerB],
        100_000_000,
        #fetch_timing{},
        arweave_sync_peer:new()
    ),
    State0 = driven_peer_state(
        #{PeerA => 8, PeerB => 8}, #state{
            peer_state = InitialPeerState
        }
    ),
    State1 = arweave_sync_scheduler:tick(State0, 1000),
    PeerDispatches1 = arweave_sync_peer:start_dispatch(
        #{}, [], State1#state.peer_state
    ),
    ?assertEqual(8, arweave_sync_peer:concurrency_cap(PeerA, PeerDispatches1)),
    ?assertEqual(8, arweave_sync_peer:concurrency_cap(PeerB, PeerDispatches1)),
    %% The first productive driven observation establishes an eight-request
    %% baseline and starts its sixteen-request probe.
    ProductiveTiming = #fetch_timing{productive_ms = 1000},
    PeerState2 = record_peer_results(
        [PeerA, PeerB],
        100_000_000,
        ProductiveTiming,
        State1#state.peer_state
    ),
    State2 = arweave_sync_scheduler:tick(
        driven_peer_state(
            #{PeerA => 8, PeerB => 8}, State1#state{
                peer_state = PeerState2
            }
        ),
        2000
    ),
    PeerDispatches2 = arweave_sync_peer:start_dispatch(
        #{}, [], State2#state.peer_state
    ),
    ?assertEqual(16, arweave_sync_peer:concurrency_cap(PeerA, PeerDispatches2)),
    ?assertEqual(16, arweave_sync_peer:concurrency_cap(PeerB, PeerDispatches2)),
    %% Third tick with PeerB's queue drained and nothing inflight:
    %% PeerB drops from the caps map (its budget memory is kept in the
    %% scheduler state); PeerA records the first flat-goodput observation
    %% and advances its additive probe while waiting for repeated evidence.
    PeerState3 = record_peer_results(
        [PeerA, PeerB],
        100_000_000,
        ProductiveTiming,
        State2#state.peer_state
    ),
    State3 = arweave_sync_scheduler:tick(
        driven_peer_state(#{PeerA => 16}, State2#state{
            peer_state = PeerState3
        }),
        3000
    ),
    PeerDispatches3 = arweave_sync_peer:start_dispatch(
        #{}, [], State3#state.peer_state
    ),
    ?assertEqual(24, arweave_sync_peer:concurrency_cap(PeerA, PeerDispatches3)),
    ?assertEqual(1, arweave_sync_peer:concurrency_cap(PeerB, PeerDispatches3)),
    %% Dispatch uses the recomputed cap; an unknown peer retains one probe.
    Dispatch = (base_dispatch())#dispatch{
        peers = arweave_sync_peer:test_dispatch(#{p => 40})
    },
    ?assertEqual(
        40,
        arweave_sync_peer:concurrency_cap(p, Dispatch#dispatch.peers)
    ),
    ?assertEqual(
        1,
        arweave_sync_peer:concurrency_cap(unknown, Dispatch#dispatch.peers)
    ).

%% Drive a mix of byte + footprint tasks through the gen_server (worker
%% behaviour set by the caller's mock) and assert no residue once work drains.
drains_clean(Config) ->
    Mocks =
        case proplists:get_value(completion_mode, Config) of
            no_leak ->
                [
                    {arweave_config, get, fun
                        ([packing, entropy, cache_size]) -> 1000000;
                        (K) -> meck:passthrough([K])
                    end},
                    {ar_chunk_cache, is_full, fun() -> false end},
                    {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
                    {arweave_sync_fetch_worker, run, fun(Task) ->
                        arweave_sync_scheduler:report_fetch_completed(
                            Task#task.task_ref, 0, #fetch_timing{}
                        )
                    end}
                ];
            crash_no_leak ->
                [
                    {arweave_config, get, fun
                        ([packing, entropy, cache_size]) -> 1000000;
                        (K) -> meck:passthrough([K])
                    end},
                    {ar_chunk_cache, is_full, fun() -> false end},
                    {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
                    {arweave_sync_fetch_worker, run, fun(_Task) ->
                        exit(simulated_crash)
                    end}
                ]
        end,
    arweave_sync_test_util:with_mocks(Mocks, fun() ->
        ensure_table(),
        %% Drive this isolated scheduler directly by PID.
        {ok, PID} = gen_server:start(arweave_sync_scheduler, [], []),
        try
            Peers = [{1, 1, 1, 1, 9}, {2, 2, 2, 2, 9}],
            ByteTasks = [
                #task{
                    store_id = store1,
                    sources = [#task_source{peer = Peer} || Peer <- Peers],
                    offset = I * ?DATA_CHUNK_SIZE
                }
             || I <- lists:seq(1, 10)
            ],
            Reservations = lists:map(
                fun(I) ->
                    Footprint = #footprint{
                        store_id = store1,
                        partition = 1,
                        footprint = I
                    },
                    footprint_reservation(
                        store1,
                        lists:nth(I, Peers),
                        Footprint,
                        [(10 + I) * ?DATA_CHUNK_SIZE]
                    )
                end,
                lists:seq(1, 2)
            ),
            Tasks = ByteTasks ++ Reservations,
            ExpectedClaims = lists:sum([
                arweave_sync_store:chunks_in_claim(Task)
             || Task <- Tasks
            ]),
            ?assertEqual(
                {ok, ExpectedClaims},
                gen_server:call(PID, {claim_and_enqueue, store1, Tasks})
            ),
            ?assertEqual(
                ok,
                ar_test_await:until(dispatcher_drained, fun() ->
                    {ok, S} = gen_server:call(PID, get_state),
                    arweave_sync_store:queues_empty(S#state.stores) andalso
                        map_size(S#state.monitor_index) == 0 andalso
                        map_size(S#state.tasks) == 0
                end)
            ),
            {ok, State} = gen_server:call(PID, get_state),
            ?assertEqual(0, map_size(State#state.monitor_index)),
            ?assertEqual(0, map_size(State#state.tasks)),
            ?assert(arweave_sync_footprint:is_empty(State#state.footprints)),
            ?assert(arweave_sync_store:queues_empty(State#state.stores)),
            ?assert(arweave_sync_store:claims_empty(State#state.stores))
        after
            gen_server:stop(PID)
        end
    end).

%%====================================================================
%% Helpers
%%====================================================================

dispatch_tasks(State, Dispatch) ->
    {State2, Dispatch2} = arweave_sync_scheduler:bind_tasks(State, Dispatch),
    arweave_sync_scheduler:activate_tasks(State2, Dispatch2).

%% Build a sync task the way arweave_sync_chunk_picker does - peer-neutral, with the
%% offering peer as its one source. seed/1 routes each candidate through the
%% matching queue function so tests cover the same store queues as production.
base_dispatch() ->
    arweave_sync_scheduler:start_dispatch(#state{}).

seed(Tasks) ->
    State = lists:foldl(
        fun seed_candidate/2,
        #state{},
        Tasks
    ),
    arweave_sync_scheduler:start_dispatch(State).

seed_candidate(#task{store_id = StoreID} = Task, State) ->
    {ok, _ClaimedChunks, StoreStates} = arweave_sync_store:admit(
        StoreID, Task, State#state.stores
    ),
    State#state{stores = StoreStates};
seed_candidate(#footprint_reservation{} = Reservation, State) ->
    StoreID = arweave_sync_footprint:store_id(Reservation),
    {ok, _FootprintClaim, Footprints} = arweave_sync_footprint:admit(
        Reservation, State#state.footprints
    ),
    {ok, _StoreClaim, StoreStates} = arweave_sync_store:admit(
        StoreID, Reservation, State#state.stores
    ),
    State#state{stores = StoreStates, footprints = Footprints}.

set_max_active_footprints(MaxActive, Dispatch) ->
    Dispatch#dispatch{
        footprints = arweave_sync_footprint:set_max_active(
            MaxActive, Dispatch#dispatch.footprints
        )
    }.

peer_dispatches(InflightCounts, PeerCaps) ->
    maps:fold(
        fun(Peer, InflightCount, Acc) ->
            lists:foldl(
                fun(_, Dispatches) ->
                    arweave_sync_peer:start_task(
                        Peer,
                        #task{
                            state = fetching,
                            store_id = existing_inflight
                        },
                        Dispatches
                    )
                end,
                Acc,
                lists:seq(1, InflightCount)
            )
        end,
        arweave_sync_peer:test_dispatch(PeerCaps),
        InflightCounts
    ).

peer_dispatches_for_stores(PeerCaps, StoresByPeer) ->
    maps:fold(
        fun(Peer, StoreIDs, Acc) ->
            arweave_sync_peer:set_store_task_targets(Peer, StoreIDs, Acc)
        end,
        arweave_sync_peer:test_dispatch(PeerCaps),
        StoresByPeer
    ).

record_peer_results(Peers, DeliveredBytes, FetchTiming, PeerState) ->
    lists:foldl(
        fun(Peer, Acc) ->
            arweave_sync_peer:record_result(
                Peer, DeliveredBytes, FetchTiming, Acc
            )
        end,
        PeerState,
        Peers
    ).

driven_peer_state(InflightByPeer, State) ->
    Tasks = maps:fold(
        fun(Peer, Count, Acc) ->
            lists:foldl(
                fun(Index, TasksAcc) ->
                    Ref = {Peer, Index},
                    maps:put(
                        Ref,
                        #task{
                            task_ref = Ref,
                            state = fetching,
                            peer = Peer,
                            store_id = s
                        },
                        TasksAcc
                    )
                end,
                Acc,
                lists:seq(1, Count)
            )
        end,
        #{},
        InflightByPeer
    ),
    PeerQueues = maps:map(
        fun(Peer, _Count) ->
            queue:from_list([
                #task{
                    state = queued,
                    peer = Peer,
                    store_id = s
                }
            ])
        end,
        InflightByPeer
    ),
    State#state{
        tasks = Tasks,
        peer_queues = PeerQueues,
        driven_peers = maps:map(
            fun(_Peer, _Count) -> true end,
            InflightByPeer
        )
    }.

ensure_table() ->
    arweave_sync_peer:create_ets().

count_tasks_by_peer(Peer, Tasks) ->
    lists:foldl(
        fun(Task, Count) ->
            case Task#task.peer =:= Peer of
                true -> Count + 1;
                false -> Count
            end
        end,
        0,
        Tasks
    ).

count_tasks_by_store(StoreID, Tasks) ->
    lists:foldl(
        fun(Task, Count) ->
            case Task#task.store_id =:= StoreID of
                true -> Count + 1;
                false -> Count
            end
        end,
        0,
        Tasks
    ).

footprint_reservation(StoreID, Peer, Footprint, Offsets) ->
    Intervals = ar_intervals:from_list([
        {Offset + ?DATA_CHUNK_SIZE, Offset}
     || Offset <- Offsets
    ]),
    arweave_sync_footprint:new_reservation(
        StoreID,
        Footprint,
        [
            #task_source{
                peer = Peer,
                footprint = Footprint,
                intervals = Intervals
            }
        ]
    ).
