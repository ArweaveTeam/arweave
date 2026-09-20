-module(arweave_sync_chunk_cache_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        {group, fetch_pending},
        {group, fetch_completed},
        restart_preserves_other_stores,
        restart_preserves_queued_work,
        local_completions_feed_drain_rate,
        unavailable_write_owner
    ].

groups() ->
    [
        {fetch_pending, [], [write_owner_restart]},
        {fetch_completed, [], [write_owner_restart]}
    ].

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
    [{fetch_completed, Group =:= fetch_completed} | Config].

end_per_group(_, _) ->
    ok.

init_per_testcase(_, Config) ->
    arweave_sync_deps:override_module(arweave_sync_deps_mainnet),
    arweave_sync_peer:reset_rows(),
    ar_chunk_cache:create_ets(),
    {ok, Cache} = ar_chunk_cache:start_link(),
    %% Ten chunks leave headroom for the fetched chunk and its worker.
    ets:insert(ar_chunk_cache, {limit, 10}),
    [{chunk_cache, Cache} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(proplists:get_value(chunk_cache, Config)),
    ets:delete(ar_chunk_cache),
    ets:delete(arweave_sync_state, {writer, store1}),
    ets:delete(arweave_sync_state, {writer, store2}),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Restarting a writer releases lost work without crediting stale completions.
write_owner_restart(Config) ->
    with_scheduler(fun() ->
        FetchCompleted = proplists:get_value(fetch_completed, Config),
        Parent = self(),
        OwnerPID = start_owner(store1),
        meck:expect(
            arweave_sync_fetch_worker,
            run,
            fun(#task{store_id = StoreID, task_ref = TaskRef}) ->
                fetched(StoreID, peer1, 0, #{}, TaskRef),
                Parent ! {fetch_waiting, self(), TaskRef},
                receive
                    continue -> ok
                end,
                arweave_sync_scheduler:task_fetch_completed(
                    TaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{}
                )
            end
        ),
        {ok, Scheduler} = arweave_sync_scheduler:start_link(),
        Task = #task{
            store_id = store1,
            offset = 0,
            sources = [#task_source{peer = {1, 1, 1, 1, 1984}}]
        },
        Enqueue = {claim_and_enqueue, store1, [Task]},
        try
            ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue)),
            {WorkerPID, OldTaskRef} =
                receive
                    {fetch_waiting, PID, Ref} -> {PID, Ref}
                end,
            ?assertMatch(
                {admit, {fetched, _, _, _}, _, _}, take_chunk(OwnerPID)
            ),
            ?assertEqual({ok, 0}, gen_server:call(Scheduler, Enqueue)),
            case FetchCompleted of
                true ->
                    WorkerPID ! continue,
                    ok = ar_test_await:sync_fetches_drained(Scheduler);
                false ->
                    ok
            end,
            stop_owner(OwnerPID),
            %% Scheduler claims are reclaimed by store registration below.
            %% Re-admitting this range proves the lost write released its claim.
            NewOwnerPID = start_owner(store1),
            try
                ?assertNot(is_process_alive(WorkerPID)),
                ?assertEqual(0, ar_chunk_cache:reserved_size()),
                %% Delayed notifications for the discarded task cannot recreate
                %% a writing task or credit the lost write as completed work.
                arweave_sync_scheduler:task_fetch_completed(
                    OldTaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{}
                ),
                arweave_sync_scheduler:task_write_completed(store1, OldTaskRef),
                ?assertEqual(pong, gen_server:call(Scheduler, ping)),
                ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue)),
                ?assertEqual(
                    0,
                    meck:num_calls(
                        arweave_sync_store,
                        record_write_completed,
                        '_'
                    )
                )
            after
                gen_server:stop(Scheduler),
                stop_owner(NewOwnerPID)
            end
        after
            catch gen_server:stop(Scheduler),
            stop_owner(OwnerPID)
        end
    end).

%% @doc Restarting one store leaves another store's active fetch and claim intact.
restart_preserves_other_stores(_Config) ->
    with_scheduler(fun() ->
        Parent = self(),
        FirstOwner = start_owner(store1),
        OtherOwner = start_owner(store2),
        meck:expect(
            arweave_sync_fetch_worker,
            run,
            fun(#task{store_id = StoreID, task_ref = TaskRef}) ->
                fetched(StoreID, peer1, 0, #{}, TaskRef),
                Parent ! {fetch_waiting, StoreID, self(), TaskRef},
                receive
                    continue -> ok
                end,
                arweave_sync_scheduler:task_fetch_completed(
                    TaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{}
                )
            end
        ),
        {ok, Scheduler} = arweave_sync_scheduler:start_link(),
        FirstTask = #task{
            store_id = store1,
            offset = 0,
            sources = [#task_source{peer = {1, 1, 1, 1, 1984}}]
        },
        OtherTask = #task{
            store_id = store2,
            offset = 0,
            sources = [#task_source{peer = {2, 2, 2, 2, 1984}}]
        },
        try
            ?assertEqual(
                {ok, 1},
                arweave_sync_scheduler:claim_and_enqueue(
                    store1, [FirstTask]
                )
            ),
            ?assertEqual(
                {ok, 1},
                arweave_sync_scheduler:claim_and_enqueue(
                    store2, [OtherTask]
                )
            ),
            FirstWorker =
                receive
                    {fetch_waiting, store1, PID1, _} -> PID1
                end,
            {OtherWorker, OtherRef} =
                receive
                    {fetch_waiting, store2, PID2, Ref2} -> {PID2, Ref2}
                end,
            stop_owner(FirstOwner),
            NewOwner = start_owner(store1),
            try
                ?assertNot(is_process_alive(FirstWorker)),
                ?assert(is_process_alive(OtherWorker)),
                ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
                ?assertEqual(1, ar_chunk_cache:cached_size(store2)),
                ?assertEqual(1, ar_chunk_cache:reserved_size()),
                ?assertEqual(
                    {ok, 0},
                    arweave_sync_scheduler:claim_and_enqueue(
                        store2, [OtherTask]
                    )
                ),
                ?assertEqual(
                    {ok, 1},
                    arweave_sync_scheduler:claim_and_enqueue(
                        store1, [FirstTask]
                    )
                ),
                ?assertEqual(
                    0,
                    meck:num_calls(
                        arweave_sync_store,
                        record_write_completed,
                        '_'
                    )
                ),
                OtherWorker ! continue,
                arweave_sync_scheduler:task_write_completed(store2, OtherRef),
                ?assertEqual(
                    ok,
                    ar_test_await:until(
                        other_store_completed,
                        fun() ->
                            meck:num_calls(
                                arweave_sync_store,
                                record_write_completed,
                                [store2, '_']
                            ) =:= 1
                        end
                    )
                )
            after
                gen_server:stop(Scheduler),
                stop_owner(NewOwner)
            end
        after
            catch gen_server:stop(Scheduler),
            stop_owner(FirstOwner),
            stop_owner(OtherOwner)
        end
    end).

%% @doc Writer restart preserves queued claims until cache capacity returns.
restart_preserves_queued_work(_Config) ->
    with_scheduler(fun() ->
        Parent = self(),
        OwnerPID = start_owner(store1),
        %% Keep dispatch blocked across the restart so the queued-work contract
        %% is independent of the race between init finishing and dispatching.
        ets:insert(ar_chunk_cache, {limit, 0}),
        meck:expect(
            arweave_sync_fetch_worker,
            run,
            fun(#task{task_ref = TaskRef}) ->
                Parent ! {queued_task_started, TaskRef},
                arweave_sync_scheduler:task_fetch_completed(
                    TaskRef, 0, #fetch_timing{}
                )
            end
        ),
        {ok, Scheduler} = arweave_sync_scheduler:start_link(),
        Task = #task{
            store_id = store1,
            offset = 0,
            sources = [#task_source{peer = {1, 1, 1, 1, 1984}}]
        },
        try
            ?assertEqual(
                {ok, 1},
                arweave_sync_scheduler:claim_and_enqueue(store1, [Task])
            ),
            stop_owner(OwnerPID),
            NewOwner = start_owner(store1),
            try
                %% The original queued claim still exists, so a duplicate cannot
                %% be admitted. Releasing capacity must run that original task.
                ?assertEqual(
                    {ok, 0},
                    arweave_sync_scheduler:claim_and_enqueue(store1, [Task])
                ),
                ets:insert(ar_chunk_cache, {limit, 10}),
                gen_server:cast(Scheduler, dispatch),
                receive
                    {queued_task_started, _} -> ok
                end,
                ?assertEqual(ok, ar_test_await:sync_fetches_drained(Scheduler))
            after
                gen_server:stop(Scheduler),
                stop_owner(NewOwner)
            end
        after
            catch gen_server:stop(Scheduler),
            stop_owner(OwnerPID)
        end
    end).

%% @doc Local storage completions contribute to the measured drain rate.
local_completions_feed_drain_rate(_Config) ->
    with_scheduler(fun() ->
        %% A buffered local write keeps this observation backlogged.
        {ok, CacheRef} = ar_chunk_cache:reserve(store1),
        ar_chunk_cache:mark_cached(CacheRef),
        {ok, 1, States} = arweave_sync_store:admit(
            store1,
            #task{offset = 0, sources = []},
            arweave_sync_store:new()
        ),
        Baseline = arweave_sync_store:sample_drain_rates(0, States),
        %% 300 actual storage completions in ten seconds demonstrate 30 CPS.
        lists:foreach(
            fun(_) -> ar_chunk_cache:record_completed(store1) end,
            lists:seq(1, 300)
        ),
        Sampled = arweave_sync_store:sample_drain_rates(10_000, Baseline),
        ?assertEqual(30.0, arweave_sync_store:drain_rate(store1, Sampled)),
        ar_chunk_cache:release(CacheRef)
    end).

%% @doc A missing writer releases cache capacity and permits the range to retry.
unavailable_write_owner(_Config) ->
    with_scheduler(fun() ->
        Parent = self(),
        meck:expect(
            arweave_sync_fetch_worker,
            run,
            fun(#task{store_id = StoreID, task_ref = TaskRef}) ->
                fetched(StoreID, peer1, 0, #{}, TaskRef),
                arweave_sync_scheduler:task_fetch_completed(
                    TaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{}
                ),
                Parent ! handoff_attempted
            end
        ),
        {ok, Scheduler} = arweave_sync_scheduler:start_link(),
        Task = #task{
            store_id = store1,
            offset = 0,
            sources = [#task_source{peer = {1, 1, 1, 1, 1984}}]
        },
        Enqueue = {claim_and_enqueue, store1, [Task]},
        try
            ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue)),
            receive
                handoff_attempted -> ok
            end,
            ok = ar_test_await:sync_fetches_drained(Scheduler),
            ?assertEqual(0, ar_chunk_cache:reserved_size()),
            ?assertEqual(
                0,
                meck:num_calls(
                    arweave_sync_store,
                    record_write_completed,
                    '_'
                )
            ),
            %% The lost handoff must not leave a claim blocking a later retry.
            ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue))
        after
            gen_server:stop(Scheduler)
        end
    end).

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Mock only the fetch and host readiness boundaries around the scheduler.
with_scheduler(Test) ->
    arweave_sync_test_util:with_mocks(
        [
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
            {arweave_sync_store, record_write_completed, fun(StoreID, States) ->
                meck:passthrough([StoreID, States])
            end},
            {arweave_sync_fetch_worker, run, fun(_) -> ok end},
            {ar_timer, monotonic_ms, fun() -> 0 end}
        ],
        Test
    ).

start_owner(StoreID) ->
    Parent = self(),
    PID = spawn(fun() ->
        arweave_sync_scheduler:reset_store(StoreID),
        ets:insert(arweave_sync_state, {{writer, StoreID}, {self(), self()}}),
        Parent ! {owner_ready, self()},
        owner_loop(Parent)
    end),
    receive
        {owner_ready, PID} -> PID
    end.

owner_loop(Parent) ->
    receive
        {'$gen_cast', Chunk} ->
            case Chunk of
                {admit, _, _, CacheRef} -> ar_chunk_cache:mark_cached(CacheRef);
                _ -> ok
            end,
            Parent ! {chunk, self(), Chunk},
            owner_loop(Parent)
    end.

take_chunk(PID) ->
    receive
        {chunk, PID, Chunk} -> Chunk
    end.

stop_owner(PID) ->
    Ref = erlang:monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', Ref, process, PID, _} -> ok
    end.

fetched(StoreID, Peer, Byte, Proof, TaskRef) ->
    {ok, CacheRef} = ar_chunk_cache:reserve(StoreID),
    arweave_sync_ingest:store_fetched_chunk(
        StoreID,
        Peer,
        Byte,
        Proof,
        TaskRef,
        CacheRef
    ).
