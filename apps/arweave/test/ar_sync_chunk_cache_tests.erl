-module(ar_sync_chunk_cache_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
-include("ar_sync.hrl").

cache_accounting_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun reservation_round_trip/0,
        fun crashed_store_reclaims_only_its_reservations/0,
        fun default_store_restart_preserves_other_stores/0,
        fun interrupted_cleanup_is_recoverable/0,
        fun local_handoffs_reserve_each_target_store/0
    ]}.

write_owner_restart_test_() ->
    [with_scheduler(fun() -> write_owner_restart(FetchCompleted) end)
        || FetchCompleted <- [false, true]].

local_completions_feed_drain_rate_test_() ->
    with_scheduler(fun local_completions_feed_drain_rate/0).

unavailable_write_owner_test_() ->
    with_scheduler(fun unavailable_write_owner/0).

restart_preserves_other_stores_test_() ->
    with_scheduler(fun restart_preserves_other_stores/0).

restart_preserves_queued_work_test_() ->
    with_scheduler(fun restart_preserves_queued_work/0).

reservation_round_trip() ->
    ?assertEqual({error, not_initialized},
        ar_sync_chunk_cache:reserve(store1)),
    ar_sync_chunk_cache:init(store1),
    ?assertEqual({ok, self()}, ar_sync_chunk_cache:reserve(store1)),
    ?assertEqual(1, ar_sync_chunk_cache:size()),
    ?assertEqual(1, ar_sync_chunk_cache:size(store1)),
    ar_sync_chunk_cache:release(),
    %% An unmatched completion must not decrement an empty cache.
    ar_sync_chunk_cache:release(),
    ?assertEqual(0, ar_sync_chunk_cache:size()),
    ?assertEqual(0, ar_sync_chunk_cache:size(store1)).

crashed_store_reclaims_only_its_reservations() ->
    OldPID = start_owner(store1),
    OtherPID = start_owner(store2),
    ar_sync_chunk_cache:reserve(store1),
    ar_sync_chunk_cache:reserve(store1),
    ar_sync_chunk_cache:reserve(store2),
    ?assertEqual(3, ar_sync_chunk_cache:size()),
    stop_owner(OldPID),
    NewPID = start_owner(store1),
    try
        ?assertEqual(1, ar_sync_chunk_cache:size()),
        ?assertEqual(0, ar_sync_chunk_cache:size(store1)),
        ?assertEqual(1, ar_sync_chunk_cache:size(store2)),
        ?assertEqual({ok, NewPID}, ar_sync_chunk_cache:reserve(store1)),
        %% A late cleanup for the old process cannot reclaim the new chunk.
        ar_sync_chunk_cache:reset(OldPID),
        ?assertEqual(2, ar_sync_chunk_cache:size()),
        ?assertEqual(1, ar_sync_chunk_cache:size(store1))
    after
        stop_owner(NewPID),
        stop_owner(OtherPID)
    end.

default_store_restart_preserves_other_stores() ->
    DefaultPID = start_owner(?DEFAULT_MODULE),
    OtherPID = start_owner(store1),
    ar_sync_chunk_cache:reserve(?DEFAULT_MODULE),
    ar_sync_chunk_cache:reserve(store1),
    stop_owner(DefaultPID),
    NewPID = start_owner(?DEFAULT_MODULE),
    try
        ?assertEqual(1, ar_sync_chunk_cache:size()),
        ?assertEqual(1, ar_sync_chunk_cache:size(store1)),
        ?assertEqual(0, ar_sync_chunk_cache:size(?DEFAULT_MODULE))
    after
        stop_owner(NewPID),
        stop_owner(OtherPID)
    end.

local_handoffs_reserve_each_target_store() ->
    FirstPID = start_owner(store1),
    SecondPID = start_owner(store2),
    try
        ar_data_sync:pack_and_store_chunk(store1, first_chunk),
        ar_data_sync:pack_and_store_chunk(store2, second_chunk),
        ?assertEqual(2, ar_sync_chunk_cache:size()),
        ?assertEqual(1, ar_sync_chunk_cache:size(store1)),
        ?assertEqual(1, ar_sync_chunk_cache:size(store2)),
        ?assertEqual({pack_and_store_chunk, first_chunk}, take_chunk(FirstPID)),
        ?assertEqual({pack_and_store_chunk, second_chunk},
            take_chunk(SecondPID))
    after
        stop_owner(FirstPID),
        stop_owner(SecondPID)
    end.

interrupted_cleanup_is_recoverable() ->
    OwnerPID = start_owner(store1),
    ar_sync_chunk_cache:reserve(store1),
    stop_owner(OwnerPID),
    %% Simulate init being interrupted after removing the old counter but
    %% before publishing its replacement. No separate global total can leak.
    ar_sync_chunk_cache:reset_store(store1),
    ?assertEqual(0, ar_sync_chunk_cache:size()),
    ?assertEqual({error, not_initialized},
        ar_sync_chunk_cache:reserve(store1)),
    NewPID = start_owner(store1),
    try
        ?assertEqual({ok, NewPID}, ar_sync_chunk_cache:reserve(store1)),
        ar_sync_chunk_cache:reset(OwnerPID),
        ?assertEqual(1, ar_sync_chunk_cache:size()),
        ?assertEqual(1, ar_sync_chunk_cache:size(store1))
    after
        stop_owner(NewPID)
    end.

write_owner_restart(FetchCompleted) ->
    Parent = self(),
    OwnerPID = start_owner(store1),
    ar_test_util:mock_function(ar_sync_fetch_worker, run,
        fun(#task{ store_id = StoreID, task_ref = TaskRef }) ->
            ar_data_sync:store_fetched_chunk(StoreID, peer1, 0, #{}, TaskRef),
            Parent ! {fetch_waiting, self(), TaskRef},
            receive continue -> ok end,
            ar_sync_scheduler:task_fetch_completed(
                TaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{})
        end),
    {ok, Scheduler} = ar_sync_scheduler:start_link(),
    Task = #task{ store_id = store1, offset = 0,
        sources = [#task_source{ peer = {1, 1, 1, 1, 1984} }] },
    Enqueue = {claim_and_enqueue, store1, [Task]},
    try
        ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue)),
        {WorkerPID, OldTaskRef} = receive
            {fetch_waiting, PID, Ref} -> {PID, Ref}
        end,
        ?assertMatch({store_fetched_chunk, _, _, _, _}, take_chunk(OwnerPID)),
        ?assertEqual({ok, 0}, gen_server:call(Scheduler, Enqueue)),
        case FetchCompleted of
            true ->
                WorkerPID ! continue,
                ok = ar_test_await:sync_fetches_drained(Scheduler);
            false -> ok
        end,
        stop_owner(OwnerPID),
        %% Cleanup is restart-driven, not a per-task writer monitor.
        ?assertEqual(1, ar_sync_chunk_cache:size()),
        %% Re-admitting this range proves the lost write released its claim.
        NewOwnerPID = start_owner(store1),
        try
            ?assertNot(is_process_alive(WorkerPID)),
            ?assertEqual(0, ar_sync_chunk_cache:size()),
            %% Delayed notifications for the discarded task cannot recreate
            %% a writing task or credit the lost write as completed work.
            ar_sync_scheduler:task_fetch_completed(
                OldTaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{}),
            ar_sync:task_write_completed(store1, OldTaskRef),
            ?assertEqual(pong, gen_server:call(Scheduler, ping)),
            ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue)),
            ?assertEqual(0, meck:num_calls(ar_sync_store,
                record_write_completed, '_'))
        after
            gen_server:stop(Scheduler),
            stop_owner(NewOwnerPID)
        end
    after
        catch gen_server:stop(Scheduler),
        stop_owner(OwnerPID)
    end.

restart_preserves_other_stores() ->
    Parent = self(),
    FirstOwner = start_owner(store1),
    OtherOwner = start_owner(store2),
    ar_test_util:mock_function(ar_sync_fetch_worker, run,
        fun(#task{ store_id = StoreID, task_ref = TaskRef }) ->
            ar_data_sync:store_fetched_chunk(StoreID, peer1, 0, #{}, TaskRef),
            Parent ! {fetch_waiting, StoreID, self(), TaskRef},
            receive continue -> ok end,
            ar_sync_scheduler:task_fetch_completed(
                TaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{})
        end),
    {ok, Scheduler} = ar_sync_scheduler:start_link(),
    FirstTask = #task{ store_id = store1, offset = 0,
        sources = [#task_source{ peer = {1, 1, 1, 1, 1984} }] },
    OtherTask = #task{ store_id = store2, offset = 0,
        sources = [#task_source{ peer = {2, 2, 2, 2, 1984} }] },
    try
        ?assertEqual({ok, 1}, ar_sync_scheduler:claim_and_enqueue(
            store1, [FirstTask])),
        ?assertEqual({ok, 1}, ar_sync_scheduler:claim_and_enqueue(
            store2, [OtherTask])),
        FirstWorker = receive {fetch_waiting, store1, PID1, _} -> PID1 end,
        {OtherWorker, OtherRef} = receive
            {fetch_waiting, store2, PID2, Ref2} -> {PID2, Ref2}
        end,
        stop_owner(FirstOwner),
        NewOwner = start_owner(store1),
        try
            ?assertNot(is_process_alive(FirstWorker)),
            ?assert(is_process_alive(OtherWorker)),
            ?assertEqual(0, ar_sync_chunk_cache:size(store1)),
            ?assertEqual(1, ar_sync_chunk_cache:size(store2)),
            ?assertEqual(1, ar_sync_chunk_cache:size()),
            ?assertEqual({ok, 0}, ar_sync_scheduler:claim_and_enqueue(
                store2, [OtherTask])),
            ?assertEqual({ok, 1}, ar_sync_scheduler:claim_and_enqueue(
                store1, [FirstTask])),
            ?assertEqual(0, meck:num_calls(ar_sync_store,
                record_write_completed, '_')),
            OtherWorker ! continue,
            ar_sync:task_write_completed(store2, OtherRef),
            ?assertEqual(ok, ar_test_await:until(other_store_completed,
                fun() -> meck:num_calls(ar_sync_store,
                    record_write_completed, [store2, '_']) =:= 1 end))
        after
            gen_server:stop(Scheduler),
            stop_owner(NewOwner)
        end
    after
        catch gen_server:stop(Scheduler),
        stop_owner(FirstOwner),
        stop_owner(OtherOwner)
    end.

restart_preserves_queued_work() ->
    Parent = self(),
    OwnerPID = start_owner(store1),
    %% Keep dispatch blocked across the restart so the queued-work contract
    %% is independent of the race between init finishing and dispatching.
    ets:insert(ar_data_sync_state, {chunk_cache_size_limit, 0}),
    ar_test_util:mock_function(ar_sync_fetch_worker, run,
        fun(#task{ task_ref = TaskRef }) ->
            Parent ! {queued_task_started, TaskRef},
            ar_sync_scheduler:task_fetch_completed(
                TaskRef, 0, #fetch_timing{})
        end),
    {ok, Scheduler} = ar_sync_scheduler:start_link(),
    Task = #task{ store_id = store1, offset = 0,
        sources = [#task_source{ peer = {1, 1, 1, 1, 1984} }] },
    try
        ?assertEqual({ok, 1},
            ar_sync_scheduler:claim_and_enqueue(store1, [Task])),
        stop_owner(OwnerPID),
        NewOwner = start_owner(store1),
        try
            %% The original queued claim still exists, so a duplicate cannot
            %% be admitted. Releasing capacity must run that original task.
            ?assertEqual({ok, 0},
                ar_sync_scheduler:claim_and_enqueue(store1, [Task])),
            ets:insert(ar_data_sync_state, {chunk_cache_size_limit, 10}),
            gen_server:cast(Scheduler, dispatch),
            receive {queued_task_started, _} -> ok end,
            ?assertEqual(ok, ar_test_await:sync_fetches_drained(Scheduler))
        after
            gen_server:stop(Scheduler),
            stop_owner(NewOwner)
        end
    after
        catch gen_server:stop(Scheduler),
        stop_owner(OwnerPID)
    end.

local_completions_feed_drain_rate() ->
    {ok, Scheduler} = ar_sync_scheduler:start_link(),
    try
        %% 300 local completions in a ten-second window demonstrate 30 CPS.
        %% No scheduler-owned network task is present in this observation.
        lists:foreach(fun(_) ->
            ar_sync:task_write_completed(store1, undefined)
        end, lists:seq(1, 300)),
        ?assertEqual(pong, gen_server:call(Scheduler, ping)),
        ?assertEqual(300, meck:num_calls(ar_sync_store,
            record_write_completed, [store1, '_'])),
        States = lists:last([Result ||
            {_, {ar_sync_store, record_write_completed, _}, Result}
                <- meck:history(ar_sync_store)]),
        Sampled = ar_sync_store:sample_drain_rates(10_000, States),
        ?assertEqual(30.0, ar_sync_store:drain_rate(store1, Sampled))
    after
        gen_server:stop(Scheduler)
    end.

unavailable_write_owner() ->
    Parent = self(),
    ar_test_util:mock_function(ar_sync_fetch_worker, run,
        fun(#task{ store_id = StoreID, task_ref = TaskRef }) ->
            ar_data_sync:store_fetched_chunk(StoreID, peer1, 0, #{}, TaskRef),
            ar_sync_scheduler:task_fetch_completed(
                TaskRef, ?DATA_CHUNK_SIZE, #fetch_timing{}),
            Parent ! handoff_attempted
        end),
    {ok, Scheduler} = ar_sync_scheduler:start_link(),
    Task = #task{ store_id = store1, offset = 0,
        sources = [#task_source{ peer = {1, 1, 1, 1, 1984} }] },
    Enqueue = {claim_and_enqueue, store1, [Task]},
    try
        ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue)),
        receive handoff_attempted -> ok end,
        ok = ar_test_await:sync_fetches_drained(Scheduler),
        ?assertEqual(0, ar_sync_chunk_cache:size()),
        ?assertEqual(0, meck:num_calls(ar_sync_store,
            record_write_completed, '_')),
        %% The lost handoff must not leave a claim blocking a later retry.
        ?assertEqual({ok, 1}, gen_server:call(Scheduler, Enqueue))
    after
        gen_server:stop(Scheduler)
    end.

setup() ->
    supervisor:terminate_child(ar_data_sync_sup, ar_sync_sup),
    Rows = ets:lookup(ar_data_sync_state, chunk_cache_size_limit)
        ++ ets:match_object(ar_data_sync_state, {{chunk_cache_size, '_'}, '_'})
        ++ ets:match_object(ar_data_sync_state,
            {{chunk_cache_owner, '_'}, '_'}),
    clear_cache_rows(),
    %% Ten slots leave headroom for the single fetched chunk and its worker.
    ets:insert(ar_data_sync_state, {chunk_cache_size_limit, 10}),
    Rows.

cleanup(Rows) ->
    clear_cache_rows(),
    ets:insert(ar_data_sync_state, Rows),
    supervisor:restart_child(ar_data_sync_sup, ar_sync_sup).

clear_cache_rows() ->
    ets:delete(ar_data_sync_state, chunk_cache_size_limit),
    ets:match_delete(ar_data_sync_state, {{chunk_cache_size, '_'}, '_'}),
    ets:match_delete(ar_data_sync_state, {{chunk_cache_owner, '_'}, '_'}).

with_scheduler(Test) ->
    {setup, fun setup/0, fun cleanup/1,
        ar_test_util:with_mocked([
            {ar_data_sync, is_disk_space_sufficient, fun(_) -> true end},
            {ar_sync_store, record_write_completed,
                fun(StoreID, States) ->
                    meck:passthrough([StoreID, States])
                end},
            {ar_sync_fetch_worker, run, fun(_) -> ok end},
            {ar_timer, monotonic_ms, fun() -> 0 end}
        ], Test)}.

start_owner(StoreID) ->
    Parent = self(),
    PID = spawn(fun() ->
        ar_sync_chunk_cache:reset_store(StoreID),
        ar_sync:reset_store(StoreID),
        ar_sync_chunk_cache:init(StoreID),
        Parent ! {owner_ready, self()},
        owner_loop(Parent)
    end),
    receive {owner_ready, PID} -> PID end.

owner_loop(Parent) ->
    receive
        {'$gen_cast', Chunk} ->
            Parent ! {chunk, self(), Chunk},
            owner_loop(Parent)
    end.

take_chunk(PID) ->
    receive {chunk, PID, Chunk} -> Chunk end.

stop_owner(PID) ->
    Ref = erlang:monitor(process, PID),
    exit(PID, kill),
    receive {'DOWN', Ref, process, PID, _} -> ok end.
