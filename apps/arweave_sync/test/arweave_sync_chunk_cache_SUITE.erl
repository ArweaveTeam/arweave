-module(arweave_sync_chunk_cache_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        local_completions_feed_write_rate,
        missing_chunk_writer_releases_claim
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

init_per_testcase(_, Config) ->
    arweave_sync_deps:override_module(arweave_sync_deps_mainnet),
    ar_chunk_cache:create_ets(),
    {ok, Cache} = ar_chunk_cache:start_link(),
    %% Ten chunks leave headroom for the fetched chunk and its worker.
    ets:insert(ar_chunk_cache, {limit, 10}),
    [{chunk_cache, Cache} | Config].

end_per_testcase(_, Config) ->
    gen_server:stop(proplists:get_value(chunk_cache, Config)),
    ets:delete(ar_chunk_cache),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Local storage completions contribute to the measured write rate.
local_completions_feed_write_rate(_Config) ->
    with_scheduler(fun() ->
        %% A buffered local write keeps this observation backlogged.
        {ok, CacheRef} = ar_chunk_cache:reserve(store1),
        ar_chunk_cache:mark_cached(CacheRef),
        {ok, 1, States} = arweave_sync_store:admit(
            store1,
            #task{offset = 0, sources = []},
            arweave_sync_store:new()
        ),
        Baseline = arweave_sync_store:sample_write_rates(0, States),
        %% 300 actual storage completions in ten seconds demonstrate 30 CPS.
        lists:foreach(
            fun(_) -> ar_chunk_cache:record_completed(store1) end,
            lists:seq(1, 300)
        ),
        Sampled = arweave_sync_store:sample_write_rates(10_000, Baseline),
        ?assertEqual(30.0, arweave_sync_store:write_rate(store1, Sampled)),
        ar_chunk_cache:release(CacheRef)
    end).

%% @doc A missing chunk writer releases cache capacity and lets the range
%% retry.
missing_chunk_writer_releases_claim(_Config) ->
    with_scheduler(fun() ->
        Parent = self(),
        meck:expect(
            arweave_sync_fetch_worker,
            run,
            fun(#task{store_id = StoreID, task_ref = TaskRef}) ->
                fetched(StoreID, peer1, 0, #{}, TaskRef),
                arweave_sync_scheduler:report_fetch_completed(
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

fetched(StoreID, Peer, Byte, Proof, TaskRef) ->
    {ok, CacheRef} = ar_chunk_cache:reserve(StoreID),
    arweave_sync_chunk_writer:store_fetched_chunk(
        StoreID,
        Peer,
        Byte,
        Proof,
        TaskRef,
        CacheRef
    ).
