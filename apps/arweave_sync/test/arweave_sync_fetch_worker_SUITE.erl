-module(arweave_sync_fetch_worker_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("stdlib/include/assert.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-import(arweave_sync_fetch_worker, [
    run/1
]).

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        test_blacklist_past_end_skips_fetch,
        test_already_recorded_skips_fetch,
        test_recorded_prefix_continues_with_missing_chunk,
        test_success_stores_and_rates_ok,
        test_legacy_small_chunk_fetches_once,
        test_full_cache_skips_request,
        test_404_rates_error_and_does_not_store,
        test_generic_error_logs_and_rates_error,
        test_timeout_is_not_retried,
        test_429_not_booked_as_failure,
        test_fetch_crash_reports_zero_bytes,
        test_packed_request_selects_store_packing
    ].

init_per_testcase(_Case, Config) ->
    %% The host services selected by mainnet dependencies are mocked per case.
    arweave_sync_deps:override_module(arweave_sync_deps_mainnet),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc A blacklist covering the task range prevents any HTTP fetch.
test_blacklist_past_end_skips_fetch(_Config) ->
    run_with_mocks(
        fun(_, _, _) -> error(should_not_fetch) end,
        [
            {ar_tx_blacklist, get_next_not_blacklisted_byte, fun(_) ->
                ?DATA_CHUNK_SIZE + 1
            end}
        ],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(
                0,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')
            )
        end
    ).

%% @doc An already-synced task performs neither an HTTP fetch nor ingestion.
test_already_recorded_skips_fetch(_Config) ->
    run_with_mocks(
        fun(_, _, _) -> error(should_not_fetch) end,
        [
            {arweave_storage, get_next_interval, fun(
                synced, _, _, any_packing, _, _
            ) ->
                {?DATA_CHUNK_SIZE, 0}
            end}
        ],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(
                0,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')
            ),
            ?assertEqual(
                0,
                meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
            )
        end
    ).

%% @doc A stored prefix is skipped so the worker fetches and credits the first
%% missing chunk.
test_recorded_prefix_continues_with_missing_chunk(_Config) ->
    NextSyncedInterval = fun
        (synced, 0, _, any_packing, _, _) -> {100, 0};
        (synced, _, _, any_packing, _, _) -> not_found
    end,
    run_with_mocks(
        fun(_, _, _) -> chunk_reply(100) end,
        [{arweave_storage, get_next_interval, NextSyncedInterval}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(
                1,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')
            ),
            ?assert(
                meck:called(
                    ar_http_iface_client,
                    get_chunk_binary,
                    [test_peer(), 101, any]
                )
            ),
            ?assertEqual(
                1,
                meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
            ),
            ?assert(
                meck:called(
                    ar_peers,
                    rate_fetched_data,
                    [test_peer(), chunk, ok, '_', 100]
                )
            )
        end
    ).

%% @doc A successful response is handed to ingestion and credited by delivered
%% bytes.
test_success_stores_and_rates_ok(_Config) ->
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end, fun() ->
        ?assertEqual(ok, run(task(0))),
        ?assertEqual(
            1, meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
        ),
        %% Rated by delivered bytes (one 100-byte chunk).
        ?assert(
            meck:called(
                ar_peers,
                rate_fetched_data,
                [test_peer(), chunk, ok, '_', 100]
            )
        )
    end).

%% @doc A legacy small chunk completes after one fetch without retrying the
%% remaining task span.
test_legacy_small_chunk_fetches_once(_Config) ->
    %% A chunk-sized claim could contain several small legacy chunks. One task
    %% fetches only the first; later store sweeps rediscover the remaining holes.
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end, fun() ->
        ?assertEqual(ok, run(task(0))),
        ?assertEqual(
            1,
            meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')
        ),
        ?assertEqual(
            1,
            meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
        ),
        ?assert(
            meck:called(
                ar_peers,
                rate_fetched_data,
                [test_peer(), chunk, ok, '_', 100]
            )
        )
    end).

%% @doc A full shared cache prevents the worker from starting an HTTP request.
test_full_cache_skips_request(_Config) ->
    run_with_mocks(
        fun(_, _, _) -> error(should_not_fetch) end,
        [{ar_chunk_cache, is_full, fun() -> true end}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(
                1,
                meck:num_calls(ar_chunk_cache, is_full, '_')
            ),
            ?assertEqual(
                0,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')
            ),
            ?assertEqual(
                0,
                meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
            )
        end
    ).

%% @doc A 404 records a peer error without ingestion or failed-request logging.
test_404_rates_error_and_does_not_store(_Config) ->
    run_with_mocks(
        fun(_, _, _) ->
            {error, {ok, {{<<"404">>, <<>>}, [], <<>>, undefined, undefined}}}
        end,
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(
                0,
                meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
            ),
            ?assertEqual(
                0,
                meck:num_calls(ar_http_iface_client, log_failed_request, '_')
            ),
            ?assert(
                meck:called(
                    ar_peers,
                    rate_fetched_data,
                    ['_', chunk, {error, '_'}, '_', '_']
                )
            )
        end
    ).

%% @doc A transport error is logged and rated without handing data to ingestion.
test_generic_error_logs_and_rates_error(_Config) ->
    run_with_mocks(fun(_, _, _) -> {error, econnrefused} end, fun() ->
        ?assertEqual(ok, run(task(0))),
        ?assertEqual(
            0, meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
        ),
        ?assertEqual(
            1,
            meck:num_calls(ar_http_iface_client, log_failed_request, '_')
        ),
        ?assert(
            meck:called(
                ar_peers,
                rate_fetched_data,
                ['_', chunk, {error, econnrefused}, '_', '_']
            )
        )
    end).

%% @doc A timed-out request reports zero bytes and timeout duration without
%% retrying.
test_timeout_is_not_retried(_Config) ->
    run_with_mocks(fun(_, _, _) -> {error, timeout} end, fun() ->
        TaskRef = {self(), make_ref()},
        Task = (task(0))#task{task_ref = TaskRef},
        ?assertEqual(ok, run(Task)),
        ?assertEqual(
            1, meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')
        ),
        ?assertEqual(
            0, meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
        ),
        receive
            {'$gen_cast',
                {task_fetch_completed, TaskRef, 0, #fetch_timing{
                    timeout_ms = TimeoutMs
                }}} ->
                ?assert(TimeoutMs > 0)
        after 0 ->
            ?assert(false)
        end,
        ?assert(
            meck:called(
                ar_peers,
                rate_fetched_data,
                [test_peer(), chunk, {error, timeout}, '_', 0]
            )
        )
    end).

%% @doc A 429 feeds only the concurrency-cap channel: no delivery credited (none
%% happened) and NO invalid_data failure booked against the peer.
test_429_not_booked_as_failure(_Config) ->
    run_with_mocks(
        fun(_, _, _) ->
            {error,
                {ok, {
                    {<<"429">>, <<"Too Many Requests">>},
                    [],
                    <<>>,
                    undefined,
                    undefined
                }}}
        end,
        fun() ->
            TaskRef = {self(), make_ref()},
            Task = (task(0))#task{task_ref = TaskRef},
            ?assertEqual(ok, run(Task)),
            ?assertEqual(0, meck:num_calls(ar_peers, rate_fetched_data, '_')),
            receive
                {'$gen_cast',
                    {task_fetch_completed, TaskRef, 0, #fetch_timing{
                        productive_ms = 0,
                        reject_ms = RejectMs
                    }}} ->
                    ?assert(RejectMs > 0)
            after 0 ->
                ?assert(false)
            end
        end
    ).

%% @doc A fetch exception reports zero completion bytes before propagating the
%% crash.
test_fetch_crash_reports_zero_bytes(_Config) ->
    run_with_mocks(fun(_, _, _) -> error(simulated_crash) end, fun() ->
        TaskRef = {self(), make_ref()},
        Task = (task(0))#task{task_ref = TaskRef},
        ?assertException(error, simulated_crash, run(Task)),
        receive
            {'$gen_cast', {task_fetch_completed, TaskRef, 0, #fetch_timing{}}} ->
                ok
        after 0 ->
            ?assert(false)
        end,
        ?assertEqual(
            0, meck:num_calls(arweave_sync_ingest, store_fetched_chunk, '_')
        ),
        ?assertEqual(0, meck:num_calls(ar_peers, rate_fetched_data, '_'))
    end).

%% @doc Packed-chunk requests use the destination store's configured packing.
test_packed_request_selects_store_packing(_Config) ->
    Packing = {replica_2_9, <<"addr">>},
    run_with_mocks(
        fun(_, _, _) -> chunk_reply(100) end,
        [
            {arweave_config, get, fun
                ([sync, request_packed_chunks]) -> true;
                (K) -> meck:passthrough([K])
            end},
            {arweave_storage, store_info, fun(_) ->
                #store_info{packing = Packing}
            end}
        ],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assert(
                meck:called(
                    ar_http_iface_client,
                    get_chunk_binary,
                    ['_', '_', Packing]
                )
            )
        end
    ).

%%====================================================================
%% Helpers
%%====================================================================

test_peer() -> {1, 2, 3, 4, 1984}.

task(Offset) ->
    #task{
        offset = Offset,
        peer = test_peer(),
        store_id = store1,
        footprint = none,
        task_ref = {self(), make_ref()}
    }.

chunk_reply(Size) ->
    {ok, #{chunk => <<0:(Size * 8)>>}, 1, Size}.

run_with_mocks(GetChunkFun, TestFun) ->
    run_with_mocks(GetChunkFun, [], TestFun).

%% @doc Run a case with default dependency mocks replaced by any extra mocks.
run_with_mocks(GetChunkFun, ExtraMocks, TestFun) ->
    Defaults = [
        {arweave_config, get, fun
            ([sync, request_packed_chunks]) -> false;
            (K) -> meck:passthrough([K])
        end},
        {ar_tx_blacklist, get_next_not_blacklisted_byte, fun(X) -> X end},
        {arweave_storage, get_next_interval, fun(
            synced, _, _, any_packing, _, _
        ) ->
            not_found
        end},
        {ar_http_iface_client, get_chunk_binary, GetChunkFun},
        {ar_http_iface_client, log_failed_request, fun(_, _) -> ok end},
        {arweave_sync_ingest, store_fetched_chunk, fun(_, _, _, _, _, _) ->
            ok
        end},
        {ar_chunk_cache, is_full, fun() -> false end},
        {ar_chunk_cache, reserve, fun(_) -> {ok, make_ref()} end},
        {ar_chunk_cache, release, fun(_) -> ok end},
        {ar_peers, rate_fetched_data, fun(_, _, _, _, _) -> ok end}
    ],
    Mocks = merge_mocks(Defaults, ExtraMocks),
    Modules = lists:usort([M || {M, _, _} <- Mocks]),
    lists:foreach(
        fun(M) -> meck:new(M, [passthrough]) end,
        Modules
    ),
    try
        lists:foreach(
            fun({M, F, Impl}) -> meck:expect(M, F, Impl) end,
            Mocks
        ),
        TestFun()
    after
        lists:foreach(
            fun meck:unload/1,
            Modules
        )
    end.

%% @doc Merge mocks by module and function, with extra mocks taking precedence.
merge_mocks(Defaults, Extra) ->
    Keyed = lists:foldl(
        fun({M, F, _} = Spec, Acc) -> Acc#{{M, F} => Spec} end,
        #{},
        Defaults ++ Extra
    ),
    maps:values(Keyed).
