%%% @doc Transient network-sync fetch worker.
%%%
%%% One process per dispatched task, `spawn_monitor`'d by `ar_sync_scheduler`.
%%% It makes at most one chunk request and hands the result to `ar_data_sync`
%%% for storage. The scheduler keeps the task claim active until that handoff
%%% reaches a terminal state.
%%%
%%% Exit contract (read by the dispatcher's `'DOWN'` handler): `normal` covers
%%% both a successful request and a fetch failure; only a genuine crash exits
%%% abnormally. Unfilled bytes in the chunk-sized claim are rediscovered by a later
%%% store sweep.
%%%
%%% `ar_sync_scheduler' checks chunk-cache and disk capacity before spawn. The
%%% worker repeats the cache check before its request because another worker
%%% may fill the cache after selection.
-module(ar_sync_fetch_worker).
-test_category([fast]).

-export([run/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_sync.hrl").

%%%===================================================================
%%% Entry point.
%%%===================================================================

%% @doc Fetch from the task's offset and rate the peer by the bytes it delivered.
run(#task{ peer = Peer } = Task) ->
    %% Fetch timing uses ar_timer so the same accounting follows simulated
    %% and live time. timer:tc remains the wall-clock input to ar_peers.
    {ElapsedUs, {Result, BytesFetched, FetchTiming}} =
        timer:tc(fun() -> fetch_task(Task) end),
    ar_sync_scheduler:task_fetch_completed(Task#task.task_ref, BytesFetched,
        FetchTiming),
    rate_peer(Peer, Result, ElapsedUs, BytesFetched),
    case Result of
        {worker_crash, Class, Reason, Stacktrace} ->
            erlang:raise(Class, Reason, Stacktrace);
        _ ->
            ok
    end,
    ok.

%%%===================================================================
%%% Internal.
%%%===================================================================

%% @doc Report the fetch outcome to ar_peers. A local crash is not booked
%% against the peer. Cache pressure also ends the task without rating it.
rate_peer(Peer, ok, ElapsedUs, BytesFetched) ->
    ar_sync_deps:rate_fetched_data(Peer, chunk, ok, ElapsedUs, BytesFetched);
rate_peer(_Peer, {worker_crash, _, _, _}, _ElapsedUs, _BytesFetched) ->
    ok;
%% A 429 is the peer correctly enforcing its rate policy, not bad data or ill
%% health, and its worker time already feeds the concurrency controller, so it
%% is not booked as an invalid_data failure.
%% Booking it let the politeness equilibrium (high reject ratios at small
%% caps) sink average_success below ?MINIMUM_SUCCESS and remove healthy
%% rate-limited peers (measured 2026-07-13: average_success 0.33 on a peer
%% at cap ~9, one warning from removal).
rate_peer(_Peer, {error, {ok, {{<<"429">>, _}, _, _, _, _}}}, _ElapsedUs,
        _BytesFetched) ->
    ok;
rate_peer(_Peer, cache_full, _ElapsedUs, _BytesFetched) ->
    ok;
rate_peer(Peer, Result, ElapsedUs, _BytesFetched) ->
    ar_sync_deps:rate_fetched_data(Peer, chunk, Result, ElapsedUs, 0).

%% @doc Make at most one chunk request within the task's chunk-sized claim.
%% Returns {Result, BytesFetched, FetchTiming}.
fetch_task(Task) ->
    try do_fetch_task(Task)
    catch Class:Reason:Stacktrace ->
        {{worker_crash, Class, Reason, Stacktrace}, 0, #fetch_timing{}}
    end.

do_fetch_task(Task) ->
    #task{ offset = Offset, peer = Peer, store_id = StoreID } = Task,
    case next_fetch_action(Offset, StoreID) of
        complete ->
            {ok, 0, #fetch_timing{}};
        cache_full ->
            {cache_full, 0, #fetch_timing{}};
        {fetch, FetchOffset} ->
            Byte = FetchOffset - 1,
            Packing = get_target_packing(StoreID),
            {FetchResult, FetchTiming} = timed_fetch(
                Peer, FetchOffset, Packing),
            case FetchResult of
                {ok, #{ chunk := Chunk } = Proof, _Time, _TransferSize} ->
                    TaskRef = Task#task.task_ref,
                    ar_sync_deps:increment_chunk_cache_size(StoreID),
                    ar_sync_deps:store_fetched_chunk(
                        StoreID, Peer, Byte, Proof, TaskRef),
                    {ok, byte_size(Chunk), FetchTiming};
                {error, {ok, {{<<"404">>, _}, _, _, _, _}} = Reason} ->
                    {{error, Reason}, 0, FetchTiming};
                {error, Reason} ->
                    ar_http_iface_client:log_failed_request({error, Reason}, [
                        {event, failed_to_fetch_chunk},
                        {peer, arweave_util:format_peer(Peer)},
                        {start_offset, FetchOffset},
                        {end_offset, Offset + ?DATA_CHUNK_SIZE},
                        {reason, io_lib:format("~p", [Reason])}]),
                    {{error, Reason}, 0, FetchTiming}
            end
    end.

timed_fetch(Peer, FetchOffset, Packing) ->
    StartMs = ar_timer:monotonic_ms(),
    Result = ar_sync_deps:get_chunk_binary(Peer, FetchOffset, Packing),
    ElapsedMs = max(1, ar_timer:monotonic_ms() - StartMs),
    FetchTiming = case Result of
        {ok, _, _, _} ->
            #fetch_timing{ productive_ms = ElapsedMs };
        {error, {ok, {{<<"429">>, _}, _, _, _, _}}} ->
            #fetch_timing{ reject_ms = ElapsedMs };
        {error, timeout} ->
            #fetch_timing{ timeout_ms = ElapsedMs };
        {error, client_error} ->
            #fetch_timing{ client_error_ms = ElapsedMs };
        _ ->
            #fetch_timing{}
    end,
    {Result, FetchTiming}.

next_fetch_action(Offset, StoreID) ->
    End = Offset + ?DATA_CHUNK_SIZE,
    FindAction = fun Scan(Start) ->
        FetchOffset = ar_sync_deps:get_next_not_blacklisted_byte(Start + 1),
        Byte = FetchOffset - 1,
        case Byte >= End of
            true ->
                complete;
            false ->
                case ar_sync_deps:get_next_synced_interval(
                        Byte, End, ar_data_sync, StoreID) of
                    {RecordedEnd, RecordedStart} when RecordedStart =< Byte ->
                        Scan(RecordedEnd);
                    _ ->
                        case ar_sync_deps:is_chunk_cache_full() of
                            true -> cache_full;
                            false -> {fetch, FetchOffset}
                        end
                end
        end
    end,
    FindAction(Offset).

%% @doc Read the target packing for this store, gated by the
%% [sync, request_packed_chunks] config (a cheap ETS read).
get_target_packing(StoreID) ->
    case arweave_config:get([sync, request_packed_chunks]) of
        true -> ar_storage_module:get_packing(StoreID);
        false -> any
    end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    {foreach, fun() -> ok end, fun(_) -> ok end, [
        %% No HTTP fetch:
        fun test_blacklist_past_end_skips_fetch/0,
        fun test_already_recorded_skips_fetch/0,
        fun test_recorded_prefix_continues_with_missing_chunk/0,
        %% HTTP fetch outcomes:
        fun test_success_stores_and_rates_ok/0,
        fun test_legacy_small_chunk_fetches_once/0,
        fun test_full_cache_skips_request/0,
        fun test_404_rates_error_and_does_not_store/0,
        fun test_generic_error_logs_and_rates_error/0,
        fun test_timeout_is_not_retried/0,
        fun test_429_not_booked_as_failure/0,
        %% Worker crash accounting:
        fun test_fetch_crash_reports_zero_bytes/0,
        %% Packing selection:
        fun test_packed_request_selects_store_packing/0
    ]}.

test_blacklist_past_end_skips_fetch() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end,
        [{ar_tx_blacklist, get_next_not_blacklisted_byte,
            fun(_) -> ?DATA_CHUNK_SIZE + 1 end}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(0,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_'))
        end).

test_already_recorded_skips_fetch() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end,
        [{ar_sync_record, get_next_synced_interval,
            fun(_, _, _, _) -> {?DATA_CHUNK_SIZE, 0} end}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(0,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
            ?assertEqual(0,
                meck:num_calls(ar_data_sync, store_fetched_chunk, '_'))
        end).

test_recorded_prefix_continues_with_missing_chunk() ->
    NextSyncedInterval = fun
        (0, _, _, _) -> {100, 0};
        (_, _, _, _) -> not_found
    end,
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end,
        [{ar_sync_record, get_next_synced_interval, NextSyncedInterval}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(1,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
            ?assert(meck:called(ar_http_iface_client, get_chunk_binary,
                [test_peer(), 101, any])),
            ?assertEqual(1,
                meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
            ?assert(meck:called(ar_peers, rate_fetched_data,
                [test_peer(), chunk, ok, '_', 100]))
        end).

test_success_stores_and_rates_ok() ->
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end, fun() ->
        ?assertEqual(ok, run(task(0))),
        ?assertEqual(1, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
        %% Rated by delivered bytes (one 100-byte chunk).
        ?assert(meck:called(ar_peers, rate_fetched_data,
            [test_peer(), chunk, ok, '_', 100]))
    end).

test_legacy_small_chunk_fetches_once() ->
    %% A chunk-sized claim could contain several small legacy chunks. One task
    %% fetches only the first; later store sweeps rediscover the remaining holes.
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end, fun() ->
        ?assertEqual(ok, run(task(0))),
        ?assertEqual(1,
            meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
        ?assertEqual(1,
            meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
        ?assert(meck:called(ar_peers, rate_fetched_data,
            [test_peer(), chunk, ok, '_', 100]))
    end).

test_full_cache_skips_request() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end,
        [{ar_data_sync, is_chunk_cache_full, fun() -> true end}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(1,
                meck:num_calls(ar_data_sync, is_chunk_cache_full, '_')),
            ?assertEqual(0,
                meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
            ?assertEqual(0,
                meck:num_calls(ar_data_sync, store_fetched_chunk, '_'))
        end).

test_404_rates_error_and_does_not_store() ->
    run_with_mocks(
        fun(_, _, _) ->
            {error, {ok, {{<<"404">>, <<>>}, [], <<>>, undefined, undefined}}}
        end,
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assertEqual(0,
                meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
            ?assertEqual(0,
                meck:num_calls(ar_http_iface_client, log_failed_request, '_')),
            ?assert(meck:called(ar_peers, rate_fetched_data,
                ['_', chunk, {error, '_'}, '_', '_']))
        end).

test_generic_error_logs_and_rates_error() ->
    run_with_mocks(fun(_, _, _) -> {error, econnrefused} end, fun() ->
        ?assertEqual(ok, run(task(0))),
        ?assertEqual(0, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
        ?assertEqual(1,
            meck:num_calls(ar_http_iface_client, log_failed_request, '_')),
        ?assert(meck:called(ar_peers, rate_fetched_data,
            ['_', chunk, {error, econnrefused}, '_', '_']))
    end).

test_timeout_is_not_retried() ->
    run_with_mocks(fun(_, _, _) -> {error, timeout} end, fun() ->
        TaskRef = {self(), make_ref()},
        Task = (task(0))#task{ task_ref = TaskRef },
        ?assertEqual(ok, run(Task)),
        ?assertEqual(1, meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
        ?assertEqual(0, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
        receive
            {'$gen_cast', {task_fetch_completed, TaskRef, 0, #fetch_timing{
                        timeout_ms = TimeoutMs }}} ->
                ?assert(TimeoutMs > 0)
        after 0 ->
            ?assert(false)
        end,
        ?assert(meck:called(ar_peers, rate_fetched_data,
            [test_peer(), chunk, {error, timeout}, '_', 0]))
    end).

%% A 429 feeds only the concurrency-cap channel: no delivery credited (none
%% happened) and NO invalid_data failure booked against the peer.
test_429_not_booked_as_failure() ->
    run_with_mocks(
        fun(_, _, _) ->
            {error, {ok, {{<<"429">>, <<"Too Many Requests">>}, [], <<>>,
                    undefined, undefined}}}
        end,
        fun() ->
            TaskRef = {self(), make_ref()},
            Task = (task(0))#task{ task_ref = TaskRef },
            ?assertEqual(ok, run(Task)),
            ?assertEqual(0, meck:num_calls(ar_peers, rate_fetched_data, '_')),
            receive
                {'$gen_cast', {task_fetch_completed, TaskRef, 0, #fetch_timing{
                            productive_ms = 0,
                            reject_ms = RejectMs }}} ->
                    ?assert(RejectMs > 0)
            after 0 ->
                ?assert(false)
            end
        end).

test_fetch_crash_reports_zero_bytes() ->
    run_with_mocks(fun(_, _, _) -> error(simulated_crash) end, fun() ->
        TaskRef = {self(), make_ref()},
        Task = (task(0))#task{ task_ref = TaskRef },
        ?assertException(error, simulated_crash, run(Task)),
        receive
            {'$gen_cast', {task_fetch_completed, TaskRef, 0, #fetch_timing{}}} ->
                ok
        after 0 ->
            ?assert(false)
        end,
        ?assertEqual(0, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
        ?assertEqual(0, meck:num_calls(ar_peers, rate_fetched_data, '_'))
    end).

test_packed_request_selects_store_packing() ->
    Packing = {replica_2_9, <<"addr">>},
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end,
        [{arweave_config, get,
            fun([sync, request_packed_chunks]) -> true;
                (K) -> meck:passthrough([K]) end},
         {ar_storage_module, get_packing, fun(_) -> Packing end}],
        fun() ->
            ?assertEqual(ok, run(task(0))),
            ?assert(meck:called(ar_http_iface_client, get_chunk_binary,
                ['_', '_', Packing]))
        end).

%%%-------------------------------------------------------------------
%%% Test helpers.
%%%-------------------------------------------------------------------

test_peer() -> {1, 2, 3, 4, 1984}.

task(Offset) ->
    #task{ offset = Offset, peer = test_peer(),
        store_id = store1, footprint = none,
        task_ref = {self(), make_ref()} }.

chunk_reply(Size) ->
    {ok, #{ chunk => <<0:(Size * 8)>> }, 1, Size}.

run_with_mocks(GetChunkFun, TestFun) ->
    run_with_mocks(GetChunkFun, [], TestFun).

%% Install the default mocks (an isolated, no-op fetch environment), let
%% ExtraMocks override any default by {Module, Function}, run TestFun, unload.
run_with_mocks(GetChunkFun, ExtraMocks, TestFun) ->
    Defaults = [
        {arweave_config, get,
            fun([sync, request_packed_chunks]) -> false;
                (K) -> meck:passthrough([K]) end},
        {ar_tx_blacklist, get_next_not_blacklisted_byte, fun(X) -> X end},
        {ar_sync_record, get_next_synced_interval, fun(_, _, _, _) -> not_found end},
        {ar_http_iface_client, get_chunk_binary, GetChunkFun},
        {ar_http_iface_client, log_failed_request, fun(_, _) -> ok end},
        {ar_data_sync, store_fetched_chunk, fun(_, _, _, _, _) -> ok end},
        {ar_data_sync, is_chunk_cache_full, fun() -> false end},
        {ar_data_sync, increment_chunk_cache_size, fun(_) -> ok end},
        {ar_peers, rate_fetched_data, fun(_, _, _, _, _) -> ok end}
    ],
    Mocks = merge_mocks(Defaults, ExtraMocks),
    Modules = lists:usort([M || {M, _, _} <- Mocks]),
    lists:foreach(
        fun(M) -> ar_test_util:new_mock(M, [passthrough]) end,
        Modules),
    lists:foreach(
        fun({M, F, Impl}) -> ar_test_util:mock_function(M, F, Impl) end,
        Mocks),
    try TestFun()
    after
        lists:foreach(
            fun ar_test_util:unmock_module/1,
            Modules)
    end.

%% Merge mock specs keyed by {Module, Function}; later specs (ExtraMocks) win.
merge_mocks(Defaults, Extra) ->
    Keyed = lists:foldl(
        fun({M, F, _} = Spec, Acc) -> Acc#{ {M, F} => Spec } end,
        #{}, Defaults ++ Extra),
    maps:values(Keyed).

-endif.
