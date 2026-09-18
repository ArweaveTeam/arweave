-module(ar_chunk_cache_tests).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
-include("ar_repack.hrl").

chunk_cache_test_() ->
    {setup, fun setup/0, fun cleanup/1,
        {foreach, fun setup_cache/0, fun cleanup_cache/1, [
            {timeout, 30, fun round_trip/0},
            {timeout, 30, fun atomic_admission/0},
            {timeout, 30, fun transfer_at_capacity/0},
            {timeout, 30, fun batch_reference_lifetimes/0},
            {timeout, 30, fun batch_transfer_rejects_expired/0},
            {timeout, 30, fun batch_transfer_owner_exit/0},
            {timeout, 30, fun referenced_payload_survives_producer_exit/0},
            {timeout, 30, fun last_holder_exit_reclaims/0},
            {timeout, 30, fun coordinator_restart_preserves_payloads/0},
            {timeout, 30, fun late_release_is_harmless/0},
            {timeout, 30, fun all_sources_share_capacity/0},
            {timeout, 30, fun transformation_allowance/0},
            {timeout, 30, fun joint_sizing/0},
            {timeout, 30, fun per_store_backlog_and_completions/0},
            {timeout, 30, fun release_during_coordinator_restart/0},
            {timeout, 30, fun shrink_preserves_existing_work/0},
            {timeout, 30, fun configured_capacity/0},
            {timeout, 30, fun repack_read_write_backpressure/0},
            {timeout, 30, fun rejected_budget_keeps_live_limit/0}
        ]}}.

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Reservation, caching and repeated release keep global and per-store
%% counts consistent.
round_trip() ->
    {ok, CacheRef} = ar_chunk_cache:reserve(store1),
    ?assertEqual(1, ar_chunk_cache:reserved_size()),
    ?assertEqual(0, ar_chunk_cache:cached_size()),
    ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
    ar_chunk_cache:mark_cached(CacheRef),
    ar_chunk_cache:mark_cached(CacheRef),
    ?assertEqual(1, ar_chunk_cache:reserved_size()),
    ?assertEqual(1, ar_chunk_cache:cached_size()),
    ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
    ar_chunk_cache:release(CacheRef),
    ar_chunk_cache:release(CacheRef),
    ?assertEqual(0, ar_chunk_cache:reserved_size()),
    ?assertEqual(0, ar_chunk_cache:cached_size()),
    ?assertEqual(0, ar_chunk_cache:cached_size(store1)).

%% @doc Concurrent reservations cannot admit more chunks than the shared cache
%% limit.
atomic_admission() ->
    Parent = self(),
    %% More contenders than the cache can hold exercises atomic admission.
    PIDs = [
        spawn(fun() ->
            Result = ar_chunk_cache:reserve(store1),
            Parent ! {admitted, self(), Result},
            receive
                stop -> ok
            end
        end)
     || _ <- lists:seq(1, 32)
    ],
    try
        Results = [
            receive
                {admitted, PID, R} -> R
            end
         || PID <- PIDs
        ],
        ?assertEqual(3, length([T || {ok, T} <- Results])),
        ?assertEqual(3, ar_chunk_cache:reserved_size()),
        ?assertEqual(full, ar_chunk_cache:reserve(store2))
    after
        lists:foreach(fun(PID) -> PID ! stop end, PIDs)
    end.

%% @doc Transferring and sharing an existing reservation need no additional
%% capacity.
transfer_at_capacity() ->
    {ok, CacheRef} = ar_chunk_cache:reserve(store1, 3),
    ?assert(ar_chunk_cache:is_full()),
    {ok, Next} = ar_chunk_cache:transfer(CacheRef, self()),
    {ok, Packing} = ar_chunk_cache:add_reference(Next, self()),
    ?assertNotEqual(CacheRef, Next),
    ?assertNotEqual(Next, Packing),
    ?assertEqual(3, ar_chunk_cache:reserved_size()),
    ar_chunk_cache:release(Next),
    ?assert(ar_chunk_cache:is_full()),
    ar_chunk_cache:release(Packing),
    ?assertEqual(0, ar_chunk_cache:reserved_size()),
    ?assertNot(ar_chunk_cache:is_full()).

%% @doc Batch handoffs preserve independent holders without reserving capacity.
batch_reference_lifetimes() ->
    %% Three holders share one full-cache reservation.
    {ok, CacheRef} = ar_chunk_cache:reserve(store1, 3),
    {ok, References} = ar_chunk_cache:add_references(CacheRef, self(), 3),
    ?assertEqual(3, length(lists:usort(References))),
    ar_chunk_cache:release(CacheRef),
    {ok, [First, Second, Last]} = ar_chunk_cache:transfer_many(
        References, self()
    ),
    lists:foreach(fun ar_chunk_cache:release/1, References),
    ar_chunk_cache:release(First),
    ar_chunk_cache:release(Second),
    ?assertEqual(3, ar_chunk_cache:reserved_size()),
    ar_chunk_cache:release(Last),
    ?assertEqual(0, ar_chunk_cache:reserved_size()).

%% @doc An expired or repeated source rejects the batch without moving live holds.
batch_transfer_rejects_expired() ->
    {ok, Live} = ar_chunk_cache:reserve(store1),
    {ok, Expired} = ar_chunk_cache:reserve(store2),
    ar_chunk_cache:release(Expired),
    ?assertEqual(
        {error, expired},
        ar_chunk_cache:transfer_many([Live, Expired], self())
    ),
    ?assertEqual(
        {error, expired},
        ar_chunk_cache:transfer_many([Live, Live], self())
    ),
    ?assertEqual(
        {error, expired},
        ar_chunk_cache:add_references(Expired, self(), 2)
    ),
    ?assertEqual({ok, []}, ar_chunk_cache:transfer_many([], self())),
    ?assertEqual({ok, []}, ar_chunk_cache:add_references(Live, self(), 0)),
    {ok, Held} = ar_chunk_cache:add_reference(Live, self()),
    ar_chunk_cache:release(Live),
    ?assertEqual(1, ar_chunk_cache:reserved_size()),
    ar_chunk_cache:release(Held),
    ?assertEqual(0, ar_chunk_cache:reserved_size()).

%% @doc The new owner's exit releases every reservation transferred in a batch.
batch_transfer_owner_exit() ->
    Holder = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    try
        {ok, First} = ar_chunk_cache:reserve(store1),
        {ok, Second} = ar_chunk_cache:reserve(store2),
        ?assertMatch(
            {ok, [_, _]},
            ar_chunk_cache:transfer_many([First, Second], Holder)
        ),
        ar_chunk_cache:release(First),
        ar_chunk_cache:release(Second),
        %% Both stores remain reserved until the batch's new owner exits.
        ?assertEqual(2, ar_chunk_cache:reserved_size()),
        stop_process(Holder),
        ?assertEqual(
            ok,
            ar_test_await:until(
                batch_owner_reclaimed,
                fun() -> ar_chunk_cache:reserved_size() =:= 0 end
            )
        )
    after
        exit(Holder, kill)
    end.

%% @doc A surviving holder keeps the reservation after its producer exits.
referenced_payload_survives_producer_exit() ->
    Parent = self(),
    Producer = spawn(fun() ->
        {ok, CacheRef} = ar_chunk_cache:reserve(store1),
        {ok, Held} = ar_chunk_cache:add_reference(CacheRef, Parent),
        Parent ! {held, Held},
        receive
            stop -> ok
        end
    end),
    Held =
        receive
            {held, T} -> T
        end,
    stop_process(Producer),
    ?assertEqual(1, ar_chunk_cache:reserved_size()),
    ar_chunk_cache:release(Held),
    ?assertEqual(
        ok,
        ar_test_await:until(
            cache_drained,
            fun() -> ar_chunk_cache:reserved_size() =:= 0 end
        )
    ).

%% @doc The last holder's exit releases its reservation without affecting
%% another store.
last_holder_exit_reclaims() ->
    {ok, Other} = ar_chunk_cache:reserve(store2),
    Holder = spawn(fun() ->
        receive
            stop -> ok
        end
    end),
    {ok, CacheRef} = ar_chunk_cache:reserve(store1),
    {ok, _} = ar_chunk_cache:transfer(CacheRef, Holder),
    stop_process(Holder),
    ?assertEqual(
        ok,
        ar_test_await:until(
            owner_reclaimed,
            fun() -> ar_chunk_cache:reserved_size() =:= 1 end
        )
    ),
    ar_chunk_cache:release(Other),
    ?assertEqual(0, ar_chunk_cache:reserved_size()).

%% @doc Restarting the coordinator preserves live reservations and cached
%% counts.
coordinator_restart_preserves_payloads() ->
    {ok, CacheRef} = ar_chunk_cache:reserve(store1),
    ar_chunk_cache:mark_cached(CacheRef),
    gen_server:stop(ar_chunk_cache),
    %% Single-reference wrappers preserve errors without consuming live holds.
    ?assertEqual(
        {error, expired}, ar_chunk_cache:add_reference(CacheRef, self())
    ),
    ?assertEqual(
        {error, expired}, ar_chunk_cache:transfer(CacheRef, self())
    ),
    {ok, _} = ar_chunk_cache:start_link(),
    ?assertEqual(1, ar_chunk_cache:reserved_size()),
    ?assertEqual(1, ar_chunk_cache:cached_size(store1)),
    ar_chunk_cache:release(CacheRef),
    ?assertEqual(0, ar_chunk_cache:reserved_size()).

%% @doc A release while the coordinator is stopped is reflected after restart.
release_during_coordinator_restart() ->
    {ok, CacheRef} = ar_chunk_cache:reserve(store1),
    gen_server:stop(ar_chunk_cache),
    ar_chunk_cache:release(CacheRef),
    {ok, _} = ar_chunk_cache:start_link(),
    ?assertEqual(0, ar_chunk_cache:reserved_size()).

%% @doc An expired reference cannot release or extend a transferred reservation.
late_release_is_harmless() ->
    {ok, Old} = ar_chunk_cache:reserve(store1),
    {ok, Current} = ar_chunk_cache:transfer(Old, self()),
    ar_chunk_cache:release(Old),
    ?assertEqual({error, expired}, ar_chunk_cache:add_reference(Old, self())),
    ?assertEqual({error, expired}, ar_chunk_cache:transfer(Old, self())),
    ?assertEqual(1, ar_chunk_cache:reserved_size()),
    ar_chunk_cache:release(Current),
    ?assertEqual(0, ar_chunk_cache:reserved_size()).

%% @doc Sync, copy, repack and disk-pool reservations compete for one cache
%% budget.
all_sources_share_capacity() ->
    {ok, Sync} = ar_chunk_cache:reserve(sync_store),
    {ok, Copy} = ar_chunk_cache:reserve(copy_store),
    {ok, Repack} = ar_chunk_cache:reserve(repack_store),
    ?assertEqual(full, ar_chunk_cache:reserve(disk_pool_store)),
    ar_chunk_cache:release(Copy),
    {ok, DiskPool} = ar_chunk_cache:reserve(disk_pool_store),
    ?assertEqual(3, ar_chunk_cache:reserved_size()),
    lists:foreach(fun ar_chunk_cache:release/1, [Sync, Repack, DiskPool]).

%% @doc Each reservation budgets for input, unpacked data and packed output.
transformation_allowance() ->
    %% A lifecycle can hold input, unpacked intermediate and packed output.
    ?assertEqual(3 * ?DATA_CHUNK_SIZE, ar_chunk_cache:reservation_bytes()),
    {Bytes, _} = ar_chunk_cache:limits(64 * ?GiB, 4 * ?GiB, 1),
    ?assertEqual(?MiB, Bytes).

%% @doc Default cache sizing leaves room for entropy, interval metadata and
%% other services.
joint_sizing() ->
    {Large, Intervals} = ar_chunk_cache:limits(64 * ?GiB, 4 * ?GiB, undefined),
    ?assertEqual((2000 - 256) * 3 * ?DATA_CHUNK_SIZE, Large),
    {Small, Intervals} = ar_chunk_cache:limits(4 * ?GiB, ?GiB, undefined),
    %% Half of RAM is reserved for other services; entropy and metadata are
    %% subtracted before deriving chunk capacity.
    ?assertEqual(?GiB - Intervals, Small),
    ?assertEqual(64 * ?MiB, Intervals).

%% @doc Cached counts drain on release while per-store completion totals remain
%% cumulative.
per_store_backlog_and_completions() ->
    {ok, CacheRef} = ar_chunk_cache:reserve(store1),
    ar_chunk_cache:mark_cached(CacheRef),
    ar_chunk_cache:record_completed(store1),
    ar_chunk_cache:record_completed(store1),
    ?assertEqual(2, ar_chunk_cache:completed(store1)),
    ?assertEqual(0, ar_chunk_cache:completed(store2)),
    ar_chunk_cache:release(CacheRef),
    ?assertEqual(0, ar_chunk_cache:cached_size(store1)),
    ?assertEqual(2, ar_chunk_cache:completed(store1)).

%% @doc Shrinking the cache blocks new work without revoking existing
%% reservations.
shrink_preserves_existing_work() ->
    {ok, CacheRef} = ar_chunk_cache:reserve(store1, 3),
    %% One MiB fits one complete lifecycle; shrinking cannot revoke payloads.
    ar_chunk_cache:configure([packing, cache_size], 1),
    ?assertEqual(full, ar_chunk_cache:reserve(store2)),
    ?assertEqual(1, ar_chunk_cache:limit()),
    ?assertEqual(3, ar_chunk_cache:reserved_size()),
    {ok, Next} = ar_chunk_cache:transfer(CacheRef, self()),
    ar_chunk_cache:release(Next),
    {ok, Last} = ar_chunk_cache:reserve(store2),
    ?assertEqual(full, ar_chunk_cache:reserve(store1)),
    ar_chunk_cache:release(Last).

%% @doc The configured MiB budget determines capacity including transformation
%% memory.
configured_capacity() ->
    arweave_config:internal_with_test_config(fun() ->
        %% Each reservation includes three 256 KiB representations, so
        %% three MiB admits four chunks and nine MiB admits twelve.
        ok = arweave_config:internal_force_config(#{
            [packing, cache_size] => 3
        }),
        gen_server:stop(ar_chunk_cache),
        {ok, _} = ar_chunk_cache:start_link(),
        ?assertEqual(4, ar_chunk_cache:limit()),
        ok = arweave_config:internal_force_config(#{
            [packing, cache_size] => 9
        }),
        gen_server:stop(ar_chunk_cache),
        {ok, _} = ar_chunk_cache:start_link(),
        ?assertEqual(12, ar_chunk_cache:limit())
    end).

%% @doc Repack reads wait for cache capacity and resume after a queued write
%% releases it.
repack_read_write_backpressure() ->
    %% The cache fits one chunk; writing the first must unblock the second read.
    ets:insert(ar_chunk_cache, {limit, 1}),
    Parent = self(),
    ar_test_util:new_mock(arweave_storage, [passthrough]),
    ar_test_util:new_mock(ar_data_sync, [passthrough]),
    ar_test_util:mock_function(
        arweave_storage,
        get_chunk_range,
        fun(_, Bytes, _) ->
            Parent ! {disk_read, Bytes},
            []
        end
    ),
    ar_test_util:mock_function(
        ar_data_sync,
        get_chunk_metadata_range,
        fun(_, _, _) -> {ok, #{}} end
    ),
    ar_test_util:mock_function(
        arweave_storage,
        store_entropy,
        fun(_, _, _, _) ->
            Parent ! written,
            ok
        end
    ),
    {ok, IO} = ar_repack_io:start_link(
        ar_repack_io:name(?DEFAULT_MODULE), ?DEFAULT_MODULE
    ),
    try
        Step = ?DATA_CHUNK_SIZE,
        ar_repack_io:read_footprint(
            [Step, 2 * Step],
            1,
            3 * Step,
            1,
            ?DEFAULT_MODULE
        ),
        CacheRef =
            receive
                {'$gen_cast', {chunk_range_read, [Step], _, _, Held}} -> Held
            end,
        receive
            {disk_read, Bytes} -> ?assert(Bytes =< Step)
        end,
        %% This notification proves the next admission was attempted.
        receive
            {'$gen_cast', flush_write_queue} -> ok
        end,
        receive
            {disk_read, _} -> ?assert(false, "Read without admission")
        after 0 -> ok
        end,
        Chunk = #repack_chunk{
            cache_ref = CacheRef,
            state = write_entropy,
            target_entropy = <<0>>,
            offsets = #chunk_offsets{bucket_end_offset = Step}
        },
        ok = ar_repack_io:write_queue(
            gb_sets:from_list([{Step, Chunk}]),
            {replica_2_9, <<0:256>>},
            ?DEFAULT_MODULE
        ),
        receive
            written -> ok
        end,
        receive
            {'$gen_cast', {chunk_range_read, [Offset], _, _, Next}} ->
                ?assertEqual(2 * Step, Offset),
                ar_chunk_cache:release(Next)
        end,
        ?assertEqual(0, ar_chunk_cache:reserved_size())
    after
        gen_server:stop(IO),
        lists:foreach(
            fun ar_test_util:unmock_module/1,
            [arweave_storage, ar_data_sync]
        )
    end.

%% @doc Rejecting an oversized memory budget leaves the configured and active
%% limits intact.
rejected_budget_keeps_live_limit() ->
    %% A fixed four-GiB machine makes rejection independent of the host OS.
    Total = 4 * ?GiB,
    ar_test_util:new_mock(arweave_util, [passthrough]),
    ar_test_util:mock_function(arweave_util, system_memory, fun() -> Total end),
    try
        arweave_config:internal_with_test_config(fun() ->
            ok = arweave_config:internal_force_config(#{
                [packing, entropy, cache_size] => 0
            }),
            ok = arweave_config:runtime(),
            ok = arweave_config:set([packing, cache_size], 1),
            {ok, CacheRef} = ar_chunk_cache:reserve(store1),
            %% Exceed the reported total by one MiB to force rejection.
            TooLargeMiB = Total div ?MiB + 1,
            ?assertMatch(
                {error, _},
                arweave_config:set([packing, cache_size], TooLargeMiB)
            ),
            ?assertEqual(1, arweave_config:get([packing, cache_size])),
            ?assertEqual(full, ar_chunk_cache:reserve(store2)),
            ?assertEqual(1, ar_chunk_cache:limit()),
            ar_chunk_cache:release(CacheRef)
        end)
    after
        ar_test_util:unmock_module(arweave_util)
    end.

%%====================================================================
%% Helpers
%%====================================================================

setup() ->
    %% The EUnit runner starts the host; stop it so each case owns its cache.
    Config = arweave_config:internal_snapshot(),
    ok = application:stop(arweave),
    Config.

cleanup(Config) ->
    %% Restart dependencies too: host startup registers its metrics again.
    ar:stop_dependencies(),
    ok = arweave_config:internal_restore(Config#{runtime => false}),
    ar:start_dependencies().

setup_cache() ->
    ar_chunk_cache:create_ets(),
    {ok, PID} = ar_chunk_cache:start_link(),
    %% Three chunks distinguish a full cache from a partially released one.
    ets:insert(ar_chunk_cache, {limit, 3}),
    PID.

cleanup_cache(_) ->
    case whereis(ar_chunk_cache) of
        undefined -> ok;
        PID -> gen_server:stop(PID)
    end,
    ets:delete(ar_chunk_cache).

stop_process(PID) ->
    Ref = monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', Ref, process, PID, _} -> ok
    end.
