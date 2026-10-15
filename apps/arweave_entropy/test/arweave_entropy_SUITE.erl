-module(arweave_entropy_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_lib/include/arweave_lib_constants.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        owns_tables,
        cache_api_preserves_eviction,
        cached_and_uncached_generation,
        slice_is_detached_from_entropy,
        failed_generation_releases_lock,
        concurrent_generation_is_shared,
        dead_generator_does_not_block_reuse,
        failing_footprint_is_skipped
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_entropy),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    Snapshot = arweave_config:internal_snapshot(),
    %% Leave space for two full entropies so this tests reuse, not eviction.
    LimitMiB = (2 * ?REPLICA_2_9_ENTROPY_SIZE + ?MiB - 1) div ?MiB,
    ok = arweave_config:internal_force_config(#{
        [packing, entropy, cache_size] => LimitMiB
    }),
    ets:delete_all_objects(entropy_generation_stats),
    ok = arweave_entropy:internal_clear_cache(),
    meck:new(mocked_modules(), [passthrough, no_link]),
    meck:expect(
        arweave_lib_replica_2_9,
        get_entropy_key,
        fun(_, _, _) -> <<"key">> end
    ),
    meck:expect(
        ar_packing_server,
        get_packing_state,
        fun() -> {undefined, undefined, undefined} end
    ),
    [{config_snapshot, Snapshot} | Config].

end_per_testcase(_, Config) ->
    meck:unload(mocked_modules()),
    arweave_config:internal_restore(proplists:get_value(config_snapshot, Config)).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Entropy owns its tables and runtime lifecycle independently of host
%% workers.
owns_tables(_) ->
    ?assertEqual(undefined, whereis(ar_sup)),
    ?assertEqual(undefined, whereis(ar_packing_server)),
    ?assertEqual(undefined, whereis(arweave_entropy_runtime_sup)),
    Owner = whereis(arweave_entropy_sup),
    [
        ?assertEqual(Owner, ets:info(Table, owner))
     || Table <- [
            ar_entropy_cache,
            ar_entropy_cache_ordered_keys,
            entropy_generation_stats,
            arweave_entropy_generation
        ]
    ],
    %% Activation with no configured modules needs no host workers.
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => [],
        [repack_modules] => []
    }),
    {Module, Function, Args} = maps:get(start, arweave_entropy:child_spec()),
    {ok, Bridge} = apply(Module, Function, Args),
    unlink(Bridge),
    ?assert(is_pid(whereis(arweave_entropy_runtime_sup))),
    gen_server:stop(Bridge),
    ?assertEqual(undefined, whereis(arweave_entropy_runtime_sup)),
    ?assertEqual(Owner, ets:info(ar_entropy_cache, owner)).

%% @doc The public cache API preserves weighted eviction and clears all cached
%% entries.
cache_api_preserves_eviction(_) ->
    %% Two 64-byte entries fill the 128-byte cache, so inserting a third must
    %% evict one of them. The first entry is fetched before the insert, which
    %% spares it from this eviction; the second entry was never fetched, so it
    %% is the one evicted even though it is the newer of the two.
    EntryBytes = 64,
    MaxBytes = 2 * EntryBytes,
    ?assertEqual(not_found, arweave_entropy:internal_get_cached(first)),
    ?assertEqual(ok, arweave_entropy:internal_cache(first, ready, EntryBytes, MaxBytes)),
    ?assertEqual(ok, arweave_entropy:internal_cache(second, ready, EntryBytes, MaxBytes)),
    ?assertEqual({ok, ready}, arweave_entropy:internal_get_cached(first)),
    ?assertEqual(ok, arweave_entropy:internal_cache(third, ready, EntryBytes, MaxBytes)),
    ?assertEqual(not_found, arweave_entropy:internal_get_cached(second)),
    ?assertEqual({ok, ready}, arweave_entropy:internal_get_cached(first)),
    ?assertEqual({ok, ready}, arweave_entropy:internal_get_cached(third)),
    ?assertEqual(MaxBytes, arweave_entropy_cache:total_size()),
    ?assertEqual(ok, arweave_entropy:internal_clear_cache()),
    ?assertEqual(not_found, arweave_entropy:internal_get_cached(first)),
    ?assertEqual(not_found, arweave_entropy:internal_get_cached(third)),
    ?assertEqual(0, arweave_entropy_cache:total_size()).

%% @doc Cached requests reuse generated entropy while uncached requests generate
%% again.
cached_and_uncached_generation(_) ->
    Entropy = binary:copy(<<42>>, ?REPLICA_2_9_ENTROPY_SIZE),
    mock_generation(fun(_, _) -> Entropy end),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, true)),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, true)),
    ?assertEqual(1, generation_calls()),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, false)),
    ?assertEqual(2, generation_calls()),
    ?assertEqual(?REPLICA_2_9_ENTROPY_SIZE, arweave_entropy_cache:total_size()).

%% @doc A slice is served from the cache and shares no memory with the entropy.
slice_is_detached_from_entropy(_) ->
    Entropy = crypto:strong_rand_bytes(?REPLICA_2_9_ENTROPY_SIZE),
    mock_generation(fun(_, _) -> Entropy end),
    %% Offset 0 is the first chunk of its footprint, so slice index 0.
    Slice = arweave_entropy:generate_slice(<<>>, 0, 0),
    ?assertEqual(binary:part(Entropy, 0, ?SUB_CHUNK_SIZE), Slice),
    ?assertEqual(?SUB_CHUNK_SIZE, binary:referenced_byte_size(Slice)),
    ?assertEqual(Slice, arweave_entropy:generate_slice(<<>>, 0, 0)),
    ?assertEqual(1, generation_calls()).

%% @doc A generation exception releases the lock so a later request can succeed.
failed_generation_releases_lock(_) ->
    mock_generation(fun(_, _) -> error(generation_failed) end),
    ?assertException(
        error,
        generation_failed,
        arweave_entropy:generate(<<>>, 0, 0, true)
    ),
    ?assertEqual([], ets:tab2list(arweave_entropy_generation)),
    Entropy = binary:copy(<<42>>, ?REPLICA_2_9_ENTROPY_SIZE),
    mock_generation(fun(_, _) -> Entropy end),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, true)).

%% @doc A waiting request takes over generation after the original generator
%% dies.
dead_generator_does_not_block_reuse(_) ->
    Parent = self(),
    Entropy = binary:copy(<<42>>, ?REPLICA_2_9_ENTROPY_SIZE),
    mock_generation(fun(_, _) ->
        Parent ! {generating, self()},
        receive
            continue -> Entropy
        end
    end),
    Worker = fun() -> Parent ! {result, arweave_entropy:generate(<<>>, 0, 0, true)} end,
    First = spawn_link(Worker),
    receive
        {generating, First} -> ok
    end,
    unlink(First),
    Ref = monitor(process, First),
    exit(First, kill),
    receive
        {'DOWN', Ref, process, First, killed} -> ok
    end,
    Second = spawn_link(Worker),
    try
        receive
            {generating, Second} -> Second ! continue
        end,
        ?assertEqual(
            Entropy,
            receive
                {result, E} -> E
            end
        ),
        ?assertEqual([], ets:tab2list(arweave_entropy_generation))
    after
        unlink(Second),
        exit(Second, kill)
    end.

%% @doc Concurrent requests for the same entropy share one generation result.
concurrent_generation_is_shared(_) ->
    Parent = self(),
    Entropy = binary:copy(<<42>>, ?REPLICA_2_9_ENTROPY_SIZE),
    mock_generation(fun(_, _) ->
        Parent ! {generating, self()},
        receive
            continue -> Entropy
        end
    end),
    Worker = fun() -> Parent ! {result, arweave_entropy:generate(<<>>, 0, 0, true)} end,
    First = spawn_link(Worker),
    receive
        {generating, First} -> ok
    end,
    Second = spawn_link(Worker),
    try
        ok = ar_test_await:until(both_generation_requests_started, fun() ->
            meck:num_calls(
                arweave_lib_replica_2_9, get_entropy_key, '_'
            ) == 2
        end),
        First ! continue,
        ?assertEqual(
            Entropy,
            receive
                {result, E1} -> E1
            end
        ),
        ?assertEqual(
            Entropy,
            receive
                {result, E2} -> E2
            end
        ),
        ?assertEqual(1, generation_calls())
    after
        unlink(First),
        unlink(Second),
        exit(First, kill),
        exit(Second, kill)
    end.

%%====================================================================
%% Helpers
%%====================================================================

%% @doc The host modules the suite mocks.
%% @doc A footprint that keeps failing to store is retried a bounded number of
%% times, after which preparation moves past it and completes.
failing_footprint_is_skipped(_) ->
    StoreID = test_prepare_store,
    Addr = <<0:256>>,
    Packing = {replica_2_9, Addr},
    Failing = arweave_lib_constants:get_chunk_bucket_end(?DATA_CHUNK_SIZE),
    ModuleEnd = Failing + 2 * ?DATA_CHUNK_SIZE,
    Parent = self(),
    Mocks = [ar_device_lock, ar_footprint_limit, ar_serialize, ar,
        arweave_storage, arweave_entropy_generation],
    meck:new(Mocks, [passthrough, no_link]),
    try
        meck:expect(ar_device_lock, acquire_lock, fun(_, _, _) -> active end),
        meck:expect(ar_device_lock, release_lock, fun(_, _) -> ok end),
        meck:expect(ar_device_lock, set_device_lock_metric,
            fun(_, _, _) -> ok end),
        meck:expect(ar_footprint_limit, get, fun(_) -> undefined end),
        meck:expect(ar_footprint_limit, is_beyond, fun(_, _) -> false end),
        meck:expect(ar_serialize, encode_packing, fun(_, _) -> <<"p">> end),
        meck:expect(ar, console, fun(_, _) -> ok end),
        meck:expect(arweave_storage, store_info, fun(_) ->
            #store_info{id = StoreID, label = "test_prepare_store",
                packing = Packing, effective_range = {0, ModuleEnd}}
        end),
        meck:expect(arweave_storage, read_entropy_cursor,
            fun(_, _) -> Failing end),
        meck:expect(arweave_storage, entropy_context,
            fun(_, _) -> {false, Addr} end),
        meck:expect(arweave_storage, is_entropy_recorded,
            fun(_, _, _) -> false end),
        meck:expect(arweave_storage, store_entropy_footprint,
            fun(_, Fold) -> Fold(fun(_, _, _, _, _) -> ok end, ok) end),
        meck:expect(arweave_storage, write_entropy_cursor, fun(_, _) -> ok end),
        meck:expect(arweave_storage, get_next_unsynced_entropy_interval,
            fun(_, _, _) -> not_found end),
        meck:expect(arweave_storage, set_entropy_complete, fun(ID) ->
            Parent ! {entropy_complete, ID},
            ok
        end),
        %% Pass the bucket through as its entropy so that the store step can
        %% fail for one bucket only.
        meck:expect(arweave_entropy_generation, generate_entropies,
            fun(_, BucketEndOffset, false) -> [BucketEndOffset] end),
        meck:expect(arweave_entropy_generation, generate_entropy_keys,
            fun(_, _) -> [] end),
        meck:expect(arweave_entropy_generation, entropy_offsets,
            fun(_, _) -> [] end),
        meck:expect(arweave_entropy_generation, map_entropies,
            fun([Bucket], _, _, _, _, _, _, _) when Bucket == Failing ->
                    {error, eio};
                (_, _, _, _, _, _, _, Acc) ->
                    Acc
            end),
        Name = arweave_entropy_preparation:name(StoreID),
        {ok, PID} = arweave_entropy_preparation:start_link(
            Name, {StoreID, Packing}),
        unlink(PID),
        try
            ?assertEqual(StoreID, receive
                {entropy_complete, ID} -> ID
            after 5_000 -> timeout
            end),
            %% One attempt and five retries.
            ?assertEqual(6, meck:num_calls(arweave_entropy_generation,
                map_entropies, [[Failing], '_', '_', '_', '_', '_', '_', '_']))
        after
            gen_server:stop(PID)
        end
    after
        meck:unload(Mocks)
    end.

mocked_modules() ->
    [arweave_lib_replica_2_9, ar_packing_server, ar_mine_randomx].

%% @doc Make RandomX entropy generation run Fun.
mock_generation(Fun) ->
    meck:expect(ar_mine_randomx, randomx_generate_replica_2_9_entropy, Fun).

%% @doc Count the RandomX entropy generations in this test case.
generation_calls() ->
    meck:num_calls(ar_mine_randomx, randomx_generate_replica_2_9_entropy, '_').
