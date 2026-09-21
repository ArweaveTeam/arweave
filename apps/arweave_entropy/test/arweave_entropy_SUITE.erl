-module(arweave_entropy_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_constants/include/arweave_constants.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        owns_tables,
        cache_api_preserves_eviction,
        cached_and_uncached_generation,
        slice_is_detached_from_entropy,
        failed_generation_releases_lock,
        concurrent_generation_is_shared,
        dead_generator_does_not_block_reuse
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
    meck:new(arweave_entropy_deps, [passthrough, no_link]),
    meck:expect(
        arweave_entropy_deps,
        get_entropy_key,
        fun(_, _, _) -> <<"key">> end
    ),
    [{config_snapshot, Snapshot} | Config].

end_per_testcase(_, Config) ->
    meck:unload(arweave_entropy_deps),
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
    meck:expect(
        arweave_entropy_deps,
        generate_entropy,
        fun(_, _) -> Entropy end
    ),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, true)),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, true)),
    ?assertEqual(1, meck:num_calls(arweave_entropy_deps, generate_entropy, '_')),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, false)),
    ?assertEqual(2, meck:num_calls(arweave_entropy_deps, generate_entropy, '_')),
    ?assertEqual(?REPLICA_2_9_ENTROPY_SIZE, arweave_entropy_cache:total_size()).

%% @doc A slice is served from the cache and shares no memory with the entropy.
slice_is_detached_from_entropy(_) ->
    Entropy = crypto:strong_rand_bytes(?REPLICA_2_9_ENTROPY_SIZE),
    meck:expect(
        arweave_entropy_deps,
        generate_entropy,
        fun(_, _) -> Entropy end
    ),
    %% Offset 0 is the first chunk of its footprint, so slice index 0.
    Slice = arweave_entropy:generate_slice(<<>>, 0, 0),
    ?assertEqual(binary:part(Entropy, 0, ?SUB_CHUNK_SIZE), Slice),
    ?assertEqual(?SUB_CHUNK_SIZE, binary:referenced_byte_size(Slice)),
    ?assertEqual(Slice, arweave_entropy:generate_slice(<<>>, 0, 0)),
    ?assertEqual(1, meck:num_calls(arweave_entropy_deps, generate_entropy, '_')).

%% @doc A generation exception releases the lock so a later request can succeed.
failed_generation_releases_lock(_) ->
    meck:expect(
        arweave_entropy_deps,
        generate_entropy,
        fun(_, _) -> error(generation_failed) end
    ),
    ?assertException(
        error,
        generation_failed,
        arweave_entropy:generate(<<>>, 0, 0, true)
    ),
    ?assertEqual([], ets:tab2list(arweave_entropy_generation)),
    Entropy = binary:copy(<<42>>, ?REPLICA_2_9_ENTROPY_SIZE),
    meck:expect(
        arweave_entropy_deps,
        generate_entropy,
        fun(_, _) -> Entropy end
    ),
    ?assertEqual(Entropy, arweave_entropy:generate(<<>>, 0, 0, true)).

%% @doc A waiting request takes over generation after the original generator
%% dies.
dead_generator_does_not_block_reuse(_) ->
    Parent = self(),
    Entropy = binary:copy(<<42>>, ?REPLICA_2_9_ENTROPY_SIZE),
    meck:expect(arweave_entropy_deps, generate_entropy, fun(_, _) ->
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
    meck:expect(arweave_entropy_deps, generate_entropy, fun(_, _) ->
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
            meck:num_calls(arweave_entropy_deps, get_entropy_key, '_') == 2
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
        ?assertEqual(1, meck:num_calls(arweave_entropy_deps, generate_entropy, '_'))
    after
        unlink(First),
        unlink(Second),
        exit(First, kill),
        exit(Second, kill)
    end.
