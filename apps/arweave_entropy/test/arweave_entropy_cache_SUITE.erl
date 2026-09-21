-module(arweave_entropy_cache_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [weighted_eviction_and_reuse, fetched_entries_get_a_second_chance].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_metrics),
    ok = ar_metrics:register(),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = ar_metrics:cleanup(),
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

%%====================================================================
%% Test cases
%%====================================================================

weighted_eviction_and_reuse(_Config) ->
    %% Two 64-byte entries fit in the 128-byte test cache.
    Table = 'test_entropy_cache_table',
    OrderedKeyTable = 'test_entropy_cache_ordered_key_table',
    ets:new(Table, [set, public, named_table]),
    ets:new(OrderedKeyTable, [ordered_set, public, named_table]),
    ?assertEqual(
        0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    ?assertEqual(not_found, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    arweave_entropy_cache:put(some_key, some_value, 64, Table, OrderedKeyTable),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        1, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        2, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        3, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        4, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    arweave_entropy_cache:clean_up_space(128, 128, Table, OrderedKeyTable),
    %% The existing 64 bytes plus 128 incoming bytes exceed the 128-byte limit.
    ?assertEqual(not_found, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    %% The put itself does not clean up the cache.
    arweave_entropy_cache:put(some_key, some_value, 64, Table, OrderedKeyTable),
    arweave_entropy_cache:put(
        some_other_key, some_other_value, 64, Table, OrderedKeyTable
    ),
    arweave_entropy_cache:put(
        yet_another_key, yet_another_value, 64, Table, OrderedKeyTable
    ),
    ?assertEqual(
        0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        {ok, some_other_value},
        arweave_entropy_cache:get(some_other_key, Table)
    ),
    ?assertEqual(
        {ok, yet_another_value},
        arweave_entropy_cache:get(yet_another_key, Table)
    ),
    ?assertEqual(
        1, arweave_entropy_cache:get_fetched_key_count(Table, some_key)
    ),
    ?assertEqual(
        1, arweave_entropy_cache:get_fetched_key_count(Table, some_other_key)
    ),
    ?assertEqual(
        1, arweave_entropy_cache:get_fetched_key_count(Table, yet_another_key)
    ),
    %% Basically, we are simply reducing the cache 192 -> 128.
    arweave_entropy_cache:clean_up_space(0, 128, Table, OrderedKeyTable),
    ?assertEqual(not_found, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(
        {ok, some_other_value},
        arweave_entropy_cache:get(some_other_key, Table)
    ),
    ?assertEqual(
        {ok, yet_another_value},
        arweave_entropy_cache:get(yet_another_key, Table)
    ),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual(not_found, arweave_entropy_cache:get(some_other_key, Table)),
    ?assertEqual(
        {ok, yet_another_value},
        arweave_entropy_cache:get(yet_another_key, Table)
    ).

%% @doc Eviction considers entries oldest-first, but an entry that was fetched
%% since it was inserted (or since its last reprieve) is moved to the back
%% and spared once instead of being evicted. An entry nobody fetched in that
%% time is evicted right away, even if it was inserted more recently. When
%% every entry has been fetched, each is spared at most once per clean-up, so
%% the clean-up still ends with the requested space free.
fetched_entries_get_a_second_chance(_Config) ->
    Table = test_entropy_cache_second_chance,
    OrderedKeyTable = test_entropy_cache_second_chance_keys,
    ets:new(Table, [set, public, named_table]),
    ets:new(OrderedKeyTable, [ordered_set, public, named_table]),
    %% Two 64-byte entries fill the 128-byte test cache. Only the older one
    %% is fetched before space is needed.
    arweave_entropy_cache:put(in_use, a, 64, Table, OrderedKeyTable),
    arweave_entropy_cache:put(idle, b, 64, Table, OrderedKeyTable),
    ?assertEqual({ok, a}, arweave_entropy_cache:get(in_use, Table)),
    %% Making room for a third entry spares the fetched entry and evicts the
    %% unfetched one, although the unfetched one was inserted later.
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assert(ets:member(Table, {key, in_use})),
    ?assertNot(ets:member(Table, {key, idle})),
    ?assertEqual(64, ets:lookup_element(Table, total_size, 2)),
    %% The spared entry is not fetched again, so the next clean-up evicts it.
    arweave_entropy_cache:clean_up_space(128, 128, Table, OrderedKeyTable),
    ?assertNot(ets:member(Table, {key, in_use})),
    ?assertEqual(0, ets:lookup_element(Table, total_size, 2)),
    %% Three fetched entries and room needed for one more: each is spared
    %% once, after which the oldest two are evicted so that 64 bytes fit.
    arweave_entropy_cache:put(hot1, a, 64, Table, OrderedKeyTable),
    arweave_entropy_cache:put(hot2, b, 64, Table, OrderedKeyTable),
    arweave_entropy_cache:put(hot3, c, 64, Table, OrderedKeyTable),
    ?assertEqual({ok, a}, arweave_entropy_cache:get(hot1, Table)),
    ?assertEqual({ok, b}, arweave_entropy_cache:get(hot2, Table)),
    ?assertEqual({ok, c}, arweave_entropy_cache:get(hot3, Table)),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual(64, ets:lookup_element(Table, total_size, 2)),
    ?assertNot(ets:member(Table, {key, hot1})),
    ?assertNot(ets:member(Table, {key, hot2})),
    ?assert(ets:member(Table, {key, hot3})).
