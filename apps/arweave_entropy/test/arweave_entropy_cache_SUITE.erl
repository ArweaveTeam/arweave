-module(arweave_entropy_cache_SUITE).
-test_category([fast]).
-export([all/0, cache/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [cache].



%%%===================================================================
%%% Tests.
%%%===================================================================

cache_test() ->
    Table = 'test_entropy_cache_table',
    OrderedKeyTable = 'test_entropy_cache_ordered_key_table',
    ets:new(Table, [set, public, named_table]),
    ets:new(OrderedKeyTable, [ordered_set, public, named_table]),
    ?assertEqual(0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    ?assertEqual(not_found, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    arweave_entropy_cache:put(some_key, some_value, 64, Table, OrderedKeyTable),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(1, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(2, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(3, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(4, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    arweave_entropy_cache:clean_up_space(128, 128, Table, OrderedKeyTable),
    %% We requested an allocation of > MaxSize so the old key needs to be removed.
    ?assertEqual(not_found, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual(0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    %% The put itself does not clean up the cache.
    arweave_entropy_cache:put(some_key, some_value, 64, Table, OrderedKeyTable),
    arweave_entropy_cache:put(some_other_key, some_other_value, 64, Table, OrderedKeyTable),
    arweave_entropy_cache:put(yet_another_key, yet_another_value, 64, Table, OrderedKeyTable),
    ?assertEqual(0, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    ?assertEqual({ok, some_value}, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual({ok, some_other_value}, arweave_entropy_cache:get(some_other_key, Table)),
    ?assertEqual({ok, yet_another_value}, arweave_entropy_cache:get(yet_another_key, Table)),
    ?assertEqual(1, arweave_entropy_cache:get_fetched_key_count(Table, some_key)),
    ?assertEqual(1, arweave_entropy_cache:get_fetched_key_count(Table, some_other_key)),
    ?assertEqual(1, arweave_entropy_cache:get_fetched_key_count(Table, yet_another_key)),
    %% Basically, we are simply reducing the cache 192 -> 128.
    arweave_entropy_cache:clean_up_space(0, 128, Table, OrderedKeyTable),
    ?assertEqual(not_found, arweave_entropy_cache:get(some_key, Table)),
    ?assertEqual({ok, some_other_value}, arweave_entropy_cache:get(some_other_key, Table)),
    ?assertEqual({ok, yet_another_value}, arweave_entropy_cache:get(yet_another_key, Table)),
    arweave_entropy_cache:clean_up_space(64, 128, Table, OrderedKeyTable),
    ?assertEqual(not_found, arweave_entropy_cache:get(some_other_key, Table)),
    ?assertEqual({ok, yet_another_value}, arweave_entropy_cache:get(yet_another_key, Table)).

cache(_Config) ->
    cache_test().
