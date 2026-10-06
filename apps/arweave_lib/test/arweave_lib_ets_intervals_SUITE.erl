-module(arweave_lib_ets_intervals_SUITE).
-test_category([fast]).
-export([all/0, ets_intervals/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [ets_intervals].



%%%===================================================================
%%% Tests.
%%%===================================================================

ets_intervals_test() ->
    ets:new(ets_intervals_test, [named_table, ordered_set]),
    arweave_lib_ets_intervals:assert_is_not_inside(100, 0),
    ?assertEqual(ok, arweave_lib_ets_intervals:cut(ets_intervals_test, 10)),
    ?assertEqual(ok, arweave_lib_ets_intervals:delete(ets_intervals_test, 10, 5)),
    Set = gb_sets:from_list([{1, 0}, {5, 3}, {16, 10}]),
    arweave_lib_ets_intervals:init_from_gb_set(ets_intervals_test, Set),
    arweave_lib_ets_intervals:assert_is_inside(1, 0),
    arweave_lib_ets_intervals:assert_is_not_inside(3, 1),
    arweave_lib_ets_intervals:assert_is_inside(5, 3),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 5),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 16),
    %% 1,0 16,3
    arweave_lib_ets_intervals:add(ets_intervals_test, 11, 4),
    arweave_lib_ets_intervals:assert_is_inside(16, 3),
    arweave_lib_ets_intervals:assert_is_inside(1, 0),
    arweave_lib_ets_intervals:assert_is_not_inside(3, 1),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 16),
    %% back to 1,0 5,3 16,10
    arweave_lib_ets_intervals:delete(ets_intervals_test, 10, 5),
    arweave_lib_ets_intervals:assert_is_inside(5, 3),
    arweave_lib_ets_intervals:assert_is_inside(1, 0),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_not_inside(3, 1),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 16),
    %% 1,0 5,3 16,10 20,18
    arweave_lib_ets_intervals:add(ets_intervals_test, 20, 18),
    arweave_lib_ets_intervals:assert_is_inside(5, 3),
    arweave_lib_ets_intervals:assert_is_inside(1, 0),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_inside(20, 18),
    arweave_lib_ets_intervals:assert_is_not_inside(3, 1),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_not_inside(22, 20),
    %% 1,0 5,3 8,7 16,10 20,18
    arweave_lib_ets_intervals:add(ets_intervals_test, 8, 7),
    arweave_lib_ets_intervals:assert_is_inside(5, 3),
    arweave_lib_ets_intervals:assert_is_inside(1, 0),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_inside(20, 18),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_not_inside(3, 1),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_not_inside(22, 20),
    %% 5,0 8,7 16,10 20,18
    arweave_lib_ets_intervals:add(ets_intervals_test, 3, 1),
    arweave_lib_ets_intervals:assert_is_inside(5, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_inside(20, 18),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_not_inside(22, 20),
    %% 5,0 8,7 16,10 20,18
    arweave_lib_ets_intervals:cut(ets_intervals_test, 22),
    arweave_lib_ets_intervals:assert_is_inside(5, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_inside(20, 18),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_not_inside(22, 20),
    %% 5,0 8,7 16,10 20,18
    arweave_lib_ets_intervals:cut(ets_intervals_test, 20),
    arweave_lib_ets_intervals:assert_is_inside(5, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_inside(20, 18),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_not_inside(22, 20),
    %% 5,0 8,7 16,10 19,18
    arweave_lib_ets_intervals:cut(ets_intervals_test, 19),
    arweave_lib_ets_intervals:assert_is_inside(5, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(16, 10),
    arweave_lib_ets_intervals:assert_is_inside(19, 18),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_not_inside(22, 19),
    %% 5,0 8,7 14,10
    arweave_lib_ets_intervals:cut(ets_intervals_test, 14),
    arweave_lib_ets_intervals:assert_is_inside(5, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(14, 10),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 5),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 14),
    %% 1,0 8,7 14,10
    arweave_lib_ets_intervals:delete(ets_intervals_test, 5, 1),
    arweave_lib_ets_intervals:assert_is_inside(1, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(14, 10),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 1),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 14),
    %% 8,7 14,10
    arweave_lib_ets_intervals:delete(ets_intervals_test, 5, 0),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(14, 10),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 0),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 14),
    %% 8,7 15,10 30,20
    arweave_lib_ets_intervals:add(ets_intervals_test, 15, 14),
    arweave_lib_ets_intervals:add(ets_intervals_test, 30, 20),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(15, 10),
    arweave_lib_ets_intervals:assert_is_inside(30, 20),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 0),
    arweave_lib_ets_intervals:assert_is_not_inside(10, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(20, 15),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 30),
    %% 8,7 30,25
    arweave_lib_ets_intervals:delete(ets_intervals_test, 25, 8),
    arweave_lib_ets_intervals:assert_is_inside(8, 7),
    arweave_lib_ets_intervals:assert_is_inside(30, 25),
    arweave_lib_ets_intervals:assert_is_not_inside(25, 8),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 0),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 30),
    %% 30,7
    arweave_lib_ets_intervals:add(ets_intervals_test, 25, 8),
    arweave_lib_ets_intervals:assert_is_inside(30, 7),
    arweave_lib_ets_intervals:assert_is_not_inside(7, 0),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 30),
    %% 12,7 18,16 30,25
    arweave_lib_ets_intervals:delete(ets_intervals_test, 16, 12),
    arweave_lib_ets_intervals:delete(ets_intervals_test, 25, 18),
    arweave_lib_ets_intervals:assert_is_inside(12, 7),
    arweave_lib_ets_intervals:assert_is_inside(18, 16),
    arweave_lib_ets_intervals:assert_is_inside(30, 25),
    arweave_lib_ets_intervals:assert_is_not_inside(16, 12),
    arweave_lib_ets_intervals:assert_is_not_inside(25, 18),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 30),
    %% 12,7 21,16 30,25
    arweave_lib_ets_intervals:add(ets_intervals_test, 21, 18),
    arweave_lib_ets_intervals:assert_is_inside(12, 7),
    arweave_lib_ets_intervals:assert_is_inside(21, 16),
    arweave_lib_ets_intervals:assert_is_inside(30, 25),
    arweave_lib_ets_intervals:assert_is_not_inside(16, 12),
    arweave_lib_ets_intervals:assert_is_not_inside(25, 21),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 30),
    %% 12,7 33,13
    arweave_lib_ets_intervals:add(ets_intervals_test, 33, 13),
    arweave_lib_ets_intervals:assert_is_inside(33, 13),
    arweave_lib_ets_intervals:assert_is_inside(12, 7),
    arweave_lib_ets_intervals:assert_is_not_inside(13, 12),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 33),
    %% 12,7 34,13
    arweave_lib_ets_intervals:add(ets_intervals_test, 34, 13),
    arweave_lib_ets_intervals:assert_is_inside(34, 13),
    arweave_lib_ets_intervals:assert_is_inside(12, 7),
    arweave_lib_ets_intervals:assert_is_not_inside(13, 12),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 34),
    %% 12,7 35,13
    arweave_lib_ets_intervals:add(ets_intervals_test, 35, 22),
    arweave_lib_ets_intervals:assert_is_inside(35, 13),
    arweave_lib_ets_intervals:assert_is_inside(12, 7),
    arweave_lib_ets_intervals:assert_is_not_inside(13, 12),
    arweave_lib_ets_intervals:assert_is_not_inside(40, 35).

ets_intervals(_Config) ->
    ets_intervals_test().
