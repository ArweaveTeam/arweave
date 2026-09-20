-module(arweave_util_ets_intervals_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [ets_intervals].

%%====================================================================
%% Test cases
%%====================================================================

ets_intervals(_Config) ->
    ets:new(ets_intervals_test, [named_table, ordered_set]),
    ?assertEqual(lists:duplicate(100 - 0, false), membership(100, 0)),
    ?assertEqual(ok, ar_ets_intervals:cut(ets_intervals_test, 10)),
    ?assertEqual(ok, ar_ets_intervals:delete(ets_intervals_test, 10, 5)),
    Set = gb_sets:from_list([{1, 0}, {5, 3}, {16, 10}]),
    ar_ets_intervals:init_from_gb_set(ets_intervals_test, Set),
    ?assertEqual(lists:duplicate(1 - 0, true), membership(1, 0)),
    ?assertEqual(lists:duplicate(3 - 1, false), membership(3, 1)),
    ?assertEqual(lists:duplicate(5 - 3, true), membership(5, 3)),
    ?assertEqual(lists:duplicate(10 - 5, false), membership(10, 5)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(20 - 16, false), membership(20, 16)),
    %% 1,0 16,3
    ar_ets_intervals:add(ets_intervals_test, 11, 4),
    ?assertEqual(lists:duplicate(16 - 3, true), membership(16, 3)),
    ?assertEqual(lists:duplicate(1 - 0, true), membership(1, 0)),
    ?assertEqual(lists:duplicate(3 - 1, false), membership(3, 1)),
    ?assertEqual(lists:duplicate(20 - 16, false), membership(20, 16)),
    %% back to 1,0 5,3 16,10
    ar_ets_intervals:delete(ets_intervals_test, 10, 5),
    ?assertEqual(lists:duplicate(5 - 3, true), membership(5, 3)),
    ?assertEqual(lists:duplicate(1 - 0, true), membership(1, 0)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(3 - 1, false), membership(3, 1)),
    ?assertEqual(lists:duplicate(10 - 5, false), membership(10, 5)),
    ?assertEqual(lists:duplicate(20 - 16, false), membership(20, 16)),
    %% 1,0 5,3 16,10 20,18
    ar_ets_intervals:add(ets_intervals_test, 20, 18),
    ?assertEqual(lists:duplicate(5 - 3, true), membership(5, 3)),
    ?assertEqual(lists:duplicate(1 - 0, true), membership(1, 0)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(20 - 18, true), membership(20, 18)),
    ?assertEqual(lists:duplicate(3 - 1, false), membership(3, 1)),
    ?assertEqual(lists:duplicate(10 - 5, false), membership(10, 5)),
    ?assertEqual(lists:duplicate(18 - 16, false), membership(18, 16)),
    ?assertEqual(lists:duplicate(22 - 20, false), membership(22, 20)),
    %% 1,0 5,3 8,7 16,10 20,18
    ar_ets_intervals:add(ets_intervals_test, 8, 7),
    ?assertEqual(lists:duplicate(5 - 3, true), membership(5, 3)),
    ?assertEqual(lists:duplicate(1 - 0, true), membership(1, 0)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(20 - 18, true), membership(20, 18)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(3 - 1, false), membership(3, 1)),
    ?assertEqual(lists:duplicate(7 - 5, false), membership(7, 5)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(18 - 16, false), membership(18, 16)),
    ?assertEqual(lists:duplicate(22 - 20, false), membership(22, 20)),
    %% 5,0 8,7 16,10 20,18
    ar_ets_intervals:add(ets_intervals_test, 3, 1),
    ?assertEqual(lists:duplicate(5 - 0, true), membership(5, 0)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(20 - 18, true), membership(20, 18)),
    ?assertEqual(lists:duplicate(7 - 5, false), membership(7, 5)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(18 - 16, false), membership(18, 16)),
    ?assertEqual(lists:duplicate(22 - 20, false), membership(22, 20)),
    %% 5,0 8,7 16,10 20,18
    ar_ets_intervals:cut(ets_intervals_test, 22),
    ?assertEqual(lists:duplicate(5 - 0, true), membership(5, 0)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(20 - 18, true), membership(20, 18)),
    ?assertEqual(lists:duplicate(7 - 5, false), membership(7, 5)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(18 - 16, false), membership(18, 16)),
    ?assertEqual(lists:duplicate(22 - 20, false), membership(22, 20)),
    %% 5,0 8,7 16,10 20,18
    ar_ets_intervals:cut(ets_intervals_test, 20),
    ?assertEqual(lists:duplicate(5 - 0, true), membership(5, 0)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(20 - 18, true), membership(20, 18)),
    ?assertEqual(lists:duplicate(7 - 5, false), membership(7, 5)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(18 - 16, false), membership(18, 16)),
    ?assertEqual(lists:duplicate(22 - 20, false), membership(22, 20)),
    %% 5,0 8,7 16,10 19,18
    ar_ets_intervals:cut(ets_intervals_test, 19),
    ?assertEqual(lists:duplicate(5 - 0, true), membership(5, 0)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(16 - 10, true), membership(16, 10)),
    ?assertEqual(lists:duplicate(19 - 18, true), membership(19, 18)),
    ?assertEqual(lists:duplicate(7 - 5, false), membership(7, 5)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(18 - 16, false), membership(18, 16)),
    ?assertEqual(lists:duplicate(22 - 19, false), membership(22, 19)),
    %% 5,0 8,7 14,10
    ar_ets_intervals:cut(ets_intervals_test, 14),
    ?assertEqual(lists:duplicate(5 - 0, true), membership(5, 0)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(14 - 10, true), membership(14, 10)),
    ?assertEqual(lists:duplicate(7 - 5, false), membership(7, 5)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(20 - 14, false), membership(20, 14)),
    %% 1,0 8,7 14,10
    ar_ets_intervals:delete(ets_intervals_test, 5, 1),
    ?assertEqual(lists:duplicate(1 - 0, true), membership(1, 0)),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(14 - 10, true), membership(14, 10)),
    ?assertEqual(lists:duplicate(7 - 1, false), membership(7, 1)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(20 - 14, false), membership(20, 14)),
    %% 8,7 14,10
    ar_ets_intervals:delete(ets_intervals_test, 5, 0),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(14 - 10, true), membership(14, 10)),
    ?assertEqual(lists:duplicate(7 - 0, false), membership(7, 0)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(20 - 14, false), membership(20, 14)),
    %% 8,7 15,10 30,20
    ar_ets_intervals:add(ets_intervals_test, 15, 14),
    ar_ets_intervals:add(ets_intervals_test, 30, 20),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(15 - 10, true), membership(15, 10)),
    ?assertEqual(lists:duplicate(30 - 20, true), membership(30, 20)),
    ?assertEqual(lists:duplicate(7 - 0, false), membership(7, 0)),
    ?assertEqual(lists:duplicate(10 - 8, false), membership(10, 8)),
    ?assertEqual(lists:duplicate(20 - 15, false), membership(20, 15)),
    ?assertEqual(lists:duplicate(40 - 30, false), membership(40, 30)),
    %% 8,7 30,25
    ar_ets_intervals:delete(ets_intervals_test, 25, 8),
    ?assertEqual(lists:duplicate(8 - 7, true), membership(8, 7)),
    ?assertEqual(lists:duplicate(30 - 25, true), membership(30, 25)),
    ?assertEqual(lists:duplicate(25 - 8, false), membership(25, 8)),
    ?assertEqual(lists:duplicate(7 - 0, false), membership(7, 0)),
    ?assertEqual(lists:duplicate(40 - 30, false), membership(40, 30)),
    %% 30,7
    ar_ets_intervals:add(ets_intervals_test, 25, 8),
    ?assertEqual(lists:duplicate(30 - 7, true), membership(30, 7)),
    ?assertEqual(lists:duplicate(7 - 0, false), membership(7, 0)),
    ?assertEqual(lists:duplicate(40 - 30, false), membership(40, 30)),
    %% 12,7 18,16 30,25
    ar_ets_intervals:delete(ets_intervals_test, 16, 12),
    ar_ets_intervals:delete(ets_intervals_test, 25, 18),
    ?assertEqual(lists:duplicate(12 - 7, true), membership(12, 7)),
    ?assertEqual(lists:duplicate(18 - 16, true), membership(18, 16)),
    ?assertEqual(lists:duplicate(30 - 25, true), membership(30, 25)),
    ?assertEqual(lists:duplicate(16 - 12, false), membership(16, 12)),
    ?assertEqual(lists:duplicate(25 - 18, false), membership(25, 18)),
    ?assertEqual(lists:duplicate(40 - 30, false), membership(40, 30)),
    %% 12,7 21,16 30,25
    ar_ets_intervals:add(ets_intervals_test, 21, 18),
    ?assertEqual(lists:duplicate(12 - 7, true), membership(12, 7)),
    ?assertEqual(lists:duplicate(21 - 16, true), membership(21, 16)),
    ?assertEqual(lists:duplicate(30 - 25, true), membership(30, 25)),
    ?assertEqual(lists:duplicate(16 - 12, false), membership(16, 12)),
    ?assertEqual(lists:duplicate(25 - 21, false), membership(25, 21)),
    ?assertEqual(lists:duplicate(40 - 30, false), membership(40, 30)),
    %% 12,7 33,13
    ar_ets_intervals:add(ets_intervals_test, 33, 13),
    ?assertEqual(lists:duplicate(33 - 13, true), membership(33, 13)),
    ?assertEqual(lists:duplicate(12 - 7, true), membership(12, 7)),
    ?assertEqual(lists:duplicate(13 - 12, false), membership(13, 12)),
    ?assertEqual(lists:duplicate(40 - 33, false), membership(40, 33)),
    %% 12,7 34,13
    ar_ets_intervals:add(ets_intervals_test, 34, 13),
    ?assertEqual(lists:duplicate(34 - 13, true), membership(34, 13)),
    ?assertEqual(lists:duplicate(12 - 7, true), membership(12, 7)),
    ?assertEqual(lists:duplicate(13 - 12, false), membership(13, 12)),
    ?assertEqual(lists:duplicate(40 - 34, false), membership(40, 34)),
    %% 12,7 35,13
    ar_ets_intervals:add(ets_intervals_test, 35, 22),
    ?assertEqual(lists:duplicate(35 - 13, true), membership(35, 13)),
    ?assertEqual(lists:duplicate(12 - 7, true), membership(12, 7)),
    ?assertEqual(lists:duplicate(13 - 12, false), membership(13, 12)),
    ?assertEqual(lists:duplicate(40 - 35, false), membership(40, 35)),
    ?assertEqual(
        [{12, 7}, {35, 13}],
        gb_sets:to_list(ar_ets_intervals:to_gb_set(ets_intervals_test))
    ).

%%====================================================================
%% Helpers
%%====================================================================

membership(End, Start) ->
    [
        ar_ets_intervals:is_inside(ets_intervals_test, Offset)
     || Offset <- lists:seq(Start + 1, End)
    ].
