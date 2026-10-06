-module(arweave_lib_intervals_SUITE).
-test_category([fast]).
-export([all/0, compressed_etf_rejected/1, intervals/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [compressed_etf_rejected, intervals].



%%%===================================================================
%%% Tests.
%%%===================================================================

compressed_etf_rejected_test() ->
    Pairs = [{2 * N, 2 * N - 1} || N <- lists:seq(100, 1, -1)],
    L = [{<< End:256 >>, << Start:256 >>} || {End, Start} <- Pairs],
    Compressed = term_to_binary(L, [compressed]),
    ?assertMatch(<< 131, 80, _/binary >>, Compressed),
    ?assertEqual({ok, arweave_lib_intervals:from_list(Pairs)}, arweave_lib_intervals:safe_from_etf(term_to_binary(L))),
    ?assertEqual({error, invalid}, arweave_lib_intervals:safe_from_etf(Compressed)).

compressed_etf_rejected(_Config) ->
    compressed_etf_rejected_test().



intervals_test() ->
    I = arweave_lib_intervals:new(),
    ?assertEqual(0, arweave_lib_intervals:count(I)),
    ?assertEqual(0, arweave_lib_intervals:sum(I)),
    ?assert(not arweave_lib_intervals:is_inside(I, 0)),
    ?assert(not arweave_lib_intervals:is_inside(I, 1)),
    ?assertEqual(<<"[]">>, arweave_lib_intervals:serialize(#{ random_subset => true, format => json, limit => 1 }, I)),
    ?assertEqual(<<"[]">>, arweave_lib_intervals:serialize(#{ start => 0, format => json, limit => 1 }, I)),
    ?assertEqual(<<"[]">>, arweave_lib_intervals:serialize(#{ start => 1, format => json, limit => 1 }, I)),
    ?assertEqual(
       {ok, arweave_lib_intervals:new()},
       arweave_lib_intervals:safe_from_etf(arweave_lib_intervals:serialize(#{ random_subset => true, format => etf, limit => 1 }, I))
      ),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:outerjoin(I, I)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:delete(I, 2, 1)),
    I2 = arweave_lib_intervals:add(I, 2, 1),
    ?assertEqual(1, arweave_lib_intervals:count(I2)),
    ?assertEqual(1, arweave_lib_intervals:sum(I2)),
    ?assert(not arweave_lib_intervals:is_inside(I2, 0)),
    ?assert(not arweave_lib_intervals:is_inside(I2, 1)),
    ?assert(arweave_lib_intervals:is_inside(I2, 2)),
    ?assert(not arweave_lib_intervals:is_inside(I2, 3)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:delete(I2, 2, 1)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:delete(I2, 2, 0)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:delete(I2, 3, 1)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:delete(I2, 3, 0)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:cut(I2, 1)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:cut(I2, 0)),
    arweave_lib_intervals:compare(I2, arweave_lib_intervals:cut(I2, 2)),
    arweave_lib_intervals:compare(I2, arweave_lib_intervals:cut(I2, 3)),
    ?assertEqual(
       <<"[{\"2\":\"1\"}]">>,
       arweave_lib_intervals:serialize(#{ random_subset => true, limit => 1, format => json }, I2)
      ),
    ?assertEqual(
       <<"[]">>,
       arweave_lib_intervals:serialize(#{ random_subset => true, limit => 0, format => json }, I2)
      ),
    ?assertEqual(
       <<"[{\"2\":\"1\"}]">>,
       arweave_lib_intervals:serialize(#{ start => 2, limit => 1, format => json }, I2)
      ),
    ?assertEqual(
       <<"[]">>,
       arweave_lib_intervals:serialize(#{ start => 3, limit => 1, format => json }, I2)
      ),
    ?assertEqual(
       <<"[]">>,
       arweave_lib_intervals:serialize(#{ start => 2, limit => 0, format => json }, I2)
      ),
    {ok, I2_FromETF} =
        arweave_lib_intervals:safe_from_etf(arweave_lib_intervals:serialize(#{ format => etf, limit => 1, random_subset => true }, I2)),
    arweave_lib_intervals:compare(I2, I2_FromETF),
    ?assertEqual(
       {ok, arweave_lib_intervals:new()},
       arweave_lib_intervals:safe_from_etf(arweave_lib_intervals:serialize(#{ format => etf, limit => 0, random_subset => true }, I2))
      ),
    arweave_lib_intervals:compare(I2, arweave_lib_intervals:add(I2, 2, 1)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 3, 1), arweave_lib_intervals:add(I2, 3, 1)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 2, 0), arweave_lib_intervals:add(I2, 2, 0)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:outerjoin(I2, I)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:new(), 1, 0), 3, 2), arweave_lib_intervals:outerjoin(I2, arweave_lib_intervals:add(arweave_lib_intervals:new(), 3, 0))),
    I3 = arweave_lib_intervals:add(I2, 6, 3),
    ?assertEqual(2, arweave_lib_intervals:count(I3)),
    ?assertEqual(4, arweave_lib_intervals:sum(I3)),
    ?assert(not arweave_lib_intervals:is_inside(I3, 0)),
    ?assert(not arweave_lib_intervals:is_inside(I3, 1)),
    ?assert(arweave_lib_intervals:is_inside(I3, 2)),
    ?assert(not arweave_lib_intervals:is_inside(I3, 3)),
    ?assert(arweave_lib_intervals:is_inside(I3, 4)),
    ?assert(arweave_lib_intervals:is_inside(I3, 5)),
    ?assert(arweave_lib_intervals:is_inside(I3, 6)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:new(), 2, 1), 6, 5), 4, 3), arweave_lib_intervals:delete(I3, 5, 4)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 6, 5), arweave_lib_intervals:delete(I3, 5, 1)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 10, 0), arweave_lib_intervals:add(I3, 10, 0)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:cut(I3, 1)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:cut(I3, 0)),
    ?assertEqual(I2, arweave_lib_intervals:cut(I3, 2)),
    ?assertEqual(I2, arweave_lib_intervals:cut(I3, 3)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(I2, 4, 3), arweave_lib_intervals:cut(I3, 4)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(I2, 5, 3), arweave_lib_intervals:cut(I3, 5)),
    arweave_lib_intervals:compare(I3, arweave_lib_intervals:cut(I3, 6)),
    ?assertEqual(
       <<"[{\"6\":\"3\"},{\"2\":\"1\"}]">>,
       arweave_lib_intervals:serialize(#{ random_subset => true, limit => 1000, format => json }, I3)
      ),
    ?assertEqual(
       <<"[{\"6\":\"3\"},{\"2\":\"1\"}]">>,
       arweave_lib_intervals:serialize(#{ start => 1, limit => 10, format => json }, I3)
      ),
    ?assertEqual(
       <<"[{\"2\":\"1\"}]">>,
       arweave_lib_intervals:serialize(#{ start => 1, limit => 1, format => json }, I3)
      ),
    ?assertEqual(
       <<"[{\"6\":\"3\"}]">>,
       arweave_lib_intervals:serialize(#{ start => 3, limit => 10, format => json }, I3)
      ),
    {ok, I3_FromETF} =
        arweave_lib_intervals:safe_from_etf(arweave_lib_intervals:serialize(#{ format => etf, limit => 1000, random_subset => true }, I3)),
    arweave_lib_intervals:compare(I3, I3_FromETF),
    arweave_lib_intervals:compare(I3, arweave_lib_intervals:add(I3, 4, 3)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 6, 1), arweave_lib_intervals:add(I3, 3, 1)),
    I3_2 = arweave_lib_intervals:add(arweave_lib_intervals:new(), 7, 5),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 7, 5), arweave_lib_intervals:outerjoin(I2, I3_2)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:new(), 7, 6), arweave_lib_intervals:outerjoin(I3, I3_2)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:new(), 1, 0), 3, 2), 8, 6), arweave_lib_intervals:outerjoin(I3, arweave_lib_intervals:add(arweave_lib_intervals:new(), 8, 0))),
    I4 = arweave_lib_intervals:add(I3, 7, 6),
    ?assertEqual(2, arweave_lib_intervals:count(I4)),
    ?assertEqual(5, arweave_lib_intervals:sum(I4)),
    ?assert(not arweave_lib_intervals:is_inside(I4, 0)),
    ?assert(not arweave_lib_intervals:is_inside(I4, 1)),
    ?assert(arweave_lib_intervals:is_inside(I4, 2)),
    ?assert(not arweave_lib_intervals:is_inside(I4, 3)),
    ?assert(arweave_lib_intervals:is_inside(I4, 4)),
    ?assert(arweave_lib_intervals:is_inside(I4, 5)),
    ?assert(arweave_lib_intervals:is_inside(I4, 6)),
    ?assert(arweave_lib_intervals:is_inside(I4, 7)),
    ?assert(not arweave_lib_intervals:is_inside(I4, 8)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:cut(I4, 1)),
    ?assertEqual(arweave_lib_intervals:new(), arweave_lib_intervals:cut(I4, 0)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(I2, 5, 3), arweave_lib_intervals:cut(I4, 5)),
    arweave_lib_intervals:compare(I4, arweave_lib_intervals:cut(I4, 7)),
    ?assertEqual(
       <<"[{\"7\":\"3\"},{\"2\":\"1\"}]">>,
       arweave_lib_intervals:serialize(#{ format => json, limit => 1000, random_subset => true }, I4)
      ),
    {ok, I4_FromETF} = arweave_lib_intervals:safe_from_etf(arweave_lib_intervals:serialize(#{ limit => 1000, random_subset => true,
                                                  format => etf }, I4)),
    arweave_lib_intervals:compare(I4, I4_FromETF),
    I5 = arweave_lib_intervals:add(I4, 3, 2),
    ?assertEqual(1, arweave_lib_intervals:count(I5)),
    ?assertEqual(6, arweave_lib_intervals:sum(I5)),
    arweave_lib_intervals:compare(I5, arweave_lib_intervals:add(I5, 3, 2)),
    arweave_lib_intervals:compare(I5, arweave_lib_intervals:add(I5, 2, 1)),
    arweave_lib_intervals:compare(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:new(), 3, 2), 8, 7), arweave_lib_intervals:delete(arweave_lib_intervals:add(arweave_lib_intervals:add(arweave_lib_intervals:new(), 4, 2), 8, 6), 7, 3)).

intervals(_Config) ->
    intervals_test().
