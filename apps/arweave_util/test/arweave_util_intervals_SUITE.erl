-module(arweave_util_intervals_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [intervals].

%%====================================================================
%% Test cases
%%====================================================================

intervals(_Config) ->
    I = ar_intervals:new(),
    ?assertEqual(0, ar_intervals:count(I)),
    ?assertEqual(0, ar_intervals:sum(I)),
    ?assert(not ar_intervals:is_inside(I, 0)),
    ?assert(not ar_intervals:is_inside(I, 1)),
    ?assertEqual(
        <<"[]">>,
        ar_intervals:serialize(
            #{random_subset => true, format => json, limit => 1}, I
        )
    ),
    ?assertEqual(
        <<"[]">>,
        ar_intervals:serialize(#{start => 0, format => json, limit => 1}, I)
    ),
    ?assertEqual(
        <<"[]">>,
        ar_intervals:serialize(#{start => 1, format => json, limit => 1}, I)
    ),
    ?assertEqual(
        {ok, ar_intervals:new()},
        ar_intervals:safe_from_etf(
            ar_intervals:serialize(
                #{random_subset => true, format => etf, limit => 1}, I
            )
        )
    ),
    ?assertEqual(ar_intervals:new(), ar_intervals:outerjoin(I, I)),
    ?assertEqual(ar_intervals:new(), ar_intervals:delete(I, 2, 1)),
    I2 = ar_intervals:add(I, 2, 1),
    ?assertEqual(1, ar_intervals:count(I2)),
    ?assertEqual(1, ar_intervals:sum(I2)),
    ?assert(not ar_intervals:is_inside(I2, 0)),
    ?assert(not ar_intervals:is_inside(I2, 1)),
    ?assert(ar_intervals:is_inside(I2, 2)),
    ?assert(not ar_intervals:is_inside(I2, 3)),
    ?assertEqual(ar_intervals:new(), ar_intervals:delete(I2, 2, 1)),
    ?assertEqual(ar_intervals:new(), ar_intervals:delete(I2, 2, 0)),
    ?assertEqual(ar_intervals:new(), ar_intervals:delete(I2, 3, 1)),
    ?assertEqual(ar_intervals:new(), ar_intervals:delete(I2, 3, 0)),
    ?assertEqual(ar_intervals:new(), ar_intervals:cut(I2, 1)),
    ?assertEqual(ar_intervals:new(), ar_intervals:cut(I2, 0)),
    ?assertEqual(
        interval_contents(I2), interval_contents(ar_intervals:cut(I2, 2))
    ),
    ?assertEqual(
        interval_contents(I2), interval_contents(ar_intervals:cut(I2, 3))
    ),
    ?assertEqual(
        <<"[{\"2\":\"1\"}]">>,
        ar_intervals:serialize(
            #{random_subset => true, limit => 1, format => json}, I2
        )
    ),
    ?assertEqual(
        <<"[]">>,
        ar_intervals:serialize(
            #{random_subset => true, limit => 0, format => json}, I2
        )
    ),
    ?assertEqual(
        <<"[{\"2\":\"1\"}]">>,
        ar_intervals:serialize(#{start => 2, limit => 1, format => json}, I2)
    ),
    ?assertEqual(
        <<"[]">>,
        ar_intervals:serialize(#{start => 3, limit => 1, format => json}, I2)
    ),
    ?assertEqual(
        <<"[]">>,
        ar_intervals:serialize(#{start => 2, limit => 0, format => json}, I2)
    ),
    {ok, I2_FromETF} =
        ar_intervals:safe_from_etf(
            ar_intervals:serialize(
                #{format => etf, limit => 1, random_subset => true}, I2
            )
        ),
    ?assertEqual(interval_contents(I2), interval_contents(I2_FromETF)),
    ?assertEqual(
        {ok, ar_intervals:new()},
        ar_intervals:safe_from_etf(
            ar_intervals:serialize(
                #{format => etf, limit => 0, random_subset => true}, I2
            )
        )
    ),
    ?assertEqual(
        interval_contents(I2), interval_contents(ar_intervals:add(I2, 2, 1))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 3, 1)),
        interval_contents(ar_intervals:add(I2, 3, 1))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 2, 0)),
        interval_contents(ar_intervals:add(I2, 2, 0))
    ),
    ?assertEqual(ar_intervals:new(), ar_intervals:outerjoin(I2, I)),
    ?assertEqual(
        interval_contents(
            ar_intervals:add(ar_intervals:add(ar_intervals:new(), 1, 0), 3, 2)
        ),
        interval_contents(
            ar_intervals:outerjoin(
                I2, ar_intervals:add(ar_intervals:new(), 3, 0)
            )
        )
    ),
    I3 = ar_intervals:add(I2, 6, 3),
    ?assertEqual(2, ar_intervals:count(I3)),
    ?assertEqual(4, ar_intervals:sum(I3)),
    ?assert(not ar_intervals:is_inside(I3, 0)),
    ?assert(not ar_intervals:is_inside(I3, 1)),
    ?assert(ar_intervals:is_inside(I3, 2)),
    ?assert(not ar_intervals:is_inside(I3, 3)),
    ?assert(ar_intervals:is_inside(I3, 4)),
    ?assert(ar_intervals:is_inside(I3, 5)),
    ?assert(ar_intervals:is_inside(I3, 6)),
    ?assertEqual(
        interval_contents(
            ar_intervals:add(
                ar_intervals:add(
                    ar_intervals:add(ar_intervals:new(), 2, 1), 6, 5
                ),
                4,
                3
            )
        ),
        interval_contents(ar_intervals:delete(I3, 5, 4))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 6, 5)),
        interval_contents(ar_intervals:delete(I3, 5, 1))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 10, 0)),
        interval_contents(ar_intervals:add(I3, 10, 0))
    ),
    ?assertEqual(ar_intervals:new(), ar_intervals:cut(I3, 1)),
    ?assertEqual(ar_intervals:new(), ar_intervals:cut(I3, 0)),
    ?assertEqual(I2, ar_intervals:cut(I3, 2)),
    ?assertEqual(I2, ar_intervals:cut(I3, 3)),
    ?assertEqual(
        interval_contents(ar_intervals:add(I2, 4, 3)),
        interval_contents(ar_intervals:cut(I3, 4))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(I2, 5, 3)),
        interval_contents(ar_intervals:cut(I3, 5))
    ),
    ?assertEqual(
        interval_contents(I3), interval_contents(ar_intervals:cut(I3, 6))
    ),
    ?assertEqual(
        <<"[{\"6\":\"3\"},{\"2\":\"1\"}]">>,
        ar_intervals:serialize(
            #{random_subset => true, limit => 1000, format => json}, I3
        )
    ),
    ?assertEqual(
        <<"[{\"6\":\"3\"},{\"2\":\"1\"}]">>,
        ar_intervals:serialize(#{start => 1, limit => 10, format => json}, I3)
    ),
    ?assertEqual(
        <<"[{\"2\":\"1\"}]">>,
        ar_intervals:serialize(#{start => 1, limit => 1, format => json}, I3)
    ),
    ?assertEqual(
        <<"[{\"6\":\"3\"}]">>,
        ar_intervals:serialize(#{start => 3, limit => 10, format => json}, I3)
    ),
    {ok, I3_FromETF} =
        ar_intervals:safe_from_etf(
            ar_intervals:serialize(
                #{format => etf, limit => 1000, random_subset => true}, I3
            )
        ),
    ?assertEqual(interval_contents(I3), interval_contents(I3_FromETF)),
    ?assertEqual(
        interval_contents(I3), interval_contents(ar_intervals:add(I3, 4, 3))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 6, 1)),
        interval_contents(ar_intervals:add(I3, 3, 1))
    ),
    I3_2 = ar_intervals:add(ar_intervals:new(), 7, 5),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 7, 5)),
        interval_contents(ar_intervals:outerjoin(I2, I3_2))
    ),
    ?assertEqual(
        interval_contents(ar_intervals:add(ar_intervals:new(), 7, 6)),
        interval_contents(ar_intervals:outerjoin(I3, I3_2))
    ),
    ?assertEqual(
        interval_contents(
            ar_intervals:add(
                ar_intervals:add(
                    ar_intervals:add(ar_intervals:new(), 1, 0), 3, 2
                ),
                8,
                6
            )
        ),
        interval_contents(
            ar_intervals:outerjoin(
                I3, ar_intervals:add(ar_intervals:new(), 8, 0)
            )
        )
    ),
    I4 = ar_intervals:add(I3, 7, 6),
    ?assertEqual(2, ar_intervals:count(I4)),
    ?assertEqual(5, ar_intervals:sum(I4)),
    ?assert(not ar_intervals:is_inside(I4, 0)),
    ?assert(not ar_intervals:is_inside(I4, 1)),
    ?assert(ar_intervals:is_inside(I4, 2)),
    ?assert(not ar_intervals:is_inside(I4, 3)),
    ?assert(ar_intervals:is_inside(I4, 4)),
    ?assert(ar_intervals:is_inside(I4, 5)),
    ?assert(ar_intervals:is_inside(I4, 6)),
    ?assert(ar_intervals:is_inside(I4, 7)),
    ?assert(not ar_intervals:is_inside(I4, 8)),
    ?assertEqual(ar_intervals:new(), ar_intervals:cut(I4, 1)),
    ?assertEqual(ar_intervals:new(), ar_intervals:cut(I4, 0)),
    ?assertEqual(
        interval_contents(ar_intervals:add(I2, 5, 3)),
        interval_contents(ar_intervals:cut(I4, 5))
    ),
    ?assertEqual(
        interval_contents(I4), interval_contents(ar_intervals:cut(I4, 7))
    ),
    ?assertEqual(
        <<"[{\"7\":\"3\"},{\"2\":\"1\"}]">>,
        ar_intervals:serialize(
            #{format => json, limit => 1000, random_subset => true}, I4
        )
    ),
    {ok, I4_FromETF} = ar_intervals:safe_from_etf(
        ar_intervals:serialize(
            #{
                limit => 1000,
                random_subset => true,
                format => etf
            },
            I4
        )
    ),
    ?assertEqual(interval_contents(I4), interval_contents(I4_FromETF)),
    I5 = ar_intervals:add(I4, 3, 2),
    ?assertEqual(1, ar_intervals:count(I5)),
    ?assertEqual(6, ar_intervals:sum(I5)),
    ?assertEqual(
        interval_contents(I5), interval_contents(ar_intervals:add(I5, 3, 2))
    ),
    ?assertEqual(
        interval_contents(I5), interval_contents(ar_intervals:add(I5, 2, 1))
    ),
    ?assertEqual(
        interval_contents(
            ar_intervals:add(ar_intervals:add(ar_intervals:new(), 3, 2), 8, 7)
        ),
        interval_contents(
            ar_intervals:delete(
                ar_intervals:add(
                    ar_intervals:add(ar_intervals:new(), 4, 2), 8, 6
                ),
                7,
                3
            )
        )
    ).

%%====================================================================
%% Helpers
%%====================================================================

interval_contents(Intervals) ->
    {
        ar_intervals:serialize(
            #{
                format => json,
                limit => ar_intervals:count(Intervals),
                start => 0
            },
            Intervals
        ),
        gb_sets:fold(fun({K, V}, Acc) -> [{K, V} | Acc] end, [], Intervals)
    }.
