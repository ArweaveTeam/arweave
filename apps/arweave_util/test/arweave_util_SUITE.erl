-module(arweave_util_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        parse_peer,
        increment_map_value,
        basic_unique,
        index_of,
        split_at_most,
        basic_peer_format,
        pick_random,
        round_trip_encode,
        pmap,
        encode_list_indices
    ].

%%====================================================================
%% Test cases
%%====================================================================

parse_peer(_Config) ->
    ?assertThrow(
        empty_peer_string,
        arweave_util:parse_peer("")
    ),
    ?assertThrow(
        invalid_peer,
        arweave_util:parse_peer(1)
    ),
    ?assertEqual(
        [{127, 0, 0, 1, 1985}],
        arweave_util:parse_peer({{127, 0, 0, 1}, 1985})
    ),

    Opts = #{module_resolve => ar_test_inet_mock},
    ?assertEqual(
        [{127, 0, 0, 1, 1984}],
        arweave_util:parse_peer("single.record.local", Opts)
    ),
    ?assertEqual(
        [
            {127, 0, 0, 2, 1984},
            {127, 0, 0, 3, 1984},
            {127, 0, 0, 4, 1984},
            {127, 0, 0, 5, 1984}
        ],
        arweave_util:parse_peer("multi.record.local", Opts)
    ),
    ?assertThrow(
        {invalid_peer_string, _, _},
        arweave_util:parse_peer("error.test.local", Opts)
    ).

increment_map_value(_Config) ->
    %% A missing key starts at one; incrementing it again raises it to two.
    ?assertEqual(#{key => 1}, arweave_util:increment_map_value(key, #{})),
    ?assertEqual(
        #{key => 2}, arweave_util:increment_map_value(key, #{key => 1})
    ).

basic_unique(_Config) ->
    [a, b, c] = arweave_util:unique([a, a, b, b, b, c, c]),
    [a, b, c] = arweave_util:unique([a, b, c, c, b, a]).

index_of(_Config) ->
    ?assertEqual(1, arweave_util:index_of(a, [a, b, c])),
    ?assertEqual(3, arweave_util:index_of(c, [a, b, c])),
    ?assertEqual(1, arweave_util:index_of(a, [a, a])),
    ?assertEqual(undefined, arweave_util:index_of(d, [a, b, c])),
    ?assertEqual(undefined, arweave_util:index_of(a, [])).

split_at_most(_Config) ->
    ?assertEqual({[], []}, arweave_util:split_at_most(3, [])),
    ?assertEqual({[], [a, b]}, arweave_util:split_at_most(0, [a, b])),
    ?assertEqual({[a, b], []}, arweave_util:split_at_most(3, [a, b])),
    ?assertEqual({[a, b], [c]}, arweave_util:split_at_most(2, [a, b, c])).

basic_peer_format(_Config) ->
    <<"127.0.0.1:9001">> = arweave_util:format_peer({127, 0, 0, 1, 9001}).

pick_random(_Config) ->
    List = [a, b, c, d, e],
    true = lists:member(arweave_util:pick_random(List), List).

round_trip_encode(_Config) ->
    lists:map(
        fun(Bytes) ->
            Bin = crypto:strong_rand_bytes(Bytes),
            Bin = arweave_util:decode(arweave_util:encode(Bin))
        end,
        lists:seq(1, 64)
    ).

pmap(_Config) ->
    %% Deliberately finish out of order; pmap must preserve input order.
    Mapper = fun(X) ->
        timer:sleep(100 * X),
        X * 2
    end,
    ?assertEqual([6, 2, 4], arweave_util:pmap(Mapper, [3, 1, 2])).

encode_list_indices(_Config) ->
    %% A thousand index bits fit in 125 bytes.
    lists:foldl(
        fun(Input, N) ->
            ?assertEqual(Input, lists:sort(Input)),
            Encoded = arweave_util:encode_list_indices(Input),
            ?assert(byte_size(Encoded) =< 125),
            Indices = arweave_util:parse_list_indices(Encoded),
            ?assertEqual(Input, Indices, io_lib:format("Case ~B", [N])),
            N + 1
        end,
        0,
        [
            [],
            [0],
            [1],
            [999],
            [0, 1],
            lists:seq(0, 999),
            lists:seq(0, 999, 2),
            lists:seq(1, 999, 3)
        ]
    ).
