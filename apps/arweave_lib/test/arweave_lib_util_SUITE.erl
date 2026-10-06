-module(arweave_lib_util_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
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

increment_map_value(_Config) ->
    %% A missing key starts at one; incrementing it again raises it to two.
    ?assertEqual(#{key => 1}, arweave_lib_util:increment_map_value(key, #{})),
    ?assertEqual(
        #{key => 2}, arweave_lib_util:increment_map_value(key, #{key => 1})
    ).

basic_unique(_Config) ->
    [a, b, c] = arweave_lib_util:unique([a, a, b, b, b, c, c]),
    [a, b, c] = arweave_lib_util:unique([a, b, c, c, b, a]).

index_of(_Config) ->
    ?assertEqual(1, arweave_lib_util:index_of(a, [a, b, c])),
    ?assertEqual(3, arweave_lib_util:index_of(c, [a, b, c])),
    ?assertEqual(1, arweave_lib_util:index_of(a, [a, a])),
    ?assertEqual(undefined, arweave_lib_util:index_of(d, [a, b, c])),
    ?assertEqual(undefined, arweave_lib_util:index_of(a, [])).

split_at_most(_Config) ->
    ?assertEqual({[], []}, arweave_lib_util:split_at_most(3, [])),
    ?assertEqual({[], [a, b]}, arweave_lib_util:split_at_most(0, [a, b])),
    ?assertEqual({[a, b], []}, arweave_lib_util:split_at_most(3, [a, b])),
    ?assertEqual({[a, b], [c]}, arweave_lib_util:split_at_most(2, [a, b, c])).

basic_peer_format(_Config) ->
    <<"127.0.0.1:9001">> = arweave_lib_util:format_peer({127, 0, 0, 1, 9001}).

pick_random(_Config) ->
    List = [a, b, c, d, e],
    true = lists:member(arweave_lib_util:pick_random(List), List).

round_trip_encode(_Config) ->
    lists:map(
        fun(Bytes) ->
            Bin = crypto:strong_rand_bytes(Bytes),
            Bin = arweave_lib_util:decode(arweave_lib_util:encode(Bin))
        end,
        lists:seq(1, 64)
    ).

pmap(_Config) ->
    %% Deliberately finish out of order; pmap must preserve input order.
    Mapper = fun(X) ->
        timer:sleep(100 * X),
        X * 2
    end,
    ?assertEqual([6, 2, 4], arweave_lib_util:pmap(Mapper, [3, 1, 2])).

encode_list_indices(_Config) ->
    %% A thousand index bits fit in 125 bytes.
    lists:foldl(
        fun(Input, N) ->
            ?assertEqual(Input, lists:sort(Input)),
            Encoded = arweave_lib_util:encode_list_indices(Input),
            ?assert(byte_size(Encoded) =< 125),
            Indices = arweave_lib_util:parse_list_indices(Encoded),
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
