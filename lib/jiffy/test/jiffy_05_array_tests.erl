% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_05_array_tests).


-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


array_success_test_() ->
    [gen(ok, Case) || Case <- cases(ok)].


array_failure_test_() ->
    [gen(error, Case) || Case <- cases(error)].


gen(ok, {J, E}) ->
    gen(ok, {J, E, J});
gen(ok, {J1, E, J2}) ->
    {msg("~s", [J1]), [
        {"Decode", ?_assertEqual(E, dec(J1))},
        {"Encode", ?_assertEqual(J2, enc(E))}
    ]};

gen(error, J) ->
    {msg("Error: ~s", [J]), [
        ?_assertThrow({error, _}, dec(J))
    ]}.


cases(ok) ->
    [
        {<<"[]">>, []},
        {<<"[\t[\n]\r]">>, [[]], <<"[[]]">>},
        {<<"[\t123, \r true\n]">>, [123, true], <<"[123,true]">>},
        {<<"[1,\"foo\"]">>, [1, <<"foo">>]},
        {<<"[11993444355.0,1]">>, [11993444355.0,1]},
        {
            <<"[\"\\u00A1\",\"\\u00FC\"]">>,
            [<<194, 161>>, <<195, 188>>],
            <<"[\"", 194, 161, "\",\"", 195, 188, "\"]">>
        }
    ];

cases(error) ->
    [
        <<"[">>,
        <<"]">>,
        <<"[,]">>,
        <<"[123">>,
        <<"[123,]">>,
        <<"[32 true]">>
    ].
