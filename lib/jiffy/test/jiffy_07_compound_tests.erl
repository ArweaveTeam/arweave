% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_07_compound_tests).


-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


compound_success_test_() ->
    [gen(ok, Case) || Case <- cases(ok)].


compound_failure_test_() ->
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
        {<<"[{}]">>, [{[]}]},
        {<<"{\"foo\":[123]}">>, {[{<<"foo">>, [123]}]}},
        {<<"{\"foo\":{\"bar\":true}}">>,
            {[{<<"foo">>, {[{<<"bar">>, true}]} }]} },
        {<<"{\"foo\":[],\"bar\":{\"baz\":true},\"alice\":\"bob\"}">>,
            {[
                {<<"foo">>, []},
                {<<"bar">>, {[{<<"baz">>, true}]}},
                {<<"alice">>, <<"bob">>}
            ]}
        },
        {<<"[-123,\"foo\",{\"bar\":[]},null]">>,
            [
                -123,
                <<"foo">>,
                {[{<<"bar">>, []}]},
                null
            ]
        }
    ];

cases(error) ->
    [
        <<"[{}">>,
        <<"}]">>
    ].
