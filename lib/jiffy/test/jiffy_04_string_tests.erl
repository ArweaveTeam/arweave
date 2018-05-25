% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_04_string_tests).


-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


string_success_test_() ->
    [gen(ok, Case) || Case <- cases(ok)].


string_uescaped_test_() ->
    [gen(uescaped, Case) || Case <- cases(uescaped)].


string_error_test_() ->
    [gen(error, Case) || Case <- cases(error)].


string_utf8_test_() ->
    [gen(utf8, Case) || Case <- cases(utf8)].


string_bad_utf8_key_test_() ->
    Cases = cases(bad_utf8_key),
    {{J}, {E}} = hd(Cases),
    ExtraProps = [{<<"abcdeefeadasffasdfa">>, I} || I <- lists:seq(1, 10000)],
    Big = {{ExtraProps ++ J}, {ExtraProps ++ E}},
    AllCases = [Big | Cases],
    [gen(bad_utf8_key, Case) || Case <- AllCases].


string_escaped_slashes_test_() ->
    [gen(escaped_slashes, Case) || Case <- cases(escaped_slashes)].

gen(ok, {J, E}) ->
    gen(ok, {J, E, J});
gen(ok, {J1, E, J2}) ->
    {msg("ok - ~s", [J1]), [
        {"Decode", ?_assertEqual(E, dec(J1))},
        {"Encode", ?_assertEqual(J2, enc(E))}
    ]};

gen(uescaped, {J, E}) ->
    {msg("uescape - ~s", [J]), [
        {"Decode", ?_assertEqual(E, dec(J))},
        {"Encode", ?_assertEqual(J, enc(E, [uescape]))}
    ]};

gen(error, J) ->
    {msg("error - ~s", [J]), [
        ?_assertThrow({error, _}, dec(J))
    ]};

gen(utf8, {Case, Fixed}) ->
    Case2 = <<34, Case/binary, 34>>,
    Fixed2 = <<34, Fixed/binary, 34>>,
    {msg("UTF-8: ~s", [hex(Case)]), [
        ?_assertThrow({error, {invalid_string, _}}, jiffy:encode(Case)),
        ?_assertEqual(Fixed2, jiffy:encode(Case, [force_utf8])),
        ?_assertThrow({error, {_, invalid_string}}, jiffy:decode(Case2))
    ]};

gen(bad_utf8_key, {J, E}) ->
    {msg("Bad UTF-8 key: - ~p", [size(term_to_binary(J))]), [
        ?_assertThrow({error, {invalid_object_member_key, _}}, jiffy:encode(J)),
        ?_assertEqual(E, jiffy:decode(jiffy:encode(J, [force_utf8])))
    ]};

gen(escaped_slashes, {J, E}) ->
    {msg("escaped_slashes - ~s", [J]), [
        {"Decode", ?_assertEqual(E, dec(J))},
        {"Encode", ?_assertEqual(J, enc(E, [escape_forward_slashes]))}
    ]}.

cases(ok) ->
    [
        {<<"\"\"">>, <<"">>},
        {<<"\"/\"">>, <<"/">>},
        {<<"\"0\"">>, <<"0">>},
        {<<"\"foo\"">>, <<"foo">>},
        {<<"\"\\\"foobar\\\"\"">>, <<"\"foobar\"">>},
        {<<"\"\\n\\n\\n\"">>, <<"\n\n\n">>},
        {<<"\"\\\" \\b\\f\\r\\n\\t\\\"\"">>, <<"\" \b\f\r\n\t\"">>},
        {<<"\"foo\\u0005bar\"">>, <<"foo", 5, "bar">>},
        {
            <<"\"\\uD834\\uDD1E\"">>,
            <<240, 157, 132, 158>>,
            <<34, 240, 157, 132, 158, 34>>
        },
        {<<"\"\\uFFFF\"">>, <<239,191,191>>, <<34,239,191,191,34>>},
        {<<"\"\\uFFFE\"">>, <<239,191,190>>, <<34,239,191,190,34>>}
    ];

cases(uescaped) ->
    [
        {
            <<"\"\\u8CA8\\u5481\\u3002\\u0091\\u0091\"">>,
            <<232,178,168,229,146,129,227,128,130,194,145,194,145>>
        },
        {
            <<"\"\\uD834\\uDD1E\"">>,
            <<240, 157, 132, 158>>
        },
        {
            <<"\"\\uD83D\\uDE0A\"">>,
            <<240, 159, 152, 138>>
        }
    ];

cases(error) ->
    [
        "\"",
        <<"\"foo">>,
        <<"\"", 0, "\"">>,
        <<"\"\\g\"">>,
        <<"\"\\uD834foo\\uDD1E\"">>,
        % CouchDB-345
        <<34,78,69,73,77,69,78,32,70,216,82,82,32,70,65,69,78,33,34>>
    ];

cases(utf8) ->
    [
        % Stray continuation byte
        {<<16#C2, 16#81, 16#80>>, <<16#C2, 16#81, 16#EF, 16#BF, 16#BD>>},
        {<<"foo", 16#80, "bar">>, <<"foo", 16#EF, 16#BF, 16#BD, "bar">>},

        % Not enough extension bytes
        {<<16#C0>>, <<16#EF, 16#BF, 16#BD>>},

        {<<16#E0>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#E0, 16#80>>, <<16#EF, 16#BF, 16#BD>>},

        {<<16#F0>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#F0, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#F0, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},

        {<<16#F8>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#F8, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#F8, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#F8, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},

        {<<16#FC>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#FC, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#FC, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#FC, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#FC, 16#80, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},

        % No data in high bits.
        {<<16#C0, 16#80>>, <<"\\u0000">>},
        {<<16#C1, 16#80>>, <<"@">>},

        {<<16#E0, 16#80, 16#80>>, <<"\\u0000">>},
        {<<16#E0, 16#90, 16#80>>, <<16#D0, 16#80>>},

        {<<16#F0, 16#80, 16#80, 16#80>>, <<"\\u0000">>},
        {<<16#F0, 16#88, 16#80, 16#80>>, <<16#E8, 16#80, 16#80>>},

        % UTF-8-like sequenecs of greater than 4 bytes
        % aren't valid and are replaced with a single
        % replacement 0xFFFD character.
        {<<16#F8, 16#80, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#F8, 16#84, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#FC, 16#80, 16#80, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>},
        {<<16#FC, 16#82, 16#80, 16#80, 16#80, 16#80>>, <<16#EF, 16#BF, 16#BD>>}
    ];

cases(bad_utf8_key) ->
    [
        {
            {[{<<"foo", 16#80, "bar">>, true}]},
            {[{<<"foo", 16#EF, 16#BF, 16#BD, "bar">>, true}]}
        }
    ];

cases(escaped_slashes) ->
    [
        {<<"\"\\/\"">>, <<"/">>}
    ].
