% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_16_dedupe_keys_tests).

-include_lib("eunit/include/eunit.hrl").

dedupe_keys_test_() ->
    Opts = [dedupe_keys],
    Cases = [
        % Simple sanity check
        {
            {[{<<"foo">>, 1}]},
            {[{<<"foo">>, 1}]}
        },
        % Basic test
        {
            {[{<<"foo">>, 1}, {<<"foo">>, 2}]},
            {[{<<"foo">>, 2}]}
        },
        % Non-repeated keys are fine
        {
            {[{<<"foo">>, 1}, {<<"bar">>, 2}]},
            {[{<<"foo">>, 1}, {<<"bar">>, 2}]}
        },
        % Key order stays the same other than deduped keys
        {
            {[{<<"bar">>, 1}, {<<"foo">>, 2}, {<<"baz">>, 3}, {<<"foo">>, 4}]},
            {[{<<"bar">>, 1}, {<<"baz">>, 3}, {<<"foo">>, 4}]}
        },
        % Multiple repeats are handled
        {
            {[{<<"foo">>, 1}, {<<"foo">>, 2}, {<<"foo">>, 3}]},
            {[{<<"foo">>, 3}]}
        },
        % Sub-objects are covered
        {
            {[{<<"foo">>, {[{<<"bar">>, 1}, {<<"bar">>, 2}]}}]},
            {[{<<"foo">>, {[{<<"bar">>, 2}]}}]}
        },
        % Objets in arrays are handled
        {
            [{[{<<"foo">>, 1}, {<<"foo">>, 2}]}],
            [{[{<<"foo">>, 2}]}]
        },
        % Embedded NULL bytes are handled
        {
            {[{<<"foo\\u0000bar">>, 1}, {<<"foo\\u0000baz">>, 2}]},
            {[{<<"foo\\u0000bar">>, 1}, {<<"foo\\u0000baz">>, 2}]}
        },
        % Can dedupe with embedded NULL bytes
        {
            {[{<<"foo\\u0000bar">>, 1}, {<<"foo\\u0000bar">>, 2}]},
            {[{<<"foo\\u0000bar">>, 2}]}
        }
    ],
    {"Test dedupe_keys", lists:map(fun({Data, Result}) ->
        Json = jiffy:encode(Data),
        ?_assertEqual(Result, jiffy:decode(Json, Opts))
    end, Cases)}.
