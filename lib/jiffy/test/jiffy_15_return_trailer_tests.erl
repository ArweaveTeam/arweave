% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_15_return_trailer_tests).

-include_lib("eunit/include/eunit.hrl").

trailer_test_() ->
    Opts = [return_trailer],
    Cases = [
        {<<"true">>, true},
        {<<"true;">>, {has_trailer, true, <<";">>}},
        {<<"true[]">>, {has_trailer, true, <<"[]">>}},
        {<<"[]{}">>, {has_trailer, [], <<"{}">>}},
        {<<"1 2 3">>, {has_trailer, 1, <<"2 3">>}}
    ],
    {"Test return_trailer", lists:map(fun({Data, Result}) ->
        ?_assertEqual(Result, jiffy:decode(Data, Opts))
    end, Cases)}.