% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_13_whitespace_tests).

-include_lib("eunit/include/eunit.hrl").


trailing_whitespace_test_() ->
    Str = [<<"{\"a\":\"b\"}">>, gen_ws(2040)],
    Obj = {[{<<"a">>, <<"b">>}]},
    ?_assertEqual(Obj, jiffy:decode(Str)).


gen_ws(N) ->
   [" " || _ <- lists:seq(1, N)]. 