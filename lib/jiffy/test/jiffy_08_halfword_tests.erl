% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_08_halfword_tests).


-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


numerical_identity_test_() ->
    [
        {"1 == 1", ?_assert(jiffy:decode(<<"1">>) == 1)},
        {"1 =:= 1", ?_assert(jiffy:decode(<<"1">>) =:= 1)}
    ].
