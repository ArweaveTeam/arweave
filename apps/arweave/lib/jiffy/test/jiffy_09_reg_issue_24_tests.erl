% This file is part of Jiffy released under the MIT license.
% See the LICENSE file for more information.

-module(jiffy_09_reg_issue_24_tests).


-include_lib("eunit/include/eunit.hrl").
-include("jiffy_util.hrl").


no_segfault_test_() ->
    {"no segfault", [
        ?_assert(begin jiffy:encode(gen_string(), [uescape]), true end)
    ]}.


gen_string() ->
    << <<226,152,131>> || _ <- lists:seq(0, round(math:pow(2, 19))) >>.
