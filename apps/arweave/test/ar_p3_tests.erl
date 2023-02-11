-module(ar_p3_tests).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    ?debugMsg("HELLO"),
    gen_server:call(ar_p3, {one, two, three}).