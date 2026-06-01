%%%===================================================================
%%% @doc A test that always fail.
%%% @end
%%%===================================================================
-module(ar_canary).
-test_category([canary]).
-include_lib("eunit/include/eunit.hrl").

canary_test_() ->
    ?assert(4 =:= 5).
