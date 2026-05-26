%%%===================================================================
%%% @doc A test that always fail.
%%% @end
%%%===================================================================
%% @ar_test: canary
-module(ar_canary).
-include_lib("eunit/include/eunit.hrl").

canary_test_() ->
    ?assert(4 =:= 5).
