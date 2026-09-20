-module(arweave_limiter_util_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [worker_name, worker_assignment].

%%====================================================================
%% Test cases
%%====================================================================

worker_name(_Config) ->
    ?assertEqual(
        'arweave_limiter_test_limiter_0',
        arweave_limiter_util:worker_name(test_limiter, 0)
    ),
    ok.

worker_assignment(_Config) ->
    ?assertEqual(
        'arweave_limiter_test_limiter_0',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 4}, 1)
    ),
    ?assertEqual(
        'arweave_limiter_test_limiter_0',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 0}, 5)
    ),
    ?assertEqual(
        'arweave_limiter_test_limiter_1',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 1}, 5)
    ),
    ?assertEqual(
        'arweave_limiter_test_limiter_2',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 2}, 5)
    ),
    ?assertEqual(
        'arweave_limiter_test_limiter_3',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 3}, 5)
    ),
    ?assertEqual(
        'arweave_limiter_test_limiter_4',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 4}, 5)
    ),
    ?assertEqual(
        'arweave_limiter_test_limiter_0',
        arweave_limiter_util:worker_ref(test_limiter, {1, 2, 3, 5}, 5)
    ),
    ok.
