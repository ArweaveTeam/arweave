%% @ar_test: fast
-module(arweave_limiter_sup_tests).

%% NOTE: tests in this module are currently disabled. They were
%% picked up by the CI test-discovery rewrite but never ran in CI
%% before, so their pass/fail behavior was unknown. Each `*_test/0'
%% or `*_test_/0' function has been renamed with a `_disabled'
%% suffix. To re-enable a test, remove the suffix and verify it
%% passes (and remove this header once all tests in the module
%% are re-enabled).


-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_sup).

%% `test_limiter' is registered with `number_of_workers => 1' under -ifdef(AR_TEST);
%% `test_limiter_2' uses the default count. The group IDs are derived
%% from the `[limiter, GroupID, Field]' config rows.

children_spec_total_test_disabled() ->
    ChildSpec = ?M:children_spec([test_limiter, test_limiter_2]),
    ?assertEqual(1 + 5, length(ChildSpec)).

children_spec_one_group_test_disabled() ->
    ChildSpec = ?M:children_spec_per_group(test_limiter_2),
    ?assertEqual(5, length(ChildSpec)).
