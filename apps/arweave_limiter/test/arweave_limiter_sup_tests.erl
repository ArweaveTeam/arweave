-module(arweave_limiter_sup_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_sup).

%% `test_limiter' is registered with `number_of_workers => 1' under -ifdef(AR_TEST);
%% `test_limiter_2' uses the default count. See
%% `arweave_config_options_limiter:test_only_groups/0'.

children_spec_total_test() ->
    ChildSpec = ?M:children_spec([test_limiter, test_limiter_2]),
    ?assertEqual(1 + 5, length(ChildSpec)).

children_spec_one_group_test() ->
    ChildSpec = ?M:children_spec_per_group(test_limiter_2),
    ?assertEqual(5, length(ChildSpec)).
