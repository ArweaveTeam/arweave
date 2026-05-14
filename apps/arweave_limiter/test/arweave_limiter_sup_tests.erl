-module(arweave_limiter_sup_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_sup).

children_spec_test() ->
    ChildSpec = ?M:children_spec([#{id => first,
                                    number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS},
                                  #{id => second,
                                    number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS}]),
    ?assertEqual(2 * ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS, length(ChildSpec)),
    ok.

child_spec_test() ->
    ChildSpec = ?M:children_spec_per_group(
                   #{id => first,
                     number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS}),
    ?assertEqual(?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS, length(ChildSpec)),
    ok.
