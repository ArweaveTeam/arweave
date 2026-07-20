-module(ar_coordinated_mining_api_auth_tests).
-test_peers([peer1, peer2, peer3, peer4]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

api_auth_test_() ->
    [
        {timeout, ?TEST_NODE_TIMEOUT, fun ar_coordinated_mining_tests:test_no_secret/0},
        {timeout, ?TEST_NODE_TIMEOUT, fun ar_coordinated_mining_tests:test_bad_secret/0}
    ].
