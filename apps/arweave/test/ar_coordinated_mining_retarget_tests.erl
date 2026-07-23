-module(ar_coordinated_mining_retarget_tests).
-test_peers([peer1, peer2, peer3, peer4]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

retarget_test_() ->
    [
        {timeout, ?TEST_NODE_TIMEOUT,
            fun ar_coordinated_mining_tests:test_two_node_retarget/0},
        ar_test_node:test_with_all_nodes_mocked(
            [
                ar_test_node:mock_to_force_invalid_h1(),
                ar_test_node:mock_to_force_cross_node_h2(),
                ar_coordinated_mining_tests:mock_for_single_difficulty_adjustment_height(),
                ar_coordinated_mining_tests:mock_for_single_difficulty_adjustment_block()
            ],
            fun ar_coordinated_mining_tests:test_cross_node_retarget/0,
            2 * ?TEST_NODE_TIMEOUT)
    ].
