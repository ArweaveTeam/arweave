-module(ar_vdf_client_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").

vdf_client_test_() ->
    {foreach,
        fun ar_vdf_server_tests:setup/0,
        fun ar_vdf_server_tests:cleanup/1,
        [
            ar_test_node:test_with_all_nodes_mocked(
                [ar_vdf_server_tests:mock_reset_frequency()],
                fun ar_vdf_server_tests:test_vdf_client_fast_block/0,
                ?TEST_NODE_TIMEOUT),
            ar_test_node:test_with_all_nodes_mocked(
                [ar_vdf_server_tests:mock_reset_frequency()],
                fun ar_vdf_server_tests:test_vdf_client_fast_block_pull_interface/0,
                ?TEST_NODE_TIMEOUT),
            ar_test_node:test_with_all_nodes_mocked(
                [ar_vdf_server_tests:mock_reset_frequency()],
                fun ar_vdf_server_tests:test_vdf_client_slow_block/0,
                ?TEST_NODE_TIMEOUT),
            ar_test_node:test_with_all_nodes_mocked(
                [ar_vdf_server_tests:mock_reset_frequency()],
                fun ar_vdf_server_tests:test_vdf_client_slow_block_pull_interface/0,
                ?TEST_NODE_TIMEOUT)
        ]
    }.
