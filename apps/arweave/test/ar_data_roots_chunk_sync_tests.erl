-module(ar_data_roots_chunk_sync_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

chunk_after_data_roots_http_post_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        ar_data_roots_sync_tests:data_roots_sync_mocks(),
        fun ar_data_roots_sync_tests:test_chunk_after_data_roots_http_post/0).

chunk_after_data_roots_background_sync_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        ar_data_roots_sync_tests:data_roots_sync_mocks(),
        fun ar_data_roots_sync_tests:test_chunk_after_data_roots_background_sync/0).

chunk_in_unconfigured_partition_requires_manual_data_roots_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        ar_data_roots_sync_tests:data_roots_sync_mocks(),
        fun ar_data_roots_sync_tests:test_chunk_in_unconfigured_partition_requires_manual_data_roots/0).
