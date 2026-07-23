-module(ar_data_roots_duplicate_chunk_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

chunk_skipped_with_duplicate_data_root_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        ar_data_roots_sync_tests:data_roots_sync_mocks(),
        fun ar_data_roots_sync_tests:test_chunk_skipped_with_duplicate_data_root/0).

chunk_skipped_with_depth_exhaustion_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        ar_data_roots_sync_tests:data_roots_sync_mocks(),
        fun ar_data_roots_sync_tests:test_chunk_skipped_with_depth_exhaustion/0).

chunk_persists_with_infinite_duplicate_data_root_depth_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        ar_data_roots_sync_tests:data_roots_sync_mocks(),
        fun ar_data_roots_sync_tests:test_chunk_persists_with_infinite_duplicate_data_root_depth/0).
