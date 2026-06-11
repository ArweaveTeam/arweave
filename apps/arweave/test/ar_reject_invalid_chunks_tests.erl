-module(ar_reject_invalid_chunks_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").

rejects_invalid_chunks_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun ar_reject_chunks_tests:test_rejects_invalid_chunks/0}.

rejects_chunks_with_merkle_tree_borders_exceeding_max_chunk_size_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT,
			fun ar_reject_chunks_tests:
					test_rejects_chunks_with_merkle_tree_borders_exceeding_max_chunk_size/0}.

rejects_chunks_exceeding_disk_pool_limit_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT,
			fun ar_reject_chunks_tests:test_rejects_chunks_exceeding_disk_pool_limit/0}.

accepts_chunks_test_() ->
	ar_test_node:test_with_all_nodes_mocked([{ar_fork, height_2_5, fun() -> 0 end}],
		fun ar_reject_chunks_tests:test_accepts_chunks/0, ?TEST_NODE_TIMEOUT).
