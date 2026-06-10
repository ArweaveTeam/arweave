-module(ar_coordinated_mining_cross_node_tests).
-test_peers([peer1, peer2, peer3, peer4]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

cross_node_test_() ->
	[
		ar_test_node:test_with_all_nodes_mocked(
			[
				ar_test_node:mock_to_force_invalid_h1(),
				ar_test_node:mock_to_force_cross_node_h2(),
				{ar_retarget, is_retarget_height, fun(_Height) -> false end},
				{ar_retarget, is_retarget_block, fun(_Block) -> false end}
			],
			fun ar_coordinated_mining_tests:test_cross_node/0, ?TEST_NODE_TIMEOUT),
		ar_test_node:test_with_all_nodes_mocked(
			[
				{ar_retarget, is_retarget_height, fun(_Height) -> false end},
				{ar_retarget, is_retarget_block, fun(_Block) -> false end}
			],
			fun ar_coordinated_mining_tests:test_three_node/0, 2 * ?TEST_NODE_TIMEOUT)
	].
