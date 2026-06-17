-module(ar_reject_small_chunks_tests).
-test_peers([peer1]).

-export([run_case/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

does_not_store_small_chunks_after_2_5_test_() ->
	run_case("Even split").

run_case(Title) ->
	ar_test_node:test_with_all_nodes_mocked(
		[{ar_block, get_merkle_rebase_support_threshold,
				fun() -> 2 * ar_block:strict_data_split_threshold() end}],
		fun() -> ar_reject_chunks_tests:test_does_not_store_small_chunks_after_2_5(Title) end,
		?TEST_NODE_TIMEOUT).
