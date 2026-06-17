-module(ar_reject_small_chunks_tests).
-test_peers([peer1]).

-export([run_case/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

does_not_store_small_chunks_after_2_5_test_() ->
	run_case("Even split").

does_not_store_small_last_chunk_test_() ->
	run_case("Small last chunk").

does_not_store_small_chunks_crossing_the_bucket_test_() ->
	run_case("Small chunks crossing the bucket").

does_not_store_small_chunks_in_one_bucket_test_() ->
	run_case("Small chunks in one bucket").

does_not_store_small_chunk_preceding_full_chunk_test_() ->
	run_case("Small chunk preceding 256 KiB chunk").

%% The cases test the strict_data_split_ruleset (in effect between the
%% strict_data_split_threshold and the merkle_rebase_support_threshold). Push the
%% rebase threshold above the chunk offsets used in the test so the chunks are
%% validated under that ruleset rather than the more permissive
%% offset_rebase_support_ruleset.
run_case(Title) ->
	ar_test_node:test_with_all_nodes_mocked(
		[{ar_block, get_merkle_rebase_support_threshold,
				fun() -> 2 * ar_block:strict_data_split_threshold() end}],
		fun() -> ar_reject_chunks_tests:test_does_not_store_small_chunks_after_2_5(Title) end,
		?TEST_NODE_TIMEOUT).
