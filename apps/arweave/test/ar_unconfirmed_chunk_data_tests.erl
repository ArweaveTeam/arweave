-module(ar_unconfirmed_chunk_data_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").

-define(TIMEOUT, ?TEST_NODE_TIMEOUT).

multi_chunk_tx_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_multi_chunk_tx/0}.

same_data_different_txs_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_same_data_different_txs/0}.

same_data_second_tx_after_seed_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_same_data_second_tx_after_seed/0}.

same_data_after_first_tx_confirmed_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_same_data_after_first_tx_confirmed/0}.

same_data_after_disk_pool_cleared_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_same_data_after_disk_pool_cleared/0}.

data_path_valid_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_data_path_valid/0}.

concurrent_requests_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_concurrent_requests/0}.

discover_all_unconfirmed_chunks_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_discover_all_unconfirmed_chunks/0}.
