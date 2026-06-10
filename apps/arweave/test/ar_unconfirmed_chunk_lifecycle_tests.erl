-module(ar_unconfirmed_chunk_lifecycle_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").

-define(TIMEOUT, ?TEST_NODE_TIMEOUT).

from_disk_pool_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_from_disk_pool/0}.

tx_index_fallback_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_tx_index_fallback/0}.

not_stored_long_term_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_not_stored_long_term/0}.

partial_confirmation_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_partial_confirmation/0}.

orphaned_chunk_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_orphaned_chunk/0}.
