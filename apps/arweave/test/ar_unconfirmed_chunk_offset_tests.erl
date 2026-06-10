-module(ar_unconfirmed_chunk_offset_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").

-define(TIMEOUT, ?TEST_NODE_TIMEOUT).

offset_boundary_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_offset_boundary/0}.

offset_beyond_tx_size_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_offset_beyond_tx_size/0}.

negative_offset_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_negative_offset/0}.

offset_beyond_data_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_offset_beyond_data/0}.

not_found_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_not_found/0}.

invalid_input_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_invalid_input/0}.

sub_chunk_size_test_() ->
	{timeout, ?TIMEOUT, fun ar_unconfirmed_chunk_tests:test_sub_chunk_size/0}.
