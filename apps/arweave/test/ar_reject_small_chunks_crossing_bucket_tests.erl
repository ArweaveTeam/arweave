-module(ar_reject_small_chunks_crossing_bucket_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

does_not_store_small_chunks_after_2_5_test_() ->
	ar_reject_small_chunks_tests:run_case("Small chunks crossing the bucket").
