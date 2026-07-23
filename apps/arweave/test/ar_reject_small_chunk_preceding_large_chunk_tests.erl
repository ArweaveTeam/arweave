-module(ar_reject_small_chunk_preceding_large_chunk_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

does_not_store_small_chunks_after_2_5_test_() ->
    ar_reject_small_chunks_tests:run_case("Small chunk preceding 256 KiB chunk").
