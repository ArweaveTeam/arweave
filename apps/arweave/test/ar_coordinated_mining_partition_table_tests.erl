-module(ar_coordinated_mining_partition_table_tests).
-test_peers([peer1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

partition_table_test_() ->
	[
		{timeout, ?TEST_NODE_TIMEOUT,
			fun ar_coordinated_mining_tests:test_partition_table/0}
	].
