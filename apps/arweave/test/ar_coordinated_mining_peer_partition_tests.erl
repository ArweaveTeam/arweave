-module(ar_coordinated_mining_peer_partition_tests).
-test_peers([peer1, peer2, peer3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

peer_partitions_test_() ->
	[
		{timeout, ?TEST_NODE_TIMEOUT,
			fun ar_coordinated_mining_tests:test_peers_by_partition/0}
	].
