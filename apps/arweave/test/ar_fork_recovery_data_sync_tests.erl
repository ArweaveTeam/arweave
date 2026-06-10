-module(ar_fork_recovery_data_sync_tests).
-test_category([vdf]).
-test_peers([peer1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

fork_recovery_test_() ->
	%% Allow headroom for many sequential wait_until_syncs_chunks calls on slow CI.
	{timeout, 480, fun test_fork_recovery/0}.

test_fork_recovery() ->
	test_fork_recovery(original_split).

test_fork_recovery(Split) ->
	Wallet = ar_test_data_sync:setup_nodes(#{ packing => replica_2_9 }),
	{TX1, Chunks1} = ar_test_data_sync:tx(Wallet, {Split, 3}, v2, ?AR(10)),
	?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(TX1#tx.id)]),
	B1 = ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX1]),
	?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(B1#block.indep_hash),
			B1#block.height]),
	Proofs1 = ar_test_data_sync:post_proofs(main, B1, TX1, Chunks1),
	ar_test_data_sync:wait_until_syncs_chunks(Proofs1),
	{Height, BI} = ar_node:get_block_index_and_height(),
	UpperBound = ar_node:get_partition_upper_bound(Height, BI),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, UpperBound),
	ar_test_node:disconnect_from(peer1),
	{PeerTX2, PeerChunks2} = ar_test_data_sync:tx(Wallet, {Split, 5}, v2, ?AR(10)),
	{PeerTX3, PeerChunks3} = ar_test_data_sync:tx(Wallet, {Split, 2}, v2, ?AR(10)),
	?debugFmt("Posting tx to peer1 ~s.~n", [ar_util:encode(PeerTX2#tx.id)]),
	?debugFmt("Posting tx to peer1 ~s.~n", [ar_util:encode(PeerTX3#tx.id)]),
	PeerB2 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
			[PeerTX2, PeerTX3]),
	?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(PeerB2#block.indep_hash),
			PeerB2#block.height]),
	{MainTX2, MainChunks2} = ar_test_data_sync:tx(Wallet, {Split, 1}, v2, ?AR(10)),
	?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(MainTX2#tx.id)]),
	MainB2 = ar_test_node:post_and_mine(#{ miner => main, await_on => main },
			[MainTX2]),
	?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(MainB2#block.indep_hash),
			MainB2#block.height]),
	_PeerProofs2 = ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX2, PeerChunks2),
	_PeerProofs3 = ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX3, PeerChunks3),
	{PeerTX4, PeerChunks4} = ar_test_data_sync:tx(Wallet, {Split, 2}, v2, ?AR(10)),
	?debugFmt("Posting tx to peer1 ~s.~n", [ar_util:encode(PeerTX4#tx.id)]),
	PeerB3 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
			[PeerTX4]),
	?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(PeerB3#block.indep_hash),
			PeerB3#block.height]),
	_PeerProofs4 = ar_test_data_sync:post_proofs(peer1, PeerB3, PeerTX4, PeerChunks4),
	ar_test_node:post_and_mine(#{ miner => main, await_on => main }, []),
	MainProofs2 = ar_test_data_sync:post_proofs(main, MainB2, MainTX2, MainChunks2),
	%% Keep this proof posted before the reorg, but wait until main can serve it.
	ar_test_data_sync:wait_until_syncs_chunks(MainProofs2, infinity),
	{MainTX3, MainChunks3} = ar_test_data_sync:tx(Wallet, {Split, 1}, v2, ?AR(10)),
	?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(MainTX3#tx.id)]),
	MainB3 = ar_test_node:post_and_mine(#{ miner => main, await_on => main },
			[MainTX3]),
	?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(MainB3#block.indep_hash),
			MainB3#block.height]),
	{MainHeight, MainBI} = ar_node:get_block_index_and_height(),
	ar_test_node:connect_to_peer(peer1),
	ok = ar_test_await:block_index_matches(peer1, MainBI),
	UpperBound2 = ar_node:get_partition_upper_bound(MainHeight, MainBI),
	MainProofs3 = ar_test_data_sync:post_proofs(main, MainB3, MainTX3, MainChunks3),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, MainProofs2, UpperBound2),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, MainProofs3, UpperBound2),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, infinity),
	%% The peer1 node will return the orphaned transactions to the mempool
	%% and gossip them.
	?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(PeerTX2#tx.id)]),
	?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(PeerTX4#tx.id)]),
	ar_test_node:post_tx_to_peer(main, PeerTX2),
	ar_test_node:post_tx_to_peer(main, PeerTX4),
	?assertEqual(ok, ar_test_await:txs_ready_for_mining(main, [PeerTX2, PeerTX4])),
	MainB4 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, []),
	?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(MainB4#block.indep_hash),
			MainB4#block.height]),
	Proofs4 = ar_test_data_sync:post_proofs(main, MainB4, PeerTX4, PeerChunks4),
	%% PeerTX4's proofs were never posted to main, so they stay in the disk pool.
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs4, infinity),
	{Height3, BI3} = ar_node:get_block_index_and_height(),
	UpperBound3 = ar_node:get_partition_upper_bound(Height3, BI3),
	ar_test_data_sync:wait_until_syncs_chunks(Proofs4, UpperBound3),
	ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX2, PeerChunks2).
