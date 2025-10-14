-module(ar_data_sync_syncs_after_joining_test).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-import(ar_test_node, [assert_wait_until_height/2, test_with_mocked_functions/2]).

syncs_after_joining_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> 0 end}],
		fun test_syncs_after_joining/0, 240).

test_syncs_after_joining() ->
	test_syncs_after_joining(original_split).

test_syncs_after_joining(Split) ->
	?LOG_DEBUG([{event, test_syncs_after_joining}, {split, Split}]),
	Wallet = ar_test_data_sync:setup_nodes(),
	{TX1, Chunks1} = ar_test_data_sync:tx(Wallet, {Split, 1}, v2, ?AR(1)),
	B1 = ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX1]),
	Proofs1 = ar_test_data_sync:post_proofs(main, B1, TX1, Chunks1),
	UpperBound = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, UpperBound),
	ar_test_data_sync:wait_until_syncs_chunks(Proofs1),
	ar_test_node:disconnect_from(peer1),
	{MainTX2, MainChunks2} = ar_test_data_sync:tx(Wallet, {Split, 3}, v2, ?AR(1)),
	MainB2 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [MainTX2]),
	MainProofs2 = ar_test_data_sync:post_proofs(main, MainB2, MainTX2, MainChunks2),
	{MainTX3, MainChunks3} = ar_test_data_sync:tx(Wallet, {Split, 2}, v2, ?AR(1)),
	MainB3 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [MainTX3]),
	MainProofs3 = ar_test_data_sync:post_proofs(main, MainB3, MainTX3, MainChunks3),
	{PeerTX2, PeerChunks2} = ar_test_data_sync:tx(Wallet, {Split, 2}, v2, ?AR(1)),
	PeerB2 = ar_test_node:post_and_mine( #{ miner => peer1, await_on => peer1 }, [PeerTX2] ),
	PeerProofs2 = ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX2, PeerChunks2),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, PeerProofs2, infinity),
	_Peer2 = ar_test_node:rejoin_on(#{ node => peer1, join_on => main }),
	assert_wait_until_height(peer1, 3),
	ar_test_node:connect_to_peer(peer1),
	UpperBound2 = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, MainProofs2, UpperBound2),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, MainProofs3, UpperBound2),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, infinity). 
