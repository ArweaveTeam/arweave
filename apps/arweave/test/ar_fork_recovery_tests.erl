-module(ar_fork_recovery_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
		assert_wait_until_height/2, wait_until_height/1, read_block_when_stored/1]).

height_plus_one_fork_recovery_test_() ->
	{timeout, 120, fun test_height_plus_one_fork_recovery/0}.

test_height_plus_one_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine an extra block on one of them.
	%% Expect the other one to recover.
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 1),
	ar_test_node:mine(),
	wait_until_height(1),
	ar_test_node:mine(),
	MainBI = wait_until_height(2),
	ar_test_node:connect_to_peer(peer1),
	?assertEqual(MainBI, ar_test_node:wait_until_height(peer1, 2)),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:mine(),
	wait_until_height(3),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 3),
	ar_test_node:rejoin_on(#{ node => main, join_on => peer1 }),
	ar_test_node:mine(peer1),
	PeerBI = ar_test_node:wait_until_height(peer1, 4),
	?assertEqual(PeerBI, wait_until_height(4)).

height_plus_three_fork_recovery_test_() ->
	{timeout, 120, fun test_height_plus_three_fork_recovery/0}.

test_height_plus_three_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine three extra blocks on one of them.
	%% Expect the other one to recover.
	{_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 1),
	ar_test_node:mine(),
	wait_until_height(1),
	ar_test_node:mine(),
	wait_until_height(2),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 2),
	ar_test_node:mine(),
	wait_until_height(3),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 3),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:mine(),
	MainBI = wait_until_height(4),
	?assertEqual(MainBI, ar_test_node:wait_until_height(peer1, 4)).

missing_txs_fork_recovery_test_() ->
	{timeout, 120, fun test_missing_txs_fork_recovery/0}.

test_missing_txs_fork_recovery() ->
	%% Mine a block with a transaction on the peer1 node
	%% but do not gossip the transaction. The main node
	%% is expected fetch the missing transaction and apply the block.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	TX1 = ar_test_node:sign_tx(Key, #{}),
	ar_test_node:assert_post_tx_to_peer(peer1, TX1),
	%% Wait to make sure the tx will not be gossiped upon reconnect.
	timer:sleep(2000), % == 2 * ?CHECK_MEMPOOL_FREQUENCY
	ar_test_node:rejoin_on(#{ node => main, join_on => peer1 }),
	?assertEqual([], ar_mempool:get_all_txids()),
	ar_test_node:mine(peer1),
	[{H1, _, _} | _] = wait_until_height(1),
	?assertEqual(1, length((read_block_when_stored(H1))#block.txs)).

orphaned_txs_are_remined_after_fork_recovery_test_() ->
	{timeout, 120, fun test_orphaned_txs_are_remined_after_fork_recovery/0}.

test_orphaned_txs_are_remined_after_fork_recovery() ->
	%% Mine a transaction on peer1, mine two blocks on main to
	%% make the transaction orphaned. Mine a block on peer1 and
	%% assert the transaction is re-mined.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:disconnect_from(peer1),
	TX = #tx{ id = TXID } = ar_test_node:sign_tx(Key, #{ denomination => 1, reward => ?AR(1) }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX),
	ar_test_node:mine(peer1),
	[{H1, _, _} | _] = ar_test_node:wait_until_height(peer1, 1),
	H1TXIDs = (ar_test_node:remote_call(peer1, ar_test_node, read_block_when_stored, [H1]))#block.txs,
	?assertEqual([TXID], H1TXIDs),
	ar_test_node:mine(),
	[{H2, _, _} | _] = wait_until_height(1),
	ar_test_node:mine(),
	[{H3, _, _}, {H2, _, _}, {_, _, _}] = wait_until_height(2),
	ar_test_node:connect_to_peer(peer1),
	?assertMatch([{H3, _, _}, {H2, _, _}, {_, _, _}], ar_test_node:wait_until_height(peer1, 2)),
	ar_test_node:mine(peer1),
	[{H4, _, _} | _] = ar_test_node:wait_until_height(peer1, 3),
	H4TXIDs = (ar_test_node:remote_call(peer1, ar_test_node, read_block_when_stored, [H4]))#block.txs,
	?debugFmt("Expecting ~s to be re-mined.~n", [ar_util:encode(TXID)]),
	?assertEqual([TXID], H4TXIDs).

invalid_block_with_high_cumulative_difficulty_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun() -> test_invalid_block_with_high_cumulative_difficulty() end).

test_invalid_block_with_high_cumulative_difficulty() ->
	%% Submit an alternative fork with valid blocks weaker than the tip and
	%% an invalid block on top, much stronger than the tip. Make sure the node
	%% ignores the invalid block and continues to build on top of the valid fork.
	RewardKey = ar_wallet:new_keyfile(),
	RewardAddr = ar_wallet:to_address(RewardKey),
	WalletName = ar_util:encode(RewardAddr),
	Path = ar_wallet:wallet_filepath(WalletName),
	PeerPath = ar_test_node:remote_call(peer1, ar_wallet, wallet_filepath, [WalletName]),
	%% Copy the key because we mine blocks on both nodes using the same key in this test.
	{ok, _} = file:copy(Path, PeerPath),
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0, RewardAddr),
	ar_test_node:start_peer(peer1, B0, RewardAddr),
	ar_test_node:disconnect_from(peer1),
	ar_test_node:mine(peer1),
	[{H1, _, _} | _] = ar_test_node:wait_until_height(peer1, 1),
	ar_test_node:mine(),
	[{H2, _, _} | _] = wait_until_height(1),
	ar_test_node:connect_to_peer(peer1),
	?assertNotEqual(H2, H1),
	B1 = read_block_when_stored(H2),
	B2 = fake_block_with_strong_cumulative_difficulty(B1, B0, 10000000000000000),
	B2H = B2#block.indep_hash,
	?debugFmt("Fake block: ~s.", [ar_util:encode(B2H)]),
	ok = ar_events:subscribe(block),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
			ar_http_iface_client:send_block_binary(ar_test_node:peer_ip(main), B2#block.indep_hash,
					ar_serialize:block_to_binary(B2))),
	receive
		{event, block, {rejected, invalid_cumulative_difficulty, B2H, _Peer2}} ->
			ok;
		{event, block, {new, #block{ indep_hash = B2H }, _Peer3}} ->
			?assert(false, "Unexpected block acceptance")
	after 5000 ->
		?assert(false, "Timed out waiting for the node to pre-validate the fake "
				"block.")
	end,
	[{H1, _, _} | _] = ar_test_node:wait_until_height(peer1, 1),
	ar_test_node:mine(),
	%% Assert the nodes have continued building on the original fork.
	[{H3, _, _} | _] = ar_test_node:wait_until_height(peer1, 2),
	?assertNotEqual(B2#block.indep_hash, H3),
	{_Peer, B3, _Time, _Size} = ar_http_iface_client:get_block_shadow(1, ar_test_node:peer_ip(peer1), binary),
	?assertEqual(H2, B3#block.indep_hash).

fake_block_with_strong_cumulative_difficulty(B, PrevB, CDiff) ->
	#block{
		partition_number = PartitionNumber,
		previous_solution_hash = PrevSolutionH,
		nonce_limiter_info = #nonce_limiter_info{
				partition_upper_bound = PartitionUpperBound },
		diff = Diff
	} = B,
	B2 = B#block{ cumulative_diff = CDiff },
	Wallet = ar_wallet:new(),
	RewardAddr2 = ar_wallet:to_address(Wallet),
	H0 = ar_block:compute_h0(B, PrevB),
	{RecallByte, _RecallRange2Start} = ar_block:get_recall_range(H0, PartitionNumber,
			PartitionUpperBound),
	{ok, #{ data_path := DataPath, tx_path := TXPath,
			chunk := Chunk } } = ar_data_sync:get_chunk(RecallByte + 1,
					#{ pack => true, packing => {spora_2_6, RewardAddr2} }),
	{H1, Preimage} = ar_block:compute_h1(H0, 0, Chunk),
	case binary:decode_unsigned(H1) > Diff of
		true ->
			PoA = #poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath },
			B3 = B2#block{ hash = H1, hash_preimage = Preimage, reward_addr = RewardAddr2,
					reward_key = element(2, Wallet), recall_byte = RecallByte, nonce = 0,
					recall_byte2 = undefined, poa = #poa{ chunk = Chunk, data_path = DataPath,
							tx_path = TXPath },
					chunk_hash = crypto:hash(sha256, Chunk) },
			B4 =
				case ar_fork:height_2_8() of
					0 ->
						{ok, #{ chunk := UnpackedChunk } } = ar_data_sync:get_chunk(
								RecallByte + 1, #{ pack => true, packing => unpacked }),
						B3#block{ packing_difficulty = 1,
								poa = PoA#poa{ unpacked_chunk = UnpackedChunk },
								unpacked_chunk_hash = crypto:hash(sha256, UnpackedChunk) };
					_ ->
						B3
				end,
			PrevCDiff = PrevB#block.cumulative_diff,
			SignedH = ar_block:generate_signed_hash(B4),
			SignaturePreimage = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, PrevSolutionH/binary,
					SignedH/binary >>,
			Signature = ar_wallet:sign(element(1, Wallet), SignaturePreimage),
			B4#block{ indep_hash = ar_block:indep_hash2(SignedH, Signature),
					signature = Signature };
		false ->
			fake_block_with_strong_cumulative_difficulty(B, PrevB, CDiff)
	end.

	fork_recovery_test_() ->
		{timeout, 300, fun test_fork_recovery/0}.
	
	test_fork_recovery() ->
		test_fork_recovery(original_split).
	
	test_fork_recovery(Split) ->
		Wallet = ar_test_data_sync:setup_nodes(),
		{TX1, Chunks1} = ar_test_data_sync:tx(Wallet, {Split, 13}, v2, ?AR(10)),
		?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(TX1#tx.id)]),
		B1 = ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX1]),
		?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(B1#block.indep_hash),
				B1#block.height]),
		Proofs1 = ar_test_data_sync:post_proofs(main, B1, TX1, Chunks1),
		ar_test_data_sync:wait_until_syncs_chunks(Proofs1),
		UpperBound = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
		ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, UpperBound),
		ar_test_node:disconnect_from(peer1),
		{PeerTX2, PeerChunks2} = ar_test_data_sync:tx(Wallet, {Split, 15}, v2, ?AR(10)),
		{PeerTX3, PeerChunks3} = ar_test_data_sync:tx(Wallet, {Split, 17}, v2, ?AR(10)),
		?debugFmt("Posting tx to peer1 ~s.~n", [ar_util:encode(PeerTX2#tx.id)]),
		?debugFmt("Posting tx to peer1 ~s.~n", [ar_util:encode(PeerTX3#tx.id)]),
		PeerB2 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
				[PeerTX2, PeerTX3]),
		?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(PeerB2#block.indep_hash),
				PeerB2#block.height]),
		{MainTX2, MainChunks2} = ar_test_data_sync:tx(Wallet, {Split, 14}, v2, ?AR(10)),
		?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(MainTX2#tx.id)]),
		MainB2 = ar_test_node:post_and_mine(#{ miner => main, await_on => main },
				[MainTX2]),
		?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(MainB2#block.indep_hash),
				MainB2#block.height]),
		_PeerProofs2 = ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX2, PeerChunks2),
		_PeerProofs3 = ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX3, PeerChunks3),
		{PeerTX4, PeerChunks4} = ar_test_data_sync:tx(Wallet, {Split, 22}, v2, ?AR(10)),
		?debugFmt("Posting tx to peer1 ~s.~n", [ar_util:encode(PeerTX4#tx.id)]),
		PeerB3 = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 },
				[PeerTX4]),
		?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(PeerB3#block.indep_hash),
				PeerB3#block.height]),
		_PeerProofs4 = ar_test_data_sync:post_proofs(peer1, PeerB3, PeerTX4, PeerChunks4),
		ar_test_node:post_and_mine(#{ miner => main, await_on => main }, []),
		MainProofs2 = ar_test_data_sync:post_proofs(main, MainB2, MainTX2, MainChunks2),
		{MainTX3, MainChunks3} = ar_test_data_sync:tx(Wallet, {Split, 16}, v2, ?AR(10)),
		?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(MainTX3#tx.id)]),
		MainB3 = ar_test_node:post_and_mine(#{ miner => main, await_on => main },
				[MainTX3]),
		?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(MainB3#block.indep_hash),
				MainB3#block.height]),
		ar_test_node:connect_to_peer(peer1),
		MainProofs3 = ar_test_data_sync:post_proofs(main, MainB3, MainTX3, MainChunks3),
		UpperBound2 = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
		ar_test_data_sync:wait_until_syncs_chunks(peer1, MainProofs2, UpperBound2),
		ar_test_data_sync:wait_until_syncs_chunks(peer1, MainProofs3, UpperBound2),
		ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, infinity),
		%% The peer1 node will return the orphaned transactions to the mempool
		%% and gossip them.
		?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(PeerTX2#tx.id)]),
		?debugFmt("Posting tx to main ~s.~n", [ar_util:encode(PeerTX4#tx.id)]),
		ar_test_node:post_tx_to_peer(main, PeerTX2),
		ar_test_node:post_tx_to_peer(main, PeerTX4),
		ar_test_node:assert_wait_until_receives_txs([PeerTX2, PeerTX4]),
		MainB4 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, []),
		?debugFmt("Mined block ~s, height ~B.~n", [ar_util:encode(MainB4#block.indep_hash),
				MainB4#block.height]),
		Proofs4 = ar_test_data_sync:post_proofs(main, MainB4, PeerTX4, PeerChunks4),
		%% We did not submit proofs for PeerTX4 to main - they are supposed to be still stored
		%% in the disk pool.
		ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs4, infinity),
		UpperBound3 = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
		ar_test_data_sync:wait_until_syncs_chunks(Proofs4, UpperBound3),
		ar_test_data_sync:post_proofs(peer1, PeerB2, PeerTX2, PeerChunks2).
	
