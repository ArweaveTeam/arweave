-module(ar_fork_recovery_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
		start/1, slave_start/0, slave_start/1, start/2, slave_start/2, connect_to_slave/0,
		slave_peer/0, master_peer/0, disconnect_from_slave/0, assert_post_tx_to_slave/1,
		slave_mine/0, assert_slave_wait_until_height/1, wait_until_height/1,
		slave_wait_until_height/1, sign_tx/2, read_block_when_stored/1, slave_call/3]).

height_plus_one_fork_recovery_test_() ->
	{timeout, 120, fun test_height_plus_one_fork_recovery/0}.

test_height_plus_one_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine an extra block on one of them.
	%% Expect the other one to recover.
	{_SlaveNode, B0} = slave_start(),
	{_MasterNode, B0} = start(B0),
	disconnect_from_slave(),
	slave_mine(),
	assert_slave_wait_until_height(1),
	ar_node:mine(),
	wait_until_height(1),
	ar_node:mine(),
	MasterBI = wait_until_height(2),
	connect_to_slave(),
	?assertEqual(MasterBI, slave_wait_until_height(2)),
	disconnect_from_slave(),
	ar_node:mine(),
	wait_until_height(3),
	slave_mine(),
	assert_slave_wait_until_height(3),
	connect_to_slave(),
	slave_mine(),
	SlaveBI = slave_wait_until_height(4),
	?assertEqual(SlaveBI, wait_until_height(4)).

height_plus_three_fork_recovery_test_() ->
	{timeout, 120, fun test_height_plus_three_fork_recovery/0}.

test_height_plus_three_fork_recovery() ->
	%% Mine on two nodes until they fork. Mine three extra blocks on one of them.
	%% Expect the other one to recover.
	{_SlaveNode, B0} = slave_start(),
	{_MasterNode, B0} = start(B0),
	disconnect_from_slave(),
	slave_mine(),
	assert_slave_wait_until_height(1),
	ar_node:mine(),
	wait_until_height(1),
	ar_node:mine(),
	wait_until_height(2),
	slave_mine(),
	assert_slave_wait_until_height(2),
	ar_node:mine(),
	wait_until_height(3),
	slave_mine(),
	assert_slave_wait_until_height(3),
	connect_to_slave(),
	ar_node:mine(),
	MasterBI = wait_until_height(4),
	?assertEqual(MasterBI, slave_wait_until_height(4)).

missing_txs_fork_recovery_test_() ->
	{timeout, 120, fun test_missing_txs_fork_recovery/0}.

test_missing_txs_fork_recovery() ->
	%% Mine a block with a transaction on the slave node
	%% but do not gossip the transaction. The master node
	%% is expected fetch the missing transaction and apply the block.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{_SlaveNode, _} = slave_start(B0),
	{_MasterNode, _} = start(B0),
	disconnect_from_slave(),
	TX1 = sign_tx(Key, #{}),
	assert_post_tx_to_slave(TX1),
	%% Wait to make sure the tx will not be gossiped upon reconnect.
	timer:sleep(2000), % == 2 * ?CHECK_MEMPOOL_FREQUENCY
	connect_to_slave(),
	?assertEqual([], ar_node:get_pending_txs()),
	slave_mine(),
	[{H1, _, _} | _] = wait_until_height(1),
	?assertEqual(1, length((read_block_when_stored(H1))#block.txs)).

orphaned_txs_are_remined_after_fork_recovery_test_() ->
	{timeout, 120, fun test_orphaned_txs_are_remined_after_fork_recovery/0}.

test_orphaned_txs_are_remined_after_fork_recovery() ->
	%% Mine a transaction on slave, mine two blocks on master to
	%% make the transaction orphaned. Mine a block on slave and
	%% assert the transaction is re-mined.
	Key = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(20), <<>>}]),
	{_SlaveNode, _} = slave_start(B0),
	{_MasterNode, _} = start(B0),
	disconnect_from_slave(),
	TX = #tx{ id = TXID } = sign_tx(Key, #{}),
	assert_post_tx_to_slave(TX),
	slave_mine(),
	[{H1, _, _} | _] = slave_wait_until_height(1),
	H1TXIDs = (slave_call(ar_test_node, read_block_when_stored, [H1]))#block.txs,
	?assertEqual([TXID], H1TXIDs),
	ar_node:mine(),
	[{H2, _, _} | _] = wait_until_height(1),
	ar_node:mine(),
	[{H3, _, _}, {H2, _, _}, {_, _, _}] = wait_until_height(2),
	connect_to_slave(),
	?assertMatch([{H3, _, _}, {H2, _, _}, {_, _, _}], slave_wait_until_height(2)),
	slave_mine(),
	[{H4, _, _} | _] = slave_wait_until_height(3),
	H4TXIDs = (slave_call(ar_test_node, read_block_when_stored, [H4]))#block.txs,
	?assertEqual([TXID], H4TXIDs).

invalid_block_with_high_cumulative_difficulty_pre_fork_2_6_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> infinity end},
			{ar_fork, height_2_7, fun() -> infinity end}],
		fun() -> test_invalid_block_with_high_cumulative_difficulty(fork_2_5) end).

invalid_block_with_high_cumulative_difficulty_pre_fork_2_7_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> 0 end},
			{ar_fork, height_2_7, fun() -> infinity end}],
		fun() -> test_invalid_block_with_high_cumulative_difficulty(fork_2_6) end).

invalid_block_with_high_cumulative_difficulty_test_() ->
	ar_test_node:test_with_mocked_functions([
			{ar_fork, height_2_6, fun() -> 0 end},
			{ar_fork, height_2_7, fun() -> 0 end}],
		fun() -> test_invalid_block_with_high_cumulative_difficulty(fork_2_7) end).

test_invalid_block_with_high_cumulative_difficulty(Fork) ->
	%% Submit an alternative fork with valid blocks weaker than the tip and
	%% an invalid block on top, much stronger than the tip. Make sure the node
	%% ignores the invalid block and continues to build on top of the valid fork.
	RewardKey = ar_wallet:new_keyfile(),
	RewardAddr = ar_wallet:to_address(RewardKey),
	WalletName = ar_util:encode(RewardAddr),
	Path = ar_wallet:wallet_filepath(WalletName),
	SlavePath = slave_call(ar_wallet, wallet_filepath, [WalletName]),
	%% Copy the key because we mine blocks on both nodes using the same key in this test.
	{ok, _} = file:copy(Path, SlavePath),
	[B0] = ar_weave:init([]),
	{_SlaveNode, B0} = slave_start(B0, RewardAddr),
	{_MasterNode, B0} = start(B0, RewardAddr),
	disconnect_from_slave(),
	slave_mine(),
	[{H1, _, _} | _] = slave_wait_until_height(1),
	ar_node:mine(),
	[{H2, _, _} | _] = wait_until_height(1),
	connect_to_slave(),
	?assertNotEqual(H2, H1),
	B1 = read_block_when_stored(H2),
	B2 =
		case Fork of
			fork_2_7 ->
				fake_block_with_strong_cumulative_difficulty(B1, B0, 10000000000000000);
			fork_2_6 ->
				fake_pre_2_7_block_with_strong_cumulative_difficulty(B1, 10000000000000000);
			fork_2_5 ->
				fake_pre_2_6_block_with_strong_cumulative_difficulty(B1, 10000000000000000)
		end,
	B2H = B2#block.indep_hash,
	?debugFmt("Fake block: ~s.", [ar_util:encode(B2H)]),
	ok = ar_events:subscribe(block),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
			ar_http_iface_client:send_block_binary(master_peer(), B2#block.indep_hash,
					ar_serialize:block_to_binary(B2))),
	case Fork of
		fork_2_5 ->
			receive
				{event, block, {rejected, Reason, B2H, _Peer2}} ->
					?assert(false, iolist_to_binary(
							io_lib:format("Unexpected block rejection: ~p.", [Reason])));
				{event, block, {new, #block{ indep_hash = B2H }, _Peer3}} ->
					ok
			after 5000 ->
				?assert(false, "Timed out waiting for the node to pre-validate the fake "
						"block.")
			end;
		_ ->
			receive
				{event, block, {rejected, invalid_cumulative_difficulty, B2H, _Peer2}} ->
					ok;
				{event, block, {new, #block{ indep_hash = B2H }, _Peer3}} ->
					?assert(false, "Unexpected block acceptance")
			after 5000 ->
				?assert(false, "Timed out waiting for the node to pre-validate the fake "
						"block.")
			end
	end,
	[{H1, _, _} | _] = slave_wait_until_height(1),
	ar_node:mine(),
	%% Assert the nodes have continued building on the original fork.
	[{H3, _, _} | _] = slave_wait_until_height(2),
	?assertNotEqual(B2#block.indep_hash, H3),
	{_Peer, B3, _Time, _Size} = ar_http_iface_client:get_block_shadow(1, slave_peer(), binary),
	?assertEqual(H2, B3#block.indep_hash).

fake_pre_2_6_block_with_strong_cumulative_difficulty(B, CDiff) ->
	#block{
	    previous_block = PrevH,
	    height = Height,
	    timestamp = Timestamp,
	    poa = #poa{ chunk = Chunk },
		diff = Diff
	} = B,
	Nonce = crypto:strong_rand_bytes(32),
	B2 = B#block{ cumulative_diff = CDiff },
	BDS = ar_block:generate_block_data_segment(B2),
	{H0, Entropy} = ar_mine:spora_h0_with_entropy(BDS, Nonce, Height),
	{H, _Preimage} = ar_mine:spora_solution_hash_with_entropy(PrevH, Timestamp, H0, Chunk,
			Entropy, Height),
	case binary:decode_unsigned(H) > Diff of
		true ->
			B3 = B2#block{ hash = H, nonce = Nonce },
			B3#block{ indep_hash = ar_block:indep_hash(B3) };
		false ->
			fake_pre_2_6_block_with_strong_cumulative_difficulty(B, CDiff)
	end.

fake_pre_2_7_block_with_strong_cumulative_difficulty(B, CDiff) ->
	#block{
	    previous_block = PrevH,
	    height = Height,
	    timestamp = Timestamp,
		reward_addr = RewardAddr,
		diff = Diff
	} = B,
	Nonce = crypto:strong_rand_bytes(32),
	B2 = B#block{ cumulative_diff = CDiff },
	BDS = ar_block:generate_block_data_segment(B2),
	PartitionUpperBound =
			ar_node:get_partition_upper_bound(tl(ar_node:get_block_index())),
	{H0, Entropy} = ar_mine:spora_h0_with_entropy(BDS, Nonce, Height),
	{ok, RecallByte} = ar_mine:pick_recall_byte(H0, PrevH, PartitionUpperBound),
	{ok, #{ data_path := DataPath, tx_path := TXPath,
			chunk := Chunk } } = ar_data_sync:get_chunk(RecallByte + 1,
					#{ pack => true, packing => {spora_2_6, RewardAddr} }),
	{H, Preimage} = ar_mine:spora_solution_hash_with_entropy(PrevH, Timestamp, H0, Chunk,
			Entropy, Height),
	case binary:decode_unsigned(H) > Diff of
		true ->
			B3 = B2#block{ hash = H, hash_preimage = Preimage, nonce = Nonce,
					recall_byte = RecallByte,
					poa = #poa{ chunk = Chunk, data_path = DataPath, tx_path = TXPath } },
			B3#block{ indep_hash = ar_block:indep_hash(B3) };
		false ->
			fake_pre_2_7_block_with_strong_cumulative_difficulty(B, CDiff)
	end.

fake_block_with_strong_cumulative_difficulty(B, PrevB, CDiff) ->
	#block{
		partition_number = PartitionNumber,
		previous_solution_hash = PrevSolutionH,
		nonce_limiter_info = #nonce_limiter_info{ output = Output,
				partition_upper_bound = PartitionUpperBound },
		diff = Diff
	} = B,
	B2 = B#block{ cumulative_diff = CDiff },
	Wallet = ar_wallet:new(),
	RewardAddr2 = ar_wallet:to_address(Wallet),
	Seed = (PrevB#block.nonce_limiter_info)#nonce_limiter_info.seed,
	H0 = ar_block:compute_h0(Output, PartitionNumber, Seed, RewardAddr2),
	{RecallByte, _RecallRange2Start} = ar_block:get_recall_range(H0, PartitionNumber,
			PartitionUpperBound),
	{ok, #{ data_path := DataPath, tx_path := TXPath,
			chunk := Chunk } } = ar_data_sync:get_chunk(RecallByte + 1,
					#{ pack => true, packing => {spora_2_6, RewardAddr2} }),
	{H1, Preimage} = ar_block:compute_h1(H0, 0, Chunk),
	case binary:decode_unsigned(H1) > Diff of
		true ->
			B3 = B2#block{ hash = H1, hash_preimage = Preimage, reward_addr = RewardAddr2,
					reward_key = element(2, Wallet), recall_byte = RecallByte, nonce = 0,
					recall_byte2 = undefined, poa = #poa{ chunk = Chunk, data_path = DataPath,
							tx_path = TXPath } },
			SignedH = ar_block:generate_signed_hash(B3),
			Signature = ar_wallet:sign(element(1, Wallet),
					<< SignedH/binary, PrevSolutionH/binary >>),
			B3#block{ indep_hash = ar_block:indep_hash2(SignedH, Signature),
					signature = Signature };
		false ->
			fake_block_with_strong_cumulative_difficulty(B, PrevB, CDiff)
	end.
