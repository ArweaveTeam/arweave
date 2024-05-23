-module(ar_data_sync_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-import(ar_test_node, [sign_v1_tx/2, wait_until_height/1, assert_wait_until_height/2,
		read_block_when_stored/1, test_with_mocked_functions/2]).

syncs_data_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> 0 end}],
		fun test_syncs_data/0, 240).

test_syncs_data() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Records = ar_test_data_sync:post_random_blocks(Wallet),
	RecordsWithProofs = lists:flatmap(
			fun({B, TX, Chunks}) -> 
				ar_test_data_sync:get_records_with_proofs(B, TX, Chunks) end, Records),
	lists:foreach(
		fun({_, _, _, {_, Proof}}) ->
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
					ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))),
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
					ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof)))
		end,
		RecordsWithProofs
	),
	Proofs = [Proof || {_, _, _, Proof} <- RecordsWithProofs],
	ar_test_data_sync:wait_until_syncs_chunks(Proofs),
	DiskPoolThreshold = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs, DiskPoolThreshold),
	lists:foreach(
		fun({B, #tx{ id = TXID }, Chunks, {_, Proof}}) ->
			TXSize = byte_size(binary:list_to_bin(Chunks)),
			TXOffset = ar_merkle:extract_note(ar_util:decode(maps:get(tx_path, Proof))),
			AbsoluteTXOffset = B#block.weave_size - B#block.block_size + TXOffset,
			ExpectedOffsetInfo = ar_serialize:jsonify(#{
					offset => integer_to_binary(AbsoluteTXOffset),
					size => integer_to_binary(TXSize) }),
			true = ar_util:do_until(
				fun() ->
					case ar_test_data_sync:get_tx_offset(peer1, TXID) of
						{ok, {{<<"200">>, _}, _, ExpectedOffsetInfo, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				100,
				120 * 1000
			),
			ExpectedData = ar_util:encode(binary:list_to_bin(Chunks)),
			ar_test_node:assert_get_tx_data(main, TXID, ExpectedData),
			case AbsoluteTXOffset > DiskPoolThreshold of
				true ->
					ok;
				false ->
					ar_test_node:assert_get_tx_data(peer1, TXID, ExpectedData)
			end
		end,
		RecordsWithProofs
	).

syncs_after_joining_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> 0 end}],
		fun test_syncs_after_joining/0, 240).

test_syncs_after_joining() ->
	test_syncs_after_joining(original_split).

test_syncs_after_joining(Split) ->
	Wallet = ar_test_data_sync:setup_nodes(),
	{TX1, Chunks1} = ar_test_data_sync:tx(Wallet, {Split, 17}, v2, ?AR(1)),
	B1 = ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX1]),
	Proofs1 = ar_test_data_sync:post_proofs(main, B1, TX1, Chunks1),
	UpperBound = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs1, UpperBound),
	ar_test_data_sync:wait_until_syncs_chunks(Proofs1),
	ar_test_node:disconnect_from(peer1),
	{MainTX2, MainChunks2} = ar_test_data_sync:tx(Wallet, {Split, 13}, v2, ?AR(1)),
	MainB2 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [MainTX2]),
	MainProofs2 = ar_test_data_sync:post_proofs(main, MainB2, MainTX2, MainChunks2),
	{MainTX3, MainChunks3} = ar_test_data_sync:tx(Wallet, {Split, 12}, v2, ?AR(1)),
	MainB3 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [MainTX3]),
	MainProofs3 = ar_test_data_sync:post_proofs(main, MainB3, MainTX3, MainChunks3),
	{PeerTX2, PeerChunks2} = ar_test_data_sync:tx(Wallet, {Split, 20}, v2, ?AR(1)),
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

mines_off_only_last_chunks_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
			fun test_mines_off_only_last_chunks/0).

test_mines_off_only_last_chunks() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	%% Submit only the last chunks (smaller than 256 KiB) of transactions.
	%% Assert the nodes construct correct proofs of access from them.
	lists:foreach(
		fun(Height) ->
			RandomID = crypto:strong_rand_bytes(32),
			Chunk = crypto:strong_rand_bytes(1023),
			ChunkID = ar_tx:generate_chunk_id(Chunk),
			DataSize = ?DATA_CHUNK_SIZE + 1023,
			{DataRoot, DataTree} = ar_merkle:generate_tree([{RandomID, ?DATA_CHUNK_SIZE},
					{ChunkID, DataSize}]),
			TX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(main), data_size => DataSize,
					data_root => DataRoot }),
			ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX]),
			Offset = ?DATA_CHUNK_SIZE + 1,
			DataPath = ar_merkle:generate_path(DataRoot, Offset, DataTree),
			Proof = #{ data_root => ar_util:encode(DataRoot),
					data_path => ar_util:encode(DataPath), chunk => ar_util:encode(Chunk),
					offset => integer_to_binary(Offset),
					data_size => integer_to_binary(DataSize) },
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
					ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))),
			case Height - ?SEARCH_SPACE_UPPER_BOUND_DEPTH of
				-1 ->
					%% Make sure we waited enough to have the next block use
					%% the new entropy reset source.
					[{_, Info}] = ets:lookup(node_state, nonce_limiter_info),
					PrevStepNumber = Info#nonce_limiter_info.global_step_number,
					true = ar_util:do_until(
						fun() ->
							ar_nonce_limiter:get_current_step_number()
									> PrevStepNumber + ?NONCE_LIMITER_RESET_FREQUENCY
						end,
						200,
						20000
					);
				0 ->
					%% Wait until the new chunks fall below the new upper bound and
					%% remove the original big chunks. The protocol will increase the upper
					%% bound based on the nonce limiter entropy reset, but ar_data_sync waits
					%% for ?SEARCH_SPACE_UPPER_BOUND_DEPTH confirmations before packing the
					%% chunks.
					{ok, Config} = application:get_env(arweave, config),
					lists:foreach(
						fun(O) ->
							[ar_chunk_storage:delete(O, ar_storage_module:id(Module))
									|| Module <- Config#config.storage_modules]
						end,
						lists:seq(?DATA_CHUNK_SIZE, ?STRICT_DATA_SPLIT_THRESHOLD,
								?DATA_CHUNK_SIZE)
					);
				_ ->
					ok
			end
		end,
		lists:seq(1, 6)
	).

mines_off_only_second_last_chunks_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
			fun test_mines_off_only_second_last_chunks/0).

test_mines_off_only_second_last_chunks() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	%% Submit only the second last chunks (smaller than 256 KiB) of transactions.
	%% Assert the nodes construct correct proofs of access from them.
	lists:foreach(
		fun(Height) ->
			RandomID = crypto:strong_rand_bytes(32),
			Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE div 2),
			ChunkID = ar_tx:generate_chunk_id(Chunk),
			DataSize = (?DATA_CHUNK_SIZE) div 2 + (?DATA_CHUNK_SIZE) div 2 + 3,
			{DataRoot, DataTree} = ar_merkle:generate_tree([{ChunkID, ?DATA_CHUNK_SIZE div 2},
					{RandomID, DataSize}]),
			TX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(main), data_size => DataSize,
					data_root => DataRoot }),
			ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX]),
			Offset = 0,
			DataPath = ar_merkle:generate_path(DataRoot, Offset, DataTree),
			Proof = #{ data_root => ar_util:encode(DataRoot),
					data_path => ar_util:encode(DataPath), chunk => ar_util:encode(Chunk),
					offset => integer_to_binary(Offset),
					data_size => integer_to_binary(DataSize) },
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
					ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))),
			case Height - ?SEARCH_SPACE_UPPER_BOUND_DEPTH >= 0 of
				true ->
					%% Wait until the new chunks fall below the new upper bound and
					%% remove the original big chunks. The protocol will increase the upper
					%% bound based on the nonce limiter entropy reset, but ar_data_sync waits
					%% for ?SEARCH_SPACE_UPPER_BOUND_DEPTH confirmations before packing the
					%% chunks.
					{ok, Config} = application:get_env(arweave, config),
					lists:foreach(
						fun(O) ->
							[ar_chunk_storage:delete(O, ar_storage_module:id(Module))
									|| Module <- Config#config.storage_modules]
						end,
						lists:seq(?DATA_CHUNK_SIZE, ?STRICT_DATA_SPLIT_THRESHOLD,
								?DATA_CHUNK_SIZE)
					);
				_ ->
					ok
			end
		end,
		lists:seq(1, 6)
	).

enqueue_intervals_test() ->
	test_enqueue_intervals([], 2, [], [], [], "Empty Intervals"),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {101, 102, 103, 104, 1984},
	Peer3 = {201, 202, 203, 204, 1984},

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
				])}
		],
		5,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1},
			{7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer1},
			{8*?DATA_CHUNK_SIZE, 9*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, all chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
				])}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, 2 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			])},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
			])},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			])}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer2},
			{7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer3}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			])},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
			])},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			])}
		],
		3,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1},
			{7*?DATA_CHUNK_SIZE, 8*?DATA_CHUNK_SIZE, Peer3}
		],
		"Multiple peers, overlapping, full intervals, 3 chunks. Non-overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
					{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
					{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			])}
		],
		5,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}, {9*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{7*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer1}
		],
		"Single peer, full intervals, all chunks. Overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			])},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
			])},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			])}
		],
		2,
		[{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE}, {9*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{7*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE, Peer1},
			{5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Peer2},
			{6*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE, Peer2}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Overlapping QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, trunc(5.75*?DATA_CHUNK_SIZE)}
			])}
		],
		2,
		[
			{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE},
			{trunc(8.5*?DATA_CHUNK_SIZE), trunc(6.5*?DATA_CHUNK_SIZE)}
		],
		[
			{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, trunc(3.25*?DATA_CHUNK_SIZE), Peer1}
		],
		"Single peer, partial intervals, 2 chunks. Overlapping partial QIntervals."),

	test_enqueue_intervals(
		[
			{Peer1, ar_intervals:from_list([
				{trunc(3.25*?DATA_CHUNK_SIZE), 2*?DATA_CHUNK_SIZE},
				{9*?DATA_CHUNK_SIZE, trunc(5.75*?DATA_CHUNK_SIZE)}
			])},
			{Peer2, ar_intervals:from_list([
				{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
				{7*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
			])},
			{Peer3, ar_intervals:from_list([
				{8*?DATA_CHUNK_SIZE, 7*?DATA_CHUNK_SIZE}
			])}
		],
		2,
		[
			{20*?DATA_CHUNK_SIZE, 10*?DATA_CHUNK_SIZE},
			{trunc(8.5*?DATA_CHUNK_SIZE), trunc(6.5*?DATA_CHUNK_SIZE)}
		],
		[
			{4*?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE},
			{8*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE}
		],
		[
			{2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE, Peer1},
			{3*?DATA_CHUNK_SIZE, trunc(3.25*?DATA_CHUNK_SIZE), Peer1},
			{trunc(3.25*?DATA_CHUNK_SIZE), 4*?DATA_CHUNK_SIZE, Peer2},
			{6*?DATA_CHUNK_SIZE, trunc(6.5*?DATA_CHUNK_SIZE), Peer2}
		],
		"Multiple peers, overlapping, full intervals, 2 chunks. Overlapping QIntervals.").

test_enqueue_intervals(Intervals, ChunksPerPeer, QIntervalsRanges, ExpectedQIntervalRanges, ExpectedChunks, Label) ->
	QIntervals = ar_intervals:from_list(QIntervalsRanges),
	Q = gb_sets:new(),
	{QResult, QIntervalsResult} = ar_data_sync:enqueue_intervals(Intervals, ChunksPerPeer, {Q, QIntervals}),
	ExpectedQIntervals = lists:foldl(fun({End, Start}, Acc) ->
			ar_intervals:add(Acc, End, Start)
		end, QIntervals, ExpectedQIntervalRanges),
	?assertEqual(ar_intervals:to_list(ExpectedQIntervals), ar_intervals:to_list(QIntervalsResult), Label),
	?assertEqual(ExpectedChunks, gb_sets:to_list(QResult), Label).

