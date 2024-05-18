-module(ar_reject_chunks_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-import(ar_test_node, [sign_v1_tx/2, wait_until_height/1, assert_wait_until_height/2,
		read_block_when_stored/1, test_with_mocked_functions/2]).

rejects_invalid_chunks_test_() ->
	{timeout, 180, fun test_rejects_invalid_chunks/0}.

test_rejects_invalid_chunks() ->
	ar_test_data_sync:setup_nodes(),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"chunk_too_big\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(#{
			chunk => ar_util:encode(crypto:strong_rand_bytes(?DATA_CHUNK_SIZE + 1)),
			data_path => <<>>,
			offset => <<"0">>,
			data_size => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_path_too_big\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(#{
			data_path => ar_util:encode(crypto:strong_rand_bytes(?MAX_PATH_SIZE + 1)),
			chunk => <<>>,
			offset => <<"0">>,
			data_size => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"offset_too_big\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(#{
			offset => integer_to_binary(trunc(math:pow(2, 256))),
			data_path => <<>>,
			chunk => <<>>,
			data_size => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_size_too_big\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(#{
			data_size => integer_to_binary(trunc(math:pow(2, 256))),
			data_path => <<>>,
			chunk => <<>>,
			offset => <<"0">>
		}))
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"chunk_proof_ratio_not_attractive\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(#{
			chunk => ar_util:encode(<<"a">>),
			data_path => ar_util:encode(<<"bb">>),
			offset => <<"0">>,
			data_size => <<"0">>
		}))
	),
	ar_test_data_sync:setup_nodes(),
	Chunk = crypto:strong_rand_bytes(500),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
		ar_tx:chunks_to_size_tagged_chunks([Chunk])
	),
	{DataRoot, DataTree} = ar_merkle:generate_tree(SizedChunkIDs),
	DataPath = ar_merkle:generate_path(DataRoot, 0, DataTree),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"data_root_not_found\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(#{
			data_root => ar_util:encode(DataRoot),
			chunk => ar_util:encode(Chunk),
			data_path => ar_util:encode(DataPath),
			offset => <<"0">>,
			data_size => <<"500">>
		}))
	),
	?assertMatch(
		{ok, {{<<"413">>, _}, _, <<"Payload too large">>, _, _}},
		ar_test_node:post_chunk(main, << <<0>> || _ <- lists:seq(1, ?MAX_SERIALIZED_CHUNK_PROOF_SIZE + 1) >>)
	).

does_not_store_small_chunks_after_2_5_test_() ->
	{timeout, 600, fun test_does_not_store_small_chunks_after_2_5/0}.

test_does_not_store_small_chunks_after_2_5() ->
	Size = ?DATA_CHUNK_SIZE,
	Third = Size div 3,
	Splits = [
		{"Even split", Size * 3, Size, Size, Size, Size, Size * 2, Size * 3,
				lists:seq(0, Size - 1, 2048), lists:seq(Size, Size * 2 - 1, 2048),
				lists:seq(Size * 2, Size * 3 + 2048, 2048),
				[{O, first} || O <- lists:seq(1, Size - 1, 2048)]
						++ [{O, second} || O <- lists:seq(Size + 1, 2 * Size, 2048)]
						++ [{O, third} || O <- lists:seq(2 * Size + 1, 3 * Size, 2048)]
						++ [{3 * Size + 1, 404}, {4 * Size, 404}]},
		{"Small last chunk", 2 * Size + Third, Size, Size, Third, Size, 2 * Size,
				2 * Size + Third, lists:seq(3, Size - 1, 2048),
				lists:seq(Size, 2 * Size - 1, 2048),
				lists:seq(2 * Size, 2 * Size + Third, 2048),
				%% The chunk is expected to be returned by any offset of the 256 KiB
				%% bucket where it ends.
				[{O, first} || O <- lists:seq(1, Size - 1, 2048)]
						++ [{O, second} || O <- lists:seq(Size + 1, 2 * Size, 2048)]
						++ [{O, third} || O <- lists:seq(2 * Size + 1, 3 * Size, 2048)]
						++ [{3 * Size + 1, 404}, {4 * Size, 404}]},
		{"Small chunks crossing the bucket", 2 * Size + Third, Size, Third + 1, Size - 1,
				Size, Size + Third + 1, 2 * Size + Third, lists:seq(0, Size - 1, 2048),
				lists:seq(Size, Size + Third, 1024),
				lists:seq(Size + Third + 1, 2 * Size + Third, 2048),
				[{O, first} || O <- lists:seq(1, Size, 2048)]
						++ [{O, second} || O <- lists:seq(Size + 1, 2 * Size, 2048)]
						++ [{O, third} || O <- lists:seq(2 * Size + 1, 3 * Size, 2048)]
						++ [{3 * Size + 1, 404}, {4 * Size, 404}]},
		{"Small chunks in one bucket", 2 * Size, Size, Third, Size - Third, Size, Size + Third,
				2 * Size, lists:seq(0, Size - 1, 2048), lists:seq(Size, Size + Third - 1, 2048),
				lists:seq(Size + Third, 2 * Size, 2048),
				[{O, first} || O <- lists:seq(1, Size - 1, 2048)]
						%% The second and third chunks must not be accepted.
						%% The second chunk does not precede a chunk crossing a bucket border,
						%% the third chunk ends at a multiple of 256 KiB.
						++ [{O, 404} || O <- lists:seq(Size + 1, 2 * Size + 4096, 2048)]},
		{"Small chunk preceding 256 KiB chunk", 2 * Size + Third, Third, Size, Size, Third,
				Size + Third, 2 * Size + Third, lists:seq(0, Third - 1, 2048),
				lists:seq(Third, Third + Size - 1, 2048),
				lists:seq(Third + Size, Third + 2 * Size, 2048),
				%% The first chunk must not be accepted, the first bucket stays empty.
				[{O, 404} || O <- lists:seq(1, Size - 1, 2048)]
						%% The other chunks are rejected too - their start offsets
						%% are not aligned with the buckets.
						++ [{O, 404} || O <- lists:seq(Size + 1, 4 * Size, 2048)]}],
	lists:foreach(
		fun({Title, DataSize, FirstSize, SecondSize, ThirdSize, FirstMerkleOffset,
				SecondMerkleOffset, ThirdMerkleOffset, FirstPublishOffsets, SecondPublishOffsets,
				ThirdPublishOffsets, Expectations}) ->
			?debugFmt("Running [~s]", [Title]),
			Wallet = ar_test_data_sync:setup_nodes(),
			{FirstChunk, SecondChunk, ThirdChunk} = {crypto:strong_rand_bytes(FirstSize),
					crypto:strong_rand_bytes(SecondSize), crypto:strong_rand_bytes(ThirdSize)},
			{FirstChunkID, SecondChunkID, ThirdChunkID} = {ar_tx:generate_chunk_id(FirstChunk),
					ar_tx:generate_chunk_id(SecondChunk), ar_tx:generate_chunk_id(ThirdChunk)},
			{DataRoot, DataTree} = ar_merkle:generate_tree([{FirstChunkID, FirstMerkleOffset},
					{SecondChunkID, SecondMerkleOffset}, {ThirdChunkID, ThirdMerkleOffset}]),
			TX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(main), data_size => DataSize,
					data_root => DataRoot }),
			ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
			lists:foreach(
				fun({Chunk, Offset}) ->
					DataPath = ar_merkle:generate_path(DataRoot, Offset, DataTree),
					Proof = #{ data_root => ar_util:encode(DataRoot),
							data_path => ar_util:encode(DataPath),
							chunk => ar_util:encode(Chunk),
							offset => integer_to_binary(Offset),
							data_size => integer_to_binary(DataSize) },
					%% All chunks are accepted because we do not know their offsets yet -
					%% in theory they may end up below the strict data split threshold.
					?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
							ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof)), Title)
				end,
				[{FirstChunk, O} || O <- FirstPublishOffsets]
						++ [{SecondChunk, O} || O <- SecondPublishOffsets]
						++ [{ThirdChunk, O} || O <- ThirdPublishOffsets]
			),
			%% In practice the chunks are above the strict data split threshold so those
			%% which do not pass strict validation will not be stored.
			timer:sleep(2000),
			GenesisOffset = ?STRICT_DATA_SPLIT_THRESHOLD,
			lists:foreach(
				fun	({Offset, 404}) ->
						?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}},
								ar_test_node:get_chunk(main, GenesisOffset + Offset), Title);
					({Offset, first}) ->
						{ok, {{<<"200">>, _}, _, ProofJSON, _, _}} = ar_test_node:get_chunk(main, 
								GenesisOffset + Offset),
						?assertEqual(FirstChunk, ar_util:decode(maps:get(<<"chunk">>,
								jiffy:decode(ProofJSON, [return_maps]))), Title);
					({Offset, second}) ->
						{ok, {{<<"200">>, _}, _, ProofJSON, _, _}} = ar_test_node:get_chunk(main, 
								GenesisOffset + Offset),
						?assertEqual(SecondChunk, ar_util:decode(maps:get(<<"chunk">>,
								jiffy:decode(ProofJSON, [return_maps]))), Title);
					({Offset, third}) ->
						{ok, {{<<"200">>, _}, _, ProofJSON, _, _}} = ar_test_node:get_chunk(main, 
								GenesisOffset + Offset),
						?assertEqual(ThirdChunk, ar_util:decode(maps:get(<<"chunk">>,
								jiffy:decode(ProofJSON, [return_maps]))), Title)
				end,
				Expectations
			)
		end,
		Splits
	).

rejects_chunks_with_merkle_tree_borders_exceeding_max_chunk_size_test_() ->
	{timeout, 120,
			fun test_rejects_chunks_with_merkle_tree_borders_exceeding_max_chunk_size/0}.

test_rejects_chunks_with_merkle_tree_borders_exceeding_max_chunk_size() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	BigOutOfBoundsOffsetChunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	BigChunkID = ar_tx:generate_chunk_id(BigOutOfBoundsOffsetChunk),
	{BigDataRoot, BigDataTree} = ar_merkle:generate_tree([{BigChunkID, ?DATA_CHUNK_SIZE + 1}]),
	BigTX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(main), data_size => ?DATA_CHUNK_SIZE,
			data_root => BigDataRoot }),
	ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [BigTX]),
	BigDataPath = ar_merkle:generate_path(BigDataRoot, 0, BigDataTree),
	BigProof = #{ data_root => ar_util:encode(BigDataRoot),
			data_path => ar_util:encode(BigDataPath),
			chunk => ar_util:encode(BigOutOfBoundsOffsetChunk), offset => <<"0">>,
			data_size => integer_to_binary(?DATA_CHUNK_SIZE)},
	?assertMatch({ok, {{<<"400">>, _}, _, <<"{\"error\":\"invalid_proof\"}">>, _, _}},
			ar_test_node:post_chunk(main, ar_serialize:jsonify(BigProof))).

rejects_chunks_exceeding_disk_pool_limit_test_() ->
	{timeout, 240, fun test_rejects_chunks_exceeding_disk_pool_limit/0}.

test_rejects_chunks_exceeding_disk_pool_limit() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Data1 = crypto:strong_rand_bytes(
		(?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB * 1024 * 1024) + 1
	),
	Chunks1 = ar_test_data_sync:imperfect_split(Data1),
	{DataRoot1, _} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks1)
		)
	),
	{TX1, Chunks1} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot1, Chunks1}),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	[{_, FirstProof1} | Proofs1] = ar_test_data_sync:build_proofs(TX1, Chunks1, [TX1], 0, 0),
	lists:foreach(
		fun({_, Proof}) ->
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		Proofs1
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"exceeds_disk_pool_size_limit\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(FirstProof1))
	),
	Data2 = crypto:strong_rand_bytes(
		min(
			?DEFAULT_MAX_DISK_POOL_BUFFER_MB - ?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB,
			?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB - 1
		) * 1024 * 1024
	),
	Chunks2 = ar_test_data_sync:imperfect_split(Data2),
	{DataRoot2, _} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks2)
		)
	),
	{TX2, Chunks2} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot2, Chunks2}),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	Proofs2 = ar_test_data_sync:build_proofs(TX2, Chunks2, [TX2], 0, 0),
	lists:foreach(
		fun({_, Proof}) ->
			%% The very last chunk will be dropped later because it starts and ends
			%% in the bucket of the previous chunk (the chunk sizes are 131072).
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		Proofs2
	),
	Left =
		?DEFAULT_MAX_DISK_POOL_BUFFER_MB * 1024 * 1024 -
		lists:sum([byte_size(Chunk) || Chunk <- tl(Chunks1)]) -
		byte_size(Data2),
	?assert(Left < ?DEFAULT_MAX_DISK_POOL_DATA_ROOT_BUFFER_MB * 1024 * 1024),
	Data3 = crypto:strong_rand_bytes(Left + 1),
	Chunks3 = ar_test_data_sync:imperfect_split(Data3),
	{DataRoot3, _} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks3)
		)
	),
	{TX3, Chunks3} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot3, Chunks3}),
	ar_test_node:assert_post_tx_to_peer(main, TX3),
	[{_, FirstProof3} | Proofs3] = ar_test_data_sync:build_proofs(TX3, Chunks3, [TX3], 0, 0),
	lists:foreach(
		fun({_, Proof}) ->
			%% The very last chunk will be dropped later because it starts and ends
			%% in the bucket of the previous chunk (the chunk sizes are 131072).
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		Proofs3
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"exceeds_disk_pool_size_limit\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(FirstProof3))
	),
	ar_test_node:mine(peer1),
	true = ar_util:do_until(
		fun() ->
			%% After a block is mined, the chunks receive their absolute offsets, which
			%% end up above the strict data split threshold and so the node discovers
			%% the very last chunks of the last two transactions are invalid under these
			%% offsets and frees up 131072 + 131072 bytes in the disk pool => we can submit
			%% a 262144-byte chunk. Also, expect 303 instead of 200 because the last block
			%% was large such that the configured partitions do not cover at least two
			%% times as much space ahead of the current weave size.
			case ar_test_node:post_chunk(main, ar_serialize:jsonify(FirstProof3)) of
				{ok, {{<<"303">>, _}, _, _, _, _}} ->
					true;
				_ ->
					false
			end
		end,
		2000,
		20 * 1000
	),
	%% Now we do not have free space again.
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"exceeds_disk_pool_size_limit\"}">>, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(FirstProof1))
	),
	%% Mine two more blocks to make the chunks mature so that we can remove them from the
	%% disk pool (they will stay in the corresponding storage modules though, if any).
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 2),
	ar_test_node:mine(peer1),
	true = ar_util:do_until(
		fun() ->
			case ar_test_node:post_chunk(main, ar_serialize:jsonify(FirstProof1)) of
				{ok, {{<<"200">>, _}, _, _, _, _}} ->
					true;
				_ ->
					false
			end
		end,
		2000,
		20 * 1000
	).

accepts_chunks_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_5, fun() -> 0 end}],
		fun test_accepts_chunks/0, 120).

test_accepts_chunks() ->
	test_accepts_chunks(original_split).

test_accepts_chunks(Split) ->
	Wallet = ar_test_data_sync:setup_nodes(),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {Split, 3}),
	ar_test_node:assert_post_tx_to_peer(peer1, TX),
	ar_test_node:assert_wait_until_receives_txs([TX]),
	[{Offset, FirstProof}, {_, SecondProof}, {_, ThirdProof}] = 
			ar_test_data_sync:build_proofs(TX, Chunks, [TX], 0, 0),
	EndOffset = Offset + ?STRICT_DATA_SPLIT_THRESHOLD,
	%% Post the third proof to the disk pool.
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(ThirdProof))
	),
	ar_test_node:mine(peer1),
	[{BH, _, _} | _] = wait_until_height(1),
	B = read_block_when_stored(BH),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_chunk(main, EndOffset)
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(FirstProof))
	),
	%% Expect the chunk to be retrieved by any offset within
	%% (EndOffset - ChunkSize, EndOffset], but not outside of it.
	FirstChunk = ar_util:decode(maps:get(chunk, FirstProof)),
	FirstChunkSize = byte_size(FirstChunk),
	ExpectedProof = #{
		data_path => maps:get(data_path, FirstProof),
		tx_path => maps:get(tx_path, FirstProof),
		chunk => ar_util:encode(FirstChunk)
	},
	ar_test_data_sync:wait_until_syncs_chunk(EndOffset, ExpectedProof),
	ar_test_data_sync:wait_until_syncs_chunk(
		EndOffset - rand:uniform(FirstChunkSize - 2), ExpectedProof),
	ar_test_data_sync:wait_until_syncs_chunk(EndOffset - FirstChunkSize + 1, ExpectedProof),
	?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}}, ar_test_node:get_chunk(main, 0)),
	?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}}, ar_test_node:get_chunk(main, EndOffset + 1)),
	TXSize = byte_size(binary:list_to_bin(Chunks)),
	ExpectedOffsetInfo = ar_serialize:jsonify(#{
		offset => integer_to_binary(TXSize + ?STRICT_DATA_SPLIT_THRESHOLD),
		size => integer_to_binary(TXSize)
	}),
	?assertMatch({ok, {{<<"200">>, _}, _, ExpectedOffsetInfo, _, _}},
		ar_test_data_sync:get_tx_offset(main, TX#tx.id)),
	%% Expect no transaction data because the second chunk is not synced yet.
	?assertMatch({ok, {{<<"404">>, _}, _, _Binary, _, _}},
		ar_test_data_sync:get_tx_data(TX#tx.id)),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
			ar_test_node:post_chunk(main, ar_serialize:jsonify(SecondProof))),
	ExpectedSecondProof = #{
		data_path => maps:get(data_path, SecondProof),
		tx_path => maps:get(tx_path, SecondProof),
		chunk => maps:get(chunk, SecondProof)
	},
	SecondChunk = ar_util:decode(maps:get(chunk, SecondProof)),
	SecondChunkOffset = ?STRICT_DATA_SPLIT_THRESHOLD + FirstChunkSize + byte_size(SecondChunk),
	ar_test_data_sync:wait_until_syncs_chunk(SecondChunkOffset, ExpectedSecondProof),
	true = ar_util:do_until(
		fun() ->
			{ok, {{<<"200">>, _}, _, Data, _, _}} = ar_test_data_sync:get_tx_data(TX#tx.id),
			ar_util:encode(binary:list_to_bin(Chunks)) == Data
		end,
		500,
		10 * 1000
	),
	ExpectedThirdProof = #{
		data_path => maps:get(data_path, ThirdProof),
		tx_path => maps:get(tx_path, ThirdProof),
		chunk => maps:get(chunk, ThirdProof)
	},
	ar_test_data_sync:wait_until_syncs_chunk(B#block.weave_size, ExpectedThirdProof),
	?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}}, ar_test_node:get_chunk(main, B#block.weave_size + 1)).
