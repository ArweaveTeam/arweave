-module(ar_test_fork).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-export([
	fork_recovery/1
]).

fork_recovery(Config) ->
	Wallet = ?config(wallet, Config),
	Slave = ?config(slave, Config),
	{TX1, Chunks1} = tx(Wallet, {custom_split, 3}),
	% post TXs and run mining process (async)
	BH1 = ar_test_lib:post_txs_mine([TX1]),
	% wait until this block mined on this node
	B1 = ar_test_lib:wait_block(BH1 + 1),
	% wait until this block received by the slave node
	ct_rpc:call(Slave, ar_test_lib, wait_block, [BH1 + 1]),
	% build proofs and post them to this node
	Proofs1 = build_proofs(B1, TX1, Chunks1),
	ok = ar_test_lib:post_chunks(Proofs1),
	% wait them on the slave
	ct_rpc:call(Slave, ar_test_lib, wait_chunks, [Proofs1]),
	% leave the slave (unregistering each other as a peer)
	ar_test_lib:leave(Slave),
	% create new TXs
	{SlaveTX2, SlaveChunks2} = tx(Wallet, {custom_split, 5}),
	{SlaveTX3, SlaveChunks3} = tx(Wallet, {custom_split, 3}),
	% post them to the slave node and run mining process there (async)
	SlaveBH2 = ct_rpc:call(Slave, ar_test_lib, post_txs_mine, [[SlaveTX2, SlaveTX3]]),
	% wait until this block be ready
	SlaveB2 = ct_rpc:call(Slave, ar_test_lib, wait_block, [SlaveBH2 + 1]),
	% join back to the slave
	ar_test_lib:join(Slave),
	% create new TXs for the master node
	{MasterTX2, MasterChunks2} = tx(Wallet, {custom_split, 4}),
	% post and mine them here
	MasterBH2 = ar_test_lib:post_txs_mine([MasterTX2]),
	MasterB2 = ar_test_lib:wait_block(MasterBH2 + 1),
	% leave the slave
	ar_test_lib:leave(Slave),
	% build proofs and post them to the slave
	SlaveProofs2 = build_proofs(SlaveB2, SlaveTX2, SlaveChunks2),
	SlaveProofs3 = build_proofs(SlaveB2, SlaveTX3, SlaveChunks3),
	ok = ct_rpc:call(Slave, ar_test_lib, post_chunks, [SlaveProofs2]),
	ok = ct_rpc:call(Slave, ar_test_lib, post_chunks, [SlaveProofs3]),
	% create new TXs
	{SlaveTX4, SlaveChunks4} = tx(Wallet, {custom_split, 2}),
	% post them to the slave
	SlaveBH3 = ct_rpc:call(Slave, ar_test_lib, post_txs_mine, [[SlaveTX4]]),
	% wait until this block be ready on a slave side
	SlaveB3 = ct_rpc:call(Slave, ar_test_lib, wait_block, [SlaveBH3 + 1]),
	% join back
	ar_test_lib:join(Slave),
	% just mine
	CurrentHeightMaster = ar_test_lib:post_txs_mine([]),
	ar_test_lib:wait_block(CurrentHeightMaster + 1),
	% build proofs for the master and post them here
	MasterProofs2 = build_proofs(MasterB2, MasterTX2, MasterChunks2),
	ok = ar_test_lib:post_chunks(MasterProofs2),
	% create yet another TXs, post them here and wait the next block here as well
	{MasterTX3, MasterChunks3} = tx(Wallet, {custom_split, 6}),
	MasterBH3 = ar_test_lib:post_txs_mine([MasterTX3]),
	MasterB3 = ar_test_lib:wait_block(MasterBH3 + 1),
	% build proof, post them here
	MasterProofs3 = build_proofs(MasterB3, MasterTX3, MasterChunks3),
	ok = ar_test_lib:post_chunks(MasterProofs3),
	% wait chunks on a slave side
	ok = ct_rpc:call(Slave, ar_test_lib, wait_chunks, [MasterProofs2]),
	ok = ct_rpc:call(Slave, ar_test_lib, wait_chunks, [MasterProofs3]),
	ok = ct_rpc:call(Slave, ar_test_lib, wait_chunks, [Proofs1]),
	MasterBH4 = ar_test_lib:post_txs_mine([SlaveTX2, SlaveTX4]),
	MasterB4 = ar_test_lib:wait_block(MasterBH4 + 1),

	Proofs4 = build_proofs(MasterB4, SlaveTX2, SlaveChunks2),
	%% We did not submit proofs for SlaveTX2 - they are supposed to be still stored
	%% in the disk pool.
	ok = ct_rpc:call(Slave, ar_test_lib, wait_chunks, [Proofs4]),
	ok = ar_test_lib:wait_chunks(Proofs4),
	SlaveProofs4 = build_proofs(SlaveB3, SlaveTX4, SlaveChunks4),
	ok = ct_rpc:call(Slave, ar_test_lib, post_chunks, [SlaveProofs4]).

tx(Wallet, SplitType) ->
	tx(Wallet, SplitType, v2).

tx(Wallet, SplitType, Format) ->
	case {SplitType, Format} of
		{{fixed_data, Data, DataRoot, Chunks}, _} ->
			{ar_test_lib:sign_tx(Wallet, #{
				data_size => byte_size(Data),
				data_root => DataRoot,
				last_tx => ar_test_lib:get_tx_anchor()
			}), Chunks};
		{original_split, v1} ->
			Data = crypto:strong_rand_bytes(10 * 1024),
			{_, Chunks} = original_split(Data),
			TXParams = #{ data => Data, last_tx => ar_test_lib:get_tx_anchor() },
			{ar_test_lib:sign_tx_v1(Wallet, TXParams), Chunks};
		{{custom_split, ChunkNumber}, v2} ->
			Chunks = lists:foldl(
				fun(_, Chunks) ->
					OneThird = ?DATA_CHUNK_SIZE div 3,
					RandomSize = OneThird + rand:uniform(?DATA_CHUNK_SIZE - OneThird) - 1,
					Chunk = crypto:strong_rand_bytes(RandomSize),
					[Chunk | Chunks]
				end,
				[],
				lists:seq(
					1,
					case ChunkNumber of random -> rand:uniform(5); _ -> ChunkNumber end
				)
			),
			SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
				ar_tx:chunks_to_size_tagged_chunks(Chunks)
			),
			{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
			TXParams = #{
				data_size => byte_size(binary:list_to_bin(Chunks)),
				last_tx => ar_test_lib:get_tx_anchor(),
				data_root => DataRoot
			},
			TX = ar_test_lib:sign_tx(Wallet, TXParams),
			{TX, Chunks};
		{original_split, v2} ->
			Data = crypto:strong_rand_bytes(11 * 1024),
			{DataRoot, Chunks} = original_split(Data),
			TXParams = #{
				data_size => byte_size(Data),
				last_tx => ar_test_lib:get_tx_anchor(master),
				data_root => DataRoot,
				quantity => 1
			},
			TX = ar_test_lib:sign_tx(Wallet, TXParams),
			{TX, Chunks}
	end.

original_split(Data) ->
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, Data),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
		ar_tx:chunks_to_size_tagged_chunks(Chunks)
	),
	{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
	{DataRoot, Chunks}.

build_proofs(B, TX, Chunks) ->
	build_proofs(TX, Chunks, B#block.txs, B#block.weave_size - B#block.block_size).

build_proofs(TX, Chunks, TXs, BlockStartOffset) ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{value, {_, TXOffset}} =
		lists:search(fun({{TXID, _}, _}) -> TXID == TX#tx.id end, SizeTaggedTXs),
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	TXPath = ar_merkle:generate_path(TXRoot, TXOffset - 1, TXTree),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks)
	),
	DataSize = byte_size(binary:list_to_bin(Chunks)),
	lists:foldl(
		fun
			({<<>>, _}, Proofs) ->
				Proofs;
			({Chunk, ChunkOffset}, Proofs) ->
				TXStartOffset = TXOffset - DataSize,
				AbsoluteChunkEndOffset = BlockStartOffset + TXStartOffset + ChunkOffset,
				Proof = #{
					tx_path => ar_util:encode(TXPath),
					data_root => ar_util:encode(DataRoot),
					data_path =>
						ar_util:encode(
							ar_merkle:generate_path(DataRoot, ChunkOffset - 1, DataTree)
						),
					chunk => ar_util:encode(Chunk),
					offset => integer_to_binary(ChunkOffset - 1),
					data_size => integer_to_binary(DataSize)
				},
				Proofs ++ [{AbsoluteChunkEndOffset, Proof}]
		end,
		[],
		SizeTaggedChunks
	).
