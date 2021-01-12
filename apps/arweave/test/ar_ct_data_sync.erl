-module(ar_ct_data_sync).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-export([
	fork_recovery/1
]).

-import(ar_test_lib, [
	sign_tx/3, sign_tx_v1/3,
	get_tx_anchor/1,
	post_txs_and_mine/2,
	wait_for_block/2,
	post_chunks/2,
	wait_for_chunks/2,
	start_peering/2,
	stop_peering/2,
	mine/1
]).

%%%===================================================================
%%% Tests.
%%%===================================================================

fork_recovery(Config) ->
	Wallet = ?config(wallet, Config),
	Master = node(),
	Slave = ?config(slave, Config),
	{TX1, Chunks1} = tx(Master, Wallet, {custom_split, 3}),
	Height1 = post_txs_and_mine(Master, [TX1]),
	SlaveB1 = wait_for_block(Slave, Height1 + 1),
	Proofs1 = build_proofs(SlaveB1, TX1, Chunks1),
	%% Post chunks to master and expect them to be synced by slave.
	ok = post_chunks(Master, Proofs1),
	wait_for_chunks(Slave, Proofs1),
	stop_peering(Master, Slave),
	{SlaveTX2, SlaveChunks2} = tx(Slave, Wallet, {custom_split, 5}),
	{SlaveTX3, SlaveChunks3} = tx(Slave, Wallet, {custom_split, 3}),
	Height2 = post_txs_and_mine(Slave, [SlaveTX2, SlaveTX3]),
	SlaveB2 = wait_for_block(Slave, Height2 + 1),
	ar_test_lib:start_peering(Master, Slave),
	{MasterTX2, MasterChunks2} = tx(Master, Wallet, {custom_split, 4}),
	Height3 = post_txs_and_mine(Master, [MasterTX2]),
	MasterB2 = wait_for_block(Master, Height3 + 1),
	stop_peering(Master, Slave),
	SlaveProofs2 = build_proofs(SlaveB2, SlaveTX2, SlaveChunks2),
	SlaveProofs3 = build_proofs(SlaveB2, SlaveTX3, SlaveChunks3),
	ok = post_chunks(Slave, SlaveProofs2),
	ok = post_chunks(Slave, SlaveProofs3),
	{SlaveTX4, SlaveChunks4} = tx(Slave, Wallet, {custom_split, 2}),
	Height4 = post_txs_and_mine(Slave, [SlaveTX4]),
	SlaveB3 = wait_for_block(Slave, Height4 + 1),
	start_peering(Master, Slave),
	Height5 = mine(Master),
	wait_for_block(Master, Height5 + 1),
	MasterProofs2 = build_proofs(MasterB2, MasterTX2, MasterChunks2),
	ok = post_chunks(Master, MasterProofs2),
	{MasterTX3, MasterChunks3} = tx(Master, Wallet, {custom_split, 6}),
	Height6 = post_txs_and_mine(Master, [MasterTX3]),
	MasterB3 = wait_for_block(Master, Height6 + 1),
	MasterProofs3 = build_proofs(MasterB3, MasterTX3, MasterChunks3),
	ok = post_chunks(Master, MasterProofs3),
	wait_for_chunks(Slave, MasterProofs2),
	wait_for_chunks(Slave, MasterProofs3),
	wait_for_chunks(Slave, Proofs1),
	Height7 = post_txs_and_mine(Master, [SlaveTX2, SlaveTX4]),
	MasterB4 = wait_for_block(Master, Height7 + 1),
	Proofs4 = build_proofs(MasterB4, SlaveTX2, SlaveChunks2),
	%% We did not submit proofs for SlaveTX2 - they are supposed to be still stored
	%% in the disk pool.
	wait_for_chunks(Slave, Proofs4),
	wait_for_chunks(Master, Proofs4),
	ok = post_chunks(Slave, build_proofs(SlaveB3, SlaveTX4, SlaveChunks4)).

%%%===================================================================
%%% Private functions.
%%%===================================================================

tx(Node, Wallet, SplitType) ->
	tx(Node, Wallet, SplitType, v2).

tx(Node, Wallet, SplitType, Format) ->
	case {SplitType, Format} of
		{{fixed_data, Data, DataRoot, Chunks}, _} ->
			{sign_tx(Node, Wallet, #{
				data_size => byte_size(Data),
				data_root => DataRoot,
				last_tx => get_tx_anchor(Node)
			}), Chunks};
		{original_split, v1} ->
			Data = crypto:strong_rand_bytes(10 * 1024),
			{_, Chunks} = original_split(Data),
			TXParams = #{ data => Data, last_tx => get_tx_anchor(Node) },
			{sign_tx_v1(Node, Wallet, TXParams), Chunks};
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
				last_tx => ar_test_lib:get_tx_anchor(Node),
				data_root => DataRoot
			},
			TX = sign_tx(Node, Wallet, TXParams),
			{TX, Chunks};
		{original_split, v2} ->
			Data = crypto:strong_rand_bytes(11 * 1024),
			{DataRoot, Chunks} = original_split(Data),
			TXParams = #{
				data_size => byte_size(Data),
				last_tx => get_tx_anchor(Node),
				data_root => DataRoot,
				quantity => 1
			},
			TX = sign_tx(Node, Wallet, TXParams),
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
