-module(ar_pack_mine_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

%% ------------------------------------------------------------------------------------------------
%% Fixtures
%% ------------------------------------------------------------------------------------------------
setup_all() ->
	ok.

cleanup_all(_) ->
	ok.

setup_one() ->
	ok.

cleanup_one(_) ->
	ok.


%% ------------------------------------------------------------------------------------------------
%% Test Registration
%% ------------------------------------------------------------------------------------------------
pack_mine_test_() ->
	{setup, fun setup_all/0, fun cleanup_all/1,
		{foreach, fun setup_one/0, fun cleanup_one/1,
		[
			{timeout, 300, fun test_pack_mine/0}
		]}
    }.

%% ------------------------------------------------------------------------------------------------
%% pack_mine_test_
%% ------------------------------------------------------------------------------------------------
test_pack_mine() ->
	SourceNode = peer1,
	SinkNode = peer2,
	{Blocks, SourcePacking} = start_source_node(SourceNode, spora_2_6),
	[B0, B1, B2 | _] = Blocks,
	assert_syncs_range(SourceNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	
	FullChunkOffset = ?PARTITION_SIZE + ?DATA_CHUNK_SIZE,
	PartialChunkOffset = ?PARTITION_SIZE + floor(3.75 * ?DATA_CHUNK_SIZE),
	PartialChunkSize = floor(0.75 * ?DATA_CHUNK_SIZE),

	assert_chunk(SourceNode, B1, FullChunkOffset, SourcePacking, ?DATA_CHUNK_SIZE),
	assert_chunk(SourceNode, B2, PartialChunkOffset, SourcePacking, PartialChunkSize),

	SinkPacking = start_sink_node(SinkNode, SourceNode, B0, spora_2_6),
	assert_syncs_range(SinkNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),

	assert_chunk(SinkNode, B1, FullChunkOffset, SinkPacking, ?DATA_CHUNK_SIZE),
	assert_chunk(SinkNode, B2, PartialChunkOffset, SinkPacking, PartialChunkSize),

	CurrentHeight = ar_test_node:remote_call(SinkNode, ar_node, get_height, []),
	ar_test_node:mine(SinkNode),

	SinkBI = ar_test_node:wait_until_height(SinkNode, CurrentHeight + 1),
	{ok, SinkBlock} = ar_test_node:http_get_block(element(1, hd(SinkBI)), SinkNode),
	assert_block(SinkPacking, SinkBlock),

	SourceBI = ar_test_node:wait_until_height(SourceNode, SinkBlock#block.height),
	{ok, SourceBlock} = ar_test_node:http_get_block(element(1, hd(SourceBI)), SourceNode),
	?assertEqual(SinkBlock, SourceBlock),
	ok.

assert_chunk(Node, Block, EndOffset, Packing, ChunkSize) ->

	{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} = 
		ar_test_node:get_chunk(Node, EndOffset, any),
	Proof = ar_serialize:json_map_to_poa_map(
		jiffy:decode(EncodedProof, [return_maps])
	),
	{true, _} = ar_test_node:remote_call(Node, ar_poa, validate_paths, [
		Block#block.tx_root,
		maps:get(tx_path, Proof),
		maps:get(data_path, Proof),
		EndOffset - 1
	]),
	Chunk = maps:get(chunk, Proof),

	{ok, UnpackedChunk} = ar_packing_server:unpack(
		Packing, EndOffset, Block#block.tx_root, Chunk, ?DATA_CHUNK_SIZE),
	UnpaddedChunk = ar_packing_server:unpad_chunk(Packing, UnpackedChunk, ChunkSize, byte_size(Chunk)),
	ExpectedChunk = ar_test_node:get_genesis_chunk(EndOffset),
	?assertEqual(ExpectedChunk, UnpaddedChunk,
		iolist_to_binary(io_lib:format(
			"Chunk at offset ~p, size ~p does not match unpacked chunk",
			[EndOffset, ChunkSize]))).

start_source_node(Node, PackingType) ->
	Wallet = ar_test_node:remote_call(Node, ar_wallet, new_keyfile, []),
	RewardAddr = ar_wallet:to_address(Wallet),

	SourcePacking = packing_type_to_packing(PackingType, RewardAddr),

	[B0] = ar_weave:init([{RewardAddr, ?AR(200), <<>>}], 0, ?PARTITION_SIZE),

	{ok, Config} = ar_test_node:remote_call(Node, application, get_env, [arweave, config]),
	
	StorageModules = [
		{?PARTITION_SIZE, 0, SourcePacking},
		{?PARTITION_SIZE, 1, SourcePacking},
		{?PARTITION_SIZE, 2, SourcePacking},
		{?PARTITION_SIZE, 3, SourcePacking},
		{?PARTITION_SIZE, 4, SourcePacking},
		{?PARTITION_SIZE, 5, SourcePacking},
		{?PARTITION_SIZE, 6, SourcePacking}
	],
	?assertEqual(ar_test_node:peer_name(Node),
		ar_test_node:start_other_node(Node, B0, Config#config{
			start_from_latest_state = true,
			storage_modules = StorageModules,
			auto_join = true,
			mining_addr = RewardAddr
		}, true)
	),

	%% Note: small chunks will be padded to 256 KiB. So B1 actually contains 3 chunks of data
	%% and B2 starts at a chunk boundary and contains 1 chunk of data.
	B1 = mine_block(Node, Wallet, floor(2.5 * ?DATA_CHUNK_SIZE)),
	B2 = mine_block(Node, Wallet, floor(0.75 * ?DATA_CHUNK_SIZE)),
	B3 = mine_block(Node, Wallet, ?PARTITION_SIZE),
	B4 = mine_block(Node, Wallet, ?PARTITION_SIZE),
	B5 = mine_block(Node, Wallet, ?PARTITION_SIZE),

	{[B0, B1, B2, B3, B4, B5], SourcePacking}.

start_sink_node(Node, SourceNode, B0, PackingType) ->
	Wallet = ar_test_node:remote_call(Node, ar_wallet, new_keyfile, []),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = packing_type_to_packing(PackingType, SinkAddr),
	{ok, Config} = ar_test_node:remote_call(Node, application, get_env, [arweave, config]),
	
	StorageModules = [
		{?PARTITION_SIZE, 1, SinkPacking}
	],
	?assertEqual(ar_test_node:peer_name(Node),
		ar_test_node:start_other_node(Node, B0, Config#config{
			peers = [ar_test_node:peer_ip(SourceNode)],
			start_from_latest_state = true,
			storage_modules = StorageModules,
			auto_join = true,
			mining_addr = SinkAddr
		}, true)
	),

	SinkPacking.


mine_block(Node, Wallet, DataSize) ->
	WeaveSize = ar_test_node:remote_call(Node, ar_node, get_current_weave_size, []),
	?debugFmt("Weave size: ~p, data size: ~p", [WeaveSize, DataSize]),
	{TX, Chunks} = generate_tx(Node, Wallet, WeaveSize, DataSize),
	B = ar_test_node:post_and_mine(#{ miner => Node, await_on => Node }, [TX]),
	Proofs = ar_test_data_sync:post_proofs(Node, B, TX, Chunks),
	
	ar_test_data_sync:wait_until_syncs_chunks(Node, Proofs, infinity),
	B.

generate_tx(Node, Wallet, WeaveSize, DataSize) ->
	Chunks = generate_chunks(Node, WeaveSize, DataSize, []),
	{DataRoot, _DataTree} = ar_merkle:generate_tree(
		[{ar_tx:generate_chunk_id(Chunk), Offset} || {Chunk, Offset} <- Chunks]
	),
	TX = ar_test_node:sign_tx(Node, Wallet, #{
		data_size => DataSize,
		data_root => DataRoot
	}),
	{TX, [Chunk || {Chunk, _} <- Chunks]}.

generate_chunks(Node, WeaveSize, DataSize, Acc) when DataSize > 0 ->
	ChunkSize = min(DataSize, ?DATA_CHUNK_SIZE),
	EndOffset = (length(Acc) * ?DATA_CHUNK_SIZE) + ChunkSize,
	Chunk = ar_test_node:get_genesis_chunk(WeaveSize + EndOffset),
	generate_chunks(Node, WeaveSize, DataSize - ChunkSize, Acc ++ [{Chunk, EndOffset}]);
generate_chunks(_, _, _, Acc) ->
	Acc.

packing_type_to_packing(PackingType, Address) ->
	case PackingType of
		spora_2_6 -> {spora_2_6, Address};
		composite_1 -> {composite, Address, 1};
		composite_2 -> {composite, Address, 2};
		unpacked -> unpacked
	end.

assert_syncs_range(Node, StartOffset, EndOffset) ->
	?assert(
		ar_util:do_until(
			fun() -> has_range(Node, StartOffset, EndOffset) end,
			100,
			60 * 1000
		),
		iolist_to_binary(io_lib:format(
			"~s Failed to sync range ~p - ~p", [Node, StartOffset, EndOffset]))).


has_range(Node, StartOffset, EndOffset) ->
	NodeIP = ar_test_node:peer_ip(Node),
	case ar_http_iface_client:get_sync_record(NodeIP) of
		{ok, SyncRecord} ->
			interval_contains(SyncRecord, StartOffset, EndOffset);
		Error ->
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"Failed to get sync record from ~p: ~p", [Node, Error]))),
			false
	end.

interval_contains(Intervals, Start, End) when End > Start ->
    case gb_sets:iterator_from({Start, Start}, Intervals) of
        Iter ->
            interval_contains2(Iter, Start, End)
    end.

interval_contains2(Iter, Start, End) ->
    case gb_sets:next(Iter) of
        none ->
            false;
        {{IntervalEnd, IntervalStart}, _} when IntervalStart =< Start andalso IntervalEnd >= End ->
            true;
        _ ->
            false
    end.

assert_block({spora_2_6, Address}, MinedBlock) ->
	?assertEqual(Address, MinedBlock#block.reward_addr),
	?assertEqual(0, MinedBlock#block.packing_difficulty);
assert_block({composite, Address, PackingDifficulty}, MinedBlock) ->
	?assertEqual(Address, MinedBlock#block.reward_addr),
	?assertEqual(PackingDifficulty, MinedBlock#block.packing_difficulty).