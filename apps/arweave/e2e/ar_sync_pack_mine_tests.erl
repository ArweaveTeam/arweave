-module(ar_sync_pack_mine_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Set to true to update the chunk fixtures.
%% WARNING: ONLY SET TO true IF YOU KNOW WHAT YOU ARE DOING!
-define(UPDATE_CHUNK_FIXTURES, false).

%% --------------------------------------------------------------------------------------------
%% Fixtures
%% --------------------------------------------------------------------------------------------
setup_source_node(unpacked) ->
	SourceNode = peer1,
	TempNode = peer2,
	ar_test_node:stop(TempNode),
	{Blocks, _SourceAddr, Chunks} = start_source_node(TempNode, composite_1),
	{_, StorageModules} = source_node_storage_modules(SourceNode, unpacked),
	[B0 | _] = Blocks,
	{ok, Config} = ar_test_node:get_config(SourceNode),
	ar_test_node:start_other_node(SourceNode, B0, Config#config{
		peers = [ar_test_node:peer_ip(TempNode)],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true
	}, true),
	assert_syncs_range(SourceNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	assert_chunks(SourceNode, unpacked, Chunks),
	ar_test_node:stop(TempNode),
	{Blocks, Chunks, unpacked};

setup_source_node(PackingType) ->
	SourceNode = peer1,
	SinkNode = peer2,
	ar_test_node:stop(SinkNode),
	{Blocks, SourceAddr, Chunks} = start_source_node(SourceNode, PackingType),
	SourcePacking = packing_type_to_packing(PackingType, SourceAddr),
	assert_syncs_range(SourceNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	assert_chunks(SourceNode, SourcePacking, Chunks),

	{Blocks, Chunks, PackingType}.

instantiator(GenesisData, SinkPackingType, TestFun) ->
	{timeout, 300, {with, {GenesisData, SinkPackingType}, [TestFun]}}.
	
%% --------------------------------------------------------------------------------------------
%% Test Registration
%% --------------------------------------------------------------------------------------------
spora_2_6_sync_pack_mine_test_() ->
	{setup, fun () -> setup_source_node(spora_2_6) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_1, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_2, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

composite_1_sync_pack_mine_test_() ->
	{setup, fun () -> setup_source_node(composite_1) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_1, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_2, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

composite_2_sync_pack_mine_test_() ->
	{setup, fun () -> setup_source_node(composite_2) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_1, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_2, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

unpacked_sync_pack_mine_test_() ->
	{setup, fun () -> setup_source_node(unpacked) end, 
		fun (GenesisData) ->
				[
					instantiator(GenesisData, spora_2_6, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_1, fun test_sync_pack_mine/1),
					instantiator(GenesisData, composite_2, fun test_sync_pack_mine/1),
					instantiator(GenesisData, unpacked, fun test_sync_pack_mine/1)
				]
		end}.

%% --------------------------------------------------------------------------------------------
%% test_sync_pack_mine
%% --------------------------------------------------------------------------------------------
test_sync_pack_mine({{Blocks, Chunks, SourcePackingType}, SinkPackingType}) ->
	%% Print the specific flavor of this test since it isn't captured in the test name.
	%% Delay the print by 1 second to allow the eunit output to be flushed.
	spawn(fun() ->
		timer:sleep(1000),
		io:fwrite(user, <<" ~p -> ~p ">>, [SourcePackingType, SinkPackingType])
	end),
	[B0 | _] = Blocks,
	SourceNode = peer1,
	SinkNode = peer2,

	SinkPacking = start_sink_node(SinkNode, SourceNode, B0, SinkPackingType),
	assert_syncs_range(SinkNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	assert_chunks(SinkNode, SinkPacking, Chunks),

	case SinkPackingType of
		unpacked ->
			ok;
		_ ->
			CurrentHeight = ar_test_node:remote_call(SinkNode, ar_node, get_height, []),
			ar_test_node:mine(SinkNode),

			SinkBI = ar_test_node:wait_until_height(SinkNode, CurrentHeight + 1),
			{ok, SinkBlock} = ar_test_node:http_get_block(element(1, hd(SinkBI)), SinkNode),
			assert_block(SinkPacking, SinkBlock),

			SourceBI = ar_test_node:wait_until_height(SourceNode, SinkBlock#block.height),
			{ok, SourceBlock} = ar_test_node:http_get_block(element(1, hd(SourceBI)), SourceNode),
			?assertEqual(SinkBlock, SourceBlock),
			ok
	end.


assert_chunks(Node, Packing, Chunks) ->
	lists:foreach(fun({Block, EndOffset, ChunkSize}) ->
		assert_chunk(Node, Packing, Block, EndOffset, ChunkSize)
	end, Chunks).

assert_chunk(Node, Packing, Block, EndOffset, ChunkSize) ->
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

	maybe_write_chunk_fixture(Packing, EndOffset, Chunk),

	{ok, ExpectedPackedChunk} = ar_e2e:load_chunk_fixture(Packing, EndOffset),
	?assertEqual(ExpectedPackedChunk, Chunk,
		iolist_to_binary(io_lib:format(
			"Chunk at offset ~p, size ~p does not match previously packed chunk",
			[EndOffset, ChunkSize]))),

	{ok, UnpackedChunk} = ar_packing_server:unpack(
		Packing, EndOffset, Block#block.tx_root, Chunk, ?DATA_CHUNK_SIZE),
	UnpaddedChunk = ar_packing_server:unpad_chunk(Packing, UnpackedChunk, ChunkSize, byte_size(Chunk)),
	ExpectedUnpackedChunk = ar_test_node:get_genesis_chunk(EndOffset),
	?assertEqual(ExpectedUnpackedChunk, UnpaddedChunk,
		iolist_to_binary(io_lib:format(
			"Chunk at offset ~p, size ~p does not match unpacked chunk",
			[EndOffset, ChunkSize]))).

source_node_storage_modules(_Node, unpacked) ->
	{undefined, source_node_storage_modules(unpacked)};
source_node_storage_modules(Node, PackingType) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_a]),
	RewardAddr = ar_wallet:to_address(Wallet),
	SourcePacking = packing_type_to_packing(PackingType, RewardAddr),
	{Wallet, source_node_storage_modules(SourcePacking)}.

source_node_storage_modules(SourcePacking) ->
	[
		{?PARTITION_SIZE, 0, SourcePacking},
		{?PARTITION_SIZE, 1, SourcePacking},
		{?PARTITION_SIZE, 2, SourcePacking},
		{?PARTITION_SIZE, 3, SourcePacking},
		{?PARTITION_SIZE, 4, SourcePacking},
		{?PARTITION_SIZE, 5, SourcePacking},
		{?PARTITION_SIZE, 6, SourcePacking}
	].

start_source_node(_Node, unpacked) ->
	?assert(false, "Source nodes cannot be unpacked - they need to mine.");
start_source_node(Node, PackingType) ->
	{Wallet, StorageModules} = source_node_storage_modules(Node, PackingType),
	RewardAddr = ar_wallet:to_address(Wallet),
	[B0] = ar_weave:init([{RewardAddr, ?AR(200), <<>>}], 0, ?PARTITION_SIZE),

	{ok, Config} = ar_test_node:remote_call(Node, application, get_env, [arweave, config]),
	
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

	%% List of {Block, EndOffset, ChunkSize}
	Chunks = [
		{B1, ?PARTITION_SIZE + ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE},
		{B1, ?PARTITION_SIZE + (2*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		{B1, ?PARTITION_SIZE + floor(2.5 * ?DATA_CHUNK_SIZE), floor(0.5 * ?DATA_CHUNK_SIZE)},
		{B2, ?PARTITION_SIZE + floor(3.75 * ?DATA_CHUNK_SIZE), floor(0.75 * ?DATA_CHUNK_SIZE)},
		{B3, ?PARTITION_SIZE + (5*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		{B3, ?PARTITION_SIZE + (6*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		{B3, ?PARTITION_SIZE + (7*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		{B3, ?PARTITION_SIZE + (8*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE}
	],
	{[B0, B1, B2, B3, B4, B5], RewardAddr, Chunks}.

start_sink_node(Node, SourceNode, B0, PackingType) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = packing_type_to_packing(PackingType, SinkAddr),
	{ok, Config} = ar_test_node:get_config(Node),
	
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


%% --------------------------------------------------------------------------------------------
%% Test Data Generation
%% --------------------------------------------------------------------------------------------	
write_wallet_fixtures() ->
	Wallets = [wallet_a, wallet_b, wallet_c, wallet_d],
	lists:foreach(fun(Wallet) ->
		WalletName = atom_to_list(Wallet),
		ar_wallet:new_keyfile(?DEFAULT_KEY_TYPE, WalletName),
		ar_e2e:install_fixture(
			ar_wallet:wallet_filepath(Wallet), wallets, WalletName ++ ".json")
	end, Wallets),
	ok.

maybe_write_chunk_fixture(Packing, EndOffset, Chunk) when ?UPDATE_CHUNK_FIXTURES =:= true ->
	ar_e2e:write_chunk_fixture(Packing, EndOffset, Chunk);
maybe_write_chunk_fixture(_, _, _) ->
	ok.
