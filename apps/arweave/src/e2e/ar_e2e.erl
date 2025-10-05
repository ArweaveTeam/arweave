-module(ar_e2e).

-export([fixture_dir/1, fixture_dir/2, install_fixture/3, load_wallet_fixture/1,
	write_chunk_fixture/3, load_chunk_fixture/2]).

-export([delayed_print/2, packing_type_to_packing/2,
	start_source_node/3, start_source_node/4, 
	source_node_storage_modules/3, source_node_storage_modules/4,
	max_chunk_offset/1,
	assert_recall_byte/3,
	assert_block/2, assert_syncs_range/3, assert_syncs_range/4, assert_does_not_sync_range/3,
	assert_has_entropy/4, assert_no_entropy/4,
	assert_chunks/3, assert_chunks/4, assert_no_chunks/2,
	assert_partition_size/3, assert_partition_size/4, assert_empty_partition/3,
	assert_mine_and_validate/3]).

-include_lib("ar.hrl").
-include_lib("ar_consensus.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

%% Set to true to update the chunk fixtures.
%% WARNING: ONLY SET TO true IF YOU KNOW WHAT YOU ARE DOING!
-define(UPDATE_CHUNK_FIXTURES, false).

-spec fixture_dir(atom()) -> binary().
fixture_dir(FixtureType) ->
	Dir = filename:dirname(?FILE),
	filename:join([Dir, "fixtures", atom_to_list(FixtureType)]).

-spec fixture_dir(atom(), [binary()]) -> binary().
fixture_dir(FixtureType, SubDirs) ->
	FixtureDir = fixture_dir(FixtureType),
	filename:join([FixtureDir] ++ SubDirs).

-spec install_fixture(binary(), atom(), string()) -> binary().
install_fixture(FilePath, FixtureType, FixtureName) ->
	FixtureDir = fixture_dir(FixtureType),
	ok = filelib:ensure_dir(FixtureDir ++ "/"),
	FixturePath = filename:join([FixtureDir, FixtureName]),
	file:copy(FilePath, FixturePath),
	FixturePath.

-spec load_wallet_fixture(atom()) -> tuple().
load_wallet_fixture(WalletFixture) ->
	WalletName = atom_to_list(WalletFixture),
	FixtureDir = fixture_dir(wallets),
	FixturePath = filename:join([FixtureDir, WalletName ++ ".json"]),
	Wallet = ar_wallet:load_keyfile(FixturePath),
	Address = ar_wallet:to_address(Wallet),
	WalletPath = ar_wallet:wallet_filepath(ar_util:encode(Address)),
	file:copy(FixturePath, WalletPath),
	ar_wallet:load_keyfile(WalletPath).

-spec write_chunk_fixture(binary(), non_neg_integer(), binary()) -> ok.
write_chunk_fixture(Packing, EndOffset, Chunk) ->
	FixtureDir = fixture_dir(chunks, [ar_serialize:encode_packing(Packing, true)]),
	ok = filelib:ensure_dir(FixtureDir ++ "/"),
	FixturePath = filename:join([FixtureDir, integer_to_list(EndOffset) ++ ".bin"]),
	file:write_file(FixturePath, Chunk).

-spec load_chunk_fixture(binary(), non_neg_integer()) -> binary().
load_chunk_fixture(Packing, EndOffset) ->
	FixtureDir = fixture_dir(chunks, [ar_serialize:encode_packing(Packing, true)]),
	FixturePath = filename:join([FixtureDir, integer_to_list(EndOffset) ++ ".bin"]),
	file:read_file(FixturePath).

packing_type_to_packing(PackingType, Address) ->
	case PackingType of
		replica_2_9 -> {replica_2_9, Address};
		spora_2_6 -> {spora_2_6, Address};
		composite_1 -> {composite, Address, 1};
		composite_2 -> {composite, Address, 2};
		unpacked -> unpacked
	end.

start_source_node(Node, PackingType, WalletFixture) ->
	start_source_node(Node, PackingType, WalletFixture, default).

start_source_node(Node, unpacked, _WalletFixture, ModuleSize) ->
	?LOG_INFO("Starting source node ~p with packing type ~p and wallet fixture ~p",
		[Node, unpacked, _WalletFixture]),
	TempNode = case Node of
		peer1 -> peer2;
		peer2 -> peer1
	end,
	{Blocks, _SourceAddr, Chunks} = start_source_node(TempNode, spora_2_6, wallet_a),
	{_, StorageModules} = source_node_storage_modules(Node, unpacked, wallet_a, ModuleSize),
	[B0, _, {TX2, _} | _] = Blocks,
	{ok, Config} = ar_test_node:get_config(Node),
	ar_test_node:start_other_node(Node, B0, Config#config{
		peers = [ar_test_node:peer_ip(TempNode)],
		storage_modules = StorageModules,
		auto_join = true
	}, true),

	?LOG_INFO("Source node ~p started.", [Node]),
	
	assert_syncs_range(Node, 0, 4*ar_block:partition_size()),
	
	assert_chunks(Node, unpacked, Chunks),

	?LOG_INFO("Source node ~p assertions passed.", [Node]),

	ar_test_node:stop(TempNode),

	ar_test_node:restart_with_config(Node, Config#config{
		peers = [],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true
	}),

	%% pack_served_chunks is not enabled but the data is stored unpacked, so we should
	%% return it
	{ok, {{<<"200">>, _}, _, Data, _, _}} =
		ar_http:req(#{
			method => get,
			peer => ar_test_node:peer_ip(Node),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX2#tx.id)) ++ "/data"
		}),
	{ok, ExpectedData} = load_chunk_fixture(
		unpacked, ar_block:partition_size() + floor(3.75 * ?DATA_CHUNK_SIZE)),
	?assertEqual(ExpectedData, ar_util:decode(Data)),

	?LOG_INFO("Source node ~p restarted.", [Node]),

	{Blocks, undefined, Chunks};
start_source_node(Node, PackingType, WalletFixture, ModuleSize) ->
	?LOG_INFO("Starting source node ~p with packing type ~p and wallet fixture ~p",
		[Node, PackingType, WalletFixture]),
	{Wallet, StorageModules} = source_node_storage_modules(
		Node, PackingType, WalletFixture, ModuleSize),
	RewardAddr = ar_wallet:to_address(Wallet),
	[B0] = ar_weave:init([{RewardAddr, ?AR(200), <<>>}], 0, ar_block:partition_size()),

	{ok, Config} = ar_test_node:remote_call(Node, arweave_config, get_env, []),
	
	?assertEqual(ar_test_node:peer_name(Node),
		ar_test_node:start_other_node(Node, B0, Config#config{
			peers = [],
			start_from_latest_state = true,
			storage_modules = StorageModules,
			auto_join = true,
			mining_addr = RewardAddr
		}, true)
	),

	?LOG_INFO("Source node ~p started.", [Node]),

	%% Note: small chunks will be padded to 256 KiB. So B1 actually contains 3 chunks of data
	%% and B2 starts at a chunk boundary and contains 1 chunk of data.
	{TX1, B1} = mine_block(Node, Wallet, floor(2.5 * ?DATA_CHUNK_SIZE), false), %% p1
	{TX2, B2} = mine_block(Node, Wallet, floor(0.75 * ?DATA_CHUNK_SIZE), false), %% p1
	{TX3, B3} = mine_block(Node, Wallet, ar_block:partition_size(), false), %% p1 to p2
	{TX4, B4} = mine_block(Node, Wallet, floor(0.5 * ar_block:partition_size()), false), %% p2
	{TX5, B5} = mine_block(Node, Wallet, ar_block:partition_size(), true), %% p3 chunks are stored in disk pool

	%% List of {Block, EndOffset, ChunkSize}
	Chunks = [
		%% PaddedEndOffset: 2359296
		{B1, ar_block:partition_size() + ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}, 
		%% PaddedEndOffset: 2621440
		{B1, ar_block:partition_size() + (2*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE}, 
		%% PaddedEndOffset: 2883584
		{B1, ar_block:partition_size() + floor(2.5 * ?DATA_CHUNK_SIZE), floor(0.5 * ?DATA_CHUNK_SIZE)},
		%% PaddedEndOffset: 3145728
		{B2, ar_block:partition_size() + floor(3.75 * ?DATA_CHUNK_SIZE), floor(0.75 * ?DATA_CHUNK_SIZE)},
		%% PaddedEndOffset: 3407872
		{B3, ar_block:partition_size() + (5*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		%% PaddedEndOffset: 3670016
		{B3, ar_block:partition_size() + (6*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		%% PaddedEndOffset: 3932160
		{B3, ar_block:partition_size() + (7*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		%% PaddedEndOffset: 4194304
		{B3, ar_block:partition_size() + (8*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE}
	],

	?LOG_INFO("Source node ~p blocks mined.", [Node]),

	SourcePacking = packing_type_to_packing(PackingType, RewardAddr),

	assert_syncs_range(Node, SourcePacking, 0, 4*ar_block:partition_size()),
	
	assert_chunks(Node, SourcePacking, Chunks),

	%% pack_served_chunks is not enabled so we shouldn't return unpacked data
	?assertMatch({ok, {{<<"404">>, _}, _, _, _, _}},
		ar_http:req(#{
			method => get,
			peer => ar_test_node:peer_ip(Node),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX1#tx.id)) ++ "/data"
		})),

	?LOG_INFO("Source node ~p assertions passed.", [Node]),

	{[B0, {TX1, B1}, {TX2, B2}, {TX3, B3}, {TX4, B4}, {TX5, B5}], RewardAddr, Chunks}.

max_chunk_offset(Chunks) ->
	lists:foldl(fun({_, EndOffset, _}, Acc) -> max(Acc, EndOffset) end, 0, Chunks).

source_node_storage_modules(Node, PackingType, WalletFixture) ->
	source_node_storage_modules(Node, PackingType, WalletFixture, default).

source_node_storage_modules(_Node, unpacked, _WalletFixture, ModuleSize) ->
	{undefined, source_node_storage_modules(unpacked, ModuleSize)};
source_node_storage_modules(Node, PackingType, WalletFixture, ModuleSize) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [WalletFixture]),
	RewardAddr = ar_wallet:to_address(Wallet),
	SourcePacking = packing_type_to_packing(PackingType, RewardAddr),
	{Wallet, source_node_storage_modules(SourcePacking, ModuleSize)}.

source_node_storage_modules(SourcePacking, default) ->
	Size = ar_block:partition_size(),
	lists:map(fun(I) -> {Size, I, SourcePacking} end, lists:seq(0, 4));

source_node_storage_modules(SourcePacking, small) ->
	Size = ar_block:partition_size() div 4,
	lists:map(fun(I) -> {Size, I, SourcePacking} end, lists:seq(0, 19)).
	
mine_block(Node, Wallet, DataSize, IsTemporary) ->
	WeaveSize = ar_test_node:remote_call(Node, ar_node, get_current_weave_size, []),
	Addr = ar_wallet:to_address(Wallet),
	{TX, Chunks} = generate_tx(Node, Wallet, WeaveSize, DataSize),
	B = ar_test_node:post_and_mine(#{ miner => Node, await_on => Node }, [TX]),

	?assertEqual(Addr, B#block.reward_addr),

	Proofs = ar_test_data_sync:post_proofs(Node, B, TX, Chunks, IsTemporary),
	
	ar_test_data_sync:wait_until_syncs_chunks(Node, Proofs, infinity),
	{TX, B}.

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

assert_recall_byte(Node, RangeStart, RangeEnd) when RangeStart > RangeEnd ->
	ok;
assert_recall_byte(Node, RangeStart, RangeEnd) ->
	Options = #{ pack => true, packing => unpacked, origin => miner },
	Result = ar_test_node:remote_call(
		Node, ar_data_sync, get_chunk, [RangeStart + 1, Options]),
	case Result of
		{ok, _} ->
			?LOG_INFO("Recall byte found at ~p", [RangeStart + 1]),
			assert_recall_byte(Node, RangeStart + 1, RangeEnd);
		Error ->
			?LOG_ERROR([{event, recall_byte_not_found}, 
						{recall_byte, RangeStart}, 
						{error, Error}])
	end.
assert_block({spora_2_6, Address}, MinedBlock) ->
	?assertEqual(Address, MinedBlock#block.reward_addr),
	?assertEqual(0, MinedBlock#block.packing_difficulty);
assert_block({composite, Address, PackingDifficulty}, MinedBlock) ->
	?assertEqual(Address, MinedBlock#block.reward_addr),
	?assertEqual(PackingDifficulty, MinedBlock#block.packing_difficulty);
assert_block({replica_2_9, Address}, MinedBlock) ->
	?assertEqual(Address, MinedBlock#block.reward_addr),
	?assertEqual(?REPLICA_2_9_PACKING_DIFFICULTY, MinedBlock#block.packing_difficulty).
	
assert_has_entropy(Node, StartOffset, EndOffset, StoreID) ->
	RangeSize = EndOffset - StartOffset,
	HasEntropy = ar_util:do_until(
		fun() -> 
			Intersection = ar_test_node:remote_call(
				Node, ar_sync_record, get_intersection_size,
				[EndOffset, StartOffset, ar_entropy_storage:sync_record_id(), StoreID]),
			Intersection >= RangeSize
		end,
		100,
		60_000
	),
	case HasEntropy of
		true ->
			ok;
		_ ->
			Intersection = ar_test_node:remote_call(
				Node, ar_sync_record, get_intersection_size,
				[EndOffset, StartOffset, ar_entropy_storage:sync_record_id(), StoreID]),
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"~s failed to prepare entropy range ~p - ~p. Intersection: ~p", 
					[Node, StartOffset, EndOffset, Intersection])))
	end.

assert_no_entropy(Node, StartOffset, EndOffset, StoreID) ->
	HasEntropy = ar_util:do_until(
		fun() -> 
			Intersection = ar_test_node:remote_call(
				Node, ar_sync_record, get_intersection_size,
				[EndOffset, StartOffset, ar_entropy_storage:sync_record_id(), StoreID]),
			Intersection > 0
		end,
		100,
		15_000
	),
	case HasEntropy of
		true ->
			Intersection = ar_test_node:remote_call(
				Node, ar_sync_record, get_intersection_size,
				[EndOffset, StartOffset, ar_entropy_storage:sync_record_id(), StoreID]),
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"~s found entropy when it should not have. Range: ~p - ~p. "
					"Intersection: ~p", 
					[Node, StartOffset, EndOffset, Intersection])));
		_ ->
			ok
	end.

assert_syncs_range(_Node, {replica_2_9, _}, _StartOffset, _EndOffset) ->
	%% For now GET /data_sync_record does not work for replica_2_9. We could call
	%% GET /footprints but we end up with race conditions around the disk pool
	%% threshold (the chunks above the threshold are initially stored as unpacked).
	%% So for now we'll just skip the test.
	ok;
assert_syncs_range(Node, _Packing, StartOffset, EndOffset) ->
	assert_syncs_range(Node, StartOffset, EndOffset).

assert_syncs_range(Node, StartOffset, EndOffset) ->
	HasRange = ar_util:do_until(
		fun() -> has_range(Node, StartOffset, EndOffset) end,
		100,
		300_000
	),
	case HasRange of
		true ->
			ok;
		_ ->
			{ok, SyncRecord} = ar_http_iface_client:get_sync_record(
				ar_test_node:peer_ip(Node)),
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"~s failed to sync range ~p - ~p. Sync record: ~p", 
					[Node, StartOffset, EndOffset, ar_intervals:to_list(SyncRecord)])))
	end.

assert_does_not_sync_range(Node, StartOffset, EndOffset) ->
	ar_util:do_until(
		fun() -> has_range(Node, StartOffset, EndOffset) end,
		1000,
		15_000
	),
	?assertEqual(false, has_range(Node, StartOffset, EndOffset),
		iolist_to_binary(io_lib:format(
			"~s synced range when it should not have: ~p - ~p", 
			[Node, StartOffset, EndOffset]))).

assert_partition_size(Node, PartitionNumber, Packing) ->
	Overlap = ar_storage_module:get_overlap(Packing),
	assert_partition_size(Node, PartitionNumber, Packing, ar_block:partition_size() + Overlap).
assert_partition_size(Node, PartitionNumber, Packing, Size) ->
	?LOG_INFO("~p: Asserting partition ~p,~p is size ~p",
		[Node, PartitionNumber, ar_serialize:encode_packing(Packing, true), Size]),
	ar_util:do_until(
		fun() -> 
			ar_test_node:remote_call(Node, ar_mining_stats, get_partition_data_size, 
				[PartitionNumber, Packing]) >= Size
		end,
		100,
		120_000
	),
	?assertEqual(
		Size,
		ar_test_node:remote_call(Node, ar_mining_stats, get_partition_data_size, 
			[PartitionNumber, Packing]),
		iolist_to_binary(io_lib:format(
			"~s partition ~p,~p was not the expected size.", 
			[Node, PartitionNumber, ar_serialize:encode_packing(Packing, true)]))).

assert_empty_partition(Node, PartitionNumber, Packing) ->
	ar_util:do_until(
		fun() -> 
			ar_test_node:remote_call(Node, ar_mining_stats, get_partition_data_size, 
				[PartitionNumber, Packing]) > 0
		end,
		100,
		15_000
	),
	?assertEqual(
		0,
		ar_test_node:remote_call(Node, ar_mining_stats, get_partition_data_size, 
			[PartitionNumber, Packing]),
		iolist_to_binary(io_lib:format(
			"~s partition ~p,~p is not empty", [Node, PartitionNumber, 
				ar_serialize:encode_packing(Packing, true)]))).

assert_mine_and_validate(MinerNode, ValidatorNode, MinerPacking) ->
	CurrentHeight = max(
		ar_test_node:remote_call(ValidatorNode, ar_node, get_height, []),
		ar_test_node:remote_call(MinerNode, ar_node, get_height, [])
	),
	ar_test_node:wait_until_height(ValidatorNode, CurrentHeight),
	ar_test_node:wait_until_height(MinerNode, CurrentHeight),
	ar_test_node:mine(MinerNode),

	MinerBI = ar_test_node:wait_until_height(MinerNode, CurrentHeight + 1),
	{ok, MinerBlock} = ar_test_node:http_get_block(element(1, hd(MinerBI)), MinerNode),
	assert_block(MinerPacking, MinerBlock),

	ValidatorBI = ar_test_node:wait_until_height(ValidatorNode, MinerBlock#block.height),
	{ok, ValidatorBlock} = ar_test_node:http_get_block(element(1, hd(ValidatorBI)), ValidatorNode),
	?assertEqual(MinerBlock, ValidatorBlock).

has_range(Node, StartOffset, EndOffset) ->
	NodeIP = ar_test_node:peer_ip(Node),
	case ar_http_iface_client:get_sync_record(NodeIP) of
		{ok, RegularIntervals} ->
			FootprintIntervals = collect_footprint_intervals(NodeIP, StartOffset, EndOffset),
			AllIntervals = ar_intervals:union(RegularIntervals, FootprintIntervals),
			interval_contains(AllIntervals, StartOffset, EndOffset);
		Error ->
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"Failed to get sync record from ~p: ~p", [Node, Error]))),
			false
	end.

collect_footprint_intervals(NodeIP, StartOffset, EndOffset) ->
	StartPartition = ar_replica_2_9:get_entropy_partition(StartOffset + 1),
	LastPartition = ar_replica_2_9:get_entropy_partition(EndOffset + 1),
	FootprintsPerPartition = ar_footprint_record:get_footprints_per_partition(),
	collect_footprint_intervals(NodeIP, StartPartition, LastPartition, 0, FootprintsPerPartition - 1, ar_intervals:new()).

collect_footprint_intervals(_NodeIP, Partition, LastPartition, _Footprint, _MaxFootprint, Acc)
		when Partition > LastPartition ->
	Acc;
collect_footprint_intervals(NodeIP, Partition, LastPartition, Footprint, MaxFootprint, Acc)
		when Footprint > MaxFootprint ->
	collect_footprint_intervals(NodeIP, Partition + 1, LastPartition, 0, MaxFootprint, Acc);
collect_footprint_intervals(NodeIP, Partition, LastPartition, Footprint, MaxFootprint, Acc) ->
	FootprintByteIntervals =
		case ar_http_iface_client:get_footprints(NodeIP, Partition, Footprint) of
			{ok, FootprintIntervals} ->
				ar_footprint_record:get_intervals_from_footprint_intervals(FootprintIntervals);
			not_found ->
				?debugFmt("No footprint record found for partition ~B, footprint ~B~n",
					[Partition, Footprint]),
				ar_intervals:new();
			Error ->
				?assert(false,
					iolist_to_binary(io_lib:format(
					"Failed to get footprint record from ~p: ~p, partition: ~B, footprint: ~B",
					[NodeIP, Error, Partition, Footprint])))
		end,
	NewAcc = ar_intervals:union(Acc, FootprintByteIntervals),
	collect_footprint_intervals(NodeIP, Partition, LastPartition, Footprint + 1, MaxFootprint, NewAcc).

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

assert_chunks(Node, Packing, Chunks) ->
	assert_chunks(Node, any, Packing, Chunks).

assert_chunks(Node, RequestPacking, Packing, Chunks) ->
	lists:foreach(fun({Block, EndOffset, ChunkSize}) ->
		assert_chunk(Node, RequestPacking, Packing, Block, EndOffset, ChunkSize)
	end, Chunks).

assert_chunk(Node, RequestPacking, Packing, Block, EndOffset, ChunkSize) ->
	?LOG_INFO("Asserting chunk at offset ~p, size ~p", [EndOffset, ChunkSize]),

	Result = ar_test_node:get_chunk(Node, EndOffset, RequestPacking),
	{ok, {{StatusCode, _}, _, EncodedProof, _, _}} = Result,
	?assertEqual(<<"200">>, StatusCode, iolist_to_binary(io_lib:format(
		"Chunk not found. Node: ~p, Offset: ~p",
		[Node, EndOffset]))),
	Proof = ar_serialize:json_map_to_poa_map(
		jiffy:decode(EncodedProof, [return_maps])
	),
	Proof = ar_serialize:json_map_to_poa_map(
		jiffy:decode(EncodedProof, [return_maps])
	),
	ChunkMetadata = #chunk_metadata{
		tx_root = Block#block.tx_root,
		tx_path = maps:get(tx_path, Proof),
		data_path = maps:get(data_path, Proof)
	},
	ChunkProof = ar_test_node:remote_call(Node, ar_poa, chunk_proof, [ChunkMetadata, EndOffset - 1]),
	{true, _} = ar_test_node:remote_call(Node, ar_poa, validate_paths, [ChunkProof]),
	Chunk = maps:get(chunk, Proof),

	maybe_write_chunk_fixture(Packing, EndOffset, Chunk),

	{ok, ExpectedPackedChunk} = load_chunk_fixture(Packing, EndOffset),
	?assertEqual(ExpectedPackedChunk, Chunk,
		iolist_to_binary(io_lib:format(
			"~p: Chunk at offset ~p, size ~p, packing ~p does not match packed chunk",
			[Node, EndOffset, ChunkSize, ar_serialize:encode_packing(Packing, true)]))),

	{ok, UnpackedChunk} = ar_packing_server:unpack(
		Packing, EndOffset, Block#block.tx_root, Chunk, ?DATA_CHUNK_SIZE),
	UnpaddedChunk = ar_packing_server:unpad_chunk(
		Packing, UnpackedChunk, ChunkSize, byte_size(Chunk)),
	ExpectedUnpackedChunk = ar_test_node:get_genesis_chunk(EndOffset),
	?assertEqual(ExpectedUnpackedChunk, UnpaddedChunk,
		iolist_to_binary(io_lib:format(
			"~p: Chunk at offset ~p, size ~p does not match unpacked chunk",
			[Node, EndOffset, ChunkSize]))).

assert_no_chunks(Node, Chunks) ->
	lists:foreach(fun({_Block, EndOffset, _ChunkSize}) ->
		assert_no_chunk(Node, EndOffset)
	end, Chunks).

assert_no_chunk(Node, EndOffset) ->
	Result = ar_test_node:get_chunk(Node, EndOffset, any),
	{ok, {{StatusCode, _}, _, _, _, _}} = Result,
	?assertEqual(<<"404">>, StatusCode, iolist_to_binary(io_lib:format(
		"Chunk found when it should not have been. Node: ~p, Offset: ~p",
		[Node, EndOffset]))).

delayed_print(Format, Args) ->
	%% Print the specific flavor of this test since it isn't captured in the test name.
	%% Delay the print by 1 second to allow the eunit output to be flushed.
	spawn(fun() ->
		timer:sleep(1000),
		io:fwrite(user, Format, Args)
	end).

%% --------------------------------------------------------------------------------------------
%% Test Data Generation
%% --------------------------------------------------------------------------------------------	
write_wallet_fixtures() ->
	Wallets = [wallet_a, wallet_b, wallet_c, wallet_d],
	lists:foreach(fun(Wallet) ->
		WalletName = atom_to_list(Wallet),
		ar_wallet:new_keyfile(?DEFAULT_KEY_TYPE, WalletName),
		install_fixture(
			ar_wallet:wallet_filepath(Wallet), wallets, WalletName ++ ".json")
	end, Wallets),
	ok.

maybe_write_chunk_fixture(Packing, EndOffset, Chunk) when ?UPDATE_CHUNK_FIXTURES =:= true ->
	?LOG_ERROR("WARNING: Updating chunk fixture! EndOffset: ~p, Packing: ~p", 
		[EndOffset, ar_serialize:encode_packing(Packing, true)]),
	write_chunk_fixture(Packing, EndOffset, Chunk);
maybe_write_chunk_fixture(_, _, _) ->
	ok.
