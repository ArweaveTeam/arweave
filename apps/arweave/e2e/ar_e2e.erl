-module(ar_e2e).

-export([fixture_dir/1, fixture_dir/2, install_fixture/3, load_wallet_fixture/1,
	write_chunk_fixture/3, load_chunk_fixture/2]).

-export([delayed_print/2, packing_type_to_packing/2,
	start_source_node/3, source_node_storage_modules/3, max_chunk_offset/1,
	assert_recall_byte/3,
	assert_block/2, assert_syncs_range/3, assert_syncs_range/4, assert_does_not_sync_range/3,
	assert_has_entropy/4, assert_no_entropy/4,
	assert_chunks/3, assert_chunks/4, assert_no_chunks/2,
	assert_partition_size/3, assert_partition_size/4, assert_empty_partition/3,
	assert_mine_and_validate/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
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

start_source_node(Node, unpacked, _WalletFixture) ->
	?LOG_INFO("Starting source node ~p with packing type ~p and wallet fixture ~p",
		[Node, unpacked, _WalletFixture]),
	TempNode = case Node of
		peer1 -> peer2;
		peer2 -> peer1
	end,
	{Blocks, _SourceAddr, Chunks} = ar_e2e:start_source_node(TempNode, spora_2_6, wallet_a),
	{_, StorageModules} = ar_e2e:source_node_storage_modules(Node, unpacked, wallet_a),
	[B0, _, {TX2, _} | _] = Blocks,
	{ok, Config} = ar_test_node:get_config(Node),
	ar_test_node:start_other_node(Node, B0, Config#config{
		peers = [ar_test_node:peer_ip(TempNode)],
		storage_modules = StorageModules,
		auto_join = true
	}, true),

	?LOG_INFO("Source node ~p started.", [Node]),
	
	ar_e2e:assert_syncs_range(Node, 0, 4*?PARTITION_SIZE),
	
	ar_e2e:assert_partition_size(Node, 0, unpacked),
	ar_e2e:assert_partition_size(Node, 1, unpacked),
	ar_e2e:assert_partition_size(Node, 2, unpacked, floor(0.5*?PARTITION_SIZE)),

	ar_e2e:assert_chunks(Node, unpacked, Chunks),

	ar_e2e:assert_empty_partition(Node, 3, unpacked),

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
	{ok, ExpectedData} = ar_e2e:load_chunk_fixture(
		unpacked, ?PARTITION_SIZE + floor(3.75 * ?DATA_CHUNK_SIZE)),
	?assertEqual(ExpectedData, ar_util:decode(Data)),

	?LOG_INFO("Source node ~p restarted.", [Node]),

	{Blocks, undefined, Chunks};
start_source_node(Node, PackingType, WalletFixture) ->
	?LOG_INFO("Starting source node ~p with packing type ~p and wallet fixture ~p",
		[Node, PackingType, WalletFixture]),
	{Wallet, StorageModules} = source_node_storage_modules(Node, PackingType, WalletFixture),
	RewardAddr = ar_wallet:to_address(Wallet),
	[B0] = ar_weave:init([{RewardAddr, ?AR(200), <<>>}], 0, ?PARTITION_SIZE),

	{ok, Config} = ar_test_node:remote_call(Node, application, get_env, [arweave, config]),
	
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
	{TX3, B3} = mine_block(Node, Wallet, ?PARTITION_SIZE, false), %% p1 to p2
	{TX4, B4} = mine_block(Node, Wallet, floor(0.5 * ?PARTITION_SIZE), false), %% p2
	{TX5, B5} = mine_block(Node, Wallet, ?PARTITION_SIZE, true), %% p3 chunks are stored in disk pool

	%% List of {Block, EndOffset, ChunkSize}
	Chunks = [
		%% PaddedEndOffset: 2359296
		{B1, ?PARTITION_SIZE + ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE}, 
		%% PaddedEndOffset: 2621440
		{B1, ?PARTITION_SIZE + (2*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE}, 
		%% PaddedEndOffset: 2883584
		{B1, ?PARTITION_SIZE + floor(2.5 * ?DATA_CHUNK_SIZE), floor(0.5 * ?DATA_CHUNK_SIZE)},
		%% PaddedEndOffset: 3145728
		{B2, ?PARTITION_SIZE + floor(3.75 * ?DATA_CHUNK_SIZE), floor(0.75 * ?DATA_CHUNK_SIZE)},
		%% PaddedEndOffset: 3407872
		{B3, ?PARTITION_SIZE + (5*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		%% PaddedEndOffset: 3670016
		{B3, ?PARTITION_SIZE + (6*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		%% PaddedEndOffset: 3932160
		{B3, ?PARTITION_SIZE + (7*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE},
		%% PaddedEndOffset: 4194304
		{B3, ?PARTITION_SIZE + (8*?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE}
	],

	?LOG_INFO("Source node ~p blocks mined.", [Node]),

	SourcePacking = ar_e2e:packing_type_to_packing(PackingType, RewardAddr),

	ar_e2e:assert_syncs_range(Node, SourcePacking, 0, 4*?PARTITION_SIZE),

	%% No overlap since we aren't syncing or repacking chunks.
	ar_e2e:assert_partition_size(Node, 0, SourcePacking, ?PARTITION_SIZE),
	ar_e2e:assert_partition_size(Node, 1, SourcePacking, ?PARTITION_SIZE),
	ar_e2e:assert_partition_size(Node, 2, SourcePacking, floor(0.5*?PARTITION_SIZE)),
	
	ar_e2e:assert_chunks(Node, SourcePacking, Chunks),

	ar_e2e:assert_empty_partition(Node, 3, SourcePacking),

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

source_node_storage_modules(_Node, unpacked, _WalletFixture) ->
	{undefined, source_node_storage_modules(unpacked)};
source_node_storage_modules(Node, PackingType, WalletFixture) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [WalletFixture]),
	RewardAddr = ar_wallet:to_address(Wallet),
	SourcePacking = packing_type_to_packing(PackingType, RewardAddr),
	{Wallet, source_node_storage_modules(SourcePacking)}.

source_node_storage_modules(SourcePacking) ->
	[
		{?PARTITION_SIZE, 0, SourcePacking},
		{?PARTITION_SIZE, 1, SourcePacking},
		{?PARTITION_SIZE, 2, SourcePacking},
		{?PARTITION_SIZE, 3, SourcePacking},
		{?PARTITION_SIZE, 4, SourcePacking}
	].
	
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
				[EndOffset, StartOffset, ar_chunk_storage_replica_2_9_1_entropy, StoreID]),
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
				[EndOffset, StartOffset, ar_chunk_storage_replica_2_9_1_entropy, StoreID]),
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
				[EndOffset, StartOffset, ar_chunk_storage_replica_2_9_1_entropy, StoreID]),
			Intersection > 0
		end,
		100,
		15_000
	),
	case HasEntropy of
		true ->
			Intersection = ar_test_node:remote_call(
				Node, ar_sync_record, get_intersection_size,
				[EndOffset, StartOffset, ar_chunk_storage_replica_2_9_1_entropy, StoreID]),
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"~s found entropy when it should not have. Range: ~p - ~p. "
					"Intersection: ~p", 
					[Node, StartOffset, EndOffset, Intersection])));
		_ ->
			ok
	end.

assert_syncs_range(Node, {replica_2_9, _}, StartOffset, EndOffset) ->
	%% For now GET /data_sync_record does not work for replica_2_9. So we'll assert
	%% tat the node *does not* sync the range.
	assert_does_not_sync_range(Node, StartOffset, EndOffset);
assert_syncs_range(Node, _Packing, StartOffset, EndOffset) ->
	assert_syncs_range(Node, StartOffset, EndOffset).

assert_syncs_range(Node, StartOffset, EndOffset) ->
	HasRange = ar_util:do_until(
		fun() -> has_range(Node, StartOffset, EndOffset) end,
		100,
		60_000
	),
	case HasRange of
		true ->
			ok;
		_ ->
			SyncRecord = ar_http_iface_client:get_sync_record(
				ar_test_node:peer_ip(Node)),
			?assert(false, 
				iolist_to_binary(io_lib:format(
					"~s failed to sync range ~p - ~p. Sync record: ~p", 
					[Node, StartOffset, EndOffset, SyncRecord])))
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
	assert_partition_size(Node, PartitionNumber, Packing, ?PARTITION_SIZE + Overlap).
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
	ar_e2e:assert_block(MinerPacking, MinerBlock),

	ValidatorBI = ar_test_node:wait_until_height(ValidatorNode, MinerBlock#block.height),
	{ok, ValidatorBlock} = ar_test_node:http_get_block(element(1, hd(ValidatorBI)), ValidatorNode),
	?assertEqual(MinerBlock, ValidatorBlock).

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

assert_chunks(Node, Packing, Chunks) ->
	%% Normally we can't sync replica_2_9 data since it's too expensive to unpack. The
	%% one exception is if you request the exact format stored by the node.
	RequestPacking = case Packing of
		{replica_2_9, _} -> Packing;
		_ -> any
	end,
	assert_chunks(Node, RequestPacking, Packing, Chunks).

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
		ar_e2e:install_fixture(
			ar_wallet:wallet_filepath(Wallet), wallets, WalletName ++ ".json")
	end, Wallets),
	ok.

maybe_write_chunk_fixture(Packing, EndOffset, Chunk) when ?UPDATE_CHUNK_FIXTURES =:= true ->
	?LOG_ERROR("WARNING: Updating chunk fixture! EndOffset: ~p, Packing: ~p", 
		[EndOffset, ar_serialize:encode_packing(Packing, true)]),
	ar_e2e:write_chunk_fixture(Packing, EndOffset, Chunk);
maybe_write_chunk_fixture(_, _, _) ->
	ok.
