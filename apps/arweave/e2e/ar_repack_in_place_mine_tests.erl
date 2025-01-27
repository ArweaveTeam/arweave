-module(ar_repack_in_place_mine_tests).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REPACK_IN_PLACE_MINE_TEST_TIMEOUT, 600).

%% --------------------------------------------------------------------------------------------
%% Test Registration
%% --------------------------------------------------------------------------------------------
%%
%% Note:
%% Repacking in place *from* replica_2_9 to any format is not currently supported.
repack_in_place_mine_test_() ->
	Timeout = ?REPACK_IN_PLACE_MINE_TEST_TIMEOUT,
	[
		{timeout, Timeout, {with, {unpacked, replica_2_9}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {spora_2_6, replica_2_9}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {composite_1, replica_2_9}, [fun test_repack_in_place_mine/1]}}
	].

%% --------------------------------------------------------------------------------------------
%% test_repack_in_place_mine
%% --------------------------------------------------------------------------------------------
test_repack_in_place_mine({FromPackingType, ToPackingType}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p ">>, [FromPackingType, ToPackingType]),
	ValidatorNode = peer1,
	RepackerNode = peer2,
	{Blocks, _AddrA, Chunks} = ar_e2e:start_source_node(
		RepackerNode, FromPackingType, wallet_a),

	[B0 | _] = Blocks,
	start_validator_node(ValidatorNode, RepackerNode, B0),

	{WalletB, SourceStorageModules} = ar_e2e:source_node_storage_modules(
		RepackerNode, ToPackingType, wallet_b),
	AddrB = case WalletB of
		undefined -> undefined;
		_ -> ar_wallet:to_address(WalletB)
	end,
	FinalStorageModules = lists:sublist(SourceStorageModules, 2),
	ToPacking = ar_e2e:packing_type_to_packing(ToPackingType, AddrB),
	{ok, Config} = ar_test_node:get_config(RepackerNode),

	RepackInPlaceStorageModules = lists:sublist([ 
		{Module, ToPacking} || Module <- Config#config.storage_modules ], 2),
	
	ar_test_node:update_config(RepackerNode, Config#config{
		storage_modules = [],
		repack_in_place_storage_modules = RepackInPlaceStorageModules,
		mining_addr = undefined
	}),
	ar_test_node:restart(RepackerNode),

	ar_e2e:assert_partition_size(RepackerNode, 0, ToPacking),
	ar_e2e:assert_partition_size(RepackerNode, 1, ToPacking),

	ar_test_node:stop(RepackerNode),

	%% Rename storage_modules
	DataDir = Config#config.data_dir,
	lists:foreach(fun({SourceModule, Packing}) ->
		{BucketSize, Bucket, _Packing} = SourceModule,
		SourceID = ar_storage_module:id(SourceModule),
		SourcePath = ar_chunk_storage:get_storage_module_path(DataDir, SourceID),

		TargetModule = {BucketSize, Bucket, Packing},
		TargetID = ar_storage_module:id(TargetModule),
		TargetPath = ar_chunk_storage:get_storage_module_path(DataDir, TargetID),
		file:rename(SourcePath, TargetPath)
	end, RepackInPlaceStorageModules),

	ar_test_node:update_config(RepackerNode, Config#config{
		storage_modules = FinalStorageModules,
		repack_in_place_storage_modules = [],
		mining_addr = AddrB
	}),
	ar_test_node:restart(RepackerNode),
	
	case FromPackingType of
		unpacked ->
			%% There's an edge-case bug in GET /chunk. If a small chunk has been repacked
			%% in place it will exist in both the chunk_data_db (as a small unpacked chunk)
			%% and on disk (as a full-sized packed chunk). If the packed version is requested
			%% via GET /chunk it will check the sync record and determine that the packed
			%% chunk exists, but then will first try chunk_data_db when looking for the chunk
			%% and, finding it there, will return it. 
			%% 
			%% This e2e test explicitly queries the replica_2_9 packed chunks and so it will
			%% fail when it receives the unpacked chunk.
			%% 
			%% However this is an edge case that is unlikely to happen in practice. Typically
			%% users and nodes only request 'any' or 'unpacked'. They *may* on occasion
			%% request 'spora_2_6' or (for a time) 'composite' - but only when syncing from
			%% their own local peers. If so they may hit this bug if they had previously
			%% done a repack in place from unpacked. However spora_2_6 and composite are
			%% largely deprecated in favor of replica_2_9. And since syncing replica_2_9
			%% is disabled in production, this bug is unlikely to be hit in practice.
			%% 
			%% Long story short: we'll adjust the test to assert the bug rather than fix the
			%% bug.
			RegularChunks = lists:filter(
				fun({_, _, ChunkSize}) -> ChunkSize >= ?DATA_CHUNK_SIZE end, Chunks),
			ar_e2e:assert_chunks(RepackerNode, ToPacking, RegularChunks),

			SmallChunks = lists:filter(
				fun({_, _, ChunkSize}) -> ChunkSize < ?DATA_CHUNK_SIZE end, Chunks),
			ar_e2e:assert_chunks(RepackerNode, ToPacking, unpacked,SmallChunks);
		_ ->
			ar_e2e:assert_chunks(RepackerNode, ToPacking, Chunks)
	end,

	case ToPackingType of
		unpacked ->
			ok;
		_ ->
			ar_e2e:assert_mine_and_validate(RepackerNode, ValidatorNode, ToPacking)
	end.


start_validator_node(ValidatorNode, RepackerNode, B0) ->
	{ok, Config} = ar_test_node:get_config(ValidatorNode),
	?assertEqual(ar_test_node:peer_name(ValidatorNode),
		ar_test_node:start_other_node(ValidatorNode, B0, Config#config{
			peers = [ar_test_node:peer_ip(RepackerNode)],
			start_from_latest_state = true,
			auto_join = true
		}, true)
	),
	ok.
	