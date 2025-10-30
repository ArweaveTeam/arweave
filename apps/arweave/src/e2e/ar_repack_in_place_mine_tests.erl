-module(ar_repack_in_place_mine_tests).

-include_lib("arweave_config/include/arweave_config.hrl").

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
		{timeout, Timeout, {with, {unpacked, replica_2_9, default}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {spora_2_6, replica_2_9, default}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {replica_2_9, replica_2_9, default}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {replica_2_9, unpacked, default}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {spora_2_6, unpacked, default}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {unpacked, replica_2_9, small}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {spora_2_6, replica_2_9, small}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {replica_2_9, replica_2_9, small}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {replica_2_9, unpacked, small}, [fun test_repack_in_place_mine/1]}},
		{timeout, Timeout, {with, {spora_2_6, unpacked, small}, [fun test_repack_in_place_mine/1]}}
	].

%% --------------------------------------------------------------------------------------------
%% test_repack_in_place_mine
%% --------------------------------------------------------------------------------------------
test_repack_in_place_mine({FromPackingType, ToPackingType, ModuleSize}) ->
	ar_e2e:delayed_print(<<" ~p -> ~p (~p) ">>, [FromPackingType, ToPackingType, ModuleSize]),
	?LOG_INFO([{event, test_repack_in_place_mine}, {module, ?MODULE},
		{from_packing_type, FromPackingType}, {to_packing_type, ToPackingType},
		{module_size, ModuleSize}]),
	ValidatorNode = peer1,
	RepackerNode = peer2,
	ar_test_node:stop(ValidatorNode),
	ar_test_node:stop(RepackerNode),
	{Blocks, _AddrA, Chunks} = ar_e2e:start_source_node(
		RepackerNode, FromPackingType, wallet_a, ModuleSize),

	[B0 | _] = Blocks,
	start_validator_node(ValidatorNode, RepackerNode, B0),

	NumModules = case ModuleSize of
		default -> 2;
		small -> 8
	end,

	{WalletB, SourceStorageModules} = ar_e2e:source_node_storage_modules(
		RepackerNode, ToPackingType, wallet_b, ModuleSize),
	AddrB = case WalletB of
		undefined -> undefined;
		_ -> ar_wallet:to_address(WalletB)
	end,
	FinalStorageModules = lists:sublist(SourceStorageModules, NumModules),
	ToPacking = ar_e2e:packing_type_to_packing(ToPackingType, AddrB),
	{ok, Config} = ar_test_node:get_config(RepackerNode),

	RepackInPlaceStorageModules = lists:sublist([ 
		{Module, ToPacking} || Module <- Config#config.storage_modules ], NumModules),
	
	ar_test_node:restart_with_config(RepackerNode, Config#config{
		storage_modules = [],
		repack_in_place_storage_modules = RepackInPlaceStorageModules,
		mining_addr = undefined
	}),

	ExpectedSize0 = ar_e2e:aligned_partition_size(RepackerNode, 0, ToPacking),
	ar_e2e:assert_partition_size(RepackerNode, 0, ToPacking, ExpectedSize0),
	ExpectedSize1 = ar_e2e:aligned_partition_size(RepackerNode, 1, ToPacking),
	ar_e2e:assert_partition_size(RepackerNode, 1, ToPacking, ExpectedSize1),

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

	ar_test_node:restart_with_config(RepackerNode, Config#config{
		storage_modules = FinalStorageModules,
		repack_in_place_storage_modules = [],
		mining_addr = AddrB
	}),
	
	ar_e2e:assert_chunks(RepackerNode, ToPacking, Chunks),

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