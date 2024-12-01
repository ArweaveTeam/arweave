-module(ar_repack_in_place_mine_tests).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

%% --------------------------------------------------------------------------------------------
%% Test Registration
%% --------------------------------------------------------------------------------------------
repack_in_place_mine_test_() ->
	[
		{timeout, 300, {with, {spora_2_6, spora_2_6}, [fun test_repack_in_place_mine/1]}}
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

	{WalletB, FinalStorageModules} = ar_e2e:source_node_storage_modules(
		RepackerNode, ToPackingType, wallet_b),
	AddrB = case WalletB of
		undefined -> undefined;
		_ -> ar_wallet:to_address(WalletB)
	end,
	ToPacking = ar_e2e:packing_type_to_packing(ToPackingType, AddrB),
	{ok, Config} = ar_test_node:get_config(RepackerNode),

	RepackInPlaceStorageModules = [ 
		{Module, ToPacking} || Module <- Config#config.storage_modules ],
	?LOG_INFO("Repack in place storage modules: ~p", [RepackInPlaceStorageModules]),
	
	ar_test_node:update_config(RepackerNode, Config#config{
		storage_modules = [],
		repack_in_place_storage_modules = RepackInPlaceStorageModules,
		mining_addr = undefined
	}),
	ar_test_node:restart(RepackerNode),

	ar_e2e:assert_partition_size(RepackerNode, 1, ToPacking, ?PARTITION_SIZE),

	ar_test_node:update_config(RepackerNode, Config#config{
		storage_modules = FinalStorageModules,
		repack_in_place_storage_modules = [],
		mining_addr = AddrB
	}),
	ar_test_node:restart(RepackerNode),
	ar_e2e:assert_chunks(RepackerNode, ToPacking, Chunks),

	case ToPackingType of
		unpacked ->
			ok;
		_ ->
			CurrentHeight = ar_test_node:remote_call(RepackerNode, ar_node, get_height, []),
			ar_test_node:mine(RepackerNode),

			RepackerBI = ar_test_node:wait_until_height(RepackerNode, CurrentHeight + 1),
			{ok, RepackerBlock} = ar_test_node:http_get_block(element(1, hd(RepackerBI)), RepackerNode),
			ar_e2e:assert_block(ToPacking, RepackerBlock),

			ValidatorBI = ar_test_node:wait_until_height(ValidatorNode, RepackerBlock#block.height),
			{ok, ValidatorBlock} = ar_test_node:http_get_block(element(1, hd(ValidatorBI)), ValidatorNode),
			?assertEqual(RepackerBlock, ValidatorBlock)
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
	