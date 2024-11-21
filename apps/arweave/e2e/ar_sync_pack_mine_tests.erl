-module(ar_sync_pack_mine_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

%% --------------------------------------------------------------------------------------------
%% Fixtures
%% --------------------------------------------------------------------------------------------
setup_source_node(unpacked) ->
	SourceNode = peer1,
	TempNode = peer2,
	ar_test_node:stop(TempNode),
	{Blocks, _SourceAddr, Chunks} = ar_e2e:start_source_node(TempNode, composite_1, wallet_a),
	{_, StorageModules} = ar_e2e:source_node_storage_modules(SourceNode, unpacked, wallet_a),
	[B0 | _] = Blocks,
	{ok, Config} = ar_test_node:get_config(SourceNode),
	ar_test_node:start_other_node(SourceNode, B0, Config#config{
		peers = [ar_test_node:peer_ip(TempNode)],
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true
	}, true),
	ar_e2e:assert_syncs_range(SourceNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	ar_e2e:assert_chunks(SourceNode, unpacked, Chunks),
	ar_test_node:stop(TempNode),
	{Blocks, Chunks, unpacked};

setup_source_node(PackingType) ->
	SourceNode = peer1,
	SinkNode = peer2,
	ar_test_node:stop(SinkNode),
	{Blocks, SourceAddr, Chunks} = ar_e2e:start_source_node(SourceNode, PackingType, wallet_a),
	SourcePacking = ar_e2e:packing_type_to_packing(PackingType, SourceAddr),
	ar_e2e:assert_syncs_range(SourceNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	ar_e2e:assert_chunks(SourceNode, SourcePacking, Chunks),

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
	ar_e2e:assert_syncs_range(SinkNode, ?PARTITION_SIZE, 2*?PARTITION_SIZE),
	ar_e2e:assert_chunks(SinkNode, SinkPacking, Chunks),

	case SinkPackingType of
		unpacked ->
			ok;
		_ ->
			CurrentHeight = ar_test_node:remote_call(SinkNode, ar_node, get_height, []),
			ar_test_node:mine(SinkNode),

			SinkBI = ar_test_node:wait_until_height(SinkNode, CurrentHeight + 1),
			{ok, SinkBlock} = ar_test_node:http_get_block(element(1, hd(SinkBI)), SinkNode),
			ar_e2e:assert_block(SinkPacking, SinkBlock),

			SourceBI = ar_test_node:wait_until_height(SourceNode, SinkBlock#block.height),
			{ok, SourceBlock} = ar_test_node:http_get_block(element(1, hd(SourceBI)), SourceNode),
			?assertEqual(SinkBlock, SourceBlock),
			ok
	end.

start_sink_node(Node, SourceNode, B0, PackingType) ->
	Wallet = ar_test_node:remote_call(Node, ar_e2e, load_wallet_fixture, [wallet_b]),
	SinkAddr = ar_wallet:to_address(Wallet),
	SinkPacking = ar_e2e:packing_type_to_packing(PackingType, SinkAddr),
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
