-module(ar_repack_mine_tests).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

%% --------------------------------------------------------------------------------------------
%% Fixtures
%% --------------------------------------------------------------------------------------------


%% --------------------------------------------------------------------------------------------
%% Test Registration
%% --------------------------------------------------------------------------------------------
spora_2_6_repack_mine_test_() ->
	[
		{timeout, 300, fun test_repack_mine/0}	
	].

%% --------------------------------------------------------------------------------------------
%% test_repack_mine
%% --------------------------------------------------------------------------------------------



test_repack_mine() ->
	{Blocks, _AddrA, Chunks} = ar_e2e:start_source_node(peer2, spora_2_6, wallet_a),

	[B0 | _] = Blocks,
	start_validator_node(peer1, peer2, B0),

	{WalletB, StorageModules} = ar_e2e:source_node_storage_modules(peer2, spora_2_6, wallet_b),
	AddrB = ar_wallet:to_address(WalletB),
	{ok, Config} = ar_test_node:get_config(peer2),
	ar_test_node:update_config(peer2, Config#config{
		storage_modules = Config#config.storage_modules ++ StorageModules,
		mining_addr = AddrB
	}),
	ar_test_node:restart(peer2),

	ar_e2e:assert_partition_size(peer2, 1, {spora_2_6, AddrB}, ?PARTITION_SIZE),

	ar_test_node:update_config(peer2, Config#config{
		storage_modules = StorageModules,
		mining_addr = AddrB
	}),
	ar_test_node:restart(peer2),
	ar_e2e:assert_chunks(peer2, {spora_2_6, AddrB}, Chunks),


	CurrentHeight = ar_test_node:remote_call(peer2, ar_node, get_height, []),
	ar_test_node:mine(peer2),

	SinkBI = ar_test_node:wait_until_height(peer2, CurrentHeight + 1),
	{ok, SinkBlock} = ar_test_node:http_get_block(element(1, hd(SinkBI)), peer2),
	ar_e2e:assert_block({spora_2_6, AddrB}, SinkBlock),

	ValidatorBI = ar_test_node:wait_until_height(peer1, SinkBlock#block.height),
	{ok, ValidatorBlock} = ar_test_node:http_get_block(element(1, hd(ValidatorBI)), peer1),
	?assertEqual(SinkBlock, ValidatorBlock).


start_validator_node(Node, SourceNode,B0) ->
	{ok, Config} = ar_test_node:get_config(Node),
	?assertEqual(ar_test_node:peer_name(Node),
		ar_test_node:start_other_node(Node, B0, Config#config{
			peers = [ar_test_node:peer_ip(SourceNode)],
			start_from_latest_state = true,
			auto_join = true
		}, true)
	),

	ok.
	