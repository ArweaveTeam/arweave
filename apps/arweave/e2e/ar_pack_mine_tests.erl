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
			{timeout, 120, fun test_pack_mine/0}
		]}
    }.

%% ------------------------------------------------------------------------------------------------
%% pack_mine_test_
%% ------------------------------------------------------------------------------------------------
test_pack_mine() ->
	?LOG_INFO([{event, test_pack_mine}]),
	Node  = peer1,
	Wallet = ar_test_node:remote_call(Node, ar_wallet, new_keyfile, []),
	RewardAddr = ar_wallet:to_address(Wallet),
	[B0] = ar_weave:init([{RewardAddr, ?AR(200), <<>>}], 0, ?PARTITION_SIZE),


	{ok, Config} = ar_test_node:remote_call(Node, application, get_env, [arweave, config]),
	%% We'll use partition 0 for any unsynced ranges.
	StorageModules = [
		{?PARTITION_SIZE, 0, {spora_2_6, RewardAddr}},
		{?PARTITION_SIZE, 1, {spora_2_6, RewardAddr}},
		{?PARTITION_SIZE, 2, {spora_2_6, RewardAddr}}
	],
	?LOG_INFO([{event, starting_node}, {node, ar_test_node:peer_name(Node)}]),
	?debugFmt("Starting node ~s~n", [ar_test_node:peer_name(Node)]),
	StartResult = ar_test_node:start_other_node(Node, B0, Config#config{
		start_from_latest_state = true,
		storage_modules = StorageModules,
		auto_join = true,
		mining_addr = RewardAddr
		%enable = [pack_served_chunks | Config#config.enable]
	}, true),
	?debugFmt("Start result: ~p~n", [StartResult]),

	{TX, Chunks} = generate_tx(Wallet),
	B = ar_test_node:post_and_mine(#{ miner => Node, await_on => Node }, [TX]),
	Proofs = ar_test_data_sync:post_proofs(Node, B, TX, Chunks),
	
	ar_test_data_sync:wait_until_syncs_chunks(Node, Proofs, infinity),

	timer:sleep(10000),

	Offset = ?PARTITION_SIZE + ?DATA_CHUNK_SIZE,
	{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} = ar_test_node:get_chunk(Node, Offset, any),
	Proof = ar_serialize:json_map_to_poa_map(
		jiffy:decode(EncodedProof, [return_maps])
	),
	?assertEqual(ar_test_node:get_genesis_chunk(Offset), maps:get(chunk, Proof)),
	ok.



generate_tx(Wallet) ->
	DataSize = 3 * ?DATA_CHUNK_SIZE,
	Offset1 = ?DATA_CHUNK_SIZE,
	Offset2 = Offset1 + ?DATA_CHUNK_SIZE,
	Offset3 = Offset2 + ?DATA_CHUNK_SIZE,
	Chunk1 = ar_test_node:get_genesis_chunk(?PARTITION_SIZE + Offset1),
	Chunk2 = ar_test_node:get_genesis_chunk(?PARTITION_SIZE + Offset2),
	Chunk3 = ar_test_node:get_genesis_chunk(?PARTITION_SIZE + Offset3),
	{DataRoot, _DataTree} = ar_merkle:generate_tree([
		{ar_tx:generate_chunk_id(Chunk1), Offset1},
		{ar_tx:generate_chunk_id(Chunk2), Offset2},
		{ar_tx:generate_chunk_id(Chunk3), Offset3}]),
	TX = ar_test_node:sign_tx(peer1, Wallet, #{ 
		data_size => DataSize,
		data_root => DataRoot 
	}),
	{TX, [Chunk1, Chunk2, Chunk3]}.

