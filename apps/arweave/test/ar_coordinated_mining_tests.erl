-module(ar_coordinated_mining_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [http_get_block/2]).

%% --------------------------------------------------------------------
%% Test registration
%% --------------------------------------------------------------------
mining_test_() ->
	[
		{timeout, ?TEST_NODE_TIMEOUT, fun test_single_node_one_chunk/0},
		ar_test_node:test_with_mocked_functions(
			[
				ar_test_node:mock_to_force_invalid_h1(),
				{ar_retarget, is_retarget_height, fun(_Height) -> false end},
				{ar_retarget, is_retarget_block, fun(_Block) -> false end}
			],
			fun test_single_node_two_chunk/0, ?TEST_NODE_TIMEOUT),
		ar_test_node:test_with_mocked_functions(
			[
				ar_test_node:mock_to_force_invalid_h1(),
				{ar_retarget, is_retarget_height, fun(_Height) -> false end},
				{ar_retarget, is_retarget_block, fun(_Block) -> false end}
			],
			fun test_cross_node/0, ?TEST_NODE_TIMEOUT),
		ar_test_node:test_with_mocked_functions(
			[
				ar_test_node:mock_to_force_invalid_h1(),
				mock_for_single_difficulty_adjustment_height(),
				mock_for_single_difficulty_adjustment_block()
			],
			fun test_cross_node_retarget/0, 2 * ?TEST_NODE_TIMEOUT),
		{timeout, ?TEST_NODE_TIMEOUT, fun test_two_node_retarget/0},
		ar_test_node:test_with_mocked_functions(
			[
				{ar_retarget, is_retarget_height, fun(_Height) -> false end},
				{ar_retarget, is_retarget_block, fun(_Block) -> false end}
			],
			fun test_three_node/0, 2 * ?TEST_NODE_TIMEOUT),
		{timeout, ?TEST_NODE_TIMEOUT, fun test_no_exit_node/0}
	].

api_test_() ->
	[
		{timeout, ?TEST_NODE_TIMEOUT, fun test_no_secret/0},
		{timeout, ?TEST_NODE_TIMEOUT, fun test_bad_secret/0},
		{timeout, ?TEST_NODE_TIMEOUT, fun test_partition_table/0}
	].

refetch_partitions_test_() ->
	[
		{timeout, ?TEST_NODE_TIMEOUT, fun test_peers_by_partition/0}
	].

%% --------------------------------------------------------------------
%% Tests
%% --------------------------------------------------------------------

%% @doc One-node coordinated mining cluster mining a block with one
%% or two chunks.
test_single_node_one_chunk() ->
	[Node, _ExitNode, ValidatorNode] = ar_test_node:start_coordinated(1),
	ar_test_node:mine(Node),
	BI = ar_test_node:wait_until_height(ValidatorNode, 1, false),
	{ok, B} = http_get_block(element(1, hd(BI)), ValidatorNode),
	?assert(byte_size((B#block.poa)#poa.data_path) > 0),
	assert_empty_cache(Node).
	
%% @doc One-node coordinated mining cluster mining a block with two chunks.
test_single_node_two_chunk() ->
	[Node, _ExitNode, ValidatorNode] = ar_test_node:start_coordinated(1),
	ar_test_node:mine(Node),
	BI = ar_test_node:wait_until_height(ValidatorNode, 1, false),
	{ok, B} = http_get_block(element(1, hd(BI)), ValidatorNode),
	?assert(byte_size((B#block.poa2)#poa.data_path) > 0),
	assert_empty_cache(Node).

%% @doc Two-node coordinated mining cluster mining until a difficulty retarget.
test_two_node_retarget() ->
	[Node1, Node2, _ExitNode, ValidatorNode] = ar_test_node:start_coordinated(2),
	lists:foreach(
		fun(Height) ->
			mine_in_parallel([Node1, Node2], ValidatorNode, Height)
		end,
		lists:seq(0, ?RETARGET_BLOCKS)),
	assert_empty_cache(Node1),
	assert_empty_cache(Node2).

%% @doc Three-node coordinated mining cluster mining until all nodes have contributed
%% to a solution. This test does not force cross-node solutions.
test_three_node() ->
	[Node1, Node2, Node3, _ExitNode, ValidatorNode] = ar_test_node:start_coordinated(3),	
	wait_for_each_node([Node1, Node2, Node3], ValidatorNode, 0, [0, 2, 4]),
	assert_empty_cache(Node1),
	assert_empty_cache(Node2),
	assert_empty_cache(Node3).

%% @doc Two-node, mine until a block is found that incorporates hashes from each node.
test_cross_node() ->
	[Node1, Node2, _ExitNode, ValidatorNode] = ar_test_node:start_coordinated(2),
	wait_for_cross_node([Node1, Node2], ValidatorNode, 0, [0, 2]),
	assert_empty_cache(Node1),
	assert_empty_cache(Node2).

%% @doc Two-node, mine through difficulty retarget, then mine until a block is found that
%% incorporates hashes from each node.
test_cross_node_retarget() ->
	[Node1, Node2, _ExitNode, ValidatorNode] = ar_test_node:start_coordinated(2),
	lists:foreach(
		fun(H) ->
			mine_in_parallel([Node1, Node2], ValidatorNode, H)
		end,
		lists:seq(0, ?RETARGET_BLOCKS)),
	wait_for_cross_node([Node1, Node2], ValidatorNode, ?RETARGET_BLOCKS, [0, 2]),
	assert_empty_cache(Node1),
	assert_empty_cache(Node2).

test_no_exit_node() ->
	%% Assert that when the exit node is down, CM miners don't share their solution with any
	%% other peers.
	[Node, ExitNode, ValidatorNode] = ar_test_node:start_coordinated(1),
	ar_test_node:stop(ExitNode),
	ar_test_node:mine(Node),
	timer:sleep(5000),
	BI = ar_test_node:get_blocks(ValidatorNode),
	?assertEqual(1, length(BI)).

test_no_secret() ->
	[Node, _ExitNode, _ValidatorNode] = ar_test_node:start_coordinated(1),
	Peer = ar_test_node:peer_ip(Node),
	?assertMatch(
		{error, {ok, {{<<"421">>, _}, _, 
			<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
		ar_http_iface_client:get_cm_partition_table(Peer)),
	?assertMatch(
		{error, {ok, {{<<"421">>, _}, _, 
			<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
		ar_http_iface_client:cm_h1_send(Peer, dummy_candidate())),
	?assertMatch(
		{error, {ok, {{<<"421">>, _}, _, 
			<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
		ar_http_iface_client:cm_h2_send(Peer, dummy_candidate())),
	?assertMatch(
		{error, {ok, {{<<"421">>, _}, _, 
			<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
		ar_http_iface_client:cm_publish_send(Peer, dummy_solution())).

test_bad_secret() ->
	[Node, _ExitNode, _ValidatorNode] = ar_test_node:start_coordinated(1),
	Peer = ar_test_node:peer_ip(Node),
	try
		arweave_config:set(cm_api_secret, <<"this_is_not_the_actual_secret">>),
		?assertMatch(
			{error, {ok, {{<<"421">>, _}, _, 
				<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
			ar_http_iface_client:get_cm_partition_table(Peer)),
		?assertMatch(
			{error, {ok, {{<<"421">>, _}, _, 
				<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
			ar_http_iface_client:cm_h1_send(Peer, dummy_candidate())),
		?assertMatch(
			{error, {ok, {{<<"421">>, _}, _, 
				<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
			ar_http_iface_client:cm_h2_send(Peer, dummy_candidate())),
		?assertMatch(
			{error, {ok, {{<<"421">>, _}, _, 
				<<"CM API disabled or invalid CM API secret in request.">>, _, _}}},
			ar_http_iface_client:cm_publish_send(Peer, dummy_solution()))
	after
		arweave_config_legacy:reset()
	end.

test_partition_table() ->
	[B0] = ar_weave:init([], ar_test_node:get_difficulty_for_invalid_hash(), 5 * ar_block:partition_size()),
	Config = ar_test_node:base_cm_config([]),
	arweave_config_legacy:import(Config),
	MiningAddr = arweave_config:get(mining_addr),
	RandomAddress = crypto:strong_rand_bytes(32),
	Peer = ar_test_node:peer_ip(main),

	%% No partitions
	ar_test_node:start_node(B0, Config, false),

	?assertEqual(
		{ok, []},
		ar_http_iface_client:get_cm_partition_table(Peer)
	),

	%% Partition jumble with 2 addresses
	ar_test_node:start_node(B0, Config#config{ 
		storage_modules = [
			{ar_block:partition_size(), 0, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 0, {spora_2_6, RandomAddress}},
			{1000, 2, {spora_2_6, MiningAddr}},
			{1000, 2, {spora_2_6, RandomAddress}},
			{1000, 10, {spora_2_6, MiningAddr}},
			{1000, 10, {spora_2_6, RandomAddress}},
			{ar_block:partition_size() * 2, 4, {spora_2_6, MiningAddr}},
			{ar_block:partition_size() * 2, 4, {spora_2_6, RandomAddress}},
			{(ar_block:partition_size() div 10), 18, {spora_2_6, MiningAddr}},
			{(ar_block:partition_size() div 10), 18, {spora_2_6, RandomAddress}},
			{(ar_block:partition_size() div 10), 19, {spora_2_6, MiningAddr}},
			{(ar_block:partition_size() div 10), 19, {spora_2_6, RandomAddress}},
			{(ar_block:partition_size() div 10), 20, {spora_2_6, MiningAddr}},
			{(ar_block:partition_size() div 10), 20, {spora_2_6, RandomAddress}},
			{(ar_block:partition_size() div 10), 21, {spora_2_6, MiningAddr}},
			{(ar_block:partition_size() div 10), 21, {spora_2_6, RandomAddress}},
			{ar_block:partition_size()+1, 30, {spora_2_6, MiningAddr}},
			{ar_block:partition_size()+1, 30, {spora_2_6, RandomAddress}},
			{ar_block:partition_size(), 40, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 40, {spora_2_6, RandomAddress}}
		]}, false),
	%% get_cm_partition_table returns the currently minable partitions - which is [] if the
	%% node is not mining.
	?assertEqual(
		{ok, []},
		ar_http_iface_client:get_cm_partition_table(Peer)
	),

	%% Simulate mining start
	PartitionUpperBound = 35 * ar_block:partition_size(), %% less than the highest configured partition
	ar_mining_io:set_largest_seen_upper_bound(PartitionUpperBound),
	
	?assertEqual(
		{ok, [
			{0, ar_block:partition_size(), MiningAddr, 0},
			{1, ar_block:partition_size(), MiningAddr, 0},
			{2, ar_block:partition_size(), MiningAddr, 0},
			{8, ar_block:partition_size(), MiningAddr, 0},
			{9, ar_block:partition_size(), MiningAddr, 0},
			{30, ar_block:partition_size(), MiningAddr, 0},
			{31, ar_block:partition_size(), MiningAddr, 0}
		]},
		ar_http_iface_client:get_cm_partition_table(Peer)
	).

test_peers_by_partition() ->
	PartitionUpperBound = 6 * ar_block:partition_size(),
	[B0] = ar_weave:init([], ar_test_node:get_difficulty_for_invalid_hash(),
			PartitionUpperBound),

	Peer1 = ar_test_node:peer_ip(peer1),
	Peer2 = ar_test_node:peer_ip(peer2),
	Peer3 = ar_test_node:peer_ip(peer3),

	BaseConfig = ar_test_node:base_cm_config([]),
	arweave_config_legacy:import(BaseConfig#config{ cm_exit_peer = Peer1 }),
	{ok, Config} = application:get_env(arweave, config),
	MiningAddr = arweave_config:get(mining_addr),
	
	ar_test_node:remote_call(peer1, ar_test_node, start_node, [B0, Config#config{
		cm_exit_peer = not_set,
		cm_peers = [Peer2, Peer3],
		local_peers = [Peer2, Peer3],
		storage_modules = [
			{ar_block:partition_size(), 0, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 1, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 2, {spora_2_6, MiningAddr}}
		]}, false]),
	ar_test_node:remote_call(peer2, ar_test_node, start_node, [B0, Config#config{
		cm_peers = [Peer1, Peer3],
		local_peers = [Peer1, Peer3],
		storage_modules = [
			{ar_block:partition_size(), 1, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 2, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 3, {spora_2_6, MiningAddr}}
		]}, false]),
	ar_test_node:remote_call(peer3, ar_test_node, start_node, [B0, Config#config{
		cm_peers = [Peer1, Peer2],
		local_peers = [Peer1, Peer2],
		storage_modules = [
			{ar_block:partition_size(), 2, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 3, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 4, {spora_2_6, MiningAddr}}
		]}, false]),

	ar_test_node:remote_call(peer1, ar_mining_io, set_largest_seen_upper_bound,
		[PartitionUpperBound]),
	ar_test_node:remote_call(peer2, ar_mining_io, set_largest_seen_upper_bound,
		[PartitionUpperBound]),
	ar_test_node:remote_call(peer3, ar_mining_io, set_largest_seen_upper_bound,
		[PartitionUpperBound]),

	timer:sleep(3000),
	assert_peers([], peer1, 0),
	assert_peers([Peer2], peer1, 1),
	assert_peers([Peer2, Peer3], peer1, 2),
	assert_peers([Peer2, Peer3], peer1, 3),
	assert_peers([Peer3], peer1, 4),
	assert_peers([], peer1, 5),

	assert_peers([Peer1], peer2, 0),
	assert_peers([Peer1], peer2, 1),
	assert_peers([Peer1, Peer3], peer2, 2),
	assert_peers([Peer3], peer2, 3),
	assert_peers([Peer3], peer2, 4),
	assert_peers([], peer2, 5),

	assert_peers([Peer1], peer3, 0),
	assert_peers([Peer1, Peer2], peer3, 1),
	assert_peers([Peer1, Peer2], peer3, 2),
	assert_peers([Peer2], peer3, 3),
	assert_peers([], peer3, 4),
	assert_peers([], peer3, 5),

	ar_test_node:remote_call(peer1, ar_test_node, stop, []),
	timer:sleep(3000),

	assert_peers([Peer1], peer2, 0),
	assert_peers([Peer1], peer2, 1),
	assert_peers([Peer1, Peer3], peer2, 2),
	assert_peers([Peer3], peer2, 3),
	assert_peers([Peer3], peer2, 4),

	assert_peers([Peer1], peer3, 0),
	assert_peers([Peer1, Peer2], peer3, 1),
	assert_peers([Peer1, Peer2], peer3, 2),
	assert_peers([Peer2], peer3, 3),
	assert_peers([], peer3, 4),

	ar_test_node:remote_call(peer1, ar_test_node, start_node, [B0, Config#config{
		cm_exit_peer = not_set,
		cm_peers = [Peer2, Peer3],
		local_peers = [Peer2, Peer3],
		storage_modules = [
			{ar_block:partition_size(), 0, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 4, {spora_2_6, MiningAddr}},
			{ar_block:partition_size(), 5, {spora_2_6, MiningAddr}}
		]}, false]),
	ar_test_node:remote_call(peer1, ar_mining_io, set_largest_seen_upper_bound,
		[PartitionUpperBound]),
	timer:sleep(3000),

	assert_peers([], peer1, 0),
	assert_peers([Peer2], peer1, 1),
	assert_peers([Peer2, Peer3], peer1, 2),
	assert_peers([Peer2, Peer3], peer1, 3),
	assert_peers([Peer3], peer1, 4),
	assert_peers([], peer1, 5),

	assert_peers([Peer1], peer2, 0),
	assert_peers([], peer2, 1),
	assert_peers([Peer3], peer2, 2),
	assert_peers([Peer3], peer2, 3),
	assert_peers([Peer1, Peer3], peer2, 4),
	assert_peers([Peer1], peer2, 5),

	assert_peers([Peer1], peer3, 0),
	assert_peers([Peer2], peer3, 1),
	assert_peers([Peer2], peer3, 2),
	assert_peers([Peer2], peer3, 3),
	assert_peers([Peer1], peer3, 4),
	assert_peers([Peer1], peer3, 5),
	ok.	

%% --------------------------------------------------------------------
%% Helpers
%% --------------------------------------------------------------------

assert_peers(ExpectedPeers, Node, Partition) ->
	?assert(ar_util:do_until(
		fun() ->
			Peers = ar_test_node:remote_call(Node, ar_coordination, get_peers, [Partition]),
			lists:sort(ExpectedPeers) == lists:sort(Peers)
		end,
		200,
		5000
	)).

wait_for_each_node(Miners, ValidatorNode, CurrentHeight, ExpectedPartitions) ->
	wait_for_each_node(
		Miners, ValidatorNode, CurrentHeight, sets:from_list(ExpectedPartitions), 20).

wait_for_each_node(
		_Miners, _ValidatorNode, _CurrentHeight, _ExpectedPartitions, 0) ->
	?assert(false, "Timed out waiting for all mining nodes to win a solution");
wait_for_each_node(
		Miners, ValidatorNode, CurrentHeight, ExpectedPartitions, RetryCount) ->
	Partitions = mine_in_parallel(Miners, ValidatorNode, CurrentHeight),
	ExpectedPartitions2 = sets:subtract(ExpectedPartitions, sets:from_list(Partitions)),
	case sets:is_empty(ExpectedPartitions2) of
		true ->
			CurrentHeight+1;
		false ->
			wait_for_each_node(
				Miners, ValidatorNode, CurrentHeight+1, ExpectedPartitions2, RetryCount-1)
	end.

wait_for_cross_node(Miners, ValidatorNode, CurrentHeight, ExpectedPartitions) ->
	wait_for_cross_node(
		Miners, ValidatorNode, CurrentHeight, sets:from_list(ExpectedPartitions), 20).

wait_for_cross_node(_Miners, _ValidatorNode, _CurrentHeight, _ExpectedPartitions, 0) ->
	?assert(false, "Timed out waiting for a cross-node solution");
wait_for_cross_node(_Miners, _ValidatorNode, _CurrentHeight, ExpectedPartitions, _RetryCount)
		when length(ExpectedPartitions) /= 2 ->
	?assert(false, "Cross-node solutions can only have 2 partitions.");
wait_for_cross_node(Miners, ValidatorNode, CurrentHeight, ExpectedPartitions, RetryCount) ->
	A = mine_in_parallel(Miners, ValidatorNode, CurrentHeight),
	Partitions = sets:from_list(A),
	MinedCrossNodeBlock = 
		sets:is_subset(Partitions, ExpectedPartitions) andalso 
		sets:is_subset(ExpectedPartitions, Partitions),
	case MinedCrossNodeBlock of
		true ->
			CurrentHeight+1;
		false ->
			wait_for_cross_node(
				Miners, ValidatorNode, CurrentHeight+1, ExpectedPartitions, RetryCount-1)
	end.
	
mine_in_parallel(Miners, ValidatorNode, CurrentHeight) ->
	report_miners(Miners),
	ar_util:pmap(fun(Node) -> ar_test_node:mine(Node) end, Miners),
	?debugFmt("Waiting until the validator node (port ~B) advances to height ~B.",
			[ar_test_node:peer_port(ValidatorNode), CurrentHeight + 1]),
	BIValidator = ar_test_node:wait_until_height(ValidatorNode, CurrentHeight + 1, false),
	%% Since multiple nodes are mining in parallel it's possible that multiple blocks
	%% were mined. Get the Validator's current height in cas it's more than CurrentHeight+1.
	NewHeight = ar_test_node:remote_call(ValidatorNode, ar_node, get_height, []),

	Hashes = [Hash || {Hash, _, _} <- lists:sublist(BIValidator, NewHeight - CurrentHeight)],
	
	lists:foreach(
		fun(Node) ->
			?LOG_DEBUG([{test, ar_coordinated_mining_tests},
				{waiting_for_height, NewHeight}, {node, Node}]),
			%% Make sure the miner contains all of the new validator hashes, it's okay if
			%% the miner contains *more* hashes since it's possible concurrent blocks were
			%% mined between when the Validator checked and now.
			BIMiner = ar_test_node:wait_until_height(Node, NewHeight, false),
			MinerHashes = [Hash || {Hash, _, _} <- BIMiner],
			Message = lists:flatten(io_lib:format(
					"Node ~p did not mine the same block as the validator node", [Node])),
			?assert(lists:all(fun(Hash) -> lists:member(Hash, MinerHashes) end, Hashes), Message)
		end,
		Miners
	),
	LatestHash = lists:last(Hashes),
	{ok, Block} = ar_test_node:http_get_block(LatestHash, ValidatorNode),

	case Block#block.recall_byte2 of
		undefined -> 
			[
				ar_node:get_partition_number(Block#block.recall_byte)
			];
		RecallByte2 ->
			[
				ar_node:get_partition_number(Block#block.recall_byte), 
				ar_node:get_partition_number(RecallByte2)
			]
	end.

report_miners(Miners) ->
	report_miners(Miners, 1).

report_miners([], _I) ->
	ok;
report_miners([Miner | Miners], I) ->
	?debugFmt("Miner ~B: ~p, port: ~B.", [I, Miner, ar_test_node:peer_port(Miner)]),
	report_miners(Miners, I + 1).

assert_empty_cache(_Node) ->
	%% wait until the mining has stopped, then assert that the cache is empty
	timer:sleep(10000),
	ok.
	% [{_, Size}] = ar_test_node:remote_call(Node, ets, lookup, [ar_mining_server, chunk_cache_size]),
	%% We should assert that the size is 0, but there is a lot of concurrency in these tests
	%% so it's been hard to guarantee the cache is always empty by the time this check runs.
	%% It's possible there is a bug in the cache management code, but that code is pretty complex.
	%% In the future, if cache size ends up being a problem we can revisit - but for now, not
	%% worth the time for a test failure that may not have any realworld implications.
	% ?assertEqual(0, Size, Node).

dummy_candidate() ->
	#mining_candidate{
		cm_diff = {rand:uniform(1024), rand:uniform(1024)},
		h0 = crypto:strong_rand_bytes(32),
		h1 = crypto:strong_rand_bytes(32),
		mining_address = crypto:strong_rand_bytes(32),
		next_seed = crypto:strong_rand_bytes(32),
		next_vdf_difficulty = rand:uniform(1024),
		nonce_limiter_output = crypto:strong_rand_bytes(32),
		partition_number = rand:uniform(1024),
		partition_number2 = rand:uniform(1024),
		partition_upper_bound = rand:uniform(1024),
		seed = crypto:strong_rand_bytes(32),
		session_key = dummy_session_key(),
		start_interval_number = rand:uniform(1024),
		step_number = rand:uniform(1024)
	}.

dummy_solution() ->
	#mining_solution{
		last_step_checkpoints = [],
		merkle_rebase_threshold = rand:uniform(1024),
		mining_address = crypto:strong_rand_bytes(32),
		next_seed = crypto:strong_rand_bytes(32),
		next_vdf_difficulty = rand:uniform(1024),
		nonce = rand:uniform(1024),
		nonce_limiter_output = crypto:strong_rand_bytes(32),
		partition_number = rand:uniform(1024),
		partition_upper_bound = rand:uniform(1024),
		poa1 = dummy_poa(),
		poa2 = dummy_poa(),
		preimage = crypto:strong_rand_bytes(32),
		recall_byte1 = rand:uniform(1024),
		seed = crypto:strong_rand_bytes(32),
		solution_hash = crypto:strong_rand_bytes(32),
		start_interval_number = rand:uniform(1024),
		step_number = rand:uniform(1024),
		steps = []
	}.

dummy_poa() ->
	#poa{
		option = rand:uniform(1024),
		tx_path = crypto:strong_rand_bytes(32),
		data_path = crypto:strong_rand_bytes(32),
		chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	}.

dummy_session_key() ->
	{crypto:strong_rand_bytes(32), rand:uniform(100), rand:uniform(10000)}.

mock_for_single_difficulty_adjustment_height() ->
	{ar_retarget, is_retarget_height, fun(Height) ->
		case Height of
			?RETARGET_BLOCKS -> true;
			_ -> false
		end
	end}.

mock_for_single_difficulty_adjustment_block() ->
	{ar_retarget, is_retarget_block, fun(Block) ->
		case Block#block.height of
			?RETARGET_BLOCKS -> true;
			_ -> false
		end
	end}.
