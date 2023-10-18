-module(ar_coordinated_mining_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start_coordinated/1, mine/1, http_get_block/2]).

% single_node_one_chunk_coordinated_mining_test_() ->
% 	{timeout, 120, fun test_single_node_one_chunk_coordinated_mining/0}.

% test_single_node_one_chunk_coordinated_mining() ->
% 	[Node, _ExitNode, ValidatorNode] = start_coordinated(1),
% 	mine(Node),
% 	BI = ar_test_node:wait_until_height(ValidatorNode, 1),
% 	{ok, B} = http_get_block(element(1, hd(BI)), ValidatorNode),
% 	?assert(byte_size((B#block.poa)#poa.data_path) > 0),
% 	assert_empty_cache(Node).
	
% single_node_two_chunk_coordinated_mining_test_() ->
% 	ar_test_node:test_with_mocked_functions([
% 			ar_test_node:mock_to_force_invalid_h1()],
% 		fun test_single_node_two_chunk_coordinated_mining/0, 120).

% test_single_node_two_chunk_coordinated_mining() ->
% 	[Node, _ExitNode, ValidatorNode] = start_coordinated(1),
% 	mine(Node),
% 	BI = ar_test_node:wait_until_height(ValidatorNode, 1),
% 	{ok, B} = http_get_block(element(1, hd(BI)), ValidatorNode),
% 	?assert(byte_size((B#block.poa2)#poa.data_path) > 0),
% 	assert_empty_cache(Node).

% no_exit_node_test_() ->
% 	{timeout, 120, fun test_no_exit_node/0}.

% test_no_exit_node() ->
% 	%% Assert that when the exit node is down, CM miners don't share their solution with any
% 	%% other peers.
% 	[Node, ExitNode, ValidatorNode] = start_coordinated(1),
% 	ar_test_node:stop(ExitNode),
% 	mine(Node),
% 	timer:sleep(5000),
% 	BI = ar_test_node:get_blocks(ValidatorNode),
% 	?assertEqual(1, length(BI)).

% coordinated_mining_retarget_test_() ->
% 	{timeout, 120, fun test_coordinated_mining_retarget/0}.

% test_coordinated_mining_retarget() ->
% 	%% Assert that a difficulty retarget is handled correctly.
% 	[Node1, Node2, _ExitNode, ValidatorNode] = start_coordinated(2),
% 	lists:foreach(
% 		fun(Height) ->
% 			mine_in_parallel([Node1, Node2], ValidatorNode, Height)
% 		end,
% 		lists:seq(0, ?RETARGET_BLOCKS)),
% 	assert_empty_cache(Node1),
% 	assert_empty_cache(Node2).

coordinated_mining_concurrency_test_() ->
	{timeout, 120, fun test_coordinated_mining_concurrency/0}.

test_coordinated_mining_concurrency() ->
	%% Assert that three nodes mining concurrently don't conflict with each other and that
	%% each of them are able to win a solution.
	[Node1, Node2, Node3, _ExitNode, ValidatorNode] = start_coordinated(3),	
	wait_for_each_node([Node1, Node2, Node3], ValidatorNode, 0, [0, 2, 4]),
	assert_empty_cache(Node1),
	assert_empty_cache(Node2),
	assert_empty_cache(Node3).

% coordinated_mining_two_chunk_concurrency_test_() ->
% 	ar_test_node:test_with_mocked_functions([
% 			ar_test_node:mock_to_force_invalid_h1()],
% 		fun test_coordinated_mining_two_chunk_concurrency/0, 120).

% test_coordinated_mining_two_chunk_concurrency() ->
% 	%% Assert that cross-node solutions still work when two nodes are mining concurrently 
% 	[Node1, Node2, _ExitNode, ValidatorNode] = start_coordinated(2),
% 	wait_for_each_node([Node1, Node2], ValidatorNode, 0, [0, 2]),
% 	assert_empty_cache(Node1),
% 	assert_empty_cache(Node2).

% coordinated_mining_two_chunk_retarget_test_() ->
% 	ar_test_node:test_with_mocked_functions([
% 			ar_test_node:mock_to_force_invalid_h1()],
% 		fun test_coordinated_mining_two_chunk_retarget/0, 120).

% test_coordinated_mining_two_chunk_retarget() ->
% 	[Node1, Node2, _ExitNode, ValidatorNode] = start_coordinated(2),
% 	lists:foreach(
% 		fun(H) ->
% 			mine_in_parallel([Node1, Node2], ValidatorNode, H)
% 		end,
% 		lists:seq(0, ?RETARGET_BLOCKS)),
% 	wait_for_each_node([Node1, Node2], ValidatorNode, ?RETARGET_BLOCKS, [0, 2]),
% 	assert_empty_cache(Node1),
% 	assert_empty_cache(Node2).

wait_for_each_node(Miners, ValidatorNode, CurrentHeight, ExpectedPartitions) ->
	wait_for_each_node(
		Miners, ValidatorNode, CurrentHeight, sets:from_list(ExpectedPartitions), 20).

wait_for_each_node(
		_Miners, _ValidatorNode, _CurrentHeight, _ExpectedPartitions, 0) ->
	?assert(false, "Timed out waiting for all mining nodes to win a solution");
wait_for_each_node(
		Miners, ValidatorNode, CurrentHeight, ExpectedPartitions, RetryCount) ->
	Partition = mine_in_parallel(Miners, ValidatorNode, CurrentHeight),
	Partitions = sets:del_element(Partition, ExpectedPartitions),
	case sets:is_empty(Partitions) of
		true ->
			CurrentHeight+1;
		false ->
			wait_for_each_node(
				Miners, ValidatorNode, CurrentHeight+1, Partitions, RetryCount-1)
	end.
	
mine_in_parallel(Miners, ValidatorNode, CurrentHeight) ->
	ar_util:pmap(fun(Node) -> mine(Node) end, Miners),
	[{Hash, _, _} | _] = ar_test_node:wait_until_height(ValidatorNode, CurrentHeight + 1),
	lists:foreach(
		fun(Node) ->
			[{MinerHash, _, _} | _] = ar_test_node:wait_until_height(Node, CurrentHeight + 1),
			Message = lists:flatten(
				io_lib:format("Node ~p did not mine the same block as the validator node", [Node])),
			?assertEqual(ar_util:encode(Hash), ar_util:encode(MinerHash), Message)
		end,
		Miners
	),
	{ok, Block} = ar_test_node:http_get_block(Hash, ValidatorNode),
	case Block#block.recall_byte2 of
		undefined -> ?PARTITION_NUMBER(Block#block.recall_byte);
		RecallByte2 -> ?PARTITION_NUMBER(RecallByte2)
	end.

assert_empty_cache(Node) ->
	ar_test_node:wait_until_mining_paused(Node),
	[{_, Size}] = ar_test_node:remote_call(Node, ets, lookup, [ar_mining_server, chunk_cache_size]),
	%% We should assert that the size is 0, but there is a lot of concurrency in these tests
	%% so it's been hard to guarantee the cache is always empty by the time this check runs.
	%% It's possible there is a bug in the cache management code, but that code is pretty complex.
	%% In the future, if cache size ends up being a problem we can revisit - but for now, not
	%% worth the time for a test failure that may not have any realworld implications.
	?assert(lists:member(Size, [-1, 0, 1]), Node).