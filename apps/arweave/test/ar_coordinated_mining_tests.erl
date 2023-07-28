-module(ar_coordinated_mining_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start_coordinated/1, mine/1, wait_until_height/2, http_get_block/2,
		turn_off_one_chunk_mining/1]).

single_node_coordinated_mining_test_() ->
	{timeout, 120, fun test_single_node_coordinated_mining/0}.

test_single_node_coordinated_mining() ->
	[Node, _ExitNode, ValidatorNode] = start_coordinated(1),
	mine(Node),
	BI = wait_until_height(1, ValidatorNode),
	{ok, #block{}} = http_get_block(element(1, hd(BI)), ValidatorNode),
	turn_off_one_chunk_mining(Node),
	mine(Node),
	BI2 = wait_until_height(2, ValidatorNode),
	{ok, B2} = http_get_block(element(1, hd(BI2)), ValidatorNode),
	?assert(byte_size((B2#block.poa2)#poa.data_path) > 0).

% two_node_coordinated_mining_concurrency_test_() ->
% 	{timeout, 120, fun test_two_node_coordinated_mining_concurrency/0}.

% test_two_node_coordinated_mining_concurrency() ->
% 	%% Assert the two nodes mining in parallel do not conflict.
% 	[Node1, Node2, _ExitNode, ValidatorNode] = start_coordinated(2),
% 	mine_in_parallel(Node1, Node2, ValidatorNode, 0),
% 	mine_in_parallel(Node1, Node2, ValidatorNode, 2),
% 	mine_in_parallel(Node1, Node2, ValidatorNode, 4),
% 	mine_in_parallel(Node1, Node2, ValidatorNode, 6),
% 	mine_in_parallel(Node1, Node2, ValidatorNode, 8).

% mine_in_parallel(Node1, Node2, ValidatorNode, CurrentHeight) ->
% 	ar_util:pmap(fun(Node) -> mine(Node) end, [Node1, Node2]),
% 	wait_until_height(CurrentHeight + 2, ValidatorNode).

% two_node_coordinated_mining_two_chunk_solution_test_() ->
% 	{timeout, 120, fun test_two_node_coordinated_mining_two_chunk_solution/0}.

% test_two_node_coordinated_mining_two_chunk_solution() ->
% 	[Node1, Node2, ExitNode, ValidatorNode] = start_coordinated(2),
% 	turn_off_one_chunk_mining(Node1),
% 	mine(Node1),
% 	BI = wait_until_height(1, ValidatorNode),
% 	BI = wait_until_height(1, ExitNode),
% 	{ok, B} = http_get_block(element(1, hd(BI)), ValidatorNode),
% 	?assertNotEqual(undefined, B#block.recall_byte2),
% 	?assert(byte_size((B#block.poa2)#poa.data_path) > 0),
% 	turn_off_one_chunk_mining(Node2),
% 	mine(Node2),
% 	BI2 = wait_until_height(1, ValidatorNode),
% 	BI2 = wait_until_height(1, ExitNode),
% 	{ok, B2} = http_get_block(element(1, hd(BI2)), ValidatorNode),
% 	?assertNotEqual(undefined, B2#block.recall_byte2),
% 	?assert(byte_size((B2#block.poa2)#poa.data_path) > 0).
