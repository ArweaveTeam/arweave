
-module(ar_fork_recovery).
-export([start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An asynchronous process that asks another node on a different fork
%%% for all of the blocks required to 'catch up' with the network,
%%% verifying each in turn. Once the blocks since divergence have been
%%% verified, the process returns the new state to its parent. Target is
%%% height at which block height ~ought~ to be. Hash lists is forked.

%% Defines the server state
-record(state, {
	parent,
	peers,
	target_block,
	blocks,
	hash_list
}).

%% @doc Start the 'catch up' server.
start(Peers, TargetB, HashList) ->
	Parent = self(),
	spawn(
		fun() ->
			ar:report(
				[
					{started_fork_recovery_proc, self()},
					{target_height, TargetB#block.height},
					{peer, Peers},
					{hash_list, HashList}
				]
			),
			server(
				#state {
					parent = Parent,
					peers = Peers,
					target_block = TargetB,
					blocks = [],
					hash_list =
						drop_until_diverge(
							lists:reverse(TargetB#block.hash_list),
							lists:reverse(HashList)
						)
				}
			)
		end
	).

%% @doc Take two lists, drop elements until they do not match.
%% Return the remainder of the _first_ list.
%% TODO: Verify that this drops to the correct spot!
drop_until_diverge([X|R1], [X|R2]) -> drop_until_diverge(R1, R2);
drop_until_diverge(R1, _) -> R1.

%% @doc Main server loop.
server(
	#state {
		parent = Parent,
		target_block = TargetB,
		blocks = Blocks = [B|_]
	}) when TargetB == B ->
	% The fork has been recovered write the blocks to disk
	% and return the new hash list.
	ar_storage:write_block(Blocks),
	Parent ! {fork_recovered, [B#block.indep_hash|B#block.hash_list]};
server(S = #state { blocks = [], peers = Peers, hash_list = [LastH|Rest] }) ->
	% Get the first block in fork.
	server(
		S#state {
			blocks = [ ar_node:get_block(Peers, LastH) ],
			hash_list = Rest
		}
	);
server(S = #state { blocks = Blocks = [B|_], peers = Peers, hash_list = [NextH|HashList] }) ->
	% Get and verify the next block.
	NextB = ar_node:get_block(Peers, NextH),
	RecallB =
		ar_node:get_block(
			Peers,
			ar_util:get_recall_hash(NextB, NextB#block.hash_list)
		),
	case try_apply_block(HashList, NextB, B, RecallB) of
		false ->
			ar:report_console([could_not_validate_recovery_block]),
			ok;
		NextB ->
			server(S#state { blocks = [NextB|Blocks] })
	end.

try_apply_block(HashList, NextB, B, RecallB) ->
	ar_node:validate(HashList,
		ar_node:apply_txs(B#block.wallet_list, NextB#block.txs),
		NextB,
		B,
		RecallB
	).

%%% Tests

%% @doc Ensure forks that are one block behind will resolve.
single_block_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init(), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 2000 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 2000 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	5 = B#block.height.

%% @doc Ensure that nodes on a fork that is far behind will catchup correctly.
multiple_blocks_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init(), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	8 = B#block.height.

%% @doc Ensure that nodes that have diverged by multiple blocks each can reconcile.
multiple_blocks_since_fork_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init(), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	7 = B#block.height.
