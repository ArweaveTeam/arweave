-module(ar_fork_recovery).
-export([start/4]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An asynchronous process that asks another node on a different fork
%%% for all of the blocks required to 'catch up' with the network,
%%% verifying each in turn. Once the blocks since divergence have been
%%% verified, the process returns the new state to its parent.

%% Defines the server state
-record(state, {
	parent,
	peer,
	target,
	blocks
}).

%% Start the 'catch up' server.
start(Parent, Peer, TargetHeight, BlockList) ->
	spawn(
		fun() ->
			ar:report(
				[
					{started_fork_recovery_proc, self()},
					{target_height, TargetHeight}
				]
			),
			server(
				#state {
					parent = Parent,
					peer = Peer,
					target = TargetHeight,
					blocks = BlockList
				}
			)
		end
	).

%% Main server loop.
server(
	#state {
		parent = Parent,
		target = Target,
		blocks = Bs = [#block { height = Target }|_]
	}) ->
	% The fork has been recovered. Return.
	Parent ! {fork_recovered, Bs};
server(S = #state { blocks = [], peer = Peer }) ->
	% We are starting from scratch -- get the first block, for now.
	% TODO: Update only from last sync block.
	server(
		S#state {
			blocks =
				[
					ar_node:get_block(Peer, 1),
					ar_node:get_block(Peer, 0)
				]
		}
	);
server(S = #state { peer = Peer, blocks = Bs = [B|_] }) ->
	% Get and verify the next block.
	NextB = ar_node:get_block(Peer, B#block.height + 1),
	BHL = B#block.hash_list ++ [B#block.indep_hash],
	case ar_node:validate(
			BHL,
			ar_node:apply_txs(B#block.wallet_list, NextB#block.txs),
			NextB, B, ar_node:find_recall_block(Bs)) of
		false ->
			ar:report([could_not_validate_recovery_block]),
			ok;
		true ->
			server(S#state { blocks = [NextB|Bs] })
	end.

%%% Tests

%% Ensure forks that are one block behind will resolve.
single_block_ahead_recovery_test() ->
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init(), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	5 = B#block.height.

%% Ensure that nodes on a fork that is far behind will catchup correctly.
multiple_blocks_ahead_recovery_test() ->
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init(), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
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

%% Ensure that nodes that have diverged by multiple blocks each can reconcile.
multiple_blocks_since_fork_test() ->
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init(), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
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

%% Check that nodes can join a running network by using the fork recoverer.
node_join_test() ->
	Node1 = ar_node:start([], ar_weave:init()),
	Node2 = ar_node:start(),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 600 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	5 = B#block.height.
