-module(ar_join).
-export([start/2, start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles creating an initial, minimal
%%% block list to be used by a node joining a network already in progress.

%% @doc Start a process that will attempt to join a network from the last
%% sync block.
start(Peers, NewB) ->
	start(self(), Peers, NewB).
start(Node, Peers, NewB) ->
	ar:report(
		[
			joining_network,
			{node, Node},
			{peers, Peers},
			{height, NewB#block.height}
		]
	),
	spawn(
		fun() ->
			ar_storage:write_block(NewB),
			Node ! {fork_recovered, [NewB#block.hash|NewB#block.hash_list]}
		end
	).

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([], _B0 = ar_weave:init()),
	Node2 = ar_node:start([Node1], undefined),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 600 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	2 = B#block.height.

%% @doc Ensure that both nodes can mine after a join.
node_join_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([], _B0 = ar_weave:init()),
	Node2 = ar_node:start([Node1], undefined),
	ar_node:add_peers(Node1, Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 600 -> ok end,
	ar_node:mine(Node2),
	receive after 600 -> ok end,
	[B|_] = ar_node:get_blocks(Node1),
	3 = B#block.height.
