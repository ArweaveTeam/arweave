-module(ar_join).
-export([start/2, start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles creating an initial, minimal
%%% block list to be used by a node joining a network already in progress.

%% @doc Start a process that will attempt to join a network from the last
%% sync block.
start(Peers, NewB) when is_record(NewB, block) ->
	start(self(), Peers, NewB);
start(Node, Peers) ->
	start(Node, Peers, ar_node:get_current_block(Peers)).
start(_Node, _Peers, B) when is_atom(B) ->
	do_nothing;
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
			fill_to_capacity(NewB, Peers),
			Node ! {fork_recovered, [NewB#block.indep_hash|NewB#block.hash_list]}
		end
	).

%% @doc Fills node to capacity based on weave storage limit.
fill_to_capacity(NewB, Peers) ->
	Height = NewB#block.height,
	RandBlock = lists:nth(rand:uniform(Height - 1), NewB#block.hash_list),
	case at_capacity(Height) of
		true ->
			ar_storage:delete_block(
				RandBlock
			),
			ar_storage:write_block(NewB);
		false ->
			ar_storage:write_block(
				ar_node:get_block(
					Peers,
					RandBlock
				)
			),
			fill_to_capacity(NewB, Peers)
		end.

%% @doc Figures out if node is at capacity based on predifined weave storage limit.
at_capacity(Height) ->
	(ar_storage:blocks_on_disk() / Height) > ?WEAVE_STOR_AMT.

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([], _B0 = ar_weave:init([])),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 600 -> ok end,
	Node2 = ar_node:start([Node1]),
	receive after 600 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	2 = (ar_storage:read_block(B))#block.height.

%% @doc Ensure that both nodes can mine after a join.
node_join_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([], _B0 = ar_weave:init([])),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	Node2 = ar_node:start([Node1]),
	receive after 600 -> ok end,
	ar_node:mine(Node2),
	receive after 600 -> ok end,
	[B|_] = ar_node:get_blocks(Node1),
	3 = (ar_storage:read_block(B))#block.height.
