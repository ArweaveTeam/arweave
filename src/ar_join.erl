-module(ar_join).
-export([start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles creating an initial, minimal
%%% block list to be used by a node joining a network already in progress.

%% Process state.
-record(state, {
	parent,
	target,
	blocks = [],
	peers = []
}).

%% @doc Start a process that will attempt to join a network from the last
%% sync block.
start(Node, Peers, Height) ->
	ar:report(
		[
			{node, Node},
			{peers, Peers},
			{height, Height}
		]
	),
	spawn(
		fun() ->
			server(
				#state {
					parent = Node,
					peers = Peers,
					target = Height
				}
			)
		end
	).

%% @doc Attempt to catch-up and validate the blocks from the last sync block.
server(S = #state { peers = [Peer|_], blocks = [], target = Height }) ->
	LastB =
		ar_node:get_block(Peer, (BNum = calculate_last_sync_block(Height)) - 1),
	B = ar_node:get_block(Peer, BNum),
	RecallB = ar_node:get_block(Peer, ar_weave:calculate_recall_block(B)),
	case ar_node:validate(B, LastB, RecallB) of
		false ->
			ar:report([couldnt_validate_last_sync_block]),
			giving_up;
		true ->
			server(
				S#state {
					blocks = [B] ++
						if LastB == RecallB -> [LastB];
						true -> [LastB, RecallB]
						end
					}
			)
	end;
server(
		S = #state {
			peers = [Peer|_],
			blocks = Bs = [LastB|_],
			parent = Node,
			target = Height
		}) ->
	case LastB#block.height of
		Height -> Node ! {fork_recovered, Bs};
		_ ->
			B = ar_node:get_block(Peer, LastB#block.height + 1),
			RecallB = ar_node:get_block(Peer, ar_weave:calculate_recall_block(LastB)),
			case ar_node:validate(B, LastB, RecallB) of
				false ->
					ar:report_console([couldnt_validate_catchup_block]),
					giving_up;
				true ->
					server(S#state { blocks = [B|Bs] })
			end
	end.

%% @doc Find the last block with a complete block hash and wallet list.
calculate_last_sync_block(Height) ->
	%% TODO: Calcualte this from sync block frequency.
	Height.

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
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
