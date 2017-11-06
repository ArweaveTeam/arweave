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
			joining_network,
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
server(S = #state { peers = Peers, blocks = [], target = Height }) ->
	LastB =
		case ar_node:get_block(Peers, (BNum = calculate_last_sync_block(Height)) - 1) of
			LastBs when is_list(LastBs) -> hd(LastBs);
			X -> X
		end,
	%% TODO: Ensure that this only hits the peer that started the join process.
	B =
		case ar_node:get_block(Peers, BNum) of
			[HdB|_] when is_record(HdB, block) -> HdB;
			unavailable -> throw(join_block_not_available_from_any_peers);
			XB -> XB
		end,
	ar:d([{block, B}, {peer, hd(Peers)}]),
	RecallBs = ar_node:get_block(Peers, ar_weave:calculate_recall_block(B)),
	case
		ar_fork_recovery:try_apply_blocks(
			B,
			get_hash_list(B),
			LastB,
			RecallBs
		)
	of
		false ->
			ar:report([couldnt_validate_last_sync_block]),
			giving_up;
		B -> server(S#state { blocks = [B, LastB] })
	end;
server(
		S = #state {
			peers = Peers,
			blocks = Bs = [LastB|_],
			parent = Node,
			target = Height
		}) ->
	case LastB#block.height of
		Height -> Node ! {fork_recovered, Bs};
		_ ->
			Bs = ar_node:get_block(Peers, LastB#block.height + 1),
			RecallBs = ar_node:get_block(Peers, ar_weave:calculate_recall_block(LastB)),
			case
				ar_fork_recovery:try_apply_blocks(
					Bs,
					get_hash_list(Bs),
					LastB,
					RecallBs
				)
			of
				false ->
					ar:report_console([couldnt_validate_catchup_block]),
					giving_up;
				NextB ->
					server(S#state { blocks = [NextB|Bs] })
			end
	end.

%% @doc From the return types of get_block, retreive a hash_list
get_hash_list(unavailable) -> unavailable;
get_hash_list(B) when is_record(B, block) -> B#block.hash_list;
get_hash_list([B|_Bs]) -> B#block.hash_list.

%% @doc Find the last block with a complete block hash and wallet list.
calculate_last_sync_block(Height) ->
	%% TODO: Calcualte this from sync block frequency.
	Height.

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
