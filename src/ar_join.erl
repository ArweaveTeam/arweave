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
	ar:d(starting_join_server),
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
			ar:report_console([couldnt_validate_last_sync_block]),
			giving_up;
		true ->
			server(
				S#state {
					blocks =
						if LastB == RecallB -> [LastB];
						true -> [RecallB, LastB]
						end ++ [B]
					}
			)
	end;
server(
		S = #state {
			peers = [Peer|_],
			blocks = Bs,
			parent = Node,
			target = Height
		}) ->
	case ar:d((hd(lists:reverse(Bs)))#block.height) of
		Height -> Node ! {fork_recovered, Bs};
		_ ->
			LastB = lists:last(Bs),
			B = ar_node:get_block(Peer, LastB#block.height + 1),
			RecallB = ar_node:get_block(Peer, ar_weave:calculate_recall_block(B)),
			case ar_node:validate(B, LastB, RecallB) of
				false ->
					ar:report_console([couldnt_validate_catchup_block]),
					giving_up;
				true ->
					server(S#state { blocks = Bs ++ [B] })
			end
	end.

%% @doc Find the last block with a complete block hash and wallet list.
calculate_last_sync_block(Height) ->
	%% TODO: Calcualte this from sync block frequency.
	Height - 1.

%% @doc Check that nodes can join a running network by using the fork recoverer.
node_join_test() ->
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
	5 = B#block.height.
