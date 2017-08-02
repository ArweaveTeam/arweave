-module(ar_network).
-export([start/1, start/2]).

%%% Manages virtual networks of gossip nodes.

%% Create a netwokr of Size ar_nodes, with Links connections each.
%% Defaults to creating a fully connected network.
start(Size) -> start(Size, Size).
start(Size, Connections) ->
	B0 = ar_weave:init(),
	Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, Size) ],
	lists:foreach(
		fun(Node) ->
			ar_node:add_peers(
				Node,
				ar_gossip:pick_random_peers(Nodes, Connections)
			)
		end,
		Nodes
	),
	Nodes.
