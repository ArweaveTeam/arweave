-module(ar_network).
-export([start/1, start/2, start/3, start/4, start/5, spawn/1]).
-export([spawn_and_mine/1]).
-export([set_loss_probability/2, set_delay/2, set_mining_delay/2]).
-export([automine/1, add_tx/2]).
-include("ar.hrl").
-compile({no_auto_import, [{spawn,1}]}).

%%% Manages virtual networks of gossip nodes.

%% Default spawned network size.
-define(DEFAULT_SIZE, 5).
%% Default speed of transmission in the network (in bytes/sec).
-define(DEFAULT_XFER_SPEED, 512 * 1024).
%% Calculate MS to wait in order to hit target block time.
-define(DEFAULT_MINING_DELAY,
	((?TARGET_TIME * 1000) div erlang:trunc(math:pow(2, ?DEFAULT_DIFF - 1)))).

%% Create a netwokr of Size ar_nodes, with Links connections each.
%% Defaults to creating a fully connected network.
start(Size) -> start(Size, Size).
start(Size, Connections) -> start(Size, Connections, 0).
start(Size, Connections, LossProb) ->
	start(Size, Connections, LossProb, 0).
start(Size, Connections, LossProb, MaxDelay) ->
	start(Size, Connections, LossProb, MaxDelay, ?DEFAULT_XFER_SPEED).
start(Size, Connections, LossProb, MaxDelay, XferSpeed) ->
	start(Size, Connections, LossProb, MaxDelay, XferSpeed, 0).
start(Size, Connections, LossProb, MaxDelay, XferSpeed, MiningDelay) ->
	B0 = ar_weave:init(),
	Nodes = [ ar_node:start([], B0) || _ <- lists:seq(1, Size) ],
	lists:foreach(
		fun(Node) ->
			ar_node:add_peers(
				Node,
				ar_gossip:pick_random_peers(Nodes, Connections)
			),
			ar_node:set_loss_probability(Node, LossProb),
			ar_node:set_delay(Node, MaxDelay),
			ar_node:set_mining_delay(Node, MiningDelay),
			ar_node:set_xfer_speed(Node, XferSpeed)
		end,
		Nodes
	),
	Nodes.

%% Create a template network.
spawn(realistic) -> spawn({realistic, ?DEFAULT_SIZE});
spawn({realistic, Size}) ->
	start(Size, 3, 0.025, 200, ?DEFAULT_XFER_SPEED, ?DEFAULT_MINING_DELAY * Size);
spawn(hard) -> spawn({hard, ?DEFAULT_SIZE});
spawn({hard, Size}) ->
	start(
		Size,
		2,
		0.1,
		3000,
		?DEFAULT_XFER_SPEED div 10,
		?DEFAULT_MINING_DELAY * Size
	).

%% Create and start an automining network.
spawn_and_mine([Type]) -> spawn_and_mine(Type);
spawn_and_mine([[Type]]) -> spawn_and_mine(Type);
spawn_and_mine(Type) ->
	Net = spawn(Type),
	automine(Net),
	Net.

%% Change the likelihood of experiencing simulated network packet loss
%% for an entire network.
set_loss_probability(Net, Prob) ->
	lists:foreach(
		fun(Node) -> ar_node:set_loss_probability(Node, Prob) end,
		Net
	),
	ok.

%% Change the maximum delay time for a network.
set_delay(Net, MaxDelay) ->
	lists:foreach(
		fun(Node) -> ar_node:set_delay(Node, MaxDelay) end,
		Net
	),
	ok.

%% Set the idle miner delay for each hash.
%% Wait this many MS before performing each individual hash.
set_mining_delay(Net, Delay) ->
	lists:foreach(
		fun(Node) -> ar_node:set_mining_delay(Node, Delay) end,
		Net
	),
	ok.

%% Make every node in a network begin mining (if it can).
automine(Net) ->
	lists:foreach(
		fun(Node) -> ar_node:automine(Node) end,
		Net
	),
	ok.

%% Deliver a TX to a randomly selected node in the network.
add_tx(Net, TX) ->
	ar_node:add_tx(ar_gossip:pick_random(Net), TX).
