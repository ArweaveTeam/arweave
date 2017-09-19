-module(ar_sim_client).
-export([start/1]).

%%% Represents a simulated Archain user client.
%%% Currently implemented behaviours:
%%%		- Create wallet
%%%		- Sign and add transactions to network

%% The number of peers to connect to in the gossip network.
-define(NUM_CONNECTIONS, 3).
%% The maximum time to wait between actions.
%% The average case wait time will be 50% of this value.
-define(ACTION_TIME, 600).
%% Maximum length of data segment of transaction.
-define(MAX_TX_LEN, 1024 * 1024).

-record(state, {
	private_key,
	public_key,
	gossip
}).

%% Spawns a simulated client, when given a list
%% of peers to connect to.
start(Peers) ->
	{Priv, Pub} = ar_wallet:new(),
	spawn(
		fun() ->
			server(
				#state {
					private_key = Priv,
					public_key = Pub,
					gossip =
						ar_gossip:init(
							ar_gossip:pick_random_peers(
								Peers,
								?NUM_CONNECTIONS
							)
						)
				}
			)
		end
	).

%% Main client server loop.
server(S = #state { private_key = Priv, public_key = Pub, gossip = GS }) ->
	receive
		stop -> ok
	after ar:scale_time(rand:uniform(?ACTION_TIME * 1000)) ->
		% Choose a random behaviour
		case rand:uniform(1) of
			1 ->
				% Generate and dispatch a new data transaction.
				TX = ar_tx:new(<< 0:(rand:uniform(?MAX_TX_LEN) * 8) >>),
				SignedTX = ar_tx:sign(TX, Priv, Pub),
				server(S#state { gossip = ar_node:add_tx(GS, SignedTX) })
		end
	end.
