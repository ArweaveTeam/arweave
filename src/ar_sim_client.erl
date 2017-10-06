-module(ar_sim_client).
-export([start/1, start/2, start/3, start/4, stop/1]).

%%% Represents a simulated Archain user client.
%%% Currently implemented behaviours:
%%%		- Create wallet
%%%		- Sign and add transactions to network

%% The number of peers to connect to in the gossip network.
-define(DEFAULT_NUM_CONNECTIONS, 3).
%% The maximum time to wait between actions.
%% The average case wait time will be 50% of this value.
-define(DEFAULT_ACTION_TIME, 600).
%% Maximum length of data segment of transaction.
-define(DEFAULT_MAX_TX_LEN, 1024 * 1024).

-record(state, {
	private_key,
	public_key,
	gossip,
	action_time,
	max_tx_len
}).

%% @doc Spawns a simulated client, when given a list
%% of peers to connect to.
start(Peers) -> start(Peers, ?DEFAULT_ACTION_TIME).
start(Peers, ActionTime) -> start(Peers, ActionTime, ?DEFAULT_MAX_TX_LEN).
start(Peers, ActionTime, MaxTXLen) ->
	start(Peers, ActionTime, MaxTXLen, ?DEFAULT_NUM_CONNECTIONS).
start(Peers, ActionTime, MaxTXLen, NumConnections) ->
	{Priv, Pub} = ar_wallet:new(),
	spawn(
		fun() ->
			server(
				#state {
					private_key = Priv,
					public_key = Pub,
					action_time = ActionTime,
					max_tx_len = MaxTXLen,
					gossip =
						ar_gossip:init(
							ar_util:pick_random(
								Peers,
								NumConnections
							)
						)
				}
			)
		end
	).

%% @doc Stop a client node.
stop(PID) ->
	PID ! stop,
	ok.

%% @doc Main client server loop.
server(
	S = #state {
		private_key = Priv,
		public_key = Pub,
		gossip = GS,
		max_tx_len = MaxTXLen,
		action_time = ActionTime
	}) ->
	receive
		stop -> ok
	after ar:scale_time(rand:uniform(ActionTime * 1000)) ->
		% Choose a random behaviour
		case rand:uniform(1) of
			1 ->
				% Generate and dispatch a new data transaction.
				TX = ar_tx:new(<< 0:(rand:uniform(MaxTXLen) * 8) >>),
				SignedTX = ar_tx:sign(TX, Priv, Pub),
				server(S#state { gossip = ar_node:add_tx(GS, SignedTX) })
		end
	end.
