-module(ar_gossip).
-export([init/0, init/1, peers/1, add_peers/2, send/2, recv/2]).
-export([set_loss_probability/2, set_delay/2, set_xfer_speed/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% The ArkChain gossip protocol implementation.
%%% Can be plugged into processes by receiving
%%% #gs_msg records and running them through the recv function.
%%% The state object is changed and returned as the first element
%%% of a two element tuple for every library call.

%% @doc Create a new gossip node state. Optionally, with peer list.
init() -> init([]).
init(PID) when is_pid(PID) -> init([PID]);
init(Peers) when is_list(Peers) ->
    #gs_state { peers = Peers };
init(PacketLossP) when is_float(PacketLossP) ->
	#gs_state { loss_probability = PacketLossP };
init(MaxDelayMS) when is_integer(MaxDelayMS) ->
	#gs_state { delay = MaxDelayMS }.

%% @doc Update a gossip protocol state with new peers.
add_peers(S, []) -> S;
add_peers(S, [Peer|Peers]) when self() == Peer -> add_peers(S, Peers);
add_peers(S, [Peer|Peers]) ->
	case lists:member(Peer, S#gs_state.peers) of
		false -> add_peers(S#gs_state { peers = [Peer|S#gs_state.peers] }, Peers);
		true -> add_peers(S, Peers)
	end.

%% @doc Take a gossip state, return the contained peers.
peers(S) -> S#gs_state.peers.

%% @doc Update the probability that a packet will be loss.
set_loss_probability(S, Prob) ->
	S#gs_state { loss_probability = Prob }.

%% @doc Adjust the maximum network delay length.
%% Note: This is the /maximum/ delay in milliseconds.
%% All messages will be delayed for a random number of
%% milliseconds between 0 and this value.
set_delay(S, Delay) ->
	S#gs_state { delay = Delay }.

%% @doc Set the number of bytes transferred from a node per second.
set_xfer_speed(S, Speed) ->
	S#gs_state { xfer_speed = Speed }.

%% @doc Send a message to your peers in the gossip network,
%% if the message has not already been sent.
send(S, Data) when not is_record(Data, gs_msg) ->
	send(S,
		#gs_msg {
			hash = (crypto:hash(?HASH_ALG, term_to_binary(Data))),
			data = Data
		}
	);
send(S, Msg) ->
	case already_heard(S, Msg) of
		false ->
			lists:foreach(
				fun(Peer) ->
					spawn(fun() -> possibly_send(S, Peer, Msg) end)
				end,
				S#gs_state.peers
			),
			{S#gs_state { heard = [Msg#gs_msg.hash|S#gs_state.heard] }, sent};
		true -> {S, ignored}
	end.

%% @doc Potentially send a message to a node, depending on state.
%% No warning is issued for messages that cannot be sent to network peers!
possibly_send(_S, Peer, #gs_msg { data = {new_block, _Node, _Height, _NewB, _Recall} })
		when (not is_pid(Peer)) ->
	ignore;
possibly_send(_S, Peer, #gs_msg { data = {add_tx, _TX} }) when not is_pid(Peer) ->
	%ar_http_iface:send_new_tx(Peer, TX);
	ignore;
possibly_send(S, Peer, Msg) when is_pid(Peer) ->
	case rand:uniform() >= S#gs_state.loss_probability of
		true ->
			case S#gs_state.delay of
				0 ->
					Peer ! Msg;
				MaxDelay ->
					erlang:send_after(
						ar:scale_time(
							rand:uniform(MaxDelay) + calculate_xfer_time(S, Msg)
						),
						Peer,
						Msg
					)
			end;
		false -> not_sent
	end;
possibly_send(_S, _NetworkPeer, _Msg) ->
	not_sent.

%% @doc Returns a number of milliseconds to wait in order to simulate transfer time.
calculate_xfer_time(#gs_state { xfer_speed = undefined }, _) -> 0;
calculate_xfer_time(S, Msg) ->
	erlang:byte_size(term_to_binary(Msg)) div S#gs_state.xfer_speed.

%% @doc Takes a gs_msg and gs_state, returning the message, if it needs to
%% be processed.
recv(S, Msg) ->
	case already_heard(S, Msg) of
		false ->
			{NewS, _} = send(S, Msg),
			{NewS, Msg#gs_msg.data};
		true ->
			{S, ignore}
	end.

%% @doc Has this node already heard about this message?
already_heard(S, Msg) when is_record(Msg, gs_msg) ->
	already_heard(S, Msg#gs_msg.hash);
already_heard(S, Hash) ->
	lists:member(Hash, S#gs_state.heard).

%%% Gossip protocol tests.

%% @doc Ensure single message receipt on every process in a fully
%% connected network of gossipers.
fully_connected_test() ->
	TestPID = self(),
	BasicServer =
		fun Server(S) ->
			receive
				Msg when is_record(Msg, gs_msg) ->
					case recv(S, Msg) of
						{NewS, ignore} ->
							io:format("~p ignoring message.~n", [self()]),
							Server(NewS);
						{NewS, Data} ->
							io:format("Sending message from ~p.~n", [self()]),
							TestPID ! Data,
							Server(NewS)
					end;
				{peers, Peers} -> Server(add_peers(S, Peers))
			end
		end,
	% Start the gossip servers and send them the complete list of peers.
	Servers = [ spawn(fun() -> BasicServer(init()) end) || _ <- lists:seq(1, 100) ],
	[ Serv ! {peers, Servers} || Serv <- Servers ],
	% Start a local gossip node.
	State = init([lists:last(Servers)]),
	send(State, test_message),
	100 = count_receipts(test_message, 1000).

%% @doc Ensure single message receipt on every process in a partially
%% connected network of gossipers.
partially_connected_test() ->
	TestPID = self(),
	BasicServer =
		fun Server(S) ->
			receive
				Msg when is_record(Msg, gs_msg) ->
					case recv(S, Msg) of
						{NewS, ignore} ->
							io:format("~p ignoring message.~n", [self()]),
							Server(NewS);
						{NewS, Data} ->
							io:format("Sending message from ~p.~n", [self()]),
							TestPID ! Data,
							Server(NewS)
					end;
				{peers, Peers} -> Server(add_peers(S, Peers))
			end
		end,
	% Start the gossip servers and send them the complete list of peers.
	Servers = [ spawn(fun() -> BasicServer(init()) end) || _ <- lists:seq(1, 1000) ],
	[ Serv ! {peers, ar_util:pick_random(Servers, 20)} || Serv <- Servers ],
	% Start a local gossip node.
	State = init([lists:last(Servers)]),
	send(State, test_message),
	1000 = count_receipts(test_message, 1000).

%%% Testing utility functions

%% @doc Count the number of times a message is received before the timeout is hit.
count_receipts(Msg, Timeout) -> count_receipts(Msg, Timeout, 0).
count_receipts(Msg, Timeout, N) ->
	receive Msg -> count_receipts(Msg, Timeout, N + 1)
	after Timeout -> N
	end.