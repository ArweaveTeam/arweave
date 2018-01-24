-module(ar_bridge).
-export([start/0, start/1, start/2]).
-export([add_tx/2, add_block/3]).
-include("ar.hrl").

%%% Represents a bridge node in the internal gossip network
%%% to the external message passing interfaces.

%% Internal state definition.
-record(state, {
	protocol = http, % Interface to bridge across
	gossip, % Gossip state
	external_peers, % Peers to send message to.
	processed = [] % IDs to ignore.
}).

%% Launch a bridge node.
start() -> start([]).
start(ExtPeers) -> start(ExtPeers, []).
start(ExtPeers, IntPeers) ->
    spawn(
		server(
			#state {
				gossip = ar_gossip:init(IntPeers),
				external_peers = ExtPeers
			}
		)
	).

%% Notify the bridge of a new external block.
add_block(PID, Block, RecallBlock) ->
	PID ! {add_block, Block, RecallBlock}.

%% Notify the bridge of a new external block.
add_tx(PID, TX) ->
	PID ! {add_tx, TX}.

%%% INTERNAL FUNCTIONS

%% Main server loop.
server(S = #state { gossip = GS0 }) ->
	receive
		% TODO: Propagate external to external nodes.
		{add_tx, TX} ->
			server(maybe_send_to_internal(S, tx, TX));
		{add_block, Block, RecallBlock} ->
			server(maybe_send_to_internal(S, block, {Block, RecallBlock}));
		Msg when is_record(Msg, gs_msg) ->
			server(send_to_external(S, ar_gossip:recv(GS0, Msg)))
	end.

%% Potentially send a message to internal processes.
maybe_send_to_internal(S = #state { gossip = GS }, Type, Data) ->
	case verify(Type, Data) of
		false ->
			% If the data does not validate, ignore the message.
			S;
		true ->
			% The message is at least valid, distribute it.
			{NewGS, _} =
				ar_gossip:send(
					GS,
					case Type of
						tx -> {add_tx, Data};
						block ->
							{NewB, RecallB} = Data,
							{new_block, undefined, NewB#block.height, NewB, RecallB}
					end),
			S#state { gossip = NewGS }
	end.

%% Send an internal message externally
send_to_external(S = #state {external_peers = Peers}, {add_tx, TX}) ->
	lists:foreach(
		fun(Peer) ->
			ar_http_iface:send_new_tx(Peer, TX)
		end,
		Peers
	),
	S;
send_to_external(
		S = #state {external_peers = Peers},
		{new_block, _Peer, _Height, NewB, RecallB}) ->
	lists:foreach(
		fun(Peer) ->
			ar_http_iface:send_new_block(Peer, NewB, RecallB)
		end,
		Peers
	),
	S;
send_to_external(S, {NewGS, Msg}) ->
	send_to_external(S#state { gossip = NewGS }, Msg).

%% Validate messages before propagating them.
verify(_, _) -> true.