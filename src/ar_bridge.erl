-module(ar_bridge).
-export([start/0, start/1, start/2, start/3]).
-export([add_tx/2, add_block/4]). % Called from ar_http_iface
-export([add_remote_peer/2, add_local_peer/2]).
-export([get_remote_peers/1]).
-include("ar.hrl").

%%% Represents a bridge node in the internal gossip network
%%% to the external message passing interfaces.

%% Internal state definition.
-record(state, {
	protocol = http, % Interface to bridge across
	gossip, % Gossip state
	external_peers, % Peers to send message to.
	processed = [], % IDs to ignore.
	firewall = ar_firewall:start(),
	port
}).

%% Launch a bridge node.
start() -> start([]).
start(ExtPeers) -> start(ExtPeers, []).
start(ExtPeers, IntPeers) -> start(ExtPeers, IntPeers, ?DEFAULT_HTTP_IFACE_PORT).
start(ExtPeers, IntPeers, Port) ->
    spawn(
		fun() ->
			server(
				#state {
					gossip = ar_gossip:init(IntPeers),
					external_peers = ExtPeers,
					port = Port
				}
			)
		end
	).

%% Get a list of remote peers
get_remote_peers(PID) ->
	PID ! {get_peers, remote, self()},
	receive
		{remote_peers, ExternalPeers} ->
			ExternalPeers
	end.

%% Notify the bridge of a new external block.
%% TODO: Add peer sending to bridge implementation.
add_block(PID, OriginPeer, Block, RecallBlock) ->
	PID ! {add_block, OriginPeer, Block, RecallBlock}.

%% Notify the bridge of a new external block.
add_tx(PID, TX) ->
	PID ! {add_tx, TX}.

%% Add a remote HTTP peer.
add_remote_peer(PID, Node) ->
	PID ! {add_peer, remote, Node}.

%% Add a local gossip peer.
add_local_peer(PID, Node) ->
	PID ! {add_peer, local, Node}.

%%% INTERNAL FUNCTIONS

%% Main server loop.
server(S = #state { gossip = GS0, external_peers = ExtPeers }) ->
	receive
		% TODO: Propagate external to external nodes.
		{add_tx, TX} ->
			server(maybe_send_to_internal(S, tx, TX));
		{add_block, OriginPeer, Block, RecallBlock} ->
			server(maybe_send_to_internal(S, block, {OriginPeer, Block, RecallBlock}));
		{add_peer, remote, Peer} ->
			ar:report_console({adding_remote_peer, Peer}),
			server(S#state { external_peers = [Peer|ExtPeers]});
		{add_peer, local, Peer} ->
			server(S#state { gossip = ar_gossip:add_peers(GS0, Peer)});
		{get_peers, remote, Peer} ->
			Peer ! {remote_peers, S#state.external_peers},
			server(S);
		Msg when is_record(Msg, gs_msg) ->
			server(do_send_to_external(S, ar_gossip:recv(GS0, Msg)))
	end.

%% Potentially send a message to internal processes.
maybe_send_to_internal(
		S = #state {
			gossip = GS,
			firewall = FW,
			processed = Procd
		},
		Type,
		Data) ->
	case
		already_processed(Procd, Type, Data) andalso
		ar_firewall:scan(FW, Data)
	of
		false ->
			% If the data does not pass the scan, ignore the message.
			S;
		true ->
			% The message is at least valid, distribute it.
			{NewGS, _} =
				ar_gossip:send(
					GS,
					Msg = case Type of
						tx -> {add_tx, Data};
						block ->
							{OriginPeer, NewB, RecallB} = Data,
							{new_block,
								OriginPeer,
								NewB#block.height,
								NewB,
								RecallB
							}
					end),
			send_to_external(S, Msg),
			S#state {
				gossip = NewGS,
				processed = add_processed(Type, Data, Procd)
			}
	end.

%% Add the ID of a new TX/block to a processed list.
add_processed({add_tx, TX}, Procd) ->
	add_processed(tx, TX, Procd);
add_processed({new_block, _OriginPeer, _, B, _}, Procd) ->
	add_processed(block, B, Procd).
add_processed(tx, #tx { id = ID }, Procd) -> [ID|Procd];
add_processed(block, #block { indep_hash = Hash }, Procd) ->
	[Hash|Procd];
add_processed(block, {_, B, _}, Procd) ->
	add_processed(block, B, Procd).

%% Find the ID of a 'data', from type.
get_id(tx, #tx { id = ID}) -> ID;
get_id(block, {_OriginPeer, #block { indep_hash = Hash}, _}) -> Hash.

%% Send an internal message externally
%% TODO: add Peer functionality in the same way that blocks do
send_to_external(S = #state {external_peers = Peers}, {add_tx, TX}) ->
	lists:foreach(
		fun(Peer) ->
			ar_http_iface:send_new_tx(Peer, TX)
		end,
		Peers
	),
	S;
send_to_external(
		S = #state {external_peers = Peers, port = Port},
		{new_block, _Peer, _Height, NewB, RecallB}) ->
	lists:foreach(
		fun(Peer) ->
			ar_http_iface:send_new_block(Peer, Port, NewB, RecallB)
		end,
		Peers
	),
	S;
send_to_external(S, {NewGS, Msg}) ->
	send_to_external(S#state { gossip = NewGS }, Msg).

%% Possibly send a new message to external peers.
do_send_to_external(S = #state { processed = Procd }, {NewGS, Msg}) ->
	(send_to_external(S#state { gossip = NewGS }, Msg))#state {
		processed = add_processed(Msg, Procd)
	}.

%% Check whether a message has already been seen.
already_processed(Procd, Type, Data) ->
	not lists:member(get_id(Type, Data), Procd).