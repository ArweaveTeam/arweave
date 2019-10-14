-module(ar_bridge).
-export([start/3]).
-export([add_tx/2, add_block/5]).
-export([add_remote_peer/2, add_local_peer/2]).
-export([get_remote_peers/1, set_remote_peers/2]).
-export([start_link/1]).
-export([ignore_id/1, is_id_ignored/1]).
-include("ar.hrl").

%%% Represents a bridge node in the internal gossip network
%%% to the external message passing interfaces.

%% Internal state definition.
-record(state, {
	protocol = http, % Interface to bridge across
	gossip, % Gossip state
	external_peers, % Peers to send message to ordered by best to worst.
	processed = [], % IDs to ignore.
	port
}).

%% @doc Start a node, linking to a supervisor process
start_link([_, _, _] = Args) ->
	PID = erlang:apply(ar_bridge, start, Args),
	{ok, PID}.

%% @doc Launch a bridge node.
start(ExtPeers, IntPeers, Port) ->
	ar_firewall:start(),
	spawn(
		fun() ->
			ar:report([starting_ignored_ids_db]),
			ets:new(ignored_ids, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	receive after 250 -> ok end,
    PID =
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
		),
	reset_timer(PID, get_more_peers),
	PID.

%% @doc Get a list of remote peers
get_remote_peers(PID) ->
	PID ! {get_peers, remote, self()},
	receive
		{remote_peers, ExternalPeers} ->
			ExternalPeers
	after ?LOCAL_NET_TIMEOUT -> []
	end.

%% @doc Reset the remote peers list to a specific set.
set_remote_peers(PID, Peers) ->
	PID ! {set_peers, Peers}.

%% @doc Notify the bridge of a new external block.
add_block(PID, OriginPeer, Block, BDS, Recall) ->
	PID ! {add_block, OriginPeer, Block, BDS, Recall}.

%% @doc Notify the bridge of a new external transaction.
add_tx(PID, TX) ->
	PID ! {add_tx, TX}.

%% @doc Add a remote HTTP peer.
add_remote_peer(PID, Node) ->
	case is_loopback_ip(Node) of
		true -> do_nothing;
		false ->
			PID ! {add_peer, remote, Node}
	end.

-ifdef(DEBUG).
%% Do not filter out loopback IP addresses with custom port in the debug mode
%% to allow multiple local VMs to peer with each other.
is_loopback_ip({127, _, _, _, Port}) -> Port == ar_meta_db:get(port);
is_loopback_ip({_, _, _, _, _}) -> false.
-else.
%% @doc Is the IP address in question a loopback ('us') address?
is_loopback_ip({A, B, C, D, _Port}) -> is_loopback_ip({A, B, C, D});
is_loopback_ip({127, _, _, _}) -> true;
is_loopback_ip({0, _, _, _}) -> true;
is_loopback_ip({169, 254, _, _}) -> true;
is_loopback_ip({255, 255, 255, 255}) -> true;
is_loopback_ip({_, _, _, _}) -> false.
-endif.

%% @doc Add a local gossip peer.
add_local_peer(PID, Node) ->
	PID ! {add_peer, local, Node}.

%% @doc Ignore messages matching the given ID.
ignore_id(ID) ->
	ets:insert(ignored_ids, {ID, ignored}).

%% @doc Schedule a message timer.
reset_timer(PID, get_more_peers) ->
	erlang:send_after(?GET_MORE_PEERS_TIME, PID, {get_more_peers, PID}).

is_id_ignored(ID) ->
	case ets:lookup(ignored_ids, ID) of
		[{ID, ignored}] -> true;
		[] -> false
	end.

%%% INTERNAL FUNCTIONS

%% @doc Main server loop.
server(S) ->
	receive
		Msg ->
			try handle(S, Msg) of
				NewS ->
					server(NewS)
			catch
				throw:Term ->
					ar:report( [ {'BridgeEXCEPTION', Term} ]),
					server(S);
				exit:Term ->
					ar:report( [ {'BridgeEXIT', Term} ]),
					server(S);
				error:Term ->
					ar:report( [ {'BridgeEXIT', {Term, erlang:get_stacktrace()}} ]),
					server(S)
			end
	end.

%% @doc Handle the server messages.
handle(S, {add_tx, TX}) ->
	maybe_send_tx(S, TX);
handle(S, {add_block, OriginPeer, B, BDS, Recall}) ->
	send_block(S, OriginPeer, B, BDS, Recall);
handle(S = #state{ external_peers = ExtPeers }, {add_peer, remote, Peer}) ->
	case {lists:member(Peer, ?PEER_PERMANENT_BLACKLIST), lists:member(Peer, ExtPeers)} of
		{true, _} ->
			S;
		{_, true} ->
			S;
		{_, false} ->
			S#state{ external_peers = ExtPeers ++ [Peer] }
	end;
handle(S = #state{ gossip = GS0 }, {add_peer, local, Peer}) ->
	S#state{ gossip = ar_gossip:add_peers(GS0, Peer)};
handle(S, {get_peers, remote, Peer}) ->
	Peer ! {remote_peers, S#state.external_peers},
	S;
handle(S, {set_peers, Peers}) ->
	S#state{ external_peers = Peers };
handle(S, {update_peers, remote, Peers}) ->
	S#state{ external_peers = Peers };
handle(S = #state{ gossip = GS0 }, Msg) when is_record(Msg, gs_msg) ->
	case ar_gossip:recv(GS0, Msg) of
		{_, ignore} -> S;
		Gossip -> gossip_to_external(S, Gossip)
	end;
handle(S, {get_more_peers, PID}) ->
	spawn(
		fun() ->
			Peers = ar_manage_peers:update(S#state.external_peers),
			lists:map(fun ar_http_iface_client:add_peer/1, Peers),
			PID ! {update_peers, remote, Peers},
			reset_timer(PID, get_more_peers)
		end
	),
	S;
handle(S, UnknownMsg) ->
	ar:report([{ar_bridge_handle_unknown_message, UnknownMsg}]),
	S.

%% @doc Send the transaction to internal processes and to peers.
maybe_send_tx(S, Data) ->
	#state {
		gossip = GS,
		processed = Procd
	} = S,
	case ar_firewall:scan_tx(Data) of
		reject ->
			% If the data does not pass the scan, ignore the message.
			S;
		accept ->
			% The message is at least valid, distribute it.
			Msg = {add_tx, Data},
			{NewGS, _} = ar_gossip:send(GS,	Msg),
			send_to_external(S, Msg),
			add_processed(tx, Data, Procd),
			S#state { gossip = NewGS }
	end.

%% @doc Send the block to internal processes and to peers.
send_block(S, OriginPeer, B, BDS, Recall) ->
	#state {
		gossip = GS,
		processed = Procd,
		external_peers = ExternalPeers
	} = S,
	% TODO: Is it always appropriate not to check whether the block has
	% already been processed?
	%(not already_processed(Procd, Type, Data)) andalso
	% The message is at least valid, distribute it.
	% {OriginPeer, NewB, RecallIndepHash} = Data,
	Msg = {new_block, OriginPeer, B#block.height, B, BDS, Recall},
	{NewGS, _} = ar_gossip:send(GS, Msg),
	send_block_to_external(ExternalPeers, B, BDS, Recall),
	add_processed(block, B, Procd),
	S#state {
		gossip = NewGS
	}.

%% @doc Add the ID of a new TX/block to a processed list.
add_processed({add_tx, TX}, Procd) ->
	add_processed(tx, TX, Procd);
add_processed({new_block, _, _, B, _, _}, Procd) ->
	add_processed(block, B, Procd);
add_processed(X, _Procd) ->
	ar:report(
		[
			{could_not_ignore, X},
			{record, X}
		]),
	ok.
add_processed(tx, #tx { id = ID }, _Procd) ->
	ignore_id(ID);
add_processed(block, #block { indep_hash = Hash }, _Procd) ->
	ignore_id(Hash);
add_processed(X, Y, _Procd) ->
	ar:report(
		[
			{could_not_ignore, X},
			{record, Y}
		]),
	ok.

%% @doc Find the ID of a 'data', from type.
% get_id(tx, #tx { id = ID}) -> ID;
% get_id(block, B) when ?IS_BLOCK(B) -> B#block.indep_hash;
% get_id(block, {_, #block { indep_hash = Hash}, _}) -> Hash.

%% @doc Send an internal message externally.
send_to_external(S = #state {external_peers = ExternalPeers}, {add_tx, TX}) ->
	send_tx_to_external(ExternalPeers, TX),
	S;
send_to_external(S, {new_block, _, _Height, _NewB, no_data_segment, _Recall}) ->
	S;
send_to_external(S, {new_block, _, _Height, NewB, BDS, Recall}) ->
	send_block_to_external(
		S#state.external_peers,
		NewB,
		BDS,
		Recall
	),
	S;
send_to_external(S, {NewGS, Msg}) ->
	send_to_external(S#state { gossip = NewGS }, Msg).

send_tx_to_external(ExternalPeers, TX) ->
	spawn(
		fun() ->
			send_tx_to_external_parallel(ExternalPeers, TX)
		end
	).

%% @doc Send a block to external peers in a spawned process.
send_block_to_external(ExternalPeers, B, BDS, Recall) ->
	spawn(fun() ->
		send_block_to_external_parallel(ExternalPeers, B, BDS, Recall)
	end).

%% @doc Send the new tx to the peers by first sending it in parallel to the
%% best/first peers and then continuing sequentially with the rest of the peers
%% in order.
send_tx_to_external_parallel(Peers, TX) ->
	{PeersParallel, PeersRest} = lists:split(
		min(length(Peers), ?TX_PROPAGATION_PARALLELIZATION),
		Peers
	),
	NSeqPeers = max(0, ?MAX_PROPAGATION_PEERS - ?TX_PROPAGATION_PARALLELIZATION),
	PeersSequential = lists:sublist(PeersRest, NSeqPeers),
	ar:report(
		[
			{sending_tx_to_external_peers, ar_util:encode(TX#tx.id)},
			{peers, length(PeersParallel) + length(PeersSequential)}
		]
	),
	Send = fun(Peer) ->
		ar_http_iface_client:send_new_tx(Peer, TX)
	end,
	ar_util:pmap(Send, PeersParallel),
	lists:foreach(Send, PeersSequential).

%% @doc Send the new block to the peers by first sending it in parallel to the
%% best/first peers and then continuing sequentially with the rest of the peers
%% in order.
send_block_to_external_parallel(Peers, NewB, BDS, Recall) ->
	{PeersParallel, PeersRest} = lists:split(
		min(length(Peers), ?BLOCK_PROPAGATION_PARALLELIZATION),
		Peers
	),
	NSeqPeers = max(0, ?MAX_PROPAGATION_PEERS - ?BLOCK_PROPAGATION_PARALLELIZATION),
	PeersSequential = lists:sublist(PeersRest, NSeqPeers),
	ar:report(
		[
			{sending_block_to_external_peers, ar_util:encode(NewB#block.indep_hash)},
			{peers, length(PeersParallel) + length(PeersSequential)}
		]
	),
	Send = fun(Peer) ->
		ar_http_iface_client:send_new_block(Peer, NewB, BDS, Recall)
	end,
	ar_util:pmap(Send, PeersParallel),
	lists:foreach(Send, PeersSequential).

%% @doc Possibly send a new message to external peers.
gossip_to_external(S = #state { processed = Procd }, {NewGS, Msg}) ->
	NewS = (send_to_external(S#state { gossip = NewGS }, Msg)),
	add_processed(Msg, Procd),
	NewS.

%% @doc Check whether a message has already been seen.
% already_processed(_Procd, _Type, {_, not_found, _}) ->
% 	true;
% already_processed(_Procd, _Type, {_, unavailable, _}) ->
% 	true;
% already_processed(Procd, Type, Data) ->
% 	already_processed(Procd, Type, Data, undefined).
% already_processed(_Procd, Type, Data, _IP) ->
% 	is_id_ignored(get_id(Type, Data)).
