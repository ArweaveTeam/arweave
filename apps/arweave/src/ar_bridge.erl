%% This Source Code Form is subject to the terms of the GNU General 
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed 
%% with this file, You can obtain one at 
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%
%% @author Sam Williams
%% @author Frank Mueller
%% @author Ivan Uemlianin
%% @author Martin Torhage
%% @author Lev Berman <lev@arweave.org>
%% @author Taras Halturin <taras@arweave.org>
%%
%% @doc Represents a bridge node in the internal gossip network
%% to the external message passing interfaces.
-module(ar_bridge).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
		 handle_call/3,
		 handle_cast/2,
		 handle_info/2,
		 terminate/2,
		 code_change/3]).

-export([add_tx/1, move_tx_to_mining_pool/1, add_block/4]).
-export([add_remote_peer/1, add_local_peer/1]).
-export([get_remote_peers/0, set_remote_peers/1]).
-export([ignore_id/1, unignore_id/1, is_id_ignored/1]).
-export([drop_waiting_txs/1]).

-include("ar.hrl").
-include("ar_config.hrl").
-include_lib("common.hrl").

%% Internal state definition.
-record(state, {
	protocol = http, % Interface to bridge across
	gossip, % Gossip state
	external_peers, % Peers to send message to ordered by best to worst.
	processed = [], % IDs to ignore.
	updater = undefined, % spawned process for updating peer list
	port
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Notify the bridge of a new external transaction.
add_tx(TX) ->
	gen_server:cast(?MODULE, {add_tx, TX}).

%% @doc Notify the bridge of a transaction ready to be mined.
move_tx_to_mining_pool(TX) ->
	gen_server:cast(?MODULE, {move_tx_to_mining_pool, TX}).

%% @doc Notify the bridge of a new external block.
add_block(OriginPeer, Block, BDS, ReceiveTimestamp) ->
	gen_server:cast(?MODULE, {add_block, OriginPeer, Block, BDS, ReceiveTimestamp}).

%% @doc Add a remote HTTP peer.
add_remote_peer(Node) ->
	case is_loopback_ip(Node) of
		true -> ok; % do nothing
		false ->
			gen_server:cast(?MODULE, {add_peer, remote, Node})
	end.

%% @doc Add a local gossip peer.
add_local_peer(Node) ->
	gen_server:cast(?MODULE, {add_peer, local, Node}).

%% @doc Get a list of remote peers
get_remote_peers() ->
	gen_server:call(?MODULE, {get_peers, remote}).

%% @doc Reset the remote peers list to a specific set.
set_remote_peers(Peers) ->
	gen_server:cast(?MODULE, {set_peers, Peers}).

%% @doc Notify the bridge of the dropped transactions from
%% the awaiting propgation transaction set.
drop_waiting_txs(TXs) ->
	gen_server:cast(?MODULE, {drop_waiting_txs, TXs}).

%% @doc Ignore messages matching the given ID.
ignore_id(ID) ->
	ets:insert(ignored_ids, {ID, ignored}).

unignore_id(ID) ->
	ets:delete_object(ignored_ids, {ID, ignored}).

is_id_ignored(ID) ->
	case ets:lookup(ignored_ids, ID) of
		[{ID, ignored}] -> true;
		[] -> false
	end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%					   {ok, State, Timeout} |
%%					   ignore |
%%					   {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),

	%% Start a bridge, add it to the node's peer list.
	ar_node:add_peers([self()]),

	ok = ar_tx_queue:start_link(),

	ar_firewall:start(),
	%% Add pending transactions from the persisted mempool to the propagation queue.
	maps:map(
		fun (_TXID, {_TX, ready_for_mining}) ->
				ok;
			(_TXID, {TX, waiting}) ->
				add_tx(TX)
		end,
		ar_node:get_pending_txs([as_map])
	),

	erlang:send_after(0, self(), get_more_peers),

	State = #state {
		gossip = ar_gossip:init(whereis(ar_node)),
		external_peers = ar_join:filter_peer_list(Config#config.peers),
		port = Config#config.port
	},
	{ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%									 {reply, Reply, State} |
%%									 {reply, Reply, State, Timeout} |
%%									 {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, Reply, State} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({get_peers, remote}, _From, State) ->
	{reply, State#state.external_peers, State};

handle_call(Request, _From, State) ->
	?LOG_ERROR("unhandled call: ~p", [Request]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%									{noreply, State, Timeout} |
%%									{stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

%% @doc Send the transaction to internal processes.
handle_cast({add_tx, TX}, State) ->
	#state {
		gossip = GS,
		processed = Procd
	} = State,

	case ar_firewall:scan_tx(TX) of
		reject ->
			{noreply, State};
		accept ->
			Msg = {add_waiting_tx, TX},
			{NewGS, _} = ar_gossip:send(GS, Msg),
			ar_tx_queue:add_tx(TX),
			add_processed(tx, TX, Procd),
			{noreply, State#state { gossip = NewGS }}
	end;

handle_cast({move_tx_to_mining_pool, _TX} = Msg, State) ->
	#state { gossip = GS } = State,
	{NewGS, _} = ar_gossip:send(GS, Msg),
	{noreply, State#state { gossip = NewGS }};

handle_cast({add_block, OriginPeer, B, BDS, ReceiveTimestamp}, State) ->
	#state {
		gossip = GS,
		processed = Procd,
		external_peers = ExternalPeers
	} = State,

	Msg = {new_block, OriginPeer, B#block.height, B, BDS, ReceiveTimestamp},
	{NewGS, _} = ar_gossip:send(GS, Msg),

	send_block_to_external(ExternalPeers, B, BDS),
	add_processed(block, B, Procd),

	{noreply, State#state { gossip = NewGS }};

handle_cast({add_peer, remote, Peer}, State) ->
	#state{ external_peers = ExtPeers } = State,
	case {lists:member(Peer, ?PEER_PERMANENT_BLACKLIST), lists:member(Peer, ExtPeers)} of
		{true, _} ->
			{noreply, State};
		{_, true} ->
			{noreply, State};
		{_, false} ->
			{noreply, State#state{ external_peers = ExtPeers ++ [Peer] }}
	end;

handle_cast({add_peer, local, Peer}, State) ->
	#state{ gossip = GS0 } = State, 
	GS1 = ar_gossip:add_peers(GS0, Peer),
	{noreply, State#state{ gossip = GS1}};

handle_cast({set_peers, Peers}, State) ->
	update_state_metrics(Peers),
	{noreply, State#state{ external_peers = Peers }};

handle_cast({drop_waiting_txs, _TXs} = Msg, State) ->
	#state{ gossip = GS } = State,
	{NewGS, _} = ar_gossip:send(GS, Msg),
	{noreply, State#state{ gossip = NewGS }};

handle_cast(Msg, State) ->
	?LOG_ERROR("unhandled cast: ~p", [Msg]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

%FIXME do we realy serve gossip messages by the bridge? 
handle_info(Info, State) when is_record(Info, gs_msg) ->
	#state{ gossip = GS0 } = State,
	case ar_gossip:recv(GS0, Info) of
		{_, ignore} -> 
			{noreply, State};
		Gossip -> 
			State1 = gossip_to_external(State, Gossip),
			{noreply, State1}
	end;

handle_info(get_more_peers, #state{updater = undefined} = State) ->
	Self = self(),
	erlang:send_after(?GET_MORE_PEERS_TIME, Self, get_more_peers),
	Updater = spawn(
		fun() ->
			?LOG_INFO("spawn peers external_peers", State#state.external_peers),
			Peers = ar_manage_peers:update(State#state.external_peers),
			lists:map(fun ar_http_iface_client:add_peer/1, Peers),
			?LOG_INFO("spawn peers update", Peers),
			Self ! {update_peers, remote, Peers}
		end
	),
	{noreply, State#state{updater = Updater}};

handle_info(get_more_peers, State) ->
	?LOG_WARNING("peers updating seems stuck"),
	erlang:send_after(?GET_MORE_PEERS_TIME, self(), get_more_peers),
	{noreply, State};

handle_info({update_peers, remote, Peers}, State) ->
	update_state_metrics(Peers),
	State1 = State#state{ external_peers = Peers, updater = undefined},
	{noreply, State1};

handle_info(Info, State) ->
	?LOG_ERROR("unhandled info: ~p", [Info]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

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

%%% INTERNAL FUNCTIONS

%% @doc Add the ID of a new TX/block to a processed list.
add_processed({add_tx, TX}, Procd) ->
	add_processed(tx, TX, Procd);
add_processed({new_block, _, _, B, _, _}, Procd) ->
	add_processed(block, B, Procd);
add_processed(X, _Procd) ->
	?LOG_INFO(
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
	?LOG_INFO(
		[
			{could_not_ignore, X},
			{record, Y}
		]),
	ok.

%% @doc Send an internal message externally.
send_to_external(S, {new_block, _, _Height, _NewB, no_data_segment, _Timestamp}) ->
	S;
send_to_external(S, {new_block, _, _Height, NewB, BDS, _Timestamp}) ->
	send_block_to_external(
		S#state.external_peers,
		NewB,
		BDS
	),
	S;
send_to_external(S, {add_tx, _TX}) ->
	%% The message originates from the internal network, do not gossip.
	S;
send_to_external(S, {NewGS, Msg}) ->
	send_to_external(S#state { gossip = NewGS }, Msg).

%% @doc Send a block to external peers in a spawned process.
send_block_to_external(ExternalPeers, B, BDS) ->
	spawn(fun() ->
		send_block_to_external_parallel(ExternalPeers, B, BDS)
	end).

%% @doc Send the new block to the peers by first sending it in parallel to the
%% best/first peers and then continuing sequentially with the rest of the peers
%% in order.
send_block_to_external_parallel(Peers, NewB, BDS) ->
	{PeersParallel, PeersRest} = lists:split(
		min(length(Peers), ?BLOCK_PROPAGATION_PARALLELIZATION),
		Peers
	),
	NSeqPeers = max(0, ar_meta_db:get(max_propagation_peers) - ?BLOCK_PROPAGATION_PARALLELIZATION),
	PeersSequential = lists:sublist(PeersRest, NSeqPeers),
	?LOG_INFO(
		[
			{sending_block_to_external_peers, ar_util:encode(NewB#block.indep_hash)},
			{peers, length(PeersParallel) + length(PeersSequential)}
		]
	),
	Send = fun(Peer) ->
		ar_http_iface_client:send_new_block(Peer, NewB, BDS)
	end,
	ar_util:pmap(Send, PeersParallel),
	lists:foreach(Send, PeersSequential).

%% @doc Possibly send a new message to external peers.
gossip_to_external(S = #state { processed = Procd }, {NewGS, Msg}) ->
	NewS = send_to_external(S#state { gossip = NewGS }, Msg),
	add_processed(Msg, Procd),
	NewS.

%% @doc Check whether a message has already been seen.
% already_processed(_Procd, _Type, {_, not_found, _}) ->
%	true;
% already_processed(_Procd, _Type, {_, unavailable, _}) ->
%	true;
% already_processed(Procd, Type, Data) ->
%	already_processed(Procd, Type, Data, undefined).
% already_processed(_Procd, Type, Data, _IP) ->
%	is_id_ignored(get_id(Type, Data)).

update_state_metrics(Peers) when is_list(Peers) ->
	prometheus_gauge:set(arweave_peer_count, length(Peers));
update_state_metrics(_) ->
	ok.
