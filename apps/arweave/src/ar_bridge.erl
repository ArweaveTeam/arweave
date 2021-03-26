%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc Represents a bridge node in the internal gossip network
%%% to the external message passing interfaces.
%%% @end
-module(ar_bridge).

-behaviour(gen_server).

-export([
	start_link/0,
	add_tx/1, move_tx_to_mining_pool/1, add_block/4,
	add_remote_peer/1, add_local_peer/1,
	get_remote_peers/0, get_remote_peers/1, set_remote_peers/1,
	drop_waiting_txs/1
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% Internal state definition.
-record(state, {
	gossip,				% The internal gossip state.
	external_peers,		% External peers ordered by best to worst.
	updater = undefined	% Spawned process for updating peer list.
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

%% @doc Get a list of remote peers.
get_remote_peers(Timeout) ->
	gen_server:call(?MODULE, {get_peers, remote}, Timeout).

%% @doc Get a list of remote peers.
get_remote_peers() ->
	gen_server:call(?MODULE, {get_peers, remote}).

%% @doc Reset the remote peers list to a specific set.
set_remote_peers(Peers) ->
	gen_server:cast(?MODULE, {set_peers, Peers}).

%% @doc Notify the bridge of the dropped transactions from
%% the awaiting propgation transaction set.
drop_waiting_txs(TXs) ->
	gen_server:cast(?MODULE, {drop_waiting_txs, TXs}).

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
	%% Start asking peers about their peers.
	erlang:send_after(0, self(), get_more_peers),
	State = #state {
		gossip = ar_gossip:init([]),
		external_peers = Config#config.peers
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

%% Send the transaction to internal processes.
handle_cast({add_tx, TX}, State) ->
	#state{ gossip = GS } = State,
	Msg = {add_waiting_tx, TX},
	{NewGS, _} = ar_gossip:send(GS, Msg),
	ar_tx_queue:add_tx(TX),
	ar_ignore_registry:add(TX#tx.id),
	{noreply, State#state { gossip = NewGS }};

handle_cast({move_tx_to_mining_pool, _TX} = Msg, State) ->
	#state { gossip = GS } = State,
	{NewGS, _} = ar_gossip:send(GS, Msg),
	{noreply, State#state { gossip = NewGS }};

handle_cast({add_block, OriginPeer, B, BDS, ReceiveTimestamp}, State) ->
	#state {
		gossip = GS,
		external_peers = ExternalPeers
	} = State,
	Msg = {new_block, OriginPeer, B#block.height, B, BDS, ReceiveTimestamp},
	{NewGS, _} = ar_gossip:send(GS, Msg),
	send_block_to_external(ExternalPeers, B, BDS),
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
	GS1 = ar_gossip:add_peers(GS0, [Peer]),
	{noreply, State#state{ gossip = GS1}};

handle_cast({set_peers, Peers}, State) ->
	update_state_metrics(Peers),
	{noreply, State#state{ external_peers = Peers }};

handle_cast({drop_waiting_txs, _TXs} = Msg, State) ->
	#state{ gossip = GS } = State,
	{NewGS, _} = ar_gossip:send(GS, Msg),
	{noreply, State#state{ gossip = NewGS }};

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
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

handle_info(Info, State) when is_record(Info, gs_msg) ->
	#state{ gossip = GS0 } = State,
	case ar_gossip:recv(GS0, Info) of
		{_, ignore} ->
			{noreply, State};
		Gossip ->
			State1 = gossip_to_external(State, Gossip),
			{noreply, State1}
	end;

handle_info(get_more_peers, #state{ updater = undefined } = State) ->
	Self = self(),
	erlang:send_after(?GET_MORE_PEERS_TIME, Self, get_more_peers),
	Updater = spawn(
		fun() ->
			Peers = ar_manage_peers:update(State#state.external_peers),
			lists:map(fun ar_http_iface_client:add_peer/1, Peers),
			Self ! {update_peers, remote, Peers}
		end
	),
	{noreply, State#state{ updater = Updater }};

handle_info(get_more_peers, State) ->
	?LOG_WARNING([{event, ar_bridge_update_peers_process_is_stuck}]),
	erlang:send_after(?GET_MORE_PEERS_TIME, self(), get_more_peers),
	{noreply, State};

handle_info({update_peers, remote, Peers}, State) ->
	update_state_metrics(Peers),
	State2 = State#state{ external_peers = Peers, updater = undefined },
	{noreply, State2};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
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
	?LOG_INFO([{event, ar_bridge_terminated}, {module, ?MODULE}]),
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
is_loopback_ip({127, _, _, _, Port}) ->
	{ok, Config} = application:get_env(arweave, config),
	Port == Config#config.port;
is_loopback_ip({_, _, _, _, _}) ->
	false.
-else.
%% @doc Is the IP address in question a loopback ('us') address?
is_loopback_ip({A, B, C, D, _Port}) -> is_loopback_ip({A, B, C, D});
is_loopback_ip({127, _, _, _}) -> true;
is_loopback_ip({0, _, _, _}) -> true;
is_loopback_ip({169, 254, _, _}) -> true;
is_loopback_ip({255, 255, 255, 255}) -> true;
is_loopback_ip({_, _, _, _}) -> false.
-endif.

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
	NSeqPeers =
		max(0, ar_meta_db:get(max_propagation_peers) - ?BLOCK_PROPAGATION_PARALLELIZATION),
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
	SendRetry = fun(Peer) ->
		case ar_http_iface_client:send_new_block(Peer, NewB, BDS) of
			{ok, {{<<"412">>, _}, _, _, _, _}} ->
				timer:sleep(5000),
				Send(Peer);
			_ ->
				ok
		end
	end,
	ar_util:pmap(SendRetry, PeersParallel),
	lists:foreach(Send, PeersSequential).

%% @doc Possibly send a new message to external peers.
gossip_to_external(S, {NewGS, Msg}) ->
	send_to_external(S#state { gossip = NewGS }, Msg).

update_state_metrics(Peers) when is_list(Peers) ->
	prometheus_gauge:set(arweave_peer_count, length(Peers));
update_state_metrics(_) ->
	ok.
