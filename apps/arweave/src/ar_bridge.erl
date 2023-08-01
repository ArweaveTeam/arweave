%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html

%%% @doc The module gossips blocks to peers.
-module(ar_bridge).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	block_propagation_queue = gb_sets:new(),
	workers
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

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
init(Workers) ->
	process_flag(trap_exit, true),
	ar_events:subscribe(block),
	WorkerMap = lists:foldl(fun(W, Acc) -> maps:put(W, free, Acc) end, #{}, Workers),
	State = #state{ workers = WorkerMap },
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
handle_call(Request, _From, State) ->
	?LOG_WARNING("unhandled call: ~p", [Request]),
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

handle_cast({may_be_send_block, W}, State) ->
	#state{ workers = Workers, block_propagation_queue = Q } = State,
	case dequeue(Q) of
		empty ->
			{noreply, State};
		{{_Priority, Peer, BlockData}, Q2} ->
			case maps:get(W, Workers) of
				free ->
					send_to_worker(Peer, BlockData, W),
					{noreply, State#state{ block_propagation_queue = Q2,
							workers = maps:put(W, busy, Workers) }};
				busy ->
					{noreply, State}
			end
	end;

handle_cast(Msg, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
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
handle_info({event, block, {new, _B, #{ gossip := false }}}, State) ->
	{noreply, State};
handle_info({event, block, {new, B, _}}, State) ->
	#state{ block_propagation_queue = Q, workers = Workers } = State,
	case ar_block_cache:get(block_cache, B#block.previous_block) of
		not_found ->
			%% The cache should have been just pruned and this block is old.
			{noreply, State};
		_ ->
			{ok, Config} = application:get_env(arweave, config),
			TrustedPeers = ar_peers:get_trusted_peers(),
			SpecialPeers = Config#config.block_gossip_peers,
			Peers = ((SpecialPeers ++ ar_peers:get_peers(lifetime)) -- TrustedPeers) ++ TrustedPeers,
			JSON =
				case B#block.height >= ar_fork:height_2_6() of
					true ->
						none;
					false ->
						block_to_json(B)
				end,
			Q2 = enqueue_block(Peers, B#block.height, {JSON, B}, Q),
			[gen_server:cast(?MODULE, {may_be_send_block, W}) || W <- maps:keys(Workers)],
			{noreply, State#state{ block_propagation_queue = Q2 }}
	end;

handle_info({event, block, _}, State) ->
	{noreply, State};

handle_info({worker_sent_block, W},
		#state{ workers = Workers, block_propagation_queue = Q } = State) ->
	case dequeue(Q) of
		empty ->
			{noreply, State#state{ workers = maps:put(W, free, Workers) }};
		{{_Priority, Peer, BlockData}, Q2} ->
			send_to_worker(Peer, BlockData, W),
			{noreply, State#state{ block_propagation_queue = Q2,
					workers = maps:put(W, busy, Workers) }}
	end;

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

enqueue_block(Peers, Height, BlockData, Q) ->
	enqueue_block(Peers, Height, BlockData, Q, 0).

enqueue_block([], _Height, _BlockData, Q, _N) ->
	Q;
enqueue_block([Peer | Peers], Height, BlockData, Q, N) ->
	{_JSON, Block} = BlockData,
	?LOG_INFO([{event, enqueue_block}, {peer, ar_util:format_peer(Peer)}, {height, Height}, {hash, ar_util:encode(Block#block.hash)}]),
	Priority = {N, Height},
	enqueue_block(Peers, Height, BlockData,
			gb_sets:add_element({Priority, Peer, BlockData}, Q), N + 1).

dequeue(Q) ->
	case gb_sets:is_empty(Q) of
		true ->
			empty;
		false ->
			gb_sets:take_smallest(Q)
	end.

send_to_worker(Peer, {JSON, B}, W) ->
	#block{ height = Height, indep_hash = H, previous_block = PrevH, txs = TXs,
			hash = SolutionH } = B,
	Release = ar_peers:get_peer_release(Peer),
	Fork_2_6 = ar_fork:height_2_6(),
	SolutionH2 = case Height >= ar_fork:height_2_6() of true -> SolutionH; _ -> undefined end,
	case Release >= 52 orelse Height >= Fork_2_6 of
		true ->
			SendAnnouncementFun =
				fun() ->
					Announcement = #block_announcement{ indep_hash = H,
							previous_block = PrevH,
							recall_byte = B#block.recall_byte,
							recall_byte2 = B#block.recall_byte2,
							solution_hash = SolutionH2,
							tx_prefixes = [ar_node_worker:tx_id_prefix(ID)
									|| #tx{ id = ID } <- TXs] },
					ar_http_iface_client:send_block_announcement(Peer, Announcement)
				end,
			SendFun =
				fun(MissingChunk, MissingChunk2, MissingTXs) ->
					%% Some transactions might be absent from our mempool. We still gossip
					%% this block further and search for the missing transactions afterwads
					%% (the process is initiated by ar_node_worker). We are gradually moving
					%% to the new process where blocks are sent over POST /block2 along with
					%% all the missing transactions specified in the preceding
					%% POST /block_announcement reply. Once the network adopts the new release,
					%% we will turn off POST /block and remove the missing transactions search
					%% in ar_node_worker.
					case determine_included_transactions(TXs, MissingTXs) of
						missing ->
							case Height >= ar_fork:height_2_6() of
								true ->
									%% POST /block is not supported after 2.6.
									%% The recipient would have to download this block
									%% along with its transactions via ar_poller (which
									%% we made trustless in the 2.6 release).
									ok;
								false ->
									ar_http_iface_client:send_block_json(Peer, H, JSON)
							end;
						TXs2 ->
							PoA = case MissingChunk of true -> B#block.poa;
									false -> (B#block.poa)#poa{ chunk = <<>> } end,
							PoA2 = case MissingChunk2 of false ->
									(B#block.poa2)#poa{ chunk = <<>> };
									_ -> B#block.poa2 end,
							Bin = ar_serialize:block_to_binary(B#block{ txs = TXs2,
									poa = PoA, poa2 = PoA2 }),
							ar_http_iface_client:send_block_binary(Peer, H, Bin,
									B#block.recall_byte)
					end
				end,
			gen_server:cast(W, {send_block2, Peer, SendAnnouncementFun, SendFun, 1, self()});
		false ->
			SendFun = fun() -> ar_http_iface_client:send_block_json(Peer, H, JSON) end,
			gen_server:cast(W, {send_block, SendFun, 1, self()})
	end.

block_to_json(B) ->
	BDS = ar_block:generate_block_data_segment(B),
	{BlockProps} = ar_serialize:block_to_json_struct(B),
	PostProps = [
		{<<"new_block">>, {BlockProps}},
		%% Add the P2P port field to be backwards compatible with nodes
		%% running the old version of the P2P port feature.
		{<<"port">>, ?DEFAULT_HTTP_IFACE_PORT},
		{<<"block_data_segment">>, ar_util:encode(BDS)}
	],
	ar_serialize:jsonify({PostProps}).

%% @doc Return the list of transactions to gossip or 'missing'. TXs is a list of possibly
%% both tx records and transaction identifiers - whatever is found in the gossiped block.
%% MissingTXs is a list of possibly both 0-based indices and tx identifiers. The items
%% in the new list are in the same order they occur in TXs. Identifiers are simply placed
%% as-is in the new list. The tx records might be converted to their identifiers (to avoid
%% sending the entire transactions to peers who already know them) if either their 0-based
%% indices or identifiers are found in MissingTXs. Elements in MissingTXs are assumed sorted
%% in the order of their appearance in TXs. Return 'missing' if TXs contains an identifier (
%% not a tx record) which (or its index) is found in MissingTXs.
determine_included_transactions(TXs, MissingTXs) ->
	determine_included_transactions(TXs, MissingTXs, [], 0).

determine_included_transactions([], _MissingTXs, Included, _N) ->
	lists:reverse(Included);
determine_included_transactions([TXIDOrTX | TXs], [], Included, N) ->
	determine_included_transactions(TXs, [], [tx_id(TXIDOrTX) | Included], N);
determine_included_transactions([TXIDOrTX | TXs], [TXIDOrIndex | MissingTXs], Included, N) ->
	TXID = tx_id(TXIDOrTX),
	case TXIDOrIndex == N orelse TXIDOrIndex == TXID of
		true ->
			case TXID == TXIDOrTX of
				true ->
					missing;
				false ->
					determine_included_transactions(TXs, MissingTXs, [strip_v2_data(TXIDOrTX)
							| Included], N + 1)
			end;
		false ->
			determine_included_transactions(TXs, [TXIDOrIndex | MissingTXs], [TXID | Included],
					N + 1)
	end.

tx_id(#tx{ id = TXID }) ->
	TXID;
tx_id(TXID) ->
	TXID.

strip_v2_data(#tx{ format = 2 } = TX) ->
	TX#tx{ data = <<>> };
strip_v2_data(TX) ->
	TX.
