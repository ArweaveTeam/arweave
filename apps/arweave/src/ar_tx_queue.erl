-module(ar_tx_queue).

-behaviour(gen_server).

%% API
-export([
	start_link/0, stop/0,
	set_max_emitters/1, set_max_header_size/1, set_max_data_size/1, set_pause/1,
	show_queue/0,
	utility/1
]).

%% gen_server callbacks
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

-record(state, {
	tx_queue,
	emitters_running,
	max_emitters,
	max_header_size,
	max_data_size,
	header_size,
	data_size,
	paused,
	emit_map
}).

-define(EMITTER_START_WAIT, 150).
-define(EMITTER_INTER_WAIT, 5).

%% Prioritize format=1 transactions with data size bigger than this
%% value (in bytes) lower than every other transaction. The motivation
%% is to encourage people uploading data to use the new v2 transaction
%% format. Large v1 transactions may significantly slow down the rate
%% of acceptance of transactions into the weave.
-define(DEPRIORITIZE_V1_TX_SIZE_THRESHOLD, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_emitters() ->
	gen_server:cast(?MODULE, start_emitters).

stop() ->
	gen_server:stop(?MODULE).

set_pause(PauseOrNot) ->
	gen_server:call(?MODULE, {set_pause, PauseOrNot}).

set_max_header_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_header_size, Bytes}).

set_max_data_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_data_size, Bytes}).

set_max_emitters(N) ->
	gen_server:call(?MODULE, {set_max_emitters, N}).

show_queue() ->
	gen_server:call(?MODULE, show_queue).

%%%===================================================================
%%% Generic server callbacks.
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
	MaxEmitters =
		case ar_meta_db:get(max_emitters) of
			not_found -> ?NUM_EMITTER_PROCESSES;
			X -> X
		end,
	ok = ar_events:subscribe(tx),
	start_emitters(),
	{ok, #state{
		tx_queue = gb_sets:new(),
		emitters_running = 0,
		max_emitters = MaxEmitters,
		max_header_size = ?TX_QUEUE_HEADER_SIZE_LIMIT,
		max_data_size = ?TX_QUEUE_DATA_SIZE_LIMIT,
		header_size = 0,
		data_size = 0,
		paused = false,
		emit_map = #{}
	}}.

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
handle_call({set_pause, PauseOrNot}, _From, State) ->
	{reply, ok, State#state{ paused = PauseOrNot }};

handle_call({set_max_emitters, N}, _From, State) ->
	{reply, ok, State#state{ max_emitters = N }};

handle_call({set_max_header_size, Bytes}, _From, State) ->
	{reply, ok, State#state{ max_header_size = Bytes }};

handle_call({set_max_data_size, Bytes}, _From, State) ->
	{reply, ok, State#state{ max_data_size = Bytes }};

handle_call(show_queue, _From, State = #state{ tx_queue = Q }) ->
	Reply = show_queue(Q),
	{reply, Reply, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
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
handle_cast(start_emitters, State) ->
	#state{ max_emitters = MaxEmitters, emitters_running = EmittersRunning } = State,
	lists:foreach(
		fun(N) ->
			Wait = N * ?EMITTER_START_WAIT,
			timer:apply_after(Wait, gen_server, cast, [?MODULE, emitter_go])
		end,
		lists:seq(1, max(MaxEmitters - EmittersRunning, 0))
	),
	{noreply, State#state{ emitters_running = MaxEmitters, paused = false }};


handle_cast(emitter_go, State = #state{ paused = true }) ->
	timer:apply_after(?EMITTER_START_WAIT, gen_server, cast, [?MODULE, emitter_go]),
	{noreply, State};
handle_cast(emitter_go, State = #state{ emitters_running = EmittersRunning, max_emitters = MaxEmitters }) when EmittersRunning > MaxEmitters ->
	{noreply, State#state { emitters_running = EmittersRunning - 1}};
handle_cast(emitter_go, State) ->
	#state{
		tx_queue = Q,
		header_size = HeaderSize,
		data_size = DataSize,
		emit_map = EmitMap
	} = State,
	NewState =
		case gb_sets:is_empty(Q) of
			true ->
				timer:apply_after(?EMITTER_START_WAIT, gen_server, cast, [?MODULE, emitter_go]),
				State;
			false ->
				{{_, {TX, {TXHeaderSize, TXDataSize}}}, NewQ} = gb_sets:take_largest(Q),
				{Peers, TrustedPeers} = get_peers(),
				case Peers of
					[] ->
						gen_server:cast(?MODULE, {emitter_finished, TX}),
						State#state{
							tx_queue = NewQ,
							header_size = HeaderSize - TXHeaderSize,
							data_size = DataSize - TXDataSize
						};
					_ ->
						%% Send transactions to the "max_propagation_peers" best peers,
						%% tx_propagation_parallelization peers at a time. The resulting
						%% maximum for outbound connections is
						%% tx_propagation_parallelization * num_emitters.
						lists:foreach(
							fun(_) ->
								gen_server:cast(?MODULE, {emit_tx_to_peer, TX})
							end,
							lists:seq(1, ar_meta_db:get(tx_propagation_parallelization))
						),
						TXID = TX#tx.id,
						NewEmitMap = EmitMap#{
							TXID => #{
								peers => Peers,
								started_at => erlang:timestamp(),
								trusted_peers => TrustedPeers
							}},
						State#state{
							tx_queue = NewQ,
							header_size = HeaderSize - TXHeaderSize,
							data_size = DataSize - TXDataSize,
							emit_map = NewEmitMap
						}
				end
		end,
	{noreply, NewState};

handle_cast({emit_tx_to_peer, TX}, State = #state{ emit_map = EmitMap }) ->
	#tx{ id = TXID } = TX,
	case EmitMap of
		#{ TXID := #{ peers := [] } } ->
			{noreply, State};
		#{ TXID := TXIDMap = #{ peers := [Peer | Peers], trusted_peers := TrustedPeers } } ->
			spawn(
				fun() ->
					PropagatedTX = tx_to_propagated_tx(TX, Peer, TrustedPeers),
					Reply = ar_http_iface_client:send_new_tx(Peer, PropagatedTX),
					gen_server:cast(?MODULE, {emitted_tx_to_peer, {Reply, TX}})
				end
			),
			{noreply, State#state{ emit_map = EmitMap#{ TXID => TXIDMap#{ peers => Peers } } }};
		_ ->
			{noreply, State}
	end;

handle_cast({emitted_tx_to_peer, {Reply, TX}}, State = #state{ emit_map = EmitMap, tx_queue = Q }) ->
	TXID = TX#tx.id,
	case EmitMap of
		#{ TXID := #{ peers := [], started_at := StartedAt } } ->
			PropagationTimeUs = timer:now_diff(erlang:timestamp(), StartedAt),
			record_propagation_status(Reply),
			record_propagation_rate(tx_propagated_size(TX), PropagationTimeUs),
			record_queue_size(gb_sets:size(Q)),
			gen_server:cast(?MODULE, {emitter_finished, TX}),
			{noreply, State#state{ emit_map = maps:remove(TXID, EmitMap) }};
		_ ->
			gen_server:cast(?MODULE, {emit_tx_to_peer, TX}),
			{noreply, State}
	end;

handle_cast({emitter_finished, TX}, State) ->
	%% Send this TX to the mining pool with some delay depending on the TX size.
	timer:apply_after(
		ar_node_utils:calculate_delay(tx_propagated_size(TX)),
		ar_events,
		send,
		[tx, {ready_for_mining, TX}]
	),
	timer:apply_after(?EMITTER_INTER_WAIT, gen_server, cast, [?MODULE, emitter_go]),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
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
handle_info({event, tx, {new, TX, Source}}, State) ->
	#state{
		tx_queue = Q,
		max_header_size = MaxHeaderSize,
		max_data_size = MaxDataSize,
		header_size = HeaderSize,
		data_size = DataSize
	} = State,
	?LOG_DEBUG("TX Queue new tx received ~p from ~p", [ar_util:encode(TX#tx.id), Source]),
	{TXHeaderSize, TXDataSize} = tx_queue_size(TX),
	U = utility(TX),
	Item = {U, {TX, {TXHeaderSize, TXDataSize}}},
	case gb_sets:is_element(Item, Q) of
		true ->
			{noreply, State};
		false ->
			{NewQ, {NewHeaderSize, NewDataSize}, DroppedTXs} =
				maybe_drop(
					gb_sets:add_element(Item, Q),
					{HeaderSize + TXHeaderSize, DataSize + TXDataSize},
					{MaxHeaderSize, MaxDataSize}
				),
			case DroppedTXs of
				[] ->
					noop;
				_ ->
					DroppedIDs = lists:map(
						fun(DroppedTX) ->
							case TX#tx.format of
								2 ->
									ar_data_sync:maybe_drop_data_root_from_disk_pool(
										DroppedTX#tx.data_root,
										DroppedTX#tx.data_size,
										DroppedTX#tx.id
									);
								_ ->
									nothing_to_drop_from_disk_pool
							end,
							ar_events:send(tx, {dropped, DroppedTX, removed_from_tx_queue}),
							ar_util:encode(DroppedTX#tx.id)
						end,
						DroppedTXs
					),
					case lists:member(TX#tx.id, DroppedIDs) of
						false ->
							ar_ignore_registry:add(TX#tx.id);
						true ->
							ok
					end,
					?LOG_INFO([
						{event, drop_txs_from_queue},
						{dropped_txs, DroppedIDs}
					])
			end,
			NewState = State#state{
				tx_queue = NewQ,
				header_size = NewHeaderSize,
				data_size = NewDataSize
			},
			{noreply, NewState}
	end;

handle_info({event, tx, {dropped, TX, Reason}}, State) ->
	#state{
		tx_queue = Q,
		header_size = HeaderSize,
		data_size = DataSize
	} = State,
	{TXHeaderSize, TXDataSize} = tx_queue_size(TX),
	U = utility(TX),
	Item = {U, {TX, {TXHeaderSize, TXDataSize}}},
	case gb_sets:is_element(Item, Q) of
		true ->
			?LOG_DEBUG("Drop TX from queue ~p with reason: ~p", [ar_util:encode(TX#tx.id), Reason]),
			{noreply, State#state{
				tx_queue = gb_sets:del_element(Item, Q),
				header_size = HeaderSize - TXHeaderSize,
				data_size = DataSize - TXDataSize
			}};
		false ->
			{noreply, State}
	end;

handle_info({event, tx, _Event}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
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
terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_tx_queue_terminated}, {reason, Reason}]),
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
%%% Private functions.
%%%===================================================================

maybe_drop(Q, Size, MaxSize) ->
	maybe_drop(Q, Size, MaxSize, []).

maybe_drop(Q, {HeaderSize, DataSize} = Size, {MaxHeaderSize, MaxDataSize} = MaxSize, DroppedTXs) ->
	case HeaderSize > MaxHeaderSize orelse DataSize > MaxDataSize of
		true ->
			{{_, {TX, {DroppedHeaderSize, DroppedDataSize}}}, NewQ} = gb_sets:take_smallest(Q),
			maybe_drop(
				NewQ,
				{HeaderSize - DroppedHeaderSize, DataSize - DroppedDataSize},
				MaxSize,
				[TX | DroppedTXs]
			);
		false ->
			{Q, Size, lists:filter(fun(TX) -> TX /= none end, DroppedTXs)}
	end.

get_peers() ->
	Peers =
		lists:sublist(ar_bridge:get_remote_peers(10000), ar_meta_db:get(max_propagation_peers)),
	{ok, Config} = application:get_env(arweave, config),
	TrustedPeers = Config#config.peers,
	{join_peers(Peers, TrustedPeers), TrustedPeers}.

join_peers(Peers, [TrustedPeer | TrustedPeers]) ->
	case lists:member(TrustedPeer, Peers) of
		true ->
			join_peers(Peers, TrustedPeers);
		false ->
			join_peers([TrustedPeer | Peers], TrustedPeers)
	end;
join_peers(Peers, []) ->
	Peers.

show_queue(Q) ->
	gb_sets:fold(
		fun({_, {TX, _}}, Acc) ->
			[{ar_util:encode(TX#tx.id), TX#tx.reward, TX#tx.data_size} | Acc]
		end,
		[],
		Q
	).

utility(TX = #tx{ data_size = DataSize }) ->
	utility(TX, ?TX_SIZE_BASE + DataSize).

utility(#tx{ format = 1, reward = Reward, data_size = DataSize }, Size)
		when DataSize > ?DEPRIORITIZE_V1_TX_SIZE_THRESHOLD ->
	{1, erlang:trunc(Reward / Size)};
utility(#tx{ reward = Reward }, Size) ->
	{2, erlang:trunc(Reward / Size)}.

tx_propagated_size(#tx{ format = 2 }) ->
	?TX_SIZE_BASE;
tx_propagated_size(#tx{ format = 1, data = Data }) ->
	?TX_SIZE_BASE + byte_size(Data).

tx_to_propagated_tx(#tx{ format = 1 } = TX, _Peer, _TrustedPeers) ->
	TX;
tx_to_propagated_tx(#tx{ format = 2 } = TX, Peer, TrustedPeers) ->
	case lists:member(Peer, TrustedPeers) of
		true ->
			TX;
		false ->
			TX#tx{ data = <<>> }
	end.

tx_queue_size(#tx{ format = 1 } = TX) ->
	{tx_propagated_size(TX), 0};
tx_queue_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

record_propagation_status(not_sent) ->
	ok;
record_propagation_status(Data) ->
	prometheus_counter:inc(propagated_transactions_total, [ar_metrics:get_status_class(Data)]).

record_propagation_rate(PropagatedSize, PropagationTimeUs) ->
	BitsPerSecond = PropagatedSize * 1000000 / PropagationTimeUs * 8,
	prometheus_histogram:observe(tx_propagation_bits_per_second, BitsPerSecond).

record_queue_size(QSize) ->
	prometheus_gauge:set(tx_queue_size, QSize).
