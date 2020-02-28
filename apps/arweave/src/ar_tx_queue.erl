-module(ar_tx_queue).
-behaviour(gen_server).

%% API
-export([start_link/0, stop/0]).
-export([set_max_emitters/1, set_max_size/1, set_pause/1]).
-export([add_tx/1, show_queue/0]).
-export([utility/1]).

%% gen_server callbacks
-export([
	init/1, handle_call/3, handle_cast/2,
	terminate/2, code_change/3, format_status/2
]).

-include("ar.hrl").

-record(state, {
	tx_queue,
	emitters_running,
	max_emitters,
	max_size,
	size,
	paused,
	emit_map
}).

-define(EMITTER_START_WAIT, 150).
-define(EMITTER_INTER_WAIT, 5).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	Resp =
		case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
			{ok, _Pid} ->
				ok;
			{error, {already_started, _Pid}} ->
				ok;
			Error ->
				ar:err({?MODULE, error_on_start_link, Error}),
				error
		end,
	case Resp of
		ok -> start_emitters();
		_  -> pass
	end.

start_emitters() ->
	gen_server:call(?MODULE, start_emitters).

stop() ->
	gen_server:stop(?MODULE).

set_pause(PauseOrNot) ->
	gen_server:call(?MODULE, {set_pause, PauseOrNot}).

set_max_size(Bytes) ->
	gen_server:call(?MODULE, {set_max_size, Bytes}).

set_max_emitters(N) ->
	gen_server:call(?MODULE, {set_max_emitters, N}).

add_tx(TX) ->
	gen_server:cast(?MODULE, {add_tx, TX}).

show_queue() ->
	gen_server:call(?MODULE, show_queue).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	MaxEmitters =
		case ar_meta_db:get(max_emitters) of
			not_found -> ?NUM_EMITTER_PROCESSES;
			X -> X
		end,
	{ok, #state {
		tx_queue = gb_sets:new(),
		emitters_running = 0,
		max_emitters = MaxEmitters,
		max_size = ?TX_QUEUE_SIZE_LIMIT,
		size = 0,
		paused = false,
		emit_map = #{}
	}}.

handle_call({set_pause, PauseOrNot}, _From, State) ->
	{reply, ok, State#state { paused = PauseOrNot }};

handle_call({set_max_emitters, N}, _From, State) ->
	{reply, ok, State#state { max_emitters = N }};

handle_call({set_max_size, Bytes}, _From, State) ->
	{reply, ok, State#state { max_size = Bytes }};

handle_call(show_queue, _From, State = #state { tx_queue = Q }) ->
	Reply = show_queue(Q),
	{reply, Reply, State};

handle_call(start_emitters, _From, State) ->
	#state { max_emitters = MaxEmitters, emitters_running = EmittersRunning } = State,
	lists:foreach(
		fun(N) ->
			Wait = N * ?EMITTER_START_WAIT,
			timer:apply_after(Wait, gen_server, cast, [?MODULE, emitter_go])
		end,
		lists:seq(1, max(MaxEmitters - EmittersRunning, 0))
	),
	{reply, ok, State#state { emitters_running = MaxEmitters, paused = false }};

handle_call(_Request, _From, State) ->
	{noreply, State}.

handle_cast({add_tx, TX}, State) ->
	#state { tx_queue = Q, max_size = MaxSize, size = Size} = State,
	TXSize = ?TX_SIZE_BASE + byte_size(TX#tx.data),
	U = utility(TX),
	{NewQ, NewSize, DroppedTXs} =
		maybe_drop(
			gb_sets:add_element({U, {TX, TXSize}}, Q),
			Size + TXSize,
			MaxSize
		),
	case DroppedTXs of
		[] ->
			noop;
		_ ->
			ar:info([
				{event, drop_txs_from_queue},
				{dropped_txs, lists:map(fun(T) -> ar_util:encode(T#tx.id) end, DroppedTXs)}
			]),
			ar_bridge:drop_waiting_txs(whereis(http_bridge_node), DroppedTXs)
	end,
	{noreply, State#state { tx_queue = NewQ, size = NewSize }};

handle_cast(emitter_go, State = #state { paused = true }) ->
	timer:apply_after(?EMITTER_START_WAIT, gen_server, cast, [?MODULE, emitter_go]),
	{noreply, State};
handle_cast(emitter_go, State = #state { emitters_running = EmittersRunning, max_emitters = MaxEmitters }) when EmittersRunning > MaxEmitters ->
	{noreply, State#state { emitters_running = EmittersRunning - 1}};
handle_cast(emitter_go, State = #state { tx_queue = Q, size = Size, emit_map = EmitMap }) ->
	NewState =
		case gb_sets:is_empty(Q) of
			true ->
				timer:apply_after(?EMITTER_START_WAIT, gen_server, cast, [?MODULE, emitter_go]),
				State;
			false ->
				{{_, {TX, TXSize}}, NewQ} = gb_sets:take_largest(Q),
				Bridge = whereis(http_bridge_node),
				Peers = lists:sublist(ar_bridge:get_remote_peers(Bridge), ar_meta_db:get(max_propagation_peers)),
				case Peers of
					[] ->
						gen_server:cast(?MODULE, {emitter_finished, TX}),
						State#state { tx_queue = NewQ, size = Size - TXSize };
					_ ->
						%% Send transactions to the "max_propagation_peers" best peers,
						%% ?TX_PROPAGATION_PARALLELIZATION peers at a time. The resulting
						%% maximum for outbound connections is ?TX_PROPAGATION_PARALLELIZATION * num_emitters.
						lists:foreach(
							fun(_) ->
								gen_server:cast(?MODULE, {emit_tx_to_peer, TX})
							end,
							lists:seq(1, ar_meta_db:get(tx_propagation_parallelization))
						),
						TXID = TX#tx.id,
						ar:info(
							[
								{sending_tx_to_external_peers, ar_util:encode(TXID)},
								{peers, length(Peers)}
							]
						),
						NewEmitMap = EmitMap#{ TXID => #{ peers => Peers, started_at => erlang:timestamp() } },
						State#state { tx_queue = NewQ, size = Size - TXSize, emit_map = NewEmitMap }
				end
		end,
	{noreply, NewState};

handle_cast({emit_tx_to_peer, TX}, State = #state { emit_map = EmitMap }) ->
	#tx{ id = TXID } = TX,
	case EmitMap of
		#{ TXID := #{ peers := [] } } ->
			{noreply, State};
		#{ TXID := TXIDMap = #{ peers := [Peer | Peers] } } ->
			spawn(
				fun() ->
					ar_http_iface_client:send_new_tx(Peer, strip_format_2_data(TX)),
					gen_server:cast(?MODULE, {emitted_tx_to_peer, TX})
				end
			),
			{noreply, State#state{ emit_map = EmitMap#{ TXID => TXIDMap#{ peers => Peers } } }};
		_ ->
			{noreply, State}
	end;

handle_cast({emitted_tx_to_peer, TX}, State = #state { emit_map = EmitMap, tx_queue = Q }) ->
	TXID = TX#tx.id,
	case EmitMap of
		#{ TXID := #{ peers := [], started_at := StartedAt } } ->
			log_propagation_time(
				strip_format_2_data(TX), timer:now_diff(erlang:timestamp(), StartedAt), gb_sets:size(Q)),
			gen_server:cast(?MODULE, {emitter_finished, TX}),
			{noreply, State#state{ emit_map = maps:remove(TXID, EmitMap) }};
		_ ->
			gen_server:cast(?MODULE, {emit_tx_to_peer, TX}),
			{noreply, State}
	end;

handle_cast({emitter_finished, TX}, State) ->
	Bridge = whereis(http_bridge_node),
	timer:apply_after(
		ar_node_utils:calculate_delay(?TX_SIZE_BASE + byte_size((strip_format_2_data(TX))#tx.data)),
		ar_bridge,
		move_tx_to_mining_pool,
		[Bridge, TX]
	),
	timer:apply_after(?EMITTER_INTER_WAIT, gen_server, cast, [?MODULE, emitter_go]),
	{noreply, State};

handle_cast(_Request, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

format_status(_Opt, Status) ->
	Status.

%%%===================================================================
%%% Private functions.
%%%===================================================================

maybe_drop(Q, Size, MaxSize) ->
	maybe_drop(Q, Size, MaxSize, []).

maybe_drop(Q, Size, MaxSize, DroppedTXs) ->
	case Size > MaxSize of
		true ->
			{{_, {TX, DroppedSize}}, NewQ} = gb_sets:take_smallest(Q),
			maybe_drop(NewQ, Size - DroppedSize, MaxSize, [TX | DroppedTXs]);
		false ->
			{Q, Size, lists:filter(fun(TX) -> TX /= none end, DroppedTXs)}
	end.

show_queue(Q) ->
	gb_sets:fold(
		fun({_, {TX, Size}}, Acc) ->
			[{ar_util:encode(TX#tx.id), TX#tx.reward, Size} | Acc]
		end,
		[],
		Q
	).

utility(TX = #tx { data_size = DataSize }) ->
	utility(TX, ?TX_SIZE_BASE + DataSize).

utility(#tx { reward = Reward }, Size) ->
	erlang:trunc(Reward / Size).

strip_format_2_data(#tx{ format = TXFormat } = TX) ->
	case TXFormat of
		2 -> ar_tx:strip_data(TX);
		_Any -> TX
	end.

log_propagation_time(TX, Time, QLen) ->
	DataSize = ?TX_SIZE_BASE + byte_size(TX#tx.data),
	ar:info([
		{sent_tx_to_external_peers, ar_util:encode(TX#tx.id)},
		{data_size, DataSize},
		{time_seconds, Time / 1000000},
		{bytes_per_second, DataSize * 1000000 / Time},
		{queue_length, QLen}
	]).
