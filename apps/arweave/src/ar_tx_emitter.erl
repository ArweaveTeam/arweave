-module(ar_tx_emitter).

-behaviour(gen_server).

-export([start_link/2, pick_peers/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

%% Remove identifiers of recently emitted transactions from the cache after this long.
-define(CLEANUP_RECENTLY_EMITTED_TIMEOUT, 60 * 60 * 1000).

%% How long to wait for a reply from the emitter worker before considering it failed.
-define(WORKER_TIMEOUT, 30 * 1000).

%% How frequently to check whether new transactions are appeared for distribution.
-define(CHECK_MEMPOOL_FREQUENCY, 1000).

-record(state, {
	currently_emitting,
	workers
}).

%% How many transactions to send to emitters at one go. With CHUNK_SIZE=1, the propagation
%% speed is determined by the slowest peer among those chosen for the given transaction.
%% Increasing CHUNK_SIZE reduces the influence of slow peers at the cost of RAM (message
%% queues for transaction emitter workers.
-define(CHUNK_SIZE, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Return a list of peers where 80% of the peers are randomly chosen
%% from the first 20% of Peers and the other 20% of the peers are randomly
%% chosen from the other 80% of Peers.
pick_peers(Peers, N) ->
	pick_peers(Peers, length(Peers), N).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(Workers) ->
	gen_server:cast(?MODULE, process_chunk),
	{ok, #state{ workers = queue:from_list(Workers), currently_emitting = sets:new() }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(process_chunk, State) ->
	#state{ workers = Q, currently_emitting = Emitting } = State,
	case {ar_bridge:get_remote_peers(), ets:lookup(node_state, tx_propagation_queue)} of
		{[], _} ->
			ar_util:cast_after(?CHECK_MEMPOOL_FREQUENCY, ?MODULE, process_chunk),
			{noreply, State};
		{_, []} ->
			ar_util:cast_after(?CHECK_MEMPOOL_FREQUENCY, ?MODULE, process_chunk),
			{noreply, State};
		{Peers, [{tx_propagation_queue, Set}]} ->
			{ok, Config} = application:get_env(arweave, config),
			{Q2, Emitting2} = emit(Set, Q, Emitting, Peers,
					Config#config.max_propagation_peers, ?CHUNK_SIZE),
			case sets:is_empty(Emitting2) of
				true ->
					ar_util:cast_after(?CHECK_MEMPOOL_FREQUENCY, ?MODULE, process_chunk);
				false ->
					ok
			end,
			{noreply, State#state{ workers = Q2, currently_emitting = Emitting2 }}
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({emitted, TXID, Peer}, State) ->
	#state{ currently_emitting = Emitting } = State,
	case sets:is_element({TXID, Peer}, Emitting) of
		false ->
			%% Should have been cleaned up by timeout.
			{noreply, State};
		true ->
			Emitting2 = sets:del_element({TXID, Peer}, Emitting),
			case sets:is_empty(Emitting2) of
				true ->
					gen_server:cast(?MODULE, process_chunk);
				false ->
					ok
			end,
			{noreply, State#state{ currently_emitting = Emitting2 }}
	end;

handle_info({timeout, TXID, Peer}, State) ->
	#state{ currently_emitting = Emitting } = State,
	case sets:is_element({TXID, Peer}, Emitting) of
		false ->
			%% Should have been emitted.
			{noreply, State};
		true ->
			Emitting2 = sets:del_element({TXID, Peer}, Emitting),
			case sets:is_empty(Emitting2) of
				true ->
					gen_server:cast(?MODULE, process_chunk);
				false ->
					ok
			end,
			{noreply, State#state{ currently_emitting = Emitting2 }}
	end;

handle_info({remove_from_recently_emitted, TXID}, State) ->
	ets:delete(ar_tx_emitter_recently_emitted, TXID),
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

emit(_Set, Q, Emitting, _Peers, _MaxPeers, N) when N =< 0 ->
	{Q, Emitting};
emit(Set, Q, Emitting, Peers, MaxPeers, N) ->
	case gb_sets:is_empty(Set) of
		true ->
			{Q, Emitting};
		false ->
			{{Utility, TXID}, Set2} = gb_sets:take_largest(Set),
			case ets:member(ar_tx_emitter_recently_emitted, TXID) of
				true ->
					emit(Set2, Q, Emitting, Peers, MaxPeers, N);
				false ->
					PickedPeers = pick_peers(Peers, MaxPeers),
					{Emitting2, Q2} =
						lists:foldl(
							fun(Peer, {Acc, Workers}) ->
								{{value, W}, Workers2} = queue:out(Workers),
								gen_server:cast(W, {emit, TXID, Peer, self()}),
								erlang:send_after(?WORKER_TIMEOUT, ?MODULE,
										{timeout, TXID, Peer}),
								{sets:add_element({TXID, Peer}, Acc), queue:in(W, Workers2)}
							end,
							{Emitting, Q},
							PickedPeers
						),
					%% The cache storing recently emitted transactions is used instead
					%% of an explicit synchronization of the propagation queue updates
					%% with ar_node_worker - we do not rely on ar_node_worker removing
					%% emitted transactions from the queue on time.
					ets:insert(ar_tx_emitter_recently_emitted, {TXID}),
					erlang:send_after(?CLEANUP_RECENTLY_EMITTED_TIMEOUT, ?MODULE,
							{remove_from_recently_emitted, TXID}),
					ar_events:send(tx, {emitting_scheduled, Utility, TXID}),
					emit(Set2, Q2, Emitting2, Peers, MaxPeers, N - 1)
			end
	end.

pick_peers(Peers, PeerLen, N) when N >= PeerLen ->
	Peers;
pick_peers([], _PeerLen, _N) ->
	[];
pick_peers(_Peers, _PeerLen, 0) ->
	[];
pick_peers(Peers, PeerLen, N) ->
	{Best, Other} = lists:split(max(PeerLen div 5, 1), Peers),
	TakeBest = max(8 * N div 10, 1),
	Part1 = ar_util:pick_random(Best, min(length(Best), TakeBest)),
	Part2 = ar_util:pick_random(Other, min(length(Other), N - TakeBest)),
	Part1 ++ Part2.
