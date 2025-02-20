-module(ar_tx_emitter).

-behaviour(gen_server).

-export([start_link/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("../include/ar.hrl").
-include("../include/ar_config.hrl").

%% Remove identifiers of recently emitted transactions from the cache after this long.
-define(CLEANUP_RECENTLY_EMITTED_TIMEOUT, 60 * 60 * 1000).

-define(WORKER_CONNECT_TIMEOUT, 1 * 1000).
-define(WORKER_REQUEST_TIMEOUT, 5 * 1000).

%% How frequently to check whether new transactions are appeared for distribution.
-define(CHECK_MEMPOOL_FREQUENCY, 1000).

-record(state, {
	currently_emitting,
	workers,
	%% How long to wait for a reply from the emitter worker before considering it failed.
	worker_failed_timeout

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

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(Workers) ->
	gen_server:cast(?MODULE, process_chunk),

	NumWorkers = length(Workers),
	NumPeers = max_propagation_peers(),
	JobsPerWorker = (?CHUNK_SIZE * NumPeers) div NumWorkers,
	%% Only time out a worker after we've given enough time for *all* workers to complete
	%% their tasks (including a small 1000 ms buffer). This should prevent a cascade where
	%% worker queues keep growing.
	WorkerFailedTimeout = JobsPerWorker * 
		(?WORKER_CONNECT_TIMEOUT + ?WORKER_REQUEST_TIMEOUT + 1000),

	State = #state{ workers = queue:from_list(Workers)
		      , currently_emitting = sets:new()
		      , worker_failed_timeout = WorkerFailedTimeout
		      },
	{ok, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(process_chunk, State) ->
	% only current (active) peers should be used, using lifetime
	% peers will create unecessary timeouts. The first to
	% contact are the trusted peers.
	TrustedPeers = ar_peers:get_trusted_peers(),
	CurrentPeers = ar_peers:get_peers(current),
	FilteredPeers = ar_peers:filter_peers(CurrentPeers, {timestamp, 60*60*24}),
	CleanedPeers = FilteredPeers -- TrustedPeers,
	Peers = TrustedPeers ++ CleanedPeers,

	% prepare to emit chunk(s)
	PropagationQueue = ar_mempool:get_propagation_queue(),
	PropagationMax = max_propagation_peers(),
	State2 = emit(
		PropagationQueue, Peers, PropagationMax, ?CHUNK_SIZE, State),

	% check later if emit/6 returns an empty set
	case sets:is_empty(State2#state.currently_emitting) of
		true ->
			ar_util:cast_after(?CHECK_MEMPOOL_FREQUENCY, ?MODULE, process_chunk);
		false ->
			ok
	end,

	{noreply, State2};

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
			?LOG_DEBUG([{event, tx_propagation_timeout}, {txid, ar_util:encode(TXID)},
					{peer, ar_util:format_peer(Peer)}]),
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

max_propagation_peers() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.max_propagation_peers.

emit(_Set, _Peers, _MaxPeers, N, State) when N =< 0 ->
	State;
emit(Set, Peers, MaxPeers, N, State) ->
	case gb_sets:is_empty(Set) of
		true ->
			State;
		false ->
			emit_set_not_empty(Set, Peers, MaxPeers, N, State)
	end.

emit_set_not_empty(Set, Peers, MaxPeers, N, State) ->
	{{Utility, TXID}, Set2} = gb_sets:take_largest(Set),
	case ets:member(ar_tx_emitter_recently_emitted, TXID) of
		true ->
			emit(Set2, Peers, MaxPeers, N, State);
		false ->
			#state{ 
				workers = Q,
				currently_emitting = Emitting,
				worker_failed_timeout = WorkerFailedTimeout
			} = State,
			% only a subset of the whole peers list is
			% taken using max_propagation_peers value.
			% the first N peers will be used instead of
			% the whole list. unfortunately, this list can
			% also have not connected peers.
			PeersToSync = lists:sublist(Peers, MaxPeers),

			% for each peers in the sublist, a chunk is
			% sent. The workers are taken one by one from
			% a FIFO, mainly used to distribute the
			% messages across all available workers.
			Foldl = fun(Peer, {Acc, Workers}) ->
				{{value, W}, Workers2} = queue:out(Workers),
				gen_server:cast(W, 
					{emit, TXID, Peer, 
						?WORKER_CONNECT_TIMEOUT, ?WORKER_REQUEST_TIMEOUT, self()}),
				erlang:send_after(WorkerFailedTimeout, ?MODULE, {timeout, TXID, Peer}),
				{sets:add_element({TXID, Peer}, Acc), queue:in(W, Workers2)}
			end,
			{Emitting2, Q2} = lists:foldl(Foldl, {Emitting, Q}, PeersToSync),
			State2 = State#state{
				workers = Q2,
				currently_emitting = Emitting2
			},

			%% The cache storing recently emitted transactions is used instead
			%% of an explicit synchronization of the propagation queue updates
			%% with ar_node_worker - we do not rely on ar_node_worker removing
			%% emitted transactions from the queue on time.
			ets:insert(ar_tx_emitter_recently_emitted, {TXID}),
			erlang:send_after(?CLEANUP_RECENTLY_EMITTED_TIMEOUT, ?MODULE,
				{remove_from_recently_emitted, TXID}),
			ar_events:send(tx, {emitting_scheduled, Utility, TXID}),
			emit(Set2, Peers, MaxPeers, N - 1, State2)
	end.
