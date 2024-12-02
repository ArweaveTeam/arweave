-module(ar_tx_emitter_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(_) ->
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({emit, TXID, Peer, ConnectTimeout, Timeout, ReplyTo}, State) ->
	case ar_mempool:get_tx(TXID) of
		not_found ->
			ok;
		TX ->
			StartedAt = erlang:timestamp(),
			Opts = #{ connect_timeout => ConnectTimeout div 1000
				, timeout => Timeout div 1000
				},
			emit(#{ tx_id => TXID
			      , peer => Peer
			      , tx => TX
			      , started_at => StartedAt
			      , opts => Opts
			      })
	end,
	ReplyTo ! {emitted, TXID, Peer},
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

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

record_propagation_status(not_sent) ->
	ok;
record_propagation_status(Data) ->
	StatusClass = ar_metrics:get_status_class(Data),
	prometheus_counter:inc(propagated_transactions_total, [StatusClass]),
	StatusClass.

record_propagation_rate(PropagatedSize, PropagationTimeUs) ->
	BitsPerSecond = PropagatedSize * 1000000 / PropagationTimeUs * 8,
	prometheus_histogram:observe(tx_propagation_bits_per_second, BitsPerSecond),
	BitsPerSecond.

% retrieve information about peer(s)
emit(#{ tx := TX, peer := Peer } = Data) ->
	TrustedPeers = ar_peers:get_trusted_peers(),
	PropagatedTX = tx_to_propagated_tx(TX, Peer, TrustedPeers),
	Release = ar_peers:get_peer_release(Peer),
	NewData = Data#{ propagated_tx => PropagatedTX
		       , trusted_peers => TrustedPeers
		       , release => Release
		       },
	emit2(NewData).

% depending on the version of the peer, different kind of payload
% is being used, one in binary, another one in JSON.
emit2(#{ release := Release, peer := Peer, propagated_tx := PropagatedTX,
	 tx_id := TXID, opts := Opts } = Data)
	when Release >= 42 ->
		Bin = ar_serialize:tx_to_binary(PropagatedTX),
		Reply = ar_http_iface_client:send_tx_binary(Peer, TXID, Bin, Opts),
		NewData = Data#{ reply => Reply },
		emit3(NewData);
emit2(#{ peer := Peer, propagated_tx := PropagatedTX, tx_id := TXID,
	 opts := Opts } = Data) ->
	Serialize = ar_serialize:tx_to_json_struct(PropagatedTX),
	JSON = ar_serialize:jsonify(Serialize),
	Reply = ar_http_iface_client:send_tx_json(Peer, TXID, JSON, Opts),
	NewData = Data#{ reply => Reply },
	emit3(NewData).

% deal with the reply and update propagation statistics.
emit3(#{ started_at := StartedAt, reply := Reply, tx := TX } = Data) ->
	Timestamp = erlang:timestamp(),
	PropagationTimeUs = timer:now_diff(Timestamp, StartedAt),
	PropagationStatus = record_propagation_status(Reply),
	PropagatedSize = tx_propagated_size(TX),
	PropagationRate = record_propagation_rate(PropagatedSize, PropagationTimeUs),
	Data#{ propagation_time_us => PropagationTimeUs
	       , propagation_status => PropagationStatus
	       , propagated_size => PropagatedSize
	       , propagation_rate => PropagationRate
	       }.
