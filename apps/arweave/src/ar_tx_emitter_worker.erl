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

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({emit, TXID, Peer, ReplyTo}, State) ->
	case ar_mempool:get_tx(TXID) of
		not_found ->
			ok;
		TX ->
			StartedAt = erlang:timestamp(),
			TrustedPeers = ar_peers:get_trusted_peers(),
			PropagatedTX = tx_to_propagated_tx(TX, Peer, TrustedPeers),
			Release = ar_peers:get_peer_release(Peer),
			SendFun =
				case Release >= 42 of
					true ->
						fun() ->
							Bin = ar_serialize:tx_to_binary(PropagatedTX),
							ar_http_iface_client:send_tx_binary(Peer, TXID, Bin)
						end;
					false ->
						fun() ->
							JSON = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(
									PropagatedTX)),
							ar_http_iface_client:send_tx_json(Peer, TXID, JSON)
						end
				end,
			Reply = SendFun(),
			PropagationTimeUs = timer:now_diff(erlang:timestamp(), StartedAt),
			record_propagation_status(Reply),
			record_propagation_rate(tx_propagated_size(TX), PropagationTimeUs)
	end,
	ReplyTo ! {emitted, TXID, Peer},
	{noreply, State};

handle_cast(Msg, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info({gun_down, _, http, normal, _, _}, State) ->
	{noreply, State};
handle_info({gun_down, _, http, closed, _, _}, State) ->
	{noreply, State};
handle_info({gun_down, _, http, {error,econnrefused}, _, _}, State) ->
	{noreply, State};
handle_info({gun_up, _, http}, State) ->
	{noreply, State};
handle_info({gun_response, _, _, _, _, _}, State) ->
	{noreply, State};
handle_info({gun_data, _, _, _, _}, state) ->
	{noreply, state};
handle_info({gun_error, _, _, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
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
	prometheus_counter:inc(propagated_transactions_total, [ar_metrics:get_status_class(Data)]).

record_propagation_rate(PropagatedSize, PropagationTimeUs) ->
	BitsPerSecond = PropagatedSize * 1000000 / PropagationTimeUs * 8,
	prometheus_histogram:observe(tx_propagation_bits_per_second, BitsPerSecond).
