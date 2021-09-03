-module(ar_peer_mempool_monitor).

-export([
	start_link/0,
	cleanup_peer_mempools/0
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2
]).

-include_lib("arweave/include/ar.hrl").

-ifdef(DEBUG).
-define(PEER_MEMPOOLS_MAX_SIZE, 4).
-else.
-define(PEER_MEMPOOLS_MAX_SIZE, 1000000).
-endif.

-ifdef(DEBUG).
-define(CLEANUP_PEER_MEMPOOLS_FREQUENCY_MS, 1000).
-else.
-define(CLEANUP_PEER_MEMPOOLS_FREQUENCY_MS, 120 * 1000).
-endif.

-ifdef(DEBUG).
-define(COLLECT_PEER_MEMPOOLS_FREQUENCY_MS, 1000).
-else.
-define(COLLECT_PEER_MEMPOOLS_FREQUENCY_MS, 30 * 1000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = ar_events:subscribe(tx),
	timer:apply_interval(
		?CLEANUP_PEER_MEMPOOLS_FREQUENCY_MS,
		ar_peer_mempool_monitor,
		cleanup_peer_mempools,
		[]
	),
	gen_server:cast(?MODULE, collect_peer_mempools),
	{ok, no_state}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(collect_peer_mempools, State) ->
	Peers = get_peers(),
	gen_server:cast(?MODULE, {collect_peer_mempools, Peers}),
	{noreply, State};

handle_cast({collect_peer_mempools, [Peer | Peers]}, State) ->
	case ar_http_iface_client:get_mempool(Peer) of
		{ok, TXIDs} ->
			lists:foreach(
				fun(TXID) ->
					case ets:lookup(node_state, {tx, TXID}) of
						[] ->
							ok;
						_ ->
							add(TXID, Peer)
					end
				end,
				TXIDs
			);
		_ ->
			ok
	end,
	gen_server:cast(?MODULE, {collect_peer_mempools, Peers}),
	{noreply, State};

handle_cast({collect_peer_mempools, []}, State) ->
	timer:apply_after(
		?COLLECT_PEER_MEMPOOLS_FREQUENCY_MS, gen_server, cast, [?MODULE, collect_peer_mempools]),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, tx, {dropped, #tx{ id = TXID }, _Reason}}, State) ->
	remove(ets:next(txid_peer, {TXID, n}), TXID),
	{noreply, State};

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

cleanup_peer_mempools() ->
	Size = ets:info(timestamp_txid_peer, size),
	cleanup_peer_mempools(ets:first(timestamp_txid_peer), Size - ?PEER_MEMPOOLS_MAX_SIZE).

cleanup_peer_mempools('$end_of_table', _N) ->
	ok;
cleanup_peer_mempools(Timestamp, N) when N > 0 ->
	case ets:lookup(timestamp_txid_peer, Timestamp) of
		[{_, TXIDPeer}] ->
			ets:delete(txid_peer, TXIDPeer),
			ets:delete(timestamp_txid_peer, Timestamp),
			cleanup_peer_mempools(ets:next(timestamp_txid_peer, Timestamp), N - 1);
		[] ->
			ok
	end;
cleanup_peer_mempools(_Timestamp, _N) ->
	ok.

get_peers() ->
	lists:sublist(ar_bridge:get_remote_peers(10000), ar_meta_db:get(max_propagation_peers)).

add(TXID, Peer) ->
	case ets:lookup(txid_peer, {TXID, Peer}) of
		[] ->
			Timestamp = os:system_time(microsecond),
			ets:insert(timestamp_txid_peer, {Timestamp, {TXID, Peer}}),
			ets:insert(txid_peer, {{TXID, Peer}, Timestamp}),
			ok;
		_ ->
			ok
	end.

remove('$end_of_table', _TXID) ->
	ok;
remove({TXID, _} = TXIDPeer, TXID) ->
	case ets:lookup(timestamp_txid_peer, TXIDPeer) of
		[{_, Timestamp}] ->
			ets:delete(txid_peer, TXIDPeer),
			ets:delete(timestamp_txid_peer, Timestamp);
		[] ->
			ets:delete(txid_peer, TXIDPeer)
	end,
	remove(ets:next(txid_peer, TXIDPeer), TXID);
remove(_, _) ->
	ok.
