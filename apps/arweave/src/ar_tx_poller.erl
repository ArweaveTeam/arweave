-module(ar_tx_poller).
-behaviour(gen_server).

-export([
	start_link/0
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2
]).

-include("../include/ar.hrl").
-include("../include/ar_config.hrl").
-record(state, {
	last_seen_tx_timestamp = 0,
	pending_txids = []
}).

%% Number of peers to query for a transaction.
-define(QUERY_PEERS_COUNT, 5).

%% Check interval in milliseconds - how long to wait before polling
%% since the last transaction push. If the node is not public (so it
%% never receives transactions by push), we wait this long starting from
%% the moment we join the network only once and then keep polling
%% for transactions more frequently.
-ifdef(AR_TEST).
-define(CHECK_INTERVAL_MS, 5_000).
-else.
-define(CHECK_INTERVAL_MS, 30_000).
-endif.

%% Poll interval in milliseconds - how long we wait before downloading a new
%% transaction or polling the mempools for new transactions.
-ifdef(AR_TEST).
-define(POLL_INTERVAL_MS, 500).
-else.
-define(POLL_INTERVAL_MS, 200).
-endif.

%%% Public API.

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% Gen server callbacks.

init([]) ->
    [ok, ok] = ar_events:subscribe([tx, node_state]),
	{ok, #state{}}.

handle_call(Request, From, State) ->
	?LOG_WARNING("Unexpected call: ~p from ~p", [Request, From]),
	{reply, ignored, State}.

handle_cast(check_for_received_txs, State) ->
	%% Check if there have been any transactions received in the last
	%% ?CHECK_INTERVAL_MS milliseconds.
	TimestampDiff = erlang:system_time(microsecond) - State#state.last_seen_tx_timestamp,
	State3 =
		case TimestampDiff > 0 andalso TimestampDiff > (?CHECK_INTERVAL_MS * 1000) of
			true ->
				check_for_received_txs(State);
			false ->
				ar_util:cast_after(?CHECK_INTERVAL_MS, self(), check_for_received_txs),
				State
		end,
	{noreply, State3};

handle_cast(Request, State) ->
	?LOG_WARNING("Unexpected cast: ~p", [Request]),
	{noreply, State}.

handle_info({event, node_state, {initialized, _}}, State) ->
	%% Send a check_for_received_txs cast periodically to check for externally submitted
	%% transactions. If there have not been any for longer than 30 seconds, request the
	%% mempool from a peer and download the transactions.
    {ok, Config} = application:get_env(arweave, config),
    case lists:member(tx_poller, Config#config.disable) of
        true ->
            ok;
        false ->
            gen_server:cast(self(), check_for_received_txs)
    end,
    {noreply, State};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info({event, tx, {new, _TX, {pushed, _Peer}}}, State) ->
	{noreply, State#state{
		pending_txids = [],
		last_seen_tx_timestamp = erlang:system_time(microsecond)
	}};

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING("event: unhandled_info, info: ~p", [Info]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_WARNING("Unexpected terminate: ~p", [Reason]),
	ok.

%%% Internal functions.

check_for_received_txs(#state{ pending_txids = [TXID | PendingTXIDs] } = State) ->
	case ar_mempool:is_known_tx(TXID) of
		true ->
			ok;
		false ->
			download_and_verify_tx(TXID)
	end,
	gen_server:cast(self(), check_for_received_txs),
	State#state{ pending_txids = PendingTXIDs };

check_for_received_txs(#state{ pending_txids = [] } = State) ->
	Peers = lists:sublist(ar_peers:get_peers(current), ?QUERY_PEERS_COUNT),
	Reply = ar_http_iface_client:get_mempool(Peers),
	ar_util:cast_after(?POLL_INTERVAL_MS, self(), check_for_received_txs),
	case Reply of
		{ok, TXIDs} ->
			State#state{ pending_txids = TXIDs };
		{error, _Error} ->
			State
	end.

download_and_verify_tx(TXID) ->
	ar_ignore_registry:add_temporary(TXID, 10_000),
	Peers = lists:sublist(ar_peers:get_peers(current), ?QUERY_PEERS_COUNT),
	case ar_http_iface_client:get_tx_from_remote_peers(Peers, TXID, false) of
		not_found ->
			ar_ignore_registry:remove_temporary(TXID),
			?LOG_DEBUG([{event, failed_to_get_tx_from_peers},
					{peers, [ar_util:format_peer(Peer) || Peer <- Peers]},
					{txid, ar_util:encode(TXID)}
			]);
		{TX, Peer, Time, Size} ->
			case ar_tx_validator:validate(TX) of
				{invalid, Code} ->
					log_invalid_tx(Code, TXID, TX, Peer);
				{valid, TX2} ->
					ar_peers:rate_fetched_data(Peer, tx, Time, Size),
					ar_data_sync:add_data_root_to_disk_pool(TX2#tx.data_root,
							TX2#tx.data_size, TX#tx.id),
					ar_events:send(tx, {new, TX2, {pulled, Peer}}),
					TXID = TX2#tx.id,
					ar_ignore_registry:remove_temporary(TXID),
					ar_ignore_registry:add_temporary(TXID, 10 * 60 * 1000)
			end
	end.

log_invalid_tx(tx_bad_anchor, TXID, TX, Peer) ->
	LastTX = ar_util:encode(TX#tx.last_tx),
	CurrentHeight = ar_node:get_height(),
	CurrentBlockHash = ar_util:encode(ar_node:get_current_block_hash()),
	?LOG_INFO(format_invalid_tx_message(tx_bad_anchor, TXID, Peer, [
		{last_tx, LastTX},
		{current_height, CurrentHeight},
		{current_block_hash, CurrentBlockHash}
	]));
log_invalid_tx(tx_verification_failed, TXID, TX, Peer) ->
	LastTX = ar_util:encode(TX#tx.last_tx),
	CurrentHeight = ar_node:get_height(),
	CurrentBlockHash = ar_util:encode(ar_node:get_current_block_hash()),
	ErrorCodes = ar_tx_db:get_error_codes(TXID),
	?LOG_INFO(format_invalid_tx_message(tx_verification_failed, TXID, Peer, [
		{last_tx, LastTX},
		{current_height, CurrentHeight},
		{current_block_hash, CurrentBlockHash},
		{error_codes, ErrorCodes}
	]));
log_invalid_tx(Code, TXID, _TX, Peer) ->
	?LOG_INFO(format_invalid_tx_message(Code, TXID, Peer, [])).

format_invalid_tx_message(Code, TXID, Peer, ExtraLogs) ->
	[
		{event, fetched_invalid_tx},
		{txid, ar_util:encode(TXID)},
		{code, Code},
		{peer, ar_util:format_peer(Peer)}
		| ExtraLogs
	].
