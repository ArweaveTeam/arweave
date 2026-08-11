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


-include("ar.hrl").

-record(state, {
    last_seen_tx_timestamp = 0,
    pending_txids = [],
    latest_txid_source_peer = none
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
    %% Polling can be toggled at runtime; read it live each tick. When
    %% disabled, just reschedule and skip the work so re-enabling resumes
    %% polling without a restart.
    case arweave_config:get([gossip, tx, polling_enabled]) of
        false ->
            arweave_util:cast_after(?CHECK_INTERVAL_MS, self(), check_for_received_txs),
            {noreply, State};
        true ->
            %% Check if there have been any transactions received in the last
            %% ?CHECK_INTERVAL_MS milliseconds.
            TimestampDiff = erlang:system_time(microsecond)
                - State#state.last_seen_tx_timestamp,
            State3 =
                case TimestampDiff > 0
                        andalso TimestampDiff > (?CHECK_INTERVAL_MS * 1000) of
                    true ->
                        check_for_received_txs(State);
                    false ->
                        arweave_util:cast_after(?CHECK_INTERVAL_MS, self(),
                            check_for_received_txs),
                        State
                end,
            {noreply, State3}
    end;

handle_cast(Request, State) ->
    ?LOG_WARNING("Unexpected cast: ~p", [Request]),
    {noreply, State}.

handle_info({event, node_state, {initialized, _}}, State) ->
    %% Start the check_for_received_txs loop unconditionally; the loop reads
    %% [gossip, tx, polling_enabled] live each tick, so it can be enabled or
    %% disabled at runtime without a restart.
    gen_server:cast(self(), check_for_received_txs),
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
    ?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
    ok.

%%% Internal functions.

check_for_received_txs(#state{ pending_txids = [TXID | PendingTXIDs] } = State) ->
    case ar_mempool:is_known_tx(TXID) of
        true ->
            ok;
        false ->
            download_and_verify_tx(TXID, State#state.latest_txid_source_peer)
    end,
    gen_server:cast(self(), check_for_received_txs),
    State#state{ pending_txids = PendingTXIDs };

check_for_received_txs(#state{ pending_txids = [] } = State) ->
    Peers = lists:sublist(ar_peers:get_peers(current), ?QUERY_PEERS_COUNT),
    Reply = ar_http_iface_client:get_mempool(Peers),
    arweave_util:cast_after(?POLL_INTERVAL_MS, self(), check_for_received_txs),
    case Reply of
        {{ok, TXIDs}, TXIDPeer} ->
            State#state{ pending_txids = TXIDs,
                latest_txid_source_peer = TXIDPeer };
        {error, Error} ->
            ?LOG_DEBUG([{event, failed_to_get_mempool_txids_from_peers},
                    {peers, [arweave_util:format_peer(Peer) || Peer <- Peers]},
                    {error, io_lib:format("~p", [Error])}
            ]),
            State
    end.

download_and_verify_tx(TXID, TXIDPeer) ->
    Ref = make_ref(),
    ar_ignore_registry:add_ref(TXID, Ref, 10_000),
    Peers = lists:sublist(ar_peers:get_peers(current), ?QUERY_PEERS_COUNT),
    case ar_http_iface_client:get_tx_from_remote_peers(Peers, TXID, false) of
        not_found ->
            ar_ignore_registry:remove_ref(TXID, Ref),
            ?LOG_DEBUG([{event, failed_to_get_tx_from_peers},
                    {peers, [arweave_util:format_peer(Peer) || Peer <- Peers]},
                    {txid, arweave_util:encode(TXID)},
                    {txid_peer, arweave_util:format_peer(TXIDPeer)}
            ]);
        {TX, Peer, Time, Size} ->
            case ar_tx_validator:validate(TX) of
                {invalid, Code} ->
                    log_invalid_tx(Code, TXID, TX, Peer, TXIDPeer);
                {valid, TX2} ->
                    ar_peers:rate_fetched_data(Peer, tx, Time, Size),
                    ar_disk_pool:add_data_root(TX2#tx.data_root,
                            TX2#tx.data_size, TX#tx.id),
                    ar_events:send(tx, {new, TX2, {pulled, Peer}}),
                    TXID = TX2#tx.id,
                    ar_ignore_registry:remove_ref(TXID, Ref),
                    ar_ignore_registry:add_temporary(TXID, 10 * 60 * 1000)
            end
    end.

log_invalid_tx(tx_bad_anchor, TXID, TX, Peer, TXIDPeer) ->
    LastTX = arweave_util:encode(TX#tx.last_tx),
    CurrentHeight = ar_node:get_height(),
    CurrentBlockHash = arweave_util:encode(ar_node:get_current_block_hash()),
    ?LOG_INFO(format_invalid_tx_message(tx_bad_anchor,
        TXID, Peer, TXIDPeer, [
            {last_tx, LastTX},
            {current_height, CurrentHeight},
            {current_block_hash, CurrentBlockHash}
        ]));
log_invalid_tx(tx_verification_failed, TXID, TX, Peer, TXIDPeer) ->
    LastTX = arweave_util:encode(TX#tx.last_tx),
    CurrentHeight = ar_node:get_height(),
    CurrentBlockHash = arweave_util:encode(ar_node:get_current_block_hash()),
    ErrorCodes = ar_tx_db:get_error_codes(TXID),
    ?LOG_INFO(format_invalid_tx_message(tx_verification_failed,
        TXID, Peer, TXIDPeer, [
            {last_tx, LastTX},
            {current_height, CurrentHeight},
            {current_block_hash, CurrentBlockHash},
            {error_codes, ErrorCodes}
        ]));
log_invalid_tx(Code, TXID, _TX, Peer, TXIDPeer) ->
    ?LOG_INFO(format_invalid_tx_message(Code, TXID, Peer, TXIDPeer, [])).

format_invalid_tx_message(Code, TXID, Peer, TXIDPeer, ExtraLogs) ->
    [
        {event, fetched_already_included_or_invalid_tx},
        {txid, arweave_util:encode(TXID)},
        {code, Code},
        {peer, arweave_util:format_peer(Peer)},
        {txid_peer, arweave_util:format_peer(TXIDPeer)}
        | ExtraLogs
    ].
