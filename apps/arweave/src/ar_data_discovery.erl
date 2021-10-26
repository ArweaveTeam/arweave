-module(ar_data_discovery).

-behaviour(gen_server).

-export([start_link/0, get_bucket_peers/1, collect_peers/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
		code_change/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").

-record(state, {
	peer_queue,
	peers_pending,
	network_map,
	expiration_map
}).

%% The frequency of asking peers about their data.
-ifdef(DEBUG).
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 60 * 1000).
-endif.

%% The expiration time of peer's buckets. If a peer is found in the list of
%% the first best ?DATA_DISCOVERY_COLLECT_PEERS_COUNT peers (checked every
%% ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS milliseconds), the timer is refreshed.
-define(PEER_EXPIRATION_TIME_MS, 10 * 60 * 1000).

%% The maximum number of requests running at any time.
-define(DATA_DISCOVERY_PARALLEL_PEER_REQUESTS, 10).

%% The number of peers from the top of the rating to schedule for inclusion
%% into the peer map every DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS milliseconds.
-define(DATA_DISCOVERY_COLLECT_PEERS_COUNT, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return the list of ?QUERY_BEST_PEERS_COUNT peers who have at least one byte of data
%% synced in the given Bucket of size ?NETWORK_DATA_BUCKET_SIZE, sorted from the biggest
%% synced share to the lowest.
get_bucket_peers(Bucket) ->
	case ets:lookup(?MODULE, network_map) of
		[] ->
			[];
		[{_, NetworkMap}] ->
			BucketSize = ?NETWORK_DATA_BUCKET_SIZE,
			Shares = maps:fold(
				fun(Peer, Buckets, Acc) ->
					[{-ar_sync_buckets:get(Bucket, BucketSize, Buckets), Peer} | Acc]
				end,
				[],
				NetworkMap
			),
			[Peer || {_, Peer} <- lists:sublist(lists:sort(Shares), ?QUERY_BEST_PEERS_COUNT)]
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, _} = timer:apply_interval(
		?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, ?MODULE, collect_peers, []),
	gen_server:cast(?MODULE, update_network_data_map),
	ok = ar_events:subscribe(peer),
	{ok, #state{
		peer_queue = queue:new(),
		peers_pending = 0,
		network_map = #{},
		expiration_map = #{}
	}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({add_peer, Peer}, #state{ peer_queue = Queue } = State) ->
	{noreply, State#state{ peer_queue = queue:in(Peer, Queue) }};

handle_cast(update_network_data_map, #state{ peers_pending = N } = State)
		when N < ?DATA_DISCOVERY_PARALLEL_PEER_REQUESTS ->
	case queue:out(State#state.peer_queue) of
		{empty, _} ->
			ar_util:cast_after(200, ?MODULE, update_network_data_map),
			{noreply, State};
		{{value, Peer}, Queue} ->
			monitor(process, spawn(
				fun() ->
					case ar_http_iface_client:get_sync_buckets(Peer) of
						{ok, SyncBuckets} ->
							gen_server:cast(?MODULE, {add_peer_sync_buckets, Peer,
									SyncBuckets});
						{error, request_type_not_found} ->
							get_sync_buckets(Peer);
						Error ->
							?LOG_DEBUG([{event, failed_to_fetch_sync_buckets},
								{reason, io_lib:format("~p", [Error])}])
					end
				end
			)),
			gen_server:cast(?MODULE, update_network_data_map),
			{noreply, State#state{ peers_pending = N + 1, peer_queue = Queue }}
	end;
handle_cast(update_network_data_map, State) ->
	ar_util:cast_after(200, ?MODULE, update_network_data_map),
	{noreply, State};

handle_cast({add_peer_sync_buckets, Peer, SyncBuckets}, State) ->
	#state{ network_map = Map } = State,
	State2 = refresh_expiration_timer(Peer, State),
	Map2 = maps:put(Peer, SyncBuckets, Map),
	ets:insert(?MODULE, {network_map, Map2}),
	{noreply, State2#state{ network_map = Map2 }};

handle_cast({remove_peer, Peer}, State) ->
	#state{ network_map = Map, expiration_map = E } = State,
	Map2 = maps:remove(Peer, Map),
	E2 = maps:remove(Peer, E),
	ets:insert(?MODULE, {network_map, Map2}),
	{noreply, State#state{ network_map = Map2, expiration_map = E2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({'DOWN', _,  process, _, _}, #state{ peers_pending = N } = State) ->
	{noreply, State#state{ peers_pending = N - 1 }};

handle_info({event, peer, {bad_response, {Peer, _Resource, _Reason}}}, State) ->
	gen_server:cast(?MODULE, {remove_peer, Peer}),
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

collect_peers() ->
	N = ?DATA_DISCOVERY_COLLECT_PEERS_COUNT,
	collect_peers(lists:sublist(ar_bridge:get_remote_peers(), N)).

collect_peers([Peer | Peers]) ->
	gen_server:cast(?MODULE, {add_peer, Peer}),
	collect_peers(Peers);
collect_peers([]) ->
	ok.

get_sync_buckets(Peer) ->
	case ar_http_iface_client:get_sync_record(Peer) of
		{ok, SyncRecord} ->
			SyncBuckets = ar_sync_buckets:from_intervals(SyncRecord),
			{SyncBuckets2, _} = ar_sync_buckets:serialize(SyncBuckets, ?MAX_SYNC_BUCKETS_SIZE),
			gen_server:cast(?MODULE, {add_peer_sync_buckets, Peer, SyncBuckets2});
		Error ->
			?LOG_DEBUG([{event, failed_to_fetch_sync_record_from_peer},
					{peer, ar_util:format_peer(Peer)}, {reason, io_lib:format("~p", [Error])}])
	end.

refresh_expiration_timer(Peer, State) ->
	#state{ expiration_map = Map } = State,
	case maps:get(Peer, Map, not_found) of
		not_found ->
			ok;
		Timer ->
			timer:cancel(Timer)
	end,
	Timer2 = ar_util:cast_after(?PEER_EXPIRATION_TIME_MS, ?MODULE, {remove_peer, Peer}),
	State#state{ expiration_map = maps:put(Peer, Timer2, Map) }.
