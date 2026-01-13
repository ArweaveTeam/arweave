-module(ar_data_discovery).

-behaviour(gen_server).

-export([start_link/0, get_bucket_peers/1, get_footprint_bucket_peers/1,
		collect_peers/0, pick_peers/2, report_bucket_stats/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_discovery.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-record(state, {
	peer_queue,
	peers_pending,
	network_map,
	footprint_map,
	expiration_map
}).

%% The frequency of asking peers about their data.
-ifdef(AR_TEST).
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 4 * 60 * 1000).
-endif.

%% The frequency of logging bucket stats.
-ifdef(AR_TEST).
-define(REPORT_BUCKET_STATS_FREQUENCY_MS, 10 * 1000).
-else.
-define(REPORT_BUCKET_STATS_FREQUENCY_MS, 60 * 1000).
-endif.

%% The expiration time of peer's buckets. If a peer is found in the list of
%% the first best ?DATA_DISCOVERY_COLLECT_PEERS_COUNT peers (checked every
%% ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS milliseconds), the timer is refreshed.
-define(PEER_EXPIRATION_TIME_MS, 60 * 60 * 1000).

%% The maximum number of requests running at any time.
-define(DATA_DISCOVERY_PARALLEL_PEER_REQUESTS, 10).

%% The number of peers from the top of the rating to schedule for inclusion
%% into the peer map every DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS milliseconds.
-define(DATA_DISCOVERY_COLLECT_PEERS_COUNT, 1000).

%% The number of the release adding support for the
%% GET /footprint_buckets endpoint.
-define(GET_FOOTPRINT_BUCKETS_SUPPORT_RELEASE, 87).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return the list of ?QUERY_BEST_PEERS_COUNT peers who have at least one byte of
%% data synced in the given Bucket of size ?NETWORK_DATA_BUCKET_SIZE. 80% of the peers
%% are chosen from the 20% of peers with the biggest share in the given bucket.
get_bucket_peers(Bucket) ->
	case ets:member(ar_peers, block_connections) of
		true ->
			[];
		false ->
			get_bucket_peers(Bucket, {Bucket, 0, no_peer}, [])
	end.

get_bucket_peers(Bucket, Cursor, Peers) ->
	case ets:next(?MODULE, Cursor) of
		{Bucket, _Share, Peer} = Key ->
			get_bucket_peers(Bucket, Key, [Peer | Peers]);
		_ -> % matches `end_of_table` or an unexpected value
			ar_util:unique(Peers)
	end.

%% @doc Return the list of ?QUERY_BEST_PEERS_COUNT peers who have at least one byte of
%% data synced in the given footprint bucket of size ?NETWORK_FOOTPRINT_BUCKET_SIZE.
%% 80% of the peers are chosen from the 20% of peers with the biggest share
%% in the given bucket.
get_footprint_bucket_peers(Bucket) ->
	case ets:member(ar_peers, block_connections) of
		true ->
			[];
		false ->
			get_footprint_bucket_peers(Bucket, {Bucket, 0, no_peer}, [])
	end.

get_footprint_bucket_peers(Bucket, Cursor, Peers) ->
	case ets:next(ar_data_discovery_footprint_buckets, Cursor) of
		{Bucket, _Share, Peer} = Key ->
			get_footprint_bucket_peers(Bucket, Key, [Peer | Peers]);
		_ ->
			ar_util:unique(Peers)
	end.

%% @doc Return a list of peers where 80% of the peers are randomly chosen
%% from the first 20% of Peers and the other 20% of the peers are randomly
%% chosen from the other 80% of Peers.
pick_peers(Peers, N) ->
	pick_peers(Peers, length(Peers), N).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, _} = ar_timer:apply_interval(
		?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS,
		?MODULE,
		collect_peers,
		[],
		#{ skip_on_shutdown => false }
	),
	{ok, _} = ar_timer:apply_interval(
		?REPORT_BUCKET_STATS_FREQUENCY_MS,
		?MODULE,
		report_bucket_stats,
		[],
		#{ skip_on_shutdown => true }
	),
	gen_server:cast(?MODULE, update_network_data_map),
	ok = ar_events:subscribe(peer),
	{ok, #state{
		peer_queue = queue:new(),
		peers_pending = 0,
		network_map = #{},
		footprint_map = #{},
		expiration_map = #{}
	}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
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
			monitor(process, spawn_link(
				fun() ->
					fetch_sync_buckets(Peer),
					fetch_footprint_buckets(Peer)
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
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(?MODULE, {{Bucket, Share, Peer}})
		end,
		?NETWORK_DATA_BUCKET_SIZE,
		SyncBuckets
	),
	{noreply, State2#state{ network_map = Map2 }};

handle_cast({add_peer_footprint_buckets, Peer, FootprintBuckets}, State) ->
	#state{ footprint_map = Map } = State,
	State2 = refresh_expiration_timer(Peer, State),
	Map2 = maps:put(Peer, FootprintBuckets, Map),
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(ar_data_discovery_footprint_buckets, {{Bucket, Share, Peer}})
		end,
		?NETWORK_FOOTPRINT_BUCKET_SIZE,
		FootprintBuckets
	),
	{noreply, State2#state{ footprint_map = Map2 }};

handle_cast({remove_peer, Peer}, State) ->
	#state{ network_map = Map, footprint_map = FootprintMap, expiration_map = E } = State,
	Map2 =
		case maps:take(Peer, Map) of
			error ->
				Map;
			{SyncBuckets, Map3} ->
				ar_sync_buckets:foreach(
					fun(Bucket, Share) ->
						ets:delete(?MODULE, {Bucket, Share, Peer})
					end,
					?NETWORK_DATA_BUCKET_SIZE,
					SyncBuckets
				),
				Map3
		end,
	FootprintMap2 =
		case maps:take(Peer, FootprintMap) of
			error ->
				FootprintMap;
			{FootprintBuckets, Map4} ->
				ar_sync_buckets:foreach(
					fun(Bucket, Share) ->
						ets:delete(ar_data_discovery_footprint_buckets, {Bucket, Share, Peer})
					end,
					?NETWORK_FOOTPRINT_BUCKET_SIZE,
					FootprintBuckets
				),
				Map4
		end,
	E2 = maps:remove(Peer, E),
	{noreply, State#state{ network_map = Map2, footprint_map = FootprintMap2, expiration_map = E2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', _,  process, _, _}, #state{ peers_pending = N } = State) ->
	{noreply, State#state{ peers_pending = N - 1 }};

handle_info({event, peer, {removed, Peer}}, State) ->
	gen_server:cast(?MODULE, {remove_peer, Peer}),
	{noreply, State};

handle_info({event, peer, _}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

pick_peers(Peers, PeerLen, N) when N >= PeerLen ->
	Peers;
pick_peers([], _PeerLen, _N) ->
	[];
pick_peers(_Peers, _PeerLen, N) when N =< 0 ->
	[];
pick_peers(Peers, PeerLen, N) ->
	%% N: the target number of peers to pick
	%% Best: top 20% of the Peers list
	%% Other: the rest of the Peers list
	{Best, Other} = lists:split(max(PeerLen div 5, 1), Peers),
	%% TakeBest: Select 80% of N worth of Best - or all of Best if Best is short.
	TakeBest = max((8 * N) div 10, 1),
	Part1 = ar_util:pick_random(Best, min(length(Best), TakeBest)),
	%% TakeOther: rather than strictly take 20% of N, take enough to ensure we're
	%% getting the full N of picked peers.
	TakeOther = N - length(Part1),
	Part2 = ar_util:pick_random(Other, min(length(Other), TakeOther)),
	Part1 ++ Part2.

collect_peers() ->
	N = ?DATA_DISCOVERY_COLLECT_PEERS_COUNT,
	{ok, Config} = arweave_config:get_env(),
	Peers =
		case Config#config.sync_from_local_peers_only of
			true ->
				Config#config.local_peers;
			false ->
				%% rank peers by current rating since we care about their recent throughput performance
				ar_peers:get_peers(current)
		end,
	collect_peers(lists:sublist(Peers, N)).

collect_peers([Peer | Peers]) ->
	gen_server:cast(?MODULE, {add_peer, Peer}),
	collect_peers(Peers);
collect_peers([]) ->
	ok.

%% @doc Log bucket statistics for each configured storage module.
report_bucket_stats() ->
	StartTime = erlang:monotonic_time(millisecond),
	{ok, Config} = arweave_config:get_env(),
	StorageModules = Config#config.storage_modules,
	lists:foreach(
		fun(Module) ->
			StoreID = ar_storage_module:id(Module),
			{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
			report_bucket_stats(StoreID, RangeStart, RangeEnd, normal),
			report_bucket_stats(StoreID, RangeStart, RangeEnd, footprint)
		end,
		StorageModules
	),
	ElapsedMs = erlang:monotonic_time(millisecond) - StartTime,
	?LOG_DEBUG([{event, bucket_stats_complete}, {elapsed_ms, ElapsedMs}]).

report_bucket_stats(StoreID, RangeStart, RangeEnd, normal) ->
	StartBucket = RangeStart div ?NETWORK_DATA_BUCKET_SIZE,
	EndBucket = (RangeEnd - 1) div ?NETWORK_DATA_BUCKET_SIZE,
	TotalBuckets = EndBucket - StartBucket + 1,
	{AllPeers, ZeroCount, HealthyCount} =
		bucket_stats(StartBucket, EndBucket, ?MODULE, sets:new()),
	set_bucket_stats_metrics(StoreID, normal, AllPeers, TotalBuckets, ZeroCount, HealthyCount);
report_bucket_stats(StoreID, RangeStart, RangeEnd, footprint) ->
	StartBucket = ar_footprint_record:get_footprint_bucket(RangeStart + ?DATA_CHUNK_SIZE),
	EndBucket = ar_footprint_record:get_footprint_bucket(RangeEnd),
	TotalBuckets = max(0, EndBucket - StartBucket + 1),
	{AllPeers, ZeroCount, HealthyCount} =
		bucket_stats(StartBucket, EndBucket, ar_data_discovery_footprint_buckets, sets:new()),
	set_bucket_stats_metrics(StoreID, footprint, AllPeers, TotalBuckets, ZeroCount, HealthyCount).

bucket_stats(StartBucket, EndBucket, _Table, AllPeers) when StartBucket > EndBucket ->
	{AllPeers, 0, 0};
bucket_stats(StartBucket, EndBucket, Table, AllPeers) ->
	bucket_stats(StartBucket, EndBucket, Table, AllPeers, 0, 0).

bucket_stats(Bucket, EndBucket, _Table, AllPeers, ZeroCount, HealthyCount)
		when Bucket > EndBucket ->
	{AllPeers, ZeroCount, HealthyCount};
bucket_stats(Bucket, EndBucket, Table, AllPeers, ZeroCount, HealthyCount) ->
	{BucketPeers, AllPeers2} = get_bucket_peers_and_collect(Bucket, Table, AllPeers),
	PeerCount = length(BucketPeers),
	{ZeroCount2, HealthyCount2} =
		case PeerCount of
			0 -> {ZeroCount + 1, HealthyCount};
			N when N >= 3 -> {ZeroCount, HealthyCount + 1};
			_ -> {ZeroCount, HealthyCount}
		end,
	bucket_stats(Bucket + 1, EndBucket, Table, AllPeers2, ZeroCount2, HealthyCount2).

get_bucket_peers_and_collect(Bucket, Table, AllPeers) ->
	get_bucket_peers_and_collect(Bucket, Table, {Bucket, 0, no_peer}, [], AllPeers).

get_bucket_peers_and_collect(Bucket, Table, Cursor, BucketPeers, AllPeers) ->
	case ets:next(Table, Cursor) of
		{Bucket, _Share, Peer} = Key ->
			get_bucket_peers_and_collect(Bucket, Table, Key,
				[Peer | BucketPeers], sets:add_element(Peer, AllPeers));
		_ ->
			{ar_util:unique(BucketPeers), AllPeers}
	end.

set_bucket_stats_metrics(StoreID, Type, AllPeers, TotalBuckets, ZeroCount, HealthyCount) ->
	NumPeers = sets:size(AllPeers),
	StoreIDLabel = ar_storage_module:label(StoreID),
	prometheus_gauge:set(data_discovery, [Type, StoreIDLabel, num_peers], NumPeers),
	prometheus_gauge:set(data_discovery, [Type, StoreIDLabel, total_buckets], TotalBuckets),
	prometheus_gauge:set(data_discovery, [Type, StoreIDLabel, zero_peer_count], ZeroCount),
	prometheus_gauge:set(data_discovery, [Type, StoreIDLabel, healthy_peer_count], HealthyCount).

fetch_sync_buckets(Peer) ->
	case ar_http_iface_client:get_sync_buckets(Peer) of
		{ok, SyncBuckets} ->
			gen_server:cast(?MODULE, {add_peer_sync_buckets, Peer, SyncBuckets});
		{error, request_type_not_found} ->
			?LOG_DEBUG([{event, sync_buckets_request_type_not_found},
					{peer, ar_util:format_peer(Peer)}]);
		{error, Reason} ->
			ar_http_iface_client:log_failed_request(Reason,
				[{event, failed_to_fetch_sync_buckets},
				{peer, ar_util:format_peer(Peer)},
				{reason, io_lib:format("~p", [Reason])}]);
		Error ->
			?LOG_DEBUG([{event, failed_to_fetch_sync_buckets},
				{peer, ar_util:format_peer(Peer)},
				{reason, io_lib:format("~p", [Error])}])
	end.

fetch_footprint_buckets(Peer) ->
	case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_BUCKETS_SUPPORT_RELEASE of
		true ->
			fetch_footprint_buckets2(Peer);
		false ->
			ok
	end.

fetch_footprint_buckets2(Peer) ->
	case ar_http_iface_client:get_footprint_buckets(Peer) of
		{ok, SyncBuckets} ->
			gen_server:cast(?MODULE, {add_peer_footprint_buckets, Peer, SyncBuckets});
		{error, request_type_not_found} ->
			?LOG_DEBUG([{event, footprint_buckets_request_type_not_found},
					{peer, ar_util:format_peer(Peer)}]);
		{error, Reason} ->
			ar_http_iface_client:log_failed_request(Reason,
				[{event, failed_to_fetch_footprint_buckets},
				{peer, ar_util:format_peer(Peer)},
				{reason, io_lib:format("~p", [Reason])}]);
		Error ->
			?LOG_DEBUG([{event, failed_to_fetch_footprint_buckets},
				{peer, ar_util:format_peer(Peer)},
				{reason, io_lib:format("~p", [Error])}])
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
