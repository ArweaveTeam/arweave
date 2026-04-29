-module(ar_data_discovery).

-behaviour(gen_server).

-export([start_link/0, get_bucket_peers/1, get_footprint_bucket_peers/1,
		collect_peers/0, pick_peers/2, report_bucket_stats/0,
		get_peer_intervals/3, get_peer_footprint_intervals/3,
		advance_cursor/3, invalidate/2]).

-ifdef(AR_TEST).
-export([clear_interval_cache/0]).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_discovery.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

%% Rate-limit keys used when talking to peers. Kept here rather than inside
%% ar_peer_intervals so the cache layer and the rate limiter share one source.
-define(GET_SYNC_RECORD_RPM_KEY, data_sync_record).
-define(GET_FOOTPRINT_RECORD_RPM_KEY, footprints).
-define(GET_SYNC_RECORD_COOLDOWN_MS, 60 * 1000).

%% The peer interval cache. Entries take two shapes, both keyed by
%% `{Peer, CacheKey, Mode}':
%%
%%   normal:    Value = {Intervals, PeerRightBound, FetchedAtMs}
%%   footprint: Value = {Intervals, none,          FetchedAtMs}
%%
%% Value format is uniform so readers can branch on Mode alone.
-define(INTERVAL_CACHE_TABLE, ar_data_discovery_intervals).

%% Fetch at most this many sync intervals from a peer at a time.
-ifdef(AR_TEST).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 10).
-else.
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 1000).
-endif.

-record(state, {
	peer_queue,
	peers_pending,
	network_map,
	footprint_map,
	expiration_map,
	%% Refresh pool (distinct from the sync_buckets pool above) that fills
	%% the fine-grained peer interval cache. Driven by cursor prefetch and
	%% invalidation casts.
	refresh_queue = queue:new(),
	refresh_pending = 0,
	%% pid() => job_key(). Tracks spawn_link'd refresh workers so the DOWN
	%% handler can distinguish them from the sync_buckets pool's workers
	%% and remove the job_key from `refresh_inflight'.
	refresh_pids = #{},
	%% job_key() in {Peer, CacheKey, Mode} form. Used to dedupe enqueues:
	%% if a prefetch already covers this key we don't enqueue it again.
	refresh_inflight = sets:new()
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

%% @doc Return the peer's raw sync-record intervals starting at byte offset
%% `Left'. `Right' is the requested upper bound (passed through to peers that
%% support the right-bound endpoint; older peers ignore it). The result is
%% intersection-free and per-peer; callers intersect with their own sought
%% set. Reads from the interval cache when fresh, otherwise issues a
%% `GET /data_sync_record' HTTP call and writes the response back.
%%
%% Returns `{error, cooldown}` without touching cache or HTTP when the peer
%% is on the rate-limit cooldown for this endpoint.
-spec get_peer_intervals(Peer, Left, Right) ->
		{ok, ar_intervals:intervals(), non_neg_integer() | infinity}
		| {error, cooldown | term()} when
	Peer :: term(),
	Left :: non_neg_integer(),
	Right :: non_neg_integer() | infinity.
get_peer_intervals(Peer, Left, _Right) ->
	case ar_rate_limiter:is_on_cooldown(Peer, ?GET_SYNC_RECORD_RPM_KEY) of
		true ->
			metric_inc(data_discovery_cache_events, [normal, cooldown]),
			{error, cooldown};
		false ->
			case cache_lookup({Peer, Left, normal}) of
				{hit, Intervals, PeerRightBound} ->
					metric_inc(data_discovery_cache_events, [normal, hit]),
					{ok, Intervals, PeerRightBound};
				miss ->
					metric_inc(data_discovery_cache_events, [normal, miss]),
					{error, cache_miss}
			end
	end.

%% @doc Return the peer's raw footprint intervals for the given
%% {Partition, Footprint} coordinate. One HTTP call returns all 1024
%% chunk-sized intervals for the footprint, scattered across the partition.
%% Cached under `{Peer, {Partition, Footprint}, footprint}'.
-spec get_peer_footprint_intervals(Peer, Partition, Footprint) ->
		{ok, ar_intervals:intervals()} | {error, cooldown | term()} when
	Peer :: term(),
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer().
get_peer_footprint_intervals(Peer, Partition, Footprint) ->
	case ar_rate_limiter:is_on_cooldown(Peer, ?GET_FOOTPRINT_RECORD_RPM_KEY) of
		true ->
			metric_inc(data_discovery_cache_events, [footprint, cooldown]),
			{error, cooldown};
		false ->
			case cache_lookup({Peer, {Partition, Footprint}, footprint}) of
				{hit, Intervals, _Meta} ->
					metric_inc(data_discovery_cache_events, [footprint, hit]),
					{ok, Intervals};
				miss ->
					metric_inc(data_discovery_cache_events, [footprint, miss]),
					{error, cache_miss}
			end
	end.

%% @doc ar_peer_sync calls this to publish where each storage module's
%% discover cursor currently is. The directory enqueues prefetch refresh
%% jobs for the next `?PREFETCH_STEPS_AHEAD'/`?PREFETCH_FOOTPRINTS_AHEAD'
%% units ahead, so the cache is populated before discover reaches them.
-spec advance_cursor(StoreID, Offset, Mode) -> ok when
	StoreID :: term(),
	Offset :: non_neg_integer(),
	Mode :: normal | footprint.
advance_cursor(StoreID, Offset, Mode) ->
	gen_server:cast(?MODULE, {advance_cursor, StoreID, Offset, Mode}).

%% @doc A fetch from `Peer' for byte range `Range' failed; mark the
%% corresponding cache rows stale so the next prefetch cycle re-issues the
%% HTTP call. `Range' is a {EndByte, StartByte} tuple matching ar_intervals.
-spec invalidate(Peer, Range) -> ok when
	Peer :: term(),
	Range :: {non_neg_integer(), non_neg_integer()}.
invalidate(Peer, Range) ->
	gen_server:cast(?MODULE, {invalidate, Peer, Range}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	%% ar_data_discovery_intervals is created in ar_sup alongside the
	%% ar_data_discovery / ar_data_discovery_footprint_buckets tables so
	%% the gen_server crashing or restarting doesn't wipe the cache.
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
		expiration_map = #{},
		refresh_queue = queue:new(),
		refresh_pending = 0,
		refresh_pids = #{},
		refresh_inflight = sets:new()
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

handle_cast({advance_cursor, _StoreID, Offset, Mode}, State) ->
	State2 = enqueue_prefetch_jobs(Offset, Mode, State),
	maybe_cast_refresh_step(State2),
	{noreply, State2};

handle_cast({invalidate, Peer, Range}, State) ->
	State2 = invalidate_range(Peer, Range, State),
	maybe_cast_refresh_step(State2),
	{noreply, State2};

handle_cast(refresh_step, State) ->
	{noreply, drain_refresh_queue(State)};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', _,  process, Pid, _}, State) ->
	case maps:take(Pid, State#state.refresh_pids) of
		{JobKey, Pids2} ->
			Inflight2 = sets:del_element(JobKey, State#state.refresh_inflight),
			State2 = State#state{
				refresh_pids = Pids2,
				refresh_inflight = Inflight2,
				refresh_pending = State#state.refresh_pending - 1 },
			maybe_cast_refresh_step(State2),
			{noreply, State2};
		error ->
			{noreply, State#state{ peers_pending = State#state.peers_pending - 1 }}
	end;

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
	case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE of
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

%%%===================================================================
%%% Prefetch / refresh worker pool.
%%%===================================================================

%% Build the list of (Peer, CacheKey, Mode) jobs ahead of Offset in this Mode
%% and enqueue the ones not already fresh or inflight.
enqueue_prefetch_jobs(Offset, normal, State) ->
	Starts = [Offset + N * ?QUERY_RANGE_STEP_SIZE || N <- lists:seq(0, ?PREFETCH_STEPS_AHEAD - 1)],
	lists:foldl(
		fun(Left, Acc) ->
			Bucket = Left div ?NETWORK_DATA_BUCKET_SIZE,
			Peers = get_bucket_peers(Bucket),
			lists:foldl(
				fun(Peer, Acc1) ->
					maybe_enqueue_refresh({Peer, Left, normal}, Acc1)
				end,
				Acc,
				Peers
			)
		end,
		State,
		Starts
	);
enqueue_prefetch_jobs(Offset, footprint, State) ->
	%% Walk ?PREFETCH_FOOTPRINTS_AHEAD footprints starting at Offset. Use
	%% the same iteration rule ar_peer_sync's discover loop uses so the
	%% directory warms the rows discover will actually consume.
	case ar_footprint_record:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE) of
		FootprintBucket when is_integer(FootprintBucket) ->
			Peers = get_footprint_bucket_peers(FootprintBucket),
			Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
			StartFootprint = ar_footprint_record:get_footprint(Offset + ?DATA_CHUNK_SIZE),
			Footprints = [StartFootprint + N || N <- lists:seq(0, ?PREFETCH_FOOTPRINTS_AHEAD - 1)],
			lists:foldl(
				fun(Footprint, Acc) ->
					lists:foldl(
						fun(Peer, Acc1) ->
							Key = {Peer, {Partition, Footprint}, footprint},
							maybe_enqueue_refresh(Key, Acc1)
						end,
						Acc,
						Peers
					)
				end,
				State,
				Footprints
			);
		_ ->
			State
	end.

maybe_enqueue_refresh(JobKey, #state{ refresh_inflight = Inflight,
		refresh_queue = Q } = State) ->
	case sets:is_element(JobKey, Inflight) of
		true ->
			State;
		false ->
			case cache_is_fresh(JobKey) of
				true ->
					State;
				false ->
					State#state{
						refresh_queue = queue:in(JobKey, Q),
						refresh_inflight = sets:add_element(JobKey, Inflight) }
			end
	end.

cache_is_fresh(Key) ->
	case cache_lookup(Key) of
		{hit, _, _} -> true;
		miss -> false
	end.

invalidate_range(Peer, {End, Start}, State) ->
	%% Drop any cache rows for this peer whose key intersects [Start, End).
	ToDelete = lists:foldl(
		fun({Key, _Intervals, _Meta, _FetchedAt}, Acc) ->
			case key_intersects(Key, Peer, Start, End) of
				true -> [Key | Acc];
				false -> Acc
			end
		end,
		[],
		ets:tab2list(?INTERVAL_CACHE_TABLE)
	),
	lists:foreach(
		fun(K) -> ets:delete(?INTERVAL_CACHE_TABLE, K) end,
		ToDelete
	),
	State.

key_intersects({KPeer, Left, normal}, Peer, Start, End)
		when KPeer =:= Peer ->
	not (Left >= End orelse Left + ?QUERY_RANGE_STEP_SIZE =< Start);
key_intersects({KPeer, {_Partition, _Footprint}, footprint}, Peer, _Start, _End)
		when KPeer =:= Peer ->
	true;
key_intersects(_, _, _, _) ->
	false.

maybe_cast_refresh_step(#state{ refresh_pending = P, refresh_queue = Q })
		when P < ?MAX_CONCURRENT_INTERVAL_REFRESHES ->
	report_refresh_metrics(P, Q),
	case queue:is_empty(Q) of
		true -> ok;
		false -> gen_server:cast(?MODULE, refresh_step)
	end;
maybe_cast_refresh_step(#state{ refresh_pending = P, refresh_queue = Q }) ->
	report_refresh_metrics(P, Q),
	ok.

report_refresh_metrics(Pending, Queue) ->
	metric_set(data_discovery_refresh_queue_depth, [], queue:len(Queue)),
	metric_set(data_discovery_refresh_in_flight, [], Pending).

metric_inc(Name, Labels) ->
	try prometheus_counter:inc(Name, Labels)
	catch _:_ -> ok
	end.

metric_set(Name, [], Value) ->
	try prometheus_gauge:set(Name, Value)
	catch _:_ -> ok
	end;
metric_set(Name, Labels, Value) ->
	try prometheus_gauge:set(Name, Labels, Value)
	catch _:_ -> ok
	end.

drain_refresh_queue(#state{ refresh_pending = P } = State)
		when P >= ?MAX_CONCURRENT_INTERVAL_REFRESHES ->
	State;
drain_refresh_queue(#state{ refresh_queue = Q, refresh_pending = P,
		refresh_pids = Pids } = State) ->
	case queue:out(Q) of
		{empty, _} ->
			State;
		{{value, JobKey}, Q2} ->
			Pid = spawn_link(fun() -> run_refresh_job(JobKey) end),
			_ = monitor(process, Pid),
			State2 = State#state{
				refresh_queue = Q2,
				refresh_pending = P + 1,
				refresh_pids = maps:put(Pid, JobKey, Pids) },
			maybe_cast_refresh_step(State2),
			State2
	end.

run_refresh_job({Peer, Left, normal}) ->
	case ar_rate_limiter:is_on_cooldown(Peer, ?GET_SYNC_RECORD_RPM_KEY) of
		true ->
			ok;
		false ->
			Right = Left + ?QUERY_RANGE_STEP_SIZE,
			_ = fetch_peer_intervals_http(Peer, Left, Right, {Peer, Left, normal}),
			ok
	end;
run_refresh_job({Peer, {Partition, Footprint}, footprint}) ->
	case ar_rate_limiter:is_on_cooldown(Peer, ?GET_FOOTPRINT_RECORD_RPM_KEY) of
		true ->
			ok;
		false ->
			Key = {Peer, {Partition, Footprint}, footprint},
			_ = fetch_peer_footprint_intervals_http(Peer, Partition, Footprint, Key),
			ok
	end.

%%%===================================================================
%%% Peer interval cache.
%%%===================================================================

cache_lookup(Key) ->
	case ets:lookup(?INTERVAL_CACHE_TABLE, Key) of
		[{_, Intervals, Meta, FetchedAtMs}] ->
			case erlang:system_time(millisecond) - FetchedAtMs
					=< ?PEER_INTERVAL_CACHE_TTL_MS of
				true -> {hit, Intervals, Meta};
				false -> miss
			end;
		[] ->
			miss
	end.

cache_store(Key, Intervals, Meta) ->
	ets:insert(?INTERVAL_CACHE_TABLE,
		{Key, Intervals, Meta, erlang:system_time(millisecond)}),
	ok.

-ifdef(AR_TEST).
%% @doc Drop all cached peer intervals. Intended for test setup so entries
%% from a previous test case don't bleed into the next one (peer identifiers
%% are reused across cases).
clear_interval_cache() ->
	ets:delete_all_objects(?INTERVAL_CACHE_TABLE),
	ok.
-endif.

fetch_peer_intervals_http(Peer, Left, Right, Key) ->
	Limit = ?QUERY_SYNC_INTERVALS_COUNT_LIMIT,
	PeerReply =
		case ar_peers:get_peer_release(Peer) >= ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE of
			true ->
				ar_http_iface_client:get_sync_record(Peer, Left + 1, Right, Limit);
			false ->
				ar_http_iface_client:get_sync_record(Peer, Left + 1, Limit)
		end,
	case PeerReply of
		{ok, PeerIntervals} ->
			PeerRightBound =
				case ar_intervals:is_empty(PeerIntervals) of
					true -> infinity;
					false -> element(1, ar_intervals:largest(PeerIntervals))
				end,
			cache_store(Key, PeerIntervals, PeerRightBound),
			Outcome = case ar_intervals:is_empty(PeerIntervals) of
				true -> empty;
				false -> ok
			end,
			metric_inc(data_discovery_refresh_completed, [normal, Outcome]),
			{ok, PeerIntervals, PeerRightBound};
		{error, too_many_requests} = Error ->
			ar_rate_limiter:set_cooldown(Peer,
				?GET_SYNC_RECORD_RPM_KEY, ?GET_SYNC_RECORD_COOLDOWN_MS),
			metric_inc(data_discovery_refresh_completed, [normal, cooldown]),
			Error;
		Error ->
			metric_inc(data_discovery_refresh_completed, [normal, http_error]),
			Error
	end.

fetch_peer_footprint_intervals_http(Peer, Partition, Footprint, Key) ->
	PeerReply =
		case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE of
			true ->
				ar_http_iface_client:get_footprints(Peer, Partition, Footprint);
			false ->
				%% We expect to get here only if the peer is upgraded and then
				%% downgraded again, because we check the peer release at the
				%% bucket collection stage.
				not_found
		end,
	case PeerReply of
		{ok, Intervals} ->
			cache_store(Key, Intervals, none),
			Outcome = case ar_intervals:is_empty(Intervals) of
				true -> empty;
				false -> ok
			end,
			metric_inc(data_discovery_refresh_completed, [footprint, Outcome]),
			{ok, Intervals};
		not_found ->
			Empty = ar_intervals:new(),
			cache_store(Key, Empty, none),
			metric_inc(data_discovery_refresh_completed, [footprint, empty]),
			{ok, Empty};
		{error, too_many_requests} = Error ->
			ar_rate_limiter:set_cooldown(Peer,
				?GET_FOOTPRINT_RECORD_RPM_KEY, ?GET_SYNC_RECORD_COOLDOWN_MS),
			metric_inc(data_discovery_refresh_completed, [footprint, cooldown]),
			Error;
		Error ->
			metric_inc(data_discovery_refresh_completed, [footprint, http_error]),
			Error
	end.
