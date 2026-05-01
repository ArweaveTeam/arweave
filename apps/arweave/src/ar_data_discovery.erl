-module(ar_data_discovery).

-behaviour(gen_server).

-export([start_link/0, get_bucket_peers/1, get_footprint_bucket_peers/1,
		collect_peers/0, pick_peers/2,
		get_peer_intervals/3, get_peer_footprint_intervals/3]).

-ifdef(AR_TEST).
-export([clear_interval_cache/0]).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_discovery.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Per-peer cache of byte intervals reported by each peer over the
%% network (via /data_sync_record for normal mode, /footprints for
%% footprint mode). Populated by the scanner pool in this module;
%% read by ar_peer_sync to compute fetchable intervals.
%%
%% Rows keyed by `{Peer, CacheKey, Mode}'. The MonotonicMs slot records
%% the time of the cache_store/3 that wrote the row; the periodic
%% expiration check uses ets:select to find each peer's most recent
%% write and evicts peers that haven't been written for too long. This
%% piggybacks liveness tracking on the writes that already happen,
%% avoiding a separate ETS table or per-fetch heartbeat casts.
%%
%%   normal:    {Key, Intervals, PeerRightBound, MonotonicMs}
%%   footprint: {Key, Intervals, none,           MonotonicMs}
-define(PEER_INTERVAL_CACHE_TABLE, ar_data_discovery_peer_intervals).

%% Fetch at most this many sync intervals from a peer at a time.
-ifdef(AR_TEST).
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 10).
-else.
-define(QUERY_SYNC_INTERVALS_COUNT_LIMIT, 1000).
-endif.

%% Emit a peer_scan progress log every N fine-grained fetches.
-define(PROGRESS_LOG_INTERVAL, 1000).

%% Periodic telemetry tick. On each fire the gen_server emits a
%% state-snapshot log line and the per-bucket coverage Prometheus
%% gauges. Single timer + handler covers both.
-ifdef(AR_TEST).
-define(TELEMETRY_TICK_MS, 10_000).
-else.
-define(TELEMETRY_TICK_MS, 60_000).
-endif.

%% Minimum interval between successive scan starts for the same
%% (Peer, Mode). A scanner that finishes faster than this is requeued via
%% cast_after for the remaining delay. A peer is auto-evicted if its
%% last_seen entry doesn't get touched within 2x this interval.
-ifdef(AR_TEST).
-define(MIN_SCAN_INTERVAL_MS, 200).
-else.
-define(MIN_SCAN_INTERVAL_MS, 60 * 60 * 1000). %% 1 hour
-endif.


%% Per-(Peer, Mode) scan stats, accumulated across the fine-grained walk
%% and reported on completion. Indicates how productive the peer is.
%% peer/mode/start_ms are carried so the accumulator can emit periodic
%% progress logs without restructuring its callers.
-record(scan_stats, {
	peer,                  %% target peer for this scan
	mode,                  %% normal | footprint
	start_ms,              %% monotonic ms at scan start; used for elapsed
	fetches = 0,           %% number of fine-grained HTTP calls issued
	fetches_with_data = 0, %% subset that returned non-empty intervals
	advertised_bytes = 0   %% sum of byte ranges across all returned intervals
}).

-record(state, {
	%% Peer => latest sync_buckets advertised by Peer. Updated by the scanner
	%% at the top of each (Peer, normal) cycle; also drives the per-bucket
	%% peer index in the ?MODULE ETS table.
	network_map = #{},
	%% Peer => latest footprint_buckets advertised by Peer. Updated by the
	%% scanner at the top of each (Peer, footprint) cycle; drives the
	%% ar_data_discovery_footprint_buckets ETS index.
	footprint_map = #{},
	%% Per-(Peer, Mode) scan loop. Scanners run one at a time per
	%% {Peer, Mode}; each scanner refreshes that peer's buckets first, then
	%% walks the fine-grained units (byte windows for normal,
	%% (Partition, Footprint) coordinates for footprint) intersected with
	%% the union of configured storage modules' unsynced ranges.
	%%
	%% scan_waiting: FIFO of {Peer, Mode} awaiting a scanner slot.
	%% scan_inflight: Pid => {Peer, Mode}; bounded by ?MAX_CONCURRENT_PEER_SCANS.
	%% scan_jobs: set({Peer, Mode}) - all live jobs (waiting + inflight +
	%% cooling-down between scans). Dedups add_peer re-enqueues and acts
	%% as a tombstone so delayed requeues / DOWN handlers can no-op when
	%% a peer was removed mid-cycle.
	%% scan_started_at: {Peer, Mode} => MonotonicMs of the last scan start.
	%% Used to enforce ?MIN_SCAN_INTERVAL_MS between successive scan starts
	%% for the same (Peer, Mode).
	scan_waiting = queue:new(),
	scan_inflight = #{},
	scan_jobs = sets:new(),
	scan_started_at = #{}
}).

%% The frequency of asking peers about their data.
-ifdef(AR_TEST).
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 4 * 60 * 1000).
-endif.

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

%% @doc Return the peer's most recently fetched sync-record intervals
%% starting at byte offset `Left'. The result is intersection-free and
%% per-peer; callers intersect with their own sought set. Pure ETS read -
%% the cache is populated by the per-peer scanner loop.
-spec get_peer_intervals(Peer, Left, Right) ->
		{ok, ar_intervals:intervals(), non_neg_integer() | infinity}
		| {error, cache_miss} when
	Peer :: term(),
	Left :: non_neg_integer(),
	Right :: non_neg_integer() | infinity.
get_peer_intervals(Peer, Left, _Right) ->
	%% Cache rows are written by the per-peer scanner at
	%% ?QUERY_RANGE_STEP_SIZE-aligned offsets. The caller's Left can be
	%% any byte offset (the discover cursor advances by EndReached, which
	%% is a peer-bound byte position, not aligned). Align down to the
	%% scanner's grid so the key matches what was written.
	Aligned = (Left div ?QUERY_RANGE_STEP_SIZE) * ?QUERY_RANGE_STEP_SIZE,
	case cache_lookup({Peer, Aligned, normal}) of
		{hit, Intervals, PeerRightBound} ->
			metric_inc(data_discovery_cache_events, [normal, hit]),
			{ok, Intervals, PeerRightBound};
		miss ->
			metric_inc(data_discovery_cache_events, [normal, miss]),
			{error, cache_miss}
	end.

%% @doc Return the peer's most recently fetched footprint intervals for
%% the given {Partition, Footprint} coordinate.
-spec get_peer_footprint_intervals(Peer, Partition, Footprint) ->
		{ok, ar_intervals:intervals()} | {error, cache_miss} when
	Peer :: term(),
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer().
get_peer_footprint_intervals(Peer, Partition, Footprint) ->
	case cache_lookup({Peer, {Partition, Footprint}, footprint}) of
		{hit, Intervals, _Meta} ->
			metric_inc(data_discovery_cache_events, [footprint, hit]),
			{ok, Intervals};
		miss ->
			metric_inc(data_discovery_cache_events, [footprint, miss]),
			{error, cache_miss}
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	%% ar_data_discovery_peer_intervals is created in ar_sup alongside the
	%% ar_data_discovery / ar_data_discovery_footprint_buckets tables so
	%% the gen_server crashing or restarting doesn't wipe the cache.
	{ok, _} = ar_timer:apply_interval(
		?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS,
		?MODULE,
		collect_peers,
		[],
		#{ skip_on_shutdown => false }
	),
	%% Single periodic telemetry tick: emits the state-snapshot log and
	%% the per-bucket coverage Prometheus gauges. Fires from inside the
	%% gen_server so it has direct access to State.
	erlang:send_after(?TELEMETRY_TICK_MS, self(), telemetry_tick),
	ok = ar_events:subscribe(peer),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
	{reply, ok, State}.

handle_cast({add_peer, Peer}, State) ->
	{noreply, maybe_start_scanners(enqueue_peer_scans(Peer, State))};

handle_cast({check_expiration, Peer}, State) ->
	%% Per-peer periodic expiration check. Reads the most recent
	%% cache_store timestamp for this peer from the cache table - the
	%% scanner writes there on every successful bucket / fine-grained
	%% fetch, so it doubles as the liveness signal. No heartbeat cast
	%% from the scanner is needed.
	case peer_last_cache_write(Peer) of
		none ->
			%% No cache rows for this peer (already evicted or never
			%% successfully scanned); drop the polling loop.
			gen_server:cast(?MODULE, {remove_peer, Peer}),
			{noreply, State};
		LastSeen ->
			Age = erlang:monotonic_time(millisecond) - LastSeen,
			case Age >= (?MIN_SCAN_INTERVAL_MS) * 2 of
				true ->
					gen_server:cast(?MODULE, {remove_peer, Peer}),
					{noreply, State};
				false ->
					ar_util:cast_after((?MIN_SCAN_INTERVAL_MS) * 2 - Age,
						?MODULE, {check_expiration, Peer}),
					{noreply, State}
			end
	end;

handle_cast({requeue_scan, Peer, Mode}, State) ->
	%% Delayed requeue from a recently-finished scan. Re-add to scan_waiting
	%% only if the (Peer, Mode) is still active - a remove_peer in the
	%% meantime drops it.
	State2 = case sets:is_element({Peer, Mode}, State#state.scan_jobs) of
		true ->
			State#state{ scan_waiting =
					queue:in({Peer, Mode}, State#state.scan_waiting) };
		false ->
			State
	end,
	{noreply, maybe_start_scanners(State2)};

handle_cast({add_peer_sync_buckets, Peer, SyncBuckets}, State) ->
	#state{ network_map = Map, footprint_map = FootprintMap } = State,
	maybe_schedule_expiration_check(Peer, Map, FootprintMap),
	Map2 = maps:put(Peer, SyncBuckets, Map),
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(?MODULE, {{Bucket, Share, Peer}})
		end,
		?NETWORK_DATA_BUCKET_SIZE,
		SyncBuckets
	),
	{noreply, State#state{ network_map = Map2 }};

handle_cast({add_peer_footprint_buckets, Peer, FootprintBuckets}, State) ->
	#state{ network_map = NetworkMap, footprint_map = Map } = State,
	maybe_schedule_expiration_check(Peer, NetworkMap, Map),
	Map2 = maps:put(Peer, FootprintBuckets, Map),
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(ar_data_discovery_footprint_buckets, {{Bucket, Share, Peer}})
		end,
		?NETWORK_FOOTPRINT_BUCKET_SIZE,
		FootprintBuckets
	),
	{noreply, State#state{ footprint_map = Map2 }};

handle_cast({remove_peer, Peer}, State) ->
	#state{ network_map = Map, footprint_map = FootprintMap,
			scan_waiting = Waiting, scan_jobs = Jobs } = State,
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
	%% Drop waiting {Peer, _} entries.
	Waiting2 = queue:filter(fun({P, _Mode}) -> P =/= Peer end, Waiting),
	Jobs2 = sets:filter(fun({P, _Mode}) -> P =/= Peer end, Jobs),
	StartedAt2 = maps:filter(
			fun({P, _Mode}, _T) -> P =/= Peer end,
			State#state.scan_started_at),
	%% Kill any in-flight scanners for this peer; their {Peer, Mode} is
	%% no longer in scan_jobs so on exit the DOWN handler won't requeue,
	%% but leaving them alive holds a slot for the entire fine-grained
	%% walk (hours). The DOWN handler still fires after exit(Pid, kill)
	%% and decrements scan_inflight there.
	{Inflight2, KilledScanners} = take_peer_inflight(
			Peer, State#state.scan_inflight),
	lists:foreach(fun(Pid) -> exit(Pid, kill) end, KilledScanners),
	wipe_peer_cache_rows(Peer),
	?LOG_INFO([{event, peer_removed_from_discovery},
			{peer, ar_util:format_peer(Peer)},
			{had_sync_buckets, maps:is_key(Peer, Map)},
			{had_footprint_buckets, maps:is_key(Peer, FootprintMap)},
			{killed_scanners, length(KilledScanners)},
			{remaining_known_peers,
				maps:size(Map2) + maps:size(FootprintMap2)}]),
	{noreply, State#state{ network_map = Map2, footprint_map = FootprintMap2,
			scan_waiting = Waiting2, scan_jobs = Jobs2,
			scan_inflight = Inflight2,
			scan_started_at = StartedAt2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', _, process, Pid, _Reason}, State) ->
	case maps:take(Pid, State#state.scan_inflight) of
		{{Peer, Mode}, Inflight2} ->
			State2 = State#state{ scan_inflight = Inflight2 },
			%% Requeue only if the (Peer, Mode) is still active (peer not
			%% removed, scan still wanted). Honor ?MIN_SCAN_INTERVAL_MS:
			%% if the previous scan started < interval ago, defer the
			%% requeue via cast_after; otherwise enqueue immediately.
			State3 = case sets:is_element({Peer, Mode}, State2#state.scan_jobs) of
				true ->
					schedule_requeue({Peer, Mode}, State2);
				false ->
					State2#state{ scan_started_at =
							maps:remove({Peer, Mode},
									State2#state.scan_started_at) }
			end,
			{noreply, maybe_start_scanners(State3)};
		error ->
			{noreply, State}
	end;

handle_info({event, peer, {removed, Peer}}, State) ->
	gen_server:cast(?MODULE, {remove_peer, Peer}),
	{noreply, State};

handle_info({event, peer, _}, State) ->
	{noreply, State};

handle_info(telemetry_tick, State) ->
	erlang:send_after(?TELEMETRY_TICK_MS, self(), telemetry_tick),
	emit_state_snapshot(State),
	emit_bucket_stats(),
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

%% Periodic state-snapshot log emitted by the telemetry tick. Used to
%% diagnose flatlines (e.g., peer count stuck at 0).
emit_state_snapshot(#state{ network_map = NMap, footprint_map = FMap,
		scan_waiting = Waiting, scan_inflight = Inflight, scan_jobs = Jobs }) ->
	ArPeersCount = try
			length(ar_peers:get_peers(current))
		catch _:_ -> -1
		end,
	{_, MailboxLen} = erlang:process_info(self(), message_queue_len),
	?LOG_INFO([{event, data_discovery_state_snapshot},
			{network_map_size, maps:size(NMap)},
			{footprint_map_size, maps:size(FMap)},
			{scan_waiting, queue:len(Waiting)},
			{scan_inflight, maps:size(Inflight)},
			{scan_jobs, sets:size(Jobs)},
			{ar_peers_current_pool, ArPeersCount},
			{mailbox_len, MailboxLen}]).

%% Per-storage-module per-mode bucket coverage Prometheus gauges,
%% emitted by the telemetry tick.
emit_bucket_stats() ->
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

%% On the peer's first appearance in either bucket map, schedule the
%% periodic expiration check. Subsequent bucket-fetch responses see
%% the peer is already known and skip the schedule. The check
%% reschedules itself via {check_expiration, Peer} until the peer is
%% removed.
maybe_schedule_expiration_check(Peer, NetworkMap, FootprintMap) ->
	case maps:is_key(Peer, NetworkMap) orelse maps:is_key(Peer, FootprintMap) of
		true ->
			ok;
		false ->
			ar_util:cast_after((?MIN_SCAN_INTERVAL_MS) * 2, ?MODULE,
				{check_expiration, Peer}),
			ok
	end.

%%%===================================================================
%%% Per-peer scanner pool.
%%%===================================================================

%% Enqueue scan jobs for a peer's two modes (footprint only if the peer's
%% release supports it). Caller must drive maybe_start_scanners/1 after.
enqueue_peer_scans(Peer, State) ->
	State2 = enqueue_scan({Peer, normal}, State),
	case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE of
		true -> enqueue_scan({Peer, footprint}, State2);
		false -> State2
	end.

%% Remove all entries from Inflight whose value is {Peer, _}, returning
%% the trimmed map and the list of pids that were dropped (so the caller
%% can exit them).
take_peer_inflight(Peer, Inflight) ->
	maps:fold(
		fun(Pid, {P, _Mode}, {Acc, Killed}) when P =:= Peer ->
				{Acc, [Pid | Killed]};
			(Pid, V, {Acc, Killed}) ->
				{maps:put(Pid, V, Acc), Killed}
		end,
		{#{}, []},
		Inflight
	).

enqueue_scan(Job, #state{ scan_waiting = Q, scan_jobs = Jobs } = State) ->
	case sets:is_element(Job, Jobs) of
		true ->
			State;
		false ->
			State#state{
				scan_waiting = queue:in(Job, Q),
				scan_jobs = sets:add_element(Job, Jobs) }
	end.

%% Decide how to requeue a just-finished (Peer, Mode) honoring the
%% ?MIN_SCAN_INTERVAL_MS minimum spacing between scan starts.
schedule_requeue({Peer, Mode}, State) ->
	#state{ scan_started_at = Started } = State,
	Now = erlang:monotonic_time(millisecond),
	StartedAt = maps:get({Peer, Mode}, Started, undefined),
	Elapsed = case StartedAt of
		undefined -> ?MIN_SCAN_INTERVAL_MS;
		_ -> Now - StartedAt
	end,
	Started2 = maps:remove({Peer, Mode}, Started),
	State2 = State#state{ scan_started_at = Started2 },
	case Elapsed >= ?MIN_SCAN_INTERVAL_MS of
		true ->
			State2#state{ scan_waiting =
					queue:in({Peer, Mode}, State2#state.scan_waiting) };
		false ->
			Delay = ?MIN_SCAN_INTERVAL_MS - Elapsed,
			ar_util:cast_after(Delay, ?MODULE, {requeue_scan, Peer, Mode}),
			State2
	end.

%% Spawn scanners up to ?MAX_CONCURRENT_PEER_SCANS, draining scan_waiting.
maybe_start_scanners(#state{ scan_inflight = Inflight,
		scan_waiting = Waiting } = State) ->
	report_scan_metrics(Inflight, Waiting),
	case maps:size(Inflight) >= max_concurrent_peer_scans() of
		true ->
			State;
		false ->
			case queue:out(Waiting) of
				{empty, _} ->
					State;
				{{value, {Peer, Mode}}, Waiting2} ->
					{Pid, _Ref} = spawn_monitor(fun() -> run_peer_scan(Peer, Mode) end),
					State2 = State#state{
						scan_waiting = Waiting2,
						scan_inflight =
							maps:put(Pid, {Peer, Mode}, Inflight),
						scan_started_at =
							maps:put({Peer, Mode},
								erlang:monotonic_time(millisecond),
								State#state.scan_started_at) },
					maybe_start_scanners(State2)
			end
	end.

max_concurrent_peer_scans() ->
	{ok, Config} = arweave_config:get_env(),
	Config#config.data_discovery_max_concurrent_peer_scans.

report_scan_metrics(Inflight, Waiting) ->
	metric_set(data_discovery_refresh_in_flight, [], maps:size(Inflight)),
	metric_set(data_discovery_refresh_queue_depth, [], queue:len(Waiting)).

%% Scanner body. Refreshes the peer's bucket map for this mode, then walks
%% the union of configured storage modules' unsynced ranges, fetching only
%% windows that fall in buckets the peer advertises. Blocks (sleeps) on
%% rate-limit cooldowns rather than skipping; non-cooldown HTTP failures
%% are logged and the unit is skipped.
run_peer_scan(Peer, Mode) ->
	StartMs = erlang:monotonic_time(millisecond),
	?LOG_INFO([{event, peer_scan}, {stage, started},
			{peer, ar_util:format_peer(Peer)}, {mode, Mode}]),
	Init = #scan_stats{ peer = Peer, mode = Mode, start_ms = StartMs },
	Stats = case Mode of
		normal ->
			case fetch_sync_buckets(Peer) of
				{ok, SyncBuckets} -> scan_normal_for_peer(Peer, SyncBuckets, Init);
				_ -> Init
			end;
		footprint ->
			case fetch_footprint_buckets(Peer) of
				{ok, FootprintBuckets} ->
					scan_footprint_for_peer(Peer, FootprintBuckets, Init);
				_ -> Init
			end
	end,
	?LOG_INFO([{event, peer_scan}, {stage, completed},
			{peer, ar_util:format_peer(Peer)}, {mode, Mode},
			{elapsed_ms, erlang:monotonic_time(millisecond) - StartMs},
			{fetches, Stats#scan_stats.fetches},
			{fetches_with_data, Stats#scan_stats.fetches_with_data},
			{advertised_mib, Stats#scan_stats.advertised_bytes div (1024 * 1024)}]).

scan_normal_for_peer(Peer, SyncBuckets, Init) ->
	{ok, Config} = arweave_config:get_env(),
	%% Shuffle modules so concurrent scanners don't all hammer the same
	%% module first - spreads load across the configured range.
	Modules = ar_util:shuffle_list(Config#config.storage_modules),
	lists:foldl(
		fun(StorageModule, Acc) ->
			StoreID = ar_storage_module:id(StorageModule),
			{Start, End} = ar_storage_module:get_range(StoreID),
			scan_normal_module(Peer, SyncBuckets, Start, End, StoreID, Acc)
		end,
		Init,
		Modules
	).

scan_normal_module(Peer, SyncBuckets, RangeStart, RangeEnd, StoreID, Acc) ->
	%% Shuffle window offsets so each scan pass visits the module's range
	%% in a different order. Restarts and concurrent scanners spread
	%% coverage across the partition rather than always walking from the
	%% lowest offset.
	Offsets = ar_util:shuffle_list(
			lists:seq(RangeStart, RangeEnd - 1, ?QUERY_RANGE_STEP_SIZE)),
	lists:foldl(
		fun(Offset, Acc1) ->
			scan_normal_window(Peer, SyncBuckets, Offset, RangeEnd, StoreID, Acc1)
		end,
		Acc,
		Offsets
	).

scan_normal_window(Peer, SyncBuckets, Start, RangeEnd, StoreID, Acc) ->
	StepEnd = min(Start + ?QUERY_RANGE_STEP_SIZE, RangeEnd),
	Bucket = Start div ?NETWORK_DATA_BUCKET_SIZE,
	case ar_sync_buckets:get(Bucket, ?NETWORK_DATA_BUCKET_SIZE, SyncBuckets) > 0 of
		true ->
			Unsynced = unsynced_intervals_in_window(Start, StepEnd, StoreID),
			case ar_intervals:is_empty(Unsynced) of
				true ->
					Acc;
				false ->
					Key = {Peer, Start, normal},
					accumulate_fetch_normal(
							fetch_peer_intervals_http(Peer, Start, StepEnd, Key),
							Acc)
			end;
		false ->
			Acc
	end.

unsynced_intervals_in_window(Start, End, StoreID) ->
	unsynced_intervals_in_window(Start, End, ar_intervals:new(), StoreID).

unsynced_intervals_in_window(Start, End, Acc, _StoreID) when Start >= End ->
	Acc;
unsynced_intervals_in_window(Start, End, Acc, StoreID) ->
	case ar_sync_record:get_next_synced_interval(Start, End, ar_data_sync, StoreID) of
		not_found ->
			ar_intervals:add(Acc, End, Start);
		{End2, Start2} ->
			case Start2 > Start of
				true ->
					End3 = min(Start2, End),
					unsynced_intervals_in_window(End2, End,
							ar_intervals:add(Acc, End3, Start), StoreID);
				_ ->
					unsynced_intervals_in_window(End2, End, Acc, StoreID)
			end
	end.

scan_footprint_for_peer(Peer, FootprintBuckets, Init) ->
	{ok, Config} = arweave_config:get_env(),
	Modules = ar_util:shuffle_list(Config#config.storage_modules),
	lists:foldl(
		fun(StorageModule, Acc) ->
			StoreID = ar_storage_module:id(StorageModule),
			{Start, End} = ar_storage_module:get_range(StoreID),
			scan_footprint_module(Peer, FootprintBuckets,
					Start, Start, End, StoreID, Acc)
		end,
		Init,
		Modules
	).

scan_footprint_module(Peer, FootprintBuckets, _Cursor, RangeStart, RangeEnd, StoreID, Acc) ->
	%% Shuffle the per-chunk offsets that map to unique footprints so each
	%% scan pass visits them in a different order. ar_replica_2_9's
	%% get_next_fetch_offset/3 defines the canonical iteration: walk
	%% chunk-by-chunk inside one sector, then jump to the next partition.
	Offsets = ar_util:shuffle_list(
			footprint_offsets(RangeStart, RangeEnd)),
	lists:foldl(
		fun(Cursor, Acc1) ->
			scan_footprint_offset(Peer, FootprintBuckets, Cursor, StoreID, Acc1)
		end,
		Acc,
		Offsets
	).

footprint_offsets(RangeStart, RangeEnd) ->
	footprint_offsets(RangeStart, RangeStart, RangeEnd, []).

footprint_offsets(Cursor, _RangeStart, RangeEnd, Acc) when Cursor >= RangeEnd ->
	lists:reverse(Acc);
footprint_offsets(Cursor, RangeStart, RangeEnd, Acc) ->
	Acc2 = [Cursor | Acc],
	Next = ar_replica_2_9:get_next_fetch_offset(Cursor, RangeStart, RangeEnd),
	case Next =< Cursor of
		true -> lists:reverse(Acc2);
		false -> footprint_offsets(Next, RangeStart, RangeEnd, Acc2)
	end.

scan_footprint_offset(Peer, FootprintBuckets, Cursor, StoreID, Acc) ->
	Partition = ar_replica_2_9:get_entropy_partition(Cursor + ?DATA_CHUNK_SIZE),
	Footprint = ar_footprint_record:get_footprint(Cursor + ?DATA_CHUNK_SIZE),
	FootprintBucket = ar_footprint_record:get_footprint_bucket(Cursor + ?DATA_CHUNK_SIZE),
	case is_integer(FootprintBucket)
			andalso ar_sync_buckets:get(FootprintBucket,
					?NETWORK_FOOTPRINT_BUCKET_SIZE, FootprintBuckets) > 0 of
		true ->
			Unsynced = ar_footprint_record:get_unsynced_intervals(
					Partition, Footprint, StoreID),
			case ar_intervals:is_empty(Unsynced) of
				true ->
					Acc;
				false ->
					Key = {Peer, {Partition, Footprint}, footprint},
					accumulate_fetch_footprint(
							fetch_peer_footprint_intervals_http(
									Peer, Partition, Footprint, Key),
							Acc)
			end;
		false ->
			Acc
	end.

%% Update scan_stats from the result of one fine-grained fetch. Errors
%% leave the byte count unchanged but bump fetch count so we can see
%% activity even when the peer returns nothing useful.
%%
%% Normal mode returns byte intervals - sum directly.
accumulate_fetch_normal({ok, Intervals, _PeerRightBound}, Acc) ->
	bump(ar_intervals:sum(Intervals), Acc);
accumulate_fetch_normal(_Other, Acc) ->
	bump(0, Acc).

%% Footprint mode returns intervals in footprint-offset units where one
%% unit = one chunk = ?DATA_CHUNK_SIZE bytes.
accumulate_fetch_footprint({ok, Intervals}, Acc) ->
	bump(ar_intervals:sum(Intervals) * ?DATA_CHUNK_SIZE, Acc);
accumulate_fetch_footprint(_Other, Acc) ->
	bump(0, Acc).

bump(Bytes, Acc) ->
	WithData = case Bytes of
		0 -> 0;
		_ -> 1
	end,
	Acc2 = Acc#scan_stats{
		fetches = Acc#scan_stats.fetches + 1,
		fetches_with_data = Acc#scan_stats.fetches_with_data + WithData,
		advertised_bytes = Acc#scan_stats.advertised_bytes + Bytes },
	maybe_log_progress(Acc2),
	Acc2.

maybe_log_progress(#scan_stats{ fetches = N } = Stats)
		when N rem ?PROGRESS_LOG_INTERVAL =:= 0 ->
	?LOG_INFO([{event, peer_scan}, {stage, progress},
			{peer, ar_util:format_peer(Stats#scan_stats.peer)},
			{mode, Stats#scan_stats.mode},
			{elapsed_ms,
				erlang:monotonic_time(millisecond) - Stats#scan_stats.start_ms},
			{fetches, N},
			{fetches_with_data, Stats#scan_stats.fetches_with_data},
			{advertised_mib, Stats#scan_stats.advertised_bytes div (1024 * 1024)}]);
maybe_log_progress(_Stats) ->
	ok.

fetch_sync_buckets(Peer) ->
	case ar_http_iface_client:get_sync_buckets(Peer) of
		{ok, SyncBuckets} ->
			gen_server:cast(?MODULE, {add_peer_sync_buckets, Peer, SyncBuckets}),
			{ok, SyncBuckets};
		{error, request_type_not_found} ->
			?LOG_DEBUG([{event, sync_buckets_request_type_not_found},
					{peer, ar_util:format_peer(Peer)}]),
			error;
		{error, Reason} ->
			ar_http_iface_client:log_failed_request(Reason,
				[{event, failed_to_fetch_sync_buckets},
				{peer, ar_util:format_peer(Peer)},
				{reason, io_lib:format("~p", [Reason])}]),
			error;
		Other ->
			?LOG_DEBUG([{event, failed_to_fetch_sync_buckets},
				{peer, ar_util:format_peer(Peer)},
				{reason, io_lib:format("~p", [Other])}]),
			error
	end.

fetch_footprint_buckets(Peer) ->
	case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE of
		false ->
			error;
		true ->
			case ar_http_iface_client:get_footprint_buckets(Peer) of
				{ok, FootprintBuckets} ->
					gen_server:cast(?MODULE,
							{add_peer_footprint_buckets, Peer, FootprintBuckets}),
					{ok, FootprintBuckets};
				{error, request_type_not_found} ->
					?LOG_DEBUG([{event, footprint_buckets_request_type_not_found},
							{peer, ar_util:format_peer(Peer)}]),
					error;
				{error, Reason} ->
					ar_http_iface_client:log_failed_request(Reason,
						[{event, failed_to_fetch_footprint_buckets},
						{peer, ar_util:format_peer(Peer)},
						{reason, io_lib:format("~p", [Reason])}]),
					error;
				Other ->
					?LOG_DEBUG([{event, failed_to_fetch_footprint_buckets},
						{peer, ar_util:format_peer(Peer)},
						{reason, io_lib:format("~p", [Other])}]),
					error
			end
	end.

wipe_peer_cache_rows(Peer) ->
	ets:select_delete(?PEER_INTERVAL_CACHE_TABLE,
		[{ {{Peer, '_', '_'}, '_', '_', '_'}, [], [true] }]).

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

%%%===================================================================
%%% Peer interval cache.
%%%===================================================================

cache_lookup(Key) ->
	case ets:lookup(?PEER_INTERVAL_CACHE_TABLE, Key) of
		[{_, Intervals, Meta, _MonotonicMs}] -> {hit, Intervals, Meta};
		[] -> miss
	end.

cache_store(Key, Intervals, Meta) ->
	ets:insert(?PEER_INTERVAL_CACHE_TABLE,
		{Key, Intervals, Meta, erlang:monotonic_time(millisecond)}),
	ok.

%% Most recent cache_store timestamp across all of Peer's rows, or
%% `none' if the peer has no cache rows. Used by the periodic
%% expiration check as the liveness signal.
peer_last_cache_write(Peer) ->
	case ets:select(?PEER_INTERVAL_CACHE_TABLE,
			[{ {{Peer, '_', '_'}, '_', '_', '$1'}, [], ['$1'] }]) of
		[] -> none;
		Timestamps -> lists:max(Timestamps)
	end.

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
		Error ->
			metric_inc(data_discovery_refresh_completed, [footprint, http_error]),
			Error
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
%% @doc Drop all cached peer intervals. Intended for test setup so entries
%% from a previous test case don't bleed into the next one (peer identifiers
%% are reused across cases).
clear_interval_cache() ->
	ets:delete_all_objects(?PEER_INTERVAL_CACHE_TABLE),
	ok.

cache_miss_returns_cache_miss_test() ->
	clear_interval_cache(),
	Peer = {10, 0, 0, 1, 1984},
	?assertEqual({error, cache_miss},
		get_peer_intervals(Peer, 0, infinity)),
	?assertEqual({error, cache_miss},
		get_peer_footprint_intervals(Peer, 0, 0)).
-endif.
