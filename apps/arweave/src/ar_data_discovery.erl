%%% @doc Maintains a cache of the data ranges served by each peer.
%%%
%%% Scanner jobs ask peers for byte ranges and replica-2.9 footprints, then
%%% store the answers in ETS.
%%%
%%% ar_peer_sync reads this cache when deciding what which chunks to fetch from
%%% which peers.
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

%% Per-peer cache of byte intervals reported by each peer
%% (via /data_sync_record for normal mode and /footprints for
%% footprint mode). Populated by the scanner pool in this module;
%% read by ar_peer_sync to compute fetchable intervals.
%%
%% Rows keyed by `{Peer, CacheKey, Mode}'. The MonotonicMs slot records
%% the time of the cache_store/3 that wrote the row - used by the periodic
%% expiration to determine if a peer is non-responsive and should be removed.
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
			{ok, Intervals, PeerRightBound};
		miss ->
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
			{ok, Intervals};
		miss ->
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
			gen_server:cast(?MODULE, {remove_peer, Peer, no_cache_rows}),
			{noreply, State};
		LastSeen ->
			Age = erlang:monotonic_time(millisecond) - LastSeen,
			case Age >= (?MIN_SCAN_INTERVAL_MS) * 2 of
				true ->
					gen_server:cast(?MODULE,
							{remove_peer, Peer, {cache_stale, Age}}),
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
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(?MODULE, {{Bucket, Share, Peer}})
		end,
		?NETWORK_DATA_BUCKET_SIZE,
		SyncBuckets
	),
	{noreply, State};

handle_cast({add_peer_footprint_buckets, Peer, FootprintBuckets}, State) ->
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(ar_data_discovery_footprint_buckets, {{Bucket, Share, Peer}})
		end,
		?NETWORK_FOOTPRINT_BUCKET_SIZE,
		FootprintBuckets
	),
	{noreply, State};

handle_cast({remove_peer, Peer}, State) ->
	%% Legacy callers without a reason — keep working but flag it so we
	%% know to update them.
	handle_cast({remove_peer, Peer, unspecified}, State);

handle_cast({remove_peer, Peer, Reason}, State) ->
	#state{ scan_waiting = Waiting, scan_jobs = Jobs } = State,
	%% Wipe Peer's rows from the bucket tables directly. Match-spec scan
	%% over the table is bounded (?NETWORK_*_BUCKET_SIZE = 10 GB → typical
	%% tables stay well under a million rows). Returns deletion counts so
	%% we can keep the had_*_buckets log signal.
	NumSync = ets:select_delete(?MODULE,
			[{ {{'_', '_', Peer}}, [], [true] }]),
	NumFootprint = ets:select_delete(ar_data_discovery_footprint_buckets,
			[{ {{'_', '_', Peer}}, [], [true] }]),
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
			%% peer_event_removed comes from ar_peers:remove_peer/2
			%% (low_success | banned | rotated) - grep ar_peers for the
			%% upstream reason. cache_stale / no_cache_rows mean the
			%% peer's scans stopped landing data on time.
			{reason, Reason},
			{had_sync_buckets, NumSync > 0},
			{had_footprint_buckets, NumFootprint > 0},
			{killed_scanners, length(KilledScanners)},
			{remaining_buckets_rows,
				ets:info(?MODULE, size)
				+ ets:info(ar_data_discovery_footprint_buckets, size)}]),
	{noreply, State#state{ scan_waiting = Waiting2, scan_jobs = Jobs2,
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
	gen_server:cast(?MODULE, {remove_peer, Peer, peer_event_removed}),
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
%% diagnose flatlines (e.g., peer count stuck at 0). The bucket-row
%% counts pair with `scan_jobs' to distinguish "no peers in rotation"
%% from "peers in rotation but no scans landing data."
emit_state_snapshot(#state{ scan_waiting = Waiting, scan_inflight = Inflight,
		scan_jobs = Jobs }) ->
	ArPeersCount = try
			length(ar_peers:get_peers(current))
		catch _:_ -> -1
		end,
	{_, MailboxLen} = erlang:process_info(self(), message_queue_len),
	?LOG_INFO([{event, data_discovery_state_snapshot},
			{network_buckets_rows, ets:info(?MODULE, size)},
			{footprint_buckets_rows,
					ets:info(ar_data_discovery_footprint_buckets, size)},
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
	AllPeers = collect_bucket_peers(StartBucket, EndBucket, ?MODULE, sets:new()),
	CoverageBytes = coverage_bytes_normal(AllPeers, RangeStart, RangeEnd),
	set_data_discovery_metrics(StoreID, normal, AllPeers, CoverageBytes);
report_bucket_stats(StoreID, RangeStart, RangeEnd, footprint) ->
	StartBucket = ar_footprint_record:get_footprint_bucket(RangeStart + ?DATA_CHUNK_SIZE),
	EndBucket = ar_footprint_record:get_footprint_bucket(RangeEnd),
	AllPeers = collect_bucket_peers(StartBucket, EndBucket,
			ar_data_discovery_footprint_buckets, sets:new()),
	CoverageBytes = coverage_bytes_footprint(AllPeers, RangeStart, RangeEnd),
	set_data_discovery_metrics(StoreID, footprint, AllPeers, CoverageBytes).

%% Walk [StartBucket, EndBucket] in `Table` and return the union of peers
%% advertising any bucket in the range.
collect_bucket_peers(StartBucket, EndBucket, _Table, Peers)
		when StartBucket > EndBucket ->
	Peers;
collect_bucket_peers(Bucket, EndBucket, Table, Peers) ->
	Peers2 = collect_peers_for_bucket(Bucket, Table, Peers,
			{Bucket, 0, no_peer}),
	collect_bucket_peers(Bucket + 1, EndBucket, Table, Peers2).

collect_peers_for_bucket(Bucket, Table, Peers, Cursor) ->
	case ets:next(Table, Cursor) of
		{Bucket, _Share, Peer} = Key ->
			collect_peers_for_bucket(Bucket, Table,
					sets:add_element(Peer, Peers), Key);
		_ ->
			Peers
	end.

%% Bytes covered by ≥1 peer (union, deduplicated) within [RangeStart, RangeEnd].
%% Iterates each peer's normal-mode cache rows (byte ranges in absolute weave
%% space), unions them, then restricts to the storage module's range and sums.
coverage_bytes_normal(Peers, RangeStart, RangeEnd) ->
	Union = sets:fold(
		fun(Peer, Acc) ->
			Rows = ets:select(?PEER_INTERVAL_CACHE_TABLE,
					[{ {{Peer, '_', normal}, '$1', '_', '_'}, [], ['$1'] }]),
			lists:foldl(fun ar_intervals:union/2, Acc, Rows)
		end,
		ar_intervals:new(),
		Peers
	),
	%% Restrict to [RangeStart, RangeEnd]: cut to right bound, then drop the
	%% left tail [0, RangeStart) via outerjoin.
	Cut = ar_intervals:cut(Union, RangeEnd),
	Restricted = ar_intervals:outerjoin(
			ar_intervals:from_list([{RangeStart, -1}]), Cut),
	ar_intervals:sum(Restricted).

%% Bytes covered by ≥1 peer in footprint mode, restricted to the storage
%% module's partition range. Cache rows are keyed by {Peer, {Partition,
%% Footprint}, footprint} and store footprint chunk indices; chunk indices
%% are namespaced by (Partition, Footprint), so we union per key (across
%% peers) before counting. Result is total distinct chunks × ?DATA_CHUNK_SIZE.
coverage_bytes_footprint(Peers, RangeStart, RangeEnd) ->
	StartPartition = ar_replica_2_9:get_entropy_partition(
			RangeStart + ?DATA_CHUNK_SIZE),
	%% RangeEnd is exclusive (one past the last byte). Subtract DATA_CHUNK_SIZE
	%% so an aligned RangeEnd = N * PARTITION_SIZE doesn't overshoot into
	%% partition N and double-count.
	EndPartition = ar_replica_2_9:get_entropy_partition(
			RangeEnd - ?DATA_CHUNK_SIZE),
	%% Group cache rows by (Partition, Footprint), union intervals across peers
	%% per key, then sum the union sizes.
	ByKey = sets:fold(
		fun(Peer, Acc) ->
			Rows = ets:select(?PEER_INTERVAL_CACHE_TABLE,
					[{ {{Peer, '$1', footprint}, '$2', '_', '_'},
						[], [{{'$1', '$2'}}] }]),
			lists:foldl(
				fun({{Partition, _Footprint} = Key, Intervals}, A)
						when Partition >= StartPartition,
							Partition =< EndPartition ->
					Existing = maps:get(Key, A, ar_intervals:new()),
					maps:put(Key, ar_intervals:union(Existing, Intervals), A);
				   ({_Key, _Intervals}, A) ->
					A
				end, Acc, Rows)
		end,
		#{},
		Peers
	),
	TotalChunks = maps:fold(
			fun(_K, Intervals, Acc) -> Acc + ar_intervals:sum(Intervals) end,
			0, ByKey),
	TotalChunks * ?DATA_CHUNK_SIZE.

set_data_discovery_metrics(StoreID, Type, AllPeers, CoverageBytes) ->
	StoreIDLabel = ar_storage_module:label(StoreID),
	prometheus_gauge:set(data_discovery, [Type, StoreIDLabel, num_peers],
			sets:size(AllPeers)),
	prometheus_gauge:set(data_discovery, [Type, StoreIDLabel, coverage_bytes],
			CoverageBytes).

%%%===================================================================
%%% Per-peer scanner pool.
%%%===================================================================

%% Enqueue scan jobs for a peer's two modes (footprint only if the peer's
%% release supports it). Caller must drive maybe_start_scanners/1 after.
%%
%% The first time a peer enters scan_jobs we kick off its periodic
%% {check_expiration, Peer} loop. The check reschedules itself until
%% remove_peer drops the peer; by gating on scan_jobs membership we
%% schedule it exactly once per peer-lifetime.
enqueue_peer_scans(Peer, State) ->
	maybe_schedule_expiration_check(Peer, State#state.scan_jobs),
	State2 = enqueue_scan({Peer, normal}, State),
	case ar_peers:get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE of
		true -> enqueue_scan({Peer, footprint}, State2);
		false -> State2
	end.

maybe_schedule_expiration_check(Peer, ScanJobs) ->
	case sets:is_element({Peer, normal}, ScanJobs)
			orelse sets:is_element({Peer, footprint}, ScanJobs) of
		true ->
			ok;
		false ->
			ar_util:cast_after((?MIN_SCAN_INTERVAL_MS) * 2, ?MODULE,
				{check_expiration, Peer}),
			ok
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
	%%
	%% Walk on the step-size grid (align RangeStart down). The cache key
	%% written here MUST match what `get_peer_intervals/3` looks up — that
	%% function aligns the caller's `Left` down to a step boundary, so the
	%% scan key has to be on the same grid. Storage modules whose RangeStart
	%% is not on a step boundary (common in tests where step=10MB and
	%% partitions are 2MB) used to write keys the discover loop never
	%% read, leaving sync stuck above the first window.
	StepStart = (RangeStart div ?QUERY_RANGE_STEP_SIZE) * ?QUERY_RANGE_STEP_SIZE,
	Offsets = ar_util:shuffle_list(
			lists:seq(StepStart, RangeEnd - 1, ?QUERY_RANGE_STEP_SIZE)),
	lists:foldl(
		fun(Offset, Acc1) ->
			scan_normal_window(Peer, SyncBuckets, Offset, StoreID, Acc1)
		end,
		Acc,
		Offsets
	).

scan_normal_window(Peer, SyncBuckets, Start, StoreID, Acc) ->
	StepEnd = Start + ?QUERY_RANGE_STEP_SIZE,
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
			{ok, PeerIntervals, PeerRightBound};
		Error ->
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
			{ok, Intervals};
		not_found ->
			Empty = ar_intervals:new(),
			cache_store(Key, Empty, none),
			{ok, Empty};
		Error ->
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
