%% @doc Network chunk-sync subsystem. Mirror of ar_chunk_copy: where
%% ar_chunk_copy fetches chunks from local sibling storage modules,
%% ar_peer_sync fetches them from network peers.
%%
%% Two stateless entry points, both called from ar_data_sync's mailbox:
%%
%%  - `discover/1' (producer): match cached peer offers (from
%%    ar_data_discovery) against this module's unsynced gaps (from
%%    ar_sync_record), enqueue per-chunk tasks into sync_task_queue,
%%    advance scan_cursor. Returns `{State', cast_now}' (run another
%%    discover immediately) or `{State', {cast_after, Ms}}' (pause).
%%    No HTTP happens here - peer query latency lives in the directory's
%%    background refresh pool instead.
%%
%%  - `sync/1' (consumer): pop one task from sync_task_queue, dispatch
%%    it to ar_data_sync_coordinator (which routes through peer_worker
%%    to a sync_worker that performs the HTTP fetch). Returns updated
%%    State'.
-module(ar_peer_sync).

-export([discover/1, sync/1]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([cut_peer_footprint_intervals/3, get_next_fetch_offset/3]).
-endif.

-define(GET_SYNC_RECORD_RPM_KEY, data_sync_record).
-define(GET_FOOTPRINT_RECORD_RPM_KEY, footprints).
-define(GET_SYNC_RECORD_PATH, [<<"data_sync_record">>]).
-define(GET_FOOTPRINT_RECORD_PATH, [<<"footprints">>]).
-define(FOOTPRINT_MIGRATION_CURSOR_KEY, <<"footprint_migration_cursor">>).

%% Max queued chunks per sync worker. Shared with ar_data_sync's queue-full
%% threshold so both modules compute MaxQueueSize identically.
-define(INTERVALS_QUEUE_CHUNKS_PER_WORKER, 250).

%% How long to pause after init_cursor primes prefetch but before the first
%% discover call reads the cache. Small enough that startup lag is negligible,
%% big enough that refresh workers can land at least one HTTP response.
-ifdef(AR_TEST).
-define(COLD_START_DELAY_MS, 200).
-else.
-define(COLD_START_DELAY_MS, 2000).
-endif.

%% Adaptive restart delay for the discover loop. Productive scans restart at
%% the min delay; unproductive scans double (capped at max). Resets on any
%% productive scan.
-ifdef(AR_TEST).
-define(DISCOVER_RESTART_MIN_DELAY_MS, 1_000).
-define(DISCOVER_RESTART_MAX_DELAY_MS, 1_000).
-else.
-define(DISCOVER_RESTART_MIN_DELAY_MS, 10_000).
-define(DISCOVER_RESTART_MAX_DELAY_MS, 120_000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

discover(#sync_data_state{ scan_cursor = undefined } = State) ->
	case init_cursor(State, normal) of
		{ok, Cursor} ->
			%% Cold start: init_cursor has primed the directory's prefetch,
			%% but the HTTP calls haven't landed yet. Pause before the first
			%% real discover so it reads a warm cache instead of racing
			%% through an empty one.
			{State#sync_data_state{ scan_cursor = Cursor }, {cast_after, ?COLD_START_DELAY_MS}};
		not_ready ->
			%% Node not joined yet, or footprint migration in flight.
			{State, {cast_after, 1000}}
	end;
discover(#sync_data_state{ scan_cursor = #scan_cursor{ offset = O, end_ = E } } = State)
		when O >= E ->
	#sync_data_state{ store_id = StoreID,
			scan_cursor = #scan_cursor{ mode = Mode, tasks_produced = TasksProduced,
					had_peers = HadPeers } } = State,
	{Delay, Backoff2} = scan_restart_delay(State#sync_data_state.scan_cursor),
	NextMode = flip_mode(Mode),
	?LOG_INFO([{event, sync_network}, {stage, discovery_complete},
		{store_id, StoreID}, {mode, Mode}, {tasks_produced, TasksProduced},
		{had_peers, HadPeers}, {next_mode, NextMode}, {restart_delay_ms, Delay}]),
	case init_cursor(State, NextMode, Backoff2) of
		{ok, Cursor2} ->
			{State#sync_data_state{ scan_cursor = Cursor2 }, {cast_after, Delay}};
		not_ready ->
			{State, {cast_after, 1000}}
	end;
discover(State) ->
	case scan_ready(State) of
		{wait, _Reason, Delay} ->
			{State, {cast_after, Delay}};
		ready ->
			do_discover(State)
	end.

%% @doc Consumer side: pop one task from sync_task_queue and dispatch it
%% to ar_data_sync_coordinator. Self-perpetuating - on success schedules
%% the next sync immediately; on backpressure or empty queue schedules a
%% retry via cast_after. Called from ar_data_sync's mailbox, so `self()'
%% refers to the calling ar_data_sync_<StoreID> process.
sync(State) ->
	#sync_data_state{ sync_task_queue = Q, store_id = StoreID } = State,
	IsQueueEmpty =
		case ar_sync_task_queue:is_empty(Q) of
			true ->
				ar_util:cast_after(500, self(), sync),
				true;
			false ->
				false
		end,
	IsDiskSpaceSufficient =
		case IsQueueEmpty of
			true ->
				false;
			false ->
				case ar_data_sync:is_disk_space_sufficient(StoreID) of
					true ->
						true;
					_ ->
						ar_util:cast_after(30000, self(), sync),
						false
				end
		end,
	IsChunkCacheFull =
		case IsDiskSpaceSufficient of
			false ->
				true;
			true ->
				case ar_data_sync:is_chunk_cache_full() of
					true ->
						ar_util:cast_after(1000, self(), sync),
						true;
					false ->
						false
				end
		end,
	AreSyncWorkersBusy =
		case IsChunkCacheFull of
			true ->
				true;
			false ->
				case ar_data_sync_coordinator:ready_for_work() of
					false ->
						ar_util:cast_after(200, self(), sync),
						true;
					true ->
						false
				end
		end,
	case AreSyncWorkersBusy of
		true ->
			State;
		false ->
			gen_server:cast(self(), sync),
			{{FootprintKey, Start, End, Peer}, Q2} = ar_sync_task_queue:take_smallest(Q),
			ar_data_sync_coordinator:sync_range(#sync_task{
						start_offset = Start,
						end_offset = End,
						peer = Peer,
						store_id = StoreID,
						footprint_key = FootprintKey
					}),
			State#sync_data_state{ sync_task_queue = Q2 }
	end.

%% @doc Build a cursor for a scan pass in the given mode. Clamps `end' by
%% WeaveSize and (for normal mode) DiskPoolThreshold. Returns `not_ready'
%% if prerequisites (joined, footprint migration) aren't satisfied.
init_cursor(State, Mode) ->
	init_cursor(State, Mode, 0).

init_cursor(#sync_data_state{ store_id = StoreID } = State, Mode, Backoff) ->
	case prereqs_ok(StoreID) of
		false ->
			not_ready;
		true ->
			#sync_data_state{ range_start = Start, range_end = End } = State,
			End2 = case Mode of
				footprint ->
					min(End, ar_disk_pool:get_threshold());
				normal ->
					End
			end,
			%% Prime the directory's prefetch at the cursor's starting point
			%% so the first discover call has a warm cache instead of reading
			%% misses for a full pass while prefetch catches up.
			ar_data_discovery:advance_cursor(StoreID, Start, Mode),
			?LOG_INFO([{event, sync_network}, {stage, discovery_started},
				{store_id, StoreID}, {mode, Mode},
				{start, Start}, {end_, End2}, {backoff_ms, Backoff}]),
			{ok, #scan_cursor{
				start = Start, end_ = End2, offset = Start, mode = Mode,
				backoff_ms = Backoff }}
	end.

%% @doc Can the discover side take a productive advance right now? Consolidates every
%% "no, wait" case: disk space, queue fullness, cursor bounds.
scan_ready(#sync_data_state{ scan_cursor = undefined }) ->
	ready;
scan_ready(#sync_data_state{ store_id = StoreID,
		sync_task_queue = Q,
		weave_size = WeaveSize,
		scan_cursor = #scan_cursor{ offset = Offset, end_ = End } = _Cursor }) ->
	case ar_data_sync:is_disk_space_sufficient(StoreID) of
		false ->
			{wait, disk_full, 30_000};
		not_initialized ->
			{wait, disk_info_missing, 1_000};
		true ->
			StoreIDLabel = ar_storage_module:label(StoreID),
			QSize = ar_sync_task_queue:size(Q),
			prometheus_gauge:set(sync_intervals_queue_size, [StoreIDLabel], QSize),
			MaxQueueSize = max(
				?NETWORK_DATA_BUCKET_SIZE div ?DATA_CHUNK_SIZE,
				ar_data_sync_coordinator:sync_jobs() * ?INTERVALS_QUEUE_CHUNKS_PER_WORKER),
			case QSize > MaxQueueSize of
				true ->
					{wait, queue_full, 500};
				false ->
					End2 = min(End, WeaveSize),
					case Offset >= End2 of
						true ->
							{wait, offset_past_end, 500};
						false ->
							ready
					end
			end
	end.

%%%===================================================================
%%% Step implementation.
%%%===================================================================

do_discover(#sync_data_state{ scan_cursor = #scan_cursor{ mode = Mode, offset = Offset } } = State) ->
	#sync_data_state{ store_id = StoreID } = State,
	ar_data_discovery:advance_cursor(StoreID, Offset, Mode),
	case Mode of
		normal -> do_discover_normal(State);
		footprint -> do_discover_footprint(State)
	end.

do_discover_normal(State) ->
	#sync_data_state{ store_id = StoreID, sync_task_queue = Q,
			weave_size = WeaveSize,
			scan_cursor = #scan_cursor{ offset = Offset, end_ = End } = Cursor } = State,
	End2 = min(min(Offset + ?QUERY_RANGE_STEP_SIZE, End), WeaveSize),
	UnsyncedIntervals = get_unsynced_intervals(Offset, End2, StoreID),
	case ar_intervals:is_empty(UnsyncedIntervals) of
		true ->
			%% No unsynced bytes in this window; advance.
			NewCursor = Cursor#scan_cursor{ offset = End2 },
			{State#sync_data_state{ scan_cursor = NewCursor }, cast_now};
		false ->
			case get_hot_peers(Offset, normal) of
				wait ->
					%% All known peers for this bucket are on cooldown; hold
					%% the offset and retry.
					{State, {cast_after, 1000}};
				Peers ->
					{EndReached, EnqueueIntervals, HadPeers} =
						collect_peer_intervals_normal(Offset, Peers, UnsyncedIntervals),
					NewQ = enqueue(EnqueueIntervals, Q),
					Produced = ar_sync_task_queue:size(NewQ) - ar_sync_task_queue:size(Q),
					maybe_log_chunk_sync_started(StoreID, normal, Cursor, Produced),
					NewCursor = Cursor#scan_cursor{
						offset = min(End2, EndReached),
						had_peers = Cursor#scan_cursor.had_peers orelse HadPeers,
						tasks_produced = Cursor#scan_cursor.tasks_produced + max(0, Produced)
					},
					{State#sync_data_state{ sync_task_queue = NewQ,
							scan_cursor = NewCursor }, cast_now}
			end
	end.

do_discover_footprint(State) ->
	#sync_data_state{ store_id = StoreID, sync_task_queue = Q,
			scan_cursor = #scan_cursor{ start = Start, end_ = End, offset = Offset }
					= Cursor } = State,
	Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
	Footprint = ar_footprint_record:get_footprint(Offset + ?DATA_CHUNK_SIZE),
	UnsyncedIntervals =
		ar_footprint_record:get_unsynced_intervals(Partition, Footprint, StoreID),
	case ar_intervals:is_empty(UnsyncedIntervals) of
		true ->
			Offset2 = get_next_fetch_offset(Offset, Start, End),
			NewCursor = Cursor#scan_cursor{ offset = Offset2 },
			{State#sync_data_state{ scan_cursor = NewCursor }, cast_now};
		false ->
			case get_hot_peers(Offset, footprint) of
				wait ->
					{State, {cast_after, 1000}};
				Peers ->
					{EnqueueIntervals, HadPeers} = collect_peer_intervals_footprint(
							Partition, Footprint, Start, End, Peers, UnsyncedIntervals),
					NewQ = enqueue(EnqueueIntervals, Q),
					Produced = ar_sync_task_queue:size(NewQ) - ar_sync_task_queue:size(Q),
					maybe_log_chunk_sync_started(StoreID, footprint, Cursor, Produced),
					Offset2 = get_next_fetch_offset(Offset, Start, End),
					NewCursor = Cursor#scan_cursor{
						offset = Offset2,
						had_peers = Cursor#scan_cursor.had_peers orelse HadPeers,
						tasks_produced = Cursor#scan_cursor.tasks_produced + max(0, Produced)
					},
					{State#sync_data_state{ sync_task_queue = NewQ,
							scan_cursor = NewCursor }, cast_now}
			end
	end.

%% Log once per scan pass, on the transition from "no tasks produced yet"
%% to "first tasks enqueued". The dispatch side (sync/1) picks up tasks
%% off the queue continuously, so there's no clean per-task log; this
%% fires exactly when a pass first hands work to the dispatch loop.
maybe_log_chunk_sync_started(StoreID, Mode,
		#scan_cursor{ tasks_produced = 0 }, Produced) when Produced > 0 ->
	?LOG_INFO([{event, sync_network}, {stage, chunk_sync_started},
		{store_id, StoreID}, {mode, Mode}, {tasks_enqueued, Produced}]);
maybe_log_chunk_sync_started(_StoreID, _Mode, _Cursor, _Produced) ->
	ok.

%%%===================================================================
%%% Per-peer gather from the directory cache.
%%%===================================================================

collect_peer_intervals_normal(Left, Peers, SoughtIntervals) ->
	{MinRightBound, Entries, HadPeers} = lists:foldl(
		fun(Peer, {RightAcc, Acc, HadAcc}) ->
			case ar_data_discovery:get_peer_intervals(Peer, Left, infinity) of
				{ok, PeerIntervals, PeerRight} ->
					Intersected = ar_intervals:intersection(PeerIntervals, SoughtIntervals),
					case ar_intervals:is_empty(Intersected) of
						true -> {min(RightAcc, PeerRight), Acc, true};
						false ->
							{min(RightAcc, PeerRight),
								[{Peer, Intersected, none} | Acc], true}
					end;
				{error, _} ->
					{RightAcc, Acc, HadAcc}
			end
		end,
		{infinity, [], false},
		Peers
	),
	{MinRightBound, Entries, HadPeers}.

collect_peer_intervals_footprint(Partition, Footprint, Start, End, Peers, SoughtIntervals) ->
	{Entries, HadPeers} = lists:foldl(
		fun(Peer, {Acc, HadAcc}) ->
			case ar_data_discovery:get_peer_footprint_intervals(Peer, Partition, Footprint) of
				{ok, PeerIntervals} ->
					Intersected = ar_intervals:intersection(PeerIntervals, SoughtIntervals),
					case ar_intervals:is_empty(Intersected) of
						true -> {Acc, true};
						false ->
							ByteIntervals = cut_peer_footprint_intervals(
								Intersected, Start, End),
							FootprintKey = {Partition, Footprint, Peer},
							{[{Peer, ByteIntervals, FootprintKey} | Acc], true}
					end;
				{error, _} ->
					{Acc, HadAcc}
			end
		end,
		{[], false},
		Peers
	),
	{Entries, HadPeers}.

%% Distribute work fairly across peers in a scan pass: shuffle so no one
%% peer dominates, cap each peer at ~1.5x fair share of the bucket.
enqueue([], Queue) ->
	Queue;
enqueue(PeerEntries, Queue) ->
	TotalChunksToEnqueue = ?DEFAULT_SYNC_BUCKET_SIZE div ?DATA_CHUNK_SIZE,
	NumPeers = length(PeerEntries),
	ScalingFactor = 1.5,
	ChunksPerPeer = trunc(((TotalChunksToEnqueue + NumPeers - 1) div NumPeers) * ScalingFactor),
	ar_sync_task_queue:insert_batch(
		ar_util:shuffle_list(PeerEntries), ChunksPerPeer, Queue).

%%%===================================================================
%%% Peer picking.
%%%===================================================================

get_hot_peers(Offset, normal) ->
	Bucket = Offset div ?NETWORK_DATA_BUCKET_SIZE,
	get_hot_peers_for_bucket(
		fun() -> ar_data_discovery:get_bucket_peers(Bucket) end,
		?GET_SYNC_RECORD_RPM_KEY,
		?GET_SYNC_RECORD_PATH);
get_hot_peers(Offset, footprint) ->
	FootprintBucket = ar_footprint_record:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE),
	get_hot_peers_for_bucket(
		fun() -> ar_data_discovery:get_footprint_bucket_peers(FootprintBucket) end,
		?GET_FOOTPRINT_RECORD_RPM_KEY,
		?GET_FOOTPRINT_RECORD_PATH).

get_hot_peers_for_bucket(GetAllFun, RPMKey, Path) ->
	{ok, Config} = arweave_config:get_env(),
	AllPeers =
		case Config#config.sync_from_local_peers_only of
			true -> Config#config.local_peers;
			false -> GetAllFun()
		end,
	HotPeers = [
		Peer || Peer <- AllPeers,
			not ar_rate_limiter:is_on_cooldown(Peer, RPMKey) andalso
			not ar_rate_limiter:is_throttled(Peer, Path)
	],
	case length(AllPeers) > 0 andalso length(HotPeers) == 0 of
		true ->
			wait;
		false ->
			ar_data_discovery:pick_peers(HotPeers, ?QUERY_BEST_PEERS_COUNT)
	end.

%%%===================================================================
%%% Unsynced interval gathering.
%%%===================================================================

get_unsynced_intervals(Start, End, StoreID) ->
	UnsyncedIntervals = get_unsynced_intervals(Start, End, ar_intervals:new(), StoreID),
	BlacklistedIntervals = ar_tx_blacklist:get_blacklisted_intervals(Start, End),
	ar_intervals:outerjoin(BlacklistedIntervals, UnsyncedIntervals).

get_unsynced_intervals(Start, End, Intervals, _StoreID) when Start >= End ->
	Intervals;
get_unsynced_intervals(Start, End, Intervals, StoreID) ->
	case ar_sync_record:get_next_synced_interval(Start, End, ar_data_sync, StoreID) of
		not_found ->
			ar_intervals:add(Intervals, End, Start);
		{End2, Start2} ->
			case Start2 > Start of
				true ->
					End3 = min(Start2, End),
					get_unsynced_intervals(End2, End,
							ar_intervals:add(Intervals, End3, Start), StoreID);
				_ ->
					get_unsynced_intervals(End2, End, Intervals, StoreID)
			end
	end.

%%%===================================================================
%%% Cursor bookkeeping.
%%%===================================================================

%% @doc Calculate the next fetch start position after processing a sector.
%% Advances by one chunk within a sector, or jumps to the next partition
%% boundary when near the sector end.
get_next_fetch_offset(Offset, Start, End) ->
	SectorSize = ar_block:get_replica_2_9_entropy_sector_size(),
	Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
	{PartitionStart, PartitionEnd} = ar_replica_2_9:get_entropy_partition_range(Partition),
	SectorStart = max(Start, PartitionStart),
	SectorEnd = min(PartitionEnd, SectorStart + SectorSize),
	Offset2 =
		case Offset + 2 * ?DATA_CHUNK_SIZE > SectorEnd of
			true ->
				PartitionEnd;
			false ->
				Offset + ?DATA_CHUNK_SIZE
		end,
	min(Offset2, End).

%% @doc The intervals returned by a peer may include intervals beyond the
%% storage module boundaries. This is because we end up querying all seeded
%% intervals belonging to a footprint that intersects this node's unsynced
%% intervals. Remove everything outside [Start, End].
cut_peer_footprint_intervals(FootprintIntervals, Start, End) ->
	ByteIntervals =
		ar_footprint_record:get_intervals_from_footprint_intervals(FootprintIntervals),
	ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
	PaddedStart =
		case ar_block:get_chunk_padded_offset(Start) of
			Start -> Start;
			PaddedOffset -> PaddedOffset - ?DATA_CHUNK_SIZE
		end,
	ar_intervals:outerjoin(
		ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2).

%%%===================================================================
%%% Scan pass lifecycle.
%%%===================================================================

prereqs_ok(StoreID) ->
	case ar_node:is_joined() of
		false -> false;
		true ->
			case ar_kv:get(ar_data_sync:migration_db(StoreID),
					?FOOTPRINT_MIGRATION_CURSOR_KEY) of
				{ok, <<"complete">>} -> true;
				_ -> false
			end
	end.

flip_mode(normal) -> footprint;
flip_mode(footprint) -> normal;
flip_mode(undefined) -> normal.

%% Adaptive restart delay. Scans that produced tasks reset to the minimum;
%% scans that touched peers but produced nothing double the backoff; scans
%% that never saw peers reset to the minimum (data discovery just hadn't
%% warmed yet - not an empty-module signal).
scan_restart_delay(#scan_cursor{ tasks_produced = N }) when N > 0 ->
	{?DISCOVER_RESTART_MIN_DELAY_MS, 0};
scan_restart_delay(#scan_cursor{ had_peers = false }) ->
	{?DISCOVER_RESTART_MIN_DELAY_MS, 0};
scan_restart_delay(#scan_cursor{ backoff_ms = PrevBackoff }) ->
	NewBackoff = min(
		max(?DISCOVER_RESTART_MIN_DELAY_MS, PrevBackoff * 2),
		?DISCOVER_RESTART_MAX_DELAY_MS),
	{NewBackoff, NewBackoff}.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

cut_peer_footprint_intervals_test() ->
	?assertEqual(
		ar_intervals:from_list([{786432, 524288}, {1310720, 1048576}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{4, 0}]), 262144, 1572864),
		"Full Footprint 0, aligned boundaries"),

	?assertEqual(
		ar_intervals:from_list([{524288,262144}, {1048576,786432}, {1572864,1310720}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{8, 4}]), 262144, 1572864),
		"Full Footprint 1 cut to aligned boundaries"),

	?assertEqual(
		ar_intervals:from_list([
			{262144,200000}, {786432, 524288}, {1310720, 1048576}, {1600000,1572864}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{4, 0}]), 200000, 1600000),
		"Full Footprint 0, unaligned boundaries, pre-strict"),

	?assertEqual(
		ar_intervals:from_list([{524288,262144}, {1048576, 786432}, {1572864, 1310720}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{8, 4}]), 200000, 1600000),
		"Full Footprint 1, unaligned boundaries, pre-strict"),

	?assertEqual(
		ar_intervals:from_list([{2883584,2621440}, {3407872,3145728}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{12, 8}]), 2400000, 3500000),
		"Full Footprint 2, unaligned boundaries, post-strict"),

	?assertEqual(
		ar_intervals:from_list([{2621440,2359296}, {3145728,2883584}, {3500000,3407872}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{16, 12}]), 2400000, 3500000),
		"Full Footprint 3, unaligned boundaries, post-strict"),

	?assertEqual(
		ar_intervals:from_list([{2621440,2359296}, {3500000,3407872}]),
		cut_peer_footprint_intervals(
			ar_intervals:from_list([{16, 14}, {13, 12}]), 2400000, 3500000),
		"Partial Footprint 3, unaligned boundaries, post-strict"),

	ok.

get_next_fetch_offset_test() ->
	SectorSize = ar_block:get_replica_2_9_entropy_sector_size(),
	{P0Start, P0End} = ar_replica_2_9:get_entropy_partition_range(0),
	Chunk = ?DATA_CHUNK_SIZE,

	?assertEqual(P0Start + Chunk,
		get_next_fetch_offset(P0Start, P0Start, P0End),
		"simple advance"),

	?assertEqual(P0Start + 1000,
		get_next_fetch_offset(P0Start, P0Start, P0Start + 1000),
		"simple advance, limited by End"),

	?assertEqual(P0End,
		get_next_fetch_offset(P0Start + SectorSize - 1, P0Start, P0End),
		"jump to PartitionEnd"),

	?assertEqual(P0Start + SectorSize,
		get_next_fetch_offset(P0Start + SectorSize - 1, P0Start, P0Start + SectorSize),
		"jump to PartitionEnd, limited by End"),

	ok.

-endif.
