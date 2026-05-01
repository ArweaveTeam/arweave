%% @doc Network chunk-sync subsystem. Mirror of ar_chunk_copy: where
%% ar_chunk_copy fetches chunks from local sibling storage modules,
%% ar_peer_sync fetches them from network peers.
%%
%% This module owns an opaque per-StoreID state (task queue + the
%% in-progress enqueue pass + range bookkeeping). Callers build it via
%% `new/3' at init, thread it through the API below, and update it on
%% chain-tip moves via `set_weave_size/2'. The state record is private;
%% outside this module, treat the term as opaque.
%%
%% An "enqueue pass" is one walk of `[range_start, range_end)' in a
%% given mode (normal or footprint). Each call to `enqueue/1' advances
%% the pass by one step (one ?QUERY_RANGE_STEP_SIZE window for normal
%% mode, one footprint for footprint mode); when the pass reaches its
%% end the mode flips and a new pass starts after an adaptive
%% restart delay.
%%
%%  - `new/3' (constructor): build the initial opaque state.
%%
%%  - `enqueue/1' (producer): match cached peer offers (from
%%    ar_data_discovery) against this module's unsynced gaps (from
%%    ar_sync_record), enqueue per-chunk tasks into the task queue,
%%    advance the current pass. Returns `{State', cast_now}' (run the
%%    next step immediately) or `{State', {cast_after, Ms}}' (pause).
%%    No HTTP happens here - peer query latency lives in
%%    ar_data_discovery's background scanner pool instead.
%%
%%  - `sync/1' (consumer): pop one task from the queue, dispatch it
%%    to ar_data_sync_coordinator (which routes through peer_worker
%%    to a sync_worker that performs the HTTP fetch). Returns updated
%%    State'.
%%
%%  - `task_completed/3': release a finished task's byte range from
%%    the queue's dedup overlay so subsequent passes can re-enqueue
%%    if the chunk remains unsynced.
%%
%%  - `set_weave_size/2': update the weave-size snapshot that the
%%    enqueue loop consults to clamp the pass against the current tip.
-module(ar_peer_sync).

-export([new/3, enqueue/1, sync/1, task_completed/3, set_weave_size/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").

%% One in-progress enqueue pass. All pass state lives in this record;
%% it moves through the enqueue state machine and is replaced when the
%% pass completes and a new one starts.
-record(enqueue_pass, {
	%% Left bound of the pass range. For footprint mode, an inclusive
	%% boundary used when cutting per-peer footprint intervals to the
	%% module.
	start :: non_neg_integer(),
	%% Right bound of the pass range (clamped to WeaveSize /
	%% DiskPoolThreshold when the pass is built).
	end_ :: non_neg_integer(),
	%% Current position inside [start, end_). Advances on each step.
	offset :: non_neg_integer(),
	%% Which protocol we're querying peers with on this pass.
	mode :: normal | footprint,
	%% Count of tasks produced in the current pass. Resets each pass.
	tasks_produced = 0 :: non_neg_integer(),
	%% Whether any peer had data to enqueue this pass. Passes that never
	%% saw peers don't trigger adaptive backoff - they probably just ran
	%% before ar_data_discovery's cache warmed up.
	had_peers = false :: boolean(),
	%% Current unproductive-pass backoff (doubles each pass, capped).
	backoff_ms = 0 :: non_neg_integer()
}).

%% Opaque per-StoreID state. Holds the task queue, the in-progress
%% enqueue pass, and the range bookkeeping the enqueue loop needs.
-record(peer_sync_state, {
	%% Storage module identifier this state belongs to.
	store_id,
	%% Start offset of the storage module's range.
	range_start = -1 :: integer(),
	%% End offset of the storage module's range.
	range_end = -1 :: integer(),
	%% Latest known weave size (chain tip). Updated via set_weave_size/2.
	weave_size :: undefined | non_neg_integer(),
	%% Per-module task queue. Holds {FootprintKey, Start, End, Peer}
	%% entries ordered for dispatch, plus a compact intervals overlay for
	%% O(log n) dedup. See ar_sync_task_queue for the invariant.
	queue = ar_sync_task_queue:new(),
	%% In-progress enqueue pass. `undefined' means the loop hasn't
	%% started its first pass yet.
	pass = undefined :: undefined | #enqueue_pass{}
}).

-include_lib("arweave_config/include/arweave_config.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([cut_peer_footprint_intervals/3]).
-endif.

-define(GET_SYNC_RECORD_RPM_KEY, data_sync_record).
-define(GET_FOOTPRINT_RECORD_RPM_KEY, footprints).
-define(GET_SYNC_RECORD_PATH, [<<"data_sync_record">>]).
-define(GET_FOOTPRINT_RECORD_PATH, [<<"footprints">>]).
-define(FOOTPRINT_MIGRATION_CURSOR_KEY, <<"footprint_migration_cursor">>).

%% Max queued chunks per sync worker.
-define(SYNC_TASK_QUEUE_CHUNKS_PER_WORKER, 250).

%% Adaptive restart delay between passes. Productive passes restart at
%% the min delay; unproductive passes double (capped at max). Resets on
%% any productive pass.
-ifdef(AR_TEST).
-define(PASS_RESTART_MIN_DELAY_MS, 1_000).
-define(PASS_RESTART_MAX_DELAY_MS, 1_000).
-else.
-define(PASS_RESTART_MIN_DELAY_MS, 10_000).
-define(PASS_RESTART_MAX_DELAY_MS, 120_000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Build the initial opaque state for a storage module.
new(StoreID, RangeStart, RangeEnd) ->
	#peer_sync_state{ store_id = StoreID, range_start = RangeStart,
		range_end = RangeEnd }.

%% @doc Update the weave-size snapshot. Called by ar_data_sync on
%% chain-tip moves so the enqueue loop's range clamp follows the tip.
set_weave_size(WeaveSize, #peer_sync_state{} = State) ->
	State#peer_sync_state{ weave_size = WeaveSize }.

enqueue(#peer_sync_state{ pass = undefined } = State) ->
	case init_pass(State, normal) of
		{ok, Pass} ->
			{State#peer_sync_state{ pass = Pass }, cast_now};
		not_ready ->
			%% Node not joined yet, or footprint migration in flight.
			{State, {cast_after, 1000}}
	end;
enqueue(#peer_sync_state{ store_id = StoreID,
		pass = #enqueue_pass{ offset = O, end_ = E, mode = Mode,
			tasks_produced = TasksProduced, had_peers = HadPeers } = Pass } = State)
		when O >= E ->
	{Delay, Backoff2} = pass_restart_delay(Pass),
	NextMode = flip_mode(Mode),
	?LOG_INFO([{event, sync_network}, {stage, pass_complete},
		{store_id, StoreID}, {mode, Mode}, {tasks_produced, TasksProduced},
		{had_peers, HadPeers}, {next_mode, NextMode}, {restart_delay_ms, Delay}]),
	case init_pass(State, NextMode, Backoff2) of
		{ok, Pass2} ->
			{State#peer_sync_state{ pass = Pass2 }, {cast_after, Delay}};
		not_ready ->
			{State, {cast_after, 1000}}
	end;
enqueue(State) ->
	case step_ready(State) of
		{wait, _Reason, Delay} ->
			{State, {cast_after, Delay}};
		ready ->
			do_enqueue(State)
	end.

%% @doc Consumer side: pop one task from the queue and dispatch it to
%% ar_data_sync_coordinator. Self-perpetuating - on success schedules
%% the next sync immediately; on backpressure or empty queue schedules a
%% retry via cast_after.
sync(#peer_sync_state{ queue = Q, store_id = StoreID } = State) ->
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
			State#peer_sync_state{ queue = Q2 }
	end.

%% @doc Release a finished task's byte range from the queue's dedup
%% overlay so subsequent passes can re-enqueue if the chunk remains
%% unsynced. Called from ar_data_sync's mailbox when ar_data_sync_worker
%% reports a sync_range definitively completed (success or non-recast
%% failure).
task_completed(Start, End, #peer_sync_state{ queue = Q } = State) ->
	State#peer_sync_state{ queue = ar_sync_task_queue:task_completed(End, Start, Q) }.

%% @doc Build a new pass in the given mode.
init_pass(State, Mode) ->
	init_pass(State, Mode, 0).

init_pass(#peer_sync_state{ store_id = StoreID, range_start = Start, range_end = End },
		Mode, Backoff) ->
	case ready_to_start(StoreID) of
		false ->
			not_ready;
		true ->
			End2 = case Mode of
				footprint ->
					min(End, ar_disk_pool:get_threshold());
				normal ->
					End
			end,
			?LOG_INFO([{event, sync_network}, {stage, pass_started},
				{store_id, StoreID}, {mode, Mode},
				{start, Start}, {end_, End2}, {backoff_ms, Backoff}]),
			{ok, #enqueue_pass{
				start = Start, end_ = End2, offset = Start, mode = Mode,
				backoff_ms = Backoff }}
	end.

%% @doc Can the current pass take another step right now? Consolidates
%% every "no, wait" case: disk space, queue fullness, position bounds.
step_ready(#peer_sync_state{ pass = undefined }) ->
	ready;
step_ready(#peer_sync_state{ queue = Q, store_id = StoreID, weave_size = WeaveSize,
		pass = #enqueue_pass{ offset = Offset, end_ = End } }) ->
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
				ar_data_sync_coordinator:sync_jobs() * ?SYNC_TASK_QUEUE_CHUNKS_PER_WORKER),
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

do_enqueue(#peer_sync_state{ pass = #enqueue_pass{ mode = normal } } = State) ->
	do_enqueue_normal(State);
do_enqueue(#peer_sync_state{ pass = #enqueue_pass{ mode = footprint } } = State) ->
	do_enqueue_footprint(State).

do_enqueue_normal(State) ->
	#peer_sync_state{ queue = Q, store_id = StoreID, weave_size = WeaveSize,
			pass = #enqueue_pass{ offset = Offset, end_ = End } = Pass } = State,
	End2 = min(min(Offset + ?QUERY_RANGE_STEP_SIZE, End), WeaveSize),
	UnsyncedIntervals = get_unsynced_intervals(Offset, End2, StoreID),
	case ar_intervals:is_empty(UnsyncedIntervals) of
		true ->
			%% No unsynced bytes in this window; advance.
			NewPass = Pass#enqueue_pass{ offset = End2 },
			{State#peer_sync_state{ pass = NewPass }, cast_now};
		false ->
			case get_hot_peers(Offset, normal) of
				wait ->
					%% All known peers for this bucket are on cooldown; hold
					%% the offset and retry.
					{State, {cast_after, 1000}};
				Peers ->
				{EndReached, EnqueueIntervals, HadPeers} =
					collect_peer_intervals_normal(Offset, Peers, UnsyncedIntervals),
				NewQ = add_to_queue(EnqueueIntervals, Q),
				Produced = ar_sync_task_queue:size(NewQ) - ar_sync_task_queue:size(Q),
				maybe_log_chunk_sync_started(StoreID, normal, Pass, Produced),
				NewPass = Pass#enqueue_pass{
					offset = min(End2, EndReached),
					had_peers = Pass#enqueue_pass.had_peers orelse HadPeers,
					tasks_produced = Pass#enqueue_pass.tasks_produced + max(0, Produced)
				},
				{State#peer_sync_state{ queue = NewQ, pass = NewPass }, cast_now}
		end
	end.

do_enqueue_footprint(State) ->
	#peer_sync_state{ queue = Q, store_id = StoreID,
			pass = #enqueue_pass{ start = Start, end_ = End, offset = Offset }
					= Pass } = State,
	Partition = ar_replica_2_9:get_entropy_partition(Offset + ?DATA_CHUNK_SIZE),
	Footprint = ar_footprint_record:get_footprint(Offset + ?DATA_CHUNK_SIZE),
	UnsyncedIntervals =
		ar_footprint_record:get_unsynced_intervals(Partition, Footprint, StoreID),
	case ar_intervals:is_empty(UnsyncedIntervals) of
		true ->
			Offset2 = ar_replica_2_9:get_next_fetch_offset(Offset, Start, End),
			NewPass = Pass#enqueue_pass{ offset = Offset2 },
			{State#peer_sync_state{ pass = NewPass }, cast_now};
		false ->
			case get_hot_peers(Offset, footprint) of
				wait ->
					{State, {cast_after, 1000}};
				Peers ->
				{EnqueueIntervals, HadPeers} = collect_peer_intervals_footprint(
						Partition, Footprint, Start, End, Peers, UnsyncedIntervals),
				NewQ = add_to_queue(EnqueueIntervals, Q),
				Produced = ar_sync_task_queue:size(NewQ) - ar_sync_task_queue:size(Q),
				maybe_log_chunk_sync_started(StoreID, footprint, Pass, Produced),
				Offset2 = ar_replica_2_9:get_next_fetch_offset(Offset, Start, End),
				NewPass = Pass#enqueue_pass{
					offset = Offset2,
					had_peers = Pass#enqueue_pass.had_peers orelse HadPeers,
					tasks_produced = Pass#enqueue_pass.tasks_produced + max(0, Produced)
				},
				{State#peer_sync_state{ queue = NewQ, pass = NewPass }, cast_now}
		end
	end.

%% Log once per pass.
maybe_log_chunk_sync_started(StoreID, Mode,
		#enqueue_pass{ tasks_produced = 0 }, Produced) when Produced > 0 ->
	?LOG_INFO([{event, sync_network}, {stage, chunk_sync_started},
		{store_id, StoreID}, {mode, Mode}, {tasks_enqueued, Produced}]);
maybe_log_chunk_sync_started(_StoreID, _Mode, _Pass, _Produced) ->
	ok.

%%%===================================================================
%%% Per-peer gather from the ar_data_discovery cache.
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

%% Distribute work fairly across peers in a pass: shuffle so no one
%% peer dominates, cap each peer at ~1.5x fair share of the bucket.
add_to_queue([], Queue) ->
	Queue;
add_to_queue(PeerEntries, Queue) ->
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
%%% Pass lifecycle.
%%%===================================================================

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

ready_to_start(StoreID) ->
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

%% Adaptive restart delay. Passes that produced tasks reset to the
%% minimum; passes that touched peers but produced nothing double the
%% backoff; passes that never saw peers reset to the minimum (data
%% discovery just hadn't warmed yet - not an empty-module signal).
pass_restart_delay(#enqueue_pass{ tasks_produced = N }) when N > 0 ->
	{?PASS_RESTART_MIN_DELAY_MS, 0};
pass_restart_delay(#enqueue_pass{ had_peers = false }) ->
	{?PASS_RESTART_MIN_DELAY_MS, 0};
pass_restart_delay(#enqueue_pass{ backoff_ms = PrevBackoff }) ->
	NewBackoff = min(
		max(?PASS_RESTART_MIN_DELAY_MS, PrevBackoff * 2),
		?PASS_RESTART_MAX_DELAY_MS),
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

-endif.
