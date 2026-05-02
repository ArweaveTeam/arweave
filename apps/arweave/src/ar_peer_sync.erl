%%% @doc Per-storage-module network-sync state machine (gen_server).
%%%
%%% Owns the per-StoreID task queue (ar_sync_task_queue), the in-progress
%%% enqueue pass, the device-lock state, and the latest known weave size.
%%% Self-drives two cast loops:
%%%
%%%  - `enqueue' (producer): walks the module range in normal and
%%%    footprint modes, intersects ar_data_discovery's cached peer offers
%%%    with this module's unsynced gaps (via ar_sync_record), inserts
%%%    chunk-sized tasks into the queue.
%%%
%%%  - `sync' (consumer): pops one task from the queue, dispatches it to
%%%    ar_data_sync_coordinator. Acquires the device lock first.
%%%
%%% No peer HTTP discovery happens here; that latency is isolated in
%%% ar_data_discovery's scanner pool. This module is the bridge between
%%% cached peer coverage and executable sync tasks.
%%%
%%% **Sole owner of the queue's `in_flight_intervals' overlay.** Drop
%%% paths (peer worker queue-full, rebalance cut, reaper, coordinator
%%% worker-unavailable) all funnel through `task_completed/3' to release
%%% byte ranges. The success path uses `task_completed/4' which also
%%% fans out per-peer accounting via ar_peer_worker.
-module(ar_peer_sync).

-behaviour(gen_server).

-export([start_link/1, name/1, register_workers/0]).
-export([enqueue/1, sync/1, task_completed/3, task_completed/4, set_weave_size/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").
-export([cut_peer_footprint_intervals/3]).
-endif.

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
	%% Logged at pass_complete and used to gate the chunk_sync_started
	%% log to fire only on the first productive step.
	tasks_produced = 0 :: non_neg_integer()
}).

-record(state, {
	%% Storage module identifier this state belongs to.
	store_id,
	%% Start offset of the storage module's range.
	range_start = -1 :: integer(),
	%% End offset of the storage module's range.
	range_end = -1 :: integer(),
	%% Latest known weave size (chain tip). Updated via set_weave_size/2.
	weave_size :: undefined | non_neg_integer(),
	%% Mirror of ar_device_lock's view of this module's sync-mode lock.
	sync_status = undefined,
	%% Per-module task queue (ar_sync_task_queue library; record
	%% mutated only by handlers in this gen_server).
	queue = ar_sync_task_queue:new(),
	%% In-progress enqueue pass. `undefined' means the loop hasn't
	%% started its first pass yet.
	pass = undefined :: undefined | #enqueue_pass{}
}).

-define(GET_SYNC_RECORD_RPM_KEY, data_sync_record).
-define(GET_FOOTPRINT_RECORD_RPM_KEY, footprints).
-define(GET_SYNC_RECORD_PATH, [<<"data_sync_record">>]).
-define(GET_FOOTPRINT_RECORD_PATH, [<<"footprints">>]).
-define(FOOTPRINT_MIGRATION_CURSOR_KEY, <<"footprint_migration_cursor">>).

%% Max queued chunks per sync worker.
-define(SYNC_TASK_QUEUE_CHUNKS_PER_WORKER, 250).

%% Fixed delay between passes. Producer no longer issues HTTP (that
%% lives in ar_data_discovery's per-peer scanner pool, which has its
%% own pacing), so this only prevents tight-loop log spam and CPU spin
%% on fully-synced modules.
-ifdef(AR_TEST).
-define(PASS_RESTART_DELAY_MS, 1_000).
-else.
-define(PASS_RESTART_DELAY_MS, 10_000).
-endif.

%%%===================================================================
%%% Supervisor wiring.
%%%===================================================================

name(?DEFAULT_MODULE) ->
	ar_peer_sync_default;
name(StoreID) ->
	list_to_atom("ar_peer_sync_" ++ ar_storage_module:label(StoreID)).

register_workers() ->
	{ok, Config} = arweave_config:get_env(),
	StoreIDs = [
		ar_storage_module:id(SM) || SM <- Config#config.storage_modules
	] ++ [?DEFAULT_MODULE],
	[?CHILD_WITH_ARGS(?MODULE, worker, name(SID), [SID]) || SID <- StoreIDs].

start_link(StoreID) ->
	gen_server:start_link({local, name(StoreID)}, ?MODULE, StoreID, []).

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Kick off the producer loop for the StoreID. Invoked by
%% ar_data_sync after the chunk_copy phase completes.
enqueue(StoreID) ->
	gen_server:cast(name(StoreID), enqueue).

%% @doc Kick off the consumer loop for the StoreID. Self-perpetuating
%% once started.
sync(StoreID) ->
	gen_server:cast(name(StoreID), sync).

%% @doc Drop path - release a byte range from the dedup overlay without
%% touching per-peer accounting. Used by ar_peer_worker:release_dropped_task/1
%% and any other "task left the pipeline without completing" site.
task_completed(StoreID, Start, End) ->
	gen_server:cast(name(StoreID), {task_completed, Start, End}).

%% @doc Success path - fans out per-peer completion accounting (via
%% ar_peer_worker:task_completed/6) and the byte-range release. Called by
%% ar_data_sync_worker on definitive success or non-recast failure.
task_completed(SyncTask, WorkerPid, Result, ElapsedUs) ->
	#sync_task{ start_offset = Start, end_offset = End, peer = Peer,
			store_id = StoreID, footprint_key = FootprintKey } = SyncTask,
	ar_peer_worker:task_completed(Peer, WorkerPid, FootprintKey, Result,
		ElapsedUs, End - Start),
	task_completed(StoreID, Start, End).

%% @doc Update the weave-size snapshot. Called by ar_data_sync on chain-tip
%% moves so the enqueue loop's range clamp follows the tip.
set_weave_size(StoreID, WeaveSize) ->
	gen_server:cast(name(StoreID), {set_weave_size, WeaveSize}).

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init(StoreID) ->
	{RangeStart, RangeEnd} = init_range(StoreID),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {store_id, StoreID},
		{range_start, RangeStart}, {range_end, RangeEnd}]),
	{ok, #state{
		store_id = StoreID,
		range_start = RangeStart,
		range_end = RangeEnd,
		sync_status = ar_data_sync:init_sync_status(StoreID)
	}}.

init_range(?DEFAULT_MODULE) ->
	{-1, -1};
init_range(StoreID) ->
	case (catch ar_storage_module:get_range(StoreID)) of
		{'EXIT', _} -> {-1, -1};
		{RangeStart, RangeEnd} ->
			{max(0, ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
				ar_block:get_chunk_padded_offset(RangeEnd)}
	end.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, {error, unhandled}, State}.

%% Producer step.
handle_cast(enqueue, #state{ pass = undefined } = State) ->
	case init_pass(State, normal) of
		{ok, Pass} ->
			gen_server:cast(self(), enqueue),
			{noreply, State#state{ pass = Pass }};
		not_ready ->
			%% Node not joined yet, or footprint migration in flight.
			ar_util:cast_after(1000, self(), enqueue),
			{noreply, State}
	end;
handle_cast(enqueue, #state{ store_id = StoreID,
		pass = #enqueue_pass{ offset = O, end_ = E, mode = Mode,
			tasks_produced = TasksProduced } } = State)
		when O >= E ->
	NextMode = flip_mode(Mode),
	?LOG_INFO([{event, sync_network}, {stage, pass_complete},
		{store_id, StoreID}, {mode, Mode}, {tasks_produced, TasksProduced},
		{next_mode, NextMode}]),
	case init_pass(State, NextMode) of
		{ok, Pass2} ->
			ar_util:cast_after(?PASS_RESTART_DELAY_MS, self(), enqueue),
			{noreply, State#state{ pass = Pass2 }};
		not_ready ->
			ar_util:cast_after(1000, self(), enqueue),
			{noreply, State}
	end;
handle_cast(enqueue, State) ->
	case can_enqueue(State) of
		{wait, _Reason, Delay} ->
			ar_util:cast_after(Delay, self(), enqueue),
			{noreply, State};
		ready ->
			{Action, NewState} = do_enqueue(State),
			case Action of
				cast_now -> gen_server:cast(self(), enqueue);
				{cast_after, Ms} -> ar_util:cast_after(Ms, self(), enqueue)
			end,
			{noreply, NewState}
	end;

%% Consumer step.
handle_cast(sync, State) ->
	#state{ store_id = StoreID } = State,
	Status = ar_device_lock:acquire_lock(sync, StoreID, State#state.sync_status),
	State2 = State#state{ sync_status = Status },
	case Status of
		active ->
			{noreply, do_sync(State2)};
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), sync),
			{noreply, State2};
		_ ->
			{noreply, State2}
	end;

%% Drop-path byte-range release (called from any drop site).
handle_cast({task_completed, Start, End}, State) ->
	NewQ = ar_sync_task_queue:task_completed(End, Start, State#state.queue),
	{noreply, State#state{ queue = NewQ }};

%% Chain-tip update from ar_data_sync.
handle_cast({set_weave_size, WeaveSize}, State) ->
	{noreply, State#state{ weave_size = WeaveSize }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE},
		{reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Step implementation (operate on #state{}).
%%%===================================================================

do_sync(#state{ store_id = StoreID, queue = Q } = State) ->
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
			State#state{ queue = Q2 }
	end.

do_enqueue(#state{ pass = #enqueue_pass{ mode = normal } } = State) ->
	do_enqueue_normal(State);
do_enqueue(#state{ pass = #enqueue_pass{ mode = footprint } } = State) ->
	do_enqueue_footprint(State).

do_enqueue_normal(State) ->
	#state{ store_id = StoreID, weave_size = WeaveSize, queue = Q,
			pass = #enqueue_pass{ offset = Offset, end_ = End } = Pass } = State,
	End2 = min(min(Offset + ?QUERY_RANGE_STEP_SIZE, End), WeaveSize),
	UnsyncedIntervals = get_unsynced_intervals(Offset, End2, StoreID),
	case ar_intervals:is_empty(UnsyncedIntervals) of
		true ->
			NewPass = Pass#enqueue_pass{ offset = End2 },
			{cast_now, State#state{ pass = NewPass }};
		false ->
			case get_hot_peers(Offset, normal) of
				wait ->
					{{cast_after, 1000}, State};
				Peers ->
					{PeerCoverageEnd, FetchableEntries} =
						determine_fetchable_intervals_normal(
							Offset, Peers, UnsyncedIntervals),
					{NewQ, Produced} = add_to_queue(FetchableEntries, Q),
					maybe_log_chunk_sync_started(StoreID, normal, Pass, Produced),
					%% If peers don't advertise data past Offset for this window,
					%% skip the whole window to avoid spinning at the same cursor.
					NewOffset =
						case PeerCoverageEnd > Offset of
							true -> min(End2, PeerCoverageEnd);
							false -> End2
						end,
					NewPass = Pass#enqueue_pass{
						offset = NewOffset,
						tasks_produced = Pass#enqueue_pass.tasks_produced
								+ max(0, Produced)
					},
					{cast_now, State#state{ queue = NewQ, pass = NewPass }}
			end
	end.

do_enqueue_footprint(State) ->
	#state{ store_id = StoreID, queue = Q,
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
			{cast_now, State#state{ pass = NewPass }};
		false ->
			case get_hot_peers(Offset, footprint) of
				wait ->
					{{cast_after, 1000}, State};
				Peers ->
					FetchableEntries = determine_fetchable_intervals_footprint(
							Partition, Footprint, Start, End, Peers, UnsyncedIntervals),
					{NewQ, Produced} = add_to_queue(FetchableEntries, Q),
					maybe_log_chunk_sync_started(StoreID, footprint, Pass, Produced),
					Offset2 = ar_replica_2_9:get_next_fetch_offset(Offset, Start, End),
					NewPass = Pass#enqueue_pass{
						offset = Offset2,
						tasks_produced = Pass#enqueue_pass.tasks_produced
								+ max(0, Produced)
					},
					{cast_now, State#state{ queue = NewQ, pass = NewPass }}
			end
	end.

%% Log once per pass.
maybe_log_chunk_sync_started(StoreID, Mode,
		#enqueue_pass{ tasks_produced = 0 }, Produced) when Produced > 0 ->
	?LOG_INFO([{event, sync_network}, {stage, chunk_sync_started},
		{store_id, StoreID}, {mode, Mode}, {tasks_enqueued, Produced}]);
maybe_log_chunk_sync_started(_StoreID, _Mode, _Pass, _Produced) ->
	ok.

can_enqueue(#state{ pass = undefined }) ->
	ready;
can_enqueue(#state{ store_id = StoreID, weave_size = WeaveSize, queue = Q,
		pass = #enqueue_pass{ offset = Offset, end_ = End } }) ->
	case ar_data_sync:is_disk_space_sufficient(StoreID) of
		false ->
			{wait, disk_full, 30_000};
		not_initialized ->
			{wait, disk_info_missing, 1_000};
		true ->
			StoreIDLabel = ar_storage_module:label(StoreID),
			QSize = ar_sync_task_queue:size(Q),
			{NormalCount, FootprintCount} = ar_sync_task_queue:size_by_mode(Q),
			prometheus_gauge:set(sync_task_queue_size,
					[StoreIDLabel, normal], NormalCount),
			prometheus_gauge:set(sync_task_queue_size,
					[StoreIDLabel, footprint], FootprintCount),
			prometheus_gauge:set(sync_task_queue_inflight_bytes,
					[StoreIDLabel], ar_sync_task_queue:inflight_bytes(Q)),
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

%% Build a new pass in the given mode.
init_pass(#state{ store_id = StoreID, range_start = Start, range_end = End },
		Mode) ->
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
				{start, Start}, {end_, End2}]),
			{ok, #enqueue_pass{
				start = Start, end_ = End2, offset = Start, mode = Mode }}
	end.

%%%===================================================================
%%% Per-peer fetchable-interval computation from the ar_data_discovery cache.
%%%===================================================================

%% @doc For each peer, intersect its cached advertised intervals with
%% our UnsyncedIntervals to determine what we can fetch. Returns
%% {PeerCoverageEnd, FetchableEntries}, where PeerCoverageEnd is the min right
%% bound across peers (used to bound cursor advance) and FetchableEntries
%% is a list of {Peer, FetchableIntervals, FootprintKey} entries.
determine_fetchable_intervals_normal(Left, Peers, UnsyncedIntervals) ->
	lists:foldl(
		fun(Peer, {RightAcc, Acc}) ->
			case ar_data_discovery:get_peer_intervals(Peer, Left, infinity) of
				{ok, PeerIntervals, PeerRight} ->
					FetchableIntervals = ar_intervals:intersection(
						PeerIntervals, UnsyncedIntervals),
					case ar_intervals:is_empty(FetchableIntervals) of
						true -> {min(RightAcc, PeerRight), Acc};
						false ->
							{min(RightAcc, PeerRight),
								[{Peer, FetchableIntervals, none} | Acc]}
					end;
				{error, _} ->
					{RightAcc, Acc}
			end
		end,
		{infinity, []},
		Peers
	).

determine_fetchable_intervals_footprint(
		Partition, Footprint, Start, End, Peers, UnsyncedIntervals) ->
	lists:foldl(
		fun(Peer, Acc) ->
			case ar_data_discovery:get_peer_footprint_intervals(Peer, Partition, Footprint) of
				{ok, PeerIntervals} ->
					FetchableIntervals = ar_intervals:intersection(
						PeerIntervals, UnsyncedIntervals),
					case ar_intervals:is_empty(FetchableIntervals) of
						true -> Acc;
						false ->
							ByteIntervals = cut_peer_footprint_intervals(
								FetchableIntervals, Start, End),
							FootprintKey = {Partition, Footprint, Peer},
							[{Peer, ByteIntervals, FootprintKey} | Acc]
					end;
				{error, _} ->
					Acc
			end
		end,
		[],
		Peers
	).

%% Distribute work fairly across peers in a pass. Returns the updated
%% queue and the number of tasks actually enqueued (delta).
add_to_queue([], Queue) ->
	{Queue, 0};
add_to_queue(PeerEntries, Queue) ->
	TotalChunksToEnqueue = ?DEFAULT_SYNC_BUCKET_SIZE div ?DATA_CHUNK_SIZE,
	NumPeers = length(PeerEntries),
	ScalingFactor = 1.5,
	ChunksPerPeer = trunc(((TotalChunksToEnqueue + NumPeers - 1) div NumPeers) * ScalingFactor),
	PrevSize = ar_sync_task_queue:size(Queue),
	NewQueue = ar_sync_task_queue:insert_batch(
		ar_util:shuffle_list(PeerEntries), ChunksPerPeer, Queue),
	{NewQueue, ar_sync_task_queue:size(NewQueue) - PrevSize}.

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

get_hot_peers_for_bucket(GetAllFun, _RPMKey, Path) ->
	{ok, Config} = arweave_config:get_env(),
	AllPeers =
		case Config#config.sync_from_local_peers_only of
			true -> Config#config.local_peers;
			false -> GetAllFun()
		end,
	HotPeers = [
		Peer || Peer <- AllPeers, not ar_rate_limiter:is_throttled(Peer, Path)
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
