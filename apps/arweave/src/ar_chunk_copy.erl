%%% @doc Cross-module chunk-copy subsystem.
%%%
%%% Two responsibilities, one gen_server:
%%%
%%%  1. **Producer** (per-StoreID copy loop): scan for unsynced byte ranges
%%%     that already exist on this node's disk under another storage module's
%%%     ID, and enqueue cross-module copy tasks. 
%%%
%%%  2. **Worker pool** (per-StoreID): receive `read_range' tasks and
%%%     dispatch them to `ar_chunk_copy_worker' instances.
%%%
-module(ar_chunk_copy).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, task_completed/3, start_copy/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(READ_RANGE_CHUNKS, 400).
-define(MAX_ACTIVE_TASKS, 10).
-define(MAX_QUEUED_TASKS, 50).
-define(SYNC_RECORD_READY_TIMEOUT_MS, 900).

-record(worker_tasks, {
	worker,
	task_queue = queue:new(),
	active_count = 0
}).

%% Producer-side state for one storage module's copy operation.
-record(copy_state, {
	store_id,
	range_start,
	range_end,
	%% Intervals discovered in another storage module that should be copied
	%% into this module. Element shape: {OtherStoreID, {Start, End}}.
	pending_intervals = [],
	%% Other storage modules still to scan for shared intervals.
	pending_modules = [],
	%% Mirror of ar_device_lock's view of this module's sync-mode lock.
	sync_status = off
}).

-record(state, {
	workers = #{},
	%% In-progress copy operations, one entry per StoreID:
	%% StoreID => #copy_state{}. An entry is created on start_copy/1,
	%% progresses through scan + read_range steps via {copy, StoreID}
	%% casts, and is removed when the operation finishes.
	in_progress = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(WorkerMap) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, WorkerMap, []).

register_workers() ->
	{Workers, WorkerMap} = register_read_workers(),
	ChunkCopy = ?CHILD_WITH_ARGS(ar_chunk_copy, worker, ar_chunk_copy, [WorkerMap]),
	Workers ++ [ChunkCopy].

register_read_workers() ->
	{ok, Config} = arweave_config:get_env(),
	StoreIDs = [
		ar_storage_module:id(StorageModule) || StorageModule <- Config#config.storage_modules
	] ++ [?DEFAULT_MODULE],
	{Workers, WorkerMap} =
		lists:foldl(
			fun(StoreID, {AccWorkers, AccWorkerMap}) ->
				Label = ar_storage_module:label(StoreID),
				Name = list_to_atom("ar_chunk_copy_worker_" ++ Label),
				Worker = ?CHILD_WITH_ARGS(ar_chunk_copy_worker, worker, Name, [Name]),
				{[Worker | AccWorkers], AccWorkerMap#{StoreID => Name}}
			end,
			{[], #{}},
			StoreIDs
		),
	{Workers, WorkerMap}.

%% @doc Returns true if we can accept new tasks. Will always return false if syncing is
%% disabled (i.e. sync_jobs = 0).
ready_for_work(StoreID) ->
	try
		gen_server:call(?MODULE, {ready_for_work, StoreID}, 1000)
	catch
		exit:{timeout,_} ->
			false
	end.

%% @doc Notify ar_chunk_copy that a read_range task has completed.
task_completed(Worker, ReadResult, Args) ->
	gen_server:cast(?MODULE, {task_completed, {read_range, {Worker, ReadResult, Args}}}).

%% @doc Start (or restart) a copy operation for the given storage
%% module. Scans neighboring on-disk modules for unsynced intervals and
%% enqueues cross-module copy tasks. On completion an
%% `{event, chunk_copy, {complete, StoreID}}' message is published via
%% `ar_events'.
start_copy(StoreID) ->
	gen_server:cast(?MODULE, {start_copy, StoreID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(WorkerMap) ->
	?LOG_DEBUG([{event, init}, {module, ?MODULE}, {worker_map, WorkerMap}]),
	Workers = maps:fold(
		fun(StoreID, Name, Acc) ->
			Acc#{StoreID => #worker_tasks{worker = Name}}
		end,
		#{},
		WorkerMap
	),
	ar_util:cast_after(1000, self(), process_queues),
	{ok, #state{
		workers = Workers
	}}.

handle_call({ready_for_work, StoreID}, _From, State) ->
	{reply, do_ready_for_work(StoreID, State), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({read_range, Args}, State) ->
	{noreply, enqueue_read_range(Args, State)};

handle_cast(process_queues, State) ->
	ar_util:cast_after(1000, self(), process_queues),
	{noreply, process_queues(State)};

handle_cast({task_completed, {read_range, {Worker, _, Args}}}, State) ->
	{noreply, task_completed(Args, State)};

handle_cast({start_copy, StoreID}, State) ->
	{noreply, do_start_copy(StoreID, State)};

handle_cast({copy, StoreID}, State) ->
	{noreply, copy(StoreID, State)};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_DEBUG([{event, terminate}, {module, ?MODULE}, {reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions — copy (producer side).
%%%===================================================================

%% @doc Initialize copy state for a storage module and kick off the loop.
%% pending_modules is seeded with the default module first (it holds
%% pre-strict-split data clamped by DiskPoolThreshold) followed by every
%% other on-disk module overlapping this StoreID's range.
do_start_copy(StoreID, State) ->
	InProgress = State#state.in_progress,
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	%% Match ar_data_sync's range adjustment.
	RangeStart2 = max(0, ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
	RangeEnd2 = ar_block:get_chunk_padded_offset(RangeEnd),
	SyncStatus = ar_data_sync:init_sync_status(StoreID),
	OtherStorageModules = [ar_storage_module:id(Module)
		|| Module <- ar_storage_module:get_all(RangeStart2, RangeEnd2),
		ar_storage_module:id(Module) /= StoreID],
	CopyState = #copy_state{
		store_id = StoreID,
		range_start = RangeStart2,
		range_end = RangeEnd2,
		sync_status = SyncStatus,
		pending_modules = [?DEFAULT_MODULE | OtherStorageModules]
	},
	gen_server:cast(?MODULE, {copy, StoreID}),
	State#state{ in_progress = maps:put(StoreID, CopyState, InProgress) }.

copy(StoreID, State) ->
	with_lock(StoreID, State, fun do_copy/2,
		fun(StoreID2) ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, ?MODULE, {copy, StoreID2})
		end).

with_lock(StoreID, State, Active, Retry) ->
	case maps:get(StoreID, State#state.in_progress, undefined) of
		undefined ->
			State;
		#copy_state{} = CopyState ->
			Status = ar_device_lock:acquire_lock(sync, StoreID, CopyState#copy_state.sync_status),
			CopyState2 = CopyState#copy_state{ sync_status = Status },
			case Status of
				active ->
					Active(CopyState2, State);
				paused ->
					Retry(StoreID),
					update_progress(CopyState2, State);
				_ ->
					finish(CopyState2, State)
			end
	end.

%% Dispatcher: route to the right action based on what's left in copy_state.
%% Three distinct operations:
%%   - finish:      both work-lists empty → emit completion event
%%   - scan_module: no pending intervals, pop next module to scan
%%   - read_range:  pending interval, issue the cross-module read
do_copy(#copy_state{
		pending_intervals = [],
		pending_modules = [] } = CopyState, State) ->
	finish(CopyState, State);
do_copy(#copy_state{
		pending_intervals = [],
		pending_modules = [OtherStoreID | OtherStoreIDs] } = CopyState, State) ->
	scan_module(OtherStoreID, OtherStoreIDs, CopyState, State);
do_copy(#copy_state{
		pending_intervals = [{OtherStoreID, Range} | Rest] } = CopyState, State) ->
	read_range(OtherStoreID, Range, Rest, CopyState, State).

%% Scan one source storage module for unsynced intervals belonging to
%% this StoreID. The default module's range is clamped to
%% DiskPoolThreshold because it holds pre-strict-split data that
%% shouldn't be copied past the threshold; permanent modules are
%% scanned across the full range.
scan_module(SourceStoreID, OtherStoreIDs, #copy_state{
		store_id = StoreID,
		range_start = RangeStart,
		range_end = RangeEnd } = CopyState, State) ->
	ScanEnd = case SourceStoreID of
		?DEFAULT_MODULE -> min(RangeEnd, ar_disk_pool:get_threshold());
		_ -> RangeEnd
	end,
	Intervals = determine_intervals_to_copy_from_module(
		StoreID, SourceStoreID, RangeStart, ScanEnd),
	?LOG_INFO([{event, sync_local}, {stage, scan},
		{store_id, StoreID}, {source_store_id, SourceStoreID},
		{range_start, RangeStart}, {range_end, ScanEnd},
		{found_intervals, length(Intervals)}]),
	CopyState2 = CopyState#copy_state{
		pending_intervals = Intervals,
		pending_modules = OtherStoreIDs
	},
	gen_server:cast(?MODULE, {copy, StoreID}),
	update_progress(CopyState2, State).

%% Issue the cross-module read for one pending interval.
read_range(OtherStoreID, {Start, End}, Rest, #copy_state{
		store_id = StoreID } = CopyState, State) ->
	%% Direct private call - we're already inside the gen_server, so the
	%% public ready_for_work/1 (which does gen_server:call self) would
	%% throw `calling_self'.
	CopyState2 = case do_ready_for_work(OtherStoreID, State) of
		true ->
			gen_server:cast(?MODULE,
				{read_range, {Start, End, OtherStoreID, StoreID}}),
			CopyState#copy_state{ pending_intervals = Rest };
		false ->
			CopyState
	end,
	ar_util:cast_after(50, ?MODULE, {copy, StoreID}),
	update_progress(CopyState2, State).

finish(#copy_state{
		store_id = StoreID,
		range_start = RangeStart,
		range_end = RangeEnd } = _CopyState, State) ->
	?LOG_INFO([{event, sync_local}, {stage, complete},
		{store_id, StoreID}, {range_start, RangeStart}, {range_end, RangeEnd},
		{next, network_sync}]),
	ar_events:send(chunk_copy, {complete, StoreID}),
	State#state{ in_progress = maps:remove(StoreID, State#state.in_progress) }.

update_progress(#copy_state{ store_id = StoreID } = CopyState, State) ->
	State#state{ in_progress = maps:put(StoreID, CopyState, State#state.in_progress) }.

%% @doc Find unsynced intervals belonging to StoreID that are already
%% present in OriginStoreID's sync record. Returns a list of
%% {OriginStoreID, {Start, End}} tuples ready for cross-module copy.
determine_intervals_to_copy_from_module(StoreID, OtherStoreID, RangeStart,
		RangeEnd) ->
	determine_intervals_to_copy_from_module(StoreID, OtherStoreID, RangeStart,
			RangeEnd, []).

determine_intervals_to_copy_from_module(_StoreID, _OtherStoreID, RangeStart,
		RangeEnd, Intervals) when RangeStart >= RangeEnd ->
	Intervals;
determine_intervals_to_copy_from_module(StoreID, OtherStoreID, RangeStart,
		RangeEnd, Intervals) ->
	FindNextMissing =
		case ar_sync_record:get_next_synced_interval(RangeStart, RangeEnd, ar_data_sync,
		StoreID) of
			not_found ->
				{request, {RangeStart, RangeEnd}};
			{End, Start} when Start =< RangeStart ->
				{skip, End};
			{_End, Start} ->
				{request, {RangeStart, Start}}
		end,
	case FindNextMissing of
		{skip, End2} ->
			determine_intervals_to_copy_from_module(StoreID, OtherStoreID, End2,
					RangeEnd, Intervals);
		{request, {Cursor, RightBound}} ->
			case ar_sync_record:get_next_synced_interval(Cursor, RightBound, ar_data_sync,
					OtherStoreID) of
				not_found ->
					determine_intervals_to_copy_from_module(StoreID, OtherStoreID,
							RightBound, RangeEnd, Intervals);
				{End2, Start2} ->
					Start3 = max(Start2, Cursor),
					Intervals2 = [{OtherStoreID, {Start3, End2}} | Intervals],
					determine_intervals_to_copy_from_module(StoreID, OtherStoreID,
							End2, RangeEnd, Intervals2)
			end
	end.

%%%===================================================================
%%% Private functions — worker pool.
%%%===================================================================

do_ready_for_work(StoreID, State) ->
	Worker = maps:get(StoreID, State#state.workers, undefined),
	case Worker of
		undefined ->
			?LOG_ERROR([{event, worker_not_found}, {module, ?MODULE}, {call, ready_for_work},
				{store_id, StoreID}]),
			false;
		_ ->
			%% The origin store's ar_sync_record must have finished loading before
			%% we try to read chunks from it. Otherwise ar_chunk_storage:get can return
			%% not_found for a chunk whose metadata is present but whose chunk-storage
			%% sync record has not yet been loaded, causing read_range2 to permanently
			%% invalidate a valid record.
			ar_sync_record:await_initialized(StoreID, ?SYNC_RECORD_READY_TIMEOUT_MS)
				andalso queue:len(Worker#worker_tasks.task_queue) < ?MAX_QUEUED_TASKS
	end.

enqueue_read_range(Args, State) ->
	{_Start, _End, OriginStoreID, _TargetStoreID} = Args,
	Worker = maps:get(OriginStoreID, State#state.workers, undefined),
	case Worker of
		undefined ->
			?LOG_ERROR([{event, worker_not_found}, {module, ?MODULE},
				{call, enqueue_read_range}, {store_id, OriginStoreID}]),
			State;
		_ ->
			Worker2 = do_enqueue_read_range(Args, Worker),
			State#state{
				workers = maps:put(OriginStoreID, Worker2, State#state.workers)
			}
	end.

do_enqueue_read_range(Args, Worker) ->
	{Start, End, OriginStoreID, TargetStoreID} = Args,
	End2 = min(Start + (?READ_RANGE_CHUNKS * ?DATA_CHUNK_SIZE), End),
	Args2 = {Start, End2, OriginStoreID, TargetStoreID},
	TaskQueue = queue:in(Args2, Worker#worker_tasks.task_queue),
	Worker2 = Worker#worker_tasks{task_queue = TaskQueue},
	case End2 == End of
		true ->
			Worker2;
		false ->
			Args3 = {End2, End, OriginStoreID, TargetStoreID},
			do_enqueue_read_range(Args3, Worker2)
	end.

process_queues(State) ->
	Workers = State#state.workers,
	UpdatedWorkers = maps:map(
		fun(_Key, Worker) ->
			process_queue(Worker)
		end,
		Workers
	),
	State#state{workers = UpdatedWorkers}.

process_queue(Worker) ->
	case Worker#worker_tasks.active_count < ?MAX_ACTIVE_TASKS of
		true ->
			case queue:out(Worker#worker_tasks.task_queue) of
				{empty, _} ->
					Worker;
				{{value, Args}, Q2}->
					gen_server:cast(Worker#worker_tasks.worker, {read_range, Args}),
					Worker2 = Worker#worker_tasks{
						task_queue = Q2,
						active_count = Worker#worker_tasks.active_count + 1
					},
					process_queue(Worker2)
			end;
		false ->
			Worker
	end.

task_completed(Args, State) ->
	{_Start, _End, OriginStoreID, _TargetStoreID} = Args,
	Worker = maps:get(OriginStoreID, State#state.workers, undefined),
	case Worker of
		undefined ->
			?LOG_ERROR([{event, worker_not_found}, {module, ?MODULE}, {call, task_completed},
				{store_id, OriginStoreID}]),
			State;
		_ ->
			ActiveCount = Worker#worker_tasks.active_count - 1,
			Worker2 = Worker#worker_tasks{active_count = ActiveCount},
			Worker3 = process_queue(Worker2),
			State2 = State#state{
				workers = maps:put(OriginStoreID, Worker3, State#state.workers)
			},
			State2
	end.

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_ready_for_work/0},
		{timeout, 30, fun test_enqueue_read_range/0},
		{timeout, 30, fun test_process_queue/0},
		{timeout, 30, fun test_register_workers/0}
	].

test_ready_for_work() ->
	ReadySyncRecord = fun Loop() ->
		receive
			{'$gen_call', From, await_initialized} ->
				gen_server:reply(From, initialized),
				Loop()
		end
	end,
	SyncRecords = lists:map(
		fun(StoreID) ->
			Name = ar_sync_record:name(StoreID),
			Pid = spawn_link(ReadySyncRecord),
			true = register(Name, Pid),
			{Name, Pid}
		end,
		[store1, store2]
	),
	State = #state{
		workers = #{
			store1 => #worker_tasks{
				task_queue = queue:from_list(lists:seq(1, ?MAX_QUEUED_TASKS - 1))},
			store2 => #worker_tasks{
				task_queue = queue:from_list(lists:seq(1, ?MAX_QUEUED_TASKS))}
		}
	},
	try
		?assertEqual(true, do_ready_for_work(store1, State)),
		?assertEqual(false, do_ready_for_work(store2, State))
	after
		lists:foreach(
			fun({Name, Pid}) ->
				unregister(Name),
				exit(Pid, normal)
			end,
			SyncRecords
		)
	end.

test_enqueue_read_range() ->
	ExpectedWorker = #worker_tasks{
		task_queue = queue:from_list(
					[{
						floor(2.5 * ?DATA_CHUNK_SIZE),
						floor((2.5 + ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						"store1", "store2"
					},
					{
						floor((2.5 + ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						floor((2.5 + 2 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						"store1", "store2"
					},
					{
						floor((2.5 + 2 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						floor((2.5 + 3 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						"store1", "store2"
					}]
				)
			},
	Worker = do_enqueue_read_range(
		{
			floor(2.5 * ?DATA_CHUNK_SIZE),
			floor((2.5 + 3 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
			"store1", "store2"
		},
		#worker_tasks{task_queue = queue:new()}
	),
	?assertEqual(
		queue:to_list(ExpectedWorker#worker_tasks.task_queue),
		queue:to_list(Worker#worker_tasks.task_queue)).

test_process_queue() ->
	Worker1 = #worker_tasks{
		active_count = ?MAX_ACTIVE_TASKS
	},
	?assertEqual(Worker1, process_queue(Worker1)),

	Worker2 = #worker_tasks{
		active_count = ?MAX_ACTIVE_TASKS + 1
	},
	?assertEqual(Worker2, process_queue(Worker2)),

	Worker3 = process_queue(
		#worker_tasks{
			active_count = ?MAX_ACTIVE_TASKS - 2,
			task_queue = queue:from_list(
				[{floor(2.5 * ?DATA_CHUNK_SIZE), floor(12.5 * ?DATA_CHUNK_SIZE),
				"store1", "store2"},
			{floor(12.5 * ?DATA_CHUNK_SIZE), floor(22.5 * ?DATA_CHUNK_SIZE),
				"store1", "store2"},
			{floor(22.5 * ?DATA_CHUNK_SIZE), floor(30 * ?DATA_CHUNK_SIZE),
				"store1", "store2"}])
		}
	),
	ExpectedWorker3 = #worker_tasks{
		active_count = ?MAX_ACTIVE_TASKS,
		task_queue = queue:from_list(
			[{floor(22.5 * ?DATA_CHUNK_SIZE), floor(30 * ?DATA_CHUNK_SIZE),
				"store1", "store2"}]
		)
	},
	?assertEqual(
		ExpectedWorker3#worker_tasks.active_count, Worker3#worker_tasks.active_count),
	?assertEqual(
		queue:to_list(ExpectedWorker3#worker_tasks.task_queue),
		queue:to_list(Worker3#worker_tasks.task_queue)).

test_register_workers() ->
	{ok, Config} = arweave_config:get_env(),
	StoreIDs = [
		ar_storage_module:id(StorageModule) || StorageModule <- Config#config.storage_modules],
	lists:foreach(
		fun(StoreID) ->
			?assertEqual(true, ready_for_work(StoreID))
		end,
		StoreIDs ++ [?DEFAULT_MODULE]
	).
