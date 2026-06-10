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
%% @ar_test: fast
-module(ar_chunk_copy).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, task_completed/3, start_copy/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
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
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Start (or restart) a cross-module copy for `StoreID': scan neighboring
%% on-disk modules for unsynced intervals and dispatch read-range workers as
%% their source modules free up. Publishes `{chunk_copy, {complete, StoreID}}'
%% via `ar_events' once scanning is done AND every worker has exited. Returns
%% `ignore' when chunk-copy is disabled (sync_jobs = 0).
start_copy(StoreID) ->
	case whereis(?MODULE) of
		undefined ->
			ignore;
		_Pid ->
			gen_server:cast(?MODULE, {start_copy, StoreID}),
			ok
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_DEBUG([{event, init}, {module, ?MODULE}]),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({start_copy, StoreID}, State) ->
	{noreply, do_start_copy(StoreID, State)};

handle_cast({step, StoreID}, State) ->
	{noreply, maybe_step(StoreID, State)};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', Ref, process, _Pid, Reason}, State) ->
	{noreply, on_worker_down(Ref, Reason, State)};

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
	?LOG_DEBUG([{event, sync_local}, {stage, scan},
		{store_id, StoreID}, {source_store_id, SourceStoreID},
		{range_start, RangeStart}, {range_end, ScanEnd},
		{found_intervals, length(Intervals)}]),
	CopyState#copy_state{
		pending_intervals = Intervals,
		pending_modules = OtherStoreIDs
	}.

save_progress(StoreID, CopyState, State) ->
	State#state{
		in_progress = maps:put(StoreID, CopyState, State#state.in_progress)
	}.

finish(StoreID, State) ->
	?LOG_DEBUG([{event, sync_local}, {stage, complete},
		{store_id, StoreID}, {next, network_sync}]),
	ar_events:send(chunk_copy, {complete, StoreID}),
	State#state{
		in_progress = maps:remove(StoreID, State#state.in_progress)
	}.

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
%%% Worker bookkeeping.
%%%===================================================================

is_source_busy(SourceStoreID, State) ->
	lists:any(
		fun({Source, _Args}) -> Source == SourceStoreID end,
		maps:values(State#state.monitors)).

target_has_pending_workers(TargetStoreID, State) ->
	lists:any(
		fun({_Source, {_, _, _, T}}) -> T == TargetStoreID end,
		maps:values(State#state.monitors)).

%% A worker exit frees its source (crashes are logged but still let the copy
%% finish, the Ref having left `monitors'). Wake the finished worker's target and
%% any target parked in `step/3' on the freed source — those have no worker to
%% wake them. The gen_server serialises the casts, so one-worker-per-source holds.
on_worker_down(Ref, Reason, State) ->
	case maps:take(Ref, State#state.monitors) of
		error ->
			State;
		{{FreedSource, Args}, Monitors2} ->
			{_, _, _, TargetStoreID} = Args,
			log_if_crash(Args, Reason),
			maps:foreach(
				fun(T, #copy_state{ waiting_on = W })
						when T =:= TargetStoreID; W =:= FreedSource ->
						gen_server:cast(?MODULE, {step, T});
					(_, _) ->
						ok
				end, State#state.in_progress),
			State#state{ monitors = Monitors2 }
	end.

log_if_crash(_Args, normal) ->
	ok;
log_if_crash(Args, Reason) ->
	?LOG_ERROR([{event, chunk_copy_worker_crash}, {module, ?MODULE},
		{args, Args}, {reason, io_lib:format("~p", [Reason])}]).

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_is_source_busy/0},
		{timeout, 30, fun test_target_has_pending_workers/0}
	].

test_is_source_busy() ->
	Ref = make_ref(),
	State = #state{
		monitors = #{
			Ref => {"source_a", {0, 100, "source_a", "target_x"}}
		}
	},
	?assertEqual(true, is_source_busy("source_a", State)),
	?assertEqual(false, is_source_busy("source_b", State)),
	?assertEqual(false, is_source_busy("source_a", #state{})).

test_target_has_pending_workers() ->
	Ref = make_ref(),
	State = #state{
		monitors = #{
			Ref => {"source_a", {0, 100, "source_a", "target_x"}}
		}
	},
	?assertEqual(true, target_has_pending_workers("target_x", State)),
	?assertEqual(false, target_has_pending_workers("target_y", State)),
	?assertEqual(false, target_has_pending_workers("target_x", #state{})).
