%%% @doc Cross-module chunk-copy subsystem.
%%%
%%% A single registered gen_server runs the per-StoreID producer loop and
%%% tracks in-flight transient `ar_chunk_copy_worker' processes. At most one
%%% worker runs per source module at a time (preserving disk-seek
%%% serialization); the producer carves each pending interval into
%%% `?READ_RANGE_CHUNKS'-sized sub-tasks, spawning the next only when the
%%% source frees up.
%%%
%%% A copy publishes `{chunk_copy, {complete, StoreID}}' via `ar_events' only
%%% once scanning is done AND no worker is still running for the target, so
%%% subscribers can read the event as "every chunk I asked for has been read".
-module(ar_chunk_copy).
-test_category([fast]).

-behaviour(gen_server).

-export([start_link/0, start_copy/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(READ_RANGE_CHUNKS, 400).

%% Per-StoreID producer state.
-record(copy_state, {
	range_start,
	range_end,
	%% Intervals discovered for the current source module, as
	%% `{SourceStoreID, {Start, End}}'. Consumed head-first by `step/3'.
	pending_intervals = [],
	%% Source storage modules still to scan.
	pending_modules = [],
	%% Mirror of `ar_device_lock''s sync-mode lock, carried across
	%% iterations so re-acquires can notice state transitions.
	sync_status = off,
	%% Source module this target is parked on in `step/3' (head interval
	%% blocked by `is_source_busy/2'), or `undefined'. Lets
	%% `on_worker_down/3' wake only the targets waiting on the freed source.
	waiting_on = undefined
}).

-record(state, {
	%% TargetStoreID => #copy_state{} for in-flight copy operations.
	in_progress = #{},
	%% Monitor Ref => {SourceStoreID, {Start, End, Source, Target}} for
	%% running workers; on `'DOWN'' this says which source freed up and
	%% which task it was.
	monitors = #{}
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
%%% Producer.
%%%===================================================================

%% @doc Initialise per-StoreID producer state and kick the step machine.
%% `pending_modules' leads with the default module (holding pre-strict-split
%% data clamped by `DiskPoolThreshold'), then every other on-disk module
%% overlapping this StoreID's range.
do_start_copy(StoreID, State) ->
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	%% Match ar_data_sync's range adjustment.
	RangeStart2 = max(0,
		ar_block:get_chunk_padded_offset(RangeStart) - ?DATA_CHUNK_SIZE),
	RangeEnd2 = ar_block:get_chunk_padded_offset(RangeEnd),
	SyncStatus = ar_data_sync:init_sync_status(StoreID),
	OtherStorageModules = [ar_storage_module:id(M)
		|| M <- ar_storage_module:get_all(RangeStart2, RangeEnd2),
		ar_storage_module:id(M) /= StoreID],
	CopyState = #copy_state{
		range_start = RangeStart2,
		range_end = RangeEnd2,
		sync_status = SyncStatus,
		pending_modules = [?DEFAULT_MODULE | OtherStorageModules]
	},
	gen_server:cast(?MODULE, {step, StoreID}),
	save_progress(StoreID, CopyState, State).

maybe_step(StoreID, State) ->
	case maps:find(StoreID, State#state.in_progress) of
		error ->
			State;
		{ok, CopyState} ->
			Status = ar_device_lock:acquire_lock(sync, StoreID,
				CopyState#copy_state.sync_status),
			CopyState2 = CopyState#copy_state{ sync_status = Status },
			case Status of
				active ->
					step(StoreID, CopyState2, State);
				paused ->
					ar_util:cast_after(?DEVICE_LOCK_WAIT, ?MODULE,
						{step, StoreID}),
					save_progress(StoreID, CopyState2, State);
				_ ->
					finish(StoreID, State)
			end
	end.

%% Scanning + dispatching done; finish iff no in-flight worker
%% remains for this target.
step(StoreID,
		#copy_state{ pending_intervals = [], pending_modules = [] } = CopyState,
		State) ->
	case target_has_pending_workers(StoreID, State) of
		true -> save_progress(StoreID, CopyState, State);
		false -> finish(StoreID, State)
	end;
step(StoreID,
		#copy_state{ pending_intervals = [],
			pending_modules = [Source | Rest] } = CopyState,
		State) ->
	CopyState2 = enqueue_intervals_from_source(StoreID, Source, Rest, CopyState),
	gen_server:cast(?MODULE, {step, StoreID}),
	save_progress(StoreID, CopyState2, State);
step(StoreID,
		#copy_state{ pending_intervals = [{Source, {Start, End}} | Rest] } =
			CopyState,
		State) ->
	case is_source_busy(Source, State) of
		true ->
			%% Wait — `on_worker_down/3' wakes us when this source frees up.
			save_progress(StoreID,
				CopyState#copy_state{ waiting_on = Source }, State);
		false ->
			read_chunk_range(StoreID, Source, Start, End, Rest, CopyState,
				State)
	end.

%% Carve one sub-task off the head interval, spawn+monitor a worker, and drop
%% or shrink the head accordingly. Cast `{step, StoreID}' to try the next
%% interval; the source-busy gate stops us piling up on this same source.
read_chunk_range(StoreID, Source, Start, End, Rest, CopyState, State) ->
	Span = ?READ_RANGE_CHUNKS * ?DATA_CHUNK_SIZE,
	ChunkEnd = min(Start + Span, End),
	Args = {Start, ChunkEnd, Source, StoreID},
	{_Pid, Ref} = spawn_monitor(ar_chunk_copy_worker, run, [Args]),
	Intervals2 = case ChunkEnd == End of
		true -> Rest;
		false -> [{Source, {ChunkEnd, End}} | Rest]
	end,
	CopyState2 = CopyState#copy_state{ pending_intervals = Intervals2,
		waiting_on = undefined },
	State2 = State#state{
		monitors = maps:put(Ref, {Source, Args}, State#state.monitors)
	},
	gen_server:cast(?MODULE, {step, StoreID}),
	save_progress(StoreID, CopyState2, State2).

enqueue_intervals_from_source(StoreID, SourceStoreID, OtherStoreIDs,
		#copy_state{ range_start = RangeStart,
			range_end = RangeEnd } = CopyState) ->
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
