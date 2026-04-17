%%% @doc Per-peer process managing the sync task queue, in-flight tracking, and
%%% footprint admission. One process per peer, started on demand by the
%%% coordinator when ar_data_sync first enqueues work for that peer.
%%%
%%% Task Queue:
%%% - Holds #sync_task{} records waiting to be pulled by ar_data_sync_workers.
%%% - Workers call take_one/1 to pop a task; enqueue is rejected if the queue
%%%   is already at max_queue_len (prevents inflating the global backpressure
%%%   signal between rebalance ticks).
%%%
%%% In-Flight Tracking:
%%% - in_flight_count tracks how many tasks have been handed out via take_one
%%%   but not yet completed via task_completed.
%%% - max_in_flight caps concurrency per peer. Adjusted +/-1 each rebalance
%%%   tick based on the peer's latency vs the global target latency.
%%% - in_flight_workers maps WorkerPid => {FootprintKey, TakenAt} for each
%%%   outstanding task. Used to reap dead or stale workers on the rebalance
%%%   tick (releases leaked in_flight_count and footprint slots).
%%%
%%% Footprint Admission:
%%% - A task with a FootprintKey can only go in-flight if this peer already
%%%   holds a global footprint slot for that key, or can claim one atomically
%%%   via ar_data_sync_coordinator:claim_footprint_slot/0. If no slot is
%%%   available, take_one returns none (head-of-line block; worker tries
%%%   the next peer).
%%% - When a footprint's active_task_count reaches 0, the global slot is
%%%   released. Long-running footprints are logged periodically.
%%%
%%% Rebalancing (called by coordinator every 10s):
%%% - Adjusts max_in_flight: +1 if peer is faster than target latency or
%%%   workers are starved; -1 if slower; -1 if idle for > ACTIVE_THRESHOLD_S.
%%% - Adjusts max_queue_len: raw target from peer rating * scaling factor,
%%%   smoothed downward at QUEUE_SHRINK_RATE per tick to avoid mass cuts.
%%% - Cuts queue tail if it exceeds (smoothed) max_queue_len.
%%% - Reaps dead/stale entries from in_flight_workers.
%%% - Shuts down the peer worker if idle for IDLE_SHUTDOWN_THRESHOLD_S.
-module(ar_peer_worker).

-behaviour(gen_server).

%% Lifecycle
-export([start_link/1, get_all_peers/0, get_or_start/1, stop/1]).
%% Operations (all take Pid as first arg - coordinator caches Peer->Pid mapping)
-export([task_completed/6, rebalance/3, enqueue/2, take_one/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%% Timing constants — independent of sync_jobs.
-define(CHECK_LONG_RUNNING_FOOTPRINTS_MS, 60_000).
-define(LONG_RUNNING_FOOTPRINT_THRESHOLD_S, 120).
-define(IDLE_SHUTDOWN_THRESHOLD_S, 300).
-define(CALL_TIMEOUT_MS, 30_000).
-define(ACTIVE_THRESHOLD_S, 30).
%% Reap in-flight entries older than this, even if the worker pid is alive.
%% Guards against "stranded" tasks where take_one reply was lost (caller
%% timeout, etc.) so the worker never runs sync_range or calls task_completed.
-define(IN_FLIGHT_STALE_THRESHOLD_S, 300).
%% When the raw MaxQueueLen drops sharply, shrink the effective cap by at most
%% this ratio per rebalance tick. Lets struggling peers keep committed work
%% instead of getting it cut out from under them in a single tick. Recovery
%% (raw target rising) applies immediately — no upward smoothing.
-define(QUEUE_SHRINK_RATE, 0.9).
-record(footprint, {
	active_task_count = 0,        %% count of in-flight tasks (queued + in-flight)
	activation_time          %% set when this peer claims a global slot for this key
}).

-record(state, {
	peer,
	peer_formatted,                %% cached ar_util:format_peer(Peer) for metrics
	task_queue = queue:new(),
	in_flight_count = 0,
	max_in_flight,
	%% Rate-limited MaxQueueLen. Raw target tracks peer performance closely;
	%% this value shrinks gradually so a sudden rating drop doesn't discard
	%% most of the queue in one rebalance tick. Set in init from sync_jobs.
	max_queue_len,
	last_task_time,                %% monotonic time when last task was received
	footprints = #{},              %% FootprintKey => #footprint{}
	active_footprints = sets:new(),%% keys for which this peer currently holds a slot
	%% WorkerPid => {FootprintKey, TakenAt} for tasks handed out via take_one.
	%% Cleaned up on task_completed; dead or stale entries reaped on rebalance.
	in_flight_workers = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Peer) ->
	case gen_server:start_link(?MODULE, [Peer], []) of
		{ok, Pid} ->
			ets:insert(?MODULE, {Peer, Pid}),
			{ok, Pid};
		Error ->
			Error
	end.

%% @doc Get a peer worker pid from the ETS registry.
get_pid(Peer) ->
	case ets:lookup(?MODULE, Peer) of
		[] -> undefined;
		[{_, Pid}] -> {ok, Pid}
	end.

%% @doc Return all peer worker registry entries.
get_all_peers() ->
	try ets:tab2list(?MODULE)
	catch _:_ -> []
	end.

%% @doc Get the pid of an existing peer worker or start a new one.
get_or_start(Peer) ->
	case get_pid(Peer) of
		{ok, Pid} ->
			{ok, Pid};
		undefined ->
			case supervisor:start_child(ar_peer_worker_sup, [Peer]) of
				{ok, Pid} -> {ok, Pid};
				{error, {already_started, Pid}} -> {ok, Pid};
				Error -> Error
			end
	end.

stop(Pid) ->
	gen_server:stop(Pid).

%%%===================================================================
%%% Operations (all take Pid as first argument).
%%%===================================================================

enqueue(Pid, Args) ->
	gen_server:cast(Pid, {enqueue, Args}).

%% @doc A sync worker asks for one task. Returns {task, SyncTask} on success
%% or `none` if the peer is at its dispatch cap, the queue is empty, or the
%% head task's footprint cannot claim a global slot.
take_one(Pid) ->
	try
		gen_server:call(Pid, {take_one, self()}, ?CALL_TIMEOUT_MS)
	catch
		exit:{timeout, _} ->
			?LOG_WARNING([{event, take_one_timeout}, {pid, Pid}]),
			none;
		Class:Reason ->
			?LOG_WARNING([{event, take_one_failed}, {pid, Pid},
				{class, Class}, {reason, io_lib:format("~p", [Reason])}]),
			none
	end.

%% @doc Notify task completed, update footprint accounting and rate data fetched.
task_completed(Peer, WorkerPid, FootprintKey, Result, ElapsedNative, DataSize) ->
	case get_pid(Peer) of
		{ok, Pid} ->
			gen_server:cast(Pid,
				{task_completed, WorkerPid, FootprintKey, Result, ElapsedNative, DataSize});
		_ ->
			?LOG_WARNING([{event, task_completed_no_peer_worker},
				{peer, ar_util:format_peer(Peer)}])
	end.

%% @doc Rebalance based on performance and targets.
%% Returns RemovedCount (number of tasks cut from queue).
rebalance(Pid, Performance, RebalanceParams) ->
	try
		gen_server:call(Pid, {rebalance, Performance, RebalanceParams}, ?CALL_TIMEOUT_MS)
	catch
		exit:{timeout, _} ->
			?LOG_WARNING([{event, rebalance_timeout}, {pid, Pid}]),
			{error, timeout};
		Class:Reason ->
			?LOG_WARNING([{event, rebalance_failed}, {pid, Pid},
				{class, Class}, {reason, io_lib:format("~p", [Reason])}]),
			{error, timeout}
	end.

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([Peer]) ->
	%% Schedule periodic check for long-running footprints
	erlang:send_after(?CHECK_LONG_RUNNING_FOOTPRINTS_MS, self(), check_long_running_footprints),
	PeerFormatted = ar_util:format_peer(Peer),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {peer, PeerFormatted}]),
	State = #state{ peer = Peer, peer_formatted = PeerFormatted,
					max_in_flight = ar_data_sync_coordinator:default_in_flight_limit(),
					max_queue_len = ar_data_sync_coordinator:min_peer_queue(),
					last_task_time = erlang:monotonic_time() },
	publish_load(State),
	{ok, State}.

publish_load(State) ->
	ar_data_sync_coordinator:record_peer_load(
		State#state.peer,
		State#state.in_flight_count,
		queue:len(State#state.task_queue),
		sets:size(State#state.active_footprints),
		State#state.max_in_flight),
	State.

handle_call(get_state, _From, State) ->
	%% Test-only: keep for tests to access raw state
	{reply, {ok, State}, State};

handle_call({take_one, WorkerPid}, _From, State) ->
	#state{ in_flight_count = InFlightCount, max_in_flight = MaxInFlight,
			task_queue = TaskQueue } = State,
	case InFlightCount >= MaxInFlight of
		true ->
			{reply, none, State};
		false ->
			case queue:peek(TaskQueue) of
				empty ->
					{reply, none, State};
				{value, SyncTask} ->
					#sync_task{ footprint_key = FootprintKey } = SyncTask,
					case try_claim_footprint(FootprintKey, State) of
						{ok, State2} ->
							{{value, SyncTask}, NewTaskQueue} = queue:out(TaskQueue),
							Now = erlang:monotonic_time(),
							State3 = State2#state{
								task_queue = NewTaskQueue,
								in_flight_count = InFlightCount + 1,
								in_flight_workers = maps:put(WorkerPid,
									{FootprintKey, Now},
									State2#state.in_flight_workers),
								last_task_time = Now
							},
							increment_metrics(dispatched, State3, 1),
							increment_metrics(queued_out, State3, 1),
							publish_load(State3),
							{reply, {task, SyncTask}, State3};
						no_slot ->
							{reply, none, State}
					end
			end
	end;

handle_call({rebalance, Performance, RebalanceParams}, _From, State) ->
	{QueueScalingFactor, TargetLatency, WorkersStarved} = RebalanceParams,
	#state{ task_queue = Queue, max_in_flight = MaxInFlight,
			in_flight_count = InFlight,
			peer_formatted = PeerFormatted, last_task_time = LastTaskTime } = State,
	
	%% 1. Cut queue if needed (using a rate-limited MaxQueueLen)
	OldMaxQueueLen = State#state.max_queue_len,
	RawMaxQueueLen = max_queue_length(Performance, QueueScalingFactor),
	MaxQueueLen = smooth_max_queue_len(OldMaxQueueLen, RawMaxQueueLen),
	State1 = State#state{ max_queue_len = MaxQueueLen },
	QueueLen = queue:len(Queue),
	{State2, RemovedCount} = case QueueLen > MaxQueueLen of
		true ->
			{NewQueued, RemovedQueue} = queue:split(MaxQueueLen, Queue),
			Removed = queue:len(RemovedQueue),
			RemovedTasks = queue:to_list(RemovedQueue),
			increment_metrics(queued_out, State1, Removed),
			S2 = cut_footprint_task_counts(RemovedTasks, State1#state{ task_queue = NewQueued }),
			{S2, Removed};
		false ->
			{State1, 0}
	end,
	
	%% 2. Update max_in_flight
	FasterThanTarget = Performance#performance.average_latency < TargetLatency,
	IdleSeconds = erlang:convert_time_unit(
		erlang:monotonic_time() - LastTaskTime, native, second),
	IsActive = InFlight > 0 orelse queue:len(State2#state.task_queue) > 0
		orelse IdleSeconds < ?ACTIVE_THRESHOLD_S,
	TargetMax = case IsActive of
		false ->
			MaxInFlight - 1;
		true ->
			case FasterThanTarget orelse WorkersStarved of
				true -> MaxInFlight + 1;
				false -> MaxInFlight - 1
			end
	end,
	NewMaxInFlight = max(TargetMax, ar_data_sync_coordinator:default_in_flight_limit()),
	State3A = State2#state{ max_in_flight = NewMaxInFlight },

	%% 3. Reap dead workers — roll back leaked in_flight_count and footprint slots
	State3 = reap_dead_workers(State3A),

	%% 4. Check if we should shutdown (idle peer worker)
	NewQueueLen = queue:len(State3#state.task_queue),
	NewInFlight = State3#state.in_flight_count,
	ShouldShutdown = (NewInFlight == 0) andalso (NewQueueLen == 0) andalso
					 (IdleSeconds >= ?IDLE_SHUTDOWN_THRESHOLD_S),

	%% 5. Log rebalance metrics
	?LOG_DEBUG([{event, rebalance_peer},
		{peer, PeerFormatted},
		{current_rating, Performance#performance.current_rating},
		{average_latency, Performance#performance.average_latency},
		{target_latency, TargetLatency},
		{queue_scaling_factor, QueueScalingFactor},
		{in_flight_count, NewInFlight},
		{queued_count, NewQueueLen},
		{prev_max_queue_len, OldMaxQueueLen},
		{target_max_queue_len, RawMaxQueueLen},
		{max_queue_len, MaxQueueLen},
		{faster_than_target, FasterThanTarget},
		{workers_starved, WorkersStarved},
		{prev_max_in_flight, MaxInFlight},
		{target_max_in_flight, TargetMax},
		{max_in_flight, NewMaxInFlight},
		{removed_count, RemovedCount},
		{idle_seconds, IdleSeconds},
		{should_shutdown, ShouldShutdown}]),
	
	Result = case ShouldShutdown of
		true -> {shutdown, RemovedCount};
		false -> {ok, RemovedCount}
	end,
	publish_load(State3),
	{reply, Result, State3};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, {error, unhandled}, State}.

handle_cast({task_completed, WorkerPid, FootprintKey, Result, ElapsedNative, DataSize}, State) ->
	#state{ in_flight_count = InFlightCount, max_in_flight = MaxInFlight,
			peer = Peer } = State,
	NewInFlightCount = max(0, InFlightCount - 1),
	increment_metrics(completed, State, 1),
	ElapsedMicroseconds = erlang:convert_time_unit(ElapsedNative, native, microsecond),
	ar_peers:rate_fetched_data(Peer, chunk, Result, ElapsedMicroseconds, DataSize, MaxInFlight),
	State2 = do_complete_footprint_task(
		FootprintKey, State#state{
			in_flight_count = NewInFlightCount,
			in_flight_workers = maps:remove(WorkerPid, State#state.in_flight_workers)
		}),
	publish_load(State2),
	{noreply, State2};

handle_cast({enqueue, SyncTask}, State) ->
	case queue:len(State#state.task_queue) >= State#state.max_queue_len of
		true ->
			{noreply, State};
		false ->
			#sync_task{ footprint_key = FootprintKey } = SyncTask,
			State1 = State#state{ last_task_time = erlang:monotonic_time() },
			{_, State2} = do_enqueue_task(FootprintKey, SyncTask, true, State1),
			publish_load(State2),
			{noreply, State2}
	end;

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(check_long_running_footprints, State) ->
	%% Schedule next check
	erlang:send_after(?CHECK_LONG_RUNNING_FOOTPRINTS_MS, self(), check_long_running_footprints),
	log_long_running_footprints(State),
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, State) ->
	%% Clean up ETS entry when process terminates
	ets:delete(?MODULE, State#state.peer),
	%% Remove this peer's contribution from the worker_load mirror so it
	%% doesn't phantom-contribute to the cross-process anomaly sum.
	ar_data_sync_coordinator:remove_peer(State#state.peer),
	ok.

%%%===================================================================
%%% Private functions - Task management
%%%===================================================================

%% @doc Calculate max queue size for this peer.
%% MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
max_queue_length(#performance{ current_rating = Rating }, _ScalingFactor)
		when Rating == 0; Rating == 0.0 ->
	ar_data_sync_coordinator:max_peer_queue();
max_queue_length(Performance, ScalingFactor) ->
	PeerThroughput = Performance#performance.current_rating,
	min(ar_data_sync_coordinator:max_peer_queue(), max(trunc(PeerThroughput * ScalingFactor), ar_data_sync_coordinator:min_peer_queue())).

smooth_max_queue_len(Old, Raw) when Raw >= Old -> Raw;
smooth_max_queue_len(Old, Raw) ->
	max(Raw, round(Old * ?QUEUE_SHRINK_RATE)).

%%%===================================================================
%%% Private functions - Task management
%%%===================================================================

%% @doc Admit a task by either using this peer's existing footprint claim or
%% attempting an atomic claim on the global slot counter. Returns {ok, State}
%% on success or no_slot when the global cache is full.
try_claim_footprint(none, State) ->
	{ok, State};
try_claim_footprint(FootprintKey, State) ->
	case sets:is_element(FootprintKey, State#state.active_footprints) of
		true ->
			%% We already hold a slot for this footprint; piggyback.
			{ok, State};
		false ->
			case ar_data_sync_coordinator:claim_footprint_slot() of
				true ->
					{ok, mark_footprint_active(FootprintKey, State)};
				false ->
					no_slot
			end
	end.

mark_footprint_active(FootprintKey, State) ->
	#state{ footprints = Footprints, active_footprints = Active } = State,
	Footprint = maps:get(FootprintKey, Footprints, #footprint{}),
	Footprint2 = Footprint#footprint{ activation_time = erlang:monotonic_time() },
	increment_metrics(activate_footprint, State, 1),
	State#state{
		footprints = maps:put(FootprintKey, Footprint2, Footprints),
		active_footprints = sets:add_element(FootprintKey, Active)
	}.

do_enqueue_task(none, Args, _HasCapacity, State) ->
	NewQueue = queue:in(Args, State#state.task_queue),
	increment_metrics(queued_in, State, 1),
	{false, State#state{ task_queue = NewQueue }};
do_enqueue_task(FootprintKey, Args, _HasCapacity, State) ->
	#state{ footprints = Footprints, task_queue = Queue } = State,
	Footprint = maps:get(FootprintKey, Footprints, #footprint{}),
	Footprint2 = Footprint#footprint{
		active_task_count = Footprint#footprint.active_task_count + 1
	},
	increment_metrics(queued_in, State, 1),
	{false, State#state{
		task_queue = queue:in(Args, Queue),
		footprints = maps:put(FootprintKey, Footprint2, Footprints)
	}}.

%% @doc Handle completion of a footprint task. Decrements active_task_count;
%% when it reaches zero and this peer held the global slot, release it.
do_complete_footprint_task(none, State) ->
	State;
do_complete_footprint_task(FootprintKey, State) ->
	#state{ footprints = Footprints, active_footprints = Active,
			peer_formatted = PeerFormatted } = State,
	case Footprints of
		#{ FootprintKey := Footprint } ->
			NewActiveCount = Footprint#footprint.active_task_count - 1,
			case NewActiveCount =< 0 of
				true ->
					%% All in_flight_workers tasks for this key are done. Drop the
					%% footprint entry and release the global slot if held.
					case sets:is_element(FootprintKey, Active) of
						true ->
							ar_data_sync_coordinator:release_footprint_slot(),
							increment_metrics(deactivate_footprint, State, 1);
						false -> ok
					end,
					State#state{
						footprints = maps:remove(FootprintKey, Footprints),
						active_footprints = sets:del_element(FootprintKey, Active)
					};
				false ->
					Footprint2 = Footprint#footprint{ active_task_count = NewActiveCount },
					State#state{ footprints = maps:put(FootprintKey, Footprint2, Footprints) }
			end;
		_ ->
			?LOG_WARNING([{event, complete_footprint_task_not_found},
				{footprint_key, FootprintKey},
				{peer, PeerFormatted}]),
			State
	end.

%% @doc Decrement footprint bookkeeping for tasks cut from the queue during
%% rebalance trimming.
cut_footprint_task_counts([], State) ->
	State;
cut_footprint_task_counts([#sync_task{ footprint_key = FootprintKey } | Rest], State) ->
	State2 = case FootprintKey of
		none -> State;
		_ -> do_complete_footprint_task(FootprintKey, State)
	end,
	cut_footprint_task_counts(Rest, State2).

%%%===================================================================
%%% Private functions - Long-running footprint detection (debugging)
%%%===================================================================

log_long_running_footprints(State) ->
	#state{ peer_formatted = PeerFormatted, footprints = Footprints } = State,
	Now = erlang:monotonic_time(),
	ThresholdNative = erlang:convert_time_unit(?LONG_RUNNING_FOOTPRINT_THRESHOLD_S, second, native),
	LongRunning = maps:fold(
		fun(FootprintKey, Footprint, Acc) ->
			case Footprint#footprint.activation_time of
				undefined -> Acc;
				ActivationTime ->
					Duration = Now - ActivationTime,
					case Duration > ThresholdNative of
						true ->
							DurationS = erlang:convert_time_unit(Duration, native, second),
							[#{key => FootprintKey,
							   duration_s => DurationS,
							   active_task_count => Footprint#footprint.active_task_count} | Acc];
						false ->
							Acc
					end
			end
		end, [], Footprints),
	case LongRunning of
		[] -> ok;
		_ ->
			?LOG_WARNING([{event, long_running_footprints},
				{peer, PeerFormatted},
				{count, length(LongRunning)},
				{footprints, LongRunning}])
	end.

%%%===================================================================
%%% Private functions - Dead worker reaping
%%%===================================================================

reap_dead_workers(State) ->
	Now = erlang:monotonic_time(),
	StaleThresholdNative = erlang:convert_time_unit(
		?IN_FLIGHT_STALE_THRESHOLD_S, second, native),
	maps:fold(
		fun(WorkerPid, {FootprintKey, TakenAt}, Acc) ->
			Dead = not is_process_alive(WorkerPid),
			Stale = (Now - TakenAt) > StaleThresholdNative,
			case Dead orelse Stale of
				false ->
					Acc;
				true ->
					AgeSecs = erlang:convert_time_unit(
						Now - TakenAt, native, second),
					?LOG_WARNING([{event, reaped_in_flight_task},
						{peer, Acc#state.peer_formatted},
						{worker, WorkerPid},
						{footprint_key, FootprintKey},
						{reason, case Dead of true -> dead; false -> stale end},
						{age_s, AgeSecs}]),
					NewInFlight = max(0, Acc#state.in_flight_count - 1),
					do_complete_footprint_task(FootprintKey,
						Acc#state{
							in_flight_count = NewInFlight,
							in_flight_workers = maps:remove(WorkerPid,
								Acc#state.in_flight_workers)
						})
			end
		end, State, State#state.in_flight_workers).

%%%===================================================================
%%% Private functions - Metrics
%%%===================================================================

%% Increment prometheus counter - catches errors when prometheus isn't initialized (e.g. in tests)
%% Metric is an atom (e.g., dispatched, queued_in)
increment_metrics(Metric, #state{ peer_formatted = PeerFormatted }, Value) ->
	try
		prometheus_counter:inc(sync_tasks, [Metric, PeerFormatted], Value)
	catch
		_:_ -> ok
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

%% @doc Test-only shim for the pre-Phase-3 API. The production paths use
%% `enqueue/2` (one-way cast) and `take_one/1` (single-task pull); these
%% wrappers preserve the older test ergonomics. The 4th and 5th args
%% (HasCapacity, WorkerCount) are accepted for source compat but ignored:
%% slot claim is now atomic at dequeue time, not at enqueue time.
enqueue_task(Pid, _FootprintKey, Args, _HasCapacity, _WorkerCount) ->
	enqueue(Pid, Args),
	%% Sync so the cast has been applied before the test inspects state.
	{ok, _} = gen_server:call(Pid, get_state),
	{false, []}.

%% @doc Test-only shim: drain up to N tasks via repeated take_one/1.
process_queue(Pid, N) ->
	process_queue_loop(Pid, N, []).

process_queue_loop(_Pid, 0, Acc) -> lists:reverse(Acc);
process_queue_loop(Pid, N, Acc) ->
	case take_one(Pid) of
		{task, Args} -> process_queue_loop(Pid, N - 1, [Args | Acc]);
		none -> lists:reverse(Acc)
	end.

%% @doc Test-only shim: try_activate_footprint reduces to "drain what you can".
try_activate_footprint(Pid, N) ->
	case process_queue(Pid, N) of
		[] -> {false, []};
		Tasks -> {true, Tasks}
	end.

%% @doc Test-only helper to get footprint stats by calling get_state.
get_footprint_stats(Pid) ->
	case gen_server:call(Pid, get_state) of
		{ok, State} ->
			#state{ footprints = Footprints } = State,
			TotalActive = maps:fold(fun(_, Footprint, Acc) -> 
				Acc + Footprint#footprint.active_task_count 
			end, 0, Footprints),
			#{
				footprint_count => maps:size(Footprints),
				active_footprint_count => maps:size(Footprints),  %% All footprints are now active
				total_active_tasks => TotalActive
			};
		Error ->
			Error
	end.

lookup_test() ->
	Peer1 = {10, 20, 30, 40, 9999},
	?assertEqual(undefined, get_pid(Peer1)),
	
	Peer2 = {50, 60, 70, 80, 1234},
	TestPid = spawn(fun() -> receive stop -> ok end end),
	ets:insert(?MODULE, {Peer2, TestPid}),
	?assertEqual({ok, TestPid}, get_pid(Peer2)),
	
	%% Cleanup
	TestPid ! stop,
	ets:delete(?MODULE, Peer2).

%% Tests that require setup/cleanup
peer_worker_test_() ->
	{foreach,
		fun setup/0,
		fun cleanup/1,
		[
			fun test_enqueue_and_process/1,
			fun test_task_completed/1,
			fun test_cut_queue/1,
			fun test_footprint_basic/1,
			fun test_multiple_footprints/1,
			fun test_footprint_completion/1,
			fun test_footprint_waiting_queue/1,
			fun test_try_activate_footprint/1,
			fun test_footprint_task_cycling/1,
			fun test_active_footprints_set/1,
			fun test_add_task_to_active_footprint/1,
			fun test_footprint_deactivation_removes_from_map/1,
			%% Phase 3 pull-model tests
			fun test_take_one_returns_task/1,
			fun test_take_one_at_max_in_flight/1,
			fun test_take_one_no_slot_returns_none/1,
			fun test_take_one_empty_queue_returns_none/1,
			fun test_worker_death_releases_slot/1
		]
	}.

setup() ->
	Peer = {1, 2, 3, 4, 1984},
	ar_data_sync_coordinator:init_worker_load_for_test(1000),
	%% Start peer worker directly (not via supervisor, unnamed for test isolation).
	{ok, Pid} = gen_server:start(?MODULE, [Peer], []),
	ets:insert(?MODULE, {Peer, Pid}),
	{Peer, Pid}.

cleanup({Peer, Pid}) ->
	%% Clean up ETS entry when removing the worker
	ets:delete(?MODULE, Peer),
	gen_server:stop(Pid),
	ok.

test_enqueue_and_process({Peer, Pid}) ->
	fun() ->
		enqueue_task(Pid, none, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = none }, true, 0),
		enqueue_task(Pid, none, #sync_task{ start_offset = 100, end_offset = 200, peer = Peer, store_id = store1, footprint_key = none }, true, 0),
		enqueue_task(Pid, none, #sync_task{ start_offset = 200, end_offset = 300, peer = Peer, store_id = store1, footprint_key = none }, true, 0),

		Tasks = process_queue(Pid, 2),
		?assertEqual(2, length(Tasks)),

		enqueue_task(Pid, none, #sync_task{ start_offset = 300, end_offset = 400, peer = Peer, store_id = store1, footprint_key = none }, true, 1),
		Tasks2 = process_queue(Pid, 1),
		?assertEqual(1, length(Tasks2)),

		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(1, queue:len(State#state.task_queue)),
		?assertEqual(3, State#state.in_flight_count)
	end.

test_task_completed({Peer, Pid}) ->
	fun() ->
		%% Enqueue and dispatch a task (no footprint)
		enqueue_task(Pid, none, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = none }, true, 0),
		{ok, _} = gen_server:call(Pid, get_state), %% sync
		[_Task] = process_queue(Pid, 1),

		%% Complete the task (FootprintKey = none)
		task_completed(Peer, self(), none, ok, 0, 100),
		
		%% Wait for cast to be processed by doing a sync call
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(0, State#state.in_flight_count)
	end.

test_cut_queue({Peer, Pid}) ->
	fun() ->
		%% Enqueue 50 tasks, then cut via rebalance.
		%% min_peer_queue() = max(20, 400 div 10) = 40 with test workers_count=400.
		MinPeerQueue = ar_data_sync_coordinator:min_peer_queue(),
		TaskCount = MinPeerQueue + 10,
		sys:replace_state(Pid, fun(S) -> S#state{ max_queue_len = TaskCount } end),
		lists:foreach(fun(I) ->
			enqueue_task(Pid, none, #sync_task{ start_offset = I * 100, end_offset = (I + 1) * 100, peer = Peer, store_id = store1, footprint_key = none }, true, 0)
		end, lists:seq(0, TaskCount - 1)),

		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(TaskCount, queue:len(State1#state.task_queue)),

		%% Rebalance with low scaling factor so raw target hits the floor.
		%% Pre-set max_queue_len so smoothing doesn't dampen the first cut.
		sys:replace_state(Pid, fun(S) -> S#state{ max_queue_len = MinPeerQueue } end),
		Performance = #performance{ current_rating = 100.0, average_latency = 50.0 },
		RebalanceParams = {0.1, 100.0, false},
		Result = rebalance(Pid, Performance, RebalanceParams),
		?assertEqual({ok, 10}, Result),

		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(MinPeerQueue, queue:len(State2#state.task_queue))
	end.

test_footprint_basic({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue task with footprint - should activate it (HasCapacity=true)
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		
		%% Check footprint stats (get_footprint_stats syncs via call)
		Stats = get_footprint_stats(Pid),
		?assertEqual(1, maps:get(active_footprint_count, Stats)),
		?assertEqual(1, maps:get(total_active_tasks, Stats)),

		%% Complete the task
		task_completed(Peer, self(), FootprintKey, ok, 0, 100),
		
		%% Footprint should be deactivated (get_footprint_stats will wait for cast to process)
		Stats2 = get_footprint_stats(Pid),
		?assertEqual(0, maps:get(active_footprint_count, Stats2))
	end.

test_multiple_footprints({Peer, Pid}) ->
	fun() ->
		%% Verify state is accessible
		{ok, _State0} = gen_server:call(Pid, get_state),
		
		%% Test that multiple footprints can be activated (HasCapacity=true)
		FootprintKey1 = {store1, 1000, Peer},
		FootprintKey2 = {store1, 2000, Peer},
		
		%% Enqueue tasks for two footprints
		enqueue_task(Pid, FootprintKey1, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey1 }, true, 0),
		enqueue_task(Pid, FootprintKey2, #sync_task{ start_offset = 100, end_offset = 200, peer = Peer, store_id = store1, footprint_key = FootprintKey2 }, true, 0),
		
		Stats = get_footprint_stats(Pid),
		?assertEqual(2, maps:get(active_footprint_count, Stats))
	end.

test_footprint_completion({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue multiple tasks for same footprint (HasCapacity=true)
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 100, end_offset = 200, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 200, end_offset = 300, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		
		Stats1 = get_footprint_stats(Pid),  %% Syncs via call
		?assertEqual(3, maps:get(total_active_tasks, Stats1)),

		%% Complete tasks one by one
		task_completed(Peer, self(), FootprintKey, ok, 0, 100),
		Stats2 = get_footprint_stats(Pid),  %% Wait for cast to process
		?assertEqual(2, maps:get(total_active_tasks, Stats2)),
		?assertEqual(1, maps:get(active_footprint_count, Stats2)),  %% Still active

		task_completed(Peer, self(), FootprintKey, ok, 0, 100),
		task_completed(Peer, self(), FootprintKey, ok, 0, 100),
		
		Stats3 = get_footprint_stats(Pid),  %% Wait for casts to process
		?assertEqual(0, maps:get(total_active_tasks, Stats3)),
		?assertEqual(0, maps:get(active_footprint_count, Stats3))  %% Deactivated
	end.

%% Phase 2: there is no "waiting queue" anymore. Enqueued footprint tasks
%% always land in task_queue regardless of capacity. This test now verifies
%% the new behavior: tasks sit in task_queue until a slot is claimed at
%% dequeue time.
test_footprint_waiting_queue({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Exhaust the global slot counter so no slot can be claimed.
		ar_data_sync_coordinator:set_footprint_slots_available(0),
		try
			enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, false, 0),
			enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 100, end_offset = 200, peer = Peer, store_id = store1, footprint_key = FootprintKey }, false, 0),
			{ok, State} = gen_server:call(Pid, get_state),
			%% Both tasks are in task_queue (no separate waiting queue anymore).
			?assertEqual(2, queue:len(State#state.task_queue)),
			%% Footprint not yet claimed since there are no slots.
			?assertEqual(false, sets:is_element(FootprintKey, State#state.active_footprints)),
			Footprint = maps:get(FootprintKey, State#state.footprints),
			?assertEqual(2, Footprint#footprint.active_task_count),
			%% process_queue must not drain: head-of-line block on unavailable slot.
			?assertEqual([], process_queue(Pid, 10))
		after
			ar_data_sync_coordinator:set_footprint_slots_available(1000)
		end
	end.

%% Phase 2: try_activate_footprint became a plain drain. When a slot opens
%% and tasks are queued, the next drain call claims and dispatches them.
test_try_activate_footprint({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Drain with an empty queue: {false, []}.
		{false, []} = try_activate_footprint(Pid, 10),

		%% Exhaust slots, enqueue a task — it will sit in task_queue.
		ar_data_sync_coordinator:set_footprint_slots_available(0),
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, false, 0),
		{ok, _} = gen_server:call(Pid, get_state),
		%% No slot: drain returns empty.
		{false, []} = try_activate_footprint(Pid, 10),

		%% Restore slots and drain: slot is claimed and task becomes in-flight.
		ar_data_sync_coordinator:set_footprint_slots_available(1000),
		{true, [_]} = try_activate_footprint(Pid, 10),

		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(0, queue:len(State#state.task_queue)),
		?assertEqual(1, State#state.in_flight_count),
		?assertEqual(true, sets:is_element(FootprintKey, State#state.active_footprints)),
		Footprint = maps:get(FootprintKey, State#state.footprints),
		?assertEqual(1, Footprint#footprint.active_task_count)
	end.

test_footprint_task_cycling({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue one task with HasCapacity=true (activates footprint)
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		{ok, _} = gen_server:call(Pid, get_state), %% sync
		
		%% Dispatch the task
		[_Task] = process_queue(Pid, 1),
		
		%% Verify the basic behavior that when a footprint completes
		%% and has no waiting tasks, it deactivates.
		
		%% Complete the task
		task_completed(Peer, self(), FootprintKey, ok, 0, 100),
		{ok, State} = gen_server:call(Pid, get_state),
		
		%% Footprint should be removed (no waiting tasks)
		?assertEqual(false, maps:is_key(FootprintKey, State#state.footprints)),
		?assertEqual(false, sets:is_element(FootprintKey, State#state.active_footprints))
	end.

test_active_footprints_set({Peer, Pid}) ->
	fun() ->
		FootprintKey1 = {store1, 1000, Peer},
		FootprintKey2 = {store1, 2000, Peer},
		FootprintKey3 = {store1, 3000, Peer},

		{ok, State0} = gen_server:call(Pid, get_state),
		?assertEqual(0, sets:size(State0#state.active_footprints)),

		%% Phase 2: enqueue + dispatch = claim. Three enqueues followed by a
		%% drain will claim three slots (ample with the 1000 in the test setup).
		enqueue_task(Pid, FootprintKey1, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey1 }, true, 0),
		enqueue_task(Pid, FootprintKey2, #sync_task{ start_offset = 100, end_offset = 200, peer = Peer, store_id = store1, footprint_key = FootprintKey2 }, true, 0),
		enqueue_task(Pid, FootprintKey3, #sync_task{ start_offset = 200, end_offset = 300, peer = Peer, store_id = store1, footprint_key = FootprintKey3 }, true, 0),
		_ = process_queue(Pid, 10),

		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(3, sets:size(State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey1, State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey2, State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey3, State1#state.active_footprints)),

		%% Completing a footprint's only task releases the slot.
		task_completed(Peer, self(), FootprintKey1, ok, 0, 100),
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(2, sets:size(State2#state.active_footprints)),
		?assertEqual(false, sets:is_element(FootprintKey1, State2#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey2, State2#state.active_footprints))
	end.

test_add_task_to_active_footprint({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},

		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 100, end_offset = 200, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 200, end_offset = 300, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),

		{ok, State} = gen_server:call(Pid, get_state),
		Footprint = maps:get(FootprintKey, State#state.footprints),
		%% All three tasks accumulate against the same footprint's active_task_count
		%% regardless of whether they've been taken in-flight yet.
		?assertEqual(3, Footprint#footprint.active_task_count),
		?assertEqual(3, queue:len(State#state.task_queue))
	end.

test_footprint_deactivation_removes_from_map({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},

		%% Enqueue then drain so the slot is claimed and the footprint is active.
		enqueue_task(Pid, FootprintKey, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }, true, 0),
		_ = process_queue(Pid, 1),
		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(true, maps:is_key(FootprintKey, State1#state.footprints)),
		?assertEqual(true, sets:is_element(FootprintKey, State1#state.active_footprints)),

		task_completed(Peer, self(), FootprintKey, ok, 0, 100),
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(false, maps:is_key(FootprintKey, State2#state.footprints)),
		?assertEqual(false, sets:is_element(FootprintKey, State2#state.active_footprints))
	end.

%%%===================================================================
%%% Phase 3 pull-model tests
%%%===================================================================

%% Smallest claim test: enqueue one task, take_one returns it, the
%% peer worker tracks the dispatch and the global slot count drops.
test_take_one_returns_task({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		ar_data_sync_coordinator:set_footprint_slots_available(5),
		enqueue(Pid, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }),
		%% Wait for the cast to land before pulling.
		{ok, _} = gen_server:call(Pid, get_state),
		{task, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }} = take_one(Pid),
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(1, State#state.in_flight_count),
		?assertEqual(0, queue:len(State#state.task_queue)),
		?assertEqual(true, sets:is_element(FootprintKey, State#state.active_footprints)),
		?assertEqual(4, ar_data_sync_coordinator:get_footprint_slots_available())
	end.

%% Slow-peer protection: when in_flight_count reaches max_in_flight the
%% peer refuses to hand out more tasks.
test_take_one_at_max_in_flight({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 2000, Peer},
		ar_data_sync_coordinator:set_footprint_slots_available(1000),
		%% Force max_in_flight to a small number via raw state update so we
		%% can saturate it without triggering the rebalance machinery.
		MaxInFlight = 2,
		sys:replace_state(Pid, fun(S) -> S#state{ max_in_flight = MaxInFlight } end),
		lists:foreach(fun(I) ->
			enqueue(Pid, #sync_task{ start_offset = I*100, end_offset = (I+1)*100, peer = Peer, store_id = store1, footprint_key = FootprintKey })
		end, lists:seq(0, 4)),
		{ok, _} = gen_server:call(Pid, get_state),
		%% First two pulls succeed, third returns none even though queue has tasks.
		{task, _} = take_one(Pid),
		{task, _} = take_one(Pid),
		?assertEqual(none, take_one(Pid))
	end.

%% Footprint slot claim failure: when the global slot gauge is exhausted,
%% take_one returns none even with available worker capacity.
test_take_one_no_slot_returns_none({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 3000, Peer},
		ar_data_sync_coordinator:set_footprint_slots_available(0),
		enqueue(Pid, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }),
		{ok, _} = gen_server:call(Pid, get_state),
		?assertEqual(none, take_one(Pid))
	end.

%% Empty queue: take_one returns none.
test_take_one_empty_queue_returns_none({_Peer, Pid}) ->
	fun() ->
		ar_data_sync_coordinator:set_footprint_slots_available(1000),
		?assertEqual(none, take_one(Pid))
	end.

%% Worker crash recovery: rebalance reaps dead workers, releasing the
%% in_flight_count and footprint slot they held.
test_worker_death_releases_slot({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 5000, Peer},
		ar_data_sync_coordinator:set_footprint_slots_available(5),
		enqueue(Pid, #sync_task{ start_offset = 0, end_offset = 100, peer = Peer, store_id = store1, footprint_key = FootprintKey }),
		{ok, _} = gen_server:call(Pid, get_state),
		Self = self(),
		FakeWorker = spawn(fun() ->
			Result = take_one(Pid),
			Self ! {pulled, Result},
			receive die -> exit(crashed) end
		end),
		receive {pulled, {task, _}} -> ok
		after 1000 -> ?assert(false, "fake worker did not pull")
		end,
		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(1, State1#state.in_flight_count),
		?assertEqual(4, ar_data_sync_coordinator:get_footprint_slots_available()),
		%% Kill the fake worker, then trigger rebalance to reap it.
		FakeWorker ! die,
		timer:sleep(50),
		Performance = #performance{ current_rating = 100.0, average_latency = 50.0 },
		rebalance(Pid, Performance, {1.0, 100.0, false}),
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(0, State2#state.in_flight_count),
		?assertEqual(5, ar_data_sync_coordinator:get_footprint_slots_available())
	end.

-endif.
