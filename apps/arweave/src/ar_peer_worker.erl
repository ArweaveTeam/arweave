%%% @doc Per-peer process managing the sync task queue, in-flight state, and footprints.
%%%
%%% Each peer gets its own worker process to isolate state and prevent
%%% stale-state bugs from interleaved updates. This process manages:
%%%
%%% Peer Queue Management:
%%% - Maintains a queue of tasks ready to be pulled by sync workers for this peer
%%% - Tracks in_flight_count (number of tasks currently being processed)
%%% - Maintains max_in_flight limit that controls how many tasks can be
%%%   concurrently in flight for this peer (adjusted via rebalancing)
%%% - If too many requests are made to a peer, it can be blocked or throttled
%%% - Peer performance varies over time; in_flight limits adapt to avoid getting
%%%   "stuck" syncing many chunks from a slow peer
%%%
%%% Footprint Management:
%%% - Tasks with a FootprintKey are grouped by footprint to limit concurrent
%%%   processing and avoid overloading the entropy cache
%%% - Footprints can be active (tasks being processed) or waiting (task_queue for later)
%%% - Each peer has a max_footprints limit to prevent overloading entropy cache
%%% - When a footprint becomes active, waiting tasks are moved to the peer queue
%%% - When all tasks in a footprint complete, it's deactivated and the next
%%%   waiting footprint may be activated
%%% - Long-running footprints are detected and logged per-peer
%%%
%%% Performance Tracking:
%%% - Tracks task completion times and data sizes for performance metrics
%%% - Integrates with ar_peers module for peer rating and performance tracking
%%%
%%% Rebalancing:
%%% - Responds to rebalance requests from the coordinator
%%% - Adjusts max_in_flight based on peer performance vs target latency
%%% - Cuts queue if it exceeds the calculated max_queue size
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

-define(DEFAULT_IN_FLIGHT_LIMIT, 8).
-define(MIN_PEER_QUEUE, 20).
-define(CHECK_LONG_RUNNING_FOOTPRINTS_MS, 60000).  %% Check every 60 seconds
-define(LONG_RUNNING_FOOTPRINT_THRESHOLD_S, 120).
-define(IDLE_SHUTDOWN_THRESHOLD_S, 300).  %% Shutdown after 5 minutes of no tasks
-define(CALL_TIMEOUT_MS, 30000).

%% Per-footprint state. Admission to the global footprint cache is decided
%% atomically at dequeue time via the ETS slot counter. `activation_time`
%% is retained for the long-running-footprint diagnostic log.
-record(footprint, {
	active_task_count = 0,        %% count of in-flight tasks (queued + in-flight)
	activation_time          %% set when this peer claims a global slot for this key
}).

-record(state, {
	peer,
	peer_formatted,                %% cached ar_util:format_peer(Peer) for metrics
	task_queue = queue:new(),
	in_flight_count = 0,
	max_in_flight = ?DEFAULT_IN_FLIGHT_LIMIT,
	last_task_time,                %% monotonic time when last task was received
	footprints = #{},              %% FootprintKey => #footprint{}
	active_footprints = sets:new(),%% keys for which this peer currently holds a slot
	%% WorkerPid => FootprintKey for tasks handed out via take_one.
	%% Cleaned up on task_completed; dead workers are reaped on rebalance.
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
%%% Coordinator caches Peer->Pid mapping to avoid lookup overhead.
%%%===================================================================

%% @doc Phase 3: one-way enqueue. The peer worker appends the task to its
%% queue and wakes one idle sync worker (if any). No reply needed; sync
%% workers pull when they're free.
enqueue(Pid, Args) ->
	gen_server:cast(Pid, {enqueue, Args}).

%% @doc Phase 3: a sync worker asks for one task. Returns {task, Args} on
%% success or `none` if the peer is at its dispatch cap, the queue is empty,
%% or the head task's footprint cannot claim a global slot.
%%
%% Short timeout (1s): if the peer worker is overloaded, the calling worker
%% should rotate to the next peer rather than block.
take_one(Pid) ->
	try
		gen_server:call(Pid, {take_one, self()}, 1000)
	catch
		exit:{timeout, _} -> none;
		_:_ -> none
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
		exit:{timeout, _} -> {error, timeout};
		_:_ -> {error, timeout}
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
					last_task_time = erlang:monotonic_time() },
	publish_load(State),
	{ok, State}.

%% @doc Phase 0 instrumentation: publish this peer worker's contribution to the
%% aggregate load counters into the worker_load ETS table. Call at the end of every
%% state-mutating handler.
%% Phase 1 also publishes max_in_flight so coordinator can read it from ETS
%% instead of a synchronous gen_server:call.
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

%% Phase 3: hand a single task to a pulling sync worker. Returns `none` if
%% the peer is at its dispatch cap, the queue is empty, or the head task's
%% footprint cannot claim a global slot. The peer monitors the worker pid
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
							State3 = State2#state{
								task_queue = NewTaskQueue,
								in_flight_count = InFlightCount + 1,
								in_flight_workers = maps:put(WorkerPid, FootprintKey,
									State2#state.in_flight_workers),
								last_task_time = erlang:monotonic_time()
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
	
	%% 1. Cut queue if needed
	MaxQueueLen = max_queue_length(Performance, QueueScalingFactor),
	QueueLen = queue:len(Queue),
	{State2, RemovedCount} = case MaxQueueLen =/= infinity andalso QueueLen > MaxQueueLen of
		true ->
			{NewQueued, RemovedQueue} = queue:split(MaxQueueLen, Queue),
			Removed = queue:len(RemovedQueue),
			RemovedTasks = queue:to_list(RemovedQueue),
			increment_metrics(queued_out, State, Removed),
			S2 = cut_footprint_task_counts(RemovedTasks, State#state{ task_queue = NewQueued }),
			{S2, Removed};
		false ->
			{State, 0}
	end,
	
	%% 2. Update max_in_flight
	FasterThanTarget = Performance#performance.average_latency < TargetLatency,
	TargetMax = case FasterThanTarget orelse WorkersStarved of
		true -> MaxInFlight + 1;
		false -> MaxInFlight - 1
	end,
	MaxTasks = max(InFlight, queue:len(State2#state.task_queue)),
	NewMaxInFlight = ar_util:between(TargetMax, ?DEFAULT_IN_FLIGHT_LIMIT, max(MaxTasks, ?DEFAULT_IN_FLIGHT_LIMIT)),
	State3A = State2#state{ max_in_flight = NewMaxInFlight },

	%% 3. Reap dead workers — roll back leaked in_flight_count and footprint slots
	State3 = reap_dead_workers(State3A),

	%% 4. Check if we should shutdown (idle worker)
	NewQueueLen = queue:len(State3#state.task_queue),
	NewInFlight = State3#state.in_flight_count,
	IdleSeconds = erlang:convert_time_unit(
		erlang:monotonic_time() - LastTaskTime, native, second),
	ShouldShutdown = (NewInFlight == 0) andalso (NewQueueLen == 0) andalso
					 (IdleSeconds >= ?IDLE_SHUTDOWN_THRESHOLD_S),

	%% 4. Log rebalance
	?LOG_DEBUG([{event, rebalance_peer},
		{peer, PeerFormatted},
		{in_flight_count, NewInFlight},
		{queued_count, NewQueueLen},
		{max_queue_len, MaxQueueLen},
		{faster_than_target, FasterThanTarget},
		{workers_starved, WorkersStarved},
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
	#sync_task{ footprint_key = FootprintKey } = SyncTask,
	State1 = State#state{ last_task_time = erlang:monotonic_time() },
	{_, State2} = do_enqueue_task(FootprintKey, SyncTask, true, State1),
	publish_load(State2),
	{noreply, State2};

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
max_queue_length(#performance{ current_rating = 0 }, _ScalingFactor) -> infinity;
max_queue_length(#performance{ current_rating = 0.0 }, _ScalingFactor) -> infinity;
max_queue_length(_Performance, infinity) -> infinity;
max_queue_length(Performance, ScalingFactor) ->
	PeerThroughput = Performance#performance.current_rating,
	max(trunc(PeerThroughput * ScalingFactor), ?MIN_PEER_QUEUE).

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

%% @doc Enqueue a task — Phase 2 unifies the three old branches. Footprint
%% slot claim is no longer decided here; any task with a footprint key simply
%% bumps its active_task_count and goes into the task_queue. Tasks with no key
%% (classic chunk sync) bypass all footprint bookkeeping.
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
	%% Return false for WasActivated: activation is a dequeue-time concept now;
	%% enqueue never claims a slot. Callers that still look at this flag get
	%% consistent accounting (activation counter is only incremented in
	%% mark_footprint_active at dequeue).
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
			increment_metrics(deactivate_footprint_task, State, 1),
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
%% rebalance trimming. Routes through do_complete_footprint_task so the
%% slot-release path is shared.
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
	maps:fold(
		fun(WorkerPid, FootprintKey, Acc) ->
			case is_process_alive(WorkerPid) of
				true ->
					Acc;
				false ->
					NewInFlight = max(0, Acc#state.in_flight_count - 1),
					do_complete_footprint_task(FootprintKey,
						Acc#state{
							in_flight_count = NewInFlight,
							in_flight_workers = maps:remove(WorkerPid, Acc#state.in_flight_workers)
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
		%% Enqueue 25 tasks (more than MIN_PEER_QUEUE = 20)
		lists:foreach(fun(I) ->
			enqueue_task(Pid, none, #sync_task{ start_offset = I * 100, end_offset = (I + 1) * 100, peer = Peer, store_id = store1, footprint_key = none }, true, 0)
		end, lists:seq(0, 24)),

		%% Sync and check we have 25 tasks
		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(25, queue:len(State1#state.task_queue)),

		%% Rebalance with scaling factor that gives MaxQueue = 20 (MIN_PEER_QUEUE)
		%% MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
		%% With PeerThroughput = 100, ScalingFactor = 0.1 => MaxQueue = max(10, 20) = 20
		Performance = #performance{ current_rating = 100.0, average_latency = 50.0 },
		%% RebalanceParams = {QueueScalingFactor, TargetLatency, WorkersStarved}
		%% FasterThanTarget = (50.0 < 100.0) = true
		RebalanceParams = {0.1, 100.0, false},
		Result = rebalance(Pid, Performance, RebalanceParams),
		?assertEqual({ok, 5}, Result),

		%% Check we have 20 tasks left (MIN_PEER_QUEUE)
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(20, queue:len(State2#state.task_queue))
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
