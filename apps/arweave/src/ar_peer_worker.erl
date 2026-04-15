%%% @doc Per-peer process managing sync task queue, dispatch state, and footprints.
%%%
%%% Each peer gets its own worker process to isolate state and prevent
%%% stale-state bugs from interleaved updates. This process manages:
%%%
%%% Peer Queue Management:
%%% - Maintains a queue of tasks ready to be dispatched for this peer
%%% - Tracks dispatched_count (number of tasks currently being processed)
%%% - Maintains max_dispatched limit that controls how many tasks can be
%%%   concurrently dispatched for this peer (adjusted via rebalancing)
%%% - If too many requests are made to a peer, it can be blocked or throttled
%%% - Peer performance varies over time; dispatch limits adapt to avoid getting
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
%%% - Adjusts max_dispatched based on peer performance vs target latency
%%% - Cuts queue if it exceeds the calculated max_queue size
-module(ar_peer_worker).

-behaviour(gen_server).

%% Lifecycle
-export([start_link/1, get_or_start/1, stop/1]).
%% Operations (all take Pid as first arg - coordinator caches Peer->Pid mapping)
-export([enqueue_task/5, task_completed/5, process_queue/2,
         get_max_dispatched/1, rebalance/3, try_activate_footprint/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_peers.hrl").

-define(MIN_MAX_DISPATCHED, 8).
-define(MIN_PEER_QUEUE, 20).
-define(CHECK_LONG_RUNNING_FOOTPRINTS_MS, 60000).  %% Check every 60 seconds
-define(LONG_RUNNING_FOOTPRINT_THRESHOLD_S, 120).
-define(IDLE_SHUTDOWN_THRESHOLD_S, 300).  %% Shutdown after 5 minutes of no tasks
-define(CALL_TIMEOUT_MS, 30000).

%% Phase 2: footprint tracking simplifies to just the outstanding task count
%% per key. The old `waiting` queue is gone — tasks go straight to task_queue
%% and admission to the global footprint cache is decided atomically at
%% dequeue time via the ETS slot counter. `activation_time` is retained for
%% the long-running-footprint diagnostic log.
-record(footprint, {
	active_count = 0,        %% count of outstanding tasks (queued + dispatched)
	activation_time          %% set when this peer claims a global slot for this key
}).

-record(state, {
	peer,
	peer_formatted,                %% cached ar_util:format_peer(Peer) for metrics
	task_queue = queue:new(),
	dispatched_count = 0,
	%% waiting_count retained for backwards-compat metric reporting; always 0
	%% in Phase 2+ since there is no waiting queue.
	waiting_count = 0,
	max_dispatched = ?MIN_MAX_DISPATCHED,
	last_task_time,                %% monotonic time when last task was received
	footprints = #{},              %% FootprintKey => #footprint{}
	active_footprints = sets:new() %% keys for which this peer currently holds a slot
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

%% @doc Lookup a peer worker pid by ETS registry.
lookup(Peer) ->
	case ets:lookup(?MODULE, Peer) of
		[] -> undefined;
		[{_, Pid}] -> {ok, Pid}
	end.

%% @doc Get the pid of an existing peer worker or start a new one.
get_or_start(Peer) ->
	case lookup(Peer) of
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

%% @doc Enqueue a task and process the queue synchronously.
%% Returns {WasActivated, TasksToDispatch} where:
%% - WasActivated: true if a new footprint was just activated, false otherwise
%% - TasksToDispatch: list of tasks ready to dispatch
%% HasCapacity indicates whether the global footprint limit allows activating new footprints.
%% WorkerCount is used to calculate available dispatch slots.
enqueue_task(Pid, FootprintKey, Args, HasCapacity, WorkerCount) ->
	try
		gen_server:call(Pid,
			{enqueue_task, FootprintKey, Args, HasCapacity, WorkerCount},
			?CALL_TIMEOUT_MS)
	catch
		exit:{timeout, _} -> {false, []};
		_:_ -> {false, []}
	end.

%% @doc Try to activate a waiting footprint (called when global capacity becomes available).
%% Returns {Activated, TasksToDispatch}. If a footprint was activated the newly-promoted
%% tasks are also drained from the task_queue so the caller can dispatch them — activation
%% alone does not trigger a dispatch, and without this the tasks would sit in task_queue
%% until the next enqueue_task or task_completed for this peer.
try_activate_footprint(Pid, WorkerCount) ->
	try
		gen_server:call(Pid, {try_activate_footprint, WorkerCount}, ?CALL_TIMEOUT_MS)
	catch
		exit:{timeout, _} -> {false, []};
		_:_ -> {false, []}
	end.

%% @doc Process the queue and return tasks ready for dispatch.
%% Used to drain queued tasks without enqueuing new ones (e.g., after task completion).
process_queue(Pid, WorkerCount) ->
	try
		gen_server:call(Pid, {process_queue, WorkerCount}, ?CALL_TIMEOUT_MS)
	catch
		exit:{timeout, _} -> [];
		_:_ -> []
	end.

%% @doc Notify task completed, update footprint accounting and rate data fetched.
task_completed(Pid, FootprintKey, Result, ElapsedNative, DataSize) ->
	gen_server:cast(Pid, {task_completed, FootprintKey, Result, ElapsedNative, DataSize}).

%% @doc Get max_dispatched for this peer.
get_max_dispatched(Pid) ->
	try
		gen_server:call(Pid, get_max_dispatched, ?CALL_TIMEOUT_MS)
	catch
		exit:{timeout, _} -> {error, timeout};
		_:_ -> {error, error}
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
	%% Notify coordinator of our PID (handles restarts updating stale cached PIDs)
	gen_server:cast(ar_data_sync_coordinator, {peer_worker_started, Peer, self()}),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {peer, PeerFormatted}]),
	State = #state{ peer = Peer, peer_formatted = PeerFormatted,
					last_task_time = erlang:monotonic_time() },
	publish_metrics(State),
	{ok, State}.

%% @doc Phase 0 instrumentation: publish this peer worker's contribution to the
%% global invariants into the sync_metrics ETS table. Call at the end of every
%% state-mutating handler.
%% Phase 1 also publishes max_dispatched so coordinator can read it from ETS
%% instead of a synchronous gen_server:call.
publish_metrics(State) ->
	ar_data_sync_coordinator:sync_metrics_put_peer(
		State#state.peer,
		State#state.dispatched_count,
		queue:len(State#state.task_queue) + State#state.waiting_count,
		sets:size(State#state.active_footprints)),
	ar_data_sync_coordinator:sync_metrics_put_peer_max_dispatched(
		State#state.peer, State#state.max_dispatched),
	State.

handle_call(get_max_dispatched, _From, State) ->
	{reply, State#state.max_dispatched, State};

handle_call(get_state, _From, State) ->
	%% Test-only: keep for tests to access raw state
	{reply, {ok, State}, State};

handle_call({enqueue_task, FootprintKey, Args, HasCapacity, WorkerCount}, _From, State) ->
	State1 = State#state{ last_task_time = erlang:monotonic_time() },
	{WasActivated, State2} = do_enqueue_task(FootprintKey, Args, HasCapacity, State1),
	{TasksToDispatch, State3} = do_process_queue(State2, WorkerCount),
	publish_metrics(State3),
	{reply, {WasActivated, TasksToDispatch}, State3};

handle_call({process_queue, WorkerCount}, _From, State) ->
	{TasksToDispatch, State2} = do_process_queue(State, WorkerCount),
	publish_metrics(State2),
	{reply, TasksToDispatch, State2};

handle_call({try_activate_footprint, WorkerCount}, _From, State) ->
	%% Phase 2: activation is now a dequeue-time concept. There is no
	%% "waiting footprint" to activate — all tasks are in task_queue and
	%% admission happens inside do_process_queue via the atomic slot claim.
	%% We still honor the old call shape so coordinator and tests can call
	%% it during migration; semantically this is just "drain what you can".
	{TasksToDispatch, State2} = do_process_queue(State, WorkerCount),
	publish_metrics(State2),
	Activated = TasksToDispatch =/= [],
	{reply, {Activated, TasksToDispatch}, State2};

handle_call({rebalance, Performance, RebalanceParams}, _From, State) ->
	{QueueScalingFactor, TargetLatency, WorkersStarved} = RebalanceParams,
	#state{ task_queue = Queue, max_dispatched = MaxDispatched,
			dispatched_count = Dispatched, waiting_count = Waiting,
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
	
	%% 2. Update max_dispatched
	FasterThanTarget = Performance#performance.average_latency < TargetLatency,
	TargetMax = case FasterThanTarget orelse WorkersStarved of
		true -> MaxDispatched + 1;
		false -> MaxDispatched - 1
	end,
	MaxTasks = max(Dispatched, Waiting + queue:len(State2#state.task_queue)),
	NewMaxDispatched = ar_util:between(TargetMax, ?MIN_MAX_DISPATCHED, max(MaxTasks, ?MIN_MAX_DISPATCHED)),
	State3 = State2#state{ max_dispatched = NewMaxDispatched },
	
	%% 3. Check if we should shutdown (idle worker)
	NewQueueLen = queue:len(State3#state.task_queue),
	NewWaiting = State3#state.waiting_count,
	NewDispatched = State3#state.dispatched_count,
	IdleSeconds = erlang:convert_time_unit(
		erlang:monotonic_time() - LastTaskTime, native, second),
	ShouldShutdown = (NewDispatched == 0) andalso (NewQueueLen == 0) andalso 
					 (NewWaiting == 0) andalso (IdleSeconds >= ?IDLE_SHUTDOWN_THRESHOLD_S),
	
	%% 4. Log rebalance
	?LOG_DEBUG([{event, rebalance_peer},
		{peer, PeerFormatted},
		{dispatched_count, NewDispatched},
		{queued_count, NewQueueLen},
		{waiting_count, NewWaiting},
		{max_queue_len, MaxQueueLen},
		{faster_than_target, FasterThanTarget},
		{workers_starved, WorkersStarved},
		{max_dispatched, NewMaxDispatched},
		{removed_count, RemovedCount},
		{idle_seconds, IdleSeconds},
		{should_shutdown, ShouldShutdown}]),
	
	Result = case ShouldShutdown of
		true -> {shutdown, RemovedCount};
		false -> {ok, RemovedCount}
	end,
	publish_metrics(State3),
	{reply, Result, State3};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, {error, unhandled}, State}.

handle_cast({task_completed, FootprintKey, Result, ElapsedNative, DataSize}, State) ->
	#state{ dispatched_count = DispatchedCount, max_dispatched = MaxDispatched,
			peer = Peer } = State,
	NewDispatchedCount = max(0, DispatchedCount - 1),
	increment_metrics(completed, State, 1),
	%% Rate the fetched data with ar_peers
	ElapsedMicroseconds = erlang:convert_time_unit(ElapsedNative, native, microsecond),
	ar_peers:rate_fetched_data(Peer, chunk, Result, ElapsedMicroseconds, DataSize, MaxDispatched),
	%% Complete footprint task
	State2 = do_complete_footprint_task(
		FootprintKey, State#state{ dispatched_count = NewDispatchedCount }),
	publish_metrics(State2),
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
	%% Remove this peer's contribution from the sync_metrics mirror so it
	%% doesn't phantom-contribute to the cross-process invariant sum.
	ar_data_sync_coordinator:sync_metrics_delete_peer(State#state.peer),
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

%% @doc Process tasks from the head of the queue, dispatching those whose
%% footprint is already claimed or can claim a global slot atomically.
%%
%% Phase 2 change: footprint admission happens here (dequeue time), not at
%% enqueue. The `HasCapacity` bit is gone. When the task at the head of the
%% queue belongs to a footprint this peer has not claimed, we attempt a
%% single atomic `claim_footprint_slot/0`. On success we mark the footprint
%% active locally and dispatch. On failure we stop — head-of-line blocks the
%% rest of the queue until either the head's footprint activates (next tick)
%% or a global slot opens (the rebalance defensive drain retries).
do_process_queue(State, WorkerCount) ->
	#state{ dispatched_count = Dispatched, max_dispatched = MaxDispatched } = State,
	AvailableSlots = min(WorkerCount, MaxDispatched - Dispatched),
	dispatch_up_to(AvailableSlots, [], State).

dispatch_up_to(0, Acc, State) ->
	finish_dispatch(Acc, State);
dispatch_up_to(N, Acc, #state{ task_queue = Queue } = State) ->
	case queue:out(Queue) of
		{empty, _} ->
			finish_dispatch(Acc, State);
		{{value, Args}, NewQueue} ->
			FootprintKey = element(5, Args),
			case try_admit(FootprintKey, State) of
				{ok, State2} ->
					dispatch_up_to(N - 1, [Args | Acc],
						State2#state{ task_queue = NewQueue });
				no_slot ->
					%% Head-of-line block: leave this task at the head and return
					%% whatever we already accumulated. The rebalance tick or a
					%% task completion in the claiming peer will release a slot
					%% and a future drain will retry.
					finish_dispatch(Acc, State)
			end
	end.

finish_dispatch(AccReversed, State) ->
	Tasks = lists:reverse(AccReversed),
	TaskCount = length(Tasks),
	case TaskCount > 0 of
		true ->
			increment_metrics(dispatched, State, TaskCount),
			increment_metrics(queued_out, State, TaskCount);
		false -> ok
	end,
	NewDispatched = State#state.dispatched_count + TaskCount,
	{Tasks, State#state{ dispatched_count = NewDispatched }}.

%% @doc Admit a task by either using this peer's existing footprint claim or
%% attempting an atomic claim on the global slot counter. Returns {ok, State}
%% on success or no_slot when the global cache is full.
try_admit(none, State) ->
	{ok, State};
try_admit(FootprintKey, State) ->
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
%% admission is no longer decided here; any task with a footprint key simply
%% bumps its active_count and goes into the task_queue. Tasks with no key
%% (classic chunk sync) bypass all footprint bookkeeping.
do_enqueue_task(none, Args, _HasCapacity, State) ->
	NewQueue = queue:in(Args, State#state.task_queue),
	increment_metrics(queued_in, State, 1),
	{false, State#state{ task_queue = NewQueue }};
do_enqueue_task(FootprintKey, Args, _HasCapacity, State) ->
	#state{ footprints = Footprints, task_queue = Queue } = State,
	Footprint = maps:get(FootprintKey, Footprints, #footprint{}),
	Footprint2 = Footprint#footprint{
		active_count = Footprint#footprint.active_count + 1
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

%% @doc Handle completion of a footprint task. Decrements active_count;
%% when it reaches zero and this peer held the global slot, release it.
do_complete_footprint_task(none, State) ->
	State;
do_complete_footprint_task(FootprintKey, State) ->
	#state{ footprints = Footprints, active_footprints = Active,
			peer_formatted = PeerFormatted } = State,
	case Footprints of
		#{ FootprintKey := Footprint } ->
			increment_metrics(deactivate_footprint_task, State, 1),
			NewActiveCount = Footprint#footprint.active_count - 1,
			case NewActiveCount =< 0 of
				true ->
					%% All outstanding tasks for this key are done. Drop the
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
					Footprint2 = Footprint#footprint{ active_count = NewActiveCount },
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
cut_footprint_task_counts([Args | Rest], State) ->
	FootprintKey = element(5, Args),
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
							   active_count => Footprint#footprint.active_count} | Acc];
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

%% @doc Test-only helper to get footprint stats by calling get_state.
get_footprint_stats(Pid) ->
	case gen_server:call(Pid, get_state) of
		{ok, State} ->
			#state{ footprints = Footprints } = State,
			TotalActive = maps:fold(fun(_, Footprint, Acc) -> 
				Acc + Footprint#footprint.active_count 
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
	?assertEqual(undefined, lookup(Peer1)),
	
	Peer2 = {50, 60, 70, 80, 1234},
	TestPid = spawn(fun() -> receive stop -> ok end end),
	ets:insert(?MODULE, {Peer2, TestPid}),
	?assertEqual({ok, TestPid}, lookup(Peer2)),
	
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
			fun test_footprint_deactivation_removes_from_map/1
		]
	}.

setup() ->
	Peer = {1, 2, 3, 4, 1984},
	%% Phase 2: ensure the sync_metrics table exists with available slots so
	%% dequeue-time footprint claims can succeed during these unit tests.
	case ets:info(sync_metrics) of
		undefined ->
			ets:new(sync_metrics, [named_table, public, set]);
		_ -> ok
	end,
	ets:insert(sync_metrics,
		[{footprint_slots_available, 1000}, {footprint_slots_max, 1000}]),
	%% Start peer worker directly (not via supervisor, unnamed for test isolation)
	{ok, Pid} = gen_server:start(?MODULE, [Peer], []),
	{Peer, Pid}.

cleanup({Peer, Pid}) ->
	%% Clean up ETS entry when removing the worker
	ets:delete(?MODULE, Peer),
	gen_server:stop(Pid),
	ok.

test_enqueue_and_process({Peer, Pid}) ->
	fun() ->
		%% Enqueue some tasks (no footprint)
		{false, []} = enqueue_task(Pid, none, {0, 100, Peer, store1, none}, true, 0),
		{false, []} = enqueue_task(Pid, none, {100, 200, Peer, store1, none}, true, 0),
		{false, []} = enqueue_task(Pid, none, {200, 300, Peer, store1, none}, true, 0),

		%% Sync to ensure casts processed
		{ok, _} = gen_server:call(Pid, get_state),

		%% Process queue - should get up to 2 tasks
		Tasks = process_queue(Pid, 2),
		?assertEqual(2, length(Tasks)),

		{false, Tasks2} = enqueue_task(Pid, none, {300, 400, Peer, store1, none}, true, 1),
		?assertEqual(1, length(Tasks2)),

		%% Check state
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(1, queue:len(State#state.task_queue)),
		?assertEqual(3, State#state.dispatched_count)
	end.

test_task_completed({Peer, Pid}) ->
	fun() ->
		%% Enqueue and dispatch a task (no footprint)
		enqueue_task(Pid, none, {0, 100, Peer, store1, none}, true, 0),
		{ok, _} = gen_server:call(Pid, get_state), %% sync
		[_Task] = process_queue(Pid, 1),

		%% Complete the task (FootprintKey = none)
		task_completed(Pid, none, ok, 0, 100),
		
		%% Wait for cast to be processed by doing a sync call
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(0, State#state.dispatched_count)
	end.

test_cut_queue({Peer, Pid}) ->
	fun() ->
		%% Enqueue 25 tasks (more than MIN_PEER_QUEUE = 20)
		lists:foreach(fun(I) ->
			enqueue_task(Pid, none, {I * 100, (I + 1) * 100, Peer, store1, none}, true, 0)
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
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 0),
		
		%% Check footprint stats (get_footprint_stats syncs via call)
		Stats = get_footprint_stats(Pid),
		?assertEqual(1, maps:get(active_footprint_count, Stats)),
		?assertEqual(1, maps:get(total_active_tasks, Stats)),

		%% Complete the task
		task_completed(Pid, FootprintKey, ok, 0, 100),
		
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
		enqueue_task(Pid, FootprintKey1, {0, 100, Peer, store1, FootprintKey1}, true, 0),
		enqueue_task(Pid, FootprintKey2, {100, 200, Peer, store1, FootprintKey2}, true, 0),
		
		Stats = get_footprint_stats(Pid),
		?assertEqual(2, maps:get(active_footprint_count, Stats))
	end.

test_footprint_completion({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue multiple tasks for same footprint (HasCapacity=true)
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 0),
		enqueue_task(Pid, FootprintKey, {100, 200, Peer, store1, FootprintKey}, true, 0),
		enqueue_task(Pid, FootprintKey, {200, 300, Peer, store1, FootprintKey}, true, 0),
		
		Stats1 = get_footprint_stats(Pid),  %% Syncs via call
		?assertEqual(3, maps:get(total_active_tasks, Stats1)),

		%% Complete tasks one by one
		task_completed(Pid, FootprintKey, ok, 0, 100),
		Stats2 = get_footprint_stats(Pid),  %% Wait for cast to process
		?assertEqual(2, maps:get(total_active_tasks, Stats2)),
		?assertEqual(1, maps:get(active_footprint_count, Stats2)),  %% Still active

		task_completed(Pid, FootprintKey, ok, 0, 100),
		task_completed(Pid, FootprintKey, ok, 0, 100),
		
		Stats3 = get_footprint_stats(Pid),  %% Wait for casts to process
		?assertEqual(0, maps:get(total_active_tasks, Stats3)),
		?assertEqual(0, maps:get(active_footprint_count, Stats3))  %% Deactivated
	end.

%% Phase 2: there is no "waiting queue" anymore. Enqueued footprint tasks
%% always land in task_queue regardless of capacity. This test now verifies
%% the new behavior: tasks sit in task_queue until admission succeeds at
%% dequeue time.
test_footprint_waiting_queue({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Exhaust the global slot counter so no admission can succeed.
		ets:insert(sync_metrics, {footprint_slots_available, 0}),
		try
			enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, false, 0),
			enqueue_task(Pid, FootprintKey, {100, 200, Peer, store1, FootprintKey}, false, 0),
			{ok, State} = gen_server:call(Pid, get_state),
			%% Both tasks are in task_queue (no separate waiting queue anymore).
			?assertEqual(2, queue:len(State#state.task_queue)),
			?assertEqual(0, State#state.waiting_count),
			%% Footprint not yet claimed since there are no slots.
			?assertEqual(false, sets:is_element(FootprintKey, State#state.active_footprints)),
			Footprint = maps:get(FootprintKey, State#state.footprints),
			?assertEqual(2, Footprint#footprint.active_count),
			%% process_queue must not drain: head-of-line block on unavailable slot.
			?assertEqual([], process_queue(Pid, 10))
		after
			ets:insert(sync_metrics, {footprint_slots_available, 1000})
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
		ets:insert(sync_metrics, {footprint_slots_available, 0}),
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, false, 0),
		{ok, _} = gen_server:call(Pid, get_state),
		%% No slot: drain returns empty.
		{false, []} = try_activate_footprint(Pid, 10),

		%% Restore slots and drain: task is admitted and dispatched.
		ets:insert(sync_metrics, {footprint_slots_available, 1000}),
		{true, [_]} = try_activate_footprint(Pid, 10),

		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(0, queue:len(State#state.task_queue)),
		?assertEqual(1, State#state.dispatched_count),
		?assertEqual(true, sets:is_element(FootprintKey, State#state.active_footprints)),
		Footprint = maps:get(FootprintKey, State#state.footprints),
		?assertEqual(1, Footprint#footprint.active_count)
	end.

test_footprint_task_cycling({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue one task with HasCapacity=true (activates footprint)
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 0),
		{ok, _} = gen_server:call(Pid, get_state), %% sync
		
		%% Dispatch the task
		[_Task] = process_queue(Pid, 1),
		
		%% Verify the basic behavior that when a footprint completes
		%% and has no waiting tasks, it deactivates.
		
		%% Complete the task
		task_completed(Pid, FootprintKey, ok, 0, 100),
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
		enqueue_task(Pid, FootprintKey1, {0, 100, Peer, store1, FootprintKey1}, true, 0),
		enqueue_task(Pid, FootprintKey2, {100, 200, Peer, store1, FootprintKey2}, true, 0),
		enqueue_task(Pid, FootprintKey3, {200, 300, Peer, store1, FootprintKey3}, true, 0),
		_ = process_queue(Pid, 10),

		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(3, sets:size(State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey1, State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey2, State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey3, State1#state.active_footprints)),

		%% Completing a footprint's only task releases the slot.
		task_completed(Pid, FootprintKey1, ok, 0, 100),
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(2, sets:size(State2#state.active_footprints)),
		?assertEqual(false, sets:is_element(FootprintKey1, State2#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey2, State2#state.active_footprints))
	end.

test_add_task_to_active_footprint({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},

		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 0),
		enqueue_task(Pid, FootprintKey, {100, 200, Peer, store1, FootprintKey}, true, 0),
		enqueue_task(Pid, FootprintKey, {200, 300, Peer, store1, FootprintKey}, true, 0),

		{ok, State} = gen_server:call(Pid, get_state),
		Footprint = maps:get(FootprintKey, State#state.footprints),
		%% All three tasks accumulate against the same footprint's active_count
		%% regardless of whether they've been dispatched yet.
		?assertEqual(3, Footprint#footprint.active_count),
		?assertEqual(3, queue:len(State#state.task_queue)),
		?assertEqual(0, State#state.waiting_count)
	end.

test_footprint_deactivation_removes_from_map({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},

		%% Enqueue then drain so the slot is claimed and the footprint is active.
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 0),
		_ = process_queue(Pid, 1),
		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(true, maps:is_key(FootprintKey, State1#state.footprints)),
		?assertEqual(true, sets:is_element(FootprintKey, State1#state.active_footprints)),

		task_completed(Pid, FootprintKey, ok, 0, 100),
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(false, maps:is_key(FootprintKey, State2#state.footprints)),
		?assertEqual(false, sets:is_element(FootprintKey, State2#state.active_footprints))
	end.

-endif.
