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
         get_max_dispatched/1, rebalance/3, try_activate_footprint/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_peers.hrl").

-define(MIN_MAX_DISPATCHED, 8).
-define(MIN_PEER_QUEUE, 20).
-define(CHECK_LONG_RUNNING_FOOTPRINTS_MS, 60000).  %% Check every 60 seconds
-define(LONG_RUNNING_FOOTPRINT_THRESHOLD_S, 120).
-define(IDLE_SHUTDOWN_THRESHOLD_S, 300).  %% Shutdown after 5 minutes of no tasks

-record(footprint, {
	waiting = queue:new(),   %% queue of waiting tasks
	active_count = 0,        %% count of active tasks (0 = inactive)
	activation_time          %% monotonic time when activated (undefined if inactive)
}).

-record(state, {
	peer,
	peer_formatted,                %% cached ar_util:format_peer(Peer) for metrics
	task_queue = queue:new(),
	dispatched_count = 0,
	waiting_count = 0,
	max_dispatched = ?MIN_MAX_DISPATCHED,
	last_task_time,                %% monotonic time when last task was received
	%% Footprint management (coordinator tracks global limits)
	footprints = #{},              %% FootprintKey => #footprint{}
	active_footprints = sets:new() %% set of active FootprintKeys
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
		gen_server:call(Pid, {enqueue_task, FootprintKey, Args, HasCapacity, WorkerCount}, 5000)
	catch
		exit:{timeout, _} -> {false, []};
		_:_ -> {false, []}
	end.

%% @doc Try to activate a waiting footprint (called when global capacity becomes available).
%% Returns true if a footprint was activated, false otherwise.
try_activate_footprint(Pid) ->
	try
		gen_server:call(Pid, try_activate_footprint, 5000)
	catch
		exit:{timeout, _} -> false;
		_:_ -> false
	end.

%% @doc Process the queue and return tasks ready for dispatch.
%% Used to drain queued tasks without enqueuing new ones (e.g., after task completion).
process_queue(Pid, WorkerCount) ->
	try
		gen_server:call(Pid, {process_queue, WorkerCount}, 5000)
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
		gen_server:call(Pid, get_max_dispatched, 5000)
	catch
		exit:{timeout, _} -> {error, timeout};
		_:_ -> {error, error}
	end.

%% @doc Rebalance based on performance and targets.
%% Returns RemovedCount (number of tasks cut from queue).
rebalance(Pid, Performance, RebalanceParams) ->
	try
		gen_server:call(Pid, {rebalance, Performance, RebalanceParams}, 5000)
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
	{ok, #state{ peer = Peer, peer_formatted = PeerFormatted, 
				 last_task_time = erlang:monotonic_time() }}.

handle_call(get_max_dispatched, _From, State) ->
	{reply, State#state.max_dispatched, State};

handle_call(get_state, _From, State) ->
	%% Test-only: keep for tests to access raw state
	{reply, {ok, State}, State};

handle_call({enqueue_task, FootprintKey, Args, HasCapacity, WorkerCount}, _From, State) ->
	State1 = State#state{ last_task_time = erlang:monotonic_time() },
	{WasActivated, State2} = do_enqueue_task(FootprintKey, Args, HasCapacity, State1),
	{TasksToDispatch, State3} = do_process_queue(State2, WorkerCount),
	{reply, {WasActivated, TasksToDispatch}, State3};

handle_call({process_queue, WorkerCount}, _From, State) ->
	{TasksToDispatch, State2} = do_process_queue(State, WorkerCount),
	{reply, TasksToDispatch, State2};

handle_call({try_activate_footprint}, _From, State) ->
	%% Global capacity became available - try to activate a waiting footprint
	{Activated, State2} = try_activate_waiting_footprint(State),
	{reply, Activated, State2};

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
	ok.

%%%===================================================================
%%% Private functions - Task management
%%%===================================================================

dequeue_tasks(Queue, 0, Acc) ->
	{lists:reverse(Acc), Queue};
dequeue_tasks(Queue, N, Acc) ->
	case queue:out(Queue) of
		{empty, _} ->
			{lists:reverse(Acc), Queue};
		{{value, Args}, NewQueued} ->
			dequeue_tasks(NewQueued, N - 1, [Args | Acc])
	end.

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

%% @doc Process tasks from queue based on sync workers.
do_process_queue(State, WorkerCount) ->
	#state{ 
		dispatched_count = Dispatched,
		max_dispatched = MaxDispatched,
		task_queue = Queue } = State,

	AvailableSlots = min(WorkerCount, MaxDispatched - Dispatched),
	{Tasks, RQ} = dequeue_tasks(Queue, AvailableSlots, []),
	TaskCount = length(Tasks),
	case TaskCount > 0 of
		true ->
			increment_metrics(dispatched, State, TaskCount),
			increment_metrics(queued_out, State, TaskCount);
		false ->
			ok
	end,
	NewDispatched = Dispatched + TaskCount,
	State2 = State#state{ task_queue = RQ, dispatched_count = NewDispatched },
	{Tasks, State2}.

%% @doc Enqueue a 'normal' task. Footprint limits can be ignored.
do_enqueue_task(none, Args, _HasCapacity, State) ->
	%% No footprint key - enqueue directly to peer queue
	#state{ task_queue = Queue } = State,
	NewQueue = queue:in(Args, Queue),
	increment_metrics(queued_in, State, 1),
	{false, State#state{ task_queue = NewQueue }};

%% @doc Enqueue a 'footprint' task respecting footprint limits.
do_enqueue_task(FootprintKey, Args, HasCapacity, State) ->
	#state{ footprints = Footprints, task_queue = Queue, 
			active_footprints = ActiveFootprints } = State,
	Footprint = maps:get(FootprintKey, Footprints, #footprint{}),
	IsActive = sets:is_element(FootprintKey, ActiveFootprints),
	case IsActive of
		true ->
			%% Footprint is already active, add task to it (not a new activation)
			NewQueue = queue:in(Args, Queue),
			Footprint2 = Footprint#footprint{ active_count = Footprint#footprint.active_count + 1 },
			increment_metrics(queued_in, State, 1),
			increment_metrics(activate_footprint_task, State, 1),
			{false, State#state{ 
				task_queue = NewQueue,
				footprints = maps:put(FootprintKey, Footprint2, Footprints)
			}};
		false when HasCapacity ->
			%% New footprint and global capacity available - activate it (new activation)
			NewQueue = queue:in(Args, Queue),
			Footprint2 = Footprint#footprint{ active_count = 1 },
			increment_metrics(queued_in, State, 1),
			increment_metrics(activate_footprint_task, State, 1),
			State2 = State#state{ 
				task_queue = NewQueue,
				footprints = maps:put(FootprintKey, Footprint2, Footprints)
			},
			State3 = activate_footprint(FootprintKey, State2),
			{true, State3};
		false ->
			%% No global capacity - queue task for later (no activation)
			Footprint2 = Footprint#footprint{ 
				waiting = queue:in(Args, Footprint#footprint.waiting) 
			},
			increment_metrics(waiting_in, State, 1),
			{false, State#state{ 
				footprints = maps:put(FootprintKey, Footprint2, Footprints),
				waiting_count = State#state.waiting_count + 1
			}}
	end.

%% @doc Handle completion of a footprint task.
do_complete_footprint_task(none, State) ->
	State;
do_complete_footprint_task(FootprintKey, State) ->
	#state{ footprints = Footprints, peer_formatted = PeerFormatted } = State,
	case Footprints of
		#{ FootprintKey := Footprint } ->
			increment_metrics(deactivate_footprint_task, State, 1),
			NewActiveCount = Footprint#footprint.active_count - 1,
			case NewActiveCount =< 0 of
				true ->
					%% Footprint has no more active tasks
					case queue:is_empty(Footprint#footprint.waiting) of
						true ->
							%% No waiting tasks - deactivate footprint
							deactivate_footprint(FootprintKey, Footprint, State);
						false ->
							%% Has waiting tasks - activate them
							activate_waiting_tasks(FootprintKey, Footprint, State)
					end;
				false ->
					%% Still has active tasks
					Footprint2 = Footprint#footprint{ active_count = NewActiveCount },
					State#state{ footprints = maps:put(FootprintKey, Footprint2, Footprints) }
			end;
		_ ->
			?LOG_WARNING([{event, complete_footprint_task_not_found},
				{footprint_key, FootprintKey},
				{peer, PeerFormatted}]),
			State
	end.

%% @doc Deactivate a footprint.
deactivate_footprint(FootprintKey, Footprint, State) ->
	#state{ footprints = Footprints, peer_formatted = PeerFormatted, 
			active_footprints = ActiveFootprints } = State,
	%% Log deactivation with duration
	case Footprint#footprint.activation_time of
		undefined -> ok;
		ActivationTime ->
			DurationMs = erlang:convert_time_unit(
				erlang:monotonic_time() - ActivationTime, native, millisecond),
			?LOG_DEBUG([{event, footprint_deactivated},
				{peer, PeerFormatted},
				{footprint_key, FootprintKey},
				{duration_ms, DurationMs}])
	end,
	increment_metrics(deactivate_footprint, State, 1),
	notify_footprint_deactivated(State#state.peer),
	State#state{ 
		footprints = maps:remove(FootprintKey, Footprints),
		active_footprints = sets:del_element(FootprintKey, ActiveFootprints)
	}.

%% @doc Activate waiting tasks from a footprint that was already active.
%% Called when active_count reaches 0 but footprint has waiting tasks - just cycles tasks.
activate_waiting_tasks(FootprintKey, Footprint, State) ->
	#state{ task_queue = Queue, footprints = Footprints } = State,
	WaitingQueue = Footprint#footprint.waiting,
	WaitingCount = queue:len(WaitingQueue),
	NewQueue = queue:join(Queue, WaitingQueue),
	increment_metrics(waiting_out, State, WaitingCount),
	increment_metrics(queued_in, State, WaitingCount),
	increment_metrics(activate_footprint_task, State, WaitingCount),
	Footprint2 = Footprint#footprint{ 
		waiting = queue:new(), 
		active_count = WaitingCount 
	},
	State#state{ 
		task_queue = NewQueue, 
		footprints = maps:put(FootprintKey, Footprint2, Footprints),
		waiting_count = State#state.waiting_count - WaitingCount
	}.

%% @doc Activate a footprint - common logic for new activations.
%% Sets activation_time, adds to active set, notifies coordinator, logs.
activate_footprint(FootprintKey, State) ->
	#state{ footprints = Footprints,
			active_footprints = ActiveFootprints } = State,
	Footprint = maps:get(FootprintKey, Footprints),
	Footprint2 = Footprint#footprint{ activation_time = erlang:monotonic_time() },
	increment_metrics(activate_footprint, State, 1),
	notify_footprint_activated(State#state.peer),
	State#state{
		footprints = maps:put(FootprintKey, Footprint2, Footprints),
		active_footprints = sets:add_element(FootprintKey, ActiveFootprints)
	}.

%% @doc Try to activate the next waiting footprint if any.
%% Returns {Activated, NewState} where Activated is true if a footprint was activated.
try_activate_waiting_footprint(State) ->
	#state{ footprints = Footprints, active_footprints = ActiveFootprints } = State,
	case find_waiting_footprint(Footprints, ActiveFootprints) of
		none ->
			{false, State};
		{FootprintKey, Footprint} ->
			%% Move waiting tasks to queue, then activate the footprint
			State2 = activate_waiting_tasks(FootprintKey, Footprint, State),
			State3 = activate_footprint(FootprintKey, State2),
			{true, State3}
	end.

%% @doc Find the inactive footprint with the most waiting tasks.
find_waiting_footprint(Footprints, ActiveFootprints) ->
	maps:fold(
		fun(Key, Footprint, Acc) ->
			IsActive = sets:is_element(Key, ActiveFootprints),
			HasWaiting = not queue:is_empty(Footprint#footprint.waiting),
			case not IsActive andalso HasWaiting of
				true ->
					WaitingCount = queue:len(Footprint#footprint.waiting),
					case Acc of
						none ->
							{Key, Footprint};
						{_AccKey, AccFP} ->
							AccWaitingCount = queue:len(AccFP#footprint.waiting),
							case WaitingCount > AccWaitingCount of
								true -> {Key, Footprint};
								false -> Acc
							end
					end;
				false ->
					Acc
			end
		end, none, Footprints).

%% @doc Decrement footprint counts for tasks removed from queue (e.g., during cut).
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
							   active_count => Footprint#footprint.active_count,
							   waiting_count => queue:len(Footprint#footprint.waiting)} | Acc];
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
%%% Private functions - Coordinator notifications
%%%===================================================================

%% Notify coordinator that a footprint was activated (for global tracking)
notify_footprint_activated(Peer) ->
	gen_server:cast(ar_data_sync_coordinator, {footprint_activated, Peer}).

%% Notify coordinator that a footprint was deactivated (for global tracking)
notify_footprint_deactivated(Peer) ->
	gen_server:cast(ar_data_sync_coordinator, {footprint_deactivated, Peer}).

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

%% Standalone tests (no setup/cleanup needed)
standalone_test_() ->
	[
		{timeout, 30, fun test_lookup/0},
		{timeout, 30, fun test_get_or_start/0}
	].

test_lookup() ->
	%% Lookup of non-existent peer returns undefined
	Peer1 = {10, 20, 30, 40, 9999},
	?assertEqual(undefined, lookup(Peer1)),
	
	%% Insert a process and verify lookup finds it
	Peer2 = {50, 60, 70, 80, 1234},
	TestPid = spawn(fun() -> receive stop -> ok end end),
	ets:insert(?MODULE, {Peer2, TestPid}),
	?assertEqual({ok, TestPid}, lookup(Peer2)),
	
	%% Cleanup
	TestPid ! stop,
	ets:delete(?MODULE, Peer2).

test_get_or_start() ->
	%% Start the supervisor (unlink so its shutdown doesn't affect our test process)
	{ok, SupPid} = ar_peer_worker_sup:start_link(),
	unlink(SupPid),
	
	%% First call creates a new worker
	Peer1 = {11, 22, 33, 44, 5555},
	{ok, Pid1} = get_or_start(Peer1),
	?assertEqual(true, is_pid(Pid1)),
	?assertEqual(true, is_process_alive(Pid1)),
	
	%% Second call returns the same pid
	{ok, Pid2} = get_or_start(Peer1),
	?assertEqual(Pid1, Pid2),
	
	%% Different peer gets a different worker
	Peer2 = {55, 66, 77, 88, 6666},
	{ok, Pid3} = get_or_start(Peer2),
	?assertEqual(true, is_pid(Pid3)),
	?assertNotEqual(Pid1, Pid3),
	
	%% Cleanup - stop the supervisor which stops all children
	exit(SupPid, shutdown),
	timer:sleep(10).

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
		enqueue_task(Pid, none, {0, 100, Peer, store1, none}, true, 2),
		enqueue_task(Pid, none, {100, 200, Peer, store1, none}, true, 2),
		enqueue_task(Pid, none, {200, 300, Peer, store1, none}, true, 2),

		%% Sync to ensure casts processed
		{ok, _} = gen_server:call(Pid, get_state),

		%% Process queue - should get up to 2 tasks
		Tasks = process_queue(Pid, 2),
		?assertEqual(2, length(Tasks)),

		%% Check state
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(1, queue:len(State#state.task_queue)),
		?assertEqual(2, State#state.dispatched_count)
	end.

test_task_completed({Peer, Pid}) ->
	fun() ->
		%% Enqueue and dispatch a task (no footprint)
		enqueue_task(Pid, none, {0, 100, Peer, store1, none}, true, 1),
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
			enqueue_task(Pid, none, {I * 100, (I + 1) * 100, Peer, store1, none}, true, 1)
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
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 1),
		
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
		enqueue_task(Pid, FootprintKey1, {0, 100, Peer, store1, FootprintKey1}, true, 1),
		enqueue_task(Pid, FootprintKey2, {100, 200, Peer, store1, FootprintKey2}, true, 1),
		
		Stats = get_footprint_stats(Pid),
		?assertEqual(2, maps:get(active_footprint_count, Stats))
	end.

test_footprint_completion({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue multiple tasks for same footprint (HasCapacity=true)
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 1),
		enqueue_task(Pid, FootprintKey, {100, 200, Peer, store1, FootprintKey}, true, 1),
		enqueue_task(Pid, FootprintKey, {200, 300, Peer, store1, FootprintKey}, true, 1),
		
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

test_footprint_waiting_queue({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue task with HasCapacity=false - should go to waiting queue
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, false, 1),
		enqueue_task(Pid, FootprintKey, {100, 200, Peer, store1, FootprintKey}, false, 1),
		
		%% Sync and check state
		{ok, State} = gen_server:call(Pid, get_state),
		
		%% Task queue should be empty (tasks went to waiting)
		?assertEqual(0, queue:len(State#state.task_queue)),
		%% waiting_count should be 2
		?assertEqual(2, State#state.waiting_count),
		%% Footprint should NOT be in active_footprints set
		?assertEqual(false, sets:is_element(FootprintKey, State#state.active_footprints)),
		%% Footprint should have 2 waiting tasks
		Footprint = maps:get(FootprintKey, State#state.footprints),
		?assertEqual(2, queue:len(Footprint#footprint.waiting)),
		?assertEqual(0, Footprint#footprint.active_count)
	end.

test_try_activate_footprint({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% First, no waiting footprints - should return false
		Result1 = try_activate_footprint(Pid),
		?assertEqual(false, Result1),
		
		%% Enqueue task with HasCapacity=false - goes to waiting
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, false, 1),
		{ok, _} = gen_server:call(Pid, get_state), %% sync
		
		%% Now try_activate_footprint should return true
		Result2 = try_activate_footprint(Pid),
		?assertEqual(true, Result2),
		
		%% Check state - task should now be in task_queue
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(1, queue:len(State#state.task_queue)),
		?assertEqual(0, State#state.waiting_count),
		%% Footprint should be in active_footprints set
		?assertEqual(true, sets:is_element(FootprintKey, State#state.active_footprints)),
		%% Footprint should have active_count = 1
		Footprint = maps:get(FootprintKey, State#state.footprints),
		?assertEqual(1, Footprint#footprint.active_count),
		?assertEqual(0, queue:len(Footprint#footprint.waiting))
	end.

test_footprint_task_cycling({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue one task with HasCapacity=true (activates footprint)
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 1),
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
		
		%% Initially set should be empty
		{ok, State0} = gen_server:call(Pid, get_state),
		?assertEqual(0, sets:size(State0#state.active_footprints)),
		
		%% Activate two footprints
		enqueue_task(Pid, FootprintKey1, {0, 100, Peer, store1, FootprintKey1}, true, 1),
		enqueue_task(Pid, FootprintKey2, {100, 200, Peer, store1, FootprintKey2}, true, 1),
		%% Third one goes to waiting
		enqueue_task(Pid, FootprintKey3, {200, 300, Peer, store1, FootprintKey3}, false, 1),
		
		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(2, sets:size(State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey1, State1#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey2, State1#state.active_footprints)),
		?assertEqual(false, sets:is_element(FootprintKey3, State1#state.active_footprints)),
		
		%% Deactivate one footprint
		task_completed(Pid, FootprintKey1, ok, 0, 100),
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(1, sets:size(State2#state.active_footprints)),
		?assertEqual(false, sets:is_element(FootprintKey1, State2#state.active_footprints)),
		?assertEqual(true, sets:is_element(FootprintKey2, State2#state.active_footprints))
	end.

test_add_task_to_active_footprint({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		
		%% Activate footprint with one task
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 1),
		{ok, State1} = gen_server:call(Pid, get_state),
		Footprint1 = maps:get(FootprintKey, State1#state.footprints),
		?assertEqual(1, Footprint1#footprint.active_count),
		?assertEqual(1, queue:len(State1#state.task_queue)),
		
		%% Add more tasks to same footprint (regardless of HasCapacity, goes to task_queue since active)
		enqueue_task(Pid, FootprintKey, {100, 200, Peer, store1, FootprintKey}, true, 1),
		enqueue_task(Pid, FootprintKey, {200, 300, Peer, store1, FootprintKey}, false, 1),
		
		{ok, State2} = gen_server:call(Pid, get_state),
		Footprint2 = maps:get(FootprintKey, State2#state.footprints),
		%% active_count should be 3 (all tasks added to active footprint)
		?assertEqual(3, Footprint2#footprint.active_count),
		%% All 3 tasks should be in task_queue
		?assertEqual(3, queue:len(State2#state.task_queue)),
		%% waiting should still be empty
		?assertEqual(0, queue:len(Footprint2#footprint.waiting)),
		?assertEqual(0, State2#state.waiting_count)
	end.

test_footprint_deactivation_removes_from_map({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		
		%% Activate footprint
		enqueue_task(Pid, FootprintKey, {0, 100, Peer, store1, FootprintKey}, true, 1),
		{ok, State1} = gen_server:call(Pid, get_state),
		
		%% Footprint should exist in map
		?assertEqual(true, maps:is_key(FootprintKey, State1#state.footprints)),
		?assertEqual(true, sets:is_element(FootprintKey, State1#state.active_footprints)),
		
		%% Complete the task (footprint has no waiting tasks)
		task_completed(Pid, FootprintKey, ok, 0, 100),
		{ok, State2} = gen_server:call(Pid, get_state),
		
		%% Footprint should be completely removed from both structures
		?assertEqual(false, maps:is_key(FootprintKey, State2#state.footprints)),
		?assertEqual(false, sets:is_element(FootprintKey, State2#state.active_footprints)),
		?assertEqual(0, State2#state.waiting_count)
	end.

-endif.
