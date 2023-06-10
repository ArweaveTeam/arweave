%%% @doc The module maintains a queue of processes fetching data from the network
%%% and from the local storage modules.
-module(ar_data_sync_worker_master).

-behaviour(gen_server).

-export([start_link/2, is_syncing_enabled/0, ready_for_work/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(READ_RANGE_CHUNKS, 10).
-define(MIN_MAX_ACTIVE, 8).
-define(LATENCY_ALPHA, 0.1).
-define(SUCCESS_ALPHA, 0.1).
-define(STARTING_LATENCY_EMA, 1000). %% initial value to avoid over-weighting the first response
-define(STARTING_LATENCY_TARGET, 2000). %% initial value to avoid over-weighting the first response

-record(peer_tasks, {
	peer = undefined,
	task_queue = queue:new(),
	active_count = 0,
	max_active = ?MIN_MAX_ACTIVE,
	latency_ema = ?STARTING_LATENCY_EMA, 
	success_ema = 1.0
}).

-record(state, {
	task_queue = queue:new(),
	task_queue_len = 0,
	queued_task_count = 0, %% includes tasks queued in the main queue and in peer queues
	scheduled_task_count = 0,
	all_workers = queue:new(),
	worker_count = 0,
	latency_target = ?STARTING_LATENCY_TARGET,
	peer_tasks = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Returns true if syncing is enabled (i.e. sync_jobs > 0).
is_syncing_enabled() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.sync_jobs > 0.

%% @doc Returns true if we can accept new tasks. Will always return false if syncing is
%% disabled (i.e. sync_jobs = 0).
ready_for_work() ->
	try
		gen_server:call(?MODULE, ready_for_work, 1000)
	catch
		exit:{timeout,_} ->
			false
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, process_main_queue),

	{ok, #state{
		all_workers = queue:from_list(Workers),
		worker_count = length(Workers)
	}}.

handle_call(ready_for_work, _From, State) ->
	TotalTaskCount = State#state.scheduled_task_count + State#state.queued_task_count,
	ReadyForWork = TotalTaskCount < max_tasks(),
	{reply, ReadyForWork, State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(process_main_queue, #state{ task_queue_len = 0 } = State) ->
	ar_util:cast_after(200, ?MODULE, process_main_queue),
	{noreply, State};
handle_cast(process_main_queue, State) ->
	ar_util:cast_after(200, ?MODULE, process_main_queue),
	{noreply, process_main_queue(State)};

handle_cast({read_range, _Args}, #state{ worker_count = 0 } = State) ->
	{noreply, State};
handle_cast({read_range, Args}, State) ->
	{noreply, enqueue_main_task(read_range, Args, State)};

handle_cast({sync_range, _Args}, #state{ worker_count = 0 } = State) ->
	{noreply, State};
handle_cast({sync_range, Args}, State) ->
	{noreply, enqueue_main_task(sync_range, Args, State)};

handle_cast({task_completed, {read_range, _}}, State) ->
	State2 = update_counters(scheduled, read_range, "localhost", -1, State),
	{noreply, State2};

handle_cast({task_completed, {sync_range, {Result, Peer, Duration}}}, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	{PeerTasks2, State2} = complete_sync_range(PeerTasks, Result, Duration, State),
	{PeerTasks3, State3} = process_peer_queue(PeerTasks2, State2),	
	{noreply, set_peer_tasks(PeerTasks3, State3)};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%%--------------------------------------------------------------------
%% Stage 1a: main queue management
%%--------------------------------------------------------------------
process_main_queue(#state{ task_queue_len = 0 } = State) ->
	State;
process_main_queue(State) ->
	{Task, Args, State2} = dequeue_main_task(State),
	State4 = case Task of
		read_range ->
			schedule_read_range(Args, State2);
		sync_range ->
			{_Start, _End, Peer, _TargetStoreID} = Args,
			PeerTasks = get_peer_tasks(Peer, State2),
			PeerTasks2 = enqueue_peer_task(PeerTasks, sync_range, Args),
			{PeerTasks3, State3} = process_peer_queue(PeerTasks2, State2),
			set_peer_tasks(PeerTasks3, State3)
	end,
	process_main_queue(State4).

push_main_task(Task, Args, State) ->
	enqueue_main_task(Task, Args, State, true).

enqueue_main_task(Task, Args, State) ->	
	enqueue_main_task(Task, Args, State, false).
enqueue_main_task(Task, Args, State, Front) ->
	TaskQueue = case Front of
		true ->
			%% Enqueue the task at the front of the queue.
			queue:in_r({Task, Args}, State#state.task_queue);
		false ->
			%% Enqueue the task at the back of the queue (i.e. a standard enqueue).
			queue:in({Task, Args}, State#state.task_queue)
	end,

	FormattedPeer = format_peer(Task, Args),
	State2 = update_counters(queued, Task, FormattedPeer, 1, State),
	State2#state{
		task_queue = TaskQueue,
		task_queue_len = State#state.task_queue_len + 1 }.

dequeue_main_task(State) ->
	#state{ task_queue = Q, task_queue_len = Len } = State,
	{{value, {Task, Args}}, Q2} = queue:out(Q),
	State2 = State#state{ task_queue = Q2, task_queue_len = Len - 1 },
	{Task, Args, State2}.

%%--------------------------------------------------------------------
%% Stage 1b: peer queue management
%%--------------------------------------------------------------------

%% @doc If a peer has capacity, take the next task from its queue and schedule it.
process_peer_queue(PeerTasks, State) ->
	case peer_has_capacity(PeerTasks) andalso peer_has_queued_tasks(PeerTasks) of
		true ->
			{PeerTasks2, sync_range, Args} = dequeue_peer_task(PeerTasks),
			{PeerTasks3, State2} = schedule_sync_range(PeerTasks2, Args, State),
			process_peer_queue(PeerTasks3, State2);
		false ->
			{PeerTasks, State}
	end.

%% @doc the maximum number of tasks we can have in process - including stasks queued here as well
%% as those scheduled on ar_data_sync_workers.
max_tasks() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.sync_jobs * 50.

%% @doc The maximum number of tasks we can have queued for a given peer.
max_peer_queue(_PeerTasks, #state{ scheduled_task_count = 0 } = _State) ->
	undefined;
max_peer_queue(_PeerTasks, #state{ latency_target = 0 } = _State) ->
	undefined;
max_peer_queue(_PeerTasks, #state{ latency_target = 0.0 } = _State) ->
	undefined;
max_peer_queue(#peer_tasks{ latency_ema = 0 } = _PeerTasks, _State) ->
	undefined;
max_peer_queue(#peer_tasks{ latency_ema = 0.0 } = _PeerTasks, _State) ->
	undefined;
max_peer_queue(PeerTasks, State) ->
	CurActive = PeerTasks#peer_tasks.active_count,
	LatencyEMA = PeerTasks#peer_tasks.latency_ema,
	SuccessEMA = PeerTasks#peer_tasks.success_ema,
	LatencyTarget = State#state.latency_target,
	ScheduledTasks = State#state.scheduled_task_count,
	%% estimate of our current total throughput
	TotalThroughput = ScheduledTasks * (1000.0 / LatencyTarget),
	%% estimate of of this peer's througput
	PeerThroughput = CurActive * SuccessEMA * (1000.0 / LatencyEMA),
	%% The maximum number of tasks we allow to be queued for this peer is related to its
	%% contribution to our current throughput. Peers with a higher throughput can claim more
	%% of the queue.
	%%
	%% We also allow all peers to maintain a small queue no matter what - this is to allow for
	%% them to recover from a temporary drop in throughput. The minimum queue is set to allow
	%% enough observations to work their way through the Latency EMA calculation.
	Minimum = trunc(2 * (1.0 / ?LATENCY_ALPHA)),
	max(trunc((PeerThroughput / TotalThroughput) * max_tasks()), Minimum).

%% @doc Cut a peer's queue to store roughly 15 minutes worth of tasks. This prevents
%% the a slow peer from filling up the ar_data_sync_worker_master queues, stalling the
%% workers and preventing ar_data_sync from pushing new tasks.
cut_peer_queue(undefined, PeerTasks, State) ->
	{PeerTasks, State};
cut_peer_queue(MaxQueue, PeerTasks, State) ->
	Peer = PeerTasks#peer_tasks.peer,
	TaskQueue = PeerTasks#peer_tasks.task_queue,
	case queue:len(TaskQueue) - MaxQueue of
		TasksToCut when TasksToCut > 0 ->
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			?LOG_DEBUG([{event, cut_peer_queue},
				{peer, Peer},
				{active_count, PeerTasks#peer_tasks.active_count},
				{scheduled_tasks, State#state.scheduled_task_count},
				{success_ema, PeerTasks#peer_tasks.success_ema},
				{latency_ema, PeerTasks#peer_tasks.latency_ema},
				{latency_target, State#state.latency_target},
				{max_queue, MaxQueue}, {tasks_to_cut, TasksToCut}]),
			{TaskQueue2, _} = queue:split(MaxQueue, TaskQueue),
			{
				PeerTasks#peer_tasks{ task_queue = TaskQueue2 },
				update_counters(queued, sync_range, ar_util:format_peer(Peer), -TasksToCut, State)
			};
		_ ->
			{PeerTasks, State}
	end.

enqueue_peer_task(PeerTasks, Task, Args) ->
	PeerTaskQueue = queue:in({Task, Args}, PeerTasks#peer_tasks.task_queue),
	PeerTasks#peer_tasks{ task_queue = PeerTaskQueue }.

dequeue_peer_task(PeerTasks) ->
	{{value, {Task, Args}}, PeerTaskQueue} = queue:out(PeerTasks#peer_tasks.task_queue),
	PeerTasks2 = PeerTasks#peer_tasks{ task_queue = PeerTaskQueue },
	{PeerTasks2, Task, Args}.

peer_has_capacity(PeerTasks) ->
	PeerTasks#peer_tasks.active_count < PeerTasks#peer_tasks.max_active.

peer_has_queued_tasks(PeerTasks) ->
	queue:len(PeerTasks#peer_tasks.task_queue) > 0.

%%--------------------------------------------------------------------
%% Stage 2: schedule tasks to be run on workers
%%--------------------------------------------------------------------

%% @doc Schedule a sync_range task - this task may come from the main queue or a
%% peer-specific queue.
schedule_sync_range(PeerTasks, Args, State) ->
	{Start, End, Peer, TargetStoreID} = Args,
	State2 = schedule_task(sync_range, {Start, End, Peer, TargetStoreID, 3}, State),
	PeerTasks2 = PeerTasks#peer_tasks{ active_count = PeerTasks#peer_tasks.active_count + 1 },
	{PeerTasks2, State2}.

schedule_read_range(Args, State) ->
	{Start, End, OriginStoreID, TargetStoreID, SkipSmall} = Args,
	End2 = min(Start + (?READ_RANGE_CHUNKS * ?DATA_CHUNK_SIZE), End),
	State2 = schedule_task(
		read_range, {Start, End2, OriginStoreID, TargetStoreID, SkipSmall}, State),
	case End2 == End of
		true ->
			State2;
		false ->
			Args2 = {End2, End, OriginStoreID, TargetStoreID, SkipSmall},
			push_main_task(read_range, Args2, State2)
	end.

%% @doc Schedule a task (either sync_range or read_range) to be run on a worker.
schedule_task(Task, Args, State) ->
	{Worker, State2} = get_worker(State),
	gen_server:cast(Worker, {Task, Args}),

	FormattedPeer = format_peer(Task, Args),
	State3 = update_counters(scheduled, Task, FormattedPeer, 1, State2),
	update_counters(queued, Task, FormattedPeer, -1, State3).

%%--------------------------------------------------------------------
%% Stage 3: record a completed task and update related values (i.e.
%%          EMA, max_active, peer queue length)
%%--------------------------------------------------------------------
complete_sync_range(PeerTasks, Result, Duration, State) ->
	Milliseconds = erlang:convert_time_unit(Duration, native, millisecond) / 1.0,

	IsOK = (Result == ok andalso Milliseconds > 10),
	LatencyEMA = trunc(calculate_ema(
					PeerTasks#peer_tasks.latency_ema, IsOK, Milliseconds, ?LATENCY_ALPHA)),
	SuccessEMA = calculate_ema(
					PeerTasks#peer_tasks.success_ema, true, ar_util:bool_to_int(IsOK) / 1.0,
					?SUCCESS_ALPHA),
	%% Target Latency is the EMA of all peers' latencies
	LatencyTargetAlpha =  2.0 / (State#state.worker_count + 1), %% heuristic - update as needed.
	LatencyTarget = trunc(calculate_ema(
					State#state.latency_target, IsOK, Milliseconds, LatencyTargetAlpha)),

	{PeerTasks2, State2} = cut_peer_queue(
		max_peer_queue(PeerTasks, State),
		PeerTasks#peer_tasks{ latency_ema = LatencyEMA, success_ema = SuccessEMA },
		State),
	PeerTasks3 = update_active(
		PeerTasks2, IsOK, Milliseconds, State2#state.worker_count, LatencyTarget),
	State3 = update_counters(
		scheduled, sync_range, ar_util:format_peer(PeerTasks#peer_tasks.peer), -1, State2),
	{PeerTasks3, State3#state{ latency_target = LatencyTarget }}.


%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
update_counters(TaskState, Task, FormattedPeer, N, State) ->
	prometheus_gauge:inc(sync_tasks, [TaskState, Task, FormattedPeer], N),
	case TaskState of
		queued ->
			State#state{ queued_task_count = State#state.queued_task_count + N };
		scheduled ->
			State#state{ scheduled_task_count = State#state.scheduled_task_count + N }
	end.

calculate_ema(OldEMA, false, _Value, _Alpha) ->
	OldEMA;
calculate_ema(OldEMA, true, Value, Alpha) ->
	Alpha * Value + (1 - Alpha) * OldEMA.

get_peer_tasks(Peer, State) ->
	maps:get(Peer, State#state.peer_tasks, #peer_tasks{peer = Peer}).

set_peer_tasks(PeerTasks, State) ->
	State#state{ peer_tasks =
		maps:put(PeerTasks#peer_tasks.peer, PeerTasks, State#state.peer_tasks)
	}.

get_worker(#state{ all_workers = AllWorkerQ } = State) ->
	{Worker, AllWorkerQ2} = cycle_worker(AllWorkerQ),
	{Worker, State#state{all_workers = AllWorkerQ2}}.

cycle_worker(WorkerQ) ->
	{{value, Worker}, WorkerQ2} = queue:out(WorkerQ),
	{Worker, queue:in(Worker, WorkerQ2)}.

format_peer(Task, Args) ->
	case Task of
		read_range -> "localhost";
		sync_range ->
			ar_util:format_peer(element(3, Args))
	end.

update_active(PeerTasks, IsOK, Milliseconds, WorkerCount, LatencyTarget) ->
	%% Determine target max_active:
	%% 1. Increase max_active when the EMA is less than the threshold
	%% 2. Decrease max_active if the most recent request was slower than the threshold - this
	%%    allows us to respond more quickly to a sudden drop in performance
	%%
	%% Once we have the target max_active, find the maximum of the currently active tasks
	%% and queued tasks. The new max_active is the minimum of the target and that value.
	%% This prevents situations where we have a low number of active tasks and no queue which
	%% causes each request to complete fast and hikes up the max_active. Then we get a new
	%% batch of queued tasks and since the max_active is so high we overwhelm the peer.
	LatencyEMA = PeerTasks#peer_tasks.latency_ema,
	MaxActive = PeerTasks#peer_tasks.max_active,
	ActiveCount = PeerTasks#peer_tasks.active_count - 1,
	QueueLength = queue:len(PeerTasks#peer_tasks.task_queue),
	TargetMaxActive = case {
			IsOK, Milliseconds < LatencyTarget, LatencyEMA < LatencyTarget} of
		{false, _, _} ->
			%% Always reduce if there was an error
			MaxActive-1;
		{true, false, _} ->
			%% Milliseconds > threshold, decrease max_active
			MaxActive-1;
		{true, true, true} ->
			%% Milliseconds < threshold and EMA < threshold, increase max_active.
			MaxActive+1;
		_ ->
			%% Milliseconds < threshold and EMA > threshold, do  nothing.
			MaxActive
	end,

	%% Can't have more active tasks than workers.
	WorkerLimitedMaxActive = min(TargetMaxActive, WorkerCount),
	%% Can't have more active tasks than we have active or queued tasks.
	TaskLimitedMaxActive = min(WorkerLimitedMaxActive, max(ActiveCount, QueueLength)),
	%% Can't have less than the minimum.
	PeerTasks#peer_tasks{
		active_count = ActiveCount,
		max_active = max(TaskLimitedMaxActive, ?MIN_MAX_ACTIVE)
	}.

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

% helpers_test_() ->
% 	[
% 		{timeout, 30, fun test_counters/0},
% 		{timeout, 30, fun test_get_worker/0},
% 		{timeout, 30, fun test_format_peer/0},
% 		{timeout, 30, fun test_update_active/0}
% 	].

% queue_test_() ->
% 	[
% 		{timeout, 30, fun test_enqueue_main_task/0},
% 		{timeout, 30, fun test_enqueue_peer_task/0},
% 		{timeout, 30, fun test_process_main_queue/0}
% 	].

% complete_sync_range_test_() ->
% 	[
% 		{timeout, 30, fun test_complete_sync_range/0},
% 		{timeout, 30, fun test_cut_peer_queue/0}
% 	].

% test_counters() ->
% 	State = #state{},
% 	?assertEqual(0, State#state.scheduled_task_count),
% 	?assertEqual(0, State#state.queued_task_count),
% 	State2 = update_counters(scheduled, sync_range, "localhost", 10, State),
% 	State3 = update_counters(queued, sync_range, "localhost", 10, State2),
% 	?assertEqual(10, State3#state.scheduled_task_count),
% 	?assertEqual(10, State3#state.queued_task_count),
% 	State4 = update_counters(scheduled, sync_range, "localhost", -1, State3),
% 	State5 = update_counters(queued, sync_range, "localhost", -1, State4),
% 	?assertEqual(9, State5#state.scheduled_task_count),
% 	?assertEqual(9, State5#state.queued_task_count),
% 	State6 = update_counters(scheduled, sync_range, "1.2.3.4:1984", -1, State5),
% 	State7 = update_counters(queued, sync_range, "1.2.3.4:1984", -1, State6),
% 	?assertEqual(8, State7#state.scheduled_task_count),
% 	?assertEqual(8, State7#state.queued_task_count).

% test_get_worker() ->
% 	State0 = #state{
% 		all_workers = queue:from_list([worker1, worker2, worker3]),
% 		worker_count = 3
% 	},
% 	{worker1, State1} = get_worker(State0),
% 	{worker2, State2} = get_worker(State1),
% 	{worker3, State3} = get_worker(State2),
% 	{worker1, _} = get_worker(State3).

% test_format_peer() ->
% 	?assertEqual("localhost", format_peer(read_range, {0, 100, 1, 2, true})),
% 	?assertEqual("localhost", format_peer(read_range, undefined)),
% 	?assertEqual("1.2.3.4:1984", format_peer(sync_range, {0, 100, {1, 2, 3, 4, 1984}, 2})).

% test_update_active() ->
% 	LatencyTarget = 3000,
% 	FastTime = LatencyTarget - 1,
% 	SlowTime = LatencyTarget + 1,
% 	PeerTasks = #peer_tasks{ peer = "localhost" },

% 	PeerTasks1 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{10, 10}, {PeerTasks1#peer_tasks.active_count, PeerTasks1#peer_tasks.max_active},
% 		"Increase max_active for fast Milliseconds and EMA"),

% 	PeerTasks2 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, FastTime, 9, LatencyTarget),
% 	?assertEqual(
% 		{10, 9}, {PeerTasks2#peer_tasks.active_count, PeerTasks2#peer_tasks.max_active},
% 		"Limit max_active to number of workers"),

% 	PeerTasks3 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, FastTime, 8, LatencyTarget),
% 	?assertEqual(
% 		{10, 8}, {PeerTasks3#peer_tasks.active_count, PeerTasks3#peer_tasks.max_active},
% 		"Decrease max_active to number of workers"),

% 	PeerTasks4 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 10,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 9))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{9, 9}, {PeerTasks4#peer_tasks.active_count, PeerTasks2#peer_tasks.max_active},
% 		"Limit max_active to max(active_count, queue length)"),

% 	PeerTasks5 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 9))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{10, 10}, {PeerTasks5#peer_tasks.active_count, PeerTasks5#peer_tasks.max_active},
% 		"Limit max_active to max(active_count, queue length)"),

% 	PeerTasks6 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 10,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{9, 10}, {PeerTasks6#peer_tasks.active_count, PeerTasks6#peer_tasks.max_active},
% 		"Limit max_active to max(active_count, queue length)"),

% 	PeerTasks7 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 9,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 8))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{8, 8}, {PeerTasks7#peer_tasks.active_count, PeerTasks7#peer_tasks.max_active},
% 		"Decrease max_active to max(active_count, queue length)"),

% 	PeerTasks8 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 8))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{10, 10}, {PeerTasks8#peer_tasks.active_count, PeerTasks8#peer_tasks.max_active},
% 		"Decrease max_active to max(active_count, queue length)"),

% 	PeerTasks9 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 9,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{8, 10}, {PeerTasks9#peer_tasks.active_count, PeerTasks9#peer_tasks.max_active},
% 		"Decrease max_active to max(active_count, queue length)"),

% 	PeerTasks10 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, false, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{10, 8}, {PeerTasks10#peer_tasks.active_count, PeerTasks10#peer_tasks.max_active},
% 		"Decrease max_active for error"),

% 	PeerTasks11 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, SlowTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{10, 8}, {PeerTasks11#peer_tasks.active_count, PeerTasks11#peer_tasks.max_active},
% 		"Decrease max_active for slow Milliseconds"),

% 	PeerTasks12 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = SlowTime,
% 			active_count = 11,
% 			max_active = 9,
% 			task_queue = queue:from_list(lists:seq(1, 10))
% 		}, true, FastTime, 10, LatencyTarget),
% 	?assertEqual(
% 		{10, 9}, {PeerTasks12#peer_tasks.active_count, PeerTasks12#peer_tasks.max_active},
% 		"Do nothing for conflicting Milliseconds and EMA"),

% 	PeerTasks13 = update_active(PeerTasks#peer_tasks{
% 			latency_ema = FastTime,
% 			active_count = 10,
% 			max_active = ?MIN_MAX_ACTIVE,
% 			task_queue = queue:from_list(lists:seq(1, 9))
% 		}, true, SlowTime, 9, LatencyTarget),
% 	?assertEqual(
% 		{9, ?MIN_MAX_ACTIVE}, {PeerTasks13#peer_tasks.active_count, PeerTasks13#peer_tasks.max_active},
% 		"Can't decrease below ?MIN_MAX_ACTIVE").

% test_enqueue_main_task() ->
% 	reset_ets(),
% 	Peer1 = {1, 2, 3, 4, 1984},
% 	Peer2 = {5, 6, 7, 8, 1985},
% 	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),
% 	StoreID2 = ar_storage_module:id({?PARTITION_SIZE, 2, default}),
% 	State0 = #state{},
	
% 	State1 = enqueue_main_task(read_range, {0, 100, StoreID1, StoreID2, true}, State0),
% 	State2 = enqueue_main_task(sync_range, {0, 100, Peer1, StoreID1}, State1),
% 	State3 = push_main_task(sync_range, {100, 200, Peer2, StoreID2}, State2),
% 	assert_main_queue([
% 		{sync_range, {100, 200, Peer2, StoreID2}},
% 		{read_range, {0, 100, StoreID1, StoreID2, true}},
% 		{sync_range, {0, 100, Peer1, StoreID1}}
% 	], State3),
% 	?assertEqual(3, State3#state.queued_task_count),

% 	{Task1, Args1, State4} = dequeue_main_task(State3),
% 	assert_task(sync_range, {100, 200, Peer2, StoreID2}, Task1, Args1),

% 	{Task2, Args2, State5} = dequeue_main_task(State4),
% 	assert_task(read_range, {0, 100, StoreID1, StoreID2, true}, Task2, Args2),
% 	assert_main_queue([
% 		{sync_range, {0, 100, Peer1, StoreID1}}
% 	], State5),
% 	%% queued_task_count isn't decremented until we schedule tasks
% 	?assertEqual(3, State5#state.queued_task_count).

% test_enqueue_peer_task() ->
% 	reset_ets(),
% 	PeerA = {1, 2, 3, 4, 1984},
% 	PeerB = {5, 6, 7, 8, 1985},
% 	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),

% 	PeerATasks = #peer_tasks{ peer = PeerA },
% 	PeerBTasks = #peer_tasks{ peer = PeerB },
	
% 	PeerATasks1 = enqueue_peer_task(PeerATasks, sync_range, {0, 100, PeerA, StoreID1}),
% 	PeerATasks2 = enqueue_peer_task(PeerATasks1, sync_range, {100, 200, PeerA, StoreID1}),
% 	PeerBTasks1 = enqueue_peer_task(PeerBTasks, sync_range, {200, 300, PeerB, StoreID1}),
% 	assert_peer_tasks([
% 		{sync_range, {0, 100, PeerA, StoreID1}},
% 		{sync_range, {100, 200, PeerA, StoreID1}}
% 	], 0, 8, ?STARTING_LATENCY_EMA, 1.0, PeerATasks2),
% 	assert_peer_tasks([
% 		{sync_range, {200, 300, PeerB, StoreID1}}
% 	], 0, 8, ?STARTING_LATENCY_EMA, 1.0, PeerBTasks1),

% 	{PeerATasks3, Task1, Args1} = dequeue_peer_task(PeerATasks2),
% 	assert_task(sync_range, {0, 100, PeerA, StoreID1}, Task1, Args1),
% 	{PeerBTasks2, Task2, Args2} = dequeue_peer_task(PeerBTasks1),
% 	assert_task(sync_range, {200, 300, PeerB, StoreID1}, Task2, Args2),
% 	assert_peer_tasks([
% 		{sync_range, {100, 200, PeerA, StoreID1}}
% 	], 0, 8, ?STARTING_LATENCY_EMA, 1.0, PeerATasks3),
% 	assert_peer_tasks([], 0, 8, ?STARTING_LATENCY_EMA, 1.0, PeerBTasks2).


% test_process_main_queue() ->
% 	reset_ets(),
% 	Peer1 = {1, 2, 3, 4, 1984},
% 	Peer2 = {5, 6, 7, 8, 1985},
% 	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),
% 	StoreID2 = ar_storage_module:id({?PARTITION_SIZE, 2, default}),
% 	State0 = #state{
% 		all_workers = queue:from_list([worker1, worker2, worker3]), worker_count = 3
% 	},

% 	State1 = enqueue_main_task(read_range, {0, 100, StoreID1, StoreID2, true}, State0),
% 	State2 = enqueue_main_task(sync_range, {0, 100, Peer1, StoreID1}, State1),
% 	State3 = enqueue_main_task(sync_range, {100, 200, Peer1, StoreID1}, State2),
% 	State4 = enqueue_main_task(sync_range, {200, 300, Peer1, StoreID1}, State3),
% 	State5 = enqueue_main_task(sync_range, {300, 400, Peer1, StoreID1}, State4),
% 	State6 = enqueue_main_task(sync_range, {400, 500, Peer1, StoreID1}, State5),
% 	State7 = enqueue_main_task(sync_range, {500, 600, Peer1, StoreID1}, State6),
% 	State8 = enqueue_main_task(sync_range, {600, 700, Peer1, StoreID1}, State7),
% 	State9 = enqueue_main_task(sync_range, {700, 800, Peer1, StoreID1}, State8),
% 	%% 9th task queued for Peer1 won't be scheduled
% 	State10 = enqueue_main_task(sync_range, {800, 900, Peer1, StoreID1}, State9),
% 	State11 = enqueue_main_task(sync_range, {900, 1000, Peer2, StoreID1}, State10),
% 	State12 = enqueue_main_task(sync_range, {1000, 1100, Peer2, StoreID1}, State11),
% 	%% Will get split into 2 tasks when processed
% 	State13 = enqueue_main_task(
% 		read_range, {100, 20 * 262144, StoreID1, StoreID2, true}, State12),
% 	?assertEqual(13, State13#state.queued_task_count),
% 	?assertEqual(0, State13#state.scheduled_task_count),

% 	State14 = process_main_queue(State13),
% 	assert_main_queue([], State14),
% 	?assertEqual(1, State14#state.queued_task_count),
% 	?assertEqual(13, State14#state.scheduled_task_count),
% 	?assertEqual([worker2, worker3, worker1], queue:to_list(State14#state.all_workers)),

% 	PeerTasks = get_peer_tasks(Peer1, State14),
% 	assert_peer_tasks(
% 		[{sync_range, {800, 900, Peer1, StoreID1}}],
% 		8, 8, ?STARTING_LATENCY_EMA, 1.0, PeerTasks).

% test_complete_sync_range() ->
% 	reset_ets(),
% 	PeerA = {1, 2, 3, 4, 1984},
% 	PeerB = {5, 6, 7, 8, 1985},
% 	Workers = [list_to_atom("worker"++integer_to_list(Value)) || Value <- lists:seq(1,11)],
% 	State0 = #state{
% 		all_workers = queue:from_list(Workers),
% 		worker_count = length(Workers),
% 		latency_target = 2200
% 	},

% 	State1 = enqueue_sync_range_tasks(PeerA, 17, State0),
% 	Tasks1 = queue:to_list(State1#state.task_queue),
% 	%% Since this test only calls process_main_queue and process_peer_queue once, the
% 	%% Peer1 queue won't change
% 	ExpectedPeerAQueue = lists:sublist(Tasks1, 9, 9),
% 	State2 = enqueue_sync_range_tasks(PeerB, 4, State1),
% 	State3 = process_main_queue(State2),
% 	?assertEqual(9, State3#state.queued_task_count),
% 	?assertEqual(12, State3#state.scheduled_task_count),
% 	PeerTasksA1 = get_peer_tasks(PeerA, State3),
% 	PeerTasksB1 = get_peer_tasks(PeerB, State3),
% 	assert_peer_tasks(
% 		ExpectedPeerAQueue, 8, 8, ?STARTING_LATENCY_EMA, 1.0, PeerTasksA1),
% 	assert_peer_tasks([], 4, 8, ?STARTING_LATENCY_EMA, 1.0, PeerTasksB1),

% 	%% Quick task
% 	{PeerTasksA2, State4} = complete_sync_range(PeerTasksA1,
% 		ok, (State3#state.latency_target-500) * 1_000_000, State3),
% 	?assertEqual(9, State4#state.queued_task_count),
% 	?assertEqual(11, State4#state.scheduled_task_count),
% 	?assertEqual(2116, State4#state.latency_target),
% 	assert_peer_tasks(ExpectedPeerAQueue, 7, 9, 1070, 1.0, PeerTasksA2),

% 	%% Quick task, but can't go above Peer1 queue length
% 	{PeerTasksA3, State5} = complete_sync_range(PeerTasksA2,
% 		ok, (State4#state.latency_target-1000) * 1_000_000, State4),
% 	?assertEqual(9, State5#state.queued_task_count),
% 	?assertEqual(10, State5#state.scheduled_task_count),
% 	?assertEqual(1949, State5#state.latency_target),
% 	assert_peer_tasks(ExpectedPeerAQueue, 6, 9, 1074, 1.0, PeerTasksA3),

% 	%% Slow task but average is still quick -> max_active decreases
% 	{PeerTasksA4, State6} = complete_sync_range(PeerTasksA3, ok, (State5#state.latency_target+100) * 1_000_000, State5),
% 	?assertEqual(9, State6#state.queued_task_count),
% 	?assertEqual(9, State6#state.scheduled_task_count),
% 	?assertEqual(1965, State6#state.latency_target),
% 	assert_peer_tasks(ExpectedPeerAQueue, 5, 8, 1171, 1.0, PeerTasksA4),

% 	%% Quick task, but hit max workers
% 	{PeerTasksA5, State7} = complete_sync_range(PeerTasksA4,
% 		ok, (State6#state.latency_target-100) * 1_000_000, State6#state{ worker_count = 8 }),
% 	?assertEqual(9, State7#state.queued_task_count),
% 	?assertEqual(8, State7#state.scheduled_task_count),
% 	?assertEqual(1942, State7#state.latency_target),
% 	assert_peer_tasks(ExpectedPeerAQueue, 4, 8, 1240, 1.0, PeerTasksA5),

% 	%% Slow task pushes average slow
% 	{PeerTasksA6, State8} = complete_sync_range(PeerTasksA5, ok, (State7#state.latency_target+100_000) * 1_000_000, State7),
% 	?assertEqual(9, State8#state.queued_task_count),
% 	?assertEqual(7, State8#state.scheduled_task_count),
% 	?assertEqual(24164, State8#state.latency_target),
% 	assert_peer_tasks(ExpectedPeerAQueue, 3, 8, 11310, 1.0, PeerTasksA6),

% 	%% Too quick (error)
% 	{PeerTasksB2, State9} = complete_sync_range(PeerTasksB1, ok, 5 * 1_000_000, State8),
% 	?assertEqual(9, State9#state.queued_task_count),
% 	?assertEqual(6, State9#state.scheduled_task_count),
% 	?assertEqual(24164, State9#state.latency_target),
% 	assert_peer_tasks([], 3, 8, 1000, 0.9, PeerTasksB2),

% 	%% Error
% 	{PeerTasksB3, State10} = complete_sync_range(PeerTasksB2,
% 		{error, timeout}, (State9#state.latency_target-100) * 1_000_000, State9),
% 	?assertEqual(9, State10#state.queued_task_count),
% 	?assertEqual(5, State10#state.scheduled_task_count),
% 	?assertEqual(24164, State10#state.latency_target),
% 	assert_peer_tasks([], 2, 8, 1000, 0.81, PeerTasksB3),

% 	%% Slow task, but can't go below 8
% 	{PeerTasksB4, State11} = complete_sync_range(PeerTasksB3, ok, (State10#state.latency_target+100) * 1_000_000, State10),
% 	?assertEqual(9, State11#state.queued_task_count),
% 	?assertEqual(4, State11#state.scheduled_task_count),
% 	?assertEqual(24186, State11#state.latency_target),
% 	assert_peer_tasks([], 1, 8, 3326, 0.829, PeerTasksB4),

% 	%% Fast task, but can't go above max(active_count, peer queue length)
% 	{PeerTasksB5, State12} = complete_sync_range(PeerTasksB4, ok, (State11#state.latency_target-1000) * 1_000_000, State11),
% 	?assertEqual(9, State12#state.queued_task_count),
% 	?assertEqual(3, State12#state.scheduled_task_count),
% 	?assertEqual(23963, State12#state.latency_target),
% 	assert_peer_tasks([], 0, 8, 5312, 0.846, PeerTasksB5).

% test_cut_peer_queue() ->
% 	reset_ets(),
% 	Peer1 = {1, 2, 3, 4, 1984},
% 	Workers = [list_to_atom("worker"++integer_to_list(Value)) || Value <- lists:seq(1,8)],
% 	State0 = #state{
% 		all_workers = queue:from_list(Workers), worker_count = length(Workers)
% 	},

% 	State1 = enqueue_sync_range_tasks(Peer1, 18, State0),
% 	Tasks = queue:to_list(State1#state.task_queue),
% 	State2 = process_main_queue(State1),
% 	?assertEqual(10, State2#state.queued_task_count),
% 	?assertEqual(8, State2#state.scheduled_task_count),
% 	PeerTasks1 = get_peer_tasks(Peer1, State2),
% 	assert_peer_tasks(
% 		lists:sublist(Tasks, 9, 10),
% 		8, 8, ?STARTING_LATENCY_EMA, 1.0, PeerTasks1),

% 	%% Error at the beginning means EMA isn't set. This should not affect the peer queue.
% 	{PeerTasks2, State3} = complete_sync_range(PeerTasks1, {error, timeout}, 5 * 1_000_000, State2),
% 	?assertEqual(10, State3#state.queued_task_count),
% 	?assertEqual(7, State3#state.scheduled_task_count),
% 	?assertEqual(2000, State3#state.latency_target),
% 	assert_peer_tasks(lists:sublist(Tasks, 9, 10), 7, 8, 1000, 0.9, PeerTasks2),

% 	%% Really slow task, max queue size cut to 8. Reset success_ema to 1.0
% 	{PeerTasks3, State4} = complete_sync_range(
% 		PeerTasks2#peer_tasks{ success_ema = 1.0 }, ok, (1234 * 9) * 1_000_000, State3),
% 	?assertEqual(8, State4#state.queued_task_count),
% 	?assertEqual(6, State4#state.scheduled_task_count),
% 	?assertEqual(1_801_555, State4#state.latency_target),
% 	assert_peer_tasks(lists:sublist(Tasks, 9, 8), 6, 8, 810_900, 1.0, PeerTasks3),

% 	%% Really slow task (twice the maximum duration), EMA is updated to slightly less than
% 	%% twice the max duration duration -> max queue length is 7
% 	{PeerTasks4, State5} = complete_sync_range(PeerTasks3, ok, (1234 * 2) * 1_000_000, State4),
% 	?assertEqual(7, State5#state.queued_task_count),
% 	?assertEqual(5, State5#state.scheduled_task_count),
% 	?assertEqual(1_801_209, State5#state.latency_target),
% 	assert_peer_tasks(lists:sublist(Tasks, 9, 7), 5, 8, 909_810, 1.0, PeerTasks4),

% 	%% We should still cut a peer queue when there's an error so long as the MaxQueue is less
% 	%% than actual peer queue length
% 	State6 = set_peer_tasks(PeerTasks4, State5),
% 	State7 = enqueue_sync_range_tasks(Peer1, 10, State6),
% 	State8 = process_main_queue(State7),
% 	PeerTasks5 = get_peer_tasks(Peer1, State8),
% 	?assertEqual(14, State8#state.queued_task_count),
% 	?assertEqual(8, State8#state.scheduled_task_count),
% 	?assertEqual(1_801_209, State8#state.latency_target),
% 	?assertEqual(909_810, PeerTasks5#peer_tasks.latency_ema),
% 	?assertEqual(1.0, PeerTasks5#peer_tasks.success_ema),

% 	%% An error reduces the success_rate and therefore the max queue length
% 	{PeerTasks6, State9} = complete_sync_range(PeerTasks5, {error, timeout}, 5 * 1_000_000, State8),
% 	?assertEqual(7, State9#state.queued_task_count),
% 	?assertEqual(7, State9#state.scheduled_task_count),
% 	?assertEqual(1_801_209, State9#state.latency_target),
% 	?assertEqual(909_810, PeerTasks6#peer_tasks.latency_ema),
% 	?assertEqual(0.9, PeerTasks6#peer_tasks.success_ema).

% enqueue_sync_range_tasks(_Peer, 0, State) ->
% 	State;
% enqueue_sync_range_tasks(Peer, N, State) ->
% 	Args = {N*100, (N+1)*100, Peer, ar_storage_module:id({?PARTITION_SIZE, 1, default})},
% 	State1 = enqueue_main_task(sync_range, Args, State),
% 	enqueue_sync_range_tasks(Peer, N-1, State1).

% assert_main_queue(ExpectedTasks, State) ->
% 	?assertEqual(ExpectedTasks, queue:to_list(State#state.task_queue)),
% 	?assertEqual(length(ExpectedTasks), State#state.task_queue_len).

% assert_peer_tasks(
% 		ExpectedQueue, ExpectedActiveCount, ExpectedMaxActive,
% 		ExpectedLatencyEMA, ExpectedSuccessEMA,
% 		PeerTasks) ->
% 	?assertEqual(ExpectedQueue, queue:to_list(PeerTasks#peer_tasks.task_queue)),
% 	?assertEqual(ExpectedActiveCount, PeerTasks#peer_tasks.active_count),
% 	?assertEqual(ExpectedMaxActive, PeerTasks#peer_tasks.max_active),
% 	?assertEqual(ExpectedLatencyEMA, PeerTasks#peer_tasks.latency_ema),
% 	?assertEqual(round_3(ExpectedSuccessEMA), round_3(PeerTasks#peer_tasks.success_ema)).

% assert_task(ExpectedTask, ExpectedArgs, Task, Args) ->
% 	?assertEqual(ExpectedTask, Task),
% 	?assertEqual(ExpectedArgs, Args).

% %% This is only for testing purposes
% reset_ets() ->
% 	ets:delete_all_objects(?MODULE).

% round_3(Float) ->
% 	round(Float * 1000) / 1000.