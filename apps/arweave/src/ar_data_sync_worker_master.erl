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

-define(MIN_MAX_ACTIVE, 8).
-define(MAX_QUEUE_DURATION, 15 * 60 * 1000). %% 15 minutes in milliseconds
-define(EMA_ALPHA, 0.1).

-record(peer_tasks, {
	task_queue = queue:new(),
	active_count = 0,
	max_active = 8
}).

-record(state, {
	task_queue = queue:new(),
	task_queue_len = 0,
	all_workers = queue:new(),
	worker_count = 0,
	latency_target = 0,
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
	{ok, Config} = application:get_env(arweave, config),
	TotalTaskCount = get_task_count(scheduled) + get_task_count(queued),
	TotalTaskCount < (Config#config.sync_jobs * 50).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	reset_counters(queued),
	gen_server:cast(?MODULE, process_main_queue),

	LatencyTarget = calculate_latency_target(Workers, ar_packing_server:packing_rate()),
	ar:console("~nSync request latency target is: ~pms.~n", [LatencyTarget]),

	{ok, #state{
		all_workers = queue:from_list(Workers),
		worker_count = length(Workers),
		latency_target = LatencyTarget
	}}.

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
	update_counters(scheduled, read_range, "localhost", -1),
	{noreply, State};

handle_cast({task_completed, {sync_range, {Result, Peer, Duration}}}, State) ->
	State2 = complete_sync_range(Peer, Result, Duration, State),
	State3 = process_peer_queue(Peer, State2),	
	{noreply, State3};

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
			State3 = enqueue_peer_task(Peer, sync_range, Args, State2),
			process_peer_queue(Peer, State3)
	end,
	process_main_queue(State4).

push_main_task(Task, Args, State) ->
	enqueue_main_task(Task, Args, State, true).

enqueue_main_task(Task, Args, State) ->	
	enqueue_main_task(Task, Args, State, false).
enqueue_main_task(Task, Args, State, Front) ->
	FormattedPeer = format_peer(Task, Args),
	update_counters(queued, Task, FormattedPeer, 1),

	TaskQueue = case Front of
		true ->
			%% Enqueue the task at the front of the queue.
			queue:in_r({Task, Args}, State#state.task_queue);
		false ->
			%% Enqueue the task at the back of the queue (i.e. a standard enqueue).
			queue:in({Task, Args}, State#state.task_queue)
	end,

	State#state{
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
process_peer_queue(Peer, State) ->
	case peer_has_capacity(Peer, State) andalso peer_has_queued_tasks(Peer, State) of
		true ->
			{sync_range, Args, State2} = dequeue_peer_task(Peer, State),
			State3 = schedule_sync_range(Peer, Args, State2),
			process_peer_queue(Peer, State3);
		false ->
			State
	end.

%% @doc Cut a peer's queue to store roughly 15 minutes worth of tasks. This prevents
%% the a slow peer from filling up the ar_data_sync_worker_master queues, stalling the
%% workers and preventing ar_data_sync from pushing new tasks.
cut_peer_queue(_Peer, TaskQueue, _MaxActive, undefined) ->
	TaskQueue;
cut_peer_queue(_Peer, TaskQueue, _MaxActive, 0) ->
	TaskQueue;
cut_peer_queue(_Peer, TaskQueue, _MaxActive, 0.0) ->
	TaskQueue;
cut_peer_queue(Peer, TaskQueue, MaxActive, EMA) ->
	%% MaxQueue is an estimate of how many tasks we need queued to keep us busy for
	%% ?MAX_QUEUE_DURATION. We use the EMA to estimate how long each task will take, and
	%% assume MaxActive concurrently executing tasks.
	MaxQueue = trunc((?MAX_QUEUE_DURATION * MaxActive) / EMA),
	case queue:len(TaskQueue) - MaxQueue of
		TasksToCut when TasksToCut > 0 ->
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			?LOG_DEBUG([{event, cut_peer_queue},
				{peer, Peer}, {max_active, MaxActive}, {ema, EMA},
				{max_queue, MaxQueue}, {tasks_to_cut, TasksToCut}]),
			{TaskQueue2, _} = queue:split(MaxQueue, TaskQueue),
			update_counters(queued, sync_range, ar_util:format_peer(Peer), -TasksToCut),
			TaskQueue2;
		_ ->
			TaskQueue
	end.

enqueue_peer_task(Peer, Task, Args, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	PeerTaskQueue = queue:in({Task, Args}, PeerTasks#peer_tasks.task_queue),
	PeerTasks2 = PeerTasks#peer_tasks{ task_queue = PeerTaskQueue },
	set_peer_tasks(Peer, PeerTasks2, State).

dequeue_peer_task(Peer, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	{{value, {Task, Args}}, PeerTaskQueue} = queue:out(PeerTasks#peer_tasks.task_queue),
	PeerTasks2 = PeerTasks#peer_tasks{ task_queue = PeerTaskQueue },
	State2 = set_peer_tasks(Peer, PeerTasks2, State),
	{Task, Args, State2}.

peer_has_capacity(Peer, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	PeerTasks#peer_tasks.active_count < PeerTasks#peer_tasks.max_active.

peer_has_queued_tasks(Peer, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	queue:len(PeerTasks#peer_tasks.task_queue) > 0.

%%--------------------------------------------------------------------
%% Stage 2: schedule tasks to be run on workers
%%--------------------------------------------------------------------

%% @doc Schedule a sync_range task - this task may come from the main queue or a
%% peer-specific queue.
schedule_sync_range(Peer, Args, State) ->
	{Start, End, Peer, TargetStoreID} = Args,
	PeerTasks = get_peer_tasks(Peer, State),
	{Worker, State2} = get_worker(State),
	schedule_task(Worker, sync_range, {Start, End, Peer, TargetStoreID, 3}),
	PeerTasks2 = PeerTasks#peer_tasks{ active_count = PeerTasks#peer_tasks.active_count + 1 },
	set_peer_tasks(Peer, PeerTasks2, State2).

schedule_read_range(Args, State) ->
	{Start, End, OriginStoreID, TargetStoreID, SkipSmall} = Args,
	End2 = min(Start + 10 * 262144, End),
	{Worker, State2} = get_worker(State),
	schedule_task(Worker, read_range, {Start, End2, OriginStoreID, TargetStoreID, SkipSmall}),
	case End2 == End of
		true ->
			State2;
		false ->
			Args2 = {End2, End, OriginStoreID, TargetStoreID, SkipSmall},
			push_main_task(read_range, Args2, State2)
	end.

%% @doc Schedule a task (either sync_range or read_range) to be run on a worker.
schedule_task(Worker, Task, Args) ->
	FormattedPeer = format_peer(Task, Args),
	update_counters(scheduled, Task, FormattedPeer, 1),
	update_counters(queued, Task, FormattedPeer, -1),
	gen_server:cast(Worker, {Task, Args}).

%%--------------------------------------------------------------------
%% Stage 3: record a completed task and update related values (i.e.
%%          EMA, max_active, peer queue length)
%%--------------------------------------------------------------------
complete_sync_range(Peer, Result, Duration, State) ->
	?LOG_INFO([
		{event, complete_sync_range},
		{state, ?DATA_SIZE(State) / (1024*1024)}]),
	update_counters(scheduled, sync_range, ar_util:format_peer(Peer), -1),
	PeerTasks = get_peer_tasks(Peer, State),
	Milliseconds = erlang:convert_time_unit(Duration, native, millisecond),

	IsOK = (Result == ok andalso Milliseconds > 10),
	ActiveCount = PeerTasks#peer_tasks.active_count - 1,
	EMA = update_ema(Peer, IsOK, Milliseconds),
	TaskQueue = cut_peer_queue(
		Peer, PeerTasks#peer_tasks.task_queue, PeerTasks#peer_tasks.max_active, EMA),

	MaxActive = update_max_active(
		IsOK, Milliseconds, EMA,
		State#state.worker_count, ActiveCount, queue:len(TaskQueue), 
		State#state.latency_target, PeerTasks#peer_tasks.max_active
	),

	PeerTasks2 = PeerTasks#peer_tasks{
		active_count = ActiveCount,
		max_active = MaxActive,
		task_queue = TaskQueue
	},
	set_peer_tasks(Peer, PeerTasks2, State).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
update_counters(TaskState, Task, FormattedPeer, N) ->
	Key = ets_key(TaskState),
	ets:update_counter(?MODULE, Key, {2, N}, {Key,0}),
	prometheus_gauge:inc(sync_tasks, [TaskState, Task, FormattedPeer], N).

reset_counters(TaskState) ->
	ets:insert(?MODULE, {ets_key(TaskState), 0}).

get_task_count(TaskState) ->
	case ets:lookup(?MODULE, ets_key(TaskState)) of
		[] ->
			0;
		[{_, N}] ->
			N
	end.

ets_key(TaskState) ->
	list_to_atom(atom_to_list(TaskState) ++ "_tasks").

get_ema(Peer) ->
	case ets:lookup(?MODULE, {ema, Peer}) of
		[] ->
			undefined;
		[{_, EMA}] ->
			EMA
	end.

set_ema(Peer, EMA) ->
	ets:insert(?MODULE, {{ema, Peer}, EMA}).

update_ema(Peer, IsOK, Milliseconds) ->
	OldEMA = get_ema(Peer),
	NewEMA = case {IsOK, OldEMA} of
		{true, undefined} ->
			Milliseconds / 1.0; %% convert to float
		{true, OldEMA} ->
			?EMA_ALPHA * Milliseconds + (1 - ?EMA_ALPHA) * OldEMA;
		_ ->
			OldEMA
	end,
	set_ema(Peer, NewEMA),
	NewEMA.

get_peer_tasks(Peer, State) ->
	maps:get(Peer, State#state.peer_tasks, #peer_tasks{}).

set_peer_tasks(Peer, PeerTasks, State) ->
	State#state{ peer_tasks = maps:put(Peer, PeerTasks, State#state.peer_tasks) }.

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

update_max_active(
		IsOK, Milliseconds, EMA, WorkerCount, ActiveCount, QueueLength, LatencyTarget,
		MaxActive) ->
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
	TargetMaxActive = case {
			IsOK, Milliseconds < LatencyTarget, EMA < LatencyTarget} of
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
	max(TaskLimitedMaxActive, ?MIN_MAX_ACTIVE).

%% @doc The GET /chunk latency target is set such that we can sync chunks at a rate that will
%% saturate the configured packing rate. For example, if:
%% WorkerCount = 100
%% PackingRate = 50
%%
%% Then we need need 50 of the 100 concurrent requests to return every second, which means
%% the latency target is 2000ms - if each request takes 2 seconds then, on average, half
%% (i.e. 50) of the concurrent requests will return every second.
%%
%% The user can adjust the this number by changing the packing_rate or sync_jobs
%% configuration parameters.
calculate_latency_target(_Workers, 0) ->
	%% if sync_jobs > 0, but packing_rate is 0, the assumption is either that the user has
	%% misconfigured the node, or that they are only expecting to sync and store unpacked
	%% data (i.e. neither unpacking nor packing is required). We'll assign a default 1000ms
	%% latency target mostly to make the math easy. In order to increase the number of
	%% concurrent requests, the user can simply increase the sync_jobs.
	%%
	%% Note: if sync_jobs is 0, the ar_data_sync_worker_master will not be started.
	1000;
calculate_latency_target(Workers, PackingRate) ->
	WorkerCount = length(Workers),
	trunc((WorkerCount / PackingRate) * 1000).

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_counters/0},
		{timeout, 30, fun test_ema/0},
		{timeout, 30, fun test_get_worker/0},
		{timeout, 30, fun test_format_peer/0},
		{timeout, 30, fun test_max_active/0}
	].

queue_test_() ->
	[
		{timeout, 30, fun test_enqueue_main_task/0},
		{timeout, 30, fun test_enqueue_peer_task/0},
		{timeout, 30, fun test_process_main_queue/0}
	].

complete_sync_range_test_() ->
	[
		{timeout, 30, fun test_complete_sync_range/0},
		{timeout, 30, fun test_cut_peer_queue/0}
	].

test_counters() ->
	reset_counters(scheduled),
	reset_counters(queued),
	?assertEqual(0, get_task_count(scheduled)),
	?assertEqual(0, get_task_count(queued)),
	update_counters(scheduled, sync_range, "localhost", 10),
	update_counters(queued, sync_range, "localhost", 10),
	?assertEqual(10, get_task_count(scheduled)),
	?assertEqual(10, get_task_count(queued)),
	update_counters(scheduled, sync_range, "localhost", -1),
	update_counters(queued, sync_range, "localhost", -1),
	?assertEqual(9, get_task_count(scheduled)),
	?assertEqual(9, get_task_count(queued)),
	update_counters(scheduled, sync_range, "1.2.3.4:1984", -1),
	update_counters(queued, sync_range, "1.2.3.4:1984", -1),
	?assertEqual(8, get_task_count(scheduled)),
	?assertEqual(8, get_task_count(queued)),
	reset_counters(scheduled),
	reset_counters(queued),
	?assertEqual(0, get_task_count(scheduled)),
	?assertEqual(0, get_task_count(queued)).

test_ema() ->
	reset_ets(),
	?assertEqual(undefined, get_ema("localhost")),
	EMA1 = update_ema("localhost", true, 100),
	?assertEqual(100.0, EMA1),
	?assertEqual(100.0, get_ema("localhost")),
	EMA2 = update_ema("localhost", true, 200),
	?assertEqual(110.0, EMA2),
	?assertEqual(110.0, get_ema("localhost")),

	?assertEqual(undefined, get_ema("1.2.3.4:1984")),
	EMA3 = update_ema("1.2.3.4:1984", true, 300),
	?assertEqual(300.0, EMA3),
	?assertEqual(300.0, get_ema("1.2.3.4:1984")),
	EMA4 = update_ema("1.2.3.4:1984", true, 500),
	?assertEqual(320.0, EMA4),
	?assertEqual(320.0, get_ema("1.2.3.4:1984")),
	EMA5 = update_ema("1.2.3.4:1984", false, 500),
	?assertEqual(320.0, EMA5),
	?assertEqual(320.0, get_ema("1.2.3.4:1984")),

	?assertEqual(110.0, get_ema("localhost")).

test_get_worker() ->
	State0 = #state{
		all_workers = queue:from_list([worker1, worker2, worker3]),
		worker_count = 3
	},
	{worker1, State1} = get_worker(State0),
	{worker2, State2} = get_worker(State1),
	{worker3, State3} = get_worker(State2),
	{worker1, _} = get_worker(State3).

test_format_peer() ->
	?assertEqual("localhost", format_peer(read_range, {0, 100, 1, 2, true})),
	?assertEqual("localhost", format_peer(read_range, undefined)),
	?assertEqual("1.2.3.4:1984", format_peer(sync_range, {0, 100, {1, 2, 3, 4, 1984}, 2})).

test_max_active() ->
	LatencyTarget = 3000,
	FastTime = LatencyTarget - 1,
	SlowTime = LatencyTarget + 1,
	?assertEqual(
		10, update_max_active(true, FastTime, FastTime, 10, 10, 10, LatencyTarget, 9),
		"Increase max_active for fast Milliseconds and EMA"),
	?assertEqual(
		9, update_max_active(true, FastTime, FastTime, 9, 10, 10, LatencyTarget, 9),
		"Limit max_active to number of workers"),
	?assertEqual(
		8, update_max_active(true, FastTime, FastTime, 8, 10, 10, LatencyTarget, 9),
		"Decrease max_active to number of workers"),
	?assertEqual(
		9, update_max_active(true, FastTime, FastTime, 10, 9, 9, LatencyTarget, 9),
		"Limit max_active to max(active_count, queue length)"),
	?assertEqual(
		10, update_max_active(true, FastTime, FastTime, 10, 10, 9, LatencyTarget, 9),
		"Limit max_active to max(active_count, queue length)"),
	?assertEqual(
		10, update_max_active(true, FastTime, FastTime, 10, 9, 10, LatencyTarget, 9),
		"Limit max_active to max(active_count, queue length)"),
	?assertEqual(
		8, update_max_active(true, FastTime, FastTime, 10, 8, 8, LatencyTarget, 9),
		"Decrease max_active to max(active_count, queue length)"),
	?assertEqual(
		10, update_max_active(true, FastTime, FastTime, 10, 10, 8, LatencyTarget, 9),
		"Decrease max_active to max(active_count, queue length)"),
	?assertEqual(
		10, update_max_active(true, FastTime, FastTime, 10, 8, 10, LatencyTarget, 9),
		"Decrease max_active to max(active_count, queue length)"),
	?assertEqual(
		8, update_max_active(false, FastTime, FastTime, 10, 10, 10, LatencyTarget, 9),
		"Decrease max_active for error"),
	?assertEqual(
		8, update_max_active(true, SlowTime, FastTime, 10, 10, 10, LatencyTarget, 9),
		"Decrease max_active for slow Milliseconds"),
	?assertEqual(
		9, update_max_active(true, FastTime, SlowTime, 10, 10, 10, LatencyTarget, 9),
		"Do nothing for conflicting Milliseconds and EMA"),
	?assertEqual(
		?MIN_MAX_ACTIVE,
		update_max_active(true, SlowTime, FastTime, 9, 9, 9, LatencyTarget, ?MIN_MAX_ACTIVE),
		"Can't decrease below ?MIN_MAX_ACTIVE").

test_enqueue_main_task() ->
	reset_ets(),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),
	StoreID2 = ar_storage_module:id({?PARTITION_SIZE, 2, default}),
	State0 = #state{},
	
	State1 = enqueue_main_task(read_range, {0, 100, StoreID1, StoreID2, true}, State0),
	State2 = enqueue_main_task(sync_range, {0, 100, Peer1, StoreID1}, State1),
	State3 = push_main_task(sync_range, {100, 200, Peer2, StoreID2}, State2),
	assert_main_queue([
		{sync_range, {100, 200, Peer2, StoreID2}},
		{read_range, {0, 100, StoreID1, StoreID2, true}},
		{sync_range, {0, 100, Peer1, StoreID1}}
	], State3),
	?assertEqual(3, get_task_count(queued)),

	{Task1, Args1, State4} = dequeue_main_task(State3),
	assert_task(sync_range, {100, 200, Peer2, StoreID2}, Task1, Args1),

	{Task2, Args2, State5} = dequeue_main_task(State4),
	assert_task(read_range, {0, 100, StoreID1, StoreID2, true}, Task2, Args2),
	assert_main_queue([
		{sync_range, {0, 100, Peer1, StoreID1}}
	], State5),
	%% queued_task_count isn't decremented until we schedule tasks
	?assertEqual(3, get_task_count(queued)).

test_enqueue_peer_task() ->
	reset_ets(),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),
	State0 = #state{},
	
	State1 = enqueue_peer_task(Peer1, sync_range, {0, 100, Peer1, StoreID1}, State0),
	State2 = enqueue_peer_task(Peer1, sync_range, {100, 200, Peer1, StoreID1}, State1),
	State3 = enqueue_peer_task(Peer2, sync_range, {200, 300, Peer2, StoreID1}, State2),
	assert_peer_tasks([
		{sync_range, {0, 100, Peer1, StoreID1}},
		{sync_range, {100, 200, Peer1, StoreID1}}
	], 0, 8, Peer1, State3),
	assert_peer_tasks([
		{sync_range, {200, 300, Peer2, StoreID1}}
	], 0, 8, Peer2, State3),

	{Task1, Args1, State4} = dequeue_peer_task(Peer1, State3),
	assert_task(sync_range, {0, 100, Peer1, StoreID1}, Task1, Args1),
	{Task2, Args2, State5} = dequeue_peer_task(Peer2, State4),
	assert_task(sync_range, {200, 300, Peer2, StoreID1}, Task2, Args2),
	assert_peer_tasks([
		{sync_range, {100, 200, Peer1, StoreID1}}
	], 0, 8, Peer1, State5),
	assert_peer_tasks([], 0, 8, Peer2, State5).


test_process_main_queue() ->
	reset_ets(),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),
	StoreID2 = ar_storage_module:id({?PARTITION_SIZE, 2, default}),
	State0 = #state{
		all_workers = queue:from_list([worker1, worker2, worker3]), worker_count = 3
	},

	State1 = enqueue_main_task(read_range, {0, 100, StoreID1, StoreID2, true}, State0),
	State2 = enqueue_main_task(sync_range, {0, 100, Peer1, StoreID1}, State1),
	State3 = enqueue_main_task(sync_range, {100, 200, Peer1, StoreID1}, State2),
	State4 = enqueue_main_task(sync_range, {200, 300, Peer1, StoreID1}, State3),
	State5 = enqueue_main_task(sync_range, {300, 400, Peer1, StoreID1}, State4),
	State6 = enqueue_main_task(sync_range, {400, 500, Peer1, StoreID1}, State5),
	State7 = enqueue_main_task(sync_range, {500, 600, Peer1, StoreID1}, State6),
	State8 = enqueue_main_task(sync_range, {600, 700, Peer1, StoreID1}, State7),
	State9 = enqueue_main_task(sync_range, {700, 800, Peer1, StoreID1}, State8),
	%% 9th task queued for Peer1 won't be scheduled
	State10 = enqueue_main_task(sync_range, {800, 900, Peer1, StoreID1}, State9),
	State11 = enqueue_main_task(sync_range, {900, 1000, Peer2, StoreID1}, State10),
	State12 = enqueue_main_task(sync_range, {1000, 1100, Peer2, StoreID1}, State11),
	%% Will get split into 2 tasks when processed
	State13 = enqueue_main_task(
		read_range, {100, 20 * 262144, StoreID1, StoreID2, true}, State12),
	?assertEqual(13, get_task_count(queued)),
	?assertEqual(0, get_task_count(scheduled)),

	State14 = process_main_queue(State13),
	assert_main_queue([], State14),
	?assertEqual(1, get_task_count(queued)),
	?assertEqual(13, get_task_count(scheduled)),
	?assertEqual([worker2, worker3, worker1], queue:to_list(State14#state.all_workers)),

	PeerTasks = get_peer_tasks(Peer1, State14),
	?assertEqual(
		[{sync_range, {800, 900, Peer1, StoreID1}}],
		queue:to_list(PeerTasks#peer_tasks.task_queue)),
	?assertEqual(8, PeerTasks#peer_tasks.active_count).

test_complete_sync_range() ->
	reset_ets(),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	Workers = [list_to_atom("worker"++integer_to_list(Value)) || Value <- lists:seq(1,11)],
	LatencyTarget = calculate_latency_target(Workers, 5),
	?assertEqual(2200, LatencyTarget),
	State0 = #state{
		all_workers = queue:from_list(Workers),
		worker_count = length(Workers),
		latency_target = LatencyTarget
	},

	State1 = enqueue_sync_range_tasks(Peer1, 17, State0),
	Tasks1 = queue:to_list(State1#state.task_queue),
	%% Since this test only calls process_main_queue and process_peer_queue once, the
	%% Peer1 queue won't change
	ExpectedPeer1Queue = lists:sublist(Tasks1, 9, 9),
	State2 = enqueue_sync_range_tasks(Peer2, 4, State1),
	State3 = process_main_queue(State2),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(12, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 8, 8, Peer1, State3),
	assert_peer_tasks([], 4, 8, Peer2, State3),

	%% Quick task
	State4 = complete_sync_range(Peer1, ok, (LatencyTarget-500) * 1_000_000, State3),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(11, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 7, 9, Peer1, State4),
	assert_peer_tasks([], 4, 8, Peer2, State4),
	?assertEqual(1700.0, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Quick task, but can't go above Peer1 queue length
	State5 = complete_sync_range(Peer1, ok, (LatencyTarget-1000) * 1_000_000, State4),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(10, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 6, 9, Peer1, State5),
	assert_peer_tasks([], 4, 8, Peer2, State5),
	?assertEqual(1650.0, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Slow task but average is still quick -> max_active decreases
	State6 = complete_sync_range(Peer1, ok, (LatencyTarget+100) * 1_000_000, State5),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(9, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 5, 8, Peer1, State6),
	assert_peer_tasks([], 4, 8, Peer2, State6),
	?assertEqual(1715.0, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Quick task, but hit max workers
	State7 = complete_sync_range(
			Peer1, ok, (LatencyTarget-100) * 1_000_000, State6#state{ worker_count = 8 }),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(8, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 4, 8, Peer1, State7),
	assert_peer_tasks([], 4, 8, Peer2, State7),
	?assertEqual(1753.5, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Slow task pushes average slow
	State8 = complete_sync_range(Peer1, ok, (LatencyTarget+100_000) * 1_000_000, State7),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(7, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 3, 8, Peer1, State8),
	assert_peer_tasks([], 4, 8, Peer2, State8),
	?assertEqual(11798.15, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Too quick (error)
	State9 = complete_sync_range(Peer2, ok, 5 * 1_000_000, State8),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(6, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 3, 8, Peer1, State9),
	assert_peer_tasks([], 3, 8, Peer2, State9),
	?assertEqual(11798.15, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Error
	State10 = complete_sync_range(
		Peer2, {error, timeout}, (LatencyTarget-100) * 1_000_000, State9),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(5, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 3, 8, Peer1, State10),
	assert_peer_tasks([], 2, 8, Peer2, State10),
	?assertEqual(11798.15, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Slow task, but can't go below 8
	State11 = complete_sync_range(Peer2, ok, (LatencyTarget+100) * 1_000_000, State10),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(4, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 3, 8, Peer1, State11),
	assert_peer_tasks([], 1, 8, Peer2, State11),
	?assertEqual(11798.15, get_ema(Peer1)),
	?assertEqual(2300.0, get_ema(Peer2)),

	%% Fast task, but can't go above max(active_count, peer queue length)
	State12 = complete_sync_range(Peer2, ok, (LatencyTarget-1000) * 1_000_000, State11),
	?assertEqual(9, get_task_count(queued)),
	?assertEqual(3, get_task_count(scheduled)),
	assert_peer_tasks(ExpectedPeer1Queue, 3, 8, Peer1, State12),
	assert_peer_tasks([], 0, 8, Peer2, State12),
	?assertEqual(11798.15, get_ema(Peer1)),
	?assertEqual(2190.0, get_ema(Peer2)).

test_cut_peer_queue() ->
	reset_ets(),
	Peer1 = {1, 2, 3, 4, 1984},
	Workers = [list_to_atom("worker"++integer_to_list(Value)) || Value <- lists:seq(1,8)],
	State0 = #state{
		all_workers = queue:from_list(Workers), worker_count = length(Workers)
	},

	State1 = enqueue_sync_range_tasks(Peer1, 18, State0),
	Tasks = queue:to_list(State1#state.task_queue),
	State2 = process_main_queue(State1),
	?assertEqual(10, get_task_count(queued)),
	?assertEqual(8, get_task_count(scheduled)),
	assert_peer_tasks(lists:sublist(Tasks, 9, 10), 8, 8, Peer1, State2),

	%% Error at the beginning means EMA isn't set. This should not affect the peer queue.
	State3 = complete_sync_range(Peer1, {error, timeout}, 5 * 1_000_000, State2),
	?assertEqual(10, get_task_count(queued)),
	?assertEqual(7, get_task_count(scheduled)),
	assert_peer_tasks(lists:sublist(Tasks, 9, 10), 7, 8, Peer1, State3),

	%% Really slow task, max queue size cut to 8
	State4 = complete_sync_range(Peer1, ok, (?MAX_QUEUE_DURATION) * 1_000_000, State3),
	?assertEqual(8, get_task_count(queued)),
	?assertEqual(6, get_task_count(scheduled)),
	assert_peer_tasks(lists:sublist(Tasks, 9, 8), 6, 8, Peer1, State4),
	?assertEqual(900_000.0, get_ema(Peer1)),

	%% Really slow task (twice the maximum duration), EMA is updated to slightly less than
	%% twice the max duration duration -> max queue length is 7
	State5 = complete_sync_range(Peer1, ok, (?MAX_QUEUE_DURATION * 2) * 1_000_000, State4),
	?assertEqual(7, get_task_count(queued)),
	?assertEqual(5, get_task_count(scheduled)),
	assert_peer_tasks(lists:sublist(Tasks, 9, 7), 5, 8, Peer1, State5),
	?assertEqual(990_000.0, get_ema(Peer1)),

	%% We should still cut a peer queue when there's an error so long as the MaxQueue is less
	%% than actual peer queue length
	State6 = enqueue_sync_range_tasks(Peer1, 10, State5),
	State7 = process_main_queue(State6),
	?assertEqual(14, get_task_count(queued)),
	?assertEqual(8, get_task_count(scheduled)),
	?assertEqual(990_000.0, get_ema(Peer1)),

	_State8 = complete_sync_range(Peer1, {error, timeout}, 5 * 1_000_000, State7),
	?assertEqual(7, get_task_count(queued)),
	?assertEqual(7, get_task_count(scheduled)),
	?assertEqual(990_000.0, get_ema(Peer1)).

enqueue_sync_range_tasks(_Peer, 0, State) ->
	State;
enqueue_sync_range_tasks(Peer, N, State) ->
	Args = {N*100, (N+1)*100, Peer, ar_storage_module:id({?PARTITION_SIZE, 1, default})},
	State1 = enqueue_main_task(sync_range, Args, State),
	enqueue_sync_range_tasks(Peer, N-1, State1).

assert_main_queue(ExpectedTasks, State) ->
	?assertEqual(ExpectedTasks, queue:to_list(State#state.task_queue)),
	?assertEqual(length(ExpectedTasks), State#state.task_queue_len).

assert_peer_tasks(ExpectedQueue, ExpectedActiveCount, ExpectedMaxActive, Peer, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	?assertEqual(ExpectedQueue, queue:to_list(PeerTasks#peer_tasks.task_queue)),
	?assertEqual(ExpectedActiveCount, PeerTasks#peer_tasks.active_count),
	?assertEqual(ExpectedMaxActive, PeerTasks#peer_tasks.max_active).

assert_task(ExpectedTask, ExpectedArgs, Task, Args) ->
	?assertEqual(ExpectedTask, Task),
	?assertEqual(ExpectedArgs, Args).

%% This is only for testing purposes
reset_ets() ->
	ets:delete_all_objects(?MODULE).