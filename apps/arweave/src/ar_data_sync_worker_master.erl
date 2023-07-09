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
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REBALANCE_FREQUENCY_MS, 60*1000).
-define(READ_RANGE_CHUNKS, 10).
-define(MIN_MAX_ACTIVE, 8).
-define(MIN_PEER_QUEUE, 20).

-record(peer_tasks, {
	peer = undefined,
	task_queue = queue:new(),
	task_queue_len = 0,
	active_count = 0,
	max_active = ?MIN_MAX_ACTIVE
}).

-record(state, {
	task_queue = queue:new(),
	task_queue_len = 0,
	queued_task_count = 0, %% includes tasks queued in the main queue and in peer queues
	scheduled_task_count = 0,
	workers = queue:new(),
	worker_count = 0,
	worker_loads = #{},
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
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),

	{ok, #state{
		workers = queue:from_list(Workers),
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

handle_cast({task_completed, {read_range, {Worker, _, _}}}, State) ->
	State2 = update_scheduled_task_count(Worker, read_range, "localhost", -1, State),
	{noreply, State2};

handle_cast({task_completed, {sync_range, {Worker, Result, Args, ElapsedNative}}}, State) ->
	{Start, End, Peer, _, _} = Args,
	?LOG_DEBUG([
		{event, task_completed},
		{task, sync_range},
		{peer, ar_util:format_peer(Peer)},
		{s, Start},
		{e, End},
		{result, Result},
		{elapsed, ElapsedNative}
		]),
	DataSize = End - Start,
	State2 = update_scheduled_task_count(Worker, sync_range, ar_util:format_peer(Peer), -1, State),
	PeerTasks = get_peer_tasks(Peer, State2),
	{PeerTasks2, State3} = complete_sync_range(PeerTasks, Result, ElapsedNative, DataSize, State2),
	{PeerTasks3, State4} = process_peer_queue(PeerTasks2, State3),	
	{noreply, set_peer_tasks(PeerTasks3, State4)};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	?LOG_DEBUG([{event, rebalance_peers}]),
	Peers = maps:keys(State#state.peer_tasks),
	AllPeerPerformances = ar_peers:get_peer_performances(Peers),
	{TargetLatency, TotalThroughput} = calculate_targets(Peers, AllPeerPerformances),
	{noreply, rebalance_peers(
		Peers, State#state.peer_tasks, AllPeerPerformances, TargetLatency, TotalThroughput, State)};

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
	State2 = update_queued_task_count(Task, FormattedPeer, 1, State),
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
max_peer_queue(_Peformance, 0) ->
	undefined;
max_peer_queue(_Performance, 0.0) ->
	undefined;
max_peer_queue(#performance{ rating = 0 } = _Performance, _TotalThroughput) ->
	undefined;
max_peer_queue(#performance{ rating = 0.0 } = _Performance, _TotalThroughput) ->
	undefined;
max_peer_queue(Performance, TotalThroughput) ->
	%% estimate of of this peer's througput
	PeerThroughput = Performance#performance.rating,
	%% The maximum number of tasks we allow to be queued for this peer is related to its
	%% contribution to our current throughput. Peers with a higher throughput can claim more
	%% of the queue.
	%%
	%% We also allow all peers to maintain a small queue no matter what - this is to allow for
	%% them to recover from a temporary drop in throughput.
	max(trunc((PeerThroughput / TotalThroughput) * max_tasks()), ?MIN_PEER_QUEUE).

%% @doc Cut a peer's queue to store roughly 15 minutes worth of tasks. This prevents
%% the a slow peer from filling up the ar_data_sync_worker_master queues, stalling the
%% workers and preventing ar_data_sync from pushing new tasks.
cut_peer_queue(_MaxQueue, PeerTasks, #state{ scheduled_task_count = 0 } = State) ->
	{PeerTasks, State};
cut_peer_queue(undefined, PeerTasks, State) ->
	{PeerTasks, State};
cut_peer_queue(MaxQueue, PeerTasks, State) ->
	Peer = PeerTasks#peer_tasks.peer,
	TaskQueue = PeerTasks#peer_tasks.task_queue,
	case PeerTasks#peer_tasks.task_queue_len - MaxQueue of
		TasksToCut when TasksToCut > 0 ->
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			?LOG_DEBUG([{event, cut_peer_queue},
				{peer, ar_util:format_peer(Peer)},
				{active_count, PeerTasks#peer_tasks.active_count},
				{scheduled_tasks, State#state.scheduled_task_count},
				{max_queue, MaxQueue}, {tasks_to_cut, TasksToCut}]),
			{TaskQueue2, _} = queue:split(MaxQueue, TaskQueue),
			{
				PeerTasks#peer_tasks{ 
					task_queue = TaskQueue2, task_queue_len = queue:len(TaskQueue2) },
				update_queued_task_count(sync_range, ar_util:format_peer(Peer), -TasksToCut, State)
			};
		_ ->
			{PeerTasks, State}
	end.

enqueue_peer_task(PeerTasks, Task, Args) ->
	PeerTaskQueue = queue:in({Task, Args}, PeerTasks#peer_tasks.task_queue),
	TaskQueueLength = PeerTasks#peer_tasks.task_queue_len + 1,
	PeerTasks#peer_tasks{ task_queue = PeerTaskQueue, task_queue_len = TaskQueueLength }.

dequeue_peer_task(PeerTasks) ->
	{{value, {Task, Args}}, PeerTaskQueue} = queue:out(PeerTasks#peer_tasks.task_queue),
	TaskQueueLength = PeerTasks#peer_tasks.task_queue_len - 1,
	PeerTasks2 = PeerTasks#peer_tasks{ 
		task_queue = PeerTaskQueue, task_queue_len = TaskQueueLength },
	{PeerTasks2, Task, Args}.

peer_has_capacity(PeerTasks) ->
	PeerTasks#peer_tasks.active_count < PeerTasks#peer_tasks.max_active.

peer_has_queued_tasks(PeerTasks) ->
	PeerTasks#peer_tasks.task_queue_len > 0.

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
	State3 = update_scheduled_task_count(Worker, Task, FormattedPeer, 1, State2),
	update_queued_task_count(Task, FormattedPeer, -1, State3).

%%--------------------------------------------------------------------
%% Stage 3: record a completed task and update related values (i.e.
%%          EMA, max_active, peer queue length)
%%--------------------------------------------------------------------
complete_sync_range(PeerTasks, Result, ElapsedNative, DataSize, State) ->
	PeerTasks2 = PeerTasks#peer_tasks{ 
		active_count = PeerTasks#peer_tasks.active_count - 1
	},
	ar_peers:rate_fetched_data(
		PeerTasks2#peer_tasks.peer, chunk, Result,
		erlang:convert_time_unit(ElapsedNative, native, microsecond), DataSize,
		PeerTasks2#peer_tasks.max_active),
	{PeerTasks2, State}.

calculate_targets([], _AllPeerPerformances) ->
	{0.0, 0.0};
calculate_targets(Peers, AllPeerPerformances) ->
	TotalThroughput =
		lists:foldl(
			fun(Peer, Acc) -> 
				Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
				Acc + Performance#performance.rating
			end, 0.0, Peers),
    TotalLatency = 
		lists:foldl(
			fun(Peer, Acc) -> 
				Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
				Acc + Performance#performance.average_latency
			end, 0.0, Peers),
    TargetLatency = TotalLatency / length(Peers),
    {TargetLatency, TotalThroughput}.

rebalance_peers([], _AllPeerTasks, _AllPeerPerformances, _TargetLatency, _TotalThroughput, State) ->
	State;
rebalance_peers(
		[Peer | Peers], AllPeerTasks, AllPeerPerformances, TargetLatency, TotalThroughput, State) ->
	PeerTasks = maps:get(Peer, AllPeerTasks),
	Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
	{PeerTasks2, State2} = rebalance_peer(
		PeerTasks, Performance, TargetLatency, TotalThroughput, State),
	State3 = set_peer_tasks(PeerTasks2, State2),
	rebalance_peers(
		Peers, AllPeerTasks, AllPeerPerformances, TargetLatency, TotalThroughput, State3).

rebalance_peer(PeerTasks, Performance, TargetLatency, TotalThroughput, State) ->
	{PeerTasks2, State2} = cut_peer_queue(
		max_peer_queue(Performance, TotalThroughput),
		PeerTasks,
		State),
	WorkerCount = State2#state.worker_count,
	PeerTasks3 = update_active(PeerTasks2, Performance, WorkerCount, TargetLatency),
	?LOG_DEBUG([
		{event, update_active},
		{peer, ar_util:format_peer(PeerTasks3#peer_tasks.peer)},
		{before_max, PeerTasks2#peer_tasks.max_active},
		{after_max, PeerTasks3#peer_tasks.max_active},
		{worker_count, WorkerCount},
		{active_count, PeerTasks2#peer_tasks.active_count},
		{target_latency, TargetLatency},
		{average_latency, Performance#performance.average_latency}
		]),
	{PeerTasks3, State2}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
update_queued_task_count(Task, FormattedPeer, N, State) ->
	prometheus_gauge:inc(sync_tasks, [queued, Task, FormattedPeer], N),
	State#state{ queued_task_count = State#state.queued_task_count + N }.
update_scheduled_task_count(Worker, Task, FormattedPeer, N, State) ->
	prometheus_gauge:inc(sync_tasks, [scheduled, Task, FormattedPeer], N),
	Load = maps:get(Worker, State#state.worker_loads, 0) + N,
	State2 = State#state{
		scheduled_task_count = State#state.scheduled_task_count + N,
		worker_loads = maps:put(Worker, Load, State#state.worker_loads)
	},
	State2.

get_peer_tasks(Peer, State) ->
	maps:get(Peer, State#state.peer_tasks, #peer_tasks{peer = Peer}).

set_peer_tasks(PeerTasks, State) ->
	State#state{ peer_tasks =
		maps:put(PeerTasks#peer_tasks.peer, PeerTasks, State#state.peer_tasks)
	}.

get_worker(State) ->
	AverageLoad = State#state.scheduled_task_count / State#state.worker_count,
	cycle_workers(AverageLoad, State).

cycle_workers(AverageLoad, #state{ workers = Workers, worker_loads = WorkerLoads} = State) ->
	{{value, Worker}, Workers2} = queue:out(Workers),
	State2 = State#state{ workers = queue:in(Worker, Workers2) },
	Load = maps:get(Worker, WorkerLoads, 0),
	case Load =< AverageLoad of
		true ->
			{Worker, State2};
		false ->
			cycle_workers(AverageLoad, State2)
	end.

format_peer(Task, Args) ->
	case Task of
		read_range -> "localhost";
		sync_range ->
			ar_util:format_peer(element(3, Args))
	end.

update_active(PeerTasks, Performance, WorkerCount, TargetLatency) ->
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
	MaxActive = PeerTasks#peer_tasks.max_active,
	ActiveCount = PeerTasks#peer_tasks.active_count,
	TargetMaxActive = case Performance#performance.average_latency < TargetLatency of
		false ->
			%% latency > target, decrease max_active
			MaxActive-1;
		true ->
			%% latency < target, increase max_active.
			MaxActive+1
	end,

	%% Can't have more active tasks than workers.
	WorkerLimitedMaxActive = min(TargetMaxActive, WorkerCount),
	%% Can't have more active tasks than we have active or queued tasks.
	TaskLimitedMaxActive = min(
		WorkerLimitedMaxActive, 
		max(ActiveCount, PeerTasks#peer_tasks.task_queue_len)
	),
	%% Can't have less than the minimum.
	PeerTasks#peer_tasks{
		max_active = max(TaskLimitedMaxActive, ?MIN_MAX_ACTIVE)
	}.

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_counters/0},
		{timeout, 30, fun test_get_worker/0},
		{timeout, 30, fun test_format_peer/0}
	].

queue_test_() ->
	[
		{timeout, 30, fun test_enqueue_main_task/0},
		{timeout, 30, fun test_enqueue_peer_task/0},
		{timeout, 30, fun test_process_main_queue/0}
	].

rebalance_peers_test_() ->
	[
		{timeout, 30, fun test_max_peer_queue/0},
		{timeout, 30, fun test_cut_peer_queue/0},
		{timeout, 30, fun test_update_active/0},
		{timeout, 30, fun test_calculate_targets/0}
	].

test_counters() ->
	State = #state{},
	?assertEqual(0, State#state.scheduled_task_count),
	?assertEqual(0, maps:get("worker1", State#state.worker_loads, 0)),
	?assertEqual(0, State#state.queued_task_count),
	State2 = update_scheduled_task_count("worker1", sync_range, "localhost", 10, State),
	State3 = update_queued_task_count(sync_range, "localhost", 10, State2),
	?assertEqual(10, State3#state.scheduled_task_count),
	?assertEqual(10, maps:get("worker1", State3#state.worker_loads, 0)),
	?assertEqual(10, State3#state.queued_task_count),
	State4 = update_scheduled_task_count("worker1", sync_range, "localhost", -1, State3),
	State5 = update_queued_task_count(sync_range, "localhost", -1, State4),
	?assertEqual(9, State5#state.scheduled_task_count),
	?assertEqual(9, maps:get("worker1", State5#state.worker_loads, 0)),
	?assertEqual(9, State5#state.queued_task_count),
	State6 = update_scheduled_task_count("worker2", sync_range, "localhost", 1, State5),
	?assertEqual(10, State6#state.scheduled_task_count),
	?assertEqual(1, maps:get("worker2", State6#state.worker_loads, 0)),
	State7 = update_scheduled_task_count("worker1", sync_range, "1.2.3.4:1984", -1, State6),
	State8 = update_queued_task_count(sync_range, "1.2.3.4:1984", -1, State7),
	?assertEqual(9, State8#state.scheduled_task_count),
	?assertEqual(8, maps:get("worker1", State8#state.worker_loads, 0)),
	?assertEqual(8, State8#state.queued_task_count).

test_get_worker() ->
	State0 = #state{
		workers = queue:from_list([worker1, worker2, worker3]),
		scheduled_task_count = 6,
		worker_count = 3,
		worker_loads = #{worker1 => 3, worker2 => 2, worker3 => 1}
	},
	%% get_worker will cycle the queue until it finds a worker that has a worker_load =< the 
	%% average load (i.e. scheduled_task_count / worker_count)
	{worker2, State1} = get_worker(State0),
	State2 = update_scheduled_task_count(worker2, sync_range, "localhost", 1, State1),
	{worker3, State3} = get_worker(State2),
	State4 = update_scheduled_task_count(worker3, sync_range, "localhost", 1, State3),
	{worker3, State5} = get_worker(State4),
	State6 = update_scheduled_task_count(worker3, sync_range, "localhost", 1, State5),
	{worker1, _} = get_worker(State6).

test_format_peer() ->
	?assertEqual("localhost", format_peer(read_range, {0, 100, 1, 2, true})),
	?assertEqual("localhost", format_peer(read_range, undefined)),
	?assertEqual("1.2.3.4:1984", format_peer(sync_range, {0, 100, {1, 2, 3, 4, 1984}, 2})).

test_enqueue_main_task() ->
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
	?assertEqual(3, State3#state.queued_task_count),

	{Task1, Args1, State4} = dequeue_main_task(State3),
	assert_task(sync_range, {100, 200, Peer2, StoreID2}, Task1, Args1),

	{Task2, Args2, State5} = dequeue_main_task(State4),
	assert_task(read_range, {0, 100, StoreID1, StoreID2, true}, Task2, Args2),
	assert_main_queue([
		{sync_range, {0, 100, Peer1, StoreID1}}
	], State5),
	%% queued_task_count isn't decremented until we schedule tasks
	?assertEqual(3, State5#state.queued_task_count).

test_enqueue_peer_task() ->
	PeerA = {1, 2, 3, 4, 1984},
	PeerB = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),

	PeerATasks = #peer_tasks{ peer = PeerA },
	PeerBTasks = #peer_tasks{ peer = PeerB },
	
	PeerATasks1 = enqueue_peer_task(PeerATasks, sync_range, {0, 100, PeerA, StoreID1}),
	PeerATasks2 = enqueue_peer_task(PeerATasks1, sync_range, {100, 200, PeerA, StoreID1}),
	PeerBTasks1 = enqueue_peer_task(PeerBTasks, sync_range, {200, 300, PeerB, StoreID1}),
	assert_peer_tasks([
		{sync_range, {0, 100, PeerA, StoreID1}},
		{sync_range, {100, 200, PeerA, StoreID1}}
	], 0, 8, PeerATasks2),
	assert_peer_tasks([
		{sync_range, {200, 300, PeerB, StoreID1}}
	], 0, 8, PeerBTasks1),

	{PeerATasks3, Task1, Args1} = dequeue_peer_task(PeerATasks2),
	assert_task(sync_range, {0, 100, PeerA, StoreID1}, Task1, Args1),
	{PeerBTasks2, Task2, Args2} = dequeue_peer_task(PeerBTasks1),
	assert_task(sync_range, {200, 300, PeerB, StoreID1}, Task2, Args2),
	assert_peer_tasks([
		{sync_range, {100, 200, PeerA, StoreID1}}
	], 0, 8, PeerATasks3),
	assert_peer_tasks([], 0, 8, PeerBTasks2).


test_process_main_queue() ->
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({?PARTITION_SIZE, 1, default}),
	StoreID2 = ar_storage_module:id({?PARTITION_SIZE, 2, default}),
	State0 = #state{
		workers = queue:from_list([worker1, worker2, worker3]), worker_count = 3
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
	?assertEqual(13, State13#state.queued_task_count),
	?assertEqual(0, State13#state.scheduled_task_count),

	State14 = process_main_queue(State13),
	assert_main_queue([], State14),
	?assertEqual(1, State14#state.queued_task_count),
	?assertEqual(13, State14#state.scheduled_task_count),
	?assertEqual([worker2, worker3, worker1], queue:to_list(State14#state.workers)),

	PeerTasks = get_peer_tasks(Peer1, State14),
	assert_peer_tasks(
		[{sync_range, {800, 900, Peer1, StoreID1}}],
		8, 8, PeerTasks).

test_max_peer_queue() ->
	{ok, OriginalConfig} = application:get_env(arweave, config),
	try
		ok = application:set_env(arweave, config, OriginalConfig#config{
			sync_jobs = 10
		}),
		?assertEqual(undefined, max_peer_queue(#performance{ rating = 10 }, 0)),
		?assertEqual(undefined, max_peer_queue(#performance{ rating = 10 }, 0.0)),
		?assertEqual(undefined, max_peer_queue(#performance{ rating = 0 }, 100)),
		?assertEqual(undefined, max_peer_queue(#performance{ rating = 0.0 }, 100)),
		?assertEqual(50, max_peer_queue(#performance{ rating = 10 }, 100)),
		?assertEqual(20, max_peer_queue(#performance{ rating = 1 }, 100))
	after
		application:set_env(arweave, config, OriginalConfig)
	end.

test_cut_peer_queue() ->
	{ok, OriginalConfig} = application:get_env(arweave, config),
	try
		ok = application:set_env(arweave, config, OriginalConfig#config{
			sync_jobs = 10
		}),

		Peer1 = {1, 2, 3, 4, 1984},
		TaskQueue = lists:seq(1, 100),
		PeerTasks = #peer_tasks{
			peer = Peer1,
			task_queue = queue:from_list(TaskQueue),
			task_queue_len = length(TaskQueue)
		},
		State = #state{
			queued_task_count = length(TaskQueue),
			scheduled_task_count = 10
		},
		
		{PeerTasks1, State1} = cut_peer_queue(200, PeerTasks, State),
		assert_peer_tasks(TaskQueue, 0, 8, PeerTasks1),
		?assertEqual(100, State1#state.queued_task_count),

		{PeerTasks2, State2} = cut_peer_queue(20, PeerTasks, State),
		assert_peer_tasks(lists:sublist(TaskQueue, 1, 20), 0, 8, PeerTasks2),
		?assertEqual(20, State2#state.queued_task_count),

		{PeerTasks3, State3} = cut_peer_queue(
			20, PeerTasks, State#state{ scheduled_task_count = 0 }),
		assert_peer_tasks(TaskQueue, 0, 8, PeerTasks3),
		?assertEqual(100, State3#state.queued_task_count),

		{PeerTasks4, State4} = cut_peer_queue(undefined, PeerTasks, State),
		assert_peer_tasks(TaskQueue, 0, 8, PeerTasks4),
		?assertEqual(100, State4#state.queued_task_count)
	after
		application:set_env(arweave, config, OriginalConfig)
	end.

test_update_active() ->
	Result1 = update_active(
		#peer_tasks{max_active = 10, active_count = 10, task_queue_len = 30},
		#performance{average_latency = 100},
		20,
		200),
	?assertEqual(11, Result1#peer_tasks.max_active),

	Result2 = update_active(
		#peer_tasks{max_active = 10, active_count = 20, task_queue_len = 30},
		#performance{average_latency = 300},
		20,
		200),
	?assertEqual(9, Result2#peer_tasks.max_active),

	Result3 = update_active(
		#peer_tasks{max_active = 10, active_count = 20, task_queue_len = 30},
		#performance{average_latency = 100},
		10,
		200),
	?assertEqual(10, Result3#peer_tasks.max_active),

	Result4 = update_active(
		#peer_tasks{max_active = 10, active_count = 5, task_queue_len = 10},
		#performance{average_latency = 100},
		20,
		200),
	?assertEqual(10, Result4#peer_tasks.max_active),

	Result5 = update_active(
		#peer_tasks{max_active = 10, active_count = 10, task_queue_len = 5},
		#performance{average_latency = 100},
		20,
		200),
	?assertEqual(10, Result5#peer_tasks.max_active),

	Result6 = update_active(
		#peer_tasks{max_active = 8, active_count = 20, task_queue_len = 30},
		#performance{average_latency = 300},
		20,
		200),
	?assertEqual(8, Result6#peer_tasks.max_active).

test_calculate_targets() ->
	Result1 = calculate_targets([], #{}),
	?assertEqual({0.0, 0.0}, Result1),

    Result2 = calculate_targets(
		["peer1", "peer2"],
		#{
			"peer1" => #performance{rating = 0, average_latency = 0},
			"peer2" => #performance{rating = 0, average_latency = 0}
		}),
    ?assertEqual({0.0, 0.0}, Result2),
	
	Result3 = calculate_targets(
		["peer1", "peer2"],
		#{
			"peer1" => #performance{rating = 5, average_latency = 2},
			"peer2" => #performance{rating = 3, average_latency = 4}
		}),
    ?assertEqual({3.0, 8.0}, Result3),

	Result4 = calculate_targets(
		["peer1", "peer2"],
		#{
			"peer1" => #performance{rating = 5, average_latency = 2}
		}),
    ?assertEqual({1.0, 5.0}, Result4),

    Result5 = calculate_targets(
		["peer1"],
		#{
			"peer1" => #performance{rating = 5, average_latency = 2},
			"peer2" => #performance{rating = 3, average_latency = 4}
		}),
    ?assertEqual({2.0, 5.0}, Result5).

assert_main_queue(ExpectedTasks, State) ->
	?assertEqual(ExpectedTasks, queue:to_list(State#state.task_queue)),
	?assertEqual(length(ExpectedTasks), State#state.task_queue_len).

assert_peer_tasks(ExpectedQueue, ExpectedActiveCount, ExpectedMaxActive, PeerTasks) ->
	?assertEqual(ExpectedQueue, queue:to_list(PeerTasks#peer_tasks.task_queue)),
	?assertEqual(ExpectedActiveCount, PeerTasks#peer_tasks.active_count),
	?assertEqual(ExpectedMaxActive, PeerTasks#peer_tasks.max_active).

assert_task(ExpectedTask, ExpectedArgs, Task, Args) ->
	?assertEqual(ExpectedTask, Task),
	?assertEqual(ExpectedArgs, Args).