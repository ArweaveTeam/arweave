%%% @doc The module maintains a queue of processes fetching data from the network
%%% and from the local storage modules.
-module(ar_data_sync_worker_master).

-behaviour(gen_server).

-export([start_link/2, get_total_task_count/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

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
	peer_tasks = #{},
	sync_jobs = 0
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Return the number of tasks that are queued on the master or active on a worker.
get_total_task_count() ->
	get_scheduled_task_count() + get_queued_task_count().

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, process_task_queue),
	{ok, Config} = application:get_env(arweave, config),
	{ok, #state{
		all_workers = queue:from_list(Workers),
		worker_count = length(Workers),
		sync_jobs = Config#config.sync_jobs
	}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(process_task_queue, #state{ task_queue_len = 0 } = State) ->
	ar_util:cast_after(200, ?MODULE, process_task_queue),
	{noreply, State};
handle_cast(process_task_queue, #state{ worker_count = WorkerCount } = State) ->
	ScheduledTaskCount = get_scheduled_task_count(),
	TaskBudget = max(0, WorkerCount * 100 - ScheduledTaskCount),
	ar_util:cast_after(200, ?MODULE, process_task_queue),
	{noreply, process_task_queue(TaskBudget, State)};

handle_cast({read_range, _Args}, #state{ worker_count = 0 } = State) ->
	{noreply, State};
handle_cast({read_range, Args}, State) ->
	{noreply, enqueue_task(read_range, Args, State)};

handle_cast({sync_range, _Args}, #state{ worker_count = 0 } = State) ->
	{noreply, State};
handle_cast({sync_range, Args}, State) ->
	{noreply, enqueue_task(sync_range, Args, State)};

handle_cast({task_completed, {read_range, _}}, State) ->
	complete_task(read_range, "localhost"),
	{noreply, State};

handle_cast({task_completed, {sync_range, {Result, Peer, Duration}}}, State) ->
	State2 = complete_sync_range(Peer, Result, Duration, State),
	State3 = schedule_queued_sync_range(Peer, State2),	
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
%% Process main task queue
%%--------------------------------------------------------------------
process_task_queue(0, State) ->
	State;
process_task_queue(_N, #state{ task_queue_len = 0 } = State) ->
	State;
process_task_queue(N, State) ->
	#state{ task_queue = Q, task_queue_len = Len } = State,
	{{value, {Task, Args}}, Q2} = queue:out(Q),
	case Task of
		read_range ->
			{Start, End, OriginStoreID, TargetStoreID, SkipSmall} = Args,
			End2 = min(Start + 10 * 262144, End),
			{Worker, State2} = get_worker(State),
			schedule_task(Worker, Task, {Start, End2, OriginStoreID, TargetStoreID, SkipSmall}),
			{Q3, Len2} =
				case End2 == End of
					true ->
						{Q2, Len - 1};
					false ->
						Args2 = {End2, End, OriginStoreID, TargetStoreID, SkipSmall},
						{queue:in_r({read_range, Args2}, Q2), Len}
				end,
			State3 = State2#state{ task_queue = Q3, task_queue_len = Len2 },
			process_task_queue(N - 1, State3);
		sync_range ->
			{_Start, _End, Peer, _TargetStoreID} = Args,
			PeerTasks = get_peer_tasks(Peer, State),
			State2 = case PeerTasks#peer_tasks.active_count < PeerTasks#peer_tasks.max_active of
				true ->
					schedule_sync_range(Peer, PeerTasks, Args, State);
				false ->
					PeerTaskQueue = queue:in({sync_range, Args}, PeerTasks#peer_tasks.task_queue),
					% ?LOG_ERROR("*** enqueue_sync_range: ~p / ~p",
					% 	[ar_util:format_peer(Peer), queue:len(PeerTaskQueue)]),
					PeerTasks2 = PeerTasks#peer_tasks{ task_queue = PeerTaskQueue },
					set_peer_tasks(Peer, PeerTasks2, State)
			end,
			State3 = State2#state{ task_queue = Q2, task_queue_len = Len - 1 },
			process_task_queue(N - 1, State3)
	end.

%%--------------------------------------------------------------------
%% Stage 1: add tasks to the main queue or peer-specific queues
%%--------------------------------------------------------------------
enqueue_task(Task, Args, State) ->
	ets:update_counter(?MODULE, queued_tasks, {2, 1}, {queued_tasks, 0}),
	prometheus_gauge:inc(sync_tasks, [queued, Task, format_peer(Task, Args)]),
	State#state{ task_queue = queue:in({Task, Args}, State#state.task_queue),
			task_queue_len = State#state.task_queue_len + 1 }.

%%--------------------------------------------------------------------
%% Stage 2: schedule tasks to be run on workers
%%--------------------------------------------------------------------

decrement_queued_tasks(Task, FormattedPeer) ->
	decrement_queued_tasks(Task, FormattedPeer, 1).

decrement_queued_tasks(Task, FormattedPeer, N) ->
	ets:update_counter(?MODULE, queued_tasks, {2, -N}),
	prometheus_gauge:dec(sync_tasks, [queued, Task, FormattedPeer], N).

%% @doc If a peer has capacity, take the next task from its	queue and schedule it.
schedule_queued_sync_range(Peer, State) ->
	PeerTasks = get_peer_tasks(Peer, State),
	case PeerTasks#peer_tasks.active_count < PeerTasks#peer_tasks.max_active of
		true ->
			case queue:out(PeerTasks#peer_tasks.task_queue) of
				{{value, {sync_range, Args}}, PeerTaskQueue} ->
					PeerTasks2 = PeerTasks#peer_tasks{ task_queue = PeerTaskQueue },
					State2 = schedule_sync_range(Peer, PeerTasks2, Args, State),
					schedule_queued_sync_range(Peer, State2);
				_ ->
					State
			end;
		false ->
			State
	end.

%% @doc Schedule a sync_range task - this task may come from the main queue or a
%% peer-specific queue.
schedule_sync_range(Peer, PeerTasks, Args, State) ->
	{Start, End, Peer, TargetStoreID} = Args,
	{Worker, State2} = get_worker(State),
	schedule_task(Worker, sync_range, {Start, End, Peer, TargetStoreID, 3}),
	PeerTasks2 = PeerTasks#peer_tasks{ active_count = PeerTasks#peer_tasks.active_count + 1 },
	set_peer_tasks(Peer, PeerTasks2, State2).

%% @doc Schedule a task (either sync_range or read_range) to be run on a worker.
schedule_task(Worker, Task, Args) ->
	ets:update_counter(?MODULE, scheduled_tasks, {2, 1}, {scheduled_tasks, 0}),
	Peer = format_peer(Task, Args),
	decrement_queued_tasks(Task, Peer),
	prometheus_gauge:inc(sync_tasks, [scheduled, Task, Peer]),
	gen_server:cast(Worker, {Task, Args}).

%%--------------------------------------------------------------------
%% Stage 3: record a completed task and perhaps schedule more tasks
%%--------------------------------------------------------------------
complete_sync_range(Peer, Result, Duration, State) ->
	complete_task(sync_range, ar_util:format_peer(Peer)),
	PeerTasks = get_peer_tasks(Peer, State),
	Milliseconds = erlang:convert_time_unit(Duration, native, millisecond),
	MaxActive = case {Result, Milliseconds} of
		{ok, Milliseconds} when Milliseconds < 2000 andalso Milliseconds > 10 ->
			%% INCREASE max_active
			%% Successful, fast operation (but not so fast as to preclude a valid GET /chunk
			%% web request). This is a good peer, increase max_active to allow
			%% more concurrent requests.
			min(PeerTasks#peer_tasks.max_active + 1, State#state.sync_jobs);
		_ ->
			%% REDUCE max_active
			%% Either an error or a slow request. Reduce max_active to reduce the load on this
			%% peer.
			max(PeerTasks#peer_tasks.max_active - 1, 8)
	end,
	ActiveCount = PeerTasks#peer_tasks.active_count - 1,
	?LOG_ERROR("*** complete_sync_range: ~p / ~p / ~p / ~p -> ~p / ~p",
		[self(), ar_util:format_peer(Peer), ActiveCount, PeerTasks#peer_tasks.max_active ,
			MaxActive, Milliseconds]),
	MaxQueue = 3_600_000 div Milliseconds,
	TaskQueue = case queue:len(PeerTasks#peer_tasks.task_queue) - MaxQueue of
		TasksToCut when TasksToCut > 0 ->
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			{TaskQueue2, _} = queue:split(MaxQueue, PeerTasks#peer_tasks.task_queue),
			decrement_queued_tasks(sync_range, ar_util:format_peer(Peer), TasksToCut),
			?LOG_ERROR("*** reduce_task_queue: ~p / ~p / ~p -> ~p / ~p / ~p",
				[self(), ar_util:format_peer(Peer), queue:len(PeerTasks#peer_tasks.task_queue), queue:len(TaskQueue2), MaxQueue, Milliseconds]),
			TaskQueue2;
		_ ->
			PeerTasks#peer_tasks.task_queue
	end,

	PeerTasks2 = PeerTasks#peer_tasks{
		active_count = ActiveCount,
		max_active = MaxActive,
		task_queue = TaskQueue
	},
	set_peer_tasks(Peer, PeerTasks2, State).

complete_task(Task, Peer) ->
	ets:update_counter(?MODULE, scheduled_tasks, {2, -1}),
	prometheus_gauge:dec(sync_tasks, [scheduled, Task, Peer]).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

%% @doc Return the number of tasks that have been sent to workers and are not yet complete.
get_scheduled_task_count() ->
	case ets:lookup(?MODULE, scheduled_tasks) of
		[] ->
			0;
		[{_, N}] ->
			N
	end.

%% @doc Return the number of tasks that are queued on the master.
get_queued_task_count() ->
	case ets:lookup(?MODULE, queued_tasks) of
		[] ->
			0;
		[{_, N}] ->
			N
	end.

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

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

% get_worker_test_() ->
% 	{timeout, 60, fun test_get_peer_worker/0}.

% test_get_peer_worker() ->
% 	Peer1 = {1, 2, 3, 4, 1984},
% 	Peer2 = {101, 102, 103, 104, 1984},
% 	Peer3 = {201, 202, 203, 204, 1984},
% 	Workers = lists:seq(1, 8),
% 	State0 = #state{ 
% 		all_workers = queue:from_list(Workers),
% 		workers_per_peer = 3
% 	},
% 	{Worker1, State1} = get_peer_worker(Peer1, State0),
% 	assert_state(Peer1,
% 		1, [4, 5, 6, 7, 8, 1, 2, 3], [2, 3, 1],
% 		Worker1, State1),

% 	{Worker2, State2} = get_peer_worker(Peer1, State1),
% 	assert_state(Peer1,
% 		2, [4, 5, 6, 7, 8, 1, 2, 3], [3, 1, 2],
% 		Worker2, State2),

% 	{Worker3, State3} = get_peer_worker(Peer2, State2),
% 	assert_state(Peer2,
% 		4, [7, 8, 1, 2, 3, 4, 5, 6], [5, 6, 4],
% 		Worker3, State3),
	
% 	{Worker4, State4} = get_peer_worker(Peer1, State3),
% 	assert_state(Peer1,
% 		3, [7, 8, 1, 2, 3, 4, 5, 6], [1, 2, 3],
% 		Worker4, State4),

% 	{Worker5, State5} = get_peer_worker(Peer1, State4),
% 	assert_state(Peer1,
% 		1, [7, 8, 1, 2, 3, 4, 5, 6], [2, 3, 1],
% 		Worker5, State5),

% 	{Worker6, State6} = get_peer_worker(Peer3, State5),
% 	assert_state(Peer3,
% 		7, [2, 3, 4, 5, 6, 7, 8, 1], [8, 1, 7],
% 		Worker6, State6),

% 	{Worker7, State7} = get_peer_worker(Peer3, State6),
% 	assert_state(Peer3,
% 		8, [2, 3, 4, 5, 6, 7, 8, 1], [1, 7, 8],
% 		Worker7, State7),

% 	{Worker8, State8} = get_peer_worker(Peer3, State7),
% 	assert_state(Peer3,
% 		1, [2, 3, 4, 5, 6, 7, 8, 1], [7, 8, 1],
% 		Worker8, State8),

% 	{Worker9, State9} = get_peer_worker(Peer3, State8),
% 	assert_state(Peer3,
% 		7, [2, 3, 4, 5, 6, 7, 8, 1], [8, 1, 7],
% 		Worker9, State9).

% assert_state(Peer, ExpectedWorker, ExpectedAllWorkers, ExpectedPeerWorkers, Worker, State) ->
% 	PeerWorkers = maps:get(Peer, State#state.peer_workers, undefined),
% 	?assertEqual(ExpectedWorker, Worker),
% 	?assertEqual(ExpectedAllWorkers, queue:to_list(State#state.all_workers)),
% 	?assertEqual(ExpectedPeerWorkers, queue:to_list(PeerWorkers)).


% process_task_queue_test_() ->
% 	{timeout, 60, fun test_process_task_queue/0}.

% test_process_task_queue() ->
% 	TaskQueue = queue:from_list([{sync_range, {0, 1, peer1, storage_module_1}}]),
% 	WorkerQueue = queue:from_list([worker1, worker2]),

% 	State = process_task_queue(0, #state{ task_queue = TaskQueue, task_queue_len = 1,
% 			all_workers = WorkerQueue, worker_count = 2 }),

% 	?LOG_ERROR("Task Queue: ~p", [queue:to_list(State#state.task_queue)]),
% 	?LOG_ERROR("Worker Queue: ~p", [queue:to_list(State#state.all_workers)]).

