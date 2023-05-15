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

-define(MIN_MAX_ACTIVE, 8).
-define(DURATION_LOWER_BOUND, 2000).
-define(MAX_QUEUE_DURATION, 3_600_000).
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
	peer_tasks = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Return the number of tasks that are queued on the master or active on a worker.
get_total_task_count() ->
	get_task_count(scheduled) + get_task_count(queued).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	reset_counters(queued),
	gen_server:cast(?MODULE, process_main_queue),
	{ok, #state{
		all_workers = queue:from_list(Workers),
		worker_count = length(Workers)
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

%% @doc If a peer has capacity, take the next task from its	queue and schedule it.
process_peer_queue(Peer, State) ->
	case peer_has_capacity(Peer, State) andalso peer_has_queued_tasks(Peer, State) of
		true ->
			{sync_range, Args, State2} = dequeue_peer_task(Peer, State),
			State3 = schedule_sync_range(Peer, Args, State2),
			process_peer_queue(Peer, State3);
		false ->
			State
	end.

cut_peer_queue(_Peer, TaskQueue, 0) ->
	TaskQueue;
cut_peer_queue(Peer, TaskQueue, EMA) ->
	MaxQueue = trunc(?MAX_QUEUE_DURATION / EMA),
	?LOG_ERROR("*** check max_queue: ~p / ~p / ~p / ~p",
				[self(), ar_util:format_peer(Peer), queue:len(TaskQueue), MaxQueue]),
	case queue:len(TaskQueue) - MaxQueue of
		TasksToCut when TasksToCut > 0 ->
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			{TaskQueue2, _} = queue:split(MaxQueue, TaskQueue),
			update_counters(queued, sync_range, ar_util:format_peer(Peer), -TasksToCut),
			?LOG_ERROR("*** reduce_task_queue: ~p / ~p / ~p -> ~p / ~p / ~p",
				[self(), ar_util:format_peer(Peer), queue:len(TaskQueue), queue:len(TaskQueue2), MaxQueue, EMA]),
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
%% Stage 3: record a completed task and perhaps schedule more tasks
%%--------------------------------------------------------------------
complete_sync_range(Peer, Result, Duration, State) ->
	update_counters(scheduled, sync_range, ar_util:format_peer(Peer), -1),
	PeerTasks = get_peer_tasks(Peer, State),
	Milliseconds = erlang:convert_time_unit(Duration, native, millisecond),
	{MaxActive2, TaskQueue2} = case {Result, Milliseconds} of
		{ok, Milliseconds} when Milliseconds > 10 ->
			EMA = update_ema(Peer, Milliseconds),
			MaxActive = update_max_active(EMA, PeerTasks#peer_tasks.max_active, State),
			TaskQueue = cut_peer_queue(Peer, PeerTasks#peer_tasks.task_queue, EMA),
			{MaxActive, TaskQueue};
		_ ->
			%% Either an error occured or the request was so quick something else must
			%% have gone wrong. Reduce max_active and don't update anything else.
			MaxActive = reduce_max_active(PeerTasks#peer_tasks.max_active),
			{MaxActive, PeerTasks#peer_tasks.task_queue}
	end,
	ActiveCount = PeerTasks#peer_tasks.active_count - 1,
	?LOG_ERROR("*** complete_sync_range: ~p / ~p / ~p / ~p / ~p -> ~p / ~p / ~p",
		[self(), ar_util:format_peer(Peer), ActiveCount, queue:length(TaskQueue2), PeerTasks#peer_tasks.max_active ,
			MaxActive2, Milliseconds, get_ema(Peer)]),

	PeerTasks2 = PeerTasks#peer_tasks{
		active_count = ActiveCount,
		max_active = MaxActive2,
		task_queue = TaskQueue2
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

update_ema(Peer, Milliseconds) ->
	NewEMA = case get_ema(Peer) of
		undefined ->
			Milliseconds / 1.0; %% convert to float
		OldEMA ->
			?EMA_ALPHA * Milliseconds + (1 - ?EMA_ALPHA) * OldEMA
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

update_max_active(EMA, MaxActive, State) ->
	case EMA of
		EMA when EMA < ?DURATION_LOWER_BOUND ->
			%% INCREASE max_active
			%% Successful, fast operation. This is a good peer, increase max_active to allow
			%% more concurrent requests.
			min(MaxActive + 1, State#state.worker_count);
		_ ->
			%% REDUCE max_active
			%% Either an error or a slow request. Reduce max_active to reduce the load on this
			%% peer.
			reduce_max_active(MaxActive)
	end.

reduce_max_active(MaxActive) ->
	max(MaxActive - 1, ?MIN_MAX_ACTIVE).

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
	EMA1 = update_ema("localhost", 100),
	?assertEqual(100.0, EMA1),
	?assertEqual(100.0, get_ema("localhost")),
	EMA2 = update_ema("localhost", 200),
	?assertEqual(110.0, EMA2),
	?assertEqual(110.0, get_ema("localhost")),

	?assertEqual(undefined, get_ema("1.2.3.4:1984")),
	EMA3 = update_ema("1.2.3.4:1984", 300),
	?assertEqual(300.0, EMA3),
	?assertEqual(300.0, get_ema("1.2.3.4:1984")),
	EMA4 = update_ema("1.2.3.4:1984", 500),
	?assertEqual(320.0, EMA4),
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
	?assertEqual(8, update_max_active(?DURATION_LOWER_BOUND-1, 8, #state{worker_count = 8})),
	?assertEqual(7, update_max_active(?DURATION_LOWER_BOUND-1, 8, #state{worker_count = 7})),
	?assertEqual(9, update_max_active(?DURATION_LOWER_BOUND-1, 8, #state{worker_count = 100})),
	?assertEqual(8, update_max_active(?DURATION_LOWER_BOUND, 9, #state{worker_count = 100})),
	?assertEqual(?MIN_MAX_ACTIVE,
		update_max_active(?DURATION_LOWER_BOUND, ?MIN_MAX_ACTIVE, #state{worker_count = 100})),
	?assertEqual(8, reduce_max_active(9)),
	?assertEqual(?MIN_MAX_ACTIVE, reduce_max_active(?MIN_MAX_ACTIVE)).

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
	State13 = enqueue_main_task(read_range, {100, 20 * 262144, StoreID1, StoreID2, true}, State12),
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
	State0 = #state{
		all_workers = queue:from_list(Workers), worker_count = length(Workers)
	},

	State1 = enqueue_sync_range_tasks(Peer1, 5, State0),
	State2 = enqueue_sync_range_tasks(Peer2, 4, State1),
	State3 = process_main_queue(State2),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(9, get_task_count(scheduled)),
	assert_peer_tasks([], 5, 8, Peer1, State3),
	assert_peer_tasks([], 4, 8, Peer2, State3),

	%% Quick task
	State4 = complete_sync_range(Peer1, ok, (?DURATION_LOWER_BOUND-500) * 1_000_000, State3),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(8, get_task_count(scheduled)),
	assert_peer_tasks([], 4, 9, Peer1, State4),
	assert_peer_tasks([], 4, 8, Peer2, State4),
	?assertEqual(1500.0, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Quick task
	State5 = complete_sync_range(Peer1, ok, (?DURATION_LOWER_BOUND-1000) * 1_000_000, State4),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(7, get_task_count(scheduled)),
	assert_peer_tasks([], 3, 10, Peer1, State5),
	assert_peer_tasks([], 4, 8, Peer2, State5),
	?assertEqual(1450.0, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Slow task - but average is still quick
	State6 = complete_sync_range(Peer1, ok, (?DURATION_LOWER_BOUND+100) * 1_000_000, State5),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(6, get_task_count(scheduled)),
	assert_peer_tasks([], 2, 11, Peer1, State6),
	assert_peer_tasks([], 4, 8, Peer2, State6),
	?assertEqual(1515.0, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Quick task, but hit max
	State7 = complete_sync_range(Peer1, ok, (?DURATION_LOWER_BOUND-100) * 1_000_000, State6),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(5, get_task_count(scheduled)),
	assert_peer_tasks([], 1, 11, Peer1, State7),
	assert_peer_tasks([], 4, 8, Peer2, State7),
	?assertEqual(1553.5, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Slow task pushes average slow
	State8 = complete_sync_range(Peer1, ok, (?DURATION_LOWER_BOUND+100_000) * 1_000_000, State7),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(4, get_task_count(scheduled)),
	assert_peer_tasks([], 0, 10, Peer1, State8),
	assert_peer_tasks([], 4, 8, Peer2, State8),
	?assertEqual(11598.15, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Too quick (error)
	State9 = complete_sync_range(Peer2, ok, 5 * 1_000_000, State8),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(3, get_task_count(scheduled)),
	assert_peer_tasks([], 0, 10, Peer1, State9),
	assert_peer_tasks([], 3, 8, Peer2, State9),
	?assertEqual(11598.15, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Error
	State10 = complete_sync_range(Peer2, {error, timeout}, (?DURATION_LOWER_BOUND-100) * 1_000_000, State9),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(2, get_task_count(scheduled)),
	assert_peer_tasks([], 0, 10, Peer1, State10),
	assert_peer_tasks([], 2, 8, Peer2, State10),
	?assertEqual(11598.15, get_ema(Peer1)),
	?assertEqual(undefined, get_ema(Peer2)),

	%% Slow task, but can't go below 8
	State11 = complete_sync_range(Peer2, ok, (?DURATION_LOWER_BOUND+100) * 1_000_000, State10),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(1, get_task_count(scheduled)),
	assert_peer_tasks([], 0, 10, Peer1, State11),
	assert_peer_tasks([], 1, 8, Peer2, State11),
	?assertEqual(11598.15, get_ema(Peer1)),
	?assertEqual(2100.0, get_ema(Peer2)),

	%% Fast task
	State12 = complete_sync_range(Peer2, ok, (?DURATION_LOWER_BOUND-1000) * 1_000_000, State11),
	?assertEqual(0, get_task_count(queued)),
	?assertEqual(0, get_task_count(scheduled)),
	assert_peer_tasks([], 0, 10, Peer1, State12),
	assert_peer_tasks([], 0, 9, Peer2, State12),
	?assertEqual(11598.15, get_ema(Peer1)),
	?assertEqual(1990.0, get_ema(Peer2)).

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

	%% Really slow task (1/4 the maximum duration), max queue size is 4
	State3 = complete_sync_range(Peer1, ok, (?MAX_QUEUE_DURATION div 4) * 1_000_000, State2),
	?assertEqual(4, get_task_count(queued)),
	?assertEqual(7, get_task_count(scheduled)),
	assert_peer_tasks(lists:sublist(Tasks, 9, 4), 7, 8, Peer1, State3),
	?assertEqual(900_000.0, get_ema(Peer1)),

	%% Really slow task (one half the maximum duration), EMA is updated to slightly less than
	%% 1/2 the max duration duration -> max queue length is 3
	State4 = complete_sync_range(Peer1, ok, (?MAX_QUEUE_DURATION div 2) * 1_000_000, State3),
	?assertEqual(3, get_task_count(queued)),
	?assertEqual(6, get_task_count(scheduled)),
	assert_peer_tasks(lists:sublist(Tasks, 9, 3), 6, 8, Peer1, State4),
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