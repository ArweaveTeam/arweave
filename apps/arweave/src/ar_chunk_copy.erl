%%% @doc The module maintains a queue of processes fetching data from the network
%%% and from the local storage modules.
-module(ar_chunk_copy).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, ready_for_work/1, read_range/4]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(READ_RANGE_CHUNKS, 400).
-define(MAX_ACTIVE_TASKS, 10).
-define(MAX_QUEUED_TASKS, 50).

-record(worker_tasks, {
	worker,
	task_queue = queue:new(),
	active_count = 0
}).

-record(state, {
	workers = #{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(WorkerMap) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, WorkerMap, []).

register_workers() ->
	{Workers, WorkerMap} = register_read_workers(),
	ChunkCopy = ?CHILD_WITH_ARGS(ar_chunk_copy, worker, ar_chunk_copy, [WorkerMap]),
	Workers ++ [ChunkCopy].

register_read_workers() ->
	{ok, Config} = application:get_env(arweave, config),
	{Workers, WorkerMap} = 
		lists:foldl(
			fun(StorageModule, {AccWorkers, AccWorkerMap}) ->
				StoreID = ar_storage_module:id(StorageModule),
				Name = list_to_atom("ar_data_sync_worker_" ++ StoreID),

				Worker = ?CHILD_WITH_ARGS(ar_data_sync_worker, worker, Name, [Name]),

				{[ Worker | AccWorkers], AccWorkerMap#{StoreID => Name}}
			end,
			{[], #{}},
			Config#config.storage_modules
		),
	{Workers, WorkerMap}.

%% @doc Returns true if we can accept new tasks. Will always return false if syncing is
%% disabled (i.e. sync_jobs = 0).
ready_for_work(StoreID) ->
	try
		gen_server:call(?MODULE, {ready_for_work, StoreID}, 1000)
	catch
		exit:{timeout,_} ->
			false
	end.

read_range(Start, End, OriginStoreID, TargetStoreID) ->
	case ar_chunk_copy:ready_for_work(OriginStoreID) of
		true ->
			Args = {Start, End, OriginStoreID, TargetStoreID},
			gen_server:cast(?MODULE, {read_range, Args}),
			true;
		false ->
			false
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(WorkerMap) ->
	?LOG_DEBUG([{event, init}, {module, ?MODULE}, {worker_map, WorkerMap}]),
	Workers = maps:fold(
		fun(StoreID, Name, Acc) ->
			Acc#{StoreID => #worker_tasks{worker = Name}}
		end,
		#{},
		WorkerMap
	),
	ar_util:cast_after(1000, self(), process_queues),
	{ok, #state{
		workers = Workers
	}}.

handle_call({ready_for_work, StoreID}, _From, State) ->
	{reply, do_ready_for_work(StoreID, State), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({read_range, Args}, State) ->
	?LOG_DEBUG([{event, read_range}, {module, ?MODULE}, {args, Args}]),
	{noreply, enqueue_read_range(Args, State)};

handle_cast(process_queues, State) ->
	?LOG_DEBUG([{event, process_queues}, {module, ?MODULE}]),
	ar_util:cast_after(1000, self(), process_queues),
	{noreply, process_queues(State)};

handle_cast({task_completed, {read_range, {Worker, _, Args}}}, State) ->
	?LOG_DEBUG([{event, task_completed}, {module, ?MODULE}, {worker, Worker}, {args, Args}]),
	{noreply, task_completed(Args, State)};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_DEBUG([{event, terminate}, {module, ?MODULE}, {reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

do_ready_for_work(StoreID, State) ->
	Worker = maps:get(StoreID, State#state.workers, undefined),
	case Worker of
		undefined ->
			?LOG_ERROR([{event, worker_not_found}, {module, ?MODULE}, {call, ready_for_work},
				{store_id, StoreID}]),
			false;
		_ ->
			queue:len(Worker#worker_tasks.task_queue) < ?MAX_QUEUED_TASKS
	end.

enqueue_read_range(Args, State) ->
	{_Start, _End, OriginStoreID, _TargetStoreID} = Args,
	Worker = maps:get(OriginStoreID, State#state.workers, undefined),
	case Worker of
		undefined ->
			?LOG_ERROR([{event, worker_not_found}, {module, ?MODULE},
				{call, enqueue_read_range}, {store_id, OriginStoreID}]),
			State;
		_ ->
			Worker2 = do_enqueue_read_range(Args, Worker),
			State#state{
				workers = maps:put(OriginStoreID, Worker2, State#state.workers)
			}
	end.

do_enqueue_read_range(Args, Worker) ->
	{Start, End, OriginStoreID, TargetStoreID} = Args,
	End2 = min(Start + (?READ_RANGE_CHUNKS * ?DATA_CHUNK_SIZE), End),
	Args2 = {Start, End2, OriginStoreID, TargetStoreID},
	?LOG_DEBUG([{event, enqueue_read_range}, {module, ?MODULE}, {args, Args2}]),
	TaskQueue = queue:in(Args2, Worker#worker_tasks.task_queue),
	Worker2 = Worker#worker_tasks{task_queue = TaskQueue},
	case End2 == End of
		true ->
			Worker2;
		false ->
			Args3 = {End2, End, OriginStoreID, TargetStoreID},
			do_enqueue_read_range(Args3, Worker2)
	end.

process_queues(State) ->
	Workers = State#state.workers,
	UpdatedWorkers = maps:map(
		fun(_Key, Worker) ->
			process_queue(Worker)
		end,
		Workers
	),
	State#state{workers = UpdatedWorkers}.

process_queue(Worker) ->
	case Worker#worker_tasks.active_count < ?MAX_ACTIVE_TASKS of
		true ->
			case queue:out(Worker#worker_tasks.task_queue) of
				{empty, _} ->
					Worker;
				{{value, Args}, Q2}->
					?LOG_DEBUG([{event, process_queue}, {module, ?MODULE},
						{active_count, Worker#worker_tasks.active_count}, {args, Args}]),
					gen_server:cast(Worker#worker_tasks.worker, {read_range, Args}),
					Worker2 = Worker#worker_tasks{
						task_queue = Q2,
						active_count = Worker#worker_tasks.active_count + 1
					},
					process_queue(Worker2)
			end;
		false ->
			Worker
	end.

task_completed(Args, State) ->
	{_Start, _End, OriginStoreID, _TargetStoreID} = Args,
	Worker = maps:get(OriginStoreID, State#state.workers, undefined),
	case Worker of
		undefined ->
			?LOG_ERROR([{event, worker_not_found}, {module, ?MODULE}, {call, task_completed},
				{store_id, OriginStoreID}]),
			State;
		_ ->
			?LOG_DEBUG([{event, task_completed}, {module, ?MODULE},
				{worker, Worker#worker_tasks.worker},
				{active_count, Worker#worker_tasks.active_count}, {args, Args}]),
			ActiveCount = Worker#worker_tasks.active_count - 1,
			Worker2 = Worker#worker_tasks{active_count = ActiveCount},
			Worker3 = process_queue(Worker2),
			State2 = State#state{
				workers = maps:put(OriginStoreID, Worker3, State#state.workers)
			},
			State2
	end.

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_ready_for_work/0},
		{timeout, 30, fun test_enqueue_read_range/0},
		{timeout, 30, fun test_process_queue/0}
	].

test_ready_for_work() ->
	State = #state{
		workers = #{
			"store1" => #worker_tasks{
				task_queue = queue:from_list(lists:seq(1, ?MAX_QUEUED_TASKS - 1))},
			"store2" => #worker_tasks{
				task_queue = queue:from_list(lists:seq(1, ?MAX_QUEUED_TASKS))}
		}
	},
	?assertEqual(true, do_ready_for_work("store1", State)),
	?assertEqual(false, do_ready_for_work("store2", State)).

test_enqueue_read_range() ->
	ExpectedWorker = #worker_tasks{
		task_queue = queue:from_list(
					[{
						floor(2.5 * ?DATA_CHUNK_SIZE),
						floor((2.5 + ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						"store1", "store2"
					},
					{
						floor((2.5 + ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						floor((2.5 + 2 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						"store1", "store2"
					},
					{
						floor((2.5 + 2 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						floor((2.5 + 3 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
						"store1", "store2"
					}]
				)
			},
	Worker = do_enqueue_read_range(
		{
			floor(2.5 * ?DATA_CHUNK_SIZE),
			floor((2.5 + 3 * ?READ_RANGE_CHUNKS) * ?DATA_CHUNK_SIZE),
			"store1", "store2"
		},
		#worker_tasks{task_queue = queue:new()}
	),
	?assertEqual(
		queue:to_list(ExpectedWorker#worker_tasks.task_queue),
		queue:to_list(Worker#worker_tasks.task_queue)).

test_process_queue() ->
	Worker1 = #worker_tasks{
		active_count = ?MAX_ACTIVE_TASKS
	},
	?assertEqual(Worker1, process_queue(Worker1)),

	Worker2 = #worker_tasks{
		active_count = ?MAX_ACTIVE_TASKS + 1
	},
	?assertEqual(Worker2, process_queue(Worker2)),

	Worker3 = process_queue(
		#worker_tasks{
			active_count = ?MAX_ACTIVE_TASKS - 2,
			task_queue = queue:from_list(
				[{floor(2.5 * ?DATA_CHUNK_SIZE), floor(12.5 * ?DATA_CHUNK_SIZE),
				"store1", "store2"},
			{floor(12.5 * ?DATA_CHUNK_SIZE), floor(22.5 * ?DATA_CHUNK_SIZE),
				"store1", "store2"},
			{floor(22.5 * ?DATA_CHUNK_SIZE), floor(30 * ?DATA_CHUNK_SIZE),
				"store1", "store2"}])
		}
	),
	ExpectedWorker3 = #worker_tasks{
		active_count = ?MAX_ACTIVE_TASKS,
		task_queue = queue:from_list(
			[{floor(22.5 * ?DATA_CHUNK_SIZE), floor(30 * ?DATA_CHUNK_SIZE),
				"store1", "store2"}]
		)
	},
	?assertEqual(
		ExpectedWorker3#worker_tasks.active_count, Worker3#worker_tasks.active_count),
	?assertEqual(
		queue:to_list(ExpectedWorker3#worker_tasks.task_queue),
		queue:to_list(Worker3#worker_tasks.task_queue)).

