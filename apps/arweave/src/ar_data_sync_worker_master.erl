%%% @doc The module maintains a queue of processes fetching data from the network
%%% and from the local storage modules.
-module(ar_data_sync_worker_master).

-behaviour(gen_server).

-export([start_link/2, get_scheduled_task_count/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	task_queue = queue:new(),
	task_queue_len = 0,
	all_workers = queue:new(),
	peer_workers = #{},
	worker_count = 0,
	workers_per_peer = 8
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, Workers) ->
	gen_server:start_link({local, Name}, ?MODULE, Workers, []).

%% @doc Return the number of scheduled tasks.
get_scheduled_task_count() ->
	case ets:lookup(?MODULE, scheduled_tasks) of
		[] ->
			0;
		[{_, N}] ->
			N
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, process_task_queue),
	{ok, #state{ all_workers = queue:from_list(Workers), worker_count = length(Workers) }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(process_task_queue, #state{ task_queue_len = 0 } = State) ->
	ar_util:cast_after(200, ?MODULE, process_task_queue),
	{noreply, State};
handle_cast(process_task_queue, #state{ worker_count = WorkerCount } = State) ->
	ScheduledTaskCount = get_scheduled_task_count(),
	TaskBudget = max(0, WorkerCount * 1000 - ScheduledTaskCount),
	ar_util:cast_after(200, ?MODULE, process_task_queue),
	{noreply, process_task_queue(TaskBudget, State)};

handle_cast({read_range, _Args}, #state{ worker_count = 0 } = State) ->
	{noreply, State};
handle_cast({read_range, Args}, #state{ task_queue = Q, task_queue_len = Len } = State) ->
	{noreply, State#state{ task_queue = queue:in({read_range, Args}, Q),
			task_queue_len = Len + 1 }};

handle_cast({sync_range, _Args}, #state{ worker_count = 0 } = State) ->
	{noreply, State};
handle_cast({sync_range, Args}, #state{ task_queue = Q, task_queue_len = Len } = State) ->
	{noreply, State#state{ task_queue = queue:in({sync_range, Args}, Q),
			task_queue_len = Len + 1 }};

handle_cast(task_completed, State) ->
	ets:update_counter(?MODULE, scheduled_tasks, {2, -1}),
	prometheus_gauge:dec(scheduled_sync_tasks),
	{noreply, State};

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

process_task_queue(0, State) ->
	State;
process_task_queue(_N, #state{ task_queue_len = 0 } = State) ->
	State;
process_task_queue(N, State) ->
	#state{ task_queue = Q, task_queue_len = Len, all_workers = WorkerQ } = State,
	{{value, {Task, Args}}, Q2} = queue:out(Q),
	case Task of
		read_range ->
			{Start, End, OriginStoreID, TargetStoreID, SkipSmall} = Args,
			End2 = min(Start + 10 * 262144, End),
			{{value, W}, WorkerQ2} = queue:out(WorkerQ),
			ets:update_counter(?MODULE, scheduled_tasks, {2, 1}, {scheduled_tasks, 0}),
			prometheus_gauge:inc(scheduled_sync_tasks),
			gen_server:cast(W, {read_range, {Start, End2, OriginStoreID, TargetStoreID,
					SkipSmall}}),
			WorkerQ3 = queue:in(W, WorkerQ2),
			{Q3, Len2} =
				case End2 == End of
					true ->
						{Q2, Len - 1};
					false ->
						Args2 = {End2, End, OriginStoreID, TargetStoreID, SkipSmall},
						{queue:in_r({read_range, Args2}, Q2), Len}
				end,
			State2 = State#state{ all_workers = WorkerQ3, task_queue = Q3, task_queue_len = Len2 },
			process_task_queue(N - 1, State2);
		sync_range ->
			{Start, End, Peer, TargetStoreID} = Args,
			{Worker, State2} = get_peer_worker(Peer, State),
			ets:update_counter(?MODULE, scheduled_tasks, {2, 1}, {scheduled_tasks, 0}),
			prometheus_gauge:inc(scheduled_sync_tasks),
			gen_server:cast(Worker, {sync_range, {Start, End, Peer, TargetStoreID, 3}}),
			State3 = State2#state{ task_queue = Q2, task_queue_len = Len - 1 },
			process_task_queue(N - 1, State3)
	end.

get_peer_worker(Peer, #state{
			all_workers = AllWorkerQ,
			peer_workers = PeerWorkers,
			workers_per_peer = WorkersPerPeer
		} = State) ->
	{PeerWorkerQ2, AllWorkerQ2} = case maps:get(Peer, PeerWorkers, undefined) of
		undefined ->
			initialize_peer_worker_queue(WorkersPerPeer, Peer, queue:new(), AllWorkerQ);
		PeerWorkerQ ->
			{PeerWorkerQ, AllWorkerQ}
	end,
	{{value, Worker}, PeerWorkerQ3} = queue:out(PeerWorkerQ2),
	State2 = State#state{
		all_workers = AllWorkerQ2,
		peer_workers = maps:put(Peer, queue:in(Worker, PeerWorkerQ3), PeerWorkers)
	},
	{ Worker, State2 }.

initialize_peer_worker_queue(0, _Peer, PeerWorkerQ, AllWorkerQ) ->
	{PeerWorkerQ, AllWorkerQ};
initialize_peer_worker_queue(N, Peer, PeerWorkerQ, AllWorkerQ) ->
	{{value, Worker}, AllWorkerQ2} = queue:out(AllWorkerQ),
	initialize_peer_worker_queue(
		N-1, Peer, queue:in(Worker, PeerWorkerQ), queue:in(Worker, AllWorkerQ2)).

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

get_peer_worker_test_() ->
	{timeout, 60, fun test_get_peer_worker/0}.

test_get_peer_worker() ->
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {101, 102, 103, 104, 1984},
	Peer3 = {201, 202, 203, 204, 1984},
	Workers = lists:seq(1, 8),
	State0 = #state{ 
		all_workers = queue:from_list(Workers),
		workers_per_peer = 3
	},
	{Worker1, State1} = get_peer_worker(Peer1, State0),
	assert_state(Peer1,
		1, [4, 5, 6, 7, 8, 1, 2, 3], [2, 3, 1],
		Worker1, State1),

	{Worker2, State2} = get_peer_worker(Peer1, State1),
	assert_state(Peer1,
		2, [4, 5, 6, 7, 8, 1, 2, 3], [3, 1, 2],
		Worker2, State2),

	{Worker3, State3} = get_peer_worker(Peer2, State2),
	assert_state(Peer2,
		4, [7, 8, 1, 2, 3, 4, 5, 6], [5, 6, 4],
		Worker3, State3),
	
	{Worker4, State4} = get_peer_worker(Peer1, State3),
	assert_state(Peer1,
		3, [7, 8, 1, 2, 3, 4, 5, 6], [1, 2, 3],
		Worker4, State4),

	{Worker5, State5} = get_peer_worker(Peer1, State4),
	assert_state(Peer1,
		1, [7, 8, 1, 2, 3, 4, 5, 6], [2, 3, 1],
		Worker5, State5),

	{Worker6, State6} = get_peer_worker(Peer3, State5),
	assert_state(Peer3,
		7, [2, 3, 4, 5, 6, 7, 8, 1], [8, 1, 7],
		Worker6, State6),

	{Worker7, State7} = get_peer_worker(Peer3, State6),
	assert_state(Peer3,
		8, [2, 3, 4, 5, 6, 7, 8, 1], [1, 7, 8],
		Worker7, State7),

	{Worker8, State8} = get_peer_worker(Peer3, State7),
	assert_state(Peer3,
		1, [2, 3, 4, 5, 6, 7, 8, 1], [7, 8, 1],
		Worker8, State8),

	{Worker9, State9} = get_peer_worker(Peer3, State8),
	assert_state(Peer3,
		7, [2, 3, 4, 5, 6, 7, 8, 1], [8, 1, 7],
		Worker9, State9).

assert_state(Peer, ExpectedWorker, ExpectedAllWorkers, ExpectedPeerWorkers, Worker, State) ->
	PeerWorkers = maps:get(Peer, State#state.peer_workers, undefined),
	?assertEqual(ExpectedWorker, Worker),
	?assertEqual(ExpectedAllWorkers, queue:to_list(State#state.all_workers)),
	?assertEqual(ExpectedPeerWorkers, queue:to_list(PeerWorkers)).


% process_task_queue_test_() ->
% 	{timeout, 60, fun test_process_task_queue/0}.

% test_process_task_queue() ->
% 	TaskQueue = queue:from_list([{sync_range, {0, 1, peer1, storage_module_1}}]),
% 	WorkerQueue = queue:from_list([worker1, worker2]),

% 	State = process_task_queue(0, #state{ task_queue = TaskQueue, task_queue_len = 1,
% 			all_workers = WorkerQueue, worker_count = 2 }),

% 	?LOG_ERROR("Task Queue: ~p", [queue:to_list(State#state.task_queue)]),
% 	?LOG_ERROR("Worker Queue: ~p", [queue:to_list(State#state.all_workers)]).

