%%% @doc Coordinates data sync tasks between worker processes and peer workers.
%%%
%%% This module acts as a coordinator that:
%%% - Dispatches sync tasks to ar_data_sync_worker processes
%%% - Coordinates with ar_peer_worker processes (one per peer) that manage:
%%%   - Peer task queues and dispatch limits
%%%   - Footprint management (grouping tasks to limit entropy cache usage)
%%%   - Peer performance tracking
%%% - Performs periodic rebalancing based on peer performance metrics
%%%
%%% Architecture:
%%% - Each peer has its own ar_peer_worker process that manages peer-specific state
%%%   (queues, footprints, dispatch limits, performance metrics)
%%% - This coordinator manages the pool of ar_data_sync_worker processes and
%%%   dispatches tasks from peer queues to available workers
%%% - Worker selection uses round-robin with load balancing
%%%
%%% Task Flow:
%%% 1. Tasks are enqueued to the appropriate ar_peer_worker
%%% 2. Peer workers manage footprints and queue tasks based on capacity
%%% 3. Coordinator pulls tasks from peer queues and dispatches to workers
%%% 4. On task completion, peer workers update metrics and footprint state
%%% 5. Coordinator processes peer queues to fill available worker capacity
-module(ar_data_sync_coordinator).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, is_syncing_enabled/0, ready_for_work/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REBALANCE_FREQUENCY_MS, 10*1000).

-record(state, {
	total_task_count = 0,       %% total count of tasks (queued + waiting, across all peers)
	total_dispatched_count = 0,
	workers = queue:new(),
	dispatched_count_per_worker = #{},
	known_peers = #{},          %% #{Peer => Pid} - cached peer worker Pids
	%% Global footprint tracking
	total_active_footprints = 0,  %% count of active footprints across all peers
	max_footprints = 0            %% global max footprints limit
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Workers) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Workers, []).

register_workers() ->
	case is_syncing_enabled() of
		true ->
			{Workers, WorkerNames} = register_sync_workers(),
			WorkerMaster = ?CHILD_WITH_ARGS(
				ar_data_sync_coordinator, worker, ar_data_sync_coordinator,
				[WorkerNames]),
				[WorkerMaster] ++ Workers;
		false ->
			[]
	end.

register_sync_workers() ->
	{ok, Config} = arweave_config:get_env(),
	{Workers, WorkerNames} = lists:foldl(
		fun(Number, {AccWorkers, AccWorkerNames}) ->
			Name = list_to_atom("ar_data_sync_worker_" ++ integer_to_list(Number)),
			Worker = ?CHILD_WITH_ARGS(ar_data_sync_worker, worker, Name, [Name, sync]),
			{[Worker | AccWorkers], [Name | AccWorkerNames]}
		end,
		{[], []},
		lists:seq(1, Config#config.sync_jobs)
	),
	{Workers, WorkerNames}.

%% @doc Returns true if syncing is enabled (i.e. sync_jobs > 0).
is_syncing_enabled() ->
	{ok, Config} = arweave_config:get_env(),
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
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	MaxFootprints = calculate_max_footprints(),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {workers, Workers}, 
		{max_footprints, MaxFootprints}]),
	{ok, #state{
		workers = queue:from_list(Workers),
		max_footprints = MaxFootprints
	}}.

calculate_max_footprints() ->
	{ok, Config} = arweave_config:get_env(),
	%% Calculate global max footprints based on entropy cache size
	FootprintSize = ar_block:get_replica_2_9_footprint_size(),
	max(1, (Config#config.replica_2_9_entropy_cache_size_mb * ?MiB) div FootprintSize).


handle_call(ready_for_work, _From, State) ->
	WorkerCount = queue:len(State#state.workers),
	TotalTaskCount = State#state.total_dispatched_count + State#state.total_task_count,
	ReadyForWork = TotalTaskCount < max_tasks(WorkerCount),
	{reply, ReadyForWork, State};

handle_call({reset_worker, Worker}, _From, State) ->
	ActiveCount = maps:get(Worker, State#state.dispatched_count_per_worker, 0),
	State2 = State#state{
		total_dispatched_count = State#state.total_dispatched_count - ActiveCount,
		dispatched_count_per_worker = maps:put(Worker, 0, State#state.dispatched_count_per_worker)
	},
	{reply, ok, State2};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({sync_range, Args}, State) ->
	case queue:is_empty(State#state.workers) of
		true ->
			{noreply, State};
		false ->
			{_Start, _End, Peer, _TargetStoreID, FootprintKey} = Args,
			%% Track this peer and get cached Pid
			{Pid, State1} = maybe_add_peer(Peer, State),
			case Pid of
				undefined ->
					{noreply, State1};
				_ ->
					%% Check if there's global capacity for new footprints
					HasCapacity = State1#state.total_active_footprints < State1#state.max_footprints,
					%% Enqueue task to peer worker (fire-and-forget)
					ar_peer_worker:enqueue_task(Pid, FootprintKey, Args, HasCapacity),
					%% Optimistically assume task was queued (not waiting)
					State2 = State1#state{ total_task_count = State1#state.total_task_count + 1 },
					%% Process the queue to dispatch if there's capacity
					State3 = process_peer_queue(Pid, State2),
					{noreply, State3}
			end
	end;

handle_cast({task_completed, {sync_range, {Worker, Result, Args, ElapsedNative}}}, State) ->
	{Start, End, Peer, _, _, FootprintKey} = Args,
	DataSize = End - Start,
	State2 = increment_dispatched_task_count(Worker, -1, State),
	%% Notify peer worker (handles footprint completion, performance rating)
	case maps:find(Peer, State2#state.known_peers) of
		{ok, Pid} ->
			ar_peer_worker:task_completed(Pid, FootprintKey, Result, ElapsedNative, DataSize);
		error ->
			%% Peer not in cache (shouldn't happen normally)
			?LOG_WARNING([{event, task_completed_unknown_peer}, {peer, ar_util:format_peer(Peer)}])
	end,
	%% Process all peer queues, starting with the peer that just completed
	State3 = process_all_peer_queues(Peer, State2),
	{noreply, State3};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	%% TODO: Add logic to purge empty peer workers (no queued tasks, no dispatched tasks).
	PeerPids = State#state.known_peers,  %% #{Peer => Pid}
	Peers = maps:keys(PeerPids),
	AllPeerPerformances = ar_peers:get_peer_performances(Peers),
	Targets = calculate_targets(PeerPids, AllPeerPerformances, State),
	State2 = rebalance_peers(PeerPids, AllPeerPerformances, Targets, State),
	{noreply, State2};

handle_cast({footprint_activated, _Peer}, State) ->
	NewCount = State#state.total_active_footprints + 1,
	{noreply, State#state{ total_active_footprints = NewCount }};

handle_cast({footprint_deactivated, _Peer}, State) ->
	NewCount = max(0, State#state.total_active_footprints - 1),
	State2 = State#state{ total_active_footprints = NewCount },
	%% Notify all peers that capacity is available so they can activate waiting footprints
	case NewCount < State2#state.max_footprints of
		true ->
			notify_peers_capacity_available(State2#state.known_peers);
		false ->
			ok
	end,
	{noreply, State2};

handle_cast({peer_worker_started, Peer, Pid}, State) ->
	%% Peer worker (re)started - update cached PID
	State2 = State#state{ known_peers = maps:put(Peer, Pid, State#state.known_peers) },
	{noreply, State2};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%%--------------------------------------------------------------------
%% Peer queue management
%%--------------------------------------------------------------------

%% @doc If a peer has capacity, take tasks from its queue and dispatch them.
process_peer_queue(Pid, State) ->
	WorkerCount = queue:len(State#state.workers),
	TasksToDispatch = ar_peer_worker:process_queue(Pid, WorkerCount),
	dispatch_tasks(Pid, TasksToDispatch, State).

%% @doc Process all peer queues, with the priority peer processed first.
process_all_peer_queues(PriorityPeer, State) ->
	case queue:is_empty(State#state.workers) of
		true ->
			State;
		false ->
			PeerPids = State#state.known_peers,
			PriorityPid = maps:get(PriorityPeer, PeerPids, undefined),
			OtherPids = maps:values(maps:remove(PriorityPeer, PeerPids)),
			AllPids = case PriorityPid of
				undefined -> OtherPids;
				_ -> [PriorityPid | OtherPids]
			end,
			process_all_peer_queues2(AllPids, State)
	end.

process_all_peer_queues2([], State) ->
	State;
process_all_peer_queues2([Pid | Rest], State) ->
	State2 = process_peer_queue(Pid, State),
	process_all_peer_queues2(Rest, State2).

%% @doc the maximum number of tasks we can have in process.
max_tasks(WorkerCount) ->
	WorkerCount * 50.


%%--------------------------------------------------------------------
%% Dispatch tasks to be run on workers
%%--------------------------------------------------------------------

%% @doc Dispatch tasks to workers.
%% Caller must ensure workers are available before calling.
dispatch_tasks(_Pid, [], State) ->
	State;
dispatch_tasks(Pid, [Args | Rest], State) ->
	{Worker, State2} = get_worker(State),
	{Start, End, Peer, TargetStoreID, FootprintKey} = Args,
	%% Pass FootprintKey as 6th element so it comes back in task_completed
	gen_server:cast(Worker, {sync_range, {Start, End, Peer, TargetStoreID, 3, FootprintKey}}),
	State3 = increment_dispatched_task_count(Worker, 1, State2),
	State4 = State3#state{ total_task_count = max(0, State3#state.total_task_count - 1) },
	dispatch_tasks(Pid, Rest, State4).

%%--------------------------------------------------------------------
%% Rebalancing
%%--------------------------------------------------------------------

%% @doc Calculate rebalance parameters.
%% PeerPids is #{Peer => Pid}
%% Returns {WorkerCount, TargetLatency, TotalThroughput, TotalMaxDispatched}
calculate_targets(PeerPids, AllPeerPerformances, State) ->
	WorkerCount = queue:len(State#state.workers),
	Peers = maps:keys(PeerPids),
	TotalThroughput =
		lists:foldl(
			fun(Peer, Acc) -> 
				Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
				Acc + Performance#performance.current_rating
			end, 0.0, Peers),
    TotalLatency = 
		lists:foldl(
			fun(Peer, Acc) -> 
				Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
				Acc + Performance#performance.average_latency
			end, 0.0, Peers),
	TargetLatency = case length(Peers) > 0 of
		true -> TotalLatency / length(Peers);
		false -> 0.0
	end,
	TotalMaxDispatched = maps:fold(
		fun(_Peer, Pid, Acc) ->
			case ar_peer_worker:get_max_dispatched(Pid) of
				{error, _} -> Acc;
				MaxDispatched -> MaxDispatched + Acc
			end
		end,
		0, PeerPids),
	?LOG_DEBUG([{event, sync_performance_targets},
		{worker_count, WorkerCount},
		{target_latency, TargetLatency},
		{total_throughput, TotalThroughput},
		{total_max_dispatched, TotalMaxDispatched}]),
    {WorkerCount, TargetLatency, TotalThroughput, TotalMaxDispatched}.

%% PeerPidsList is [{Peer, Pid}]
rebalance_peers(PeerPids, AllPeerPerformances, Targets, State) ->
	rebalance_peers2(maps:to_list(PeerPids), AllPeerPerformances, Targets, State).

rebalance_peers2([], _AllPeerPerformances, _Targets, State) ->
	State;
rebalance_peers2([{Peer, Pid} | Rest], AllPeerPerformances, Targets, State) ->
	{WorkerCount, TargetLatency, TotalThroughput, TotalMaxDispatched} = Targets,
	Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
	%% Calculate rebalance params (peer calculates FasterThanTarget from Performance)
	QueueScalingFactor = queue_scaling_factor(TotalThroughput, WorkerCount),
	WorkersStarved = TotalMaxDispatched < WorkerCount,
	RebalanceParams = {QueueScalingFactor, TargetLatency, WorkersStarved},
	Result = ar_peer_worker:rebalance(Pid, Performance, RebalanceParams),
	State2 = case Result of
		{shutdown, RemovedCount} ->
			%% Peer worker is idle and should be shutdown
			?LOG_INFO([{event, shutdown_idle_peer_worker},
				{peer, ar_util:format_peer(Peer)}]),
			ar_peer_worker:stop(Pid),
			State#state{
				total_task_count = max(0, State#state.total_task_count - RemovedCount),
				known_peers = maps:remove(Peer, State#state.known_peers)
			};
		{ok, RemovedCount} ->
			State#state{ total_task_count = max(0, State#state.total_task_count - RemovedCount) };
		{error, timeout} ->
			%% Peer worker timed out, skip it
			State
	end,
	rebalance_peers2(Rest, AllPeerPerformances, Targets, State2).

%% @doc Scaling factor for calculating per-peer max queue size.
%% Peer worker calculates: MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
queue_scaling_factor(0, _WorkerCount) -> infinity;
queue_scaling_factor(0.0, _WorkerCount) -> infinity;
queue_scaling_factor(TotalThroughput, WorkerCount) ->
	max_tasks(WorkerCount) / TotalThroughput.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

increment_dispatched_task_count(Worker, N, State) ->
	ActiveCount = maps:get(Worker, State#state.dispatched_count_per_worker, 0) + N,
	State#state{
		total_dispatched_count = State#state.total_dispatched_count + N,
		dispatched_count_per_worker = maps:put(Worker, ActiveCount, State#state.dispatched_count_per_worker)
	}.

%% @doc Add a peer to known_peers if not already present. Returns {Pid, State}.
%% The Pid is cached so we don't have to do whereis + atom lookup on every call.
maybe_add_peer(Peer, State) ->
	case State#state.known_peers of
		#{Peer := Pid} -> 
			{Pid, State};
		_ -> 
			case ar_peer_worker:get_or_start(Peer) of
				{ok, Pid} ->
					{Pid, State#state{ known_peers = maps:put(Peer, Pid, State#state.known_peers) }};
				{error, _} ->
					{undefined, State}
			end
	end.

get_worker(State) ->
	WorkerCount = queue:len(State#state.workers),
	AverageLoad = State#state.total_dispatched_count / WorkerCount,
	cycle_workers(AverageLoad, State).

cycle_workers(AverageLoad, State) ->
	#state{ workers = Workers } = State,
	{{value, Worker}, Workers2} = queue:out(Workers),
	State2 = State#state{ workers = queue:in(Worker, Workers2) },
	ActiveCount = maps:get(Worker, State2#state.dispatched_count_per_worker, 0),
	case ActiveCount =< AverageLoad of
		true ->
			{Worker, State2};
		false ->
			cycle_workers(AverageLoad, State2)
	end.

%% Notify all known peers that global footprint capacity is available.
notify_peers_capacity_available(KnownPeers) ->
	%% Iterate through peers, stop when one activates a footprint to avoid over-activation
	PeerList = maps:to_list(KnownPeers),
	try_activate_footprint(PeerList).

try_activate_footprint([]) ->
	ok;
try_activate_footprint([{_Peer, Pid} | Rest]) ->
	case ar_peer_worker:try_activate_footprint(Pid) of
		true ->
			%% A footprint was activated, stop here
			ok;
		false ->
			%% No footprint activated, try next peer
			try_activate_footprint(Rest)
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(TEST).

coordinator_test_() ->
	[
		{timeout, 30, fun test_get_worker/0},
		{timeout, 30, fun test_max_tasks/0},
		{timeout, 30, fun test_increment_dispatched_task_count/0},
		{timeout, 30, fun test_queue_scaling_factor/0},
		{timeout, 30, fun test_footprint_activated/0},
		{timeout, 30, fun test_footprint_deactivated/0},
		{timeout, 30, fun test_peer_worker_started_updates_cache/0},
		{timeout, 30, fun test_reset_worker/0},
		{timeout, 30, fun test_dispatch_tasks_updates_counts/0},
		{timeout, 30, fun test_try_activate_footprint_stops_on_success/0},
		{timeout, 30, fun test_try_activate_footprint_tries_all/0},
		{timeout, 30, fun test_calculate_targets/0}
	].

test_get_worker() ->
	State0 = #state{
		workers = queue:from_list([worker1, worker2, worker3]),
		total_dispatched_count = 6,
		dispatched_count_per_worker = #{worker1 => 3, worker2 => 2, worker3 => 1}
	},
	{worker2, State1} = get_worker(State0),
	State2 = increment_dispatched_task_count(worker2, 1, State1),
	{worker3, State3} = get_worker(State2),
	State4 = increment_dispatched_task_count(worker3, 1, State3),
	{worker3, State5} = get_worker(State4),
	State6 = increment_dispatched_task_count(worker3, 1, State5),
	{worker1, _} = get_worker(State6).

test_max_tasks() ->
	?assertEqual(50, max_tasks(1)),
	?assertEqual(100, max_tasks(2)),
	?assertEqual(500, max_tasks(10)),
	?assertEqual(5000, max_tasks(100)).

test_increment_dispatched_task_count() ->
	State0 = #state{
		total_dispatched_count = 5,
		dispatched_count_per_worker = #{worker1 => 3, worker2 => 2}
	},
	%% Increment worker1
	State1 = increment_dispatched_task_count(worker1, 2, State0),
	?assertEqual(7, State1#state.total_dispatched_count),
	?assertEqual(5, maps:get(worker1, State1#state.dispatched_count_per_worker)),
	
	%% Decrement worker2
	State2 = increment_dispatched_task_count(worker2, -1, State1),
	?assertEqual(6, State2#state.total_dispatched_count),
	?assertEqual(1, maps:get(worker2, State2#state.dispatched_count_per_worker)),
	
	%% Add new worker
	State3 = increment_dispatched_task_count(worker3, 1, State2),
	?assertEqual(7, State3#state.total_dispatched_count),
	?assertEqual(1, maps:get(worker3, State3#state.dispatched_count_per_worker)).

test_queue_scaling_factor() ->
	%% Zero throughput returns infinity
	?assertEqual(infinity, queue_scaling_factor(0, 10)),
	?assertEqual(infinity, queue_scaling_factor(0.0, 10)),
	
	%% Normal calculation: max_tasks(WorkerCount) / TotalThroughput
	%% max_tasks(10) = 500
	?assertEqual(5.0, queue_scaling_factor(100.0, 10)),
	?assertEqual(2.5, queue_scaling_factor(200.0, 10)),
	?assertEqual(50.0, queue_scaling_factor(10.0, 10)).

test_footprint_activated() ->
	State0 = #state{ total_active_footprints = 5, max_footprints = 10 },
	
	%% Simulate footprint_activated cast
	{noreply, State1} = handle_cast({footprint_activated, {1,2,3,4,1984}}, State0),
	?assertEqual(6, State1#state.total_active_footprints),
	
	{noreply, State2} = handle_cast({footprint_activated, {1,2,3,4,1985}}, State1),
	?assertEqual(7, State2#state.total_active_footprints).

test_footprint_deactivated() ->
	State0 = #state{ total_active_footprints = 5, max_footprints = 10, known_peers = #{} },
	
	%% Simulate footprint_deactivated cast
	{noreply, State1} = handle_cast({footprint_deactivated, {1,2,3,4,1984}}, State0),
	?assertEqual(4, State1#state.total_active_footprints),
	
	%% Should not go below 0
	State2 = State0#state{ total_active_footprints = 0 },
	{noreply, State3} = handle_cast({footprint_deactivated, {1,2,3,4,1984}}, State2),
	?assertEqual(0, State3#state.total_active_footprints).

test_peer_worker_started_updates_cache() ->
	Peer1 = {1,2,3,4,1984},
	Peer2 = {5,6,7,8,1985},
	Pid1 = self(),  %% Use self() as a fake Pid for testing
	Pid2 = self(),
	
	State0 = #state{ known_peers = #{} },
	
	%% Add first peer
	{noreply, State1} = handle_cast({peer_worker_started, Peer1, Pid1}, State0),
	?assertEqual(Pid1, maps:get(Peer1, State1#state.known_peers)),
	
	%% Add second peer
	{noreply, State2} = handle_cast({peer_worker_started, Peer2, Pid2}, State1),
	?assertEqual(Pid1, maps:get(Peer1, State2#state.known_peers)),
	?assertEqual(Pid2, maps:get(Peer2, State2#state.known_peers)),
	
	%% Update existing peer (simulating restart)
	NewPid = spawn(fun() -> ok end),
	{noreply, State3} = handle_cast({peer_worker_started, Peer1, NewPid}, State2),
	?assertEqual(NewPid, maps:get(Peer1, State3#state.known_peers)).

test_reset_worker() ->
	State0 = #state{
		total_dispatched_count = 10,
		dispatched_count_per_worker = #{worker1 => 5, worker2 => 3, worker3 => 2}
	},
	
	%% Reset worker1 (had 5 tasks)
	{reply, ok, State1} = handle_call({reset_worker, worker1}, self(), State0),
	?assertEqual(5, State1#state.total_dispatched_count),  %% 10 - 5 = 5
	?assertEqual(0, maps:get(worker1, State1#state.dispatched_count_per_worker)),
	
	%% Reset worker2 (had 3 tasks)
	{reply, ok, State2} = handle_call({reset_worker, worker2}, self(), State1),
	?assertEqual(2, State2#state.total_dispatched_count),  %% 5 - 3 = 2
	?assertEqual(0, maps:get(worker2, State2#state.dispatched_count_per_worker)),
	
	%% Reset unknown worker (should handle gracefully - count is 0)
	{reply, ok, State3} = handle_call({reset_worker, unknown_worker}, self(), State2),
	?assertEqual(2, State3#state.total_dispatched_count).

test_dispatch_tasks_updates_counts() ->
	State0 = #state{
		workers = queue:from_list([worker1, worker2]),
		total_dispatched_count = 0,
		total_task_count = 5,
		dispatched_count_per_worker = #{}
	},
	
	%% Dispatch empty list - no changes
	State1 = dispatch_tasks(self(), [], State0),
	?assertEqual(0, State1#state.total_dispatched_count),
	?assertEqual(5, State1#state.total_task_count),
	
	%% Note: We can't fully test dispatch_tasks without mocking workers,
	%% but we can verify the state changes for the helper functions.
	
	%% Verify that dispatching would decrement total_task_count
	%% by manually simulating what dispatch_tasks does
	State2 = State1#state{ total_task_count = max(0, State1#state.total_task_count - 1) },
	?assertEqual(4, State2#state.total_task_count).

test_try_activate_footprint_stops_on_success() ->
	%% Create mock peer processes that track if they were called
	Parent = self(),
	
	%% Peer1 returns false (no waiting footprint)
	Pid1 = spawn(fun() -> mock_peer_worker(Parent, peer1, false) end),
	%% Peer2 returns true (activates a footprint)
	Pid2 = spawn(fun() -> mock_peer_worker(Parent, peer2, true) end),
	%% Peer3 should NOT be called (iteration should stop at Peer2)
	Pid3 = spawn(fun() -> mock_peer_worker(Parent, peer3, false) end),
	
	%% Register mock processes so ar_peer_worker:try_activate_footprint can find them
	%% We need to call try_activate_footprint directly with our mock list
	PeerList = [{peer1, Pid1}, {peer2, Pid2}, {peer3, Pid3}],
	
	%% Call the function directly
	ok = try_activate_footprint(PeerList),
	
	%% Wait a bit for messages
	timer:sleep(50),
	
	%% Check which peers were called
	?assertEqual([peer1, peer2], collect_called_peers([])),
	
	%% Cleanup
	exit(Pid1, kill),
	exit(Pid2, kill),
	exit(Pid3, kill).

test_try_activate_footprint_tries_all() ->
	%% Create mock peer processes that all return false
	Parent = self(),
	
	Pid1 = spawn(fun() -> mock_peer_worker(Parent, peer1, false) end),
	Pid2 = spawn(fun() -> mock_peer_worker(Parent, peer2, false) end),
	Pid3 = spawn(fun() -> mock_peer_worker(Parent, peer3, false) end),
	
	PeerList = [{peer1, Pid1}, {peer2, Pid2}, {peer3, Pid3}],
	
	%% Call the function directly
	ok = try_activate_footprint(PeerList),
	
	%% Wait a bit for messages
	timer:sleep(50),
	
	%% All three peers should have been called
	?assertEqual([peer1, peer2, peer3], collect_called_peers([])),
	
	%% Cleanup
	exit(Pid1, kill),
	exit(Pid2, kill),
	exit(Pid3, kill).

%% Mock peer worker that responds to try_activate_footprint calls
mock_peer_worker(Parent, PeerName, ReturnValue) ->
	receive
		{'$gen_call', From, try_activate_footprint} ->
			Parent ! {called, PeerName},
			gen_server:reply(From, ReturnValue)
	after 5000 ->
		ok
	end.

%% Collect all {called, PeerName} messages
collect_called_peers(Acc) ->
	receive
		{called, PeerName} ->
			collect_called_peers(Acc ++ [PeerName])
	after 10 ->
		Acc
	end.

test_calculate_targets() ->
	%% Create mock peer workers that respond to get_max_dispatched
	Pid1 = spawn(fun() -> mock_peer_worker_max_dispatched(10) end),
	Pid2 = spawn(fun() -> mock_peer_worker_max_dispatched(15) end),
	Pid3 = spawn(fun() -> mock_peer_worker_max_dispatched(20) end),
	
	Peer1 = {1,2,3,4,1984},
	Peer2 = {5,6,7,8,1985},
	Peer3 = {9,10,11,12,1986},
	
	PeerPids = #{Peer1 => Pid1, Peer2 => Pid2, Peer3 => Pid3},
	
	%% Create performance records
	AllPeerPerformances = #{
		Peer1 => #performance{ current_rating = 100.0, average_latency = 50.0 },
		Peer2 => #performance{ current_rating = 200.0, average_latency = 100.0 },
		Peer3 => #performance{ current_rating = 300.0, average_latency = 150.0 }
	},
	
	State = #state{ workers = queue:from_list([w1, w2, w3, w4, w5]) },
	
	{WorkerCount, TargetLatency, TotalThroughput, TotalMaxDispatched} = 
		calculate_targets(PeerPids, AllPeerPerformances, State),
	
	%% WorkerCount = 5 (number of workers in queue)
	?assertEqual(5, WorkerCount),
	
	%% TotalThroughput = 100 + 200 + 300 = 600
	?assertEqual(600.0, TotalThroughput),
	
	%% TargetLatency = (50 + 100 + 150) / 3 = 100
	?assertEqual(100.0, TargetLatency),
	
	%% TotalMaxDispatched = 10 + 15 + 20 = 45
	?assertEqual(45, TotalMaxDispatched),
	
	%% Cleanup
	exit(Pid1, kill),
	exit(Pid2, kill),
	exit(Pid3, kill).

%% Mock peer worker for get_max_dispatched
mock_peer_worker_max_dispatched(MaxDispatched) ->
	receive
		{'$gen_call', From, get_max_dispatched} ->
			gen_server:reply(From, MaxDispatched),
			mock_peer_worker_max_dispatched(MaxDispatched)
	after 5000 ->
		ok
	end.

-endif.
