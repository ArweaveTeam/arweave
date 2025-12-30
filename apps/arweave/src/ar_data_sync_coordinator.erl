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
	known_peers = #{}           %% #{Peer => Pid} - cached peer worker Pids
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
	?LOG_INFO([{event, init}, {module, ?MODULE}, {workers, Workers}]),
	{ok, #state{
		workers = queue:from_list(Workers)
	}}.

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
					%% Enqueue task to peer worker (fire-and-forget)
					ar_peer_worker:enqueue_task(Pid, FootprintKey, Args),
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
	Tasks = ar_peer_worker:process_queue(Pid, WorkerCount),
	dispatch_tasks(Pid, Tasks, State).

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
	RemovedCount = ar_peer_worker:rebalance(Pid, Performance, RebalanceParams),
	State2 = State#state{ total_task_count = max(0, State#state.total_task_count - RemovedCount) },
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
	case maps:find(Peer, State#state.known_peers) of
		{ok, Pid} -> 
			{Pid, State};
		error -> 
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

%%%===================================================================
%%% Tests.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_get_worker/0}
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
