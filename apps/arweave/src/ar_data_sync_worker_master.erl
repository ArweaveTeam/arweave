%%% @doc The module manages the ar_data_sync_workers used to sync chunks from peers. It attempts
%%% to optimize:
%%% - load per worker
%%% - load per peer
%%%   If this node makes too many requests to a peer it can be blocked or
%%%   throttled, also peer performance can vary over time and we want to avoid getting "stuck"
%%%   syncing a lot of chunks from a slow peer.
%%% - load per footprint
%%%   When syncing footprins of chunks to be unpacked we maintain an entropy
%%%   cache. To avoid overloading the entorpy cache we want to limit the number of footprints that
%%%   we sync chunks from at the same time.
%%%
%%% Task Flow:
%%% Tasks flow through the system in three stages: Footprint -> Peer -> Worker
%%%
%%% 1. Footprint Management:
%%%    - Tasks with a FootprintKey are grouped by footprint to limit concurrent
%%%      processing and avoid overloading the entropy cache
%%%    - Each footprint has a queue of pending tasks (those that have not been enqueued to
%%%      a peer queue yet) and a count of active tasks (those that have been enqueued to
%%%      a peer queue).
%%%    - When a footprint is active (active_count > 0), new tasks are immediately enqueued
%%%      to the peer queue
%%%    - When max_footprints is reached, new footprints are queued until capacity
%%%      becomes available
%%%
%%% 2. Peer Management:
%%%    - Each peer has a queue of pending tasks and tracks active task count
%%%    - Each peer has a max_active limit that controls how many tasks can be
%%%      concurrently scheduled for that peer
%%%    - When a peer has capacity (active_count < max_active) and queued tasks,
%%%      tasks are scheduled to workers
%%%
%%% 3. Worker Management:
%%%    - Workers are selected using round-robin with load balancing
%%%
%%% Task Completion:
%%% When a task completes:
%%% 1. Worker and peer active counts are decremented
%%% 2. Peer performance metrics are updated (EMA, latency)
%%% 3. All peer queues are processed (with the completing peer prioritized) to fill
%%%    available worker capacity
%%% 4. Footprint active count is decremented
%%% 5. If the footprint count reaches 0, the footprint is removed and the next
%%%    waiting footprint is activated
%%%
%%% Peer Rebalancing:
%%% Periodically peers are rebalanced:
%%% 1. Target latency and throughput are calculated from all peer performances
%%% 2. For each peer:
%%%    - Maximum allowed active tasks (#peer.max_active) is adjusted based on:
%%%      * Peer latency vs target latency (increase if faster, decrease if slower)
%%%      * Current active/queued task counts (prevents over-allocation)
%%%    - If needed, the queue of pending tasks is cut back to max_active - this allows the
%%%      node's data discovery processes to find new peers to sync chunks from without needing to
%%%      wait for previously enqueued tasks to be processed (which can take a long time if a peer
%%%      has gotten dramatically slower).
-module(ar_data_sync_worker_master).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, is_syncing_enabled/0, ready_for_work/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(REBALANCE_FREQUENCY_MS, 10*1000).
-define(MIN_MAX_ACTIVE, 8).
-define(MIN_PEER_QUEUE, 20).

-record(peer, {
	peer = undefined,
	pending = queue:new(),
	active_count = 0,
	max_active = ?MIN_MAX_ACTIVE
}).

-record(footprint, {
	queue = queue:new(),   %% queue of pending tasks
	active_count = 0       %% count of active tasks (0 = inactive)
}).

-record(state, {
	total_pending_count = 0,  %% total count of tasks queued in peer queues
	total_active_count = 0,
	total_footprint_pending_count = 0,
	workers = queue:new(),
	active_count_per_worker = #{},
	peers = #{},              %% Peer => #peer{}
	footprints = #{},         %% FootprintKey => #footprint{}
	max_footprints = 100      %% maximum concurrent footprints
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
				ar_data_sync_worker_master, worker, ar_data_sync_worker_master,
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
	{ok, Config} = arweave_config:get_env(),
	MaxFootprints = (Config#config.replica_2_9_entropy_cache_size_mb * ?MiB)
		div ar_block:get_replica_2_9_footprint_size(),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {workers, Workers},
		{max_footprints, MaxFootprints}]),
	{ok, #state{
		workers = queue:from_list(Workers),
		max_footprints = max(1, MaxFootprints)
	}}.

handle_call(ready_for_work, _From, State) ->
	WorkerCount = queue:len(State#state.workers),
	TotalTaskCount = 
		State#state.total_active_count +
		State#state.total_pending_count +
		State#state.total_footprint_pending_count,
	ReadyForWork = TotalTaskCount < max_tasks(WorkerCount),
	{reply, ReadyForWork, State};

handle_call({reset_worker, Worker}, _From, State) ->
	ActiveCount = maps:get(Worker, State#state.active_count_per_worker, 0),
	State2 = State#state{
		total_active_count = State#state.total_active_count - ActiveCount,
		active_count_per_worker = maps:put(Worker, 0, State#state.active_count_per_worker)
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
			{_Start, _End, _Peer, _TargetStoreID, FootprintKey} = Args,
			{noreply, enqueue_task(FootprintKey, Args, State)}
	end;

handle_cast({task_completed, {sync_range, {Worker, Result, Args, ElapsedNative}}}, State) ->
	{Start, End, Peer, _, _, FootprintKey} = Args,
	DataSize = End - Start,
	State2 = increment_active_task_count(Worker, Peer, -1, State),
	PeerTasks = get_peer_tasks(Peer, State2),
	{PeerTasks2, State3} = complete_task(
		PeerTasks, Result, ElapsedNative, DataSize, State2),
	State4 = set_peer_tasks(PeerTasks2, State3),
	%% Process all peer queues, starting with the peer that just completed
	State5 = process_all_peer_queues(Peer, State4),
	State6 = complete_footprint_task(FootprintKey, Peer, State5),
	{noreply, State6};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	State2 = purge_empty_peers(State),
	Peers = maps:keys(State2#state.peers),
	AllPeerPerformances = ar_peers:get_peer_performances(Peers),
	Targets = calculate_targets(Peers, AllPeerPerformances, State2),
	State3 = rebalance_peers(Peers, AllPeerPerformances, Targets, State2),
	{noreply, State3};

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

%% @doc If a peer has capacity, take the next task from its queue and schedule it.
process_peer_queue(PeerTasks, State) ->
	case queue:is_empty(State#state.workers) of
		true ->
			%% No workers available, can't schedule
			{PeerTasks, State};
		false ->
			case peer_has_capacity(PeerTasks) andalso peer_has_queued_tasks(PeerTasks) of
				true ->
					{PeerTasks2, Args} = dequeue_peer_task(PeerTasks),
					{PeerTasks3, State2} = dispatch_task(PeerTasks2, Args, State),
					process_peer_queue(PeerTasks3, State2);
				false ->
					{PeerTasks, State}
			end
	end.

%% @doc Process all peer queues, with the priority peer processed first.
%% This maintains locality (same peer/footprint) while ensuring worker capacity
%% is filled from all peers.
process_all_peer_queues(PriorityPeer, State) ->
	AllPeers = maps:to_list(maps:remove(PriorityPeer, State#state.peers)),
	%% Put priority peer at the front of the list
	PriorityPeerTask = maps:get(PriorityPeer, State#state.peers, undefined),
	PeerList = case PriorityPeerTask of
		undefined ->
			AllPeers;
		_ ->
			[{PriorityPeer, PriorityPeerTask} | AllPeers]
	end,
	process_all_peer_queues2(PeerList, State).

process_all_peer_queues2([], State) ->
	State;
process_all_peer_queues2([{_Peer, PeerTasks} | Rest], State) ->
	{PeerTasks2, State2} = process_peer_queue(PeerTasks, State),
	State3 = set_peer_tasks(PeerTasks2, State2),
	process_all_peer_queues2(Rest, State3).

%% @doc the maximum number of tasks we can have in process - including tasks queued here as well
%% as those active on ar_data_sync_workers.
max_tasks(WorkerCount) ->
	WorkerCount * 50.

%% @doc The maximum number of tasks we can have queued for a given peer.
max_peer_queue(_Peformance, 0, _WorkerCount) ->
	undefined;
max_peer_queue(_Performance, 0.0, _WorkerCount) ->
	undefined;
max_peer_queue(#performance{ current_rating = 0 } = _Performance, _TotalThroughput, _WorkerCount) ->
	undefined;
max_peer_queue(#performance{ current_rating = 0.0 } = _Performance, _TotalThroughput, _WorkerCount) ->
	undefined;
max_peer_queue(Performance, TotalThroughput, WorkerCount) ->
	%% estimate of of this peer's througput
	PeerThroughput = Performance#performance.current_rating,
	%% The maximum number of tasks we allow to be queued for this peer is related to its
	%% contribution to our current throughput. Peers with a higher throughput can claim more
	%% of the queue.
	%%
	%% We also allow all peers to maintain a small queue no matter what - this is to allow for
	%% them to recover from a temporary drop in throughput.
	max(trunc((PeerThroughput / TotalThroughput) * max_tasks(WorkerCount)), ?MIN_PEER_QUEUE).

%% @doc Cut a peer's queue to store roughly 15 minutes worth of tasks. This prevents
%% a slow peer from filling up the ar_data_sync_worker_master queues, stalling the
%% workers and preventing ar_data_sync from pushing new tasks.
cut_peer_queue(_MaxQueue, PeerTasks, #state{ total_active_count = 0 } = State) ->
	{PeerTasks, State};
cut_peer_queue(undefined, PeerTasks, State) ->
	{PeerTasks, State};
cut_peer_queue(MaxQueue, PeerTasks, State) ->
	Peer = PeerTasks#peer.peer,
	Pending = PeerTasks#peer.pending,
	case queue:len(Pending) - MaxQueue of
		TasksToCut when TasksToCut > 0 ->
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			{Pending2, _} = queue:split(MaxQueue, Pending),
			?LOG_DEBUG([{event, cut_peer_queue},
				{peer, ar_util:format_peer(Peer)},
				{pending_len, queue:len(Pending)},
				{active_count, PeerTasks#peer.active_count},
				{active_tasks, State#state.total_active_count},
				{max_queue, MaxQueue}, {tasks_to_cut, TasksToCut},
				{pending_len2, queue:len(Pending2)}
			]),
			{
				PeerTasks#peer{ pending = Pending2 },
				increment_pending_task_count(Peer, -TasksToCut, State)
			};
		_ ->
			{PeerTasks, State}
	end.

%% @doc Enqueue a task to a peer queue, process the queue, and update counters.
enqueue_peer_task(Peer, Args, State) ->
	{_Start, _End, Peer, _TargetStoreID, FootprintKey} = Args,
	PeerTasks = get_peer_tasks(Peer, State),
	Pending = queue:in(Args, PeerTasks#peer.pending),
	PeerTasks2 = PeerTasks#peer{ pending = Pending },
	{PeerTasks3, State2} = process_peer_queue(PeerTasks2, State),
	State3 = increment_pending_task_count(Peer, 1, State2),
	set_peer_tasks(PeerTasks3, State3).

dequeue_peer_task(PeerTasks) ->
	{{value, Args}, Pending} = queue:out(PeerTasks#peer.pending),
	PeerTasks2 = PeerTasks#peer{ pending = Pending },
	{PeerTasks2, Args}.

peer_has_capacity(PeerTasks) ->
	PeerTasks#peer.active_count < PeerTasks#peer.max_active.

peer_has_queued_tasks(PeerTasks) ->
	not queue:is_empty(PeerTasks#peer.pending).

%%--------------------------------------------------------------------
%% Footprint queue management
%%--------------------------------------------------------------------

%% @doc Enqueue a sync_range task, managing footprint concurrency limits.
%% For tasks with FootprintKey = none, enqueue directly to peer queue.
%% For tasks with a FootprintKey, limit concurrent footprints.
enqueue_task(none, Args, State) ->
	%% Non-footprint task, enqueue directly to peer queue
	{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
	enqueue_peer_task(Peer, Args, State);
enqueue_task(FootprintKey, Args, State) ->
	#state{ footprints = Footprints, max_footprints = MaxFootprints } = State,
	{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
	Footprint = maps:get(FootprintKey, Footprints, #footprint{}),
	ActiveFootprintCount = count_active_footprints(Footprints),
	IsActive = Footprint#footprint.active_count > 0,
	case IsActive of
		true ->
			%% Footprint is already active, enqueue directly to peer queue
			State2 = increment_footprint_active_task_count(FootprintKey, Peer, 1, State),
			enqueue_peer_task(Peer, Args, State2);
		false when ActiveFootprintCount < MaxFootprints ->
			%% Room for new footprint, activate it and enqueue directly to peer queue
			State2 = increment_footprint_active_task_count(FootprintKey, Peer, 1, State),
			enqueue_peer_task(Peer, Args, State2);
		false ->
			%% No room, queue the task for later
			State2 = increment_footprint_pending_task_count(Peer, 1, State),
			Queue = queue:in(Args, Footprint#footprint.queue),
			Footprint2 = Footprint#footprint{ queue = Queue },
			State2#state{ footprints = maps:put(FootprintKey, Footprint2, Footprints) }
	end.

%% @doc Count how many footprints are currently active (active_count > 0).
count_active_footprints(Footprints) ->
	maps:fold(
		fun(_Key, Footprint, Acc) ->
			case Footprint#footprint.active_count > 0 of
				true -> Acc + 1;
				false -> Acc
			end
		end, 0, Footprints).

%% @doc Handle completion of a footprint task.
%% Decrements the active active_count and activates a new footprint if this one is done.
complete_footprint_task(none, _Peer, State) ->
	State;
complete_footprint_task(FootprintKey, Peer, State) ->
	State2 = increment_footprint_active_task_count(FootprintKey, Peer, -1, State),
	IsFootprintDone = 
		case maps:get(FootprintKey, State2#state.footprints, none) of
			none ->
				true;
			Footprint ->
				Footprint#footprint.active_count =< 0
		end,
	case IsFootprintDone of
		true ->
			%% This footprint is done, remove it and try to activate a new one
			?LOG_DEBUG([{event, deactivate_footprint}, {footprint_key, FootprintKey}]),
			State3 = State2#state{
				footprints = maps:remove(FootprintKey, State2#state.footprints)
			},
			activate_next_footprint(State3);
		false ->
			State2
	end.

%% @doc Activate the next waiting footprint queue if any.
activate_next_footprint(State) ->
	#state{ footprints = Footprints, max_footprints = MaxFootprints } = State,
	ActiveCount = count_active_footprints(Footprints),
	%% Find a footprint with queued tasks
	NextFootprint = maps:fold(
		fun(Key, Footprint, Acc) ->
			case Acc of
				none ->
					case Footprint#footprint.active_count =< 0 andalso
					     not queue:is_empty(Footprint#footprint.queue) of
						true ->
							{Key, Footprint};
						false ->
							Acc
					end;
				_ ->
					Acc
			end
		end, none, Footprints),
	case ActiveCount >= MaxFootprints orelse NextFootprint == none of
		true ->
			State;
		false ->
			{FootprintKey, Footprint} = NextFootprint,
			?LOG_DEBUG([{event, activate_footprint},
				{count, ActiveCount + 1}, {footprint_key, FootprintKey}]),
			Footprint2 = Footprint#footprint{ active_count = 0, queue = queue:new() },
			State2 = State#state{
				footprints = maps:put(FootprintKey, Footprint2, Footprints)
			},
			%% Enqueue all tasks from this footprint queue directly to peer queues
			enqueue_footprint(FootprintKey, Footprint#footprint.queue, State2)
	end.

%% @doc Enqueue all tasks from a footprint queue directly to peer queues.
enqueue_footprint(FootprintKey, Queue, State) ->
	case queue:out(Queue) of
		{empty, _} ->
			State;
		{{value, Args}, Queue2} ->
			{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
			State2 = increment_footprint_active_task_count(FootprintKey, Peer, 1, State),
			State3 = increment_footprint_pending_task_count(Peer, -1, State2),
			State4 = enqueue_peer_task(Peer, Args, State3),
			enqueue_footprint(FootprintKey, Queue2, State4)
	end.

%%--------------------------------------------------------------------
%% Schedule tasks to be run on workers
%%--------------------------------------------------------------------

%% @doc Schedule a sync_range task from a peer queue.
dispatch_task(PeerTasks, Args, State) ->
	{Start, End, Peer, TargetStoreID, FootprintKey} = Args,
	{Worker, State2} = get_worker(State),
	%% Pass FootprintKey as 6th element so it comes back in task_completed
	gen_server:cast(Worker, {sync_range, {Start, End, Peer, TargetStoreID, 3, FootprintKey}}),
	State3 = increment_active_task_count(Worker, Peer, 1, State2),
	State4 = increment_pending_task_count(Peer, -1, State3),
	PeerTasks2 = PeerTasks#peer{ active_count = PeerTasks#peer.active_count + 1 },
	{PeerTasks2, State4}.

%%--------------------------------------------------------------------
%% Record a completed task and update related values (i.e.
%% EMA, max_active, peer queue length)
%%--------------------------------------------------------------------
complete_task(PeerTasks, Result, ElapsedNative, DataSize, State) ->
	PeerTasks2 = PeerTasks#peer{ 
		active_count = PeerTasks#peer.active_count - 1
	},
	ar_peers:rate_fetched_data(
		PeerTasks2#peer.peer, chunk, Result,
		erlang:convert_time_unit(ElapsedNative, native, microsecond), DataSize,
		PeerTasks2#peer.max_active),
	{PeerTasks2, State}.

calculate_targets(Peers, AllPeerPerformances, State) ->
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
	TotalMaxActive =
		maps:fold(
			fun(_Key, PeerTasks, Acc) ->
				PeerTasks#peer.max_active + Acc
			end,
		0, State#state.peers),
	?LOG_DEBUG([{event, sync_performance_targets},
		{target_latency, TargetLatency},
		{total_throughput, TotalThroughput},
		{total_max_active, TotalMaxActive}]),
    {TargetLatency, TotalThroughput, TotalMaxActive}.

purge_empty_peers(State) ->
	PurgedPeerTasks = maps:filter(
		fun(_Peer, PeerTasks) ->
			(not queue:is_empty(PeerTasks#peer.pending))
				orelse PeerTasks#peer.active_count > 0
		end,
		State#state.peers),
	State#state{ peers = PurgedPeerTasks }.

rebalance_peers([], _AllPeerPerformances, _Targets, State) ->
	State;
rebalance_peers([Peer | Peers], AllPeerPerformances, Targets, State) ->
	PeerTasks = maps:get(Peer, State#state.peers),
	Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
	{PeerTasks2, State2} = rebalance_peer(PeerTasks, Performance, Targets, State),
	State3 = set_peer_tasks(PeerTasks2, State2),
	rebalance_peers(Peers, AllPeerPerformances, Targets, State3).

rebalance_peer(PeerTasks, Performance, Targets, State) ->
	{TargetLatency, TotalThroughput, TotalMaxActive} = Targets,
	WorkerCount = queue:len(State#state.workers),
	MaxPeerQueue = max_peer_queue(Performance, TotalThroughput, WorkerCount),
	{PeerTasks2, State2} = cut_peer_queue(
		MaxPeerQueue,
		PeerTasks,
		State),
	PeerTasks3 = update_max_active(PeerTasks2, Performance, TotalMaxActive, TargetLatency, State2),
	?LOG_DEBUG([{event, rebalance_peer},
		{peer, ar_util:format_peer(PeerTasks#peer.peer)},
		{max_peer_queue, MaxPeerQueue},
		{max_active, PeerTasks3#peer.max_active}]),
	{PeerTasks3, State2}.

update_max_active(PeerTasks, Performance, TotalMaxActive, TargetLatency, State) ->
	%% Determine target max_active:
	%% 1. Increase max_active when the peer's average latency is less than the threshold
	%% 2. OR increase max_active when the workers are not fully utilized
	%% 3. Decrease max_active if the most recent request was slower than the threshold - this
	%%    allows us to respond more quickly to a sudden drop in performance
	%%
	%% Once we have the target max_active, find the maximum of the currently active tasks
	%% and queued tasks. The new max_active is the minimum of the target and that value.
	%% This prevents situations where we have a low number of active tasks and no queue which
	%% causes each request to complete fast and hikes up the max_active. Then we get a new
	%% batch of queued tasks and since the max_active is so high we overwhelm the peer.
	MaxActive = PeerTasks#peer.max_active,
	FasterThanTarget = Performance#performance.average_latency < TargetLatency,
	WorkerCount = queue:len(State#state.workers),
	WorkersStarved = TotalMaxActive < WorkerCount,
	TargetMaxActive = case FasterThanTarget orelse WorkersStarved of
		true ->
			%% latency < target, increase max_active.
			MaxActive+1;
		false ->
			%% latency > target, decrease max_active
			MaxActive-1
	end,

	%% Can't have more active tasks than workers.
	WorkerLimitedMaxActive = min(TargetMaxActive, WorkerCount),
	%% Can't have more active tasks than we have active or queued tasks.
	TaskLimitedMaxActive = min(
		WorkerLimitedMaxActive, 
		max(PeerTasks#peer.active_count, queue:len(PeerTasks#peer.pending))
	),
	%% Can't have less than the minimum.
	PeerTasks#peer{
		max_active = max(TaskLimitedMaxActive, ?MIN_MAX_ACTIVE)
	}.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
increment_pending_task_count(Peer, N, State) ->
	prometheus_gauge:inc(sync_tasks, [pending, sync_range, ar_util:format_peer(Peer)], N),
	State#state{ total_pending_count = State#state.total_pending_count + N }.
increment_active_task_count(Worker, Peer, N, State) ->
	prometheus_gauge:inc(sync_tasks, [active, sync_range, ar_util:format_peer(Peer)], N),
	ActiveCount = maps:get(Worker, State#state.active_count_per_worker, 0) + N,
	State#state{
		total_active_count = State#state.total_active_count + N,
		active_count_per_worker = maps:put(Worker, ActiveCount, State#state.active_count_per_worker)
	}.

increment_footprint_active_task_count(none, _Peer, _N, State) ->
	State;
increment_footprint_active_task_count(FootprintKey, Peer, N, State) ->
	prometheus_gauge:inc(sync_tasks, [footprint_active, sync_range, ar_util:format_peer(Peer)], N),
	#state{ footprints = Footprints } = State,
	Footprint = maps:get(FootprintKey, Footprints, #footprint{}),
	Footprint2 = Footprint#footprint{ active_count = Footprint#footprint.active_count + N },
	State#state{ footprints = maps:put(FootprintKey, Footprint2, Footprints) }.

increment_footprint_pending_task_count(Peer, N, State) ->
	prometheus_gauge:inc(sync_tasks,
		[footprint_pending, sync_range, ar_util:format_peer(Peer)], N),
	State#state{ total_footprint_pending_count = State#state.total_footprint_pending_count + N }.
	
get_peer_tasks(Peer, State) ->
	maps:get(Peer, State#state.peers, #peer{peer = Peer}).

set_peer_tasks(PeerTasks, State) ->
	State#state{ peers =
		maps:put(PeerTasks#peer.peer, PeerTasks, State#state.peers)
	}.

get_worker(State) ->
	WorkerCount = queue:len(State#state.workers),
	AverageLoad = State#state.total_active_count / WorkerCount,
	cycle_workers(AverageLoad, State).

cycle_workers(AverageLoad, State) ->
	#state{ workers = Workers } = State,
	{{value, Worker}, Workers2} = queue:out(Workers),
	State2 = State#state{ workers = queue:in(Worker, Workers2) },
	ActiveCount = maps:get(Worker, State2#state.active_count_per_worker, 0),
	case ActiveCount =< AverageLoad of
		true ->
			{Worker, State2};
		false ->
			cycle_workers(AverageLoad, State2)
	end.

%%%===================================================================
%%% Tests. Included in the module so they can reference private
%%% functions.
%%%===================================================================

helpers_test_() ->
	[
		{timeout, 30, fun test_counters/0},
		{timeout, 30, fun test_get_worker/0}
	].

queue_test_() ->
	[
		{timeout, 30, fun test_enqueue_peer_task/0}
	].

footprint_queue_test_() ->
	[
		{timeout, 30, fun test_footprint_queue_none/0},
		{timeout, 30, fun test_footprint_queue_limit/0},
		{timeout, 30, fun test_footprint_queue_completion/0}
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
	?assertEqual(0, State#state.total_active_count),
	?assertEqual(0, maps:get("worker1", State#state.active_count_per_worker, 0)),
	?assertEqual(0, State#state.total_pending_count),
	State2 = increment_active_task_count("worker1", "localhost", 10, State),
	State3 = increment_pending_task_count("localhost", 10, State2),
	?assertEqual(10, State3#state.total_active_count),
	?assertEqual(10, maps:get("worker1", State3#state.active_count_per_worker, 0)),
	?assertEqual(10, State3#state.total_pending_count),
	State4 = increment_active_task_count("worker1", "localhost", -1, State3),
	State5 = increment_pending_task_count("localhost", -1, State4),
	?assertEqual(9, State5#state.total_active_count),
	?assertEqual(9, maps:get("worker1", State5#state.active_count_per_worker, 0)),
	?assertEqual(9, State5#state.total_pending_count),
	State6 = increment_active_task_count("worker2", "localhost", 1, State5),
	?assertEqual(10, State6#state.total_active_count),
	?assertEqual(1, maps:get("worker2", State6#state.active_count_per_worker, 0)),
	State7 = increment_active_task_count("worker1", "1.2.3.4:1984", -1, State6),
	State8 = increment_pending_task_count("1.2.3.4:1984", -1, State7),
	?assertEqual(9, State8#state.total_active_count),
	?assertEqual(8, maps:get("worker1", State8#state.active_count_per_worker, 0)),
	?assertEqual(8, State8#state.total_pending_count).

test_get_worker() ->
	State0 = #state{
		workers = queue:from_list([worker1, worker2, worker3]),
		total_active_count = 6,
		active_count_per_worker = #{worker1 => 3, worker2 => 2, worker3 => 1}
	},
	%% get_worker will cycle the queue until it finds a worker that has a worker_load =< the 
	%% average load (i.e. total_active_count / queue:len(workers))
	{worker2, State1} = get_worker(State0),
	State2 = increment_active_task_count(worker2, "localhost", 1, State1),
	{worker3, State3} = get_worker(State2),
	State4 = increment_active_task_count(worker3, "localhost", 1, State3),
	{worker3, State5} = get_worker(State4),
	State6 = increment_active_task_count(worker3, "localhost", 1, State5),
	{worker1, _} = get_worker(State6).

test_enqueue_peer_task() ->
	PeerA = {1, 2, 3, 4, 1984},
	PeerB = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	State0 = #state{
		peers = #{
			PeerA => #peer{ peer = PeerA, max_active = 0 },
			PeerB => #peer{ peer = PeerB, max_active = 0 }
		}
	},
	
	State1 = enqueue_peer_task(PeerA, {0, 100, PeerA, StoreID1, none}, State0),
	State2 = enqueue_peer_task(PeerA, {100, 200, PeerA, StoreID1, none}, State1),
	State3 = enqueue_peer_task(PeerB, {200, 300, PeerB, StoreID1, none}, State2),
	
	PeerATasks = get_peer_tasks(PeerA, State3),
	PeerBTasks = get_peer_tasks(PeerB, State3),
	assert_peer_tasks([
		{0, 100, PeerA, StoreID1, none},
		{100, 200, PeerA, StoreID1, none}
	], 0, 0, PeerATasks),
	assert_peer_tasks([
		{200, 300, PeerB, StoreID1, none}
	], 0, 0, PeerBTasks),

	{PeerATasks2, Args1} = dequeue_peer_task(PeerATasks),
	?assertEqual({0, 100, PeerA, StoreID1, none}, Args1),
	{PeerBTasks2, Args2} = dequeue_peer_task(PeerBTasks),
	?assertEqual({200, 300, PeerB, StoreID1, none}, Args2),
	assert_peer_tasks([
		{100, 200, PeerA, StoreID1, none}
	], 0, 0, PeerATasks2),
	assert_peer_tasks([], 0, 0, PeerBTasks2).

%% Test that tasks with FootprintKey = none are enqueued directly to peer queues
test_footprint_queue_none() ->
	Peer1 = {1, 2, 3, 4, 1984},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	%% Set up a peer with max_active = 0 so tasks stay in queue (can't be scheduled)
	PeerTasks0 = #peer{peer = Peer1, max_active = 0},
	State0 = #state{ 
		max_footprints = 2,
		workers = queue:from_list([worker1, worker2, worker3]),
		peers = #{Peer1 => PeerTasks0}
	},
	
	%% Enqueue tasks with FootprintKey = none
	State1 = enqueue_task(none, {0, 100, Peer1, StoreID1, none}, State0),
	State2 = enqueue_task(none, {100, 200, Peer1, StoreID1, none}, State1),
	
	%% Both should be in the peer queue, not main queue (which no longer exists)
	PeerTasks = get_peer_tasks(Peer1, State2),
	?assertEqual(2, queue:len(PeerTasks#peer.pending)),
	?assertEqual(0, count_active_footprints(State2#state.footprints)).

%% Test that footprint queue limits concurrent footprints
test_footprint_queue_limit() ->
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	FootprintKey1 = {Peer1, 0, 1},
	FootprintKey2 = {Peer2, 0, 2},
	FootprintKey3 = {Peer1, 0, 3},
	State0 = #state{ max_footprints = 2 },
	
	%% Add tasks for first two footprints - should be enqueued directly to peer queues
	State1 = enqueue_task(FootprintKey1, {0, 100, Peer1, StoreID1, FootprintKey1}, State0),
	PeerTasks1 = get_peer_tasks(Peer1, State1),
	?assertEqual(1, queue:len(PeerTasks1#peer.pending)),
	?assertEqual(1, count_active_footprints(State1#state.footprints)),
	
	State2 = enqueue_task(FootprintKey2, {100, 200, Peer2, StoreID1, FootprintKey2}, State1),
	PeerTasks2 = get_peer_tasks(Peer2, State2),
	?assertEqual(1, queue:len(PeerTasks2#peer.pending)),
	?assertEqual(2, count_active_footprints(State2#state.footprints)),
	
	%% Third footprint should be queued (not in peer queues yet)
	State3 = enqueue_task(FootprintKey3, {200, 300, Peer1, StoreID1, FootprintKey3}, State2),
	PeerTasks3 = get_peer_tasks(Peer1, State3),
	?assertEqual(1, queue:len(PeerTasks3#peer.pending)), %% peer queue unchanged
	?assertEqual(2, count_active_footprints(State3#state.footprints)), %% active footprints unchanged
	Footprint3 = maps:get(FootprintKey3, State3#state.footprints, #footprint{}),
	?assertEqual(false, queue:is_empty(Footprint3#footprint.queue)), %% one footprint waiting
	
	%% Additional task for FootprintKey1 (already active) should go to peer queue
	State4 = enqueue_task(FootprintKey1, {300, 400, Peer1, StoreID1, FootprintKey1}, State3),
	PeerTasks4 = get_peer_tasks(Peer1, State4),
	?assertEqual(2, queue:len(PeerTasks4#peer.pending)),
	?assertEqual(2, count_active_footprints(State4#state.footprints)).

%% Test that completing a footprint activates the next waiting footprint
test_footprint_queue_completion() ->
	Peer1 = {1, 2, 3, 4, 1984},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	FootprintKey1 = {Peer1, 0, 1},
	FootprintKey2 = {Peer1, 0, 2},
	FootprintKey3 = {Peer1, 0, 3},
	State0 = #state{ max_footprints = 2 },
	
	%% Fill up with two footprints and queue a third
	State1 = enqueue_task(FootprintKey1, {0, 100, Peer1, StoreID1, FootprintKey1}, State0),
	State2 = enqueue_task(FootprintKey2, {100, 200, Peer1, StoreID1, FootprintKey2}, State1),
	State3 = enqueue_task(FootprintKey3, {200, 300, Peer1, StoreID1, FootprintKey3}, State2),
	
	%% Complete FootprintKey1 - should activate FootprintKey3
	State4 = complete_footprint_task(FootprintKey1, Peer1, State3),
	?assertEqual(2, count_active_footprints(State4#state.footprints)),
	Footprint2 = maps:get(FootprintKey2, State4#state.footprints, #footprint{}),
	Footprint3 = maps:get(FootprintKey3, State4#state.footprints, #footprint{}),
	?assertEqual(1, Footprint2#footprint.active_count),
	?assertEqual(1, Footprint3#footprint.active_count),
	?assertEqual(false, maps:is_key(FootprintKey1, State4#state.footprints)),
	?assertEqual(true, queue:is_empty(Footprint3#footprint.queue)), %% waiting queue now empty
	%% The task from FootprintKey3 should now be in the peer queue
	PeerTasks = get_peer_tasks(Peer1, State4),
	?assertEqual(3, queue:len(PeerTasks#peer.pending)).

test_max_peer_queue() ->
	?assertEqual(undefined, max_peer_queue(#performance{ current_rating = 10 }, 0, 10)),
	?assertEqual(undefined, max_peer_queue(#performance{ current_rating = 10 }, 0.0, 10)),
	?assertEqual(undefined, max_peer_queue(#performance{ current_rating = 0 }, 100, 10)),
	?assertEqual(undefined, max_peer_queue(#performance{ current_rating = 0.0 }, 100, 10)),
	?assertEqual(50, max_peer_queue(#performance{ current_rating = 10 }, 100, 10)),
	?assertEqual(20, max_peer_queue(#performance{ current_rating = 1 }, 100, 10)).

test_cut_peer_queue() ->
	{ok, OriginalConfig} = arweave_config:get_env(),
	try
		ok = arweave_config:set_env(OriginalConfig#config{
			sync_jobs = 10
		}),

		Peer1 = {1, 2, 3, 4, 1984},
		Pending = lists:seq(1, 100),
		PeerTasks = #peer{
			peer = Peer1,
			pending = queue:from_list(Pending)
		},
		State = #state{
			total_pending_count = length(Pending),
			total_active_count = 10
		},
		
		{PeerTasks1, State1} = cut_peer_queue(200, PeerTasks, State),
		assert_peer_tasks(Pending, 0, 8, PeerTasks1),
		?assertEqual(100, State1#state.total_pending_count),

		{PeerTasks2, State2} = cut_peer_queue(20, PeerTasks, State),
		assert_peer_tasks(lists:sublist(Pending, 1, 20), 0, 8, PeerTasks2),
		?assertEqual(20, State2#state.total_pending_count),

		{PeerTasks3, State3} = cut_peer_queue(
			20, PeerTasks, State#state{ total_active_count = 0 }),
		assert_peer_tasks(Pending, 0, 8, PeerTasks3),
		?assertEqual(100, State3#state.total_pending_count),

		{PeerTasks4, State4} = cut_peer_queue(undefined, PeerTasks, State),
		assert_peer_tasks(Pending, 0, 8, PeerTasks4),
		?assertEqual(100, State4#state.total_pending_count)
	after
		arweave_config:set_env(OriginalConfig)
	end.

test_update_active() ->
	Workers20 = queue:from_list(lists:seq(1, 20)),
	Workers10 = queue:from_list(lists:seq(1, 10)),
	Result1 = update_max_active(
		#peer{max_active = 10, active_count = 10, pending = queue:from_list(lists:seq(1, 30))},
		#performance{average_latency = 100},
		20,
		200,
		#state{ workers = Workers20 }),
	?assertEqual(11, Result1#peer.max_active),
	
	Result2 = update_max_active(
		#peer{max_active = 10, active_count = 20, pending = queue:from_list(lists:seq(1, 30))},
		#performance{average_latency = 300},
		20,
		200,
		#state{ workers = Workers20 }),
	?assertEqual(9, Result2#peer.max_active),

	Result3 = update_max_active(
		#peer{max_active = 10, active_count = 20, pending = queue:from_list(lists:seq(1, 30))},
		#performance{average_latency = 300},
		10,
		200,
		#state{ workers = Workers20 }),
	?assertEqual(11, Result3#peer.max_active),
	
	Result4 = update_max_active(
		#peer{max_active = 10, active_count = 20, pending = queue:from_list(lists:seq(1, 30))},
		#performance{average_latency = 100},
		10,
		200,
		#state{ workers = Workers10 }),
	?assertEqual(10, Result4#peer.max_active),
	
	Result5 = update_max_active(
		#peer{max_active = 10, active_count = 5, pending = queue:from_list(lists:seq(1, 10))},
		#performance{average_latency = 100},
		20,
		200,
		#state{ workers = Workers20 }),
	?assertEqual(10, Result5#peer.max_active),
	
	Result6 = update_max_active(
		#peer{max_active = 10, active_count = 10, pending = queue:from_list(lists:seq(1, 5))},
		#performance{average_latency = 100},
		20,
		200,
		#state{ workers = Workers20 }),
	?assertEqual(10, Result6#peer.max_active),

	Result7 = update_max_active(
		#peer{max_active = 8, active_count = 20, pending = queue:from_list(lists:seq(1, 30))},
		#performance{average_latency = 300},
		20,
		200,
		#state{ workers = Workers20 }),
	?assertEqual(8, Result7#peer.max_active).

test_calculate_targets() ->
	State0 = #state{},
	Result1 = calculate_targets([], #{}, State0),
	?assertEqual({0.0, 0.0, 0}, Result1),

	State2 = #state{
		peers = #{
			"peer1" => #peer{max_active = 8},
			"peer2" => #peer{max_active = 10}
		}
	},
    Result2 = calculate_targets(
		["peer1", "peer2"],
		#{
			"peer1" => #performance{current_rating = 0, average_latency = 0},
			"peer2" => #performance{current_rating = 0, average_latency = 0}
		},
		State2),
    ?assertEqual({0.0, 0.0, 18}, Result2),
	
	Result3 = calculate_targets(
		["peer1", "peer2"],
		#{
			"peer1" => #performance{current_rating = 5, average_latency = 2},
			"peer2" => #performance{current_rating = 3, average_latency = 4}
		},
		State2),
    ?assertEqual({3.0, 8.0, 18}, Result3),

	State4 = #state{
		peers = #{
			"peer1" => #peer{max_active = 8}
		}
	},
	Result4 = calculate_targets(
		["peer1", "peer2"],
		#{
			"peer1" => #performance{current_rating = 5, average_latency = 2}
		},
		State4),
    ?assertEqual({1.0, 5.0, 8}, Result4),

	State5 = #state{
		peers = #{
			"peer1" => #peer{max_active = 12}
		}
	},
    Result5 = calculate_targets(
		["peer1"],
		#{
			"peer1" => #performance{current_rating = 5, average_latency = 2},
			"peer2" => #performance{current_rating = 3, average_latency = 4}
		},
		State5),
    ?assertEqual({2.0, 5.0, 12}, Result5).

assert_peer_tasks(ExpectedQueue, ExpectedActiveCount, ExpectedMaxActive, PeerTasks) ->
	?assertEqual(ExpectedQueue, queue:to_list(PeerTasks#peer.pending)),
	?assertEqual(ExpectedActiveCount, PeerTasks#peer.active_count),
	?assertEqual(ExpectedMaxActive, PeerTasks#peer.max_active).
