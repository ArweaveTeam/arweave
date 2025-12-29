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
%%% Tasks flow through the system in three states: waiting -> queued -> dispatched
%%%
%%% 1. Footprint Management (waiting state):
%%%    - Tasks with a FootprintKey are grouped by footprint to limit concurrent
%%%      processing and avoid overloading the entropy cache
%%%    - Each footprint has a queue of waiting tasks (those that have not been enqueued to
%%%      a peer queue yet) and a count of active tasks (those that have been enqueued to
%%%      a peer queue - i.e. queued or dispatched).
%%%    - When a footprint is active (active_count > 0), new tasks are immediately enqueued
%%%      to the peer queue (queued state)
%%%    - When max_footprints is reached, new footprints are queued until capacity
%%%      becomes available
%%%
%%% 2. Peer Management (queued state):
%%%    - Each peer has a queue of queued tasks and tracks dispatched task count
%%%    - Each peer has a max_dispatched limit that controls how many tasks can be
%%%      concurrently dispatched for that peer
%%%    - When a peer has capacity (dispatched_count < max_dispatched) and queued tasks,
%%%      tasks are dispatched to workers
%%%
%%% 3. Worker Management (dispatched state):
%%%    - Workers are selected using round-robin with load balancing
%%%
%%% Task Completion:
%%% When a task completes:
%%% 1. Worker and peer dispatched counts are decremented
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
%%%    - Maximum allowed dispatched tasks (#peer.max_dispatched) is adjusted based on:
%%%      * Peer latency vs target latency (increase if faster, decrease if slower)
%%%      * Current dispatched/queued task counts (prevents over-allocation)
%%%    - If needed, the queue of queued tasks is cut back to max_dispatched - this allows the
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
	queued = queue:new(),
	dispatched_count = 0,
	waiting_count = 0,   %% count of tasks for this peer in the waiting state
	max_dispatched = ?MIN_MAX_ACTIVE
}).

-record(footprint, {
	waiting = queue:new(),   %% queue of waiting tasks
	active_count = 0         %% count of active tasks (0 = inactive)
}).

-record(state, {
	total_waiting_count = 0,    %% total count of tasks in the waiting state
	total_queued_count = 0,     %% total count of tasks in the queued state
	total_dispatched_count = 0,
	workers = queue:new(),
	dispatched_count_per_worker = #{},
	peers = #{},              %% Peer => #peer{}
	footprints = #{},         %% FootprintKey => #footprint{}
	active_footprint_count = 0, %% count of footprints with active_count > 0
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
		State#state.total_dispatched_count +
		State#state.total_queued_count +
		State#state.total_waiting_count,
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
			{_Start, _End, _Peer, _TargetStoreID, FootprintKey} = Args,
			{noreply, enqueue_task(FootprintKey, Args, State)}
	end;

handle_cast({task_completed, {sync_range, {Worker, Result, Args, ElapsedNative}}}, State) ->
	{Start, End, Peer, _, _, FootprintKey} = Args,
	DataSize = End - Start,
	State2 = increment_dispatched_task_count(Worker, Peer, -1, State),
	PeerTasks = get_peer_tasks(Peer, State2),
	ar_peers:rate_fetched_data(
		PeerTasks#peer.peer, chunk, Result,
		erlang:convert_time_unit(ElapsedNative, native, microsecond), DataSize,
		PeerTasks#peer.max_dispatched),
	%% Process all peer queues, starting with the peer that just completed
	State3 = complete_footprint_task(FootprintKey, Peer, State2),
	State4 = process_all_peer_queues(Peer, State3),
	{noreply, State4};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	State2 = State, %purge_empty_peers(State),
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

%% @doc If a peer has capacity, take the next task from its queue and dispatch it.
process_peer_queue(Peer, State) ->
	case queue:is_empty(State#state.workers) of
		true ->
			%% No workers available, can't dispatch
			?LOG_DEBUG([{event, process_peer_queue}, {peer, ar_util:format_peer(Peer)}, {no_workers}]),
			State;
		false ->
			PeerTasks = get_peer_tasks(Peer, State),
			?LOG_DEBUG([{event, process_peer_queue}, {peer, ar_util:format_peer(Peer)},
				{has_capacity, peer_has_capacity(PeerTasks)},
				{has_queued_tasks, peer_has_queued_tasks(PeerTasks)},
				{waiting_count, PeerTasks#peer.waiting_count},
				{dispatched_count, PeerTasks#peer.dispatched_count},
				{max_dispatched, PeerTasks#peer.max_dispatched}
			]),
			case peer_has_capacity(PeerTasks) andalso peer_has_queued_tasks(PeerTasks) of
				true ->
					State2 = dispatch_task(PeerTasks, State),
					process_peer_queue(Peer, State2);
				false ->
					State
			end
	end.

%% @doc Process all peer queues, with the priority peer processed first.
%% This maintains locality (same peer/footprint) while ensuring worker capacity
%% is filled from all peers.
process_all_peer_queues(PriorityPeer, State) ->
	AllPeers = maps:keys(maps:remove(PriorityPeer, State#state.peers)),
	process_all_peer_queues2([PriorityPeer | AllPeers], State).

process_all_peer_queues2([], State) ->
	State;
process_all_peer_queues2([Peer | Rest], State) ->
	State2 = process_peer_queue(Peer, State),
	process_all_peer_queues2(Rest, State2).

%% @doc the maximum number of tasks we can have in process - including tasks queued here as well
%% as those active on ar_data_sync_workers.
max_tasks(WorkerCount) ->
	WorkerCount * 50.

%% @doc The maximum number of tasks we can have queued for a given peer.
max_peer_queue(_Peformance, 0, _WorkerCount) ->
	infinity;
max_peer_queue(_Performance, 0.0, _WorkerCount) ->
	infinity;
max_peer_queue(#performance{ current_rating = 0 } = _Performance, _TotalThroughput, _WorkerCount) ->
	infinity;
max_peer_queue(#performance{ current_rating = 0.0 } = _Performance, _TotalThroughput, _WorkerCount) ->
	infinity;
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
cut_peer_queue(_Peer, _Performance, _TotalThroughput, #state{ total_dispatched_count = 0 } = State) ->
	State;
cut_peer_queue(Peer, Performance, TotalThroughput, State) ->
	WorkerCount = queue:len(State#state.workers),
	PeerTasks = get_peer_tasks(Peer, State),
	MaxQueue = max_peer_queue(Performance, TotalThroughput, WorkerCount),
	Queued = PeerTasks#peer.queued,
	case MaxQueue == infinity orelse queue:len(Queued) - MaxQueue =< 0 of
		true ->
			State;
		false ->
			TasksToCut = queue:len(Queued) - MaxQueue,
			%% The peer has a large queue of tasks. Reduce the queue size by removing the
			%% oldest tasks.
			{Queued2, RemovedQueue} = queue:split(MaxQueue, Queued),
			%% Decrement footprint active task counts for all removed tasks
			State2 = cut_active_footprint_task_counts(RemovedQueue, Peer, State),
			?LOG_DEBUG([{event, cut_peer_queue},
				{peer, ar_util:format_peer(Peer)},
				{dispatched_count, PeerTasks#peer.dispatched_count},
				{active_tasks, State#state.total_dispatched_count},
				{max_queue, MaxQueue}, {tasks_to_cut, TasksToCut},
				{old_queued_count, queue:len(Queued)},
				{new_queued_count, queue:len(Queued2)}
			]),
			State3 = set_peer_tasks(PeerTasks#peer{ queued = Queued2 }, State2),
			increment_queued_task_count(Peer, -TasksToCut, State3)
	end.

%% @doc Enqueue a task to a peer queue, process the queue, and update counters.
enqueue_peer_task(FootprintKey, Peer, Args, State) ->
	{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
	PeerTasks = get_peer_tasks(Peer, State),
	Queued = queue:in(Args, PeerTasks#peer.queued),
	PeerTasks2 = PeerTasks#peer{
		queued = Queued
	},
	State2 = set_peer_tasks(PeerTasks2, State),
	State3 = increment_queued_task_count(Peer, 1, State2),
	increment_active_footprint_task_count(FootprintKey, Peer, 1, State3).

dequeue_peer_task(PeerTasks) ->
	{{value, Args}, Queued} = queue:out(PeerTasks#peer.queued),
	PeerTasks2 = PeerTasks#peer{ queued = Queued },
	{PeerTasks2, Args}.

peer_has_capacity(PeerTasks) ->
	PeerTasks#peer.dispatched_count < PeerTasks#peer.max_dispatched.

peer_has_queued_tasks(PeerTasks) ->
	not queue:is_empty(PeerTasks#peer.queued).

%%--------------------------------------------------------------------
%% Footprint queue management
%%--------------------------------------------------------------------

%% @doc Enqueue a sync_range task, managing footprint concurrency limits.
%% For tasks with FootprintKey = none, enqueue directly to peer queue.
%% For tasks with a FootprintKey, limit concurrent footprints.
enqueue_task(none, Args, State) ->
	%% Non-footprint task, enqueue directly to peer queue
	{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
	State2 = enqueue_peer_task(none, Peer, Args, State),
	process_peer_queue(Peer, State2);
enqueue_task(FootprintKey, Args, State) ->
	#state{ max_footprints = MaxFootprints,
			active_footprint_count = ActiveCount } = State,
	{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
	State3 = case get_active_footprint_task_count(FootprintKey, State) > 0 of
		true ->
			%% Footprint is already active, enqueue directly to peer queue
			enqueue_peer_task(FootprintKey, Peer, Args, State);
		false when ActiveCount < MaxFootprints ->
			%% Room for new footprint, activate it and enqueue directly to peer queue
			Footprint = get_footprint(FootprintKey, State),
			?LOG_DEBUG([{event, activate_footprint},
						{source, enqueue_task},
						{active_footprints, ActiveCount + 1},
						{active_tasks, Footprint#footprint.active_count},
						{waiting_tasks, queue:len(Footprint#footprint.waiting)},
						{footprint_key, FootprintKey}]),
			activate_footprint(Peer),
			State2 = State#state{ active_footprint_count = ActiveCount + 1 },
			enqueue_peer_task(FootprintKey, Peer, Args, State2);
		false ->
			%% No room, queue the task for later
			Footprint = get_footprint(FootprintKey, State),
			Waiting = queue:in(Args, Footprint#footprint.waiting),
			Footprint2 = Footprint#footprint{ waiting = Waiting },
			State2 = set_footprint(FootprintKey, Footprint2, State),
			increment_waiting_task_count(Peer, 1, State2)
	end,
	process_peer_queue(Peer, State3).

%% @doc Handle completion of a footprint task.
%% Decrements the active active_count and activates a new footprint if this one is done.
complete_footprint_task(none, _Peer, State) ->
	State;
complete_footprint_task(FootprintKey, Peer, State) ->
	State2 = increment_active_footprint_task_count(FootprintKey, Peer, -1, State),
	case get_active_footprint_task_count(FootprintKey, State2) =< 0 of
		true ->
			Footprint = get_footprint(FootprintKey, State2),
			case queue:is_empty(Footprint#footprint.waiting) of
				true ->
					%% This footprint is done and has no queued tasks, remove it and try to activate a new one
					?LOG_DEBUG([{event, deactivate_footprint},
						{active_tasks, Footprint#footprint.active_count},
						{waiting_tasks, queue:len(Footprint#footprint.waiting)},
						{footprint_key, FootprintKey}]),
					deactivate_footprint(Peer),
					State3 = State2#state{
						footprints = maps:remove(FootprintKey, State2#state.footprints),
						active_footprint_count = State2#state.active_footprint_count - 1
					},
					activate_next_footprint(State3);
				false ->
					%% This footprint has queued tasks, activate it by enqueuing its tasks
					?LOG_DEBUG([{event, enqueue_footprint},
						{source, complete_footprint_task},
						{active_tasks, Footprint#footprint.active_count},
						{waiting_tasks, queue:len(Footprint#footprint.waiting)},
						{footprint_key, FootprintKey}]),
					State3 = set_footprint(
						FootprintKey, Footprint#footprint{ waiting = queue:new() }, State2),
					%% Enqueue all tasks from this footprint queue directly to peer queues
					enqueue_footprint(FootprintKey, Footprint#footprint.waiting, State3)
			end;
		false ->
			State2
	end.

%% @doc Activate the next waiting footprint queue if any.
activate_next_footprint(State) ->
	#state{ footprints = Footprints, max_footprints = MaxFootprints,
			active_footprint_count = ActiveCount } = State,
	case ActiveCount >= MaxFootprints of
		true ->
			State;
		false ->
			%% Find a footprint with queued tasks
			NextFootprint = find_next_inactive_footprint(Footprints),
			case NextFootprint of
				none ->
					?LOG_DEBUG([{event, activate_footprint_none}]),
					State;
				{FootprintKey, Footprint} ->
					?LOG_DEBUG([{event, activate_footprint},
						{source, activate_next_footprint},
						{active_footprints, ActiveCount + 1},
						{active_tasks, Footprint#footprint.active_count},
						{waiting_tasks, queue:len(Footprint#footprint.waiting)},
						{footprint_key, FootprintKey}]),
					{_, _, Peer} = FootprintKey,
					activate_footprint(Peer),
					State2 = State#state{ active_footprint_count = ActiveCount + 1 },
					State3 = set_footprint(
						FootprintKey, Footprint#footprint{ waiting = queue:new() }, State2),
					%% Enqueue all tasks from this footprint queue directly to peer queues
					enqueue_footprint(FootprintKey, Footprint#footprint.waiting, State3)
			end
	end.

%% @doc Find the next inactive footprint with the most queued tasks.
find_next_inactive_footprint(Footprints) ->
	case maps:fold(
		fun(Key, Footprint, Acc) ->
			case Footprint#footprint.active_count =< 0 andalso
			     not queue:is_empty(Footprint#footprint.waiting) of
				true ->
					WaitingCount = queue:len(Footprint#footprint.waiting),
					case Acc of
						none ->
							{Key, Footprint, WaitingCount};
						{_AccKey, _AccFootprint, AccCount} when WaitingCount > AccCount ->
							{Key, Footprint, WaitingCount};
						_ ->
							Acc
					end;
				false ->
					Acc
			end
		end, none, Footprints) of
		none ->
			none;
		{Key, Footprint, _WaitingCount} ->
			{Key, Footprint}
	end.

%% @doc Enqueue all tasks from a footprint queue directly to peer queues.
enqueue_footprint(FootprintKey, Queue, State) ->
	case queue:out(Queue) of
		{empty, _} ->
			State;
		{{value, Args}, Queue2} ->
			{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
			State2 = increment_waiting_task_count(Peer, -1, State),
			State3 = enqueue_peer_task(FootprintKey, Peer, Args, State2),
			enqueue_footprint(FootprintKey, Queue2, State3)
	end.

%%--------------------------------------------------------------------
%% Dispatch tasks to be run on workers
%%--------------------------------------------------------------------

%% @doc Dispatch a sync_range task from a peer queue.
dispatch_task(PeerTasks, State) ->
	{PeerTasks2, Args} = dequeue_peer_task(PeerTasks),
	{Start, End, Peer, TargetStoreID, FootprintKey} = Args,
	{Worker, State2} = get_worker(State),
	%% Pass FootprintKey as 6th element so it comes back in task_completed
	gen_server:cast(Worker, {sync_range, {Start, End, Peer, TargetStoreID, 3, FootprintKey}}),
	State3 = set_peer_tasks(PeerTasks2, State2),
	State4 = increment_dispatched_task_count(Worker, Peer, 1, State3),
	increment_queued_task_count(Peer, -1, State4).

%%--------------------------------------------------------------------
%% Record a completed task and update related values (i.e.
%% EMA, max_dispatched, peer queue length)
%%--------------------------------------------------------------------
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
	TotalMaxDispatched =
		maps:fold(
			fun(_Key, PeerTasks, Acc) ->
				PeerTasks#peer.max_dispatched + Acc
			end,
		0, State#state.peers),
	?LOG_DEBUG([{event, sync_performance_targets},
		{target_latency, TargetLatency},
		{total_throughput, TotalThroughput},
		{total_max_dispatched, TotalMaxDispatched}]),
    {TargetLatency, TotalThroughput, TotalMaxDispatched}.

purge_empty_peers(State) ->
	PurgedPeerTasks = maps:filter(
		fun(_Peer, PeerTasks) ->
			(not queue:is_empty(PeerTasks#peer.queued))
				orelse PeerTasks#peer.dispatched_count > 0
		end,
		State#state.peers),
	State#state{ peers = PurgedPeerTasks }.

rebalance_peers([], _AllPeerPerformances, _Targets, State) ->
	State;
rebalance_peers([Peer | Peers], AllPeerPerformances, Targets, State) ->
	Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
	State2 = rebalance_peer(Peer, Performance, Targets, State),
	log_rebalance_peer(Peer, Performance, Targets, State, State2),
	rebalance_peers(Peers, AllPeerPerformances, Targets, State2).

rebalance_peer(Peer, Performance, Targets, State) ->
	{TargetLatency, TotalThroughput, TotalMaxDispatched} = Targets,
	State2 = cut_peer_queue(Peer, Performance, TotalThroughput, State),
	update_max_dispatched(Peer, Performance, TotalMaxDispatched, TargetLatency, State2).

update_max_dispatched(Peer, Performance, TotalMaxDispatched, TargetLatency, State) ->
	%% Determine target max_dispatched:
	%% 1. Increase max_dispatched when the peer's average latency is less than the threshold
	%% 2. OR increase max_dispatched when the workers are not fully utilized
	%% 3. Decrease max_dispatched when the peer's average latency is more than the threshold
	%%
	%% Once we have the target max_dispatched, find the maximum of the currently dispatched
	%% tasks and queued tasks. The new max_dispatched is the minimum of the target and that
	%% value. This prevents situations where we have a low number of dispatched tasks and no
	%% queue which causes each request to complete fast and hikes up the max_dispatched.
	%% Then we get a new batch of queued tasks and since the max_dispatched is so high we
	%% overwhelm the peer.
	PeerTasks = get_peer_tasks(Peer, State),
	MaxDispatched = PeerTasks#peer.max_dispatched,
	FasterThanTarget = Performance#performance.average_latency < TargetLatency,
	WorkerCount = queue:len(State#state.workers),
	WorkersStarved = TotalMaxDispatched < WorkerCount,
	TargetMaxDispatched = case FasterThanTarget orelse WorkersStarved of
		true ->
			%% latency < target, increase max_dispatched.
			MaxDispatched+1;
		false ->
			%% latency > target, decrease max_dispatched
			MaxDispatched-1
	end,

	%% Can't have more dispatched tasks than workers or available tasks.
	MaxTasks = max(PeerTasks#peer.dispatched_count,
		PeerTasks#peer.waiting_count + queue:len(PeerTasks#peer.queued)),
	UpperBound = min(WorkerCount, MaxTasks),
	PeerTasks2 = PeerTasks#peer{
		max_dispatched = ar_util:between(
			TargetMaxDispatched, ?MIN_MAX_ACTIVE, max(UpperBound, ?MIN_MAX_ACTIVE))
	},
	set_peer_tasks(PeerTasks2, State).

log_rebalance_peer(Peer, Performance, Targets, StateBefore, StateAfter) ->
	{TargetLatency, TotalThroughput, TotalMaxDispatched} = Targets,
	PeerTasksBefore = get_peer_tasks(Peer, StateBefore),
	PeerTasksAfter = get_peer_tasks(Peer, StateAfter),
	?LOG_DEBUG([{event, rebalance_peer},
		{peer, ar_util:format_peer(Peer)},
		{dispatched_count, PeerTasksBefore#peer.dispatched_count},
		{queued_count, queue:len(PeerTasksBefore#peer.queued)},
		{waiting_count, PeerTasksBefore#peer.waiting_count},
		{peer_latency, Performance#performance.average_latency},
		{target_latency, TargetLatency},
		{peer_throughput, Performance#performance.current_rating},
		{total_throughput, TotalThroughput},
		{total_max_dispatched, TotalMaxDispatched},
		{worker_count, queue:len(StateBefore#state.workers)},
		{old_max_dispatched, PeerTasksBefore#peer.max_dispatched},
		{new_max_dispatched, PeerTasksAfter#peer.max_dispatched},
		{old_peer_queue, queue:len(PeerTasksBefore#peer.queued)},
		{new_peer_queue, queue:len(PeerTasksAfter#peer.queued)}]).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------
increment_waiting_task_count(Peer, N, State) ->
	case N of
		N when N > 0 ->
			prometheus_counter:inc(sync_tasks, [waiting_in, ar_util:format_peer(Peer)], N);
		N when N < 0 ->
			prometheus_counter:inc(sync_tasks, [waiting_out, ar_util:format_peer(Peer)], abs(N));
		_ ->
			ok
	end,
	PeerTasks = get_peer_tasks(Peer, State),
	PeerTasks2 = PeerTasks#peer{
		waiting_count = PeerTasks#peer.waiting_count + N
	},
	State2 = set_peer_tasks(PeerTasks2, State),
	State2#state{ total_waiting_count = State2#state.total_waiting_count + N }.
increment_queued_task_count(Peer, N, State) ->
	case N of
		N when N > 0 ->
			prometheus_counter:inc(sync_tasks, [queued_in, ar_util:format_peer(Peer)], N);
		N when N < 0 ->
			prometheus_counter:inc(sync_tasks, [queued_out, ar_util:format_peer(Peer)], abs(N));
		_ ->
			ok
	end,
	State#state{ total_queued_count = State#state.total_queued_count + N }.
increment_dispatched_task_count(Worker, Peer, N, State) ->
	case N of
		N when N > 0 ->
			prometheus_counter:inc(sync_tasks, [dispatched, ar_util:format_peer(Peer)], N);
		N when N < 0 ->
			prometheus_counter:inc(sync_tasks, [completed, ar_util:format_peer(Peer)], abs(N));
		_ ->
			ok
	end,
	PeerTasks = get_peer_tasks(Peer, State),
	PeerTasks2 = PeerTasks#peer{
		dispatched_count = PeerTasks#peer.dispatched_count + N
	},
	State2 = set_peer_tasks(PeerTasks2, State),
	ActiveCount = maps:get(Worker, State2#state.dispatched_count_per_worker, 0) + N,
	State2#state{
		total_dispatched_count = State2#state.total_dispatched_count + N,
		dispatched_count_per_worker = maps:put(Worker, ActiveCount, State2#state.dispatched_count_per_worker)
	}.

increment_active_footprint_task_count(none, _Peer, _N, State) ->
	State;
increment_active_footprint_task_count(FootprintKey, Peer, N, State) ->
	case N of
		N when N > 0 ->
			prometheus_counter:inc(sync_tasks, [activate_footprint_task, ar_util:format_peer(Peer)], N);
		N when N < 0 ->
			prometheus_counter:inc(sync_tasks, [deactivate_footprint_task, ar_util:format_peer(Peer)], abs(N));
		_ ->
			ok
	end,
	Footprint = get_footprint(FootprintKey, State),
	Footprint2 = Footprint#footprint{ active_count = Footprint#footprint.active_count + N },
	set_footprint(FootprintKey, Footprint2, State).

activate_footprint(Peer) ->
	prometheus_counter:inc(sync_tasks, [activate_footprint, ar_util:format_peer(Peer)], 1).

deactivate_footprint(Peer) ->
	prometheus_counter:inc(sync_tasks, [deactivate_footprint, ar_util:format_peer(Peer)], 1).

%% @doc Decrement footprint active task counts for all tasks in the removed queue.
cut_active_footprint_task_counts(RemovedQueue, Peer, State) ->
	case queue:out(RemovedQueue) of
		{empty, _} ->
			State;
		{{value, Args}, RemainingQueue} ->
			{_Start, _End, _Peer, _TargetStoreID, FootprintKey} = Args,
			State2 = increment_active_footprint_task_count(FootprintKey, Peer, -1, State),
			cut_active_footprint_task_counts(RemainingQueue, Peer, State2)
	end.
	
get_peer_tasks(Peer, State) ->
	maps:get(Peer, State#state.peers, #peer{peer = Peer}).

set_peer_tasks(PeerTasks, State) ->
	State#state{ peers =
		maps:put(PeerTasks#peer.peer, PeerTasks, State#state.peers)
	}.

get_footprint(FootprintKey, State) ->
	maps:get(FootprintKey, State#state.footprints, #footprint{}).

set_footprint(FootprintKey, Footprint, State) ->
	State#state{ footprints = maps:put(FootprintKey, Footprint, State#state.footprints) }.

get_active_footprint_task_count(FootprintKey, State) ->
	Footprint = get_footprint(FootprintKey, State),
	Footprint#footprint.active_count.

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
		{timeout, 30, fun test_update_dispatched/0},
		{timeout, 30, fun test_calculate_targets/0}
	].

test_counters() ->
	State = #state{},
	?assertEqual(0, State#state.total_dispatched_count),
	?assertEqual(0, maps:get("worker1", State#state.dispatched_count_per_worker, 0)),
	?assertEqual(0, State#state.total_queued_count),
	State2 = increment_dispatched_task_count("worker1", "localhost", 10, State),
	State3 = increment_queued_task_count("localhost", 10, State2),
	?assertEqual(10, State3#state.total_dispatched_count),
	?assertEqual(10, maps:get("worker1", State3#state.dispatched_count_per_worker, 0)),
	?assertEqual(10, State3#state.total_queued_count),
	State4 = increment_dispatched_task_count("worker1", "localhost", -1, State3),
	State5 = increment_queued_task_count("localhost", -1, State4),
	?assertEqual(9, State5#state.total_dispatched_count),
	?assertEqual(9, maps:get("worker1", State5#state.dispatched_count_per_worker, 0)),
	?assertEqual(9, State5#state.total_queued_count),
	State6 = increment_dispatched_task_count("worker2", "localhost", 1, State5),
	?assertEqual(10, State6#state.total_dispatched_count),
	?assertEqual(1, maps:get("worker2", State6#state.dispatched_count_per_worker, 0)),
	State7 = increment_dispatched_task_count("worker1", "1.2.3.4:1984", -1, State6),
	State8 = increment_queued_task_count("1.2.3.4:1984", -1, State7),
	?assertEqual(9, State8#state.total_dispatched_count),
	?assertEqual(8, maps:get("worker1", State8#state.dispatched_count_per_worker, 0)),
	?assertEqual(8, State8#state.total_queued_count).

test_get_worker() ->
	State0 = #state{
		workers = queue:from_list([worker1, worker2, worker3]),
		total_dispatched_count = 6,
		dispatched_count_per_worker = #{worker1 => 3, worker2 => 2, worker3 => 1}
	},
	%% get_worker will cycle the queue until it finds a worker that has a worker_load =< the 
	%% average load (i.e. total_dispatched_count / queue:len(workers))
	{worker2, State1} = get_worker(State0),
	State2 = increment_dispatched_task_count(worker2, "localhost", 1, State1),
	{worker3, State3} = get_worker(State2),
	State4 = increment_dispatched_task_count(worker3, "localhost", 1, State3),
	{worker3, State5} = get_worker(State4),
	State6 = increment_dispatched_task_count(worker3, "localhost", 1, State5),
	{worker1, _} = get_worker(State6).

test_enqueue_peer_task() ->
	PeerA = {1, 2, 3, 4, 1984},
	PeerB = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	State0 = #state{
		peers = #{
			PeerA => #peer{ peer = PeerA, max_dispatched = 0 },
			PeerB => #peer{ peer = PeerB, max_dispatched = 0 }
		}
	},
	
	State1 = enqueue_peer_task(none, PeerA, {0, 100, PeerA, StoreID1, none}, State0),
	State2 = enqueue_peer_task(none, PeerA, {100, 200, PeerA, StoreID1, none}, State1),
	State3 = enqueue_peer_task(none, PeerB, {200, 300, PeerB, StoreID1, none}, State2),
	
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
	%% Set up a peer with max_dispatched = 0 so tasks stay in queue (can't be dispatched)
	PeerTasks0 = #peer{peer = Peer1, max_dispatched = 0},
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
	?assertEqual(2, queue:len(PeerTasks#peer.queued)),
	?assertEqual(0, State2#state.active_footprint_count).

%% Test that footprint queue limits concurrent footprints
test_footprint_queue_limit() ->
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1985},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	FootprintKey1 = {0, 1, Peer1},
	FootprintKey2 = {0, 2, Peer2},
	FootprintKey3 = {0, 3, Peer1},
	State0 = #state{ max_footprints = 2 },
	
	%% Add tasks for first two footprints - should be enqueued directly to peer queues
	State1 = enqueue_task(FootprintKey1, {0, 100, Peer1, StoreID1, FootprintKey1}, State0),
	PeerTasks1 = get_peer_tasks(Peer1, State1),
	?assertEqual(1, queue:len(PeerTasks1#peer.queued)),
	?assertEqual(1, State1#state.active_footprint_count),
	
	State2 = enqueue_task(FootprintKey2, {100, 200, Peer2, StoreID1, FootprintKey2}, State1),
	PeerTasks2 = get_peer_tasks(Peer2, State2),
	?assertEqual(1, queue:len(PeerTasks2#peer.queued)),
	?assertEqual(2, State2#state.active_footprint_count),
	
	%% Third footprint should be queued (not in peer queues yet)
	State3 = enqueue_task(FootprintKey3, {200, 300, Peer1, StoreID1, FootprintKey3}, State2),
	PeerTasks3 = get_peer_tasks(Peer1, State3),
	?assertEqual(1, queue:len(PeerTasks3#peer.queued)), %% peer queue unchanged
	?assertEqual(2, State3#state.active_footprint_count), %% active footprints unchanged
	Footprint3 = get_footprint(FootprintKey3, State3),
	?assertEqual(false, queue:is_empty(Footprint3#footprint.waiting)), %% one footprint waiting
	
	%% Additional task for FootprintKey1 (already active) should go to peer queue
	State4 = enqueue_task(FootprintKey1, {300, 400, Peer1, StoreID1, FootprintKey1}, State3),
	PeerTasks4 = get_peer_tasks(Peer1, State4),
	?assertEqual(2, queue:len(PeerTasks4#peer.queued)),
	?assertEqual(2, State4#state.active_footprint_count).

%% Test that completing a footprint activates the next waiting footprint
test_footprint_queue_completion() ->
	Peer1 = {1, 2, 3, 4, 1984},
	StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
	FootprintKey1 = {0, 1, Peer1},
	FootprintKey2 = {0, 2, Peer1},
	FootprintKey3 = {0, 3, Peer1},
	State0 = #state{ max_footprints = 2 },
	
	%% Fill up with two footprints and queue a third
	State1 = enqueue_task(FootprintKey1, {0, 100, Peer1, StoreID1, FootprintKey1}, State0),
	State2 = enqueue_task(FootprintKey2, {100, 200, Peer1, StoreID1, FootprintKey2}, State1),
	State3 = enqueue_task(FootprintKey3, {200, 300, Peer1, StoreID1, FootprintKey3}, State2),
	
	%% Complete FootprintKey1 - should activate FootprintKey3
	State4 = complete_footprint_task(FootprintKey1, Peer1, State3),
	?assertEqual(2, State4#state.active_footprint_count),
	Footprint2 = get_footprint(FootprintKey2, State4),
	Footprint3 = get_footprint(FootprintKey3, State4),
	?assertEqual(1, Footprint2#footprint.active_count),
	?assertEqual(1, Footprint3#footprint.active_count),
	?assertEqual(false, maps:is_key(FootprintKey1, State4#state.footprints)),
	?assertEqual(true, queue:is_empty(Footprint3#footprint.waiting)), %% waiting queue now empty
	%% The task from FootprintKey3 should now be in the peer queue
	PeerTasks = get_peer_tasks(Peer1, State4),
	?assertEqual(3, queue:len(PeerTasks#peer.queued)).

test_max_peer_queue() ->
	?assertEqual(infinity, max_peer_queue(#performance{ current_rating = 10 }, 0, 10)),
	?assertEqual(infinity, max_peer_queue(#performance{ current_rating = 10 }, 0.0, 10)),
	?assertEqual(infinity, max_peer_queue(#performance{ current_rating = 0 }, 100, 10)),
	?assertEqual(infinity, max_peer_queue(#performance{ current_rating = 0.0 }, 100, 10)),
	?assertEqual(50, max_peer_queue(#performance{ current_rating = 10 }, 100, 10)),
	?assertEqual(20, max_peer_queue(#performance{ current_rating = 1 }, 100, 10)).

test_cut_peer_queue() ->
	{ok, OriginalConfig} = arweave_config:get_env(),
	try
		ok = arweave_config:set_env(OriginalConfig#config{
			sync_jobs = 10
		}),

		Peer1 = {1, 2, 3, 4, 1984},
		StoreID1 = ar_storage_module:id({ar_block:partition_size(), 1, default}),
		Tasks = [{I * 100, (I + 1) * 100, Peer1, StoreID1, none} || I <- lists:seq(0, 99)],
		State0 = #state{
			peers = #{Peer1 => #peer{
				peer = Peer1,
				queued = queue:from_list(Tasks),
				max_dispatched = 8
			}},
			total_queued_count = length(Tasks),
			total_dispatched_count = 10,
			workers = queue:from_list(lists:seq(1, 10))
		},
		
		%% Test with high max_queue (infinity case - TotalThroughput = 0)
		Performance1 = #performance{ current_rating = 10 },
		State1 = cut_peer_queue(Peer1, Performance1, 0, State0),
		PeerTasks1 = get_peer_tasks(Peer1, State1),
		assert_peer_tasks(Tasks, 0, 8, PeerTasks1),
		?assertEqual(100, State1#state.total_queued_count),

		%% Test cutting queue to 20
		Performance2 = #performance{ current_rating = 1 },
		State2 = cut_peer_queue(Peer1, Performance2, 100, State0),
		PeerTasks2 = get_peer_tasks(Peer1, State2),
		assert_peer_tasks(lists:sublist(Tasks, 1, 20), 0, 8, PeerTasks2),
		?assertEqual(20, State2#state.total_queued_count),

		%% Test with total_dispatched_count = 0 (should not cut)
		State3 = cut_peer_queue(Peer1, Performance2, 100, State0#state{ total_dispatched_count = 0 }),
		PeerTasks3 = get_peer_tasks(Peer1, State3),
		assert_peer_tasks(Tasks, 0, 8, PeerTasks3),
		?assertEqual(100, State3#state.total_queued_count),

		%% Test with infinity max_queue (TotalThroughput = 0)
		State4 = cut_peer_queue(Peer1, Performance1, 0, State0),
		PeerTasks4 = get_peer_tasks(Peer1, State4),
		assert_peer_tasks(Tasks, 0, 8, PeerTasks4),
		?assertEqual(100, State4#state.total_queued_count)
	after
		arweave_config:set_env(OriginalConfig)
	end.

test_update_dispatched() ->
	Workers20 = queue:from_list(lists:seq(1, 20)),
	Workers10 = queue:from_list(lists:seq(1, 10)),
	Peer1 = "peer1",
	
	State1 = #state{
		workers = Workers20,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 10,
			dispatched_count = 10,
			queued = queue:from_list(lists:seq(1, 30))
		}}
	},
	State1Result = update_max_dispatched(Peer1, #performance{average_latency = 100}, 20, 200, State1),
	PeerTasks1 = get_peer_tasks(Peer1, State1Result),
	?assertEqual(11, PeerTasks1#peer.max_dispatched),
	
	State2 = #state{
		workers = Workers20,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 10,
			dispatched_count = 20,
			queued = queue:from_list(lists:seq(1, 30))
		}}
	},
	State2Result = update_max_dispatched(Peer1, #performance{average_latency = 300}, 20, 200, State2),
	PeerTasks2 = get_peer_tasks(Peer1, State2Result),
	?assertEqual(9, PeerTasks2#peer.max_dispatched),

	State3 = #state{
		workers = Workers20,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 10,
			dispatched_count = 20,
			queued = queue:from_list(lists:seq(1, 30))
		}}
	},
	State3Result = update_max_dispatched(Peer1, #performance{average_latency = 300}, 10, 200, State3),
	PeerTasks3 = get_peer_tasks(Peer1, State3Result),
	?assertEqual(11, PeerTasks3#peer.max_dispatched),
	
	State4 = #state{
		workers = Workers10,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 10,
			dispatched_count = 20,
			queued = queue:from_list(lists:seq(1, 30))
		}}
	},
	State4Result = update_max_dispatched(Peer1, #performance{average_latency = 100}, 10, 200, State4),
	PeerTasks4 = get_peer_tasks(Peer1, State4Result),
	?assertEqual(10, PeerTasks4#peer.max_dispatched),
	
	State5 = #state{
		workers = Workers20,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 10,
			dispatched_count = 5,
			queued = queue:from_list(lists:seq(1, 10))
		}}
	},
	State5Result = update_max_dispatched(Peer1, #performance{average_latency = 100}, 20, 200, State5),
	PeerTasks5 = get_peer_tasks(Peer1, State5Result),
	?assertEqual(10, PeerTasks5#peer.max_dispatched),
	
	State6 = #state{
		workers = Workers20,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 10,
			dispatched_count = 10,
			queued = queue:from_list(lists:seq(1, 5))
		}}
	},
	State6Result = update_max_dispatched(Peer1, #performance{average_latency = 100}, 20, 200, State6),
	PeerTasks6 = get_peer_tasks(Peer1, State6Result),
	?assertEqual(10, PeerTasks6#peer.max_dispatched),

	State7 = #state{
		workers = Workers20,
		peers = #{Peer1 => #peer{
			peer = Peer1,
			max_dispatched = 8,
			dispatched_count = 20,
			queued = queue:from_list(lists:seq(1, 30))
		}}
	},
	State7Result = update_max_dispatched(Peer1, #performance{average_latency = 300}, 20, 200, State7),
	PeerTasks7 = get_peer_tasks(Peer1, State7Result),
	?assertEqual(8, PeerTasks7#peer.max_dispatched).

test_calculate_targets() ->
	State0 = #state{},
	Result1 = calculate_targets([], #{}, State0),
	?assertEqual({0.0, 0.0, 0}, Result1),

	State2 = #state{
		peers = #{
			"peer1" => #peer{max_dispatched = 8},
			"peer2" => #peer{max_dispatched = 10}
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
			"peer1" => #peer{max_dispatched = 8}
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
			"peer1" => #peer{max_dispatched = 12}
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

assert_peer_tasks(ExpectedQueue, ExpectedDispatchedCount, ExpectedMaxDispatched, PeerTasks) ->
	?assertEqual(ExpectedQueue, queue:to_list(PeerTasks#peer.queued)),
	?assertEqual(ExpectedDispatchedCount, PeerTasks#peer.dispatched_count),
	?assertEqual(ExpectedMaxDispatched, PeerTasks#peer.max_dispatched).
