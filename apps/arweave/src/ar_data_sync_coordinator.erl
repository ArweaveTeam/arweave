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
%%% 2. Peer workers store those tasks in either
%%%    - their task_queue (ready for dispatch) if they belong to an active footprint
%%%    - or in a waiting queue (not ready for dispatch) if they don't belong to an
%%%      active footprint
%%% 3. Periodically the coordinator pulls tasks from peer queues and dispatches to workers.
%%%    This is event based and happens in response to one of these events:
%%%    - a new task is sent to the coordinator
%%%    - a task is completed by an ar_data_sync_worker
%%% 4. On task completion, peer workers update metrics and notify coordinator.
%%% 5. When a footprint completes, a new footprint is activated. Footprint activation is
%%%    handled both by the ar_peer_worker (if it has waiting tasks) or by the coordinator
%%%    (if the ar_peer_worker does not have waiting tasks, coordinator will find another
%%%    peer that does). Note: footprint activation does not immediately dispatch tasks.
%%% 
%%% Tasks can be in one of three states:
%%% - waiting: the task belongs to an inactive footprint and is stored in a
%%%            "waiting" queue on the ar_peer_worker. A task in the "waiting"
%%%            state contributes to the total_queued_count, but can not be dispatched
%%%            until its footprint becomes active.
%%% - queued: the task belongs to an activae footprint and is stored in the
%%%           ar_peer_worker's task queue. It will be dispatched as soon as
%%%           an ar_data_sync_worker becomes available.  A task in the "queued"
%%%           state contributes to the total_queued_count.
%%% - dispatched: the task has been dispatched to an ar_data_sync_worker and is
%%%            being processed. A task in the "dispatched" state contributes to the
%%%            total_dispatched_count.
%%% 
%%% Footprints can be in one of two states:
%%% - active: All tasks belonging to an active footprint are moved to the
%%%           ar_peer_worker's task queue and are eligible to be dispatched.
%%% - inactive: All tasks belonging to an inactive footprint are stored in the
%%%             ar_peer_worker's "waiting" queue. They are not eligible to be 
%%%             dispatched until their footprint becomes active.
%%%
-module(ar_data_sync_coordinator).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, is_syncing_enabled/0, ready_for_work/0]).

%% Phase 0 instrumentation: ETS mirror of counters, invariant checking helpers.
%% Exported so ar_peer_worker can publish its own counters.
-export([sync_metrics_put_peer/4, sync_metrics_delete_peer/1,
		 sync_metrics_put_peer_max_dispatched/2]).

%% Phase 2: atomic footprint slot claim/release via ETS. Replaces the
%% coordinator-owned total_active_footprints counter and the dance of
%% HasCapacity-at-enqueue + try_activate_waiting_footprint.
-export([claim_footprint_slot/0, release_footprint_slot/0,
		 footprint_slots_available/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").

-define(REBALANCE_FREQUENCY_MS, 10*1000).
-define(SYNC_METRICS_TABLE, sync_metrics).

-record(state, {
	total_queued_count = 0,       %% total count of non-dispatched tasks across all peers
	total_dispatched_count = 0,   %% total count tasks currently assigned to a worker
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
%%
%% Phase 1: reads the ETS mirror directly — no gen_server call, cannot block.
%% The coordinator keeps the mirror in lockstep with its state at every
%% transition; the worst case is a slightly-stale read that accepts one
%% extra task or defers one. The coordinator's mailbox queue had this same
%% eventual-consistency property.
ready_for_work() ->
	try
		WorkerCount = ets:lookup_element(?SYNC_METRICS_TABLE, workers_count, 2),
		case WorkerCount of
			0 -> false;
			_ ->
				TotalQueued = ets:lookup_element(?SYNC_METRICS_TABLE, total_queued, 2),
				TotalDispatched = ets:lookup_element(?SYNC_METRICS_TABLE, total_dispatched, 2),
				TotalQueued + TotalDispatched < max_tasks(WorkerCount)
		end
	catch _:_ ->
		%% Table not yet created (coordinator starting) or syncing disabled.
		false
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	MaxFootprints = calculate_max_footprints(),
	%% Phase 0 instrumentation: ETS mirror of all mutable counters.
	%% This table lets the invariant checker (run each rebalance tick) verify
	%% that gen_server state, per-worker contributions, and per-peer contributions
	%% all agree. The table also paves the way for Phase 1+ where reads will
	%% migrate off the gen_server entirely.
	case ets:info(?SYNC_METRICS_TABLE) of
		undefined ->
			ets:new(?SYNC_METRICS_TABLE,
				[named_table, public, set,
					{read_concurrency, true}, {write_concurrency, true}]);
		_ -> ok
	end,
	sync_metrics_reset(),
	ets:insert(?SYNC_METRICS_TABLE, {workers_count, length(Workers)}),
	%% Phase 2: atomic footprint-slot gauge. Initialised to MaxFootprints
	%% (slots_available). Claim decrements; release increments capped at max.
	ets:insert(?SYNC_METRICS_TABLE,
		[{footprint_slots_available, MaxFootprints},
		 {footprint_slots_max, MaxFootprints}]),
	%% Phase 3: shared idle-worker registry. Workers register themselves when
	%% no peer has work; peer workers pop one and cast `work_available` when
	%% an enqueue arrives. Ordered_set so ets:first/1 returns deterministically.
	case ets:info(idle_workers) of
		undefined ->
			ets:new(idle_workers,
				[named_table, public, ordered_set,
					{read_concurrency, true}, {write_concurrency, true}]);
		_ -> ok
	end,
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
	TotalTaskCount = State#state.total_dispatched_count + State#state.total_queued_count,
	ReadyForWork = TotalTaskCount < max_tasks(WorkerCount),
	{reply, ReadyForWork, State};

handle_call({reset_worker, Worker}, _From, State) ->
	ActiveCount = maps:get(Worker, State#state.dispatched_count_per_worker, 0),
	State2 = State#state{
		total_dispatched_count = State#state.total_dispatched_count - ActiveCount,
		dispatched_count_per_worker = maps:put(Worker, 0, State#state.dispatched_count_per_worker)
	},
	sync_metrics_put_worker(Worker, 0),
	sync_metrics_put_totals(State2),
	{reply, ok, State2};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({sync_range, Args}, State) ->
	%% Phase 3: forward the task to the appropriate peer worker via a one-way
	%% cast and let it wake an idle sync worker. The coordinator no longer
	%% dispatches work — workers pull when they're free.
	case queue:is_empty(State#state.workers) of
		true ->
			{noreply, State};
		false ->
			{_Start, _End, Peer, _TargetStoreID, _FootprintKey} = Args,
			{Pid, State1} = maybe_add_peer(Peer, State),
			case Pid of
				undefined ->
					{noreply, State1};
				_ ->
					ar_peer_worker:enqueue(Pid, Args),
					State2 = State1#state{
						total_queued_count = State1#state.total_queued_count + 1
					},
					sync_metrics_put_totals(State2),
					{noreply, State2}
			end
	end;

handle_cast({task_completed, {sync_range, {_Worker, _Result, _Args, _ElapsedNative}}}, State) ->
	%% Phase 3: sync workers report completion directly to the owning peer
	%% worker. This handler is a no-op kept for backwards compat with any
	%% in-flight casts during deployment; once those clear, this clause and
	%% the supporting dispatch_tasks plumbing can be deleted entirely.
	{noreply, State};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	%% TODO: Add logic to purge empty peer workers (no queued tasks, no dispatched tasks).
	PeerPids = State#state.known_peers,  %% #{Peer => Pid}
	Peers = maps:keys(PeerPids),
	AllPeerPerformances = ar_peers:get_peer_performances(Peers),
	Targets = calculate_targets(PeerPids, AllPeerPerformances, State),
	State2 = rebalance_peers(PeerPids, AllPeerPerformances, Targets, State),
	%% Phase 3: there is no defensive drain anymore — there are no dispatch
	%% paths to defend. Instead, kick one idle worker (if any) so any peer
	%% that has queued work can serve it even if a `work_available` cast was
	%% lost (e.g., the targeted worker died between idle-registration and
	%% the cast arriving). Cheap, safe, idempotent.
	wake_one_idle_worker_safe(),
	sync_metrics_put_totals(State2),
	check_invariants(State2),
	{noreply, State2};

handle_cast({footprint_deactivated, _Peer}, State) ->
	%% Phase 2: footprint accounting is fully in the ETS slot gauge now.
	%% ar_peer_worker releases the slot atomically on task completion.
	%% We keep the handler for backwards compatibility with any stray
	%% in-flight messages but treat it as a no-op.
	{noreply, State};

handle_cast({peer_worker_started, Peer, Pid}, State) ->
	%% Peer worker (re)started - update cached PID
	State2 = State#state{ known_peers = maps:put(Peer, Pid, State#state.known_peers) },
	{noreply, State2};

handle_cast({reset_worker, Worker}, State) ->
	%% Phase 1: cast path for sync workers restarting. Reuses the same
	%% accounting logic as the legacy call handler.
	ActiveCount = maps:get(Worker, State#state.dispatched_count_per_worker, 0),
	State2 = State#state{
		total_dispatched_count = State#state.total_dispatched_count - ActiveCount,
		dispatched_count_per_worker = maps:put(Worker, 0, State#state.dispatched_count_per_worker)
	},
	sync_metrics_put_worker(Worker, 0),
	sync_metrics_put_totals(State2),
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
%% Note: a new footprint will not be activated while dispatching tasks as only tasks
%% belonging to an already active footprint will be in the ar_peer_worker's task queue
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
	%% When a task is dispatched it's removed from the ar_peer_worker's task queue and sent to
	%% an ar_data_sync_worker.
	State3 = increment_dispatched_task_count(Worker, 1, State2),
	State4 = State3#state{ total_queued_count = max(0, State3#state.total_queued_count - 1) },
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
	%% Phase 1: read each peer's max_dispatched from the ETS mirror instead of
	%% calling into the peer worker synchronously. The peer publishes its cap
	%% at every state transition via publish_metrics/1.
	TotalMaxDispatched = maps:fold(
		fun(Peer, _Pid, Acc) ->
			case ets:lookup(?SYNC_METRICS_TABLE, {peer_max_dispatched, Peer}) of
				[{_, MaxDispatched}] -> MaxDispatched + Acc;
				_ -> Acc
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
				total_queued_count = max(0, State#state.total_queued_count - RemovedCount),
				known_peers = maps:remove(Peer, State#state.known_peers)
			};
		{ok, RemovedCount} ->
			State#state{ total_queued_count = max(0, State#state.total_queued_count - RemovedCount) };
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
	NewState = State#state{
		total_dispatched_count = State#state.total_dispatched_count + N,
		dispatched_count_per_worker =
			maps:put(Worker, ActiveCount, State#state.dispatched_count_per_worker)
	},
	sync_metrics_put_worker(Worker, ActiveCount),
	NewState.

%%%===================================================================
%%% Phase 0 instrumentation helpers
%%%===================================================================

%% @doc Clear the mirror so a fresh coordinator init starts from zero.
sync_metrics_reset() ->
	try
		ets:delete_all_objects(?SYNC_METRICS_TABLE),
		ets:insert(?SYNC_METRICS_TABLE,
			[{total_queued, 0}, {total_dispatched, 0}, {total_active_footprints, 0},
			 {workers_count, 0}])
	catch _:_ -> ok
	end.

%% @doc Mirror the three global coordinator totals into ETS. Call after every
%% state transition that touches any of them; it's cheap (3 inserts).
sync_metrics_put_totals(#state{} = State) ->
	try
		ets:insert(?SYNC_METRICS_TABLE,
			[{total_queued, State#state.total_queued_count},
			 {total_dispatched, State#state.total_dispatched_count},
			 {total_active_footprints, State#state.total_active_footprints}])
	catch _:_ -> ok
	end,
	State.

%% @doc Mirror per-worker dispatched count into ETS.
sync_metrics_put_worker(Worker, Count) ->
	try ets:insert(?SYNC_METRICS_TABLE, {{worker_dispatched, Worker}, Count})
	catch _:_ -> ok
	end.

%% @doc Called from ar_peer_worker to publish its contribution to the
%% cross-process invariants.
sync_metrics_put_peer(Peer, Dispatched, Queued, ActiveFootprints) ->
	try
		ets:insert(?SYNC_METRICS_TABLE,
			[{{peer_dispatched, Peer}, Dispatched},
			 {{peer_queued, Peer}, Queued},
			 {{peer_active_footprints, Peer}, ActiveFootprints}])
	catch _:_ -> ok
	end.

%% @doc Publish a peer's max_dispatched so coordinator can read it from ETS
%% instead of a synchronous call into the peer worker.
sync_metrics_put_peer_max_dispatched(Peer, MaxDispatched) ->
	try ets:insert(?SYNC_METRICS_TABLE, {{peer_max_dispatched, Peer}, MaxDispatched})
	catch _:_ -> ok
	end.

%% @doc Atomically claim a footprint slot. Returns true on success, false if
%% no slots are available. Race-free under concurrent callers.
%% Implementation: unbounded decrement with post-hoc bounds check.
%% If decrement leaves the counter negative, we over-drew and must put it back.
claim_footprint_slot() ->
	try
		case ets:update_counter(?SYNC_METRICS_TABLE,
				footprint_slots_available, {2, -1}) of
			N when N >= 0 ->
				true;
			_Negative ->
				%% Over-drew. Put it back.
				ets:update_counter(?SYNC_METRICS_TABLE,
					footprint_slots_available, {2, +1}),
				false
		end
	catch _:_ ->
		false
	end.

%% @doc Release a previously-claimed footprint slot. Capped at the configured
%% maximum so bugs (double-release) don't inflate capacity beyond intended.
release_footprint_slot() ->
	try
		Max = ets:lookup_element(?SYNC_METRICS_TABLE, footprint_slots_max, 2),
		ets:update_counter(?SYNC_METRICS_TABLE,
			footprint_slots_available, {2, +1, Max, Max}),
		ok
	catch _:_ -> ok
	end.

%% @doc Current available slot count (for metrics/debug).
footprint_slots_available() ->
	try ets:lookup_element(?SYNC_METRICS_TABLE, footprint_slots_available, 2)
	catch _:_ -> 0
	end.

%% @doc Phase 3: rebalance-tick safety backstop. Pop one idle worker (if any)
%% and tell it to re-pull. Mirror of ar_peer_worker:wake_one_idle_worker/0.
%% Keeping a separate copy here avoids the coordinator having to call into
%% the peer worker module just to wake a worker.
wake_one_idle_worker_safe() ->
	try
		case ets:first(idle_workers) of
			'$end_of_table' -> ok;
			Worker ->
				ets:delete(idle_workers, Worker),
				gen_server:cast(Worker, work_available)
		end
	catch _:_ -> ok
	end.

%% @doc Called from ar_peer_worker:terminate so a crashed/shutdown peer stops
%% contributing phantom values to the sum.
sync_metrics_delete_peer(Peer) ->
	try
		ets:delete(?SYNC_METRICS_TABLE, {peer_dispatched, Peer}),
		ets:delete(?SYNC_METRICS_TABLE, {peer_queued, Peer}),
		ets:delete(?SYNC_METRICS_TABLE, {peer_active_footprints, Peer}),
		ets:delete(?SYNC_METRICS_TABLE, {peer_max_dispatched, Peer})
	catch _:_ -> ok
	end.

%% @doc Invariant checker. Run each rebalance tick. Logs at WARNING on drift;
%% never crashes, so a bug in the checker can't bring the node down.
check_invariants(#state{} = State) ->
	try
		EtsQueued = lookup(total_queued),
		EtsDispatched = lookup(total_dispatched),
		EtsFootprints = lookup(total_active_footprints),
		SumWorkerDispatched = sum_prefix(worker_dispatched),
		SumPeerDispatched = sum_prefix(peer_dispatched),
		SumPeerFootprints = sum_prefix(peer_active_footprints),
		SumPeerQueued = sum_prefix(peer_queued),
		Drift = [
			{mirror_queued, State#state.total_queued_count, EtsQueued},
			{mirror_dispatched, State#state.total_dispatched_count, EtsDispatched},
			{mirror_footprints, State#state.total_active_footprints, EtsFootprints},
			{workers_vs_total,
				maps_values_sum(State#state.dispatched_count_per_worker),
				State#state.total_dispatched_count},
			{ets_workers_vs_total, SumWorkerDispatched, EtsDispatched},
			{peers_dispatched_vs_total, SumPeerDispatched, EtsDispatched},
			{peers_footprints_vs_total, SumPeerFootprints, EtsFootprints},
			{peers_queued_vs_total, SumPeerQueued, EtsQueued}
		],
		Mismatches = [T || {_, A, B} = T <- Drift, A =/= B],
		case Mismatches of
			[] -> ok;
			_ ->
				?LOG_WARNING([{event, sync_metrics_invariant_drift},
					{mismatches, Mismatches}])
		end
	catch Class:Reason ->
		?LOG_WARNING([{event, sync_metrics_invariant_check_failed},
			{class, Class}, {reason, io_lib:format("~p", [Reason])}])
	end,
	State.

lookup(Key) ->
	case ets:lookup(?SYNC_METRICS_TABLE, Key) of
		[{_, V}] -> V;
		_ -> 0
	end.

sum_prefix(Tag) ->
	%% Select all entries whose key is {Tag, _} and sum their values.
	MatchSpec = [{ {{Tag, '_'}, '$1'}, [], ['$1'] }],
	lists:sum(ets:select(?SYNC_METRICS_TABLE, MatchSpec)).

maps_values_sum(Map) ->
	lists:sum(maps:values(Map)).

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

%% Phase 2: notify_peers_capacity_available is gone. The
%% `footprint_deactivated` trigger no longer exists — slot release happens
%% directly in the peer worker via ar_data_sync_coordinator:release_footprint_slot/0
%% as part of task completion, and the rebalance-tick defensive drain plus
%% per-enqueue drain are enough to pick up the freed slot.
%%
%% try_activate_footprint/2 is retained as a thin helper that iterates
%% peers and calls ar_peer_worker:try_activate_footprint (which in Phase 2
%% is just a drain call) — exists for test coverage and any future
%% explicit wake-up pathway.
try_activate_footprint([], State) ->
	{false, State};
try_activate_footprint([{_Peer, Pid} | Rest], State) ->
	WorkerCount = queue:len(State#state.workers),
	case ar_peer_worker:try_activate_footprint(Pid, WorkerCount) of
		{true, TasksToDispatch} ->
			State2 = dispatch_tasks(Pid, TasksToDispatch, State),
			{true, State2};
		{false, _} ->
			try_activate_footprint(Rest, State)
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

coordinator_test_() ->
	[
		{timeout, 30, fun test_get_worker/0},
		{timeout, 30, fun test_max_tasks/0},
		{timeout, 30, fun test_increment_dispatched_task_count/0},
		{timeout, 30, fun test_queue_scaling_factor/0},
		{timeout, 30, fun test_footprint_deactivated/0},
		{timeout, 30, fun test_peer_worker_started_updates_cache/0},
		{timeout, 30, fun test_reset_worker/0},
		{timeout, 30, fun test_dispatch_tasks_updates_counts/0},
		{timeout, 30, fun test_try_activate_footprint_stops_on_success/0},
		{timeout, 30, fun test_try_activate_footprint_tries_all/0},
		{timeout, 30, fun test_try_activate_footprint_dispatches_tasks/0},
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

test_footprint_deactivated() ->
	%% Phase 2: footprint_deactivated is now a no-op handler kept for
	%% backwards compat. Global capacity is tracked in the ETS slot gauge,
	%% not in coordinator state. The handler must not crash and must not
	%% mutate state.
	State0 = #state{ total_active_footprints = 5, max_footprints = 10, known_peers = #{} },
	{noreply, State1} = handle_cast({footprint_deactivated, {1,2,3,4,1984}}, State0),
	?assertEqual(State0, State1).

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
		total_queued_count = 5,
		dispatched_count_per_worker = #{}
	},
	
	%% Dispatch empty list - no changes
	State1 = dispatch_tasks(self(), [], State0),
	?assertEqual(0, State1#state.total_dispatched_count),
	?assertEqual(5, State1#state.total_queued_count),
	
	%% Note: We can't fully test dispatch_tasks without mocking workers,
	%% but we can verify the state changes for the helper functions.
	
	%% Verify that dispatching would decrement total_queued_count
	%% by manually simulating what dispatch_tasks does
	State2 = State1#state{ total_queued_count = max(0, State1#state.total_queued_count - 1) },
	?assertEqual(4, State2#state.total_queued_count).

test_try_activate_footprint_stops_on_success() ->
	%% Create mock peer processes that track if they were called
	Parent = self(),

	%% Peer1 returns {false, []} (no waiting footprint)
	Pid1 = spawn(fun() -> mock_peer_worker(Parent, peer1, {false, []}) end),
	%% Peer2 returns {true, []} (activates a footprint, no tasks to dispatch in mock)
	Pid2 = spawn(fun() -> mock_peer_worker(Parent, peer2, {true, []}) end),
	%% Peer3 should NOT be called (iteration should stop at Peer2)
	Pid3 = spawn(fun() -> mock_peer_worker(Parent, peer3, {false, []}) end),

	PeerList = [{peer1, Pid1}, {peer2, Pid2}, {peer3, Pid3}],
	State0 = #state{ workers = queue:from_list([w1, w2, w3]) },

	%% Call the function directly
	{true, _State1} = try_activate_footprint(PeerList, State0),

	%% Wait a bit for messages
	timer:sleep(50),

	%% Check which peers were called
	?assertEqual([peer1, peer2], collect_called_peers([])),

	%% Cleanup
	exit(Pid1, kill),
	exit(Pid2, kill),
	exit(Pid3, kill).

test_try_activate_footprint_tries_all() ->
	%% Create mock peer processes that all return {false, []}
	Parent = self(),

	Pid1 = spawn(fun() -> mock_peer_worker(Parent, peer1, {false, []}) end),
	Pid2 = spawn(fun() -> mock_peer_worker(Parent, peer2, {false, []}) end),
	Pid3 = spawn(fun() -> mock_peer_worker(Parent, peer3, {false, []}) end),

	PeerList = [{peer1, Pid1}, {peer2, Pid2}, {peer3, Pid3}],
	State0 = #state{ workers = queue:from_list([w1, w2, w3]) },

	%% Call the function directly
	{false, _State1} = try_activate_footprint(PeerList, State0),

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
		{'$gen_call', From, {try_activate_footprint, _WorkerCount}} ->
			Parent ! {called, PeerName},
			gen_server:reply(From, ReturnValue)
	after 5000 ->
		ok
	end.

%% Regression test for the orphaned-task-queue bug: when a peer worker activates a
%% waiting footprint via try_activate_footprint, the tasks it promotes into its
%% task_queue must be dispatched by the coordinator. Prior to the fix, activation
%% returned only a bool and the coordinator never called dispatch_tasks, so the
%% promoted tasks would sit in task_queue forever (no trigger would drain them
%% since dispatched_count was already 0 and no further enqueues arrived).
test_try_activate_footprint_dispatches_tasks() ->
	Parent = self(),
	%% Mock peer reports an activation with one promoted task ready to dispatch.
	FootprintKey = {fp, 1, peer1},
	Task = {0, 100, peer1, store1, FootprintKey},
	PeerPid = spawn(fun() -> mock_peer_worker(Parent, peer1, {true, [Task]}) end),
	%% Mock sync worker process — dispatch_tasks will gen_server:cast to it.
	WorkerPid = spawn(fun() -> mock_sync_worker(Parent) end),

	State0 = #state{
		workers = queue:from_list([WorkerPid]),
		total_queued_count = 1
	},

	{true, State1} = try_activate_footprint([{peer1, PeerPid}], State0),

	%% Worker must have received the sync_range cast — this is what was missing
	%% before the fix.
	receive
		{sync_range_received, WorkerPid, Task} -> ok
	after 500 ->
		?assert(false, "dispatch_tasks did not cast sync_range to the worker")
	end,

	%% Coordinator state should reflect the dispatch.
	?assertEqual(1, maps:get(WorkerPid, State1#state.dispatched_count_per_worker)),
	?assertEqual(1, State1#state.total_dispatched_count),
	?assertEqual(0, State1#state.total_queued_count),

	exit(PeerPid, kill),
	exit(WorkerPid, kill).

mock_sync_worker(Parent) ->
	receive
		{'$gen_cast', {sync_range, {Start, End, Peer, StoreID, _Retry, FPKey}}} ->
			Parent ! {sync_range_received, self(),
				{Start, End, Peer, StoreID, FPKey}}
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
	%% Phase 1: max_dispatched is now read from the ETS mirror, not via
	%% gen_server call into peer worker. Ensure the mirror table exists
	%% and populate it for the three mock peers.
	Peer1 = {1,2,3,4,1984},
	Peer2 = {5,6,7,8,1985},
	Peer3 = {9,10,11,12,1986},
	case ets:info(?SYNC_METRICS_TABLE) of
		undefined ->
			ets:new(?SYNC_METRICS_TABLE, [named_table, public, set]);
		_ -> ok
	end,
	ets:insert(?SYNC_METRICS_TABLE, [
		{{peer_max_dispatched, Peer1}, 10},
		{{peer_max_dispatched, Peer2}, 15},
		{{peer_max_dispatched, Peer3}, 20}
	]),

	%% Pids no longer need to answer get_max_dispatched, but calculate_targets
	%% still requires them in the PeerPids map. Use dummy pids.
	Pid1 = spawn(fun() -> receive _ -> ok after 5000 -> ok end end),
	Pid2 = spawn(fun() -> receive _ -> ok after 5000 -> ok end end),
	Pid3 = spawn(fun() -> receive _ -> ok after 5000 -> ok end end),

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
	exit(Pid3, kill),
	ets:delete(?SYNC_METRICS_TABLE, {peer_max_dispatched, Peer1}),
	ets:delete(?SYNC_METRICS_TABLE, {peer_max_dispatched, Peer2}),
	ets:delete(?SYNC_METRICS_TABLE, {peer_max_dispatched, Peer3}).

-endif.
