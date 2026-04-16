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

%% Phase 0 instrumentation: ETS mirror of counters, anomaly detection helpers.
%% Exported so ar_peer_worker can publish its own counters.
-export([record_peer_load/5, remove_peer/1]).

%% Phase 2: atomic footprint slot claim/release via ETS. Replaces the
%% coordinator-owned total_active_footprints counter and the dance of
%% HasCapacity-at-enqueue + try_activate_waiting_footprint.
-export([claim_footprint_slot/0, release_footprint_slot/0]).


-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-define(REBALANCE_FREQUENCY_MS, 10*1000).
-define(WORKER_LOAD_TABLE, worker_load).

%% Phase 4: coordinator state collapses to roster + config.
%% Counters live in ETS (worker_load, footprint_slots_*) — see Phase 0–2.
%% The `workers` queue is gone with the push dispatcher; sync workers are
%% looked up by name on demand and the count is published once at init.
-record(state, {
	known_peers = #{},     %% Peer => Pid — canonical peer worker roster
	max_footprints = 0,    %% retained for periodic anomaly logging
	worker_names = []      %% registered names of all sync workers (for reset_worker etc.)
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

%% @doc Returns true if we can accept new tasks. Will always return false if
%% syncing is disabled (i.e. sync_jobs = 0).
%%
%% Phase 4: derived from per-peer ETS rows. Each peer worker is the sole
%% writer of its own counters, so summing them is race-free at the
%% per-key level. The sum is non-atomic across keys, but ready_for_work
%% is back-pressure — a slightly-stale read just accepts one extra task
%% or defers one (the coordinator's mailbox already had this property).
%% O(N) in peer count; cheap with ~100 peers.
ready_for_work() ->
	try
		WorkerCount = ets:lookup_element(?WORKER_LOAD_TABLE, workers_count, 2),
		case WorkerCount of
			0 -> false;
			_ ->
				TotalQueuedTasks = sum_prefix(peer_queued),
				TotalDispatchedTasks = sum_prefix(peer_dispatched),
				TotalQueuedTasks + TotalDispatchedTasks < max_tasks(WorkerCount)
		end
	catch _:_ ->
		false
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	MaxFootprints = calculate_max_footprints(),
	%% worker_load ETS table is created in ar_data_sync_sup.
	%% Clear stale entries from a previous coordinator incarnation.
	ets:delete_all_objects(?WORKER_LOAD_TABLE),
	ets:insert(?WORKER_LOAD_TABLE, {workers_count, length(Workers)}),
	ets:insert(?WORKER_LOAD_TABLE,
		[{footprint_slots_available, MaxFootprints},
		 {footprint_slots_max, MaxFootprints}]),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {workers, Workers},
		{max_footprints, MaxFootprints}]),
	{ok, #state{
		max_footprints = MaxFootprints,
		worker_names = Workers
	}}.

calculate_max_footprints() ->
	{ok, Config} = arweave_config:get_env(),
	%% Calculate global max footprints based on entropy cache size
	FootprintSize = ar_block:get_replica_2_9_footprint_size(),
	max(1, (Config#config.replica_2_9_entropy_cache_size_mb * ?MiB) div FootprintSize).


handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

%% Phase 4/5: thin forwarder. Look up (or create) the peer worker and cast
%% the task. The peer worker's `record_peer_load` updates its own per-peer
%% ETS rows; `ready_for_work/0` derives the global total from the per-peer
%% sum on demand. No coordinator-owned counter to maintain.
handle_cast({sync_range, SyncTask}, State) ->
	#sync_task{ peer = Peer } = SyncTask,
	{Pid, State1} = maybe_add_peer(Peer, State),
	case Pid of
		undefined -> ok;
		_ -> ar_peer_worker:enqueue(Pid, SyncTask)
	end,
	{noreply, State1};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	PeerPids = State#state.known_peers,
	Peers = maps:keys(PeerPids),
	AllPeerPerformances = ar_peers:get_peer_performances(Peers),
	Targets = calculate_targets(PeerPids, AllPeerPerformances, State),
	State2 = rebalance_peers(PeerPids, AllPeerPerformances, Targets, State),
	log_anomalies(State2),
	{noreply, State2};

handle_cast({peer_worker_started, Peer, Pid}, State) ->
	State2 = State#state{ known_peers = maps:put(Peer, Pid, State#state.known_peers) },
	{noreply, State2};

%% Phase 3+: legacy shims kept for in-flight messages during deploy. All
%% no-ops now; deletion can wait until any queued casts in older binaries
%% have drained.
handle_cast({task_completed, {sync_range, _}}, State) ->
	{noreply, State};
handle_cast({footprint_deactivated, _Peer}, State) ->
	{noreply, State};
handle_cast({reset_worker, _Worker}, State) ->
	{noreply, State};

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
%% Helpers
%%--------------------------------------------------------------------

%% @doc the maximum number of tasks we can have in process.
max_tasks(WorkerCount) ->
	WorkerCount * 50.

%%--------------------------------------------------------------------
%% Rebalancing
%%--------------------------------------------------------------------

%% @doc Calculate rebalance parameters.
%% PeerPids is #{Peer => Pid}
%% Returns {WorkerCount, TargetLatency, TotalThroughput, TotalMaxDispatched}
calculate_targets(PeerPids, AllPeerPerformances, State) ->
	WorkerCount = length(State#state.worker_names),
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
	%% at every state transition via record_peer_load/1.
	TotalMaxDispatched = maps:fold(
		fun(Peer, _Pid, Acc) ->
			ets:lookup_element(?WORKER_LOAD_TABLE,
				{peer_max_dispatched, Peer}, 2, 0) + Acc
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
	QueueScalingFactor = queue_scaling_factor(TotalThroughput, WorkerCount),
	WorkersStarved = TotalMaxDispatched < WorkerCount,
	RebalanceParams = {QueueScalingFactor, TargetLatency, WorkersStarved},
	Result = ar_peer_worker:rebalance(Pid, Performance, RebalanceParams),
	%% Phase 4: cuts to total_queued go through the peer worker's
	%% record_peer_load path; coordinator only tracks roster membership.
	State2 = case Result of
		{shutdown, _RemovedCount} ->
			?LOG_INFO([{event, shutdown_idle_peer_worker},
				{peer, ar_util:format_peer(Peer)}]),
			ar_peer_worker:stop(Pid),
			State#state{ known_peers = maps:remove(Peer, State#state.known_peers) };
		{ok, _RemovedCount} ->
			State;
		{error, timeout} ->
			State
	end,
	rebalance_peers2(Rest, AllPeerPerformances, Targets, State2).

%% @doc Scaling factor for calculating per-peer max queue size.
%% Peer worker calculates: MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
queue_scaling_factor(0, _WorkerCount) -> infinity;
queue_scaling_factor(0.0, _WorkerCount) -> infinity;
queue_scaling_factor(TotalThroughput, WorkerCount) ->
	max_tasks(WorkerCount) / TotalThroughput.

%%%===================================================================
%%% Phase 0/4 ETS helpers
%%%===================================================================

%% @doc Called from ar_peer_worker to publish its current load contribution
%% (in-flight, queued, active footprints, slow-peer cap) to the worker_load
%% ETS table. Consumed by dispatch/rebalance/anomaly logic.
record_peer_load(Peer, Dispatched, Queued, ActiveFootprints, MaxDispatched) ->
	try
		ets:insert(?WORKER_LOAD_TABLE,
			[{{peer_dispatched, Peer}, Dispatched},
			 {{peer_queued, Peer}, Queued},
			 {{peer_active_footprints, Peer}, ActiveFootprints},
			 {{peer_max_dispatched, Peer}, MaxDispatched}])
	catch _:_ -> ok
	end.

%% @doc Atomically claim a footprint slot. Returns true on success, false if
%% no slots are available. Race-free under concurrent callers.
%% Implementation: unbounded decrement with post-hoc bounds check.
%% If decrement leaves the counter negative, we over-drew and must put it back.
claim_footprint_slot() ->
	try
		case ets:update_counter(?WORKER_LOAD_TABLE,
				footprint_slots_available, {2, -1}) of
			N when N >= 0 ->
				true;
			_Negative ->
				%% Over-drew. Put it back.
				ets:update_counter(?WORKER_LOAD_TABLE,
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
		Max = ets:lookup_element(?WORKER_LOAD_TABLE, footprint_slots_max, 2),
		ets:update_counter(?WORKER_LOAD_TABLE,
			footprint_slots_available, {2, +1, Max, Max}),
		ok
	catch _:_ -> ok
	end.

%% @doc Called from ar_peer_worker:terminate so a crashed/shutdown peer stops
%% contributing phantom values to the sum.
remove_peer(Peer) ->
	try
		ets:delete(?WORKER_LOAD_TABLE, {peer_dispatched, Peer}),
		ets:delete(?WORKER_LOAD_TABLE, {peer_queued, Peer}),
		ets:delete(?WORKER_LOAD_TABLE, {peer_active_footprints, Peer}),
		ets:delete(?WORKER_LOAD_TABLE, {peer_max_dispatched, Peer})
	catch _:_ -> ok
	end.

%% @doc Phase 4: anomaly checker now compares per-peer ETS sums against
%% themselves (sanity check on the sole-writer ETS data) and against the
%% authoritative footprint slot gauge. The old gen_server-vs-mirror checks
%% are gone because the gen_server no longer caches counters.
log_anomalies(#state{} = _State) ->
	try
		SumPeerFootprints = sum_prefix(peer_active_footprints),
		SlotsAvailable = ets:lookup_element(?WORKER_LOAD_TABLE,
			footprint_slots_available, 2, 0),
		SlotsMax = ets:lookup_element(?WORKER_LOAD_TABLE,
			footprint_slots_max, 2, 0),
		SlotsClaimed = SlotsMax - SlotsAvailable,
		Drift = [
			{footprint_slot_accounting, SumPeerFootprints, SlotsClaimed}
		],
		Mismatches = [T || {_, A, B} = T <- Drift, A =/= B],
		case Mismatches of
			[] -> ok;
			_ ->
				?LOG_WARNING([{event, worker_load_anomaly_drift},
					{mismatches, Mismatches}])
		end
	catch Class:Reason ->
		?LOG_WARNING([{event, worker_load_anomaly_check_failed},
			{class, Class}, {reason, io_lib:format("~p", [Reason])}])
	end.

sum_prefix(Tag) ->
	%% Select all entries whose key is {Tag, _} and sum their values.
	MatchSpec = [{ {{Tag, '_'}, '$1'}, [], ['$1'] }],
	lists:sum(ets:select(?WORKER_LOAD_TABLE, MatchSpec)).

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

%% Phase 4: get_worker / cycle_workers / try_activate_footprint are gone.
%% Sync workers pull on their own in Phase 3+; the coordinator no longer
%% selects workers or dispatches tasks.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

coordinator_test_() ->
	[
		{timeout, 30, fun test_max_tasks/0},
		{timeout, 30, fun test_queue_scaling_factor/0},
		{timeout, 30, fun test_peer_worker_started_updates_cache/0},
		{timeout, 30, fun test_calculate_targets/0}
	].

test_max_tasks() ->
	?assertEqual(50, max_tasks(1)),
	?assertEqual(100, max_tasks(2)),
	?assertEqual(500, max_tasks(10)),
	?assertEqual(5000, max_tasks(100)).

test_queue_scaling_factor() ->
	?assertEqual(infinity, queue_scaling_factor(0, 10)),
	?assertEqual(infinity, queue_scaling_factor(0.0, 10)),
	?assertEqual(5.0, queue_scaling_factor(100.0, 10)),
	?assertEqual(2.5, queue_scaling_factor(200.0, 10)),
	?assertEqual(50.0, queue_scaling_factor(10.0, 10)).

test_peer_worker_started_updates_cache() ->
	Peer1 = {1,2,3,4,1984},
	Peer2 = {5,6,7,8,1985},
	Pid1 = self(),
	Pid2 = self(),

	State0 = #state{ known_peers = #{} },
	{noreply, State1} = handle_cast({peer_worker_started, Peer1, Pid1}, State0),
	?assertEqual(Pid1, maps:get(Peer1, State1#state.known_peers)),
	{noreply, State2} = handle_cast({peer_worker_started, Peer2, Pid2}, State1),
	?assertEqual(Pid1, maps:get(Peer1, State2#state.known_peers)),
	?assertEqual(Pid2, maps:get(Peer2, State2#state.known_peers)),
	NewPid = spawn(fun() -> ok end),
	{noreply, State3} = handle_cast({peer_worker_started, Peer1, NewPid}, State2),
	?assertEqual(NewPid, maps:get(Peer1, State3#state.known_peers)).

test_calculate_targets() ->
	%% Phase 1: max_dispatched is now read from the ETS mirror, not via
	%% gen_server call into peer worker. Ensure the mirror table exists
	%% and populate it for the three mock peers.
	Peer1 = {1,2,3,4,1984},
	Peer2 = {5,6,7,8,1985},
	Peer3 = {9,10,11,12,1986},
	case ets:info(?WORKER_LOAD_TABLE) of
		undefined ->
			ets:new(?WORKER_LOAD_TABLE, [named_table, public, set]);
		_ -> ok
	end,
	ets:insert(?WORKER_LOAD_TABLE, [
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
	
	State = #state{ worker_names = [w1, w2, w3, w4, w5] },

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
	ets:delete(?WORKER_LOAD_TABLE, {peer_max_dispatched, Peer1}),
	ets:delete(?WORKER_LOAD_TABLE, {peer_max_dispatched, Peer2}),
	ets:delete(?WORKER_LOAD_TABLE, {peer_max_dispatched, Peer3}).

-endif.
