%%% @doc Thin coordinator for the sync subsystem. Pull-model dispatch means
%%% most of the logic lives elsewhere; this module holds the peer roster,
%%% runs the periodic rebalance tick, and owns the global worker_load ETS
%%% helpers.
%%% 
%%% Main objectives:
%%% - Saturate ar_data_sync_worker processes so they always have work to do
%%%   (fetching chunks from peers)
%%% - Don't let ar_data_sync_workers get stuck working through a long queue of tasks for a slow
%%%   peer. Historically this was a common cause of poor sync performance - node would "lock up"
%%%   as it tried to work through 1000+ queued sync tasks for a peer with multi-second GET /chunk2
%%%   latency.
%%% - Limit the number of footprints that are being handled at once. All chunks in a footprint
%%%   can be unpacked with the same 256MiB entropy. If the node tries to pull from too many
%%%   different footprints concurrently it either overwhelms RAM with too many cached entropies or
%%%   thrashes and ends up recomputing the same entropy multiple times (a very expensive operation)
%%%
%%% Components:
%%% - ar_peer_worker (one per peer): owns the per-peer task queue, in-flight
%%%   count and limit, footprint state. Sole writer of its worker_load rows (worker_load tracks
%%%   how much work is assigned to this peer and is used to balance load and for backpressure)
%%% - ar_data_sync_worker (pool of N): pull loop. Each worker shuffles the
%%%   peer roster and asks peer workers for tasks via take_one/1.
%%% - this module: forwards sync_range casts into the right peer worker,
%%%   runs the rebalance tick every ?REBALANCE_FREQUENCY_MS, exposes
%%%   ETS helpers (record_peer_load, claim/release_footprint_slot), provides backpressure to
%%%   ar_data_sync via ready_for_work/0.
%%%
%%% Tasks are either queued (in a peer worker's task_queue) or in-flight
%%% (held by a sync worker that called take_one). A task with a footprint
%%% key cannot go in-flight unless its peer already holds a global footprint
%%% slot or can claim one atomically (see claim_footprint_slot/0).

-module(ar_data_sync_coordinator).

-behaviour(gen_server).

-export([start_link/1, register_workers/0, is_syncing_enabled/0,
		 ready_for_work/0, work_capacity/0, max_tasks/0,
		 sync_range/1]).
-export([record_peer_load/5, remove_peer/1]).
-export([claim_footprint_slot/0, release_footprint_slot/0]).
-export([default_in_flight_limit/0, min_peer_queue/0, max_peer_queue/0]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-ifdef(AR_TEST).
-export([init_worker_load_for_test/1, set_footprint_slots/1,
		 set_footprint_slots_available/1, get_footprint_slots_available/0]).
-endif.

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-define(REBALANCE_FREQUENCY_MS, 10_000).
%% Smooth the total peer throughput estimate since it can be volatile.
-define(THROUGHPUT_SMOOTHING_ALPHA, 0.3).

%% Capacity constants — all derived from sync_jobs in the exported helpers.
%% Queued tasks per worker in the coordinator pipeline (backpressure).
-define(TASKS_PER_WORKER, 50).
%% Starting per-peer in-flight limit (floor).
-define(MIN_IN_FLIGHT_LIMIT, 8).
%% sync_jobs divisor for scaling default_in_flight_limit above the floor.
-define(IN_FLIGHT_LIMIT_DIVISOR, 50).
%% Floor for per-peer queue length.
-define(MIN_PEER_QUEUE_FLOOR, 20).
%% sync_jobs divisor for scaling min_peer_queue above the floor.
-define(PEER_QUEUE_DIVISOR, 10).
%% sync_jobs multiplier for max_peer_queue ceiling.
-define(PEER_QUEUE_MULTIPLIER, 1000).
-record(state, {
	total_throughput = undefined   %% EMA-smoothed sum of peer ratings
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

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
ready_for_work() ->
	work_capacity() > 0.

%% @doc Returns how many more tasks the pipeline can absorb. Used by
%% ar_data_sync to pace enqueue rate and size the intervals queue.
work_capacity() ->
	try
		WorkerCount = ets:lookup_element(?WORKER_LOAD_TABLE, workers_count, 2),
		case WorkerCount of
			0 -> 0;
			_ ->
				TotalQueuedTasks = sum_prefix(queued),
				TotalInFlight = sum_prefix(in_flight),
				max(0, max_tasks(WorkerCount) - TotalQueuedTasks - TotalInFlight)
		end
	catch _:_ ->
		0
	end.

%% @doc Forward a sync task into the coordinator for dispatch to the right peer worker.
sync_range(SyncTask) ->
	gen_server:cast(?MODULE, {sync_range, SyncTask}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Workers) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	MaxFootprints = calculate_max_footprints(),
	ets:delete_all_objects(?WORKER_LOAD_TABLE),
	ets:insert(?WORKER_LOAD_TABLE, {workers_count, length(Workers)}),
	ets:insert(?WORKER_LOAD_TABLE,
		[{footprint_slots_available, MaxFootprints},
		 {footprint_slots_max, MaxFootprints}]),
	?LOG_INFO([{event, init}, {module, ?MODULE}, {workers, Workers},
		{max_footprints, MaxFootprints}]),
	{ok, #state{}}.

calculate_max_footprints() ->
	{ok, Config} = arweave_config:get_env(),
	%% Calculate global max footprints based on entropy cache size
	FootprintSize = ar_block:get_replica_2_9_footprint_size(),
	max(1, (Config#config.replica_2_9_entropy_cache_size_mb * ?MiB) div FootprintSize).


handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({sync_range, SyncTask}, State) ->
	#sync_task{ peer = Peer } = SyncTask,
	case ar_peer_worker:get_or_start(Peer) of
		{ok, Pid} -> ar_peer_worker:enqueue(Pid, SyncTask);
		{error, _} -> ok
	end,
	{noreply, State};

handle_cast(rebalance_peers, State) ->
	ar_util:cast_after(?REBALANCE_FREQUENCY_MS, ?MODULE, rebalance_peers),
	PeerPids = maps:from_list(ar_peer_worker:get_all_peers()),
	Peers = maps:keys(PeerPids),
	AllPeerPerformances = ar_peers:get_peer_performances(Peers),
	{Targets, State1} = calculate_targets(PeerPids, AllPeerPerformances, State),
	State2 = rebalance_peers(PeerPids, AllPeerPerformances, Targets, State1),
	log_anomalies(State2),
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
%% Capacity constants derived from sync_jobs
%%
%% All capacity-related thresholds scale from a single operator knob:
%% Config#config.sync_jobs. Higher sync_jobs = more workers = deeper
%% buffers = higher per-peer limits. Timing constants (timeouts,
%% check intervals, smoothing factors) are independent.
%%--------------------------------------------------------------------

sync_jobs() ->
	{ok, Config} = arweave_config:get_env(),
	Config#config.sync_jobs.

max_tasks() -> max_tasks(sync_jobs()).
max_tasks(WorkerCount) -> WorkerCount * ?TASKS_PER_WORKER.

default_in_flight_limit() -> max(?MIN_IN_FLIGHT_LIMIT, sync_jobs() div ?IN_FLIGHT_LIMIT_DIVISOR).

min_peer_queue() -> max(?MIN_PEER_QUEUE_FLOOR, sync_jobs() div ?PEER_QUEUE_DIVISOR).

max_peer_queue() -> sync_jobs() * ?PEER_QUEUE_MULTIPLIER.


%%--------------------------------------------------------------------
%% Rebalancing
%%--------------------------------------------------------------------

%% @doc Calculate rebalance parameters.
%% Returns {{WorkerCount, TargetLatency, TotalThroughput, TotalMaxInFlight}, State}
calculate_targets(PeerPids, AllPeerPerformances, State) ->
	WorkerCount = ets:lookup_element(?WORKER_LOAD_TABLE, workers_count, 2, 0),
	Peers = maps:keys(PeerPids),
	%% Estimated (unsmoothed) throughput across all peers
	RawTotalThroughput =
		lists:foldl(
			fun(Peer, Acc) ->
				Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
				Acc + Performance#performance.current_rating
			end, 0.0, Peers),
	%% Smoothed throughput across all peers. Smoothing reduces volatilitity and unnesessary
	%% rebalancing.
	TotalThroughput = case State#state.total_throughput of
		undefined -> RawTotalThroughput;
		OldTotalThroughput ->
			ar_util:ema(OldTotalThroughput, RawTotalThroughput, ?THROUGHPUT_SMOOTHING_ALPHA)
	end,
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
	TotalMaxInFlight = maps:fold(
		fun(Peer, _Pid, Acc) ->
			ets:lookup_element(?WORKER_LOAD_TABLE,
				{max_in_flight, Peer}, 2, 0) + Acc
		end,
		0, PeerPids),
	?LOG_DEBUG([{event, sync_performance_targets},
		{worker_count, WorkerCount},
		{target_latency, TargetLatency},
		{raw_total_throughput, RawTotalThroughput},
		{old_total_throughput, State#state.total_throughput},
		{total_throughput, TotalThroughput},
		{total_max_in_flight, TotalMaxInFlight}]),
	State2 = State#state{ total_throughput = TotalThroughput },
	{{WorkerCount, TargetLatency, TotalThroughput, TotalMaxInFlight}, State2}.

%% PeerPidsList is [{Peer, Pid}]
rebalance_peers(PeerPids, AllPeerPerformances, Targets, State) ->
	rebalance_peers2(maps:to_list(PeerPids), AllPeerPerformances, Targets, State).

rebalance_peers2([], _AllPeerPerformances, _Targets, State) ->
	State;
rebalance_peers2([{Peer, Pid} | Rest], AllPeerPerformances, Targets, State) ->
	{WorkerCount, TargetLatency, TotalThroughput, TotalMaxInFlight} = Targets,
	Performance = maps:get(Peer, AllPeerPerformances, #performance{}),
	QueueScalingFactor = queue_scaling_factor(TotalThroughput, WorkerCount),
	WorkersStarved = TotalMaxInFlight < WorkerCount,
	RebalanceParams = {QueueScalingFactor, TargetLatency, WorkersStarved},
	Result = ar_peer_worker:rebalance(Pid, Performance, RebalanceParams),
	case Result of
		{shutdown, _RemovedCount} ->
			?LOG_INFO([{event, shutdown_idle_peer_worker},
				{peer, ar_util:format_peer(Peer)}]),
			ar_peer_worker:stop(Pid);
		{ok, _RemovedCount} -> ok;
		{error, timeout} -> ok
	end,
	rebalance_peers2(Rest, AllPeerPerformances, Targets, State).

%% @doc Scaling factor for calculating per-peer max queue size.
%% Peer worker calculates: MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
queue_scaling_factor(0, WorkerCount) -> float(max_tasks(WorkerCount));
queue_scaling_factor(0.0, WorkerCount) -> float(max_tasks(WorkerCount));
queue_scaling_factor(TotalThroughput, WorkerCount) ->
	max_tasks(WorkerCount) / TotalThroughput.

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @doc Called from ar_peer_worker to publish its current load contribution
%% (in-flight, queued, active footprints, slow-peer cap) to the worker_load
%% ETS table.
record_peer_load(Peer, InFlight, Queued, ActiveFootprints, MaxInFlight) ->
	try
		ets:insert(?WORKER_LOAD_TABLE,
			[{{in_flight, Peer}, InFlight},
			 {{queued, Peer}, Queued},
			 {{active_footprints, Peer}, ActiveFootprints},
			 {{max_in_flight, Peer}, MaxInFlight}])
	catch _:_ -> ok
	end.

%% @doc Atomically claim a footprint slot. Returns true on success, false if
%% no slots are available. 
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

%% @doc Release a previously-claimed footprint slot. 
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
		ets:delete(?WORKER_LOAD_TABLE, {in_flight, Peer}),
		ets:delete(?WORKER_LOAD_TABLE, {queued, Peer}),
		ets:delete(?WORKER_LOAD_TABLE, {active_footprints, Peer}),
		ets:delete(?WORKER_LOAD_TABLE, {max_in_flight, Peer})
	catch _:_ -> ok
	end.

log_anomalies(#state{} = _State) ->
	try
		SumPeerFootprints = sum_prefix(active_footprints),
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

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

%% @doc Test-only: create (if needed) and initialise the worker_load table
%% with a footprint-slot gauge so dequeue-time claims can succeed.
init_worker_load_for_test(Slots) ->
	case ets:info(?WORKER_LOAD_TABLE) of
		undefined ->
			ets:new(?WORKER_LOAD_TABLE, [named_table, public, set]);
		_ -> ok
	end,
	ets:insert(?WORKER_LOAD_TABLE, {workers_count, 400}),
	set_footprint_slots(Slots).

%% @doc Test-only: override the footprint-slot gauge.
set_footprint_slots(Slots) ->
	ets:insert(?WORKER_LOAD_TABLE,
		[{footprint_slots_available, Slots}, {footprint_slots_max, Slots}]),
	ok.

%% @doc Test-only: override only the available slot count (not the max).
set_footprint_slots_available(Slots) ->
	ets:insert(?WORKER_LOAD_TABLE, {footprint_slots_available, Slots}),
	ok.

%% @doc Test-only: current available slot count.
get_footprint_slots_available() ->
	ets:lookup_element(?WORKER_LOAD_TABLE, footprint_slots_available, 2).

coordinator_test_() ->
	[
		{timeout, 30, fun test_max_tasks/0},
		{timeout, 30, fun test_queue_scaling_factor/0},
		{timeout, 30, fun test_calculate_targets/0}
	].

test_max_tasks() ->
	?assertEqual(50, max_tasks(1)),
	?assertEqual(100, max_tasks(2)),
	?assertEqual(500, max_tasks(10)),
	?assertEqual(5000, max_tasks(100)).

test_queue_scaling_factor() ->
	?assertEqual(500.0, queue_scaling_factor(0, 10)),
	?assertEqual(500.0, queue_scaling_factor(0.0, 10)),
	?assertEqual(5.0, queue_scaling_factor(100.0, 10)),
	?assertEqual(2.5, queue_scaling_factor(200.0, 10)),
	?assertEqual(50.0, queue_scaling_factor(10.0, 10)).

test_calculate_targets() ->
	Peer1 = {1,2,3,4,1984},
	Peer2 = {5,6,7,8,1985},
	Peer3 = {9,10,11,12,1986},
	case ets:info(?WORKER_LOAD_TABLE) of
		undefined ->
			ets:new(?WORKER_LOAD_TABLE, [named_table, public, set]);
		_ -> ok
	end,
	ets:insert(?WORKER_LOAD_TABLE, [
		{{max_in_flight, Peer1}, 10},
		{{max_in_flight, Peer2}, 15},
		{{max_in_flight, Peer3}, 20}
	]),

	%% Pids no longer need to answer get_max_in_flight, but calculate_targets
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
	
	ets:insert(?WORKER_LOAD_TABLE, {workers_count, 5}),
	State = #state{},

	{{WorkerCount, TargetLatency, TotalThroughput, TotalMaxInFlight}, _State2} =
		calculate_targets(PeerPids, AllPeerPerformances, State),

	%% WorkerCount = 5 (number of workers in queue)
	?assertEqual(5, WorkerCount),

	%% TotalThroughput = 100 + 200 + 300 = 600 (first call, no smoothing)
	?assertEqual(600.0, TotalThroughput),
	
	%% TargetLatency = (50 + 100 + 150) / 3 = 100
	?assertEqual(100.0, TargetLatency),
	
	%% TotalMaxInFlight = 10 + 15 + 20 = 45
	?assertEqual(45, TotalMaxInFlight),
	
	%% Cleanup
	exit(Pid1, kill),
	exit(Pid2, kill),
	exit(Pid3, kill),
	ets:delete(?WORKER_LOAD_TABLE, {max_in_flight, Peer1}),
	ets:delete(?WORKER_LOAD_TABLE, {max_in_flight, Peer2}),
	ets:delete(?WORKER_LOAD_TABLE, {max_in_flight, Peer3}).

-endif.
