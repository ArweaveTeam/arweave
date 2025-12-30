%%% @doc Per-peer process managing sync task queue, dispatch state, and footprints.
%%%
%%% Each peer gets its own worker process to isolate state and prevent
%%% stale-state bugs from interleaved updates. This process manages:
%%%
%%% Peer Queue Management:
%%% - Maintains a queue of tasks ready to be dispatched for this peer
%%% - Tracks dispatched_count (number of tasks currently being processed)
%%% - Maintains max_dispatched limit that controls how many tasks can be
%%%   concurrently dispatched for this peer (adjusted via rebalancing)
%%% - If too many requests are made to a peer, it can be blocked or throttled
%%% - Peer performance varies over time; dispatch limits adapt to avoid getting
%%%   "stuck" syncing many chunks from a slow peer
%%%
%%% Footprint Management:
%%% - Tasks with a FootprintKey are grouped by footprint to limit concurrent
%%%   processing and avoid overloading the entropy cache
%%% - Footprints can be active (tasks being processed) or waiting (queued for later)
%%% - Each peer has a max_footprints limit to prevent overloading entropy cache
%%% - When a footprint becomes active, waiting tasks are moved to the peer queue
%%% - When all tasks in a footprint complete, it's deactivated and the next
%%%   waiting footprint may be activated
%%% - Long-running footprints are detected and logged per-peer
%%%
%%% Performance Tracking:
%%% - Tracks task completion times and data sizes for performance metrics
%%% - Integrates with ar_peers module for peer rating and performance tracking
%%%
%%% Rebalancing:
%%% - Responds to rebalance requests from the coordinator
%%% - Adjusts max_dispatched based on peer performance vs target latency
%%% - Cuts queue if it exceeds the calculated max_queue size
-module(ar_peer_worker).

-behaviour(gen_server).

-export([start_link/1, get_or_start/1, stop/1, lookup/1]).
-export([enqueue_task/3, task_completed/5, get_max_dispatched/1]).
-export([process_queue/2]).
-export([rebalance/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(MIN_MAX_ACTIVE, 8).
-define(MIN_PEER_QUEUE, 20).
-define(DEFAULT_MAX_FOOTPRINTS, 10).
-define(CHECK_LONG_RUNNING_FOOTPRINTS_MS, 60000).  %% Check every 60 seconds
-define(LONG_RUNNING_FOOTPRINT_THRESHOLD_S, 120).

-record(footprint, {
	waiting = queue:new(),   %% queue of waiting tasks
	active_count = 0,        %% count of active tasks (0 = inactive)
	activation_time          %% monotonic time when activated (undefined if inactive)
}).

-record(state, {
	peer,
	queued = queue:new(),
	dispatched_count = 0,
	waiting_count = 0,
	max_dispatched = ?MIN_MAX_ACTIVE,
	%% Footprint management
	footprints = #{},              %% FootprintKey => #footprint{}
	active_footprint_count = 0,    %% count of footprints with active_count > 0
	max_footprints = ?DEFAULT_MAX_FOOTPRINTS
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Peer) ->
	Name = peer_name(Peer),
	gen_server:start_link({local, Name}, ?MODULE, [Peer], []).

%% @doc Convert peer tuple to a process name atom.
peer_name(Peer) ->
	list_to_atom("ar_peer_" ++ ar_util:format_peer(Peer)).

%% @doc Lookup a peer worker pid by name.
lookup(Peer) ->
	case whereis(peer_name(Peer)) of
		undefined -> undefined;
		Pid -> {ok, Pid}
	end.

%% @doc Get the pid of an existing peer worker or start a new one.
get_or_start(Peer) ->
	case lookup(Peer) of
		{ok, Pid} ->
			{ok, Pid};
		undefined ->
			case supervisor:start_child(ar_peer_worker_sup, [Peer]) of
				{ok, Pid} -> {ok, Pid};
				{error, {already_started, Pid}} -> {ok, Pid};
				Error -> Error
			end
	end.

stop(Peer) ->
	case lookup(Peer) of
		undefined -> ok;
		{ok, Pid} -> gen_server:stop(Pid)
	end.

%% @doc Enqueue a task for this peer, respecting footprint limits.
%% Returns {ok, {WaitingDelta, QueuedDelta}} indicating count changes.
%% Uses a timeout to avoid blocking if the peer worker is unresponsive.
enqueue_task(Peer, FootprintKey, Args) ->
	{ok, Pid} = get_or_start(Peer),
	try
		gen_server:call(Pid, {enqueue_task, FootprintKey, Args}, 5000)
	catch
		exit:{timeout, _} ->
			?LOG_WARNING([{event, enqueue_task_timeout},
				{peer, ar_util:format_peer(Peer)}]),
			{error, timeout};
		_:Reason ->
			?LOG_WARNING([{event, enqueue_task_error},
				{peer, ar_util:format_peer(Peer)},
				{reason, Reason}]),
			{error, Reason}
	end.

%% @doc Notify that a task completed, update footprint accounting, and rate data fetched.
%% Result is ok | {error, Reason}, ElapsedNative is in native time units, DataSize in bytes.
task_completed(Peer, FootprintKey, Result, ElapsedNative, DataSize) ->
	case lookup(Peer) of
		undefined -> 
			ok;
		{ok, Pid} -> 
			gen_server:cast(Pid, {task_completed, FootprintKey, Result, ElapsedNative, DataSize})
	end.

%% @doc Get max_dispatched for this peer.
%% Uses a timeout to avoid blocking if the peer worker is unresponsive.
get_max_dispatched(Peer) ->
	case lookup(Peer) of
		undefined -> {error, not_found};
		{ok, Pid} -> 
			try
				gen_server:call(Pid, get_max_dispatched, 5000)
			catch
				exit:{timeout, _} ->
					?LOG_WARNING([{event, get_max_dispatched_timeout},
						{peer, ar_util:format_peer(Peer)}]),
					{error, timeout};
				_:Reason ->
					?LOG_WARNING([{event, get_max_dispatched_error},
						{peer, ar_util:format_peer(Peer)},
						{reason, Reason}]),
					{error, Reason}
			end
	end.

%% @doc Process the queue and dispatch tasks if capacity available.
%% Returns list of Args to dispatch.
%% Uses a timeout to avoid blocking if the peer worker is unresponsive.
process_queue(Peer, MaxTasks) ->
	case lookup(Peer) of
		undefined -> [];
		{ok, Pid} -> 
			try
				gen_server:call(Pid, {process_queue, MaxTasks}, 5000)
			catch
				exit:{timeout, _} ->
					?LOG_WARNING([{event, process_queue_timeout},
						{peer, ar_util:format_peer(Peer)}]),
					[];
				_:Reason ->
					?LOG_WARNING([{event, process_queue_error},
						{peer, ar_util:format_peer(Peer)},
						{reason, Reason}]),
					[]
			end
	end.

%% @doc Rebalance this peer based on performance and targets.
%% RebalanceParams = {QueueScalingFactor, FasterThanTarget, WorkersStarved}
%% Returns RemovedCount (number of tasks cut from queue) for master to update total_queued_count.
rebalance(Peer, Performance, RebalanceParams) ->
	case lookup(Peer) of
		undefined -> 0;
		{ok, Pid} -> 
			try
				gen_server:call(Pid, {rebalance, Performance, RebalanceParams}, 5000)
			catch
				exit:{timeout, _} ->
					?LOG_WARNING([{event, rebalance_timeout},
						{peer, ar_util:format_peer(Peer)}]),
					0;
				_:Reason ->
					?LOG_WARNING([{event, rebalance_error},
						{peer, ar_util:format_peer(Peer)},
						{reason, Reason}]),
					0
			end
	end.

%% @doc Get list of long-running footprints for this peer.
%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([Peer]) ->
	%% Schedule periodic check for long-running footprints
	erlang:send_after(?CHECK_LONG_RUNNING_FOOTPRINTS_MS, self(), check_long_running_footprints),
	MaxFootprints = calculate_max_footprints(),
	{ok, #state{ peer = Peer, max_footprints = MaxFootprints }}.

calculate_max_footprints() ->
	try
		{ok, Config} = arweave_config:get_env(),
		%% Calculate max footprints - divide entropy cache across expected peers
		%% Start with a reasonable per-peer default
		max(1, (Config#config.replica_2_9_entropy_cache_size_mb * 1024 * 1024)
			div ar_block:get_replica_2_9_footprint_size() div 10)  %% Assume ~10 active peers
	catch
		_:_ -> ?DEFAULT_MAX_FOOTPRINTS
	end.

handle_call({enqueue_task, FootprintKey, Args}, _From, State) ->
	{Result, State2} = do_enqueue_task(FootprintKey, Args, State),
	{reply, Result, State2};

handle_call(get_max_dispatched, _From, State) ->
	{reply, State#state.max_dispatched, State};

handle_call(get_state, _From, State) ->
	%% Test-only: keep for tests to access raw state
	{reply, {ok, State}, State};

handle_call({process_queue, MaxTasks}, _From, State) ->
	#state{ dispatched_count = Dispatched, max_dispatched = Max, queued = Queued, peer = Peer } = State,
	AvailableSlots = min(MaxTasks, Max - Dispatched),
	{Tasks, NewQueued, NewDispatched} = dequeue_tasks(Queued, Dispatched, AvailableSlots, []),
	TaskCount = length(Tasks),
	case TaskCount > 0 of
		true ->
			increment_metrics(dispatched, Peer, TaskCount),
			increment_metrics(queued_out, Peer, TaskCount);
		false ->
			ok
	end,
	{reply, Tasks, State#state{ queued = NewQueued, dispatched_count = NewDispatched }};

handle_call({rebalance, Performance, RebalanceParams}, _From, State) ->
	{QueueScalingFactor, TargetLatency, WorkersStarved} = RebalanceParams,
	#state{ queued = Queued, peer = Peer, max_dispatched = MaxDispatched,
			dispatched_count = Dispatched, waiting_count = Waiting } = State,
	
	%% 1. Cut queue if needed
	MaxQueue = max_peer_queue(Performance, QueueScalingFactor),
	QueueLen = queue:len(Queued),
	{State2, RemovedCount} = case MaxQueue =/= infinity andalso QueueLen > MaxQueue of
		true ->
			{NewQueued, RemovedQueue} = queue:split(MaxQueue, Queued),
			Removed = queue:len(RemovedQueue),
			RemovedTasks = queue:to_list(RemovedQueue),
			increment_metrics(queued_out, Peer, Removed),
			S2 = cut_footprint_task_counts(RemovedTasks, State#state{ queued = NewQueued }),
			{S2, Removed};
		false ->
			{State, 0}
	end,
	
	%% 2. Update max_dispatched
	FasterThanTarget = Performance#performance.average_latency < TargetLatency,
	TargetMax = case FasterThanTarget orelse WorkersStarved of
		true -> MaxDispatched + 1;
		false -> MaxDispatched - 1
	end,
	MaxTasks = max(Dispatched, Waiting + queue:len(State2#state.queued)),
	NewMaxDispatched = ar_util:between(TargetMax, ?MIN_MAX_ACTIVE, max(MaxTasks, ?MIN_MAX_ACTIVE)),
	State3 = State2#state{ max_dispatched = NewMaxDispatched },
	
	%% 3. Log rebalance
	?LOG_DEBUG([{event, rebalance_peer},
		{peer, ar_util:format_peer(Peer)},
		{dispatched_count, State3#state.dispatched_count},
		{queued_count, queue:len(State3#state.queued)},
		{waiting_count, State3#state.waiting_count},
		{max_queue, MaxQueue},
		{faster_than_target, FasterThanTarget},
		{workers_starved, WorkersStarved},
		{max_dispatched, NewMaxDispatched},
		{removed_count, RemovedCount}]),
	
	{reply, RemovedCount, State3};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, {error, unhandled}, State}.

handle_cast({task_completed, FootprintKey, Result, ElapsedNative, DataSize}, State) ->
	#state{ dispatched_count = Count, max_dispatched = MaxDispatched, peer = Peer } = State,
	NewCount = max(0, Count - 1),
	increment_metrics(completed, Peer, 1),
	%% Rate the fetched data with ar_peers
	ElapsedMicroseconds = erlang:convert_time_unit(ElapsedNative, native, microsecond),
	ar_peers:rate_fetched_data(Peer, chunk, Result, ElapsedMicroseconds, DataSize, MaxDispatched),
	%% Complete footprint task
	State2 = do_complete_footprint_task(FootprintKey, State#state{ dispatched_count = NewCount }),
	{noreply, State2};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(check_long_running_footprints, State) ->
	%% Schedule next check
	erlang:send_after(?CHECK_LONG_RUNNING_FOOTPRINTS_MS, self(), check_long_running_footprints),
	log_long_running_footprints(State),
	{noreply, State};

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions - Task management
%%%===================================================================

dequeue_tasks(Queued, Dispatched, 0, Acc) ->
	{lists:reverse(Acc), Queued, Dispatched};
dequeue_tasks(Queued, Dispatched, N, Acc) ->
	case queue:out(Queued) of
		{empty, _} ->
			{lists:reverse(Acc), Queued, Dispatched};
		{{value, Args}, NewQueued} ->
			dequeue_tasks(NewQueued, Dispatched + 1, N - 1, [Args | Acc])
	end.

%% @doc Calculate max queue size for this peer.
%% MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
max_peer_queue(#performance{ current_rating = 0 }, _ScalingFactor) -> infinity;
max_peer_queue(#performance{ current_rating = 0.0 }, _ScalingFactor) -> infinity;
max_peer_queue(_Performance, infinity) -> infinity;
max_peer_queue(Performance, ScalingFactor) ->
	PeerThroughput = Performance#performance.current_rating,
	max(trunc(PeerThroughput * ScalingFactor), ?MIN_PEER_QUEUE).

%%%===================================================================
%%% Private functions - Footprint management
%%%===================================================================

%% @doc Enqueue a task, respecting footprint limits.
do_enqueue_task(none, Args, State) ->
	%% No footprint key - enqueue directly to peer queue
	#state{ queued = Queued, peer = Peer } = State,
	NewQueued = queue:in(Args, Queued),
	increment_metrics(queued_in, Peer, 1),
	{{ok, {0, 1}}, State#state{ queued = NewQueued }};

do_enqueue_task(FootprintKey, Args, State) ->
	#state{ footprints = FPs, active_footprint_count = ActiveCount, 
			max_footprints = MaxFP, peer = Peer, queued = Queued } = State,
	Footprint = maps:get(FootprintKey, FPs, #footprint{}),
	case Footprint#footprint.active_count > 0 of
		true ->
			%% Footprint is already active, enqueue directly to peer queue
			NewQueued = queue:in(Args, Queued),
			Footprint2 = Footprint#footprint{ active_count = Footprint#footprint.active_count + 1 },
			increment_metrics(queued_in, Peer, 1),
			increment_metrics(activate_footprint_task, Peer, 1),
			{{ok, {0, 1}}, State#state{ 
				queued = NewQueued,
				footprints = maps:put(FootprintKey, Footprint2, FPs)
			}};
		false when ActiveCount < MaxFP ->
			%% Room for new footprint, activate it and enqueue directly
			NewQueued = queue:in(Args, Queued),
			Footprint2 = Footprint#footprint{ 
				active_count = 1, 
				activation_time = erlang:monotonic_time() 
			},
			increment_metrics(queued_in, Peer, 1),
			increment_metrics(activate_footprint, Peer, 1),
			increment_metrics(activate_footprint_task, Peer, 1),
			?LOG_DEBUG([{event, activate_footprint},
				{peer, ar_util:format_peer(Peer)},
				{footprint_key, FootprintKey},
				{active_footprints, ActiveCount + 1}]),
			{{ok, {0, 1}}, State#state{ 
				queued = NewQueued,
				footprints = maps:put(FootprintKey, Footprint2, FPs),
				active_footprint_count = ActiveCount + 1
			}};
		false ->
			%% No room, queue the task for later in footprint waiting queue
			Footprint2 = Footprint#footprint{ 
				waiting = queue:in(Args, Footprint#footprint.waiting) 
			},
			increment_metrics(waiting_in, Peer, 1),
			{{ok, {1, 0}}, State#state{ 
				footprints = maps:put(FootprintKey, Footprint2, FPs),
				waiting_count = State#state.waiting_count + 1
			}}
	end.

%% @doc Handle completion of a footprint task.
do_complete_footprint_task(none, State) ->
	State;
do_complete_footprint_task(FootprintKey, State) ->
	#state{ footprints = FPs, peer = Peer } = State,
	case maps:find(FootprintKey, FPs) of
		error ->
			?LOG_WARNING([{event, complete_footprint_task_not_found},
				{footprint_key, FootprintKey},
				{peer, ar_util:format_peer(Peer)}]),
			State;
		{ok, Footprint} ->
			increment_metrics(deactivate_footprint_task, Peer, 1),
			NewActiveCount = Footprint#footprint.active_count - 1,
			case NewActiveCount =< 0 of
				true ->
					%% Footprint has no more active tasks
					case queue:is_empty(Footprint#footprint.waiting) of
						true ->
							%% No waiting tasks - deactivate footprint
							deactivate_footprint(FootprintKey, Footprint, State);
						false ->
							%% Has waiting tasks - activate them
							activate_waiting_tasks(FootprintKey, Footprint, State)
					end;
				false ->
					%% Still has active tasks
					Footprint2 = Footprint#footprint{ active_count = NewActiveCount },
					State#state{ footprints = maps:put(FootprintKey, Footprint2, FPs) }
			end
	end.

%% @doc Deactivate a footprint and try to activate the next one.
deactivate_footprint(FootprintKey, Footprint, State) ->
	#state{ footprints = FPs, active_footprint_count = ActiveCount, peer = Peer } = State,
	%% Log deactivation with duration
	case Footprint#footprint.activation_time of
		undefined -> ok;
		ActivationTime ->
			DurationMs = erlang:convert_time_unit(
				erlang:monotonic_time() - ActivationTime, native, millisecond),
			?LOG_DEBUG([{event, footprint_deactivated},
				{peer, ar_util:format_peer(Peer)},
				{footprint_key, FootprintKey},
				{duration_ms, DurationMs}])
	end,
	increment_metrics(deactivate_footprint, Peer, 1),
	State2 = State#state{
		footprints = maps:remove(FootprintKey, FPs),
		active_footprint_count = ActiveCount - 1
	},
	activate_next_footprint(State2).

%% @doc Activate waiting tasks from a footprint that just became active.
activate_waiting_tasks(FootprintKey, Footprint, State) ->
	#state{ queued = Queued, peer = Peer, footprints = FPs } = State,
	WaitingQueue = Footprint#footprint.waiting,
	WaitingCount = queue:len(WaitingQueue),
	%% Move all waiting tasks to the queued state
	NewQueued = queue:join(Queued, WaitingQueue),
	increment_metrics(waiting_out, Peer, WaitingCount),
	increment_metrics(queued_in, Peer, WaitingCount),
	increment_metrics(activate_footprint_task, Peer, WaitingCount),
	Footprint2 = Footprint#footprint{ 
		waiting = queue:new(), 
		active_count = WaitingCount 
	},
	State#state{ 
		queued = NewQueued, 
		footprints = maps:put(FootprintKey, Footprint2, FPs),
		waiting_count = State#state.waiting_count - WaitingCount
	}.

%% @doc Activate the next waiting footprint if any.
activate_next_footprint(State) ->
	#state{ footprints = FPs, active_footprint_count = ActiveCount, 
			max_footprints = MaxFP, peer = Peer } = State,
	case ActiveCount >= MaxFP of
		true ->
			State;
		false ->
			%% Find footprint with most waiting tasks
			case find_next_waiting_footprint(FPs) of
				none ->
					State;
				{FootprintKey, Footprint} ->
					WaitingQueue = Footprint#footprint.waiting,
					WaitingCount = queue:len(WaitingQueue),
					?LOG_DEBUG([{event, activate_footprint},
						{source, activate_next_footprint},
						{peer, ar_util:format_peer(Peer)},
						{footprint_key, FootprintKey},
						{waiting_tasks, WaitingCount},
						{active_footprints, ActiveCount + 1}]),
					%% Move waiting tasks to queued
					#state{ queued = Queued } = State,
					NewQueued = queue:join(Queued, WaitingQueue),
					increment_metrics(activate_footprint, Peer, 1),
					increment_metrics(waiting_out, Peer, WaitingCount),
					increment_metrics(queued_in, Peer, WaitingCount),
					increment_metrics(activate_footprint_task, Peer, WaitingCount),
					Footprint2 = Footprint#footprint{
						waiting = queue:new(),
						active_count = WaitingCount,
						activation_time = erlang:monotonic_time()
					},
					State#state{
						queued = NewQueued,
						footprints = maps:put(FootprintKey, Footprint2, FPs),
						active_footprint_count = ActiveCount + 1,
						waiting_count = State#state.waiting_count - WaitingCount
					}
			end
	end.

%% @doc Find the waiting footprint with the most queued tasks.
find_next_waiting_footprint(Footprints) ->
	maps:fold(
		fun(Key, Footprint, Acc) ->
			case Footprint#footprint.active_count =< 0 andalso
				 not queue:is_empty(Footprint#footprint.waiting) of
				true ->
					WaitingCount = queue:len(Footprint#footprint.waiting),
					case Acc of
						none ->
							{Key, Footprint};
						{_AccKey, AccFP} ->
							AccWaitingCount = queue:len(AccFP#footprint.waiting),
							case WaitingCount > AccWaitingCount of
								true -> {Key, Footprint};
								false -> Acc
							end
					end;
				false ->
					Acc
			end
		end, none, Footprints).

%% @doc Decrement footprint counts for tasks removed from queue (e.g., during cut).
cut_footprint_task_counts([], State) ->
	State;
cut_footprint_task_counts([Args | Rest], State) ->
	FootprintKey = element(5, Args),
	State2 = case FootprintKey of
		none -> State;
		_ -> do_complete_footprint_task(FootprintKey, State)
	end,
	cut_footprint_task_counts(Rest, State2).

%%%===================================================================
%%% Private functions - Long-running footprint detection (debugging)
%%%===================================================================

log_long_running_footprints(State) ->
	#state{ peer = Peer, footprints = FPs } = State,
	Now = erlang:monotonic_time(),
	ThresholdNative = erlang:convert_time_unit(?LONG_RUNNING_FOOTPRINT_THRESHOLD_S, second, native),
	LongRunning = maps:fold(
		fun(FootprintKey, Footprint, Acc) ->
			case Footprint#footprint.activation_time of
				undefined -> Acc;
				ActivationTime ->
					Duration = Now - ActivationTime,
					case Duration > ThresholdNative of
						true ->
							DurationS = erlang:convert_time_unit(Duration, native, second),
							[{FootprintKey, DurationS, Footprint#footprint.active_count,
							  queue:len(Footprint#footprint.waiting)} | Acc];
						false ->
							Acc
					end
			end
		end, [], FPs),
	case LongRunning of
		[] -> ok;
		_ ->
			?LOG_WARNING([{event, long_running_footprints},
				{peer, ar_util:format_peer(Peer)},
				{count, length(LongRunning)},
				{footprints, LongRunning}])
	end.

%%%===================================================================
%%% Private functions - Metrics
%%%===================================================================

%% Increment prometheus counter - catches errors when prometheus isn't initialized (e.g. in tests)
%% Metric is an atom (e.g., dispatched, queued_in), Peer is the peer tuple, Value is the increment amount
increment_metrics(Metric, Peer, Value) ->
	try
		prometheus_counter:inc(sync_tasks, [Metric, ar_util:format_peer(Peer)], Value)
	catch
		_:_ -> ok
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% @doc Test-only helper to get footprint stats by calling get_state.
get_footprint_stats(Pid) ->
	case gen_server:call(Pid, get_state) of
		{ok, State} ->
			#state{ footprints = FPs, active_footprint_count = ActiveCount, max_footprints = MaxFP } = State,
			TotalWaiting = maps:fold(fun(_, FP, Acc) -> 
				Acc + queue:len(FP#footprint.waiting) 
			end, 0, FPs),
			TotalActive = maps:fold(fun(_, FP, Acc) -> 
				Acc + FP#footprint.active_count 
			end, 0, FPs),
			#{
				footprint_count => maps:size(FPs),
				active_footprint_count => ActiveCount,
				max_footprints => MaxFP,
				total_waiting_tasks => TotalWaiting,
				total_active_tasks => TotalActive
			};
		Error ->
			Error
	end.

peer_worker_test_() ->
	{foreach,
		fun setup/0,
		fun cleanup/1,
		[
			fun test_enqueue_and_process/1,
			fun test_task_completed/1,
			fun test_cut_queue/1,
			fun test_footprint_basic/1,
			fun test_footprint_limit/1,
			fun test_footprint_completion/1
		]
	}.

setup() ->
	Peer = {1, 2, 3, 4, 1984},
	%% Start peer worker directly (not via supervisor, unnamed for test isolation)
	{ok, Pid} = gen_server:start(?MODULE, [Peer], []),
	{Peer, Pid}.

cleanup({_Peer, Pid}) ->
	gen_server:stop(Pid),
	ok.

test_enqueue_and_process({Peer, Pid}) ->
	fun() ->
		%% Enqueue some tasks (no footprint)
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, none, {0, 100, Peer, store1, none}}),
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, none, {100, 200, Peer, store1, none}}),
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, none, {200, 300, Peer, store1, none}}),

		%% Process queue - should get up to 2 tasks
		Tasks = gen_server:call(Pid, {process_queue, 2}),
		?assertEqual(2, length(Tasks)),

		%% Check state
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(1, queue:len(State#state.queued)),
		?assertEqual(2, State#state.dispatched_count)
	end.

test_task_completed({Peer, Pid}) ->
	fun() ->
		%% Enqueue and dispatch a task (no footprint)
		{ok, _} = gen_server:call(Pid, {enqueue_task, none, {0, 100, Peer, store1, none}}),
		[_Task] = gen_server:call(Pid, {process_queue, 1}),

		%% Complete the task (FootprintKey = none) - cast is async
		gen_server:cast(Pid, {task_completed, none, ok, 0, 100}),
		
		%% Wait for cast to be processed by doing a sync call
		{ok, State} = gen_server:call(Pid, get_state),
		?assertEqual(0, State#state.dispatched_count)
	end.

test_cut_queue({Peer, Pid}) ->
	fun() ->
		%% Enqueue 25 tasks (more than MIN_PEER_QUEUE = 20)
		lists:foreach(fun(I) ->
			gen_server:call(Pid, {enqueue_task, none, {I * 100, (I + 1) * 100, Peer, store1, none}})
		end, lists:seq(0, 24)),

		%% Check we have 25 tasks
		{ok, State1} = gen_server:call(Pid, get_state),
		?assertEqual(25, queue:len(State1#state.queued)),

		%% Rebalance with scaling factor that gives MaxQueue = 20 (MIN_PEER_QUEUE)
		%% MaxQueue = max(PeerThroughput * ScalingFactor, MIN_PEER_QUEUE)
		%% With PeerThroughput = 100, ScalingFactor = 0.1 => MaxQueue = max(10, 20) = 20
		Performance = #performance{ current_rating = 100.0, average_latency = 50.0 },
		%% RebalanceParams = {QueueScalingFactor, TargetLatency, WorkersStarved}
		%% FasterThanTarget = (50.0 < 100.0) = true
		RebalanceParams = {0.1, 100.0, false},
		RemovedCount = gen_server:call(Pid, {rebalance, Performance, RebalanceParams}),
		?assertEqual(5, RemovedCount),

		%% Check we have 20 tasks left (MIN_PEER_QUEUE)
		{ok, State2} = gen_server:call(Pid, get_state),
		?assertEqual(20, queue:len(State2#state.queued))
	end.

test_footprint_basic({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue task with footprint - should activate it
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, FootprintKey, {0, 100, Peer, store1, FootprintKey}}),
		
		%% Check footprint stats
		Stats = get_footprint_stats(Pid),
		?assertEqual(1, maps:get(active_footprint_count, Stats)),
		?assertEqual(1, maps:get(total_active_tasks, Stats)),

		%% Complete the task (using task_completed with footprint key) - cast is async
		gen_server:cast(Pid, {task_completed, FootprintKey, ok, 0, 100}),
		
		%% Footprint should be deactivated (get_footprint_stats will wait for cast to process)
		Stats2 = get_footprint_stats(Pid),
		?assertEqual(0, maps:get(active_footprint_count, Stats2))
	end.

test_footprint_limit({Peer, Pid}) ->
	fun() ->
		%% Verify state is accessible
		{ok, _State0} = gen_server:call(Pid, get_state),
		
		%% Test that multiple footprints can be activated
		FootprintKey1 = {store1, 1000, Peer},
		FootprintKey2 = {store1, 2000, Peer},
		
		%% Enqueue tasks for two footprints
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, FootprintKey1, {0, 100, Peer, store1, FootprintKey1}}),
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, FootprintKey2, {100, 200, Peer, store1, FootprintKey2}}),
		
		Stats = get_footprint_stats(Pid),
		?assertEqual(2, maps:get(active_footprint_count, Stats))
	end.

test_footprint_completion({Peer, Pid}) ->
	fun() ->
		FootprintKey = {store1, 1000, Peer},
		%% Enqueue multiple tasks for same footprint
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, FootprintKey, {0, 100, Peer, store1, FootprintKey}}),
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, FootprintKey, {100, 200, Peer, store1, FootprintKey}}),
		{ok, {0, 1}} = gen_server:call(Pid, {enqueue_task, FootprintKey, {200, 300, Peer, store1, FootprintKey}}),
		
		Stats1 = get_footprint_stats(Pid),
		?assertEqual(3, maps:get(total_active_tasks, Stats1)),

		%% Complete tasks one by one (using task_completed with footprint key) - casts are async
		gen_server:cast(Pid, {task_completed, FootprintKey, ok, 0, 100}),
		Stats2 = get_footprint_stats(Pid),  %% Wait for cast to process
		?assertEqual(2, maps:get(total_active_tasks, Stats2)),
		?assertEqual(1, maps:get(active_footprint_count, Stats2)),  %% Still active

		gen_server:cast(Pid, {task_completed, FootprintKey, ok, 0, 100}),
		gen_server:cast(Pid, {task_completed, FootprintKey, ok, 0, 100}),
		
		Stats3 = get_footprint_stats(Pid),  %% Wait for casts to process
		?assertEqual(0, maps:get(total_active_tasks, Stats3)),
		?assertEqual(0, maps:get(active_footprint_count, Stats3))  %% Deactivated
	end.

-endif.
