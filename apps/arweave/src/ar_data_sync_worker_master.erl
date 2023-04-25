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

-record(state, {
	task_queue = queue:new(),
	task_queue_len = 0,
	workers = queue:new(),
	worker_count = 0
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
	{ok, #state{ workers = queue:from_list(Workers), worker_count = length(Workers) }}.

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
	#state{ task_queue = Q, task_queue_len = Len, workers = WorkerQ } = State,
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
			State2 = State#state{ workers = WorkerQ3, task_queue = Q3, task_queue_len = Len2 },
			process_task_queue(N - 1, State2);
		sync_range ->
			{Start, End, Peer, TargetStoreID} = Args,
			{{value, W}, WorkerQ2} = queue:out(WorkerQ),
			ets:update_counter(?MODULE, scheduled_tasks, {2, 1}, {scheduled_tasks, 0}),
			prometheus_gauge:inc(scheduled_sync_tasks),
			gen_server:cast(W, {sync_range, {Start, End, Peer, TargetStoreID, 3}}),
			WorkerQ3 = queue:in(W, WorkerQ2),
			State2 = State#state{ workers = WorkerQ3, task_queue = Q2,
					task_queue_len = Len - 1 },
			process_task_queue(N - 1, State2)
	end.
