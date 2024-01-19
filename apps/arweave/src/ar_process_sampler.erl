-module(ar_process_sampler).
-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SAMPLE_PROCESSES_INTERVAL, 10000).
-define(SAMPLE_SCHEDULERS_INTERVAL, 30000).
-define(SAMPLE_SCHEDULERS_DURATION, 5000).

-record(state, {
	scheduler_samples = undefined
}).

%% API
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
	timer:send_interval(?SAMPLE_PROCESSES_INTERVAL, sample_processes),
	ar_util:cast_after(?SAMPLE_SCHEDULERS_INTERVAL, ?MODULE, sample_schedulers),
	{ok, #state{}}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(sample_schedulers, State) ->
	State2 = sample_schedulers(State),
	{noreply, State2};
	
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(sample_processes, State) ->
	StartTime = erlang:monotonic_time(),
	Processes = erlang:processes(),
	ProcessData = lists:filtermap(fun(Pid) -> process_function(Pid) end, Processes),

	ProcessMetrics =
		lists:foldl(fun({_Status, ProcessName, Memory, Reductions, MsgQueueLen}, Acc) ->
			%% Sum the data for each process. This is a compromise for handling unregistered
			%% processes. It has the effect of summing the memory and message queue length across all unregistered processes running off the
			%% same function. In general this is what we want (e.g. for the io threads within
			%% ar_mining_io and the hashing threads within ar_mining_hashing, we wand to
			%% see if, in aggregate, their memory or message queue length has spiked).
			{MemoryTotal, ReductionsTotal, MsgQueueLenTotal} =
				maps:get(ProcessName, Acc, {0, 0, 0}),
			Metrics = {
				MemoryTotal + Memory, ReductionsTotal + Reductions, MsgQueueLenTotal + MsgQueueLen},
			maps:put(ProcessName, Metrics, Acc)
		end, 
		#{},
		ProcessData),

	%% Clear out the process_info metric so that we don't persist data about processes that
	%% have exited. We have to deregister and re-register the metric because we don't track
	%% all the label values used.
	prometheus_gauge:deregister(process_info),
	prometheus_gauge:new([{name, process_info},
		{labels, [process, type]},
		{help, "Sampling info about active processes. Only set when debug=true."}]),

	maps:foreach(fun(ProcessName, Metrics) ->
		{Memory, Reductions, MsgQueueLen} = Metrics,
		prometheus_gauge:set(process_info, [ProcessName, memory], Memory),
		prometheus_gauge:set(process_info, [ProcessName, reductions], Reductions),
		prometheus_gauge:set(process_info, [ProcessName, message_queue], MsgQueueLen)
	end, ProcessMetrics),

	prometheus_gauge:set(process_info, [total, memory], erlang:memory(total)),
	prometheus_gauge:set(process_info, [processes, memory], erlang:memory(processes)),
	prometheus_gauge:set(process_info, [processes_used, memory], erlang:memory(processes_used)),
	prometheus_gauge:set(process_info, [system, memory], erlang:memory(system)),
	prometheus_gauge:set(process_info, [atom, memory], erlang:memory(atom)),
	prometheus_gauge:set(process_info, [atom_used, memory], erlang:memory(atom_used)),
	prometheus_gauge:set(process_info, [binary, memory], erlang:memory(binary)),
	prometheus_gauge:set(process_info, [code, memory], erlang:memory(code)),
	prometheus_gauge:set(process_info, [ets, memory], erlang:memory(ets)),

	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, microsecond),
	?LOG_DEBUG([{event, sample_processes}, {elapsed_ms, ElapsedTime / 1000}]),
	{noreply, State};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%% Internal functions
sample_schedulers(#state{ scheduler_samples = undefined } = State) ->
	%% Start sampling
	erlang:system_flag(scheduler_wall_time,true),
	Samples = scheduler:sample_all(),
	%% Every ?SAMPLE_SCHEDULERS_INTERVAL ms, we'll sample the schedulers for 
	%% ?SAMPLE_SCHEDULERS_DURATION ms.
	ar_util:cast_after(?SAMPLE_SCHEDULERS_INTERVAL, ?MODULE, sample_schedulers),
	ar_util:cast_after(?SAMPLE_SCHEDULERS_DURATION, ?MODULE, sample_schedulers),
	State#state{ scheduler_samples = Samples };
sample_schedulers(#state{ scheduler_samples = Samples1 } = State) ->
	%% Finish sampling
	Samples2 = scheduler:sample_all(),
	Util = scheduler:utilization(Samples1, Samples2),
	erlang:system_flag(scheduler_wall_time,false),
	average_utilization(Util),
	State#state{ scheduler_samples = undefined }.

average_utilization(Util) ->
	Averages = lists:foldl(
		fun
		({Type, Value, _}, Acc) ->
			maps:put(Type, {Value, 1}, Acc);
		({Type, _, Value, _}, Acc) ->
			case (Type == io andalso Value > 0) orelse (Type /= io) of
				true ->
					{Sum, Count} = maps:get(Type, Acc, {0, 0}),
					maps:put(Type, {Sum+Value, Count+1}, Acc);
				false ->
					Acc
			end
		end,
		#{},
		Util),
	maps:foreach(
		fun(Type, {Sum, Count}) ->
			prometheus_gauge:set(scheduler_utilization, [Type], Sum / Count)
		end,
		Averages).
	
process_function(Pid) ->
	case process_info(Pid, [current_function, current_stacktrace, registered_name,
		status, memory, reductions, message_queue_len]) of
	[{current_function, {erlang, process_info, _A}}, _, _, _, _, _, _] ->
		false;
	[{current_function, _CurrentFunction}, {current_stacktrace, Stack},
			{registered_name, Name}, {status, Status},
			{memory, Memory}, {reductions, Reductions},
			{message_queue_len, MsgQueueLen}] ->
		ProcessName = process_name(Name, Stack),
		{true, {Status, ProcessName, Memory, Reductions, MsgQueueLen}};
	_ ->
		false
	end.

binary_memory([{ID, Size, _RefCount} | BinInfo], IDs, Total) ->
	case sets:is_element(ID, IDs) of
		true ->
			binary_memory(BinInfo, IDs, Total);
		false ->
			binary_memory(BinInfo, sets:add_element(ID, IDs), Total + Size)
	end;
binary_memory([], _IDs, Total) ->
	Total.

%% @doc Anonymous processes don't have a registered name. So we'll name them after their
%% module, function and arity.
process_name([], []) ->
	"unknown";
process_name([], Stack) ->
	InitialCall = initial_call(lists:reverse(Stack)),
	M = element(1, InitialCall),
	F = element(2, InitialCall),
	A = element(3, InitialCall),
	atom_to_list(M) ++ ":" ++ atom_to_list(F) ++ "/" ++ integer_to_list(A);
process_name(Name, _Stack) ->
	atom_to_list(Name).

initial_call([]) ->
	"unknown";
initial_call([{proc_lib, init_p_do_apply, _A, _Location} | Stack]) ->
	initial_call(Stack);
initial_call([InitialCall | _Stack]) ->
	InitialCall.


function_name(ProcessName, {M, F, A}) ->
	ProcessName ++ "~" ++ atom_to_list(M) ++ ":" ++ atom_to_list(F) ++ "/" ++ integer_to_list(A).
