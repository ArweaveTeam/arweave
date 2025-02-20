-module(ar_process_sampler).
-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SAMPLE_PROCESSES_INTERVAL, 15000).
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

	log_binary_alloc(),

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
		status, memory, reductions, message_queue_len, messages]) of
	[{current_function, {erlang, process_info, _A}}, _, _, _, _, _, _, _] ->
		false;
	[{current_function, CurrentFunction}, {current_stacktrace, Stack},
			{registered_name, Name}, {status, Status},
			{memory, Memory}, {reductions, Reductions},
			{message_queue_len, MsgQueueLen}, {messages, Messages}] ->
		ProcessName = process_name(Name, Stack),
		case MsgQueueLen > 1000 of
			true ->
				FormattedMessages =
					[format_message(Msg) || Msg <- lists:sublist(Messages, 10)],
				?LOG_DEBUG([{event, process_long_message_queue}, {pid, Pid},
					{process_name, ProcessName}, {current_function, CurrentFunction},
					{current_stacktrace, Stack}, {memory, Memory},
					{reductions, Reductions}, {message_queue_len, MsgQueueLen},
					{head_messages, FormattedMessages}]);
			false ->
				ok
		end,
		{true, {Status, ProcessName, Memory, Reductions, MsgQueueLen}};
	_ ->
		false
	end.

log_binary_alloc() ->
	[Instance0 | _Rest] = erlang:system_info({allocator, binary_alloc}),
	log_binary_alloc_instances([Instance0]).

log_binary_alloc_instances([]) ->
	ok;
log_binary_alloc_instances([Instance | _Rest]) ->
	{instance, Id, [
		_Versions,
		_Options,
		MBCS,
		SBCS,
		Calls
	]} = Instance,
	{calls, [
		{binary_alloc, AllocGigaCount, AllocCount},
		{binary_free, FreeGigaCount, FreeCount},
		{binary_realloc, ReallocGigaCount, ReallocCount},
		_MsegAllocCount, _MsegDeallocCount, _MsegReallocCount,
		_SysAllocCount, _SysDeallocCount, _SysReallocCount
	]} = Calls,

	log_binary_alloc_carrier(Id, MBCS),
	log_binary_alloc_carrier(Id, SBCS),

	prometheus_gauge:set(allocator, [binary, Id, calls, binary_alloc_count],
		(AllocGigaCount * 1000000000) + AllocCount),
	prometheus_gauge:set(allocator, [binary, Id, calls, binary_free_count],
		(FreeGigaCount * 1000000000) + FreeCount),
	prometheus_gauge:set(allocator, [binary, Id, calls, binary_realloc_count],
		(ReallocGigaCount * 1000000000) + ReallocCount).

log_binary_alloc_carrier(Id, Carrier) ->
	{CarrierType, [
		{blocks, Blocks},
		{carriers, _, CarrierCount, _},
		_MsegCount, _SysCount,
		{carriers_size, _, CarrierSize, _},
		_MsegSize, _SysSize
	]} = Carrier,

	case Blocks of
		[{binary_alloc, [{count, _, BlockCount, _}, {size, _, BlockSize, _}]}] ->
			prometheus_gauge:set(allocator, [binary, Id, CarrierType, binary_block_count],
				BlockCount),
			prometheus_gauge:set(allocator, [binary, Id, CarrierType, binary_block_size],
				BlockSize);
		_ ->
			prometheus_gauge:set(allocator, [binary, Id, CarrierType, binary_block_count],
				0),
			prometheus_gauge:set(allocator, [binary, Id, CarrierType, binary_block_size],
				0)
	end,

	prometheus_gauge:set(allocator, [binary, Id, CarrierType, binary_carrier_count],
		CarrierCount),
	prometheus_gauge:set(allocator, [binary, Id, CarrierType, binary_carrier_size], 
		CarrierSize).


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


format_message(Msg) ->
    TruncatedMsg = truncate_term(Msg),
    Formatted = io_lib:format("~p", [TruncatedMsg]),
    OutputStr = lists:flatten(Formatted),
    LimitedOutput = limit_output(OutputStr, 1000),
    io_lib:format("~s~n", [LimitedOutput]).

limit_output(Str, Limit) ->
    if
        length(Str) > Limit -> lists:sublist(Str, Limit);
        true -> Str
    end.

truncate_term(Term) when is_binary(Term) ->
    if
        byte_size(Term) > 8 ->
            <<Head:8/binary, _/binary>> = Term,
            %% Append ellipsis (three periods) to indicate truncation.
            <<Head/binary, 46,46,46>>;
        true ->
            Term
    end;
truncate_term(Term) when is_list(Term) ->
    [truncate_term(Elem) || Elem <- Term];
truncate_term(Term) when is_tuple(Term) ->
    List = tuple_to_list(Term),
    TruncatedList = [truncate_term(Elem) || Elem <- List],
    list_to_tuple(TruncatedList);
truncate_term(Term) when is_map(Term) ->
    maps:map(fun(_Key, Value) -> truncate_term(Value) end, Term);
truncate_term(Term) ->
    Term.