-module(ar_process_sampler).
-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Sample relatively infrequently - every 5 seconds - to minimize the impact on the system.
-define(SAMPLE_INTERVAL, 5000).

%% API
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server callbacks
init([]) ->
	timer:send_interval(?SAMPLE_INTERVAL, sample),
	{ok, #{}}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(sample, State) ->
	Processes = erlang:processes(),
	ProcessData = lists:filtermap(fun(Pid) -> process_function(Pid) end, Processes),
	lists:foreach(fun({ProcessName, FunctionName, Memory, Binary}) ->
		prometheus_counter:inc(process_functions, [FunctionName]),
		prometheus_gauge:set(process_memory, [ProcessName, memory], Memory),
		prometheus_gauge:set(process_memory, [ProcessName, binary], Binary)
	end, ProcessData),
	{noreply, State};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal functions
process_function(Pid) ->
	case process_info(Pid, [current_function, registered_name, status, memory, binary]) of
	[{current_function, {?MODULE, process_function, _A}}, _, _, _, _] ->
		false;
	[{current_function, {erlang, process_info, _A}}, _, _, _, _] ->
		false;
	[{current_function, {M, F, A}}, {registered_name, Name}, {status, running},
			{memory, Memory}, {binary, Binary}] ->
		ProcessName = process_name(Name),
		FunctionName = function_name(ProcessName, M, F, A),
		{true, {ProcessName, FunctionName, Memory, Binary}};
	_ ->
		false
	end.

process_name([]) ->
	process_name(unknown);
process_name(ProcessName) ->
	atom_to_list(ProcessName).

function_name(ProcessName, M, F, A) ->
	ProcessName ++ "~" ++ atom_to_list(M) ++ ":" ++ atom_to_list(F) ++ "/" ++ integer_to_list(A).