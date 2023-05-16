-module(ar_process_sampler).
-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SAMPLE_INTERVAL, 1000).

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
	ProcessNames = lists:filtermap(fun(Pid) -> process_function(Pid) end, Processes),
	lists:foreach(fun(Name) -> prometheus_counter:inc(process_functions, [Name]) end, ProcessNames),
	{noreply, State};

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal functions
process_function(Pid) ->
	case process_info(Pid, [current_function, status]) of
	[{current_function, {?MODULE, process_function, _A}}, _] ->
		false;
	[{current_function, {erlang, process_info, _A}}, _] ->
		false;
	[{current_function, {M, F, A}}, {status, running}] ->
		{true, atom_to_list(M) ++ ":" ++ atom_to_list(F) ++ "/" ++ integer_to_list(A)};
	_ ->
		false
	end.