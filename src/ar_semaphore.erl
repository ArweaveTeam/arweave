-module(ar_semaphore).
-behaviour(gen_server).

-export([start_link/2, acquire/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Open a semaphore registered with Name, with the specified
%% Capacity.
start_link(Name, InitCapacity) ->
	{ok, _} = gen_server:start_link({local, Name}, ?MODULE, [InitCapacity], []),
	ok.

%% @doc Acquire the semaphore, willing to wait for the provided
%% Timeout.
acquire(Name, Timeout) ->
	try
		gen_server:call(Name, acquire, Timeout)
	catch
		exit:{timeout, _} -> {error, timeout}
	end.

%% @doc Close the semaphore and stop the process registered under the
%% given name.
stop(Name) ->
	gen_server:stop(Name).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([InitCapacity]) ->
	{ok, {InitCapacity, #{}, queue:new()}}.

handle_call(acquire, {FromPid, FromRef}, {Capacity, WaitingPids, Queue}) ->
	case maps:is_key(FromPid, WaitingPids) of
		true ->
			{reply, {error, process_already_waiting}, {Capacity, WaitingPids, Queue}};
		false ->
			case Capacity > 0 of
				true ->
					monitor(process, FromPid),
					{reply, ok, {Capacity - 1, WaitingPids#{ FromPid => {} }, Queue}};
				false ->
					Queue1 = queue:in({FromPid, FromRef}, Queue),
					{noreply, {Capacity, WaitingPids, Queue1}}
			end
	end.

handle_cast(_, State) ->
	{stop, {error, handle_cast_unsupported}, State}.

handle_info({'DOWN', _,  process, Pid, _}, {Capacity, WaitingPids, Queue}) ->
	case maps:take(Pid, WaitingPids) of
		{{}, WaitingPids1} ->
			dequeue({Capacity + 1, WaitingPids1, Queue});
		error ->
			{noreply, {Capacity, WaitingPids, Queue}}
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

dequeue({Capacity, WaitingPids, Queue}) ->
	case Capacity > 0 of
		false ->
			{noreply, {Capacity, WaitingPids, Queue}};
		true ->
			case queue:out(Queue) of
				{empty, Queue} ->
					{noreply, {Capacity, WaitingPids, Queue}};
				{{value, {FromPid, FromRef}}, NewQueue} ->
					monitor(process, FromPid),
					gen_server:reply({FromPid, FromRef}, ok),
					{noreply, {Capacity - 1, WaitingPids#{ FromPid => {} }, NewQueue}}
			end
	end.
