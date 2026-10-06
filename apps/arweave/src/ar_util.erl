%%% @doc Host helpers with side effects: delayed casts, polling, process and
%%% system inspection, and terminal output.
-module(ar_util).

-export([
    cast_after/3,
    do_until/3,
    message_queue_len/1,
    system_memory/0,
    terminal_clear/0
]).

%% @doc Cast Message to Module after Delay milliseconds, or at once when Delay
%% is 0.
cast_after(0, Module, Message) ->
    gen_server:cast(Module, Message);
cast_after(Delay, Module, Message) ->
    %% Not using timer:apply_after here because send_after is more efficient:
    %% http://erlang.org/doc/efficiency_guide/commoncaveats.html#timer-module.
    erlang:send_after(Delay, Module, {'$gen_cast', Message}).

%% @doc Perform a function until it returns {ok, Value} | ok | true | {error, Error}.
%% That term will be returned, others will be ignored. Interval and timeout have to
%% be passed in milliseconds.
do_until(_DoFun, _Interval, Timeout) when Timeout =< 0 ->
    {error, timeout};
do_until(DoFun, Interval, Timeout) ->
    Start = erlang:system_time(millisecond),
    case DoFun() of
        {ok, Value} ->
            {ok, Value};
        ok ->
            ok;
        true ->
            true;
        {error, Error} ->
            {error, Error};
        _ ->
            timer:sleep(Interval),
            Now = erlang:system_time(millisecond),
            do_until(DoFun, Interval, Timeout - (Now - Start))
    end.

%% @doc Message queue length of a process given its registered name or
%% pid, or 0 if the name has no live process behind it. Guards against
%% `process_info/2' raising `badarg' on an unregistered name.
message_queue_len(Name) when is_atom(Name) ->
    case whereis(Name) of
        PID when is_pid(PID) ->
            message_queue_len(PID);
        _ ->
            %% Not registered, or the name points at a port.
            0
    end;
message_queue_len(PID) when is_pid(PID) ->
    case erlang:process_info(PID, message_queue_len) of
        {message_queue_len, Len} ->
            Len;
        undefined ->
            0
    end.

%% @doc Return total memory in bytes, capped by container limits, or undefined.
system_memory() ->
    %% A container's limit can be smaller than the host's physical memory.
    lists:foldl(
        fun limit_memory/2,
        host_memory(),
        [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes"
        ]
    ).

host_memory() ->
    case file:read_file("/proc/meminfo") of
        {ok, <<"MemTotal:", Rest/binary>>} ->
            {KiB, _} = string:to_integer(string:trim(binary_to_list(Rest))),
            KiB * 1024;
        _ ->
            try
                proplists:get_value(
                    total_memory,
                    memsup:get_system_memory_data()
                )
            catch
                _:_ -> undefined
            end
    end.

limit_memory(Path, Total) ->
    case file:read_file(Path) of
        {ok, Value} ->
            do_limit_memory(string:to_integer(binary_to_list(Value)), Total);
        _ ->
            Total
    end.

do_limit_memory({Bytes, _}, Total) when is_integer(Bytes), Bytes > 0 ->
    case Total of
        undefined -> Bytes;
        _ -> min(Total, Bytes)
    end;
do_limit_memory(_, Total) ->
    %% Missing numeric limits (including cgroup's "max") do not restrict RAM.
    Total.

%% @doc os aware way of clearing a terminal
terminal_clear() ->
    io:format(
        case os:type() == "darwin" of
            true -> "\e[H\e[J";
            false ->  os:cmd(clear)
        end
    ).
