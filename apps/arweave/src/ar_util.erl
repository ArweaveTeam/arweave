-module(ar_util).
-export([system_memory/0]).
-export([assert_file_exists_and_readable/1, block_index_entry_from_block/1, cast_after/3, do_until/3, genesis_wallets/0, get_system_device/1, message_queue_len/1, print_stacktrace/0, safe_ets_lookup/2, terminal_clear/0]).
-include("ar.hrl").


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


%% @doc Safely lookup a key in an ETS table.
%% Returns [] if the table doesn't exist - this can happen when running some of the helper
%% utilities like data_doctor
safe_ets_lookup(Table, Key) ->
    try
        ets:lookup(Table, Key)
    catch
        Type:Reason ->
            ?LOG_WARNING([{event, ets_table_not_found}, {table, Table}, {key, Key},
                {type, Type}, {reason, Reason}]),
            []
    end.


%% @doc Generate a list of GENESIS wallets, from the CSV file.
genesis_wallets() ->
    {ok, Bin} = file:read_file("genesis_data/genesis_wallets.csv"),
    lists:map(
        fun(Line) ->
            [Addr, RawQty] = string:tokens(Line, ","),
            {
                arweave_util:decode(Addr),
                erlang:trunc(math:ceil(list_to_integer(RawQty))) * ?WINSTON_PER_AR,
                <<>>
            }
        end,
        string:tokens(binary_to_list(Bin), [10])
    ).


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


block_index_entry_from_block(B) ->
    {B#block.indep_hash, B#block.weave_size, B#block.tx_root}.


cast_after(0, Module, Message) ->
    gen_server:cast(Module, Message);
cast_after(Delay, Module, Message) ->
    %% Not using timer:apply_after here because send_after is more efficient:
    %% http://erlang.org/doc/efficiency_guide/commoncaveats.html#timer-module.
    erlang:send_after(Delay, Module, {'$gen_cast', Message}).


%% @doc os aware way of clearing a terminal
terminal_clear() ->
    io:format(
        case os:type() == "darwin" of
            true -> "\e[H\e[J";
            false ->  os:cmd(clear)
        end
    ).

get_system_device(Path) ->
    Command = "df -P " ++ Path ++ " | awk 'NR==2 {print $1}'",
    Device = os:cmd(Command),
    string:trim(Device).


print_stacktrace() ->
    try
    throw(dummy) %% In OTP21+ try/catch is the recommended way to get the stacktrace
    catch
    _: _Exception:Stacktrace ->
        %% Remove the first element (print_stacktrace call)
        TrimmedStacktrace = lists:nthtail(1, Stacktrace),
            StacktraceString = lists:foldl(
                fun(StackTraceEntry, Acc) ->
            Acc ++ io_lib:format("  ~p~n", [StackTraceEntry])
        end, "Stack trace:~n", TrimmedStacktrace),
            ?LOG_INFO(StacktraceString)
    end.


% Function to assert that a file exists and is readable
assert_file_exists_and_readable(FilePath) ->
    case file:read_file(FilePath) of
        {ok, _} ->
            ok;
        {error, _} ->
            io:format("~nThe filepath ~p doesn't exist or isn't readable.~n~n", [FilePath]),
            init:stop(1)
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
