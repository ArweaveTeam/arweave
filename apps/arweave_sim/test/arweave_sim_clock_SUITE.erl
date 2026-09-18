-module(arweave_sim_clock_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

all() -> [simulated_clock, dead_sleeper_is_not_counted].

%%====================================================================
%% Test cases
%%====================================================================
%% @doc Host and simulator timers share time, cancellation and ordered deadline
%% delivery.
simulated_clock(_Config) ->
    arweave_sim:start_clock(),
    try
        ?assertEqual(0, ar_timer:monotonic_ms()),
        ?assertEqual(0, arweave_sim:monotonic_ms()),
        Self = self(),
        {ok, _} = ar_timer:send_after(100, Self, tick_a),
        {ok, Ref} = ar_timer:send_after(200, Self, tick_b),
        {ok, _} = ar_timer:send_interval(150, Self, tock),
        {ok, _} = ar_timer:cancel(Ref),
        %% Interleave simulator and host timers at distinct deadlines to verify
        %% both APIs use the same clock and preserve delivery order.
        {ok, _} = arweave_sim:send_after(250, Self, sim_tick),
        {ok, _} = arweave_sim:apply_after(350, erlang, send, [Self, sim_apply]),
        arweave_sim:advance(400),
        ?assertEqual(400, ar_timer:monotonic_ms()),
        ?assertEqual(400, arweave_sim:monotonic_ms()),
        Received = collect_messages(),
        ?assertEqual([tick_a, tock, sim_tick, tock, sim_apply], Received)
    after
        arweave_sim:stop_clock()
    end.

%% @doc A dead sleeper is removed from the simulated clock's sleeping-process
%% count.
dead_sleeper_is_not_counted(_Config) ->
    arweave_sim:start_clock(),
    try
        Parent = self(),
        Pid = spawn(fun() ->
            Marker = make_ref(),
            %% One minute keeps the timer pending; this test never advances time.
            {ok, _} = ar_timer:send_after(
                60_000, self(), {ar_timer_wake, Marker}
            ),
            Parent ! timer_armed,
            receive
                {ar_timer_wake, Marker} -> ok
            end
        end),
        receive
            timer_armed -> ok
        end,
        ?assertEqual(1, arweave_sim:sleeping()),
        MonitorRef = erlang:monitor(process, Pid),
        exit(Pid, kill),
        receive
            {'DOWN', MonitorRef, process, Pid, _Reason} -> ok
        end,
        ?assertEqual(0, arweave_sim:sleeping())
    after
        arweave_sim:stop_clock()
    end.

%%====================================================================
%% Helpers
%%====================================================================

collect_messages() ->
    receive
        M -> [M | collect_messages()]
    after 0 -> []
    end.
