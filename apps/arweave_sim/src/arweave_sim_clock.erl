%%% Deterministic clock; no production application depends on this module.
-module(arweave_sim_clock).
-export([start/0, stop/0, advance/1, sleeping/0, pending/0]).
-export([
    apply_after/4,
    apply_interval/4,
    send_after/3,
    send_interval/3,
    cancel/1,
    monotonic_ms/0,
    system_ms/0,
    sleep/1
]).
-define(SIMULATED_TAB, arweave_sim_clock).

apply_after(Time, Module, Function, Arguments) ->
    {ok, simulated_arm(Time, {mfa, Module, Function, Arguments}, false)}.

apply_interval(Time, Module, Function, Arguments) ->
    {ok, simulated_arm(Time, {mfa, Module, Function, Arguments}, Time)}.

send_after(Time, Pid, Message) ->
    {ok, simulated_arm(Time, {msg, Pid, Message}, false)}.

send_interval(Time, Pid, Message) ->
    {ok, simulated_arm(Time, {msg, Pid, Message}, Time)}.

cancel(Ref) ->
    catch ets:match_delete(?SIMULATED_TAB, {{'_', Ref}, '_', '_'}),
    {ok, cancel}.

monotonic_ms() -> now_simulated().
system_ms() -> now_simulated().
pending() -> ets:tab2list(?SIMULATED_TAB).

sleep(Time) ->
    Marker = make_ref(),
    {ok, _} = send_after(Time, self(), {ar_timer_wake, Marker}),
    receive
        {ar_timer_wake, Marker} -> ok
    end.

now_simulated() ->
    ets:lookup_element(?SIMULATED_TAB, now_ms, 2).

%% Register a simulated timer; fires when advance/1 crosses FireAt.
simulated_arm(Time, Action, Interval) ->
    Ref = {test_clock, ?MODULE, make_ref()},
    FireAt = now_simulated() + Time,
    ets:insert(?SIMULATED_TAB, {{FireAt, Ref}, Action, Interval}),
    Ref.

simulated_deliver({msg, Dest, Message}) ->
    case resolve_dest(Dest) of
        undefined ->
            ok;
        Pid ->
            Pid ! Message,
            ok
    end;
simulated_deliver({mfa, Module, Function, Arguments}) ->
    try
        erlang:apply(Module, Function, Arguments)
    catch
        _:_ -> ok
    end,
    ok.

resolve_dest(Pid) when is_pid(Pid) -> Pid;
resolve_dest(Name) when is_atom(Name) -> erlang:whereis(Name).

start() ->
    catch ets:new(?SIMULATED_TAB, [named_table, public, ordered_set]),
    ets:delete_all_objects(?SIMULATED_TAB),
    ets:insert(?SIMULATED_TAB, {now_ms, 0}),
    ar_timer:override_clock(?MODULE),
    ok.

%%--------------------------------------------------------------------
%% @doc Back to the real clock; pending simulated timers are dropped.
%% The sim harness kills any process still parked in a simulated sleep
%% before dropping the clock.
%% @end
%%--------------------------------------------------------------------
stop() ->
    ar_timer:reset_clock(),
    catch ets:delete(?SIMULATED_TAB),
    ok.

%%--------------------------------------------------------------------
%% @doc Return the number of processes parked in a simulated sleep/1.
sleeping() ->
    SleepingPIDs = ets:select(
        ?SIMULATED_TAB,
        [{{{'_', '_'}, {msg, '$1', {ar_timer_wake, '_'}}, '_'}, [], ['$1']}]
    ),
    length([Pid || Pid <- SleepingPIDs, is_process_alive(Pid)]).

%% @doc Advance simulated time by Ms, delivering every timer that
%% comes due, in firing order, from the calling process. now_ms is set to
%% each timer's firing instant before delivery so re-armed timers chain
%% correctly; intervals re-arm themselves.
%% @end
%%--------------------------------------------------------------------
advance(Ms) ->
    Target = now_simulated() + Ms,
    advance_until(Target),
    ets:insert(?SIMULATED_TAB, {now_ms, Target}),
    ok.

advance_until(Target) ->
    case ets:next(?SIMULATED_TAB, now_ms) of
        '$end_of_table' ->
            ok;
        {FireAt, _Ref} = Key when FireAt =< Target ->
            [{_, Action, Interval}] = ets:lookup(?SIMULATED_TAB, Key),
            ets:delete(?SIMULATED_TAB, Key),
            ets:insert(?SIMULATED_TAB, {now_ms, FireAt}),
            case Interval of
                false ->
                    ok;
                Period ->
                    ets:insert(
                        ?SIMULATED_TAB,
                        {{FireAt + Period, element(2, Key)}, Action, Interval}
                    )
            end,
            simulated_deliver(Action),
            advance_until(Target);
        _ ->
            ok
    end.
