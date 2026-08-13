%%% @doc A timer wrapper/manager for Arweave.
%%%
%%% This module has been created to deal with all timers started by
%%% Arweave. Those timers must be managed, in particular during
%%% shutdown, when no new connections or other actions are required.
%%%
%%% Not all timers need to use this module, only the ones needing
%%% to use a timer to connect to remote peers.
%%%
%%% Only intervals are currently managed, other functions are simple
%%% wrappers.
%%%
%%% This module is tightly coupled with `ar_shutdown_manager' and
%%% uses `ar_shutdown_manager:apply/4' to know if the application is
%%% in running mode or if the application is being stopped.
%%%
%%% @see ar_shutdown_manager
%%% @see ar_shutdown_manager:apply/4
%%%
%%% == Examples ==
%%%
%%% When the application is running normally, this module behave
%%% exactly like the functions exported by timers:
%%%
%%% ```
%%% {ok, Ref1} =
%%%   ar_timer:apply_after(
%%%       10_000,
%%%       io,
%%%       format,
%%%       ["hello"],
%%%       #{}
%%% ).
%%% '''
%%%
%%% If the application is stopped, for example when executing the
%%% `./bin/stop' script or `erlang:halt/1' or `init:stop/1' functions,
%%% then those functions will return `shutdown'. This is the default
%%% behavior when no specific options is passed in the last argument.
%%%
%%% ```
%%% shutdown =
%%%   ar_timer:apply_after(
%%%     10_000,
%%%     io,
%%%     format,
%%%     ["hello"],
%%%     #{}
%%% ).
%%% '''
%%%
%%% This behavior can be disabled by setting the key `skip_on_shutdown'
%%% to false when needed. In this case, these functions will simply
%%% act as wrappers around `timers' module functions.
%%%
%%% ```
%%% {ok, Ref1} =
%%%   ar_timer:apply_after(
%%%     10_000,
%%%     io,
%%%     format,
%%%     ["hello"],
%%%     #{ skip_on_shutdown => false }
%%%   ).
%%% '''
%%%
%%% @end
%%%===================================================================
-module(ar_timer).
-export([
    apply_after/4,
    apply_after/5,
    apply_interval/4,
    apply_interval/5,
    cancel/1,
    insert_timer/2,
    list_timers/0,
    monotonic_ms/0,
    sleep/1,
    system_ms/0,
    terminate_timers/0,
    send_after/2,
    send_after/3,
    send_after/4,
    send_interval/2,
    send_interval/3,
    send_interval/4
]).
-ifdef(AR_TEST).
%% The simulated clock: tests switch this module to simulated time, where the
%% time readers report a simulated now and timers fire only when advance/1
%% moves it forward.
-export([start_simulated_time/0, stop_simulated_time/0, advance/1,
        sleeping/0]).
-endif.
-include_lib("kernel/include/logger.hrl").

%% Pending simulated timers and the {now_ms, N} row (the atom key sorts
%% before the {{FireAt, Ref}, ...} timer keys in the ordered set).
-define(SIMULATED_TAB, ar_timer_simulated).

%% Dispatch to the simulated clock when it is on. stop_simulated_time/0
%% flips the mode before dropping the table, so the simulated expression
%% may hit a badarg mid-teardown: fall back to the real clock.
-define(REAL_OR_SIMULATED(Real, Simulated),
    case simulated() of
        true ->
            try Simulated
            catch error:badarg -> Real
            end;
        false ->
            Real
    end).
-type ar_timer_opts() :: #{ skip_on_shutdown => boolean() }.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:apply_after/4.
%% @see timer:apply_after/5
%% @end
%%--------------------------------------------------------------------
-spec apply_after(Time, Module, Function, Arguments) -> Return when
    Time :: pos_integer(),
    Module :: atom(),
    Function :: atom(),
    Arguments :: [term()],
    Return :: shutdown | {ok, reference()}.

apply_after(Time, Module, Function, Arguments) ->
    apply_after(Time, Module, Function, Arguments, #{}).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:apply_after/4.
%%
%% @see timer:apply_after/4
%% @end
%%--------------------------------------------------------------------
-spec apply_after(Time, Module, Function, Arguments, Opts) -> Return when
    Time :: pos_integer(),
    Module :: atom(),
    Function :: atom(),
    Arguments :: [term()],
    Opts :: ar_timer_opts(),
    Return :: shutdown
        | {ok, reference()}.

apply_after(Time, Module, Function, Arguments, Opts) ->
    ?REAL_OR_SIMULATED(
        apply_after_real(Time, Module, Function, Arguments, Opts),
        apply_after_simulated(Time, Module, Function, Arguments)).

apply_after_simulated(Time, Module, Function, Arguments) ->
    {ok, simulated_arm(Time, {mfa, Module, Function, Arguments}, false)}.

apply_after_real(Time, Module, Function, Arguments, Opts) ->
    M = timer,
    F = apply_after,
    A = [Time, Module, Function, Arguments],
    shutdown_guard(ar_shutdown_manager:apply(M, F, A, Opts)).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:apply_interval/4.
%% @see timer:apply_interval/4
%% @end
%%--------------------------------------------------------------------
-spec apply_interval(Time, Module, Function, Arguments) -> Return when
    Time :: pos_integer(),
    Module :: atom(),
    Function :: atom(),
    Arguments :: [term()],
    Return :: shutdown
        | {ok, reference()}.

apply_interval(Time, Module, Function, Arguments) ->
    apply_interval(Time, Module, Function, Arguments, #{}).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:apply_interval/4
%% @end
%%--------------------------------------------------------------------
-spec apply_interval(Time, Module, Function, Arguments, Opts) -> Return when
    Time :: pos_integer(),
    Module :: atom(),
    Function :: atom(),
    Arguments :: [term()],
    Opts :: ar_timer_opts(),
    Return :: shutdown
        | {ok, reference()}.

apply_interval(Time, Module, Function, Arguments, Opts) ->
    ?REAL_OR_SIMULATED(
        apply_interval_real(Time, Module, Function, Arguments, Opts),
        apply_interval_simulated(Time, Module, Function, Arguments)).

apply_interval_simulated(Time, Module, Function, Arguments) ->
    {ok, simulated_arm(Time, {mfa, Module, Function, Arguments}, Time)}.

apply_interval_real(Time, Module, Function, Arguments, Opts) ->
    M = timer,
    F = apply_interval,
    A = [Time, Module, Function, Arguments],
    case shutdown_guard(ar_shutdown_manager:apply(M, F, A, Opts)) of
        {ok, none} ->
            {ok, none};
        {ok, TimerRef} ->
            insert_timer(TimerRef, #{
                pid => self(),
                module => Module,
                function => Function,
                arguments => Arguments,
                time => Time,
                opts => Opts
            }),
            {ok, TimerRef};
        Else ->
            Else
    end.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_after/4.
%% @see send_after/3
%% @end
%%--------------------------------------------------------------------
-spec send_after(Time, Message) -> Return when
    Time :: non_neg_integer(),
    Message :: term(),
    Return :: shutdown | {ok, reference()}.

send_after(Time, Message) ->
    send_after(Time, self(), Message).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_after/3.
%% @see send_after/4
%% @end
%%--------------------------------------------------------------------
-spec send_after(Time, Pid, Message) -> Return when
    Time :: non_neg_integer(),
    Pid :: pid() | atom(),
    Message :: term(),
    Return :: shutdown | {ok, reference()}.

send_after(Time, Pid, Message) ->
    send_after(Time, Pid, Message, #{}).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_after/3.
%% @see timer:send_after/3
%% @end
%%--------------------------------------------------------------------
-spec send_after(Time, Pid, Message, Opts) -> Return when
    Time :: non_neg_integer(),
    Pid :: pid() | atom(),
    Message :: term(),
    Opts :: ar_timer_opts(),
    Return :: shutdown | {ok, reference()}.

send_after(Time, Pid, Message, Opts) ->
    ?REAL_OR_SIMULATED(
        send_after_real(Time, Pid, Message, Opts),
        send_after_simulated(Time, Pid, Message)).

send_after_simulated(Time, Pid, Message) ->
    {ok, simulated_arm(Time, {msg, Pid, Message}, false)}.

send_after_real(Time, Pid, Message, Opts) ->
    M = timer,
    F = send_after,
    A = [Time, Pid, Message],
    shutdown_guard(ar_shutdown_manager:apply(M, F, A, Opts)).

%% During node shutdown the shutdown manager refuses new timers. Map the
%% refusal to {ok, none}: a timer created during shutdown that never
%% fires is indistinguishable from one that shutdown beat to the punch,
%% so the refusal carries no information a caller can act on — while
%% surfacing it makes every periodic gen_server's last timer re-creation
%% a badmatch crash, and from init/1 a restart storm that escalates the
%% supervision tree, turning a clean stop into a kill. Genuine errors
%% (badarg and friends) still surface.
shutdown_guard({ok, TimerRef}) -> {ok, TimerRef};
shutdown_guard({error, shutdown}) -> {ok, none};
shutdown_guard(shutdown) -> {ok, none};
shutdown_guard(Else) -> Else.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_interval/2.
%% @see send_interval/3
%% @end
%%--------------------------------------------------------------------
-spec send_interval(Time, Message) -> Return when
    Time :: pos_integer(),
    Message :: term(),
    Return :: shutdown | {ok, reference()}.

send_interval(Time, Message) ->
    send_interval(Time, self(), Message).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:interval/3.
%% @see send_interval/4
%% @end
%%--------------------------------------------------------------------
-spec send_interval(Time, Pid, Message) -> Return when
    Time :: pos_integer(),
    Pid :: atom() | pid(),
    Message :: term(),
    Return :: shutdown | {ok, reference()}.

send_interval(Time, Pid, Message) ->
    send_interval(Time, Pid, Message, #{}).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:interval/3.
%% @see timer:send_interval/3
%% @end
%%--------------------------------------------------------------------
-spec send_interval(Time, Pid, Message, Opts) -> Return when
    Time :: pos_integer(),
    Pid :: atom() | pid(),
    Message :: term(),
    Opts :: ar_timer_opts(),
    Return :: shutdown | {ok, reference()}.

send_interval(Time, Pid, Message, Opts) ->
    ?REAL_OR_SIMULATED(
        send_interval_real(Time, Pid, Message, Opts),
        send_interval_simulated(Time, Pid, Message)).

send_interval_simulated(Time, Pid, Message) ->
    {ok, simulated_arm(Time, {msg, Pid, Message}, Time)}.

send_interval_real(Time, Pid, Message, Opts) ->
    M = timer,
    F = send_interval,
    A = [Time, Pid, Message],
    case shutdown_guard(ar_shutdown_manager:apply(M, F, A, Opts)) of
        {ok, none} ->
            {ok, none};
        {ok, TimerRef} ->
            insert_timer(TimerRef, #{
                pid => self(),
                time => Time,
                opts => Opts
            }),
            {ok, TimerRef};
        Else ->
            Else
    end.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:cancel/1.
%% @see timer:cancel/1
%% @end
%%--------------------------------------------------------------------
cancel(none) ->
    {ok, cancel};
cancel({simulated, Ref}) ->
    catch ets:match_delete(?SIMULATED_TAB, {{'_', {simulated, Ref}}, '_', '_'}),
    {ok, cancel};
cancel(TimerRef) ->
    case timer:cancel(TimerRef) of
        {ok, _} = Reply ->
            ets:delete(?MODULE, {timer, TimerRef}),
            ?LOG_DEBUG([
                {module, ?MODULE},
                {reference, TimerRef},
                {action, cancel}
            ]),
            Reply;
        Else ->
            Else
    end.

%%--------------------------------------------------------------------
%% @doc The monotonic time in milliseconds — the reading for intervals and
%% control loops. Simulated time reports the simulated now.
%% @end
%%--------------------------------------------------------------------
-spec monotonic_ms() -> integer().
monotonic_ms() ->
    ?REAL_OR_SIMULATED(erlang:monotonic_time(millisecond), now_simulated()).

%%--------------------------------------------------------------------
%% @doc The system time in milliseconds — wall-clock stamps only.
%% @end
%%--------------------------------------------------------------------
-spec system_ms() -> integer().
system_ms() ->
    ?REAL_OR_SIMULATED(os:system_time(millisecond), now_simulated()).

%%--------------------------------------------------------------------
%% @doc Sleep. Under simulated time the caller sleeps until advance/1
%% delivers its wake-up; a process must therefore never sleep on the same
%% process that drives advance/1.
%% @end
%%--------------------------------------------------------------------
-spec sleep(non_neg_integer()) -> ok.
sleep(Time) ->
    ?REAL_OR_SIMULATED(timer:sleep(Time), sleep_simulated(Time)).

sleep_simulated(Time) ->
    Marker = make_ref(),
    {ok, _} = send_after(Time, self(), {ar_timer_wake, Marker}),
    receive {ar_timer_wake, Marker} -> ok end.

%% Compiled to `false' outside AR_TEST: production cannot enter
%% simulated time, even by writing the persistent_term directly.
-ifdef(AR_TEST).
simulated() ->
    persistent_term:get({?MODULE, mode}, real) =:= simulated.
-else.
simulated() ->
    false.
-endif.

now_simulated() ->
    ets:lookup_element(?SIMULATED_TAB, now_ms, 2).

%% Register a simulated timer; fires when advance/1 crosses FireAt.
simulated_arm(Time, Action, Interval) ->
    Ref = {simulated, make_ref()},
    FireAt = now_simulated() + Time,
    ets:insert(?SIMULATED_TAB, {{FireAt, Ref}, Action, Interval}),
    Ref.

-ifdef(AR_TEST).
simulated_deliver({msg, Dest, Message}) ->
    case resolve_dest(Dest) of
        undefined -> ok;
        Pid -> Pid ! Message, ok
    end;
simulated_deliver({mfa, Module, Function, Arguments}) ->
    try erlang:apply(Module, Function, Arguments) catch _:_ -> ok end,
    ok.

resolve_dest(Pid) when is_pid(Pid) -> Pid;
resolve_dest(Name) when is_atom(Name) -> erlang:whereis(Name).
-endif.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
insert_timer(TimerRef, Meta) ->
    CreatedAt = erlang:system_time(),
    NewMeta = Meta#{
        created_at => CreatedAt
    },
    ?LOG_DEBUG([
        {module, ?MODULE},
        {pid, self()},
        {meta, NewMeta},
        {reference, TimerRef}
    ]),
    ets:insert(?MODULE, {{timer, TimerRef}, NewMeta}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
list_timers() ->
    [ Ref || [Ref] <- ets:match(?MODULE, {{timer, '$1'}, '_'}) ].

%%--------------------------------------------------------------------
%% @hidden
%% @doc terminate all timers. This function will also list the timers
%% from `timer_tab' ETS table and cancel all of them. Timers registered
%% with `skip_on_shutdown => false' are spared: that option declares the
%% timer should keep firing during shutdown (e.g. ar_process_sampler's
%% sampling interval, which lets us observe the teardown).
%% @end
%%--------------------------------------------------------------------
terminate_timers() ->
    Timers = ets:match_object(?MODULE, {{timer, '_'}, '_'}),
    Keep = [ Ref || {{timer, Ref}, #{ opts := #{ skip_on_shutdown := false } }} <- Timers ],

    % cancel all intervals first
    [ cancel(Ref) || {{timer, Ref}, _} <- Timers, not lists:member(Ref, Keep) ],

    % then cancel all others timers from timer_tab.
    case ets:whereis(timer_tab) of
        undefined ->
            ok;
        _ ->
            [ timer:cancel(Ref) || {Ref, _, _} <- ets:tab2list(timer_tab),
                    not lists:member(Ref, Keep) ]
    end.

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

-ifdef(AR_TEST).
%%--------------------------------------------------------------------
%% @doc Switch to simulated time at 0.
%% @end
%%--------------------------------------------------------------------
start_simulated_time() ->
    catch ets:new(?SIMULATED_TAB, [named_table, public, ordered_set]),
    ets:delete_all_objects(?SIMULATED_TAB),
    ets:insert(?SIMULATED_TAB, {now_ms, 0}),
    persistent_term:put({?MODULE, mode}, simulated),
    ok.

%%--------------------------------------------------------------------
%% @doc Back to the real clock; pending simulated timers are dropped.
%% The sim harness kills any process still parked in a simulated sleep
%% (ar_sync_sim_tests:stop_pipeline/0) before dropping the clock.
%% @end
%%--------------------------------------------------------------------
stop_simulated_time() ->
    persistent_term:put({?MODULE, mode}, real),
    catch ets:delete(?SIMULATED_TAB),
    ok.

%%--------------------------------------------------------------------
%% @doc Return the number of processes parked in a simulated sleep/1.
sleeping() ->
    SleepingPIDs = ets:select(?SIMULATED_TAB,
        [{{{'_', '_'}, {msg, '$1', {ar_timer_wake, '_'}}, '_'}, [], ['$1']}]),
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
                false -> ok;
                Period -> ets:insert(?SIMULATED_TAB,
                    {{FireAt + Period, element(2, Key)}, Action, Interval})
            end,
            simulated_deliver(Action),
            advance_until(Target);
        _ ->
            ok
    end.
-endif.


simulated_clock_test() ->
    start_simulated_time(),
    try
        ?assertEqual(0, monotonic_ms()),
        Self = self(),
        {ok, _} = send_after(100, Self, tick_a),
        {ok, Ref} = send_after(200, Self, tick_b),
        {ok, _} = send_interval(150, Self, tock),
        {ok, _} = cancel(Ref),
        advance(400),
        ?assertEqual(400, monotonic_ms()),
        Received = collect_messages(),
        ?assertEqual([tick_a, tock, tock], Received)
    after
        stop_simulated_time()
    end.

dead_simulated_sleeper_is_not_counted_test() ->
    start_simulated_time(),
    try
        Parent = self(),
        Pid = spawn(fun() ->
            Marker = make_ref(),
            %% One minute keeps the timer pending; this test never advances time.
            {ok, _} = send_after(
                60_000, self(), {ar_timer_wake, Marker}),
            Parent ! timer_armed,
            receive {ar_timer_wake, Marker} -> ok end
        end),
        receive timer_armed -> ok end,
        ?assertEqual(1, sleeping()),
        MonitorRef = erlang:monitor(process, Pid),
        exit(Pid, kill),
        receive
            {'DOWN', MonitorRef, process, Pid, _Reason} -> ok
        end,
        ?assertEqual(0, sleeping())
    after
        stop_simulated_time()
    end.

collect_messages() ->
    receive M -> [M | collect_messages()] after 0 -> [] end.

-endif.
