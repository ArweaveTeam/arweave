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
-export([override_clock/1, reset_clock/0]).
-endif.
-include_lib("kernel/include/logger.hrl").

%% Test clocks are injected by the harness, never selected in production.
%% Teardown can remove the clock table after dispatch, so fall back to real
%% time if that race raises badarg.
-ifdef(AR_TEST).
-define(CLOCK(Real, Function, Arguments),
    case persistent_term:get({?MODULE, clock}, real) of
        real ->
            Real;
        Clock ->
            try
                erlang:apply(Clock, Function, Arguments)
            catch
                error:badarg -> Real
            end
    end
).
-else.
-define(CLOCK(Real, Function, Arguments), Real).
-endif.
-type ar_timer_opts() :: #{skip_on_shutdown => boolean()}.

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
    ?CLOCK(
        do_apply_after(Time, Module, Function, Arguments, Opts),
        apply_after,
        [Time, Module, Function, Arguments]
    ).

do_apply_after(Time, Module, Function, Arguments, Opts) ->
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
    ?CLOCK(
        do_apply_interval(Time, Module, Function, Arguments, Opts),
        apply_interval,
        [Time, Module, Function, Arguments]
    ).

do_apply_interval(Time, Module, Function, Arguments, Opts) ->
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
    ?CLOCK(
        do_send_after(Time, Pid, Message, Opts),
        send_after,
        [Time, Pid, Message]
    ).

do_send_after(Time, Pid, Message, Opts) ->
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
    ?CLOCK(
        do_send_interval(Time, Pid, Message, Opts),
        send_interval,
        [Time, Pid, Message]
    ).

do_send_interval(Time, Pid, Message, Opts) ->
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
cancel(TimerRef) ->
    case cancel_clock(TimerRef) of
        not_clock -> do_cancel(TimerRef);
        Result -> Result
    end.

do_cancel(TimerRef) ->
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
    ?CLOCK(erlang:monotonic_time(millisecond), monotonic_ms, []).

%%--------------------------------------------------------------------
%% @doc The system time in milliseconds — wall-clock stamps only.
%% @end
%%--------------------------------------------------------------------
-spec system_ms() -> integer().
system_ms() ->
    ?CLOCK(os:system_time(millisecond), system_ms, []).

%%--------------------------------------------------------------------
%% @doc Sleep. Under simulated time the caller sleeps until advance/1
%% delivers its wake-up; a process must therefore never sleep on the same
%% process that drives advance/1.
%% @end
%%--------------------------------------------------------------------
-spec sleep(non_neg_integer()) -> ok.
sleep(Time) ->
    ?CLOCK(timer:sleep(Time), sleep, [Time]).

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
%% @doc Install a test clock implementing the timer wrapper operations.
override_clock(Module) ->
    persistent_term:put({?MODULE, clock}, Module).

%% @doc Restore real time before the harness removes its clock state.
reset_clock() ->
    persistent_term:erase({?MODULE, clock}),
    ok.

cancel_clock({test_clock, Module, _} = Ref) ->
    Module:cancel(Ref);
cancel_clock(_) ->
    not_clock.
-else.
cancel_clock(_) ->
    not_clock.
-endif.
