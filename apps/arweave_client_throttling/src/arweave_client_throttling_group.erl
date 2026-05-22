%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @doc Per-group client throttler.
%%%
%%% One `arweave_client_throttling_group' process exists per limiting
%%% group. It maintains, per peer, a quota state and a FIFO queue of
%%% waiting caller pids.
%%%
%%% == Quota state ==
%%%
%%% For each peer the group keeps:
%%%
%%% <ul>
%%%   <li>`total' - the size of the quota window as reported by the
%%%       remote (e.g. "200 requests per minute" => 200).</li>
%%%   <li>`remaining' - how many calls are still allowed before the
%%%       remote will start rejecting us.</li>
%%%   <li>`reset_seconds' - when the quota is exhausted, how many
%%%       seconds the remote says it will take before the budget
%%%       refills. Meaningful only when `remaining =:= 0'.</li>
%%% </ul>
%%%
%%% Behaviour summary:
%%%
%%% <ul>
%%%   <li>`throttle/2' issues a `gen_server:call' that returns
%%%       immediately with either `accepted' (budget available) or
%%%       `{queued, Ref}'. In the queued case the caller waits in its
%%%       own mailbox for `{request_ready, Ref}' (60s timeout, after
%%%       which the entry is cancelled and `{error, timeout}' is
%%%       returned).</li>
%%%   <li>`update_quota/3' is a `gen_server:cast'. It takes a map
%%%       `#{total := T, remaining := R, reset_seconds := S}'
%%%       reflecting the latest state advertised by the remote, drains
%%%       as many waiters as the new `remaining' allows, and never
%%%       blocks the caller. When `remaining' is 0 and `reset_seconds'
%%%       is positive, a timer is scheduled so the budget is refilled
%%%       to `total' once the remote window expires, even if no
%%%       further response arrives.</li>
%%% </ul>
%%%
%%% == Handling concurrent `update_quota' messages ==
%%%
%%% Several in-flight requests can complete around the same time and
%%% each report a different `remaining' value. Without timestamps from
%%% the remote, the safest assumption is that the smallest of two
%%% values reported close together is the most recent (each subsequent
%%% request consumed one slot on the server side). We therefore take
%%% the minimum of the old and the new `remaining' as long as they
%%% fall within `concurrency_window_ms'. Outside that window we trust
%%% the new value, which lets the budget grow back when the remote
%%% rate window resets. `total' and `reset_seconds' are always
%%% replaced with the value from the most recent update.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_group).
-vsn(1).
-behaviour(gen_server).

-export([
    start_link/1,
    throttle/2,
    update_quota/3,
    status/2,
    reset/1,
    pending/2,
    stop/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("arweave_client_throttling.hrl").
-include_lib("kernel/include/logger.hrl").

%% `reset_timer' is `undefined' when no refill is pending, or
%% `{TimerRef, Tag}' where `TimerRef' is the value returned by
%% `erlang:send_after/3' (so we can cancel it) and `Tag' is a unique
%% reference embedded in the scheduled message (so stragglers
%% delivered after cancellation can be discarded on receipt).
%%
%% `waiters' entries are `{Ref, Pid, MRef}' tuples: a unique
%% reference handed back to the caller, the caller pid we will send
%% the `{request_ready, Ref}' notification to, and the monitor
%% reference we use to remove the entry if the caller dies.
-record(peer_state, {
    total          :: non_neg_integer(),
    remaining      :: non_neg_integer(),
    reset_seconds  :: non_neg_integer(),
    reset_timer    :: {reference(), reference()} | undefined,
    waiters        :: queue:queue({reference(), pid(), reference()}),
    last_update_ts :: integer() | undefined
}).

%% How long throttle/2 waits for a `{request_ready, Ref}' message
%% after the gen_server replies with `{queued, Ref}'. On expiry the
%% caller sends a `cancel_request' cast so the entry can be evicted
%% from the queue and returns `{error, timeout}'.
-define(THROTTLE_RECEIVE_TIMEOUT_MS, 60000).

%% @doc Start a group process. `Spec' must be a normalized map (see
%% `arweave_client_throttling_config:normalize_group/1').
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(#{id := Id} = Spec) ->
    gen_server:start_link({local, registered_name(Id)}, ?MODULE, Spec, []).

%% @doc Blocking throttle call.
%%
%% The synchronous part of this function - the `gen_server:call' -
%% never blocks on quota: the group replies with `accepted' when
%% there is budget available, with `{queued, Ref}' when the caller
%% has been enqueued, or with `{error, queue_full}' when the per-peer
%% queue is already saturated.
%%
%% In the `accepted' case `throttle/2' returns `ok' immediately.
%%
%% In the `{queued, Ref}' case the caller waits - in its own mailbox,
%% outside the gen_server - for a `{request_ready, Ref}' notification
%% sent by the group when budget becomes available. The wait has a
%% 60s ceiling; on expiry the caller sends a `cancel_request' cast
%% to evict the entry from the queue and returns `{error, timeout}'.
-spec throttle(atom(), tuple()) -> ok | {error, term()}.
throttle(GroupId, Peer) ->
    Name = registered_name(GroupId),
    case gen_server:call(Name, {throttle, Peer}) of
        accepted ->
            ok;
        {queued, Ref} ->
            receive
                {request_ready, Ref} ->
                    ok
            after ?THROTTLE_RECEIVE_TIMEOUT_MS ->
                gen_server:cast(Name, {cancel_request, Peer, Ref}),
                {error, timeout}
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Non-blocking quota refresh.
%%
%% `Quota' is a map with the following keys, typically extracted from
%% the rate-limit headers of a response coming back from `Peer':
%%
%% <ul>
%%   <li>`total' - full size of the quota window.</li>
%%   <li>`remaining' - how many calls are still allowed.</li>
%%   <li>`reset_seconds' - seconds until the quota refills. Used only
%%       when the quota is exhausted; pass `0' (or any non-negative
%%       integer) when not exhausted.</li>
%% </ul>
-spec update_quota(atom(), tuple(), map()) -> ok.
update_quota(GroupId, Peer, #{
        total := Total,
        remaining := Remaining,
        reset_seconds := ResetSeconds})
  when is_integer(Total), Total >= 0,
       is_integer(Remaining), Remaining >= 0,
       is_integer(ResetSeconds), ResetSeconds >= 0 ->
    ReceivedAt = monotonic_ms(),
    gen_server:cast(registered_name(GroupId),
                    {update_quota, Peer, Total, Remaining,
                     ResetSeconds, ReceivedAt}).

%% @doc Return a snapshot of the per-peer state.
-spec status(atom(), tuple()) -> {ok, map()} | {error, term()}.
status(GroupId, Peer) ->
    gen_server:call(registered_name(GroupId), {status, Peer}).

%% @doc Number of waiting callers currently queued for `Peer'.
-spec pending(atom(), tuple()) -> non_neg_integer().
pending(GroupId, Peer) ->
    case status(GroupId, Peer) of
        {ok, #{queue_length := N}} -> N;
        _ -> 0
    end.

%% @doc Drop all per-peer state. Pending waiters receive a
%% `{request_ready, Ref}' notification so their `throttle/2' returns
%% `ok' rather than staying blocked.
-spec reset(atom()) -> ok.
reset(GroupId) ->
    gen_server:call(registered_name(GroupId), reset).

%% @doc Stop the group process.
-spec stop(atom()) -> ok.
stop(GroupId) ->
    gen_server:stop(registered_name(GroupId)).

%% gen_server callbacks

init(Spec) ->
    process_flag(trap_exit, true),
    {ok, #{
        spec => Spec,
        peers => #{},
        monitors => #{}
    }}.

handle_call({throttle, Peer}, From, #{spec := Spec, peers := Peers} = State) ->
    PS0 = get_or_init_peer(Peer, Peers, Spec),
    case PS0#peer_state.remaining > 0 of
        true ->
            PS1 = PS0#peer_state{
                remaining = PS0#peer_state.remaining - 1
            },
            {reply, accepted, State#{peers := Peers#{Peer => PS1}}};
        false ->
            enqueue_caller(Peer, From, PS0, State)
    end;
handle_call({status, Peer}, _From, #{spec := Spec, peers := Peers} = State) ->
    Reply = case maps:find(Peer, Peers) of
                {ok, PS} ->
                    {ok, peer_state_to_map(PS)};
                error ->
                    {ok, #{
                        total          => maps:get(initial_remaining, Spec),
                        remaining      => maps:get(initial_remaining, Spec),
                        reset_seconds  => 0,
                        queue_length   => 0,
                        last_update_ts => undefined
                    }}
            end,
    {reply, Reply, State};
handle_call(reset, _From, #{peers := Peers, monitors := Monitors} = State) ->
    maps:fold(fun(_Peer, PS, _) ->
                      cancel_reset_timer(PS#peer_state.reset_timer),
                      drain_for_reset(PS#peer_state.waiters)
              end, ok, Peers),
    maps:fold(fun(MRef, _Peer, _) ->
                      erlang:demonitor(MRef, [flush])
              end, ok, Monitors),
    {reply, ok, State#{peers := #{}, monitors := #{}}};
handle_call(Msg, From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE},
                  {msg, Msg}, {from, From}]),
    {reply, {error, unsupported}, State}.

handle_cast({update_quota, Peer, Total, NewRemaining, ResetSeconds, ReceivedAt},
            #{spec := Spec, peers := Peers, monitors := Monitors} = State) ->
    PS0 = get_or_init_peer(Peer, Peers, Spec),
    InitialRemaining = maps:get(initial_remaining, Spec),
    NewTotal = case Total =/= InitialRemaining of
                   true  -> Total;
                   false -> PS0#peer_state.total
               end,
    UpdatedRemaining =
        case is_significantly_different(Total, NewRemaining, Peer, State) of
            true ->
                Window = maps:get(concurrency_window_ms, Spec),
                merge_remaining(PS0#peer_state.remaining,
                                PS0#peer_state.last_update_ts,
                                NewRemaining, ReceivedAt, Window);
            false ->
                PS0#peer_state.remaining
        end,
    PS1 = PS0#peer_state{
        total = NewTotal,
        remaining = UpdatedRemaining,
        reset_seconds = ResetSeconds,
        last_update_ts = ReceivedAt
    },
    {PS2, Monitors1} = drain_waiters(PS1, Monitors),
    PS3 = arm_or_clear_reset_timer(Peer, PS2, ResetSeconds),
    {noreply, State#{
        peers := Peers#{Peer => PS3},
        monitors := Monitors1
    }};
handle_cast({cancel_request, Peer, Ref}, #{peers := Peers, monitors := Monitors} = State) ->
    case maps:find(Peer, Peers) of
        {ok, PS0} ->
            {Q1, Monitors1} = drop_waiter_by_ref(Ref,
                                                 PS0#peer_state.waiters,
                                                 Monitors),
            PS1 = PS0#peer_state{waiters = Q1},
            {noreply, State#{
                peers := Peers#{Peer => PS1},
                monitors := Monitors1
            }};
        error ->
            {noreply, State}
    end;
handle_cast(Msg, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE},
                  {msg, Msg}]),
    {noreply, State}.

handle_info({reset_quota, Peer, Tag}, #{peers := Peers, monitors := Monitors} = State) ->
    case maps:find(Peer, Peers) of
        {ok, #peer_state{reset_timer = {_TRef, Tag}} = PS0} ->
            PS1 = PS0#peer_state{
                remaining = PS0#peer_state.total,
                reset_seconds = 0,
                reset_timer = undefined
            },
            {PS2, Monitors1} = drain_waiters(PS1, Monitors),
            {noreply, State#{
                peers := Peers#{Peer => PS2},
                monitors := Monitors1
            }};
        _ ->
            %% Stale timer (cancelled or already replaced).
            {noreply, State}
    end;
handle_info({'DOWN', MRef, process, _Pid, _Reason}, #{peers := Peers, monitors := Monitors} = State) ->
    case maps:take(MRef, Monitors) of
        {Peer, Monitors1} ->
            PS0 = maps:get(Peer, Peers),
            PS1 = PS0#peer_state{
                waiters = drop_waiter_by_mref(MRef,
                                              PS0#peer_state.waiters)
            },
            {noreply, State#{
                peers := Peers#{Peer => PS1},
                monitors := Monitors1
            }};
        error ->
            {noreply, State}
    end;
handle_info(Info, State) ->
    ?LOG_DEBUG([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internals

registered_name(Id) when is_atom(Id) ->
    list_to_atom("arweave_client_throttling_group_" ++ atom_to_list(Id)).

enqueue_caller(Peer, From, PS0, State) ->
    #{spec := Spec, peers := Peers, monitors := Monitors} = State,
    MaxLen = maps:get(max_queue_length, Spec),
    case queue:len(PS0#peer_state.waiters) >= MaxLen of
        true ->
            {reply, {error, queue_full}, State};
        false ->
            {FromPid, _Tag} = From,
            MRef = erlang:monitor(process, FromPid),
            Ref = make_ref(),
            PS1 = PS0#peer_state{
                waiters = queue:in({Ref, FromPid, MRef},
                                   PS0#peer_state.waiters)
            },
            {reply, {queued, Ref}, State#{
                peers := Peers#{Peer => PS1},
                monitors := Monitors#{MRef => Peer}
            }}
    end.

get_or_init_peer(Peer, Peers, Spec) ->
    case maps:find(Peer, Peers) of
        {ok, PS} -> PS;
        error ->
            Initial = maps:get(initial_remaining, Spec),
            #peer_state{
                total = Initial,
                remaining = Initial,
                reset_seconds = 0,
                reset_timer = undefined,
                waiters = queue:new(),
                last_update_ts = undefined
            }
    end.

%% Merge an incoming `remaining' value with the current one. See the
%% module docstring for the rationale.
merge_remaining(_Old, undefined, New, _Now, _Window) ->
    New;
merge_remaining(Old, LastTs, New, Now, Window) when Now - LastTs =< Window ->
    min(Old, New);
merge_remaining(_Old, _LastTs, New, _Now, _Window) ->
    New.

%% Decide whether a freshly-reported `remaining' is worth committing
%% to the peer state. An update is significant when the remote
%% reports an exhausted quota (`NewRemaining =:= 0') or when the
%% absolute change relative to what we have stored exceeds
%% `?SIGNIFICANTLY_DIFFERENT_RATIO' of the spec's `initial_remaining'.
%% Insignificant updates are dropped so we don't churn the state on
%% every response.
is_significantly_different(_Total, 0, _Peer, _State) ->
    true;
is_significantly_different(_Total, NewRemaining, Peer, State) ->
    #{spec := Spec, peers := Peers} = State,
    PS = get_or_init_peer(Peer, Peers, Spec),
    InitialRemaining = maps:get(initial_remaining, Spec),
    Threshold = ?SIGNIFICANTLY_DIFFERENT_RATIO * InitialRemaining,
    abs(PS#peer_state.remaining - NewRemaining) > Threshold.

drain_waiters(#peer_state{remaining = 0} = PS, Monitors) ->
    {PS, Monitors};
drain_waiters(#peer_state{remaining = R, waiters = Q} = PS, Monitors)
  when R > 0 ->
    case queue:out(Q) of
        {empty, _} ->
            {PS, Monitors};
        {{value, {Ref, Pid, MRef}}, Q1} ->
            erlang:demonitor(MRef, [flush]),
            Pid ! {request_ready, Ref},
            PS1 = PS#peer_state{
                remaining = R - 1,
                waiters = Q1
            },
            drain_waiters(PS1, maps:remove(MRef, Monitors))
    end.

%% Arm a refresh timer when remaining is 0 and we have a positive
%% reset_seconds. Cancel any previous timer; do nothing (and cancel)
%% if no timer is needed.
arm_or_clear_reset_timer(_Peer,
                         #peer_state{remaining = R,
                                     reset_timer = OldTimer} = PS,
                         _ResetSeconds) when R > 0 ->
    cancel_reset_timer(OldTimer),
    PS#peer_state{reset_timer = undefined};
arm_or_clear_reset_timer(_Peer,
                         #peer_state{reset_timer = OldTimer} = PS, 0) ->
    cancel_reset_timer(OldTimer),
    PS#peer_state{reset_timer = undefined};
arm_or_clear_reset_timer(Peer,
                         #peer_state{reset_timer = OldTimer} = PS,
                         ResetSeconds) when ResetSeconds > 0 ->
    cancel_reset_timer(OldTimer),
    Tag = make_ref(),
    TRef = erlang:send_after(ResetSeconds * 1000, self(),
                             {reset_quota, Peer, Tag}),
    PS#peer_state{reset_timer = {TRef, Tag}}.

cancel_reset_timer(undefined) ->
    ok;
cancel_reset_timer({TRef, _Tag}) ->
    _ = erlang:cancel_timer(TRef),
    ok.

drop_waiter_by_mref(MRef, Q) ->
    queue:filter(fun({_Ref, _Pid, M}) -> M =/= MRef end, Q).

drop_waiter_by_ref(Ref, Q, Monitors) ->
    L0 = queue:to_list(Q),
    case lists:keytake(Ref, 1, L0) of
        {value, {Ref, _Pid, MRef}, L1} ->
            erlang:demonitor(MRef, [flush]),
            {queue:from_list(L1), maps:remove(MRef, Monitors)};
        false ->
            {Q, Monitors}
    end.

drain_for_reset(Q) ->
    case queue:out(Q) of
        {empty, _} -> ok;
        {{value, {Ref, Pid, MRef}}, Q1} ->
            erlang:demonitor(MRef, [flush]),
            Pid ! {request_ready, Ref},
            drain_for_reset(Q1)
    end.

peer_state_to_map(#peer_state{
        total = Total,
        remaining = Remaining,
        reset_seconds = ResetSeconds,
        waiters = Waiters,
        last_update_ts = LastTs}) ->
    #{
        total          => Total,
        remaining      => Remaining,
        reset_seconds  => ResetSeconds,
        queue_length   => queue:len(Waiters),
        last_update_ts => LastTs
    }.

monotonic_ms() ->
    erlang:monotonic_time(millisecond).
