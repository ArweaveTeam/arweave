%%%===================================================================
%%% @doc Per-group client throttler.
%%%
%%% One `arweave_throttling_group' process exists per limiting
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
-module(arweave_throttling_group).
-vsn(1).
-behaviour(gen_server).

-export([
	start_link/1,
	registered_name/1,
	throttle/2,
	is_throttled/2,
	info/1,
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

-ifdef(AR_TEST).
-export([turn_off/1, turn_on/1]).
-else.
-compile({nowarn_unused_function, [{turn_off, 1}, {turn_on, 1}]}).
-endif.

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

-define(CALL_TIMEOUT, 1000).
%% How long throttle/2 waits for a `{request_ready, Ref}' message
%% after the gen_server replies with `{queued, Ref}'. On expiry the
%% caller sends a `cancel_request' cast so the entry can be evicted
%% from the queue and returns `{error, timeout}'.
-define(THROTTLE_RECEIVE_TIMEOUT_MS, 60000).

%% NOTE: this threshold doesn't reflect when the peer is actually throttled,
%%       just a threshold to consider approaching throttling limit.
%%       It is implemented to replicate previous behaviour, and might/should
%%       be tweaked later.
-define(IS_THROTTLED_THRESHOLD, 0.8).

-define(DEFAULT_INITIAL_REMAINING, infinity).
-define(MAX_QUEUE_LENGTH, 5000).
-define(CONCURRENCY_WINDOW_MS, 80).

%% @doc Start a group process.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(#{id := ID} = Spec) ->
	gen_server:start_link({local, registered_name(ID)}, ?MODULE, Spec, []).

registered_name(ID) when is_atom(ID) ->
	list_to_atom("arweave_throttling_group_" ++ atom_to_list(ID)).

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
throttle(GroupID, Peer) ->
	{Time, Value} = timer:tc(fun do_throttle/2, [GroupID, Peer]),
	prometheus_histogram:observe(arweave_throttling_request_response_time_microseconds,
					[atom_to_list(GroupID)], Time),
	Value.

-spec do_throttle(atom(), tuple()) -> ok | {error, term()}.
do_throttle(GroupID, Peer) ->
	prometheus_counter:inc(arweave_throttling_requests_total, [atom_to_list(GroupID)]),
	Name = registered_name(GroupID),
	{Time, WorkerReturn} = timer:tc(fun try_throttle_call/2, [Name, Peer]),
	prometheus_histogram:observe(arweave_throttling_worker_response_time_microseconds,
					[atom_to_list(GroupID)], Time),
	case WorkerReturn of
		accepted ->
			ok;
		{queued, Ref} ->
			prometheus_counter:inc(arweave_throttling_queued_total, [atom_to_list(GroupID)]),
			receive
				{request_ready, Ref} ->
					ok
			after ?THROTTLE_RECEIVE_TIMEOUT_MS ->
					gen_server:cast(Name, {cancel_request, Peer, Ref}),
					prometheus_counter:inc(arweave_throttling_requests_error,
								[atom_to_list(GroupID), "throttle_receive_timeout"]),
				{error, throttle_receive_timeout}
			end;
		{error, Reason} = Error ->
			%% TODO: extract error reason
			?LOG_ERROR([{event, client_throttling_throttle_error}, {reason, Reason}]),
			prometheus_counter:inc(arweave_throttling_requests_error,
						[atom_to_list(GroupID), "unknown"]),
			Error
	end.

try_throttle_call(Name, Peer) ->
	try
		gen_server:call(Name, {throttle, Peer}, ?CALL_TIMEOUT)
	catch
		E:R:Stack ->
			{error, {E,R,Stack}}
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
update_quota(GroupID, Peer, #{
		total := Total,
		remaining := Remaining,
		reset_seconds := ResetSeconds})
	  when is_integer(Total), Total >= 0,
		is_integer(Remaining), Remaining >= 0,
		is_integer(ResetSeconds), ResetSeconds >= 0 ->
		prometheus_counter:inc(arweave_throttling_quota_update_requests,
				[atom_to_list(GroupID)]),
	ReceivedAt = monotonic_ms(),
	gen_server:cast(registered_name(GroupID),
					{update_quota, Peer, Total, Remaining,
						ResetSeconds, ReceivedAt}).

%% @doc Return true if Peer is being throttled for the given path
-spec is_throttled(atom(), tuple()) -> boolean().
is_throttled(GroupID, Peer) when is_atom(GroupID), is_tuple(Peer) ->
	{Time, Value} = timer:tc(fun do_is_throttled/2, [GroupID, Peer]),
	prometheus_histogram:observe(arweave_throttling_is_throttled_response_time_microseconds,
								[atom_to_list(GroupID)], Time),
	Value.

-spec do_is_throttled(atom(), tuple()) -> boolean().
do_is_throttled(GroupID, Peer) when is_atom(GroupID), is_tuple(Peer) ->
	try
		{ok, IsThrottled} = gen_server:call(registered_name(GroupID), {is_throttled, Peer}, ?CALL_TIMEOUT),
		IsThrottled
	catch
		{'EXIT', {noproc, {gen_server, call, _}}} -> false;
		{'EXIT', Reason} -> exit(Reason);
		E:R:Stack ->
			?LOG_ERROR([{event, client_throttling_is_throttled_error},
						{class, E},
						{reason, R},
						{stacktrace, Stack}]),
			%% previous solution
			false
	end.

%% @doc Get all info
info(GroupID) ->
	gen_server:call(registered_name(GroupID), get_info).

%% @doc Return a snapshot of the per-peer state.
-spec status(atom(), tuple()) -> {ok, map()} | {error, term()}.
status(GroupID, Peer) ->
	gen_server:call(registered_name(GroupID), {status, Peer}).

%% @doc Number of waiting callers currently queued for `Peer'.
-spec pending(atom(), tuple()) -> non_neg_integer().
pending(GroupID, Peer) ->
	case status(GroupID, Peer) of
		{ok, #{queue_length := N}} -> N;
		_ -> 0
	end.

%% @doc Drop all per-peer state. Pending waiters receive a
%% `{request_ready, Ref}' notification so their `throttle/2' returns
%% `ok' rather than staying blocked.
-spec reset(atom()) -> ok.
reset(GroupID) ->
	gen_server:call(registered_name(GroupID), reset).


-spec turn_off(atom()) -> ok.
turn_off(WorkerRef) ->
	gen_server:call(WorkerRef, turn_off).

-spec turn_on(atom()) -> ok.
turn_on(WorkerRef) ->
	gen_server:call(WorkerRef, turn_on).


%% @doc Stop the group process.
-spec stop(atom()) -> ok.
stop(GroupID) ->
	gen_server:stop(registered_name(GroupID)).

%% gen_server callbacks
init(#{id := GroupID}) ->
	process_flag(trap_exit, true),
	{ok, #{
		id => GroupID,
		is_enabled => true,
		peers => #{},
		monitors => #{}
		}}.

handle_call({throttle, _Peer}, _From, #{is_enabled := false} = State) ->
	{reply, accepted, State};
handle_call({throttle, Peer}, From, #{peers := Peers} = State) ->
	PS0 = get_or_init_peer(Peer, Peers),
	case PS0#peer_state.remaining of
		infinity ->
			{reply, accepted, State};
		Remaining when is_integer(Remaining) andalso Remaining > 0 ->
			PS1 = PS0#peer_state{
				remaining = PS0#peer_state.remaining - 1
			},
			{reply, accepted, State#{peers := Peers#{Peer => PS1}}};
		_ ->
			enqueue_caller(Peer, From, PS0, State)
	end;
handle_call({is_throttled, Peer}, _From, #{peers := Peers} = State) ->
	PS0 = get_or_init_peer(Peer, Peers),
	IsThrottled = PS0#peer_state.remaining / PS0#peer_state.total > ?IS_THROTTLED_THRESHOLD,
	{reply, {ok, IsThrottled}, State};
handle_call(get_info, _From, #{peers := Peers} = State) ->
	NumOfRequestsQueued =
		maps:fold(fun(_Peer, #peer_state{waiters = Waiters}, Acc) ->
						queue:len(Waiters) + Acc
				end, 0, Peers),
	Reply = #{peers => map_size(Peers),
			queued => NumOfRequestsQueued},
	{reply, Reply, State};
handle_call({status, Peer}, _From, #{peers := Peers} = State) ->
	PS = get_or_init_peer(Peer, Peers),
	Reply = {ok, peer_state_to_map(PS)},
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
handle_call(turn_off, _From, State) ->
	{reply, ok, State#{is_enabled => false}};
handle_call(turn_on, _From, State) ->
	{reply, ok, State#{is_enabled => true}};
handle_call(Msg, From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE},
				{msg, Msg}, {from, From}]),
	{reply, {error, unsupported}, State}.

handle_cast({update_quota, Peer, Total, NewRemaining, ResetSeconds, ReceivedAt},
			#{peers := Peers, monitors := Monitors} = State) ->
	PS0 = get_or_init_peer(Peer, Peers),

	UpdatedRemaining =
		%% If total changed update remaining as well.
		case Total =/= PS0#peer_state.total of
			true ->
				%% Log a warning, this might be an issue.
				?LOG_WARNING([{event, arweave_throttling_group_quota_updated},
						{peer, Peer},
						{previous, PS0#peer_state.total},
						{new, Total},
						{received_at, ReceivedAt}]),
				NewRemaining;
			false ->
				merge_remaining(PS0#peer_state.remaining,
						PS0#peer_state.last_update_ts,
						NewRemaining,
						ReceivedAt,
						?CONCURRENCY_WINDOW_MS)
		end,

	PS1 = PS0#peer_state{
		total = Total,
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
				waiters = drop_waiter_by_mref(MRef,PS0#peer_state.waiters)
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
enqueue_caller(Peer, From, PS0, #{peers := Peers, monitors := Monitors} = State) ->
	case queue:len(PS0#peer_state.waiters) >= ?MAX_QUEUE_LENGTH of
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

get_or_init_peer(Peer, Peers) ->
	case maps:find(Peer, Peers) of
		{ok, PS} -> PS;
		error ->
			#peer_state{
				total = ?DEFAULT_INITIAL_REMAINING,
				remaining = ?DEFAULT_INITIAL_REMAINING,
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
