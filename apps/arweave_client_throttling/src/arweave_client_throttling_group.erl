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
%%% group. It maintains, per peer, a "remaining budget" integer and a
%%% FIFO queue of waiting caller pids.
%%%
%%% Behaviour summary:
%%%
%%% <ul>
%%%   <li>`throttle/2' is a synchronous `gen_server:call' with timeout
%%%       `infinity'. If the peer's current `remaining' is positive,
%%%       the call returns `ok' immediately and the budget is
%%%       decremented. Otherwise the caller's `From' reference is
%%%       enqueued and the gen_server does not reply, blocking the
%%%       caller until budget becomes available.</li>
%%%   <li>`update_remaining/3' is a `gen_server:cast'. It refreshes
%%%       the budget from a value reported by a remote host, drains as
%%%       many waiters as the new budget allows, and never blocks the
%%%       caller.</li>
%%% </ul>
%%%
%%% == Handling concurrent `update_remaining' messages ==
%%%
%%% Several in-flight requests can complete around the same time and
%%% each report a different `remaining' value. Without timestamps from
%%% the remote, the safest assumption is that the smallest of two
%%% values reported close together is the most recent (each subsequent
%%% request consumed one slot on the server side). We therefore take
%%% the minimum of the old and the new value as long as they fall
%%% within `concurrency_window_ms'. Outside that window we trust the
%%% new value, which lets the budget grow back when the remote rate
%%% window resets.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_group).
-vsn(1).
-behaviour(gen_server).

-export([
	start_link/1,
	throttle/2,
	update_remaining/3,
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

-record(peer_state, {
	remaining       :: non_neg_integer(),
	waiters         :: queue:queue({gen_server:from(), reference()}),
	last_update_ts  :: integer() | undefined
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Start a group process. `Spec' must be a normalized map (see
%% `arweave_client_throttling_config:normalize_group/1').
%% @end
%%--------------------------------------------------------------------
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(#{id := Id} = Spec) ->
	gen_server:start_link({local, registered_name(Id)}, ?MODULE, Spec, []).

%%--------------------------------------------------------------------
%% @doc Blocking throttle call. Returns `ok' once the caller is
%% allowed to proceed, or `{error, queue_full}' if the per-peer queue
%% is already saturated.
%% @end
%%--------------------------------------------------------------------
-spec throttle(atom(), tuple()) -> ok | {error, term()}.
throttle(GroupId, Peer) ->
	gen_server:call(registered_name(GroupId), {throttle, Peer}, infinity).

%%--------------------------------------------------------------------
%% @doc Non-blocking budget refresh. `Remaining' is the budget the
%% remote host most recently advertised for `Peer'.
%% @end
%%--------------------------------------------------------------------
-spec update_remaining(atom(), tuple(), non_neg_integer()) -> ok.
update_remaining(GroupId, Peer, Remaining)
		when is_integer(Remaining), Remaining >= 0 ->
	ReceivedAt = monotonic_ms(),
	gen_server:cast(registered_name(GroupId),
		{update_remaining, Peer, Remaining, ReceivedAt}).

%%--------------------------------------------------------------------
%% @doc Return a snapshot of the per-peer state. Synchronous but
%% non-mutating, useful for introspection and tests.
%% @end
%%--------------------------------------------------------------------
-spec status(atom(), tuple()) -> {ok, map()} | {error, term()}.
status(GroupId, Peer) ->
	gen_server:call(registered_name(GroupId), {status, Peer}).

%%--------------------------------------------------------------------
%% @doc Number of waiting callers currently queued for `Peer'.
%% @end
%%--------------------------------------------------------------------
-spec pending(atom(), tuple()) -> non_neg_integer().
pending(GroupId, Peer) ->
	case status(GroupId, Peer) of
		{ok, #{queue_length := N}} -> N;
		_ -> 0
	end.

%%--------------------------------------------------------------------
%% @doc Drop all per-peer state. Pending waiters receive `ok' so they
%% are not left blocked forever.
%% @end
%%--------------------------------------------------------------------
-spec reset(atom()) -> ok.
reset(GroupId) ->
	gen_server:call(registered_name(GroupId), reset).

%%--------------------------------------------------------------------
%% @doc Stop the group process.
%% @end
%%--------------------------------------------------------------------
-spec stop(atom()) -> ok.
stop(GroupId) ->
	gen_server:stop(registered_name(GroupId)).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init(Spec) ->
	process_flag(trap_exit, true),
	{ok, #{
		spec => Spec,
		peers => #{},
		monitors => #{}
	}}.

handle_call({throttle, Peer}, From, State) ->
	#{spec := Spec, peers := Peers} = State,
	PS0 = get_or_init_peer(Peer, Peers, Spec),
	case PS0#peer_state.remaining > 0 of
		true ->
			PS1 = PS0#peer_state{
				remaining = PS0#peer_state.remaining - 1
			},
			{reply, ok, State#{peers := Peers#{Peer => PS1}}};
		false ->
			enqueue_caller(Peer, From, PS0, State)
	end;

handle_call({status, Peer}, _From, State) ->
	#{spec := Spec, peers := Peers} = State,
	Reply = case maps:find(Peer, Peers) of
		{ok, PS} ->
			{ok, #{
				remaining      => PS#peer_state.remaining,
				queue_length   => queue:len(PS#peer_state.waiters),
				last_update_ts => PS#peer_state.last_update_ts
			}};
		error ->
			{ok, #{
				remaining      => maps:get(initial_remaining, Spec),
				queue_length   => 0,
				last_update_ts => undefined
			}}
	end,
	{reply, Reply, State};

handle_call(reset, _From, State) ->
	#{peers := Peers, monitors := Monitors} = State,
	maps:fold(fun(_Peer, #peer_state{waiters = Q}, _) ->
		drain_for_reset(Q)
	end, ok, Peers),
	maps:fold(fun(MRef, _Peer, _) ->
		erlang:demonitor(MRef, [flush])
	end, ok, Monitors),
	{reply, ok, State#{peers := #{}, monitors := #{}}};

handle_call(Msg, From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE},
		{msg, Msg}, {from, From}]),
	{reply, {error, unsupported}, State}.

handle_cast({update_remaining, Peer, NewRemaining, ReceivedAt}, State) ->
	#{spec := Spec, peers := Peers, monitors := Monitors} = State,
	PS0 = get_or_init_peer(Peer, Peers, Spec),
	Window = maps:get(concurrency_window_ms, Spec),
	Merged = merge_remaining(PS0#peer_state.remaining,
		PS0#peer_state.last_update_ts, NewRemaining, ReceivedAt, Window),
	PS1 = PS0#peer_state{
		remaining = Merged,
		last_update_ts = ReceivedAt
	},
	{PS2, Monitors1} = drain_waiters(PS1, Monitors),
	{noreply, State#{
		peers := Peers#{Peer => PS2},
		monitors := Monitors1
	}};

handle_cast(Msg, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE},
		{msg, Msg}]),
	{noreply, State}.

handle_info({'DOWN', MRef, process, _Pid, _Reason}, State) ->
	#{peers := Peers, monitors := Monitors} = State,
	case maps:take(MRef, Monitors) of
		{Peer, Monitors1} ->
			PS0 = maps:get(Peer, Peers),
			PS1 = PS0#peer_state{
				waiters = drop_waiter(MRef, PS0#peer_state.waiters)
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

%%--------------------------------------------------------------------
%% Internals
%%--------------------------------------------------------------------

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
			PS1 = PS0#peer_state{
				waiters = queue:in({From, MRef}, PS0#peer_state.waiters)
			},
			{noreply, State#{
				peers := Peers#{Peer => PS1},
				monitors := Monitors#{MRef => Peer}
			}}
	end.

get_or_init_peer(Peer, Peers, Spec) ->
	case maps:find(Peer, Peers) of
		{ok, PS} -> PS;
		error ->
			#peer_state{
				remaining = maps:get(initial_remaining, Spec),
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
		{{value, {From, MRef}}, Q1} ->
			erlang:demonitor(MRef, [flush]),
			gen_server:reply(From, ok),
			PS1 = PS#peer_state{
				remaining = R - 1,
				waiters = Q1
			},
			drain_waiters(PS1, maps:remove(MRef, Monitors))
	end.

drop_waiter(MRef, Q) ->
	queue:filter(fun({_From, M}) -> M =/= MRef end, Q).

drain_for_reset(Q) ->
	case queue:out(Q) of
		{empty, _} -> ok;
		{{value, {From, MRef}}, Q1} ->
			erlang:demonitor(MRef, [flush]),
			gen_server:reply(From, ok),
			drain_for_reset(Q1)
	end.

monotonic_ms() ->
	erlang:monotonic_time(millisecond).
