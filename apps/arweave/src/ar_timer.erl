%%%===================================================================
%%% This Source Code Form is subject to the terms of the GNU General
%%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%%% with this file, You can obtain one at
%%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%%
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
%%% @end
%%%===================================================================
-module(ar_timer).
-export([
	apply_after/4,
	apply_interval/4,
	cancel/1,
	insert_timer/2,
	list_timers/0,
	terminate_timers/0,
	send_after/2,
	send_after/3,
	send_interval/2,
	send_interval/3
]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc wrapper around timer:apply_after/4.
%% @see timer:apply_after/4
%% @end
%%--------------------------------------------------------------------
apply_after(Time, Module, Function, Arguments) ->
	M = timer,
	F = apply_after,
	A = [Time, Module, Function, Arguments],
	case ar_shutdown_manager:apply(M, F, A) of
		{ok, TimerRef} -> {ok, TimerRef};
		Elsewise -> Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:apply_interval/4.
%% @see timer:apply_interval/4
%% @end
%%--------------------------------------------------------------------
apply_interval(Time, Module, Function, Arguments) ->
	M = timer,
	F = apply_interval,
	A = [Time, Module, Function, Arguments],
	case ar_shutdown_manager:apply(M, F, A) of
		{ok, TimerRef} ->
			insert_timer(TimerRef, #{
				pid => self(),
				module => Module,
				function => Function,
				arguments => Arguments,
				time => Time
			}),
			{ok, TimerRef};
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_after/4.
%% @see send_after/3
%% @end
%%--------------------------------------------------------------------
send_after(Time, Message) ->
	send_after(Time, self(), Message).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_after/3.
%% @see timer:send_after/3
%% @end
%%--------------------------------------------------------------------
send_after(Time, Pid, Message) ->
	M = timer,
	F = send_after,
	A = [Time, Pid, Message],
	case ar_shutdown_manager:apply(M, F, A) of
		{ok, TimerRef} -> {ok, TimerRef};
		Elsewise -> Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:send_interval/2.
%% @see send_interval/3
%% @end
%%--------------------------------------------------------------------
send_interval(Time, Message) ->
	send_interval(Time, self(), Message).

%%--------------------------------------------------------------------
%% @doc wrapper around timer:interval/3.
%% @see timer:send_interval/3
%% @end
%%--------------------------------------------------------------------
send_interval(Time, Pid, Message) ->
	M = timer,
	F = send_interval,
	A = [Time, Pid, Message],
	case ar_shutdown_manager:apply(M, F, A) of
		{ok, TimerRef} ->
			insert_timer(TimerRef, #{
				pid => self(),
				time => Time
			}),
			{ok, TimerRef};
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc wrapper around timer:cancel/1.
%% @see timer:cancel/1
%% @end
%%--------------------------------------------------------------------
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
		Elsewise ->
			Elsewise
	end.

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
%% from `timer_tab' ETS table and cancel all of them.
%% @end
%%--------------------------------------------------------------------
terminate_timers() ->
	% cancel all intervals first
	[ cancel(Ref) || Ref <- list_timers() ],

	% then cancel all others timers from timer_tab.
	case ets:whereis(timer_tab) of
		undefined ->
			ok;
		_ ->
			[ timer:cancel(Ref) || {Ref, _, _} <- ets:tab2list(timer_tab) ]
	end.
