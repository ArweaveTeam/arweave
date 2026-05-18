%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Top-level interface tests for `arweave_client_throttling'.
%%% @end
%%%===================================================================
-module(arweave_client_throttling_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
	default_groups_started/1,
	throttle_and_update_remaining/1,
	blocking_call_is_released_by_update/1,
	fifo_ordering/1,
	concurrent_remaining_updates_take_min/1,
	stale_update_outside_window_overrides/1,
	queue_full_returns_error/1,
	dead_caller_is_dropped_from_queue/1,
	reset_releases_waiters/1,
	peer_4_and_5_tuple_keys/1
]).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Common Test boilerplate
%%--------------------------------------------------------------------
suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
	{description, "arweave_client_throttling top-level API"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	%% Force a tight concurrency window so the corresponding test cases
	%% finish quickly. The other defaults remain unchanged.
	application:set_env(arweave_client_throttling, groups, [
		#{id => general,
		  initial_remaining => 2,
		  max_queue_length => 4,
		  concurrency_window_ms => 50},
		#{id => data_sync_record,
		  initial_remaining => 1,
		  max_queue_length => 2,
		  concurrency_window_ms => 50}
	]),
	ct:pal(info, 1, "start arweave_client_throttling"),
	ok = arweave_client_throttling:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ct:pal(info, 1, "stop arweave_client_throttling"),
	ok = arweave_client_throttling:stop(),
	application:unset_env(arweave_client_throttling, groups),
	ok.

all() ->
	[
		default_groups_started,
		throttle_and_update_remaining,
		blocking_call_is_released_by_update,
		fifo_ordering,
		concurrent_remaining_updates_take_min,
		stale_update_outside_window_overrides,
		queue_full_returns_error,
		dead_caller_is_dropped_from_queue,
		reset_releases_waiters,
		peer_4_and_5_tuple_keys
	].

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc The supervisor must start one worker per group spec, and the
%% public `groups/0' accessor must return them.
%% @end
%%--------------------------------------------------------------------
default_groups_started(_Config) ->
	ct:pal(test, 1, "check supervisor is alive"),
	true = is_pid(whereis(arweave_client_throttling_sup)),

	ct:pal(test, 1, "list configured groups"),
	Groups = arweave_client_throttling:groups(),
	true = lists:member(general, Groups),
	true = lists:member(data_sync_record, Groups),

	ct:pal(test, 1, "each group worker is registered"),
	true = is_pid(whereis(arweave_client_throttling_group_general)),
	true = is_pid(whereis(arweave_client_throttling_group_data_sync_record)),
	ok.

%%--------------------------------------------------------------------
%% @doc Calls succeed up to `initial_remaining', then `update_remaining'
%% refreshes the budget and lets more requests through.
%% @end
%%--------------------------------------------------------------------
throttle_and_update_remaining(_Config) ->
	Peer = {127, 0, 0, 1, 1984},

	ct:pal(test, 1, "initial budget is consumed in order"),
	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),
	{ok, #{remaining := 0}} =
		arweave_client_throttling:status(general, Peer),

	ct:pal(test, 1, "refresh budget non-blockingly"),
	ok = arweave_client_throttling:update_remaining(general, Peer, 3),
	ok = wait_until(fun() ->
		{ok, #{remaining := 3}} =:=
			arweave_client_throttling:status(general, Peer)
	end),

	ct:pal(test, 1, "the new budget is honored"),
	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),
	{ok, #{remaining := 0}} =
		arweave_client_throttling:status(general, Peer),
	ok.

%%--------------------------------------------------------------------
%% @doc A caller that hits an empty budget blocks until an
%% `update_remaining' announces capacity.
%% @end
%%--------------------------------------------------------------------
blocking_call_is_released_by_update(_Config) ->
	Peer = {10, 0, 0, 1, 1984},

	ct:pal(test, 1, "drain the initial budget"),
	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),

	ct:pal(test, 1, "next caller must block"),
	Parent = self(),
	Pid = spawn_link(fun() ->
		Reply = arweave_client_throttling:throttle(general, Peer),
		Parent ! {done, self(), Reply}
	end),

	ct:pal(test, 1, "ensure caller is queued and still blocked"),
	ok = wait_until(fun() ->
		{ok, #{queue_length := 1}} =:=
			arweave_client_throttling:status(general, Peer)
	end),
	false = receive {done, Pid, _} -> true after 100 -> false end,

	ct:pal(test, 1, "an update_remaining releases the waiter"),
	ok = arweave_client_throttling:update_remaining(general, Peer, 1),

	receive
		{done, Pid, ok} -> ok
	after 1000 ->
		ct:fail("blocked caller was not released")
	end,
	ok.

%%--------------------------------------------------------------------
%% @doc Multiple blocked callers must be released in FIFO order.
%% @end
%%--------------------------------------------------------------------
fifo_ordering(_Config) ->
	Peer = {172, 16, 0, 1, 1984},
	Parent = self(),

	ct:pal(test, 1, "drain initial budget"),
	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),

	ct:pal(test, 1, "queue three callers serially to fix the FIFO order"),
	_Pids = lists:map(fun(N) ->
		Pid = spawn(fun() ->
			ok = arweave_client_throttling:throttle(general, Peer),
			Parent ! {released, N, self()}
		end),
		ok = wait_until(fun() ->
			{ok, #{queue_length := QL}} =
				arweave_client_throttling:status(general, Peer),
			QL =:= N
		end),
		Pid
	end, [1, 2, 3]),

	ct:pal(test, 1, "release them one at a time, spacing past the window"),
	%% concurrency_window_ms is 50 in init_per_testcase, so 80ms ensures
	%% each update_remaining is treated as a fresh observation rather
	%% than merged with the previous one.
	Order = lists:map(fun(_) ->
		timer:sleep(80),
		ok = arweave_client_throttling:update_remaining(general, Peer, 1),
		receive {released, N, _} -> N after 1000 ->
			ct:fail("a waiter was never released")
		end
	end, [1, 2, 3]),

	[1, 2, 3] = Order,
	ok.

%%--------------------------------------------------------------------
%% @doc Two concurrent `update_remaining' values within the
%% concurrency window must collapse to the minimum.
%% @end
%%--------------------------------------------------------------------
concurrent_remaining_updates_take_min(_Config) ->
	Peer = {192, 168, 1, 1, 1984},

	ok = arweave_client_throttling:update_remaining(general, Peer, 10),
	ok = arweave_client_throttling:update_remaining(general, Peer, 3),
	ok = arweave_client_throttling:update_remaining(general, Peer, 7),

	ok = wait_until(fun() ->
		case arweave_client_throttling:status(general, Peer) of
			{ok, #{remaining := R, last_update_ts := T}}
					when T =/= undefined ->
				R =:= 3;
			_ ->
				false
		end
	end),
	ok.

%%--------------------------------------------------------------------
%% @doc An update arriving outside `concurrency_window_ms' should
%% replace the previous value, even if it is larger (so budgets can
%% grow back after a remote reset).
%% @end
%%--------------------------------------------------------------------
stale_update_outside_window_overrides(_Config) ->
	Peer = {192, 168, 1, 2, 1984},

	ok = arweave_client_throttling:update_remaining(general, Peer, 2),
	ok = wait_until(fun() ->
		{ok, #{remaining := 2}} =:=
			arweave_client_throttling:status(general, Peer)
	end),

	ct:pal(test, 1, "sleep past the concurrency window"),
	timer:sleep(150),

	ok = arweave_client_throttling:update_remaining(general, Peer, 9),
	ok = wait_until(fun() ->
		{ok, #{remaining := 9}} =:=
			arweave_client_throttling:status(general, Peer)
	end),
	ok.

%%--------------------------------------------------------------------
%% @doc Once the queue cap is reached, further `throttle/2' calls
%% return `{error, queue_full}' instead of blocking forever.
%% @end
%%--------------------------------------------------------------------
queue_full_returns_error(_Config) ->
	Peer = {10, 1, 1, 1, 1984},
	Parent = self(),

	ct:pal(test, 1, "drain initial budget"),
	ok = arweave_client_throttling:throttle(data_sync_record, Peer),

	ct:pal(test, 1, "fill the queue up to max_queue_length=2"),
	lists:foreach(fun(N) ->
		spawn(fun() ->
			Reply = arweave_client_throttling:throttle(data_sync_record, Peer),
			Parent ! {n, N, Reply}
		end),
		ok = wait_until(fun() ->
			{ok, #{queue_length := QL}} =
				arweave_client_throttling:status(data_sync_record, Peer),
			QL =:= N
		end)
	end, [1, 2]),

	ct:pal(test, 1, "an extra call must be rejected immediately"),
	{error, queue_full} =
		arweave_client_throttling:throttle(data_sync_record, Peer),

	ct:pal(test, 1, "release the queued waiters"),
	ok = arweave_client_throttling:update_remaining(data_sync_record, Peer, 5),
	receive {n, 1, ok} -> ok after 1000 -> ct:fail(timeout_1) end,
	receive {n, 2, ok} -> ok after 1000 -> ct:fail(timeout_2) end,
	ok.

%%--------------------------------------------------------------------
%% @doc If a caller exits while it is waiting in the queue, the
%% throttler must drop it so it does not consume budget on the next
%% `update_remaining'.
%% @end
%%--------------------------------------------------------------------
dead_caller_is_dropped_from_queue(_Config) ->
	Peer = {10, 2, 2, 2, 1984},
	Parent = self(),

	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),

	ct:pal(test, 1, "queue a doomed caller"),
	Doomed = spawn(fun() ->
		_ = (catch arweave_client_throttling:throttle(general, Peer)),
		Parent ! {done, self()}
	end),
	ok = wait_until(fun() ->
		{ok, #{queue_length := 1}} =:=
			arweave_client_throttling:status(general, Peer)
	end),

	ct:pal(test, 1, "kill the doomed caller"),
	exit(Doomed, kill),
	ok = wait_until(fun() ->
		{ok, #{queue_length := 0}} =:=
			arweave_client_throttling:status(general, Peer)
	end),

	ct:pal(test, 1, "a fresh waiter must be the one to receive the slot"),
	Live = spawn(fun() ->
		ok = arweave_client_throttling:throttle(general, Peer),
		Parent ! {live_done, self()}
	end),
	ok = wait_until(fun() ->
		{ok, #{queue_length := 1}} =:=
			arweave_client_throttling:status(general, Peer)
	end),

	ok = arweave_client_throttling:update_remaining(general, Peer, 1),
	receive {live_done, Live} -> ok after 1000 ->
		ct:fail("live waiter was not released")
	end,
	ok.

%%--------------------------------------------------------------------
%% @doc `reset/1' must drop all peer state and unblock every queued
%% waiter with `ok'.
%% @end
%%--------------------------------------------------------------------
reset_releases_waiters(_Config) ->
	Peer = {10, 3, 3, 3, 1984},
	Parent = self(),

	ok = arweave_client_throttling:throttle(general, Peer),
	ok = arweave_client_throttling:throttle(general, Peer),

	[spawn(fun() ->
		Reply = arweave_client_throttling:throttle(general, Peer),
		Parent ! {released, self(), Reply}
	end) || _ <- lists:seq(1, 2)],
	ok = wait_until(fun() ->
		{ok, #{queue_length := 2}} =:=
			arweave_client_throttling:status(general, Peer)
	end),

	ct:pal(test, 1, "reset the group"),
	ok = arweave_client_throttling:reset(general),

	ct:pal(test, 1, "both blocked waiters must be released"),
	[receive {released, _, ok} -> ok after 1000 ->
		ct:fail("reset did not release a waiter")
	end || _ <- lists:seq(1, 2)],

	ct:pal(test, 1, "state is empty after reset"),
	{ok, #{remaining := 2, queue_length := 0}} =
		arweave_client_throttling:status(general, Peer),
	ok.

%%--------------------------------------------------------------------
%% @doc Both 4-tuple and 5-tuple peer keys must work and remain
%% distinct from each other.
%% @end
%%--------------------------------------------------------------------
peer_4_and_5_tuple_keys(_Config) ->
	Peer4 = {127, 0, 0, 1},
	Peer5 = {127, 0, 0, 1, 1984},

	ok = arweave_client_throttling:throttle(general, Peer4),
	ok = arweave_client_throttling:throttle(general, Peer5),

	{ok, #{remaining := R4}} = arweave_client_throttling:status(general, Peer4),
	{ok, #{remaining := R5}} = arweave_client_throttling:status(general, Peer5),
	1 = R4,
	1 = R5,
	ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

wait_until(Fun) -> wait_until(Fun, 50).

wait_until(_Fun, 0) -> {error, timeout};
wait_until(Fun, N) ->
	case Fun() of
		true -> ok;
		_ ->
			timer:sleep(20),
			wait_until(Fun, N - 1)
	end.
