%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @author Arweave Team
%%% @copyright 2026 (c) Arweave
%%% @doc Top-level interface tests for `arweave_throttling'.
%%% @end
%%%===================================================================
-module(arweave_throttling_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling).

-define(PEER1, {1,2,3,4,1984}).
-define(PEER2, {2,3,4,5,1984}).
-define(PEER3, {3,4,5,6,1984}).

-define(PATH_GENERAL, "some/path/that/lead/to/general").
-define(PATH_DATA_SYNC, "data_sync_record").

-define(GROUPID_GENERAL, general).
-define(GROUPID_DATA_SYNC, data_sync_record).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
	{description, "arweave_throttling top-level API"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],

	ok = arweave_config:start(),
	ConfigSnapshot = arweave_config:snapshot(),
	ok = arweave_throttling:start(),

	[{apps_before, AppsBefore}, {config_snapshot, ConfigSnapshot} | Config].

end_per_testcase(_TestCase, Config) ->
	ok = arweave_config:restore(?config(config_snapshot, Config)),
	ok = arweave_throttling:stop(),

	arweave_throttling_metrics:cleanup(),

	AppsBefore = proplists:get_value(apps_before, Config),
	AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
	AppsStartedForTest = AppsNow -- AppsBefore,
	lists:foreach(fun application:stop/1, AppsStartedForTest),

	ok.

all() ->
	[
	no_groups_started,
	groups_started_on_update_quota,
	throttle_and_update_quota,
	blocking_call_is_released_by_update,
	fifo_ordering,
	concurrent_remaining_updates_take_min,
	stale_update_outside_window_overrides,
	queue_full_returns_error,
	dead_caller_is_dropped_from_queue,
	reset_releases_waiters,
	peer_4_and_5_tuple_keys,
	configured_local_peer_obeys_outbound_quota,
	configured_local_ip_does_not_exempt_peer_shapes,
	exhausted_quota_refills_after_reset_seconds,
	update_quota_cancels_reset_timer
	].

%%====================================================================
%% Test cases
%%====================================================================

no_groups_started(_Config) ->
	?assert(is_pid(whereis(arweave_throttling_sup))),

	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	?assertNot(is_pid(whereis(arweave_throttling_group_data_sync_record))),
	ok.


groups_started_on_update_quota(_Config) ->
	?assert(is_pid(whereis(arweave_throttling_sup))),

	?M:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),

	?assert(is_pid(whereis(arweave_throttling_group_general))),
	?assertNot(is_pid(whereis(arweave_throttling_group_data_sync_record))),

	?M:update_quota(?PEER1, ?PATH_DATA_SYNC, headers(?GROUPID_DATA_SYNC, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_data_sync_record))),
	ok.

throttle_and_update_quota(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(remaining, S) =:= 0
					end),

	Headers = headers(?GROUPID_GENERAL, 10, 3, 0),
	ct:pal(">> headers: ~p~n >>> parsed Quota: ~p~n", [Headers, arweave_throttling_http_headers:parse(Headers)]),
	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
										Headers),

	ok = wait_status(general, ?PEER1, fun(S) ->
						(maps:get(remaining, S) =:= 3)
						andalso (maps:get(total, S) =:= 10)
						andalso (maps:get(reset_seconds, S) =:= 0)
					end),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(remaining, S) =:= 0
					end),
	ok.

blocking_call_is_released_by_update(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),

	assert_throttle_blocks_until_quota_update(?PEER1, ?PATH_GENERAL, ?GROUPID_GENERAL).

fifo_ordering(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	Parent = self(),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),

	_Pids = lists:map(fun(N) ->
				Pid = spawn(fun() ->
						ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
						Parent ! {released, N, self()}
					end),
				ok = wait_status(general, ?PEER1, fun(S) ->
									maps:get(queue_length, S) =:= N
								end),
				Pid
			end, [1, 2, 3]),

	%% concurrency_window_ms is 50 in init_per_testcase, so 80ms ensures
	%% each update is treated as a fresh observation rather than merged
	%% with the previous one.
	Order = lists:map(fun(_) ->
				timer:sleep(80),
				ok = arweave_throttling:update_quota(
					?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 10, 1, 0)),
				receive {released, N, _} -> N after 1000 ->
					ct:fail("a waiter was never released")
				end
			end, [1, 2, 3]),

	[1, 2, 3] = Order,
	ok.

concurrent_remaining_updates_take_min(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 20, 10, 0)),
	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 20, 3, 0)),
	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 20, 7, 0)),

	ok = wait_status(general, ?PEER1, fun(S) ->
						(maps:get(remaining, S) =:= 3)
						andalso (maps:get(total, S) =:= 20)
						andalso (maps:get(last_update_ts, S) =/= undefined)
					end),
	ok.

stale_update_outside_window_overrides(_Config) ->
	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL,10, 2, 0)),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(remaining, S) =:= 2
					end),

	timer:sleep(150),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 10, 9, 0)),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(remaining, S) =:= 9
					end),
	ok.

queue_full_returns_error(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_data_sync_record))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_DATA_SYNC),
	?assertNot(is_pid(whereis(arweave_throttling_group_data_sync_record))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_DATA_SYNC, headers(?GROUPID_DATA_SYNC, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_data_sync_record))),

	Parent = self(),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_DATA_SYNC),

	lists:foreach(fun(N) ->
			spawn(fun() ->
				Reply = arweave_throttling:throttle(?PEER1, ?PATH_DATA_SYNC),
				Parent ! {n, N, Reply}
			end)
		end, lists:seq(1, 5003)), %% remaining quota (2) + max_queue_length (5000) + 1

	ok = wait_status(?GROUPID_DATA_SYNC, ?PEER1,
				fun(S) ->
					maps:get(queue_length, S) =:= 5000
				end),

	{error, queue_full} =
		arweave_throttling:throttle(?PEER1, ?PATH_DATA_SYNC),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_DATA_SYNC,
						headers(?GROUPID_DATA_SYNC, 10, 5, 0)),

	receive {n, _N1, ok} -> ok after 1000 -> ct:fail(timeout_1) end,
	receive {n, _N2, ok} -> ok after 1000 -> ct:fail(timeout_2) end,
	ok.

dead_caller_is_dropped_from_queue(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	Parent = self(),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),

	Doomed = spawn(fun() ->
			_ = (catch arweave_throttling:throttle(?PEER1, ?PATH_GENERAL)),
			Parent ! {done, self()}
		end),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(queue_length, S) =:= 1
					end),

	exit(Doomed, kill),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(queue_length, S) =:= 0
					end),

	Live = spawn(fun() ->
			ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
			Parent ! {live_done, self()}
		end),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(queue_length, S) =:= 1
					end),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 10, 1, 0)),

	receive {live_done, Live} -> ok after 1000 ->
		ct:fail("live waiter was not released")
	end,
	ok.

reset_releases_waiters(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	Parent = self(),

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),

	[spawn(fun() ->
		Reply = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
		Parent ! {released, self(), Reply}
	end) || _ <- lists:seq(1, 2)],
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(queue_length, S) =:= 2
					end),

	ok = arweave_throttling:reset(general),

	[receive {released, _, ok} -> ok after 1000 ->
		ct:fail("reset did not release a waiter")
	end || _ <- lists:seq(1, 2)],

	{ok, Status} = arweave_throttling:status(general, ?PEER1),
	%% Reset also clears the peers -> quota is infinity when peer is not present.
	?assertEqual(infinity, maps:get(remaining, Status)),
	?assertEqual(0, maps:get(queue_length, Status)),
	ok.

peer_4_and_5_tuple_keys(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	ok = arweave_throttling:update_quota(?PEER2, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	%% This one is a bit redundant, since we pass the IP and Port for each request
	%% to the client.

	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	ok = arweave_throttling:throttle(?PEER2, ?PATH_GENERAL),

	{ok, S4} = arweave_throttling:status(general, ?PEER1),
	{ok, S5} = arweave_throttling:status(general, ?PEER2),
	?assertEqual(1, maps:get(remaining, S4)),
	?assertEqual(1, maps:get(remaining, S5)),
	ok.

configured_local_peer_obeys_outbound_quota(_Config) ->
	ok = arweave_config:set([peers, local], [?PEER1]),
	ok = record_quota(?PEER1, 1, 0, 600),
	ok = record_quota(?PEER3, 10, 10, 0),

	?assert(arweave_throttling:is_throttled(?PEER1, ?PATH_GENERAL)),
	?assertNot(arweave_throttling:is_throttled(?PEER3, ?PATH_GENERAL)),

	assert_throttle_blocks_until_quota_update(?PEER1, ?PATH_GENERAL, ?GROUPID_GENERAL).

configured_local_ip_does_not_exempt_peer_shapes(_Config) ->
	LocalPeerShapes = [
		{1,2,3,4},
		{1,2,3,4,9999},
		{{1,2,3,4},9999}
	],
	ok = arweave_config:set([peers, local], [?PEER1]),
	lists:foreach(fun(Peer) -> ok = record_quota(Peer, 1, 0, 600) end, LocalPeerShapes),

	lists:foreach(fun(Peer) ->
		?assert(arweave_throttling:is_throttled(Peer, ?PATH_GENERAL))
	end, LocalPeerShapes),
	ok.

%% @doc When `update_quota' reports an exhausted quota together with
%% `reset_seconds > 0', a timer must refill `remaining' to `total'
%% once that many seconds elapse, releasing any blocked waiters.
exhausted_quota_refills_after_reset_seconds(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	Parent = self(),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 5, 0, 1)),
	ok = wait_status(general, ?PEER1, fun(S) ->
						(maps:get(remaining, S) =:= 0)
						andalso (maps:get(total, S) =:= 5)
						andalso (maps:get(reset_seconds, S) =:= 1)
					end),

	spawn(fun() ->
		ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
		Parent ! refilled
	end),
	ok = wait_status(general, ?PEER1, fun(S) ->
						maps:get(queue_length, S) =:= 1
					end),

	receive
		refilled -> ok
	after 3000 ->
		ct:fail("reset_seconds timer did not refill the quota")
	end,

	{ok, AfterStatus} = arweave_throttling:status(general, ?PEER1),
	0 = maps:get(reset_seconds, AfterStatus),
	true = maps:get(remaining, AfterStatus) >= 4,
	ok.

%% @doc A subsequent `update_quota' must cancel any pending reset
%% timer so we do not over-refill the budget later on.
update_quota_cancels_reset_timer(_Config) ->
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),
	ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
	?assertNot(is_pid(whereis(arweave_throttling_group_general))),

	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 2, 2)),
	?assert(is_pid(whereis(arweave_throttling_group_general))),

	%% This will update since the headers total is different from the previously set one.
	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
							headers(?GROUPID_GENERAL, 5, 0, 30)),
	ok = wait_status(general, ?PEER1, fun(S) ->
						(maps:get(reset_seconds, S) =:= 30)
						andalso (maps:get(remaining, S) =:= 0)
					end),

	timer:sleep(80),
	ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
						headers(?GROUPID_GENERAL, 5, 4, 0)),
	ok = wait_status(general, ?PEER1, fun(S) ->
						(maps:get(remaining, S) =:= 4)
						andalso (maps:get(reset_seconds, S) =:= 0)
					end),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

assert_throttle_blocks_until_quota_update(Peer, Path, GroupID) ->
	Parent = self(),
	Pid = spawn_link(fun() ->
		Result = arweave_throttling:throttle(Peer, Path),
		Parent ! {throttle_result, self(), Result}
	end),
	ok = wait_status(GroupID, Peer, fun(Status) ->
		maps:get(queue_length, Status) =:= 1
	end),
	false = receive
		{throttle_result, Pid, _Result} -> true
	after 100 ->
		false
	end,
	ok = arweave_throttling:update_quota(Peer, Path,
		headers(GroupID, 10, 1, 0)),
	receive
		{throttle_result, Pid, ok} -> ok
	after 1000 ->
		ct:fail(throttle_was_not_released)
	end.

record_quota(Peer, Total, Remaining, ResetSeconds) ->
	ok = arweave_throttling:update_quota(Peer, ?PATH_GENERAL,
		headers(?GROUPID_GENERAL, Total, Remaining, ResetSeconds)),
	wait_status(?GROUPID_GENERAL, Peer, fun(Status) ->
		maps:get(total, Status) =:= Total andalso
			maps:get(remaining, Status) =:= Remaining
	end).

headers(GroupID, Total, Remaining) ->
	headers(GroupID, Total, Remaining, 0).

headers(GroupID, Total, Remaining, ResetSeconds) ->
	arweave_limiter_http_headers:to_http_headers(
	{register, leaky,
	#{expiring_limit => Total,
		remaining      => Remaining,
		reset_seconds  => ResetSeconds,
		policies => policies(GroupID, Total)}
	}).


policies(Group, Total) ->
	#{id => atom_to_list(Group),
	concurrency => #{limit => 500},
	sliding_window => #{limit => 0,
				window_seconds => 1},
	leaky_bucket   => #{burst => Total,
				tick_ms => 30000,
				tick_reduction => Total}}.

wait_status(Group, Peer, Pred) ->
	ar_test_await:until(throttling_status_reached, fun() ->
			case arweave_throttling:status(Group, Peer) of
				{ok, Status} -> Pred(Status);
				_ -> false
			end
		end, 1000).
