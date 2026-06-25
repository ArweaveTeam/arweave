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
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
    default_groups_started/1,
    throttle_and_update_quota/1,
    blocking_call_is_released_by_update/1,
    fifo_ordering/1,
    concurrent_remaining_updates_take_min/1,
    stale_update_outside_window_overrides/1,
    queue_full_returns_error/1,
    dead_caller_is_dropped_from_queue/1,
    reset_releases_waiters/1,
    peer_4_and_5_tuple_keys/1,
    exhausted_quota_refills_after_reset_seconds/1,
    update_quota_cancels_reset_timer/1
]).

-include_lib("common_test/include/ct.hrl").

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
    {description, "arweave_throttling top-level API"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],

    application:ensure_all_started(arweave_config),
    apply_overrides(general, #{id => general,
                              initial_remaining => 2,
                              max_queue_length => 4,
                              concurrency_window_ms => 50}),
    apply_overrides(data_sync_record,
                    #{id => data_sync_record,
                      initial_remaining => 1,
                      max_queue_length => 2,
                      concurrency_window_ms => 50}),

    ct:pal(info, 1, "start arweave_throttling"),
    ok = arweave_throttling:start(),
    [{apps_before,AppsBefore},
     {config, Config}].

end_per_testcase(_TestCase, Config) ->
    ct:pal(info, 1, "stop arweave_throttling"),
    ok = arweave_throttling:stop(),

    arweave_throttling_metrics:cleanup(),

    AppsBefore = proplists:get_value(apps_before, Config),
    AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    AppsStartedForTest = AppsNow -- AppsBefore,
    lists:foreach(fun application:stop/1, AppsStartedForTest),

    ok.

apply_overrides(GroupID, Overrides) ->
    maps:fold(
        fun(Field, Value, ok) ->
            ok = arweave_config:set([client_throttling, GroupID, Field], Value),
            ok
        end, ok, Overrides).

all() ->
    [
        default_groups_started,
        throttle_and_update_quota,
        blocking_call_is_released_by_update,
        fifo_ordering,
        concurrent_remaining_updates_take_min,
        stale_update_outside_window_overrides,
        queue_full_returns_error,
        dead_caller_is_dropped_from_queue,
        reset_releases_waiters,
        peer_4_and_5_tuple_keys,
        exhausted_quota_refills_after_reset_seconds,
        update_quota_cancels_reset_timer
    ].

default_groups_started(_Config) ->
    ct:pal(test, 1, "check supervisor is alive"),
    true = is_pid(whereis(arweave_throttling_sup)),

    ct:pal(test, 1, "list configured groups"),
    Groups = arweave_config_options_client_throttling:group_ids(),
    true = lists:member(general, Groups),
    true = lists:member(data_sync_record, Groups),

    ct:pal(test, 1, "each group worker is registered"),
    true = is_pid(whereis(arweave_throttling_group_general)),
    true = is_pid(whereis(arweave_throttling_group_data_sync_record)),
    ok.

throttle_and_update_quota(_Config) ->
    Peer = {127, 0, 0, 1, 1984},

    ct:pal(test, 1, "initial budget is consumed in order"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(remaining, S) =:= 0
                                    end),

    ct:pal(test, 1, "refresh quota non-blockingly"),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 10, 3, 0)),
    ok = wait_status(general, Peer, fun(S) ->
                                            (maps:get(remaining, S) =:= 3)
                                                andalso (maps:get(total, S) =:= 10)
                                                andalso (maps:get(reset_seconds, S) =:= 0)
                                    end),

    ct:pal(test, 1, "the new budget is honored"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(remaining, S) =:= 0
                                    end),
    ok.

blocking_call_is_released_by_update(_Config) ->
    Peer = {10, 0, 0, 1, 1984},

    ct:pal(test, 1, "drain the initial budget"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),

    ct:pal(test, 1, "next caller must block"),
    Parent = self(),
    Pid = spawn_link(fun() ->
                             Reply = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
                             Parent ! {done, self(), Reply}
                     end),

    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(queue_length, S) =:= 1
                                    end),
    false = receive {done, Pid, _} -> true after 100 -> false end,

    ct:pal(test, 1, "an update_quota releases the waiter"),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 10, 1, 0)),

    receive
        {done, Pid, ok} -> ok
    after 1000 ->
        ct:fail("blocked caller was not released")
    end,
    ok.

fifo_ordering(_Config) ->
    Peer = {172, 16, 0, 1, 1984},
    Parent = self(),

    ct:pal(test, 1, "drain initial budget"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),

    ct:pal(test, 1, "queue three callers serially to fix the FIFO order"),
    _Pids = lists:map(fun(N) ->
                              Pid = spawn(fun() ->
                                                  ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
                                                  Parent ! {released, N, self()}
                                          end),
                              ok = wait_status(general, Peer, fun(S) ->
                                                                     maps:get(queue_length, S) =:= N
                                                             end),
                              Pid
                      end, [1, 2, 3]),

    ct:pal(test, 1, "release them one at a time, spacing past the window"),
    %% concurrency_window_ms is 50 in init_per_testcase, so 80ms ensures
    %% each update is treated as a fresh observation rather than merged
    %% with the previous one.
    Order = lists:map(fun(_) ->
                              timer:sleep(80),
                              ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                                          quota(general, 10, 1, 0)),
                              receive {released, N, _} -> N after 1000 ->
                                  ct:fail("a waiter was never released")
                              end
                      end, [1, 2, 3]),

    [1, 2, 3] = Order,
    ok.

concurrent_remaining_updates_take_min(_Config) ->
    Peer = {192, 168, 1, 1, 1984},

    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 20, 10, 0)),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 20, 3, 0)),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 20, 7, 0)),

    ok = wait_status(general, Peer, fun(S) ->
                                            (maps:get(remaining, S) =:= 3)
                                                andalso (maps:get(total, S) =:= 20)
                                                andalso (maps:get(last_update_ts, S) =/= undefined)
                                    end),
    ok.

stale_update_outside_window_overrides(_Config) ->
    Peer = {192, 168, 1, 2, 1984},

    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 10, 2, 0)),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(remaining, S) =:= 2
                                    end),

    ct:pal(test, 1, "sleep past the concurrency window"),
    timer:sleep(150),

    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 10, 9, 0)),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(remaining, S) =:= 9
                                    end),
    ok.

queue_full_returns_error(_Config) ->
    Peer = {10, 1, 1, 1, 1984},
    Parent = self(),

    ct:pal(test, 1, "drain initial budget"),
    ok = arweave_throttling:throttle(Peer, "data_sync_record"),

    ct:pal(test, 1, "fill the queue up to max_queue_length=2"),
    lists:foreach(fun(N) ->
                          spawn(fun() ->
                                        Reply = arweave_throttling:throttle(Peer, "data_sync_record"),
                                        Parent ! {n, N, Reply}
                                end),
                          ok = wait_status(data_sync_record, Peer, fun(S) ->
                                                                          maps:get(queue_length, S) =:= N
                                                                  end)
                  end, [1, 2]),

    ct:pal(test, 1, "an extra call must be rejected immediately"),
    {error, queue_full} =
        arweave_throttling:throttle(Peer, "data_sync_record"),

    ct:pal(test, 1, "release the queued waiters"),
    ok = arweave_throttling:update_quota(Peer, "data_sync_record",
                                                quota(data_sync_record, 10, 5, 0)),
    receive {n, 1, ok} -> ok after 1000 -> ct:fail(timeout_1) end,
    receive {n, 2, ok} -> ok after 1000 -> ct:fail(timeout_2) end,
    ok.

dead_caller_is_dropped_from_queue(_Config) ->
    Peer = {10, 2, 2, 2, 1984},
    Parent = self(),

    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),

    ct:pal(test, 1, "queue a doomed caller"),
    Doomed = spawn(fun() ->
                           _ = (catch arweave_throttling:throttle(Peer, "some/path/that/lead/to/general")),
                           Parent ! {done, self()}
                   end),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(queue_length, S) =:= 1
                                    end),

    ct:pal(test, 1, "kill the doomed caller"),
    exit(Doomed, kill),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(queue_length, S) =:= 0
                                    end),

    ct:pal(test, 1, "a fresh waiter must be the one to receive the slot"),
    Live = spawn(fun() ->
                         ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
                         Parent ! {live_done, self()}
                 end),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(queue_length, S) =:= 1
                                    end),

    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 10, 1, 0)),
    receive {live_done, Live} -> ok after 1000 ->
        ct:fail("live waiter was not released")
    end,
    ok.

reset_releases_waiters(_Config) ->
    Peer = {10, 3, 3, 3, 1984},
    Parent = self(),

    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),

    [spawn(fun() ->
                   Reply = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
                   Parent ! {released, self(), Reply}
           end) || _ <- lists:seq(1, 2)],
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(queue_length, S) =:= 2
                                    end),

    ct:pal(test, 1, "reset the group"),
    ok = arweave_throttling:reset(general),

    ct:pal(test, 1, "both blocked waiters must be released"),
    [receive {released, _, ok} -> ok after 1000 ->
        ct:fail("reset did not release a waiter")
     end || _ <- lists:seq(1, 2)],

    ct:pal(test, 1, "state is empty after reset"),
    {ok, Status} = arweave_throttling:status(general, Peer),
    2 = maps:get(remaining, Status),
    0 = maps:get(queue_length, Status),
    ok.

peer_4_and_5_tuple_keys(_Config) ->
    %% This one is a bit redundant, since we pass the IP and Port for each request
    %% to the client.
    Peer4 = {127, 0, 0, 1},
    Peer5 = {127, 0, 0, 1, 1984},

    ok = arweave_throttling:throttle(Peer4, "some/path/that/lead/to/general"),
    ok = arweave_throttling:throttle(Peer5, "some/path/that/lead/to/general"),

    {ok, S4} = arweave_throttling:status(general, Peer4),
    {ok, S5} = arweave_throttling:status(general, Peer5),
    1 = maps:get(remaining, S4),
    1 = maps:get(remaining, S5),
    ok.

%% @doc When `update_quota' reports an exhausted quota together with
%% `reset_seconds > 0', a timer must refill `remaining' to `total'
%% once that many seconds elapse, releasing any blocked waiters.
exhausted_quota_refills_after_reset_seconds(_Config) ->
    Peer = {10, 4, 4, 4, 1984},
    Parent = self(),

    ct:pal(test, 1, "report an exhausted quota with a 1s reset"),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 5, 0, 1)),
    ok = wait_status(general, Peer, fun(S) ->
                                            (maps:get(remaining, S) =:= 0)
                                                andalso (maps:get(total, S) =:= 5)
                                                andalso (maps:get(reset_seconds, S) =:= 1)
                                    end),

    ct:pal(test, 1, "queue a waiter while the quota is exhausted"),
    spawn(fun() ->
                  ok = arweave_throttling:throttle(Peer, "some/path/that/lead/to/general"),
                  Parent ! refilled
          end),
    ok = wait_status(general, Peer, fun(S) ->
                                            maps:get(queue_length, S) =:= 1
                                    end),

    ct:pal(test, 1,
           "after the reset interval the waiter must be released"),
    receive
        refilled -> ok
    after 3000 ->
        ct:fail("reset_seconds timer did not refill the quota")
    end,

    {ok, AfterStatus} = arweave_throttling:status(general, Peer),
    0 = maps:get(reset_seconds, AfterStatus),
    true = maps:get(remaining, AfterStatus) >= 4,
    ok.

%% @doc A subsequent `update_quota' must cancel any pending reset
%% timer so we do not over-refill the budget later on.
update_quota_cancels_reset_timer(_Config) ->
    Peer = {10, 5, 5, 5, 1984},

    ct:pal(test, 1, "report exhausted quota with a long reset window"),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 5, 0, 30)),
    ok = wait_status(general, Peer, fun(S) ->
                                            (maps:get(reset_seconds, S) =:= 30)
                                                andalso (maps:get(remaining, S) =:= 0)
                                    end),

    ct:pal(test, 1, "a non-exhausted update should clear the timer"),
    timer:sleep(80),
    ok = arweave_throttling:update_quota(Peer, "some/path/that/lead/to/general",
                                                quota(general, 5, 4, 0)),
    ok = wait_status(general, Peer, fun(S) ->
                                            (maps:get(remaining, S) =:= 4)
                                                andalso (maps:get(reset_seconds, S) =:= 0)
                                    end),
    ok.

%% Helpers

quota(GroupId, Total, Remaining, ResetSeconds) ->
    TotalLine = io_lib:format("~p, 10;w=1;policy=\"~p sliding window\", 450;w=1;burst=450;policy=\"~p leaky bucket\" 500;w=1;policy=\"~p concurrency\" ", [Total, GroupId, GroupId, GroupId]),
    [{<<"RateLimit-Limit">> , list_to_binary(TotalLine)},
     {<<"RateLimit-Remaining">>, integer_to_binary(Remaining)},
     {<<"RateLimit-Reset">>, integer_to_binary(ResetSeconds)}].

wait_status(Group, Peer, Pred) ->
    wait_until(fun() ->
                       case arweave_throttling:status(Group, Peer) of
                           {ok, Status} -> Pred(Status);
                           _ -> false
                       end
               end).

wait_until(Fun) -> wait_until(Fun, 50).

wait_until(_Fun, 0) -> {error, timeout};
wait_until(Fun, N) ->
    case Fun() of
        true -> ok;
        _ ->
            timer:sleep(20),
            wait_until(Fun, N - 1)
    end.
