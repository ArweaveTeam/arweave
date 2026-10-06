-module(arweave_throttling_idle_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PEER1, {1, 2, 3, 4, 1984}).

-define(PATH_GENERAL, "some/path/that/lead/to/general").
-define(GROUPID_GENERAL, "general").

%% Groups not defined for the local limiter, so they count against
%% `[throttling, max_processes]'.
-define(PATH_REMOTE_A, "remote_a").
-define(GROUPID_REMOTE_A, "remote_a").
-define(PATH_REMOTE_B, "remote_b").
-define(GROUPID_REMOTE_B, "remote_b").

-define(IDLE_TIMEOUT_MS, 300).
-define(METRIC, arweave_throttling_idle_shutdown_total).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
    {description, "arweave_throttling_group idle shutdown"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    ok = arweave_config:start(),
    ConfigSnapshot = arweave_config:internal_snapshot(),
    ok = arweave_config:set([throttling, idle_timeout], ?IDLE_TIMEOUT_MS),
    ok = arweave_throttling:start(),
    [{apps_before, AppsBefore}, {config_snapshot, ConfigSnapshot} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = arweave_config:internal_restore(?config(config_snapshot, Config)),
    ok = arweave_throttling:stop(),

    arweave_throttling_metrics:cleanup(),

    AppsBefore = proplists:get_value(apps_before, Config),
    AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    AppsStartedForTest = AppsNow -- AppsBefore,
    lists:foreach(fun application:stop/1, AppsStartedForTest),
    catch arweave_metrics:cleanup(),
    ok.

all() ->
    [
        idle_timeout_is_reread_on_idle_check,
        idle_group_stops_and_is_counted,
        activity_postpones_idle_shutdown,
        pending_refill_postpones_idle_shutdown,
        stopped_group_lets_requests_through,
        update_quota_restarts_stopped_group,
        idle_group_frees_process_slot
    ].

%%====================================================================
%% Test cases
%%====================================================================

%% The group arms its first idle check with the configured timeout, but
%% raising the option before that check fires keeps the group running.
idle_timeout_is_reread_on_idle_check(_Config) ->
    {ok, Pid} = start_general_group(),
    MRef = monitor(process, Pid),
    ok = arweave_config:set([throttling, idle_timeout], 60000),

    ?assertEqual(timeout, wait_down(MRef, 5 * ?IDLE_TIMEOUT_MS)),
    ?assert(is_process_alive(Pid)),
    ok.

idle_group_stops_and_is_counted(_Config) ->
    {ok, Pid} = start_general_group(),
    MRef = monitor(process, Pid),

    ?assertEqual({down, normal}, wait_down(MRef, 10 * ?IDLE_TIMEOUT_MS)),

    ?assertEqual({error, group_not_found},
                 arweave_throttling_process:get(?GROUPID_GENERAL)),
    %% `transient' child: the supervisor keeps the spec but does not
    %% restart a group that stopped with reason `normal'.
    ?assertMatch([{?GROUPID_GENERAL, undefined, worker, _}],
                 supervisor:which_children(arweave_throttling_sup)),
    ?assertEqual([], arweave_throttling_sup:all_info()),
    ok.

activity_postpones_idle_shutdown(_Config) ->
    {ok, Pid} = start_general_group(),
    MRef = monitor(process, Pid),

    %% Keep the group busy for several idle timeouts.
    lists:foreach(
        fun(_) ->
            timer:sleep(?IDLE_TIMEOUT_MS div 3),
            ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
            false = arweave_throttling:is_throttled(?PEER1, ?PATH_GENERAL)
        end,
        lists:seq(1, 12)),
    ?assert(is_process_alive(Pid)),

    ?assertEqual({down, normal}, wait_down(MRef, 10 * ?IDLE_TIMEOUT_MS)),
    ok.

pending_refill_postpones_idle_shutdown(_Config) ->
    {ok, Pid} = start_general_group(),
    MRef = monitor(process, Pid),

    %% Exhaust the quota; the remote refills it after 2 seconds.
    ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
                                         headers(?GROUPID_GENERAL, 5, 0, 5, 2)),
    Parent = self(),
    spawn(fun() ->
        ok = arweave_throttling:throttle(?PEER1, ?PATH_GENERAL),
        Parent ! refilled
    end),
    ok = ar_test_await:until(caller_queued, fun() ->
        arweave_throttling_group:pending(?GROUPID_GENERAL, ?PEER1) =:= 1
    end, 1000),

    %% Deliberately wait past the idle timeout: the queued caller and the
    %% pending refill keep the group alive.
    timer:sleep(3 * ?IDLE_TIMEOUT_MS),
    ?assert(is_process_alive(Pid)),

    receive
        refilled -> ok
    after 5000 ->
        ct:fail("reset_seconds timer did not release the queued caller")
    end,
    ?assertEqual({down, normal}, wait_down(MRef, 10 * ?IDLE_TIMEOUT_MS)),
    ok.

stopped_group_lets_requests_through(_Config) ->
    {ok, Pid} = start_general_group(),
    ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
                                         headers(?GROUPID_GENERAL, 5, 0)),
    ?assert(arweave_throttling:is_throttled(?PEER1, ?PATH_GENERAL)),
    MRef = monitor(process, Pid),
    ?assertEqual({down, normal}, wait_down(MRef, 10 * ?IDLE_TIMEOUT_MS)),

    %% The quota state is gone with the group: nothing is throttled and
    %% no call fails with `noproc'.
    ?assertEqual(ok, arweave_throttling:throttle(?PEER1, ?PATH_GENERAL)),
    ?assertEqual(false, arweave_throttling:is_throttled(?PEER1, ?PATH_GENERAL)),
    ?assertEqual({error, group_not_found},
                 arweave_throttling:status(?GROUPID_GENERAL, ?PEER1)),
    ok.

update_quota_restarts_stopped_group(_Config) ->
    {ok, Pid1} = start_general_group(),
    MRef1 = monitor(process, Pid1),
    ?assertEqual({down, normal}, wait_down(MRef1, 10 * ?IDLE_TIMEOUT_MS)),

    %% The router already maps the path to the group, so this goes
    %% through the known-group branch of `update_quota/3'.
    ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
                                         headers(?GROUPID_GENERAL, 5, 3)),
    {ok, Pid2} = arweave_throttling_process:get(?GROUPID_GENERAL),
    ?assertNotEqual(Pid1, Pid2),
    ?assertMatch([{?GROUPID_GENERAL, Pid2, worker, _}],
                 supervisor:which_children(arweave_throttling_sup)),
    ok = ar_test_await:until(quota_applied, fun() ->
        case arweave_throttling:status(?GROUPID_GENERAL, ?PEER1) of
            {ok, #{remaining := 3}} -> true;
            _ -> false
        end
    end, 1000),

    MRef2 = monitor(process, Pid2),
    ?assertEqual({down, normal}, wait_down(MRef2, 10 * ?IDLE_TIMEOUT_MS)),
    ok.

idle_group_frees_process_slot(_Config) ->
    ok = arweave_config:set([throttling, max_processes], 1),
    ok = arweave_throttling:update_quota(?PEER1, ?PATH_REMOTE_A,
                                         headers(?GROUPID_REMOTE_A, 100, 99)),
    {ok, Pid} = arweave_throttling_process:get(?GROUPID_REMOTE_A),
    ?assertEqual({error, process_limit_breached},
                 arweave_throttling:update_quota(?PEER1, ?PATH_REMOTE_B,
                     headers(?GROUPID_REMOTE_B, 5, 4))),

    MRef = monitor(process, Pid),
    ?assertEqual({down, normal}, wait_down(MRef, 10 * ?IDLE_TIMEOUT_MS)),
    ?assertEqual(0, arweave_throttling_sup:count_running()),

    ?assertEqual(ok, arweave_throttling:update_quota(?PEER1, ?PATH_REMOTE_B,
                     headers(?GROUPID_REMOTE_B, 5, 4))),
    ?assertMatch({ok, _}, arweave_throttling_process:get(?GROUPID_REMOTE_B)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% The first quota update for a path starts the group.
start_general_group() ->
    ok = arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL,
                                         headers(?GROUPID_GENERAL, 100, 99)),
    arweave_throttling_process:get(?GROUPID_GENERAL).

wait_down(MRef, Timeout) ->
    receive
        {'DOWN', MRef, process, _Pid, Reason} -> {down, Reason}
    after Timeout ->
        timeout
    end.

headers(GroupID, Total, Remaining) ->
    headers(GroupID, Total, Remaining, Total - Remaining, 0).

headers(GroupID, Total, Remaining, ResetAmount, ResetSeconds) ->
    arweave_limiter_http_headers:to_http_headers(
        {register, leaky, #{
            expiring_limit => Total,
            remaining => Remaining,
            reset_amount => ResetAmount,
            reset_seconds => ResetSeconds,
            policies => policies(GroupID, Total)
        }}
    ).

policies(Group, Total) ->
    #{
        id => Group,
        concurrency => #{limit => 500},
        sliding_window => #{
            limit => 0,
            window_seconds => 1
        },
        leaky_bucket => #{
            burst => Total,
            tick_ms => 30000,
            tick_reduction => Total
        }
    }.
