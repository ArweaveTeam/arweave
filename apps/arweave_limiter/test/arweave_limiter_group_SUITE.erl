-module(arweave_limiter_group_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_group).
-define(TABLE, eunit_arweave_limiter_tests_mock).
-define(KEY, ts_now).
-define(TEST_LIMITER, 'test_limiter').
-define(TEST_LIMITER_0, 'arweave_limiter_test_limiter_0').
-define(CALL_TIMEOUT, 1000).

-define(setTSMock(TS), ets:insert(?TABLE, {?KEY, TS})).

-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer, Now),
        begin
            ((fun () ->
                      Parent = self(),
                      ?assert(?setTSMock(Now)),
                      PID = spawn_link(fun() ->
                                               ?assertMatch(
                                                  Pattern,
                                                  ?M:register_or_reject_call(LimiterRef, Peer)),
                                               Parent ! call_done,
                                               receive
                                                   done -> ok
                                               end
                                       end),
                      receive
                          call_done ->
                              ok
                      after
                          1000 ->
                              %% This should never really happen.
                              %% The call shouldn't take like a second. And if it crashes,
                              %% we expect to spawn_link to take down the test process as well
                              erlang:error({timeout, register_or_reject_call_test})
                      end,
                      PID
              end)())
        end
       ).

suite() -> [{timetrap, {minutes, 5}}, {userdata, [description()]}].

description() -> {description, "arweave_limiter_group test interface"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

all() ->
    [
     expire,
     add_and_order,
     cleanup_timestamps_map,
     noproc,
     timeout,
     simple_sliding_happy,
     simple_leaky_happy_path,
     rate_limiter_rejected_due_concurrency,
     rejected_due_leaky_rate,
     both_exhausted,
     peer_cleanup,
     leaky_manual_reduction,
     sliding_manual_reduction,
     leaky_manual_reduction_disabled
    ].

%% Testcases that start a limiter process via the shared setup/1.
process_testcases() ->
    [
     simple_sliding_happy,
     simple_leaky_happy_path,
     rate_limiter_rejected_due_concurrency,
     rejected_due_leaky_rate,
     both_exhausted,
     peer_cleanup,
     leaky_manual_reduction,
     sliding_manual_reduction,
     leaky_manual_reduction_disabled
    ].

%% Per-testcase limiter configuration for the stateful testcases.
limiter_config(simple_sliding_happy) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 0,
      concurrency_limit => 5,
      sliding_window_limit => 2,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(simple_leaky_happy_path) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 5,
      concurrency_limit => 2,
      sliding_window_limit => 0,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(rate_limiter_rejected_due_concurrency) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit=> 5,
      concurrency_limit => 2,
      sliding_window_limit => 0,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(rejected_due_leaky_rate) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 2,
      concurrency_limit => 5,
      sliding_window_limit => 0,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(both_exhausted) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 1,
      concurrency_limit => 10,
      sliding_window_limit => 1,
      sliding_window_duration => 100000,
      leaky_tick_ms => 10000000,
      timestamp_cleanup_expiry => 1000,
      timestamp_cleanup_tick_ms => 1000000};
limiter_config(peer_cleanup) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 1,
      concurrency_limit => 2,
      sliding_window_limit => 1,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(leaky_manual_reduction) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 5,
      concurrency_limit => 10,
      sliding_window_limit => 0,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(sliding_manual_reduction) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 0,
      concurrency_limit => 10,
      sliding_window_limit => 5,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000};
limiter_config(leaky_manual_reduction_disabled) ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      is_manual_reduction_disabled => true,
      tick_reduction => 1,
      leaky_rate_limit => 5,
      concurrency_limit => 10,
      sliding_window_limit => 0,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000000,
      leaky_tick_ms => 100000}.

timeout_config() ->
    #{id => ?TEST_LIMITER,
      number_of_workers => 1,
      tick_reduction => 1,
      leaky_rate_limit => 0,
      concurrency_limit => 5,
      sliding_window_limit => 2,
      sliding_window_duration => 1000,
      timestamp_cleanup_expiry => 1000,
      leaky_tick_ms => 100000}.

init_per_testcase(timeout, Config) ->
    LimiterConfig = timeout_config(),
    LimiterPID = timeout_setup(LimiterConfig),
    [{limiter_pid, LimiterPID}, {limiter_config, LimiterConfig} | Config];
init_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, process_testcases()) of
        true ->
            LimiterConfig = limiter_config(TestCase),
            {LimiterPID, BeforeApps} = setup(LimiterConfig),
            [{limiter_pid, LimiterPID}, {limiter_config, LimiterConfig}, {before_apps, BeforeApps} | Config];
        false ->
            Config
    end.

end_per_testcase(timeout, Config) ->
    cleanup(?config(limiter_config, Config), ?config(limiter_pid, Config)),
    ok;
end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, process_testcases()) of
        true ->
            cleanup(?config(limiter_config, Config),
                    {?config(limiter_pid, Config), ?config(before_apps, Config)});
        false ->
            ok
    end,
    ok.

set_if_defined(Key, Config) ->
    case maps:get(Key, Config, undefined) of
        undefined ->
            ok;
        Value ->
            arweave_config:set([limiter, ?TEST_LIMITER, Key], Value)
    end.

%%%===================================================================
%%% Pure testcases (no fixture).
%%%===================================================================
expire(_Config) ->
    IP = {1,2,3,4},
    ?assertEqual([], ?M:expire_and_get_requests(IP, #{}, 1000, 1)),
    ?assertEqual([1], ?M:drop_expired([1], 1000, 500)),
    ?assertEqual([1], ?M:expire_and_get_requests(IP, #{IP => [1]}, 1000, 500)),
    ?assertEqual([1, 500], ?M:expire_and_get_requests(IP, #{IP => [1, 500]}, 1000, 501)),
    ?assertEqual([500, 501], ?M:expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1100)),
    ?assertEqual([500, 501], ?M:expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1499)),
    ?assertEqual([501], ?M:expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1500)),
    ?assertEqual([], ?M:expire_and_get_requests(IP, #{IP => [1, 500, 501]}, 1000, 1501)),
    ok.

add_and_order(_Config) ->
    ?assertEqual([5], ?M:add_and_order_timestamps(5, [])),
    ?assertEqual([1,2,3,4,5], ?M:add_and_order_timestamps(5, [1,2,3,4])),
    ?assertEqual([1,2,3,4,5,6,7], ?M:add_and_order_timestamps(5, [1,2,3,4,6,7])),
    ?assertEqual([5,7,8], ?M:add_and_order_timestamps(5, [7,8])),
    ok.

cleanup_timestamps_map(_Config) ->
    IP1 = {1,2,3,4},
    IP2 = {2,3,4,5},
    ?assertEqual(
       #{IP1 => [1],
         IP2 => [500]
        }, ?M:cleanup_expired_sliding_peers(
              #{IP1 => [1],
                IP2 => [500]}, 1000, 501)),
    ?assertEqual(
       #{
         IP2 => [500]
        }, ?M:cleanup_expired_sliding_peers(
              #{IP1 => [1],
                IP2 => [500]}, 1000, 1100)),
    Empty = ?M:cleanup_expired_sliding_peers(
               #{IP1 => [1],
                 IP2 => [500]}, 1000, 2100),
    %% Now it's empty
    ?assertEqual(0, maps:size(Empty)),
    ok.

noproc(_Config) ->
    %% process shouldn't be running
    ok = meck:new([prometheus_counter, prometheus_histogram, arweave_config], [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_histogram, observe, 3, ok),
    ok = meck:expect(arweave_config, get, 1, 1),
    ?assertEqual({reject, error, #{}}, ?M:register_or_reject_call(?TEST_LIMITER, {1,2,3,4})),
    meck:unload([prometheus_counter, prometheus_histogram, arweave_config]),
    ok.

%%%===================================================================
%%% Stateful testcases (limiter started in init_per_testcase).
%%%===================================================================
timeout(_Config) ->
    ?assertEqual({reject, error, #{}}, ?M:register_or_reject_call(?TEST_LIMITER, {1,2,3,4})),
    ok.

timeout_setup(Config) ->
    BeforeApps = application:which_applications(),

    ?TABLE = ets:new(?TABLE, [named_table, public]), %% This is not used, but I don't
    %% want to complicate cleanup
    {module, arweave_limiter_time} = code:ensure_loaded(arweave_limiter_time),

    application:ensure_all_started(arweave_config),

    put({?MODULE, snapshot}, arweave_config:snapshot()),

    set_if_defined(number_of_workers, Config),
    set_if_defined(no_limit, Config),
    set_if_defined(is_manual_reduction_disabled, Config),
    set_if_defined(leaky_tick_ms, Config),
    set_if_defined(timestamp_cleanup_expiry, Config),
    set_if_defined(leaky_rate_limit, Config),
    set_if_defined(concurrency_limit, Config),
    set_if_defined(tick_reduction, Config),
    set_if_defined(sliding_window_duration, Config),
    set_if_defined(sliding_window_limit, Config),

    arweave_config:set([limiter, ?TEST_LIMITER], Config),

    ok = meck:new(prometheus_counter, [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_counter, inc, 3, ok),

    ok = meck:new(prometheus_histogram, [passthrough]),
    ok = meck:expect(prometheus_histogram, observe, 3, ok),

    ok = meck:new(arweave_limiter_time, []),
    %% ts_now() is called in each register_or_reject call, we are
    %% going to delaying it beyond the gen_server:call timeout.
    ok = meck:expect(arweave_limiter_time, ts_now,
                     fun() ->
                             timer:sleep(?CALL_TIMEOUT + 1000),
                             0
                     end),
    0 = arweave_limiter_time:ts_now(),

    {ok, LimiterPID} = ?M:start_link(?TEST_LIMITER_0, ?TEST_LIMITER),
    {LimiterPID, BeforeApps}.

setup(Config) ->
    BeforeApps = application:which_applications(),

    ?TABLE = ets:new(?TABLE, [named_table, public]),
    ?setTSMock(0),

    application:ensure_all_started(arweave_config),

    put({?MODULE, snapshot}, arweave_config:snapshot()),

    set_if_defined(number_of_workers, Config),
    set_if_defined(no_limit, Config),
    set_if_defined(is_manual_reduction_disabled, Config),
    set_if_defined(leaky_tick_ms, Config),
    set_if_defined(timestamp_cleanup_expiry, Config),
    set_if_defined(leaky_rate_limit, Config),
    set_if_defined(concurrency_limit, Config),
    set_if_defined(tick_reduction, Config),
    set_if_defined(sliding_window_duration, Config),
    set_if_defined(sliding_window_limit, Config),

    {module, arweave_limiter_time} = code:ensure_loaded(arweave_limiter_time),

    ok = meck:new(prometheus_counter, [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_counter, inc, 3, ok),

    ok = meck:new(prometheus_histogram, [passthrough]),
    ok = meck:expect(prometheus_histogram, observe, 3, ok),

    ok = meck:new(arweave_limiter_time, []),
    ok = meck:expect(arweave_limiter_time, ts_now,
                     fun() ->
                             [{?KEY, Value}] = ets:lookup(?TABLE, ?KEY),
                             Value
                     end),
    0 = arweave_limiter_time:ts_now(),

    {ok, LimiterPID} = ?M:start_link(?TEST_LIMITER_0, ?TEST_LIMITER),
    {LimiterPID, BeforeApps}.

cleanup(_Config, {_LimiterPID, BeforeApps}) ->
    true = meck:validate(prometheus_counter),
    true = meck:validate(prometheus_histogram),
    true = meck:validate(arweave_limiter_time),
    ok = meck:unload([prometheus_counter,
                      prometheus_histogram,
                      arweave_limiter_time]),
    ?M:stop(?TEST_LIMITER_0),

    [application:stop(App) || App <- (application:which_applications() -- BeforeApps)],
    true = ets:delete(?TABLE),
    arweave_config:restore(erase({?MODULE, snapshot})),
    ok.

simple_sliding_happy(_Config) ->
    IP = {1,2,3,4},

    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding, _}, IP, 1),
    Caller1 ! done,

    timer:sleep(100),
    Info1 = ?M:info(?TEST_LIMITER),
    ?assertMatch(#{sliding_timestamps := #{IP := [1]}}, Info1),
    #{concurrent_monitors := ConcurrentMonitors1} = Info1,
    ?assertEqual(0, maps:size(ConcurrentMonitors1)),

    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding, _}, IP, 500),
    Caller2 ! done,

    timer:sleep(100),
    Info2 = ?M:info(?TEST_LIMITER),
    ?assertMatch(#{sliding_timestamps := #{IP := [1,500]}}, Info2),
    #{concurrent_monitors := ConcurrentMonitors2} = Info2,
    ?assertEqual(0, maps:size(ConcurrentMonitors2)),

    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding, _}, IP, 2000),
    Caller3 ! done,
    timer:sleep(100),
    %% 2 previous ts expired due to the time elapsed.
    ?assertMatch(#{sliding_timestamps := #{IP := [2000]}}, ?M:info(?TEST_LIMITER)),

    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding, _}, IP, 2001),
    Caller4 ! done,
    timer:sleep(100),
    ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, ?M:info(?TEST_LIMITER)),

    Caller5 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _} , IP, 2002),
    Caller5 ! done,
    timer:sleep(100),
    %% Wait a bit for surely have request processed, and observe, no new timestamp
    ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, ?M:info(?TEST_LIMITER)),
    ok.

simple_leaky_happy_path(Config) ->
    LimiterPID = ?config(limiter_pid, Config),
    IP = {1,2,3,4},
    %% init state, the ip is not blocked
    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 0),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 2),

    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 2, ?M:info(?TEST_LIMITER)),

    Caller1 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 1, ?M:info(?TEST_LIMITER)),

    Caller2 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% Keys deleted
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 0, ?M:info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPID ! {tick, leaky_bucket_reduction},

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 0, ?M:info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 0}} when map_size(Monitors) == 0, ?M:info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    ?assertMatch(
       #{concurrent_monitors := Monitors,
         leaky_tokens := LeakyTokens}
       when map_size(Monitors) == 0 andalso map_size(LeakyTokens) == 0,
            ?M:info(?TEST_LIMITER)),
    ok.

rate_limiter_rejected_due_concurrency(Config) ->
    LimiterPID = ?config(limiter_pid, Config),
    %% init state, the ip is not blocked
    IP = {1,2,3,4},

    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, -1),
    timer:sleep(120),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 10),
    timer:sleep(120),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, concurrency, _Data}, IP, 20),

    %% wait a bit so they are surely started.
    timer:sleep(100),

    #{concurrent_monitors := Monitors0,
      leaky_tokens := Leaky0} = ?M:info(?TEST_LIMITER),
    ?assertEqual(2, maps:size(Monitors0)),
    ?assertMatch(#{IP := 2}, Leaky0),

    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% Keys deleted
    %% NOTE: concurrent_monitors := #{} matches to any map, so we don't what's in there.
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 0, ?M:info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 0, ?M:info(?TEST_LIMITER)),

    %% Concurrency reduced, one handler terminated, will register again
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 0),
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    Caller4 ! done,
    %% Keys deleted
    ?assertMatch(#{concurrent_monitors := #{},
                   leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),


    %% manually trigger two ticks.
    LimiterPID ! {tick, leaky_bucket_reduction},
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_monitors := #{},
                   leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

    %% manually trigger a tick.
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    #{concurrent_monitors := ConcurrentMonitors,
      leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
    ?assertEqual(0, maps:size(ConcurrentMonitors)),
    ?assertEqual(0, maps:size(LeakyTokens)),
    ok.

rejected_due_leaky_rate(Config) ->
    LimiterPID = ?config(limiter_pid, Config),
    %% init state, the ip is not blocked
    IP = {1,2,3,4},

    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 1),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 2),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 3),


    %% 2 concurrent, 2 token
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 2, ?M:info(?TEST_LIMITER)),

    %% Simulate a tick
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% wait a tiny bit so the logic surely runs.

    %% 2 concurrent, but tokens reduced.
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 2, ?M:info(?TEST_LIMITER)),

    %% Tokens reduced, will register again
    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 10),

    %% 3 concurrent, 2 tokens
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 3, ?M:info(?TEST_LIMITER)),

    %% manually trigger two ticks.
    LimiterPID ! {tick, leaky_bucket_reduction},
    LimiterPID ! {tick, leaky_bucket_reduction},

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 0}} when map_size(Monitors) == 3, ?M:info(?TEST_LIMITER)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    Caller4 ! done,

    LimiterPID ! {tick, leaky_bucket_reduction},
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    LimiterPID ! {tick, leaky_bucket_reduction},

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    #{concurrent_monitors := ConcurrentMonitors,
      leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
    ?assertEqual(0, maps:size(ConcurrentMonitors)),
    ?assertEqual(0, maps:size(LeakyTokens)),
    ok.

both_exhausted(Config) ->
    LimiterPID = ?config(limiter_pid, Config),
    IP = {1,2,3,4},

    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding, _}, IP, -1),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    %% 1 concurrent, 0 token
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := [_]},
                   leaky_tokens := #{}} when map_size(Monitors) == 1, ?M:info(?TEST_LIMITER)),

    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky, _}, IP, 20),

    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 2 concurrent, but tokens reduced.
    Info = ?M:info(?TEST_LIMITER),
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := [_]},
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 2, Info),

    Caller3 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 130),

    %% Tokens reduced, will register again
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 2 concurrent, 1 token
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := [_]},
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 2, ?M:info(?TEST_LIMITER)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,

    LimiterPID ! {tick, leaky_bucket_reduction},
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    LimiterPID ! {tick, leaky_bucket_reduction},

    %% wait a tiny bit so the tick logic surely runs.
    timer:sleep(100),
    #{concurrent_monitors := ConcurrentMonitors,
      leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
    ?assertEqual(0, maps:size(ConcurrentMonitors)),
    ?assertEqual(0, maps:size(LeakyTokens)),

    ok.

peer_cleanup(Config) ->
    LimiterPID = ?config(limiter_pid, Config),
    %% init state, the ip is not blocked
    IP = {1,2,3,4},

    %% Exhausted 1 sliding window, but has 1 leaky_bucket token available to spend
    Caller1 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, sliding,
                                 #{expiring_limit := 2,
                                   remaining := 1,
                                   reset_seconds := 0,
                                   reset_amount := 1
                                  }}, IP, 1),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    %% 2 concurrent, 2 token
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := [_]},
                   leaky_tokens := #{}} when map_size(Monitors) == 1, ?M:info(?TEST_LIMITER)),

    Caller2 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 2,
                                   remaining := 0,
                                   reset_seconds := 1,
                                    %% In one seconds, we only clean up the sliding timestamps
                                   reset_amount := 1
                                  }}, IP, 20),

    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 2 concurrent, but tokens reduced.
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := [_]},
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 2, ?M:info(?TEST_LIMITER)),

    %% further requests are rejected
    Caller3 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {reject, concurrency,
                                 #{reset_seconds := 1}
                                }, IP, 300),

    %% Tokens reduced, will register again
    %% wait a tiny bit so the logic surely runs.
    timer:sleep(100),
    %% 2 concurrent, 1 token
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := [_]},
                   leaky_tokens := #{IP := 1}} when map_size(Monitors) == 2, ?M:info(?TEST_LIMITER)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    LimiterPID ! {tick, leaky_bucket_reduction},
    %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
    LimiterPID ! {tick, leaky_bucket_reduction},

    %% wait a tiny bit so the tick logic surely runs.
    %% Now we still have timestamps for IP1 in the state.
    timer:sleep(100),
    #{concurrent_monitors := ConcurrentMonitors,
      sliding_timestamps := SlidingTimestamps,
      leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
    ?assertEqual(0, maps:size(ConcurrentMonitors)),
    ?assertEqual(1, maps:size(SlidingTimestamps)),
    ?assertEqual(0, maps:size(LeakyTokens)),

    ?setTSMock(20000),

    timer:sleep(500),
    %% Trigger timestamp cleanup.
    LimiterPID ! {tick, sliding_window_timestamp_cleanup},

    %% wait a tiny bit so the tick logic surely runs.
    %% Now we should have all cleaned up.
    timer:sleep(100),
    #{concurrent_monitors := ConcurrentMonitors,
      sliding_timestamps := SlidingTimestamps2,
      leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
    ?assertEqual(0, maps:size(ConcurrentMonitors)),
    ?assertEqual(0, maps:size(SlidingTimestamps2)),
    ?assertEqual(0, maps:size(LeakyTokens)),

    ok.

leaky_manual_reduction(_Config) ->
    %% init state, the ip is not blocked
    IP = {1,2,3,4},
    NonRecordedIP = {2,3,4,5,1984},

    Caller1 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,remaining := 4,reset_seconds := 99}}, IP, 1),
    Caller2 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,remaining := 3,reset_seconds := 99}}, IP, 20),
    Caller3 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,remaining := 2,reset_seconds := 99}}, IP, 40),
    Caller4 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,remaining := 1,reset_seconds := 99}}, IP, 60),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    %% 4 concurrent, 4 token
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 4}} when map_size(Monitors) == 4, ?M:info(?TEST_LIMITER)),

    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),
    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% call for one that's surely not in the state
    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, NonRecordedIP)),

    %% 4 concurrent, but tokens reduced.
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 2}} when map_size(Monitors) == 4, ?M:info(?TEST_LIMITER)),

    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),
    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% 4 concurrent, but tokens reduced.
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 0}} when map_size(Monitors) == 4 , ?M:info(?TEST_LIMITER)),

    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% 4 concurrent, no change, there is nothing to reduce beyond 0
    ?assertMatch(#{concurrent_monitors := Monitors,
                   leaky_tokens := #{IP := 0}} when map_size(Monitors) == 4, ?M:info(?TEST_LIMITER)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    Caller4 ! done,

    ok.

sliding_manual_reduction(_Config) ->
    %% init state, the ip is not blocked
    IP = {1,2,3,4},
    NonRecordedIP = {2,3,4,5,1984},

    Caller1 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, sliding,
                                 #{expiring_limit := 5,remaining := 4,reset_seconds := 0}}, IP, 1),
    Caller2 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, sliding,
                                 #{expiring_limit := 5,remaining := 3,reset_seconds := 1}}, IP, 20),
    Caller3 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, sliding,
                                 #{expiring_limit := 5,remaining := 2,reset_seconds := 1}}, IP, 40),
    Caller4 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, sliding,
                                 #{expiring_limit := 5,remaining := 1,reset_seconds := 1}}, IP, 60),

    %% wait a bit so they are surely started.
    timer:sleep(100),
    %% 4 concurrent, 4 timestamps
   ?assertMatch(
         #{concurrent_monitors := Monitors,
           sliding_timestamps := #{IP := SlidingTimestamps},
           leaky_tokens := LeakyTokens} when
        map_size(Monitors) == 4 andalso
        length(SlidingTimestamps) == 4 andalso
        map_size(LeakyTokens) == 0,
        ?M:info(?TEST_LIMITER)),

    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),
    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% call for one that's surely not in the state
    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, NonRecordedIP)),

    ?assertMatch(
         #{concurrent_monitors := Monitors,
           sliding_timestamps := #{IP := SlidingTimestamps},
           leaky_tokens := LeakyTokens} when
        map_size(Monitors) == 4 andalso
        length(SlidingTimestamps) == 4 andalso
        map_size(LeakyTokens) == 0,
        ?M:info(?TEST_LIMITER)),

    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),
    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% 4 concurrent, but timestamps reduced.
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := SlidingTimestamps},
                   leaky_tokens := LeakyTokens}
                 when map_size(Monitors) == 4 andalso
                      length(SlidingTimestamps) == 4 andalso
                      map_size(LeakyTokens) == 0, ?M:info(?TEST_LIMITER)),

    ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% 4 concurrent, no change, there is nothing to reduce beyond 0
    ?assertMatch(#{concurrent_monitors := Monitors,
                   sliding_timestamps := #{IP := SlidingTimestamps},
                   leaky_tokens := LeakyTokens}
                 when map_size(Monitors) == 4 andalso
                      length(SlidingTimestamps) == 4 andalso
                      map_size(LeakyTokens) == 0,
                      ?M:info(?TEST_LIMITER)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    Caller4 ! done,

    ok.

leaky_manual_reduction_disabled(Config) ->
    LimiterConfig = #{id := ID} = ?config(limiter_config, Config),
    %% init state, the ip is not blocked
    IP = {1,2,3,4},

    Policies = ?M:generate_policy(LimiterConfig#{id := atom_to_list(ID)}),

    Caller1 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,
                                   remaining      := 4,
                                   reset_seconds  := 99,
                                   reset_amount   := 1,
                                   policies       := Policies}
                                }, IP, 1),
    ?assertEqual(1, arweave_limiter_time:ts_now()),
    Caller2 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,
                                   remaining      := 3,
                                   reset_seconds  := 99,
                                   reset_amount   := 1, %% Reduction is 1.
                                   policies       := Policies}
                                }, IP, 20),
    ?assertEqual(20, arweave_limiter_time:ts_now()),
    Caller3 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,
                                   remaining      := 2,
                                   reset_seconds  := 95,
                                   reset_amount   := 1,
                                   policies       := Policies}
                                }, IP, 4001),
    ?assertEqual(4001, arweave_limiter_time:ts_now()),
    Caller4 = ?assertHandlerRegisterOrRejectCall(
                 ?TEST_LIMITER, {register, leaky,
                                 #{expiring_limit := 5,
                                   remaining      := 1,
                                   reset_seconds  := 89,
                                   reset_amount   := 1,
                                   policies       := Policies}
                                }, IP, 10001),
    ?assertEqual(10001, arweave_limiter_time:ts_now()),

    ?assertMatch(#{concurrent_monitors := Monitors0,
                   leaky_tokens := #{IP := 4}} when map_size(Monitors0) == 4, ?M:info(?TEST_LIMITER)),

    ?assertEqual(disabled, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% Didn't reduce anything
    ?assertMatch(#{concurrent_monitors := Monitors1,
                   leaky_tokens := #{IP := 4}} when map_size(Monitors1) == 4, ?M:info(?TEST_LIMITER)),

    %% We can repeat this, but still disabled
    ?assertEqual(disabled, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

    %% Clean up
    Caller1 ! done,
    Caller2 ! done,
    Caller3 ! done,
    Caller4 ! done,

    ok.
