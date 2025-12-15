-module(arweave_limiter_group_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_group).
-define(TABLE, eunit_arweave_limiter_tests_mock).
-define(KEY, ts_now).
-define(TEST_LIMITER, test_limiter).

-define(setTsMock(Ts), ets:insert(?TABLE, {?KEY, Ts})).

-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer, Now),
 	((fun () ->
                  ?assert(?setTsMock(Now)),
                  spawn_link(fun() ->
                                     ?assertMatch(
                                        Pattern,
                                        ?M:register_or_reject_call(LimiterRef, Peer)),
                                     receive
                                         done -> ok
                                     end
                             end)
	  end)())).

expire_test() ->
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

add_and_order_test() ->
    ?assertEqual([5], ?M:add_and_order_timestamps(5, [])),
    ?assertEqual([1,2,3,4,5], ?M:add_and_order_timestamps(5, [1,2,3,4])),
    ?assertEqual([1,2,3,4,5,6,7], ?M:add_and_order_timestamps(5, [1,2,3,4,6,7])),
    ?assertEqual([5,7,8], ?M:add_and_order_timestamps(5, [7,8])),
    ok.

cleanup_timestamps_map_test() ->
    IP1 = {1,2,3,4},
    IP2 = {2,3,4,5},
    ?assertEqual(
       #{IP1 => [1],
         IP2 => [500]
        }, ?M:cleanup_expired_sliding_peers(
              #{IP1 => [1],
                IP2 => [500]}, 1000, 501)),
    ?assertEqual(
       #{%%IP1 => [1], - removed
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

setup(Config) ->
    ?TABLE = ets:new(?TABLE, [named_table, public]),
    ?setTsMock(0),
    {module, arweave_limiter_time} = code:ensure_loaded(arweave_limiter_time),

    ok = meck:new(prometheus_counter, [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_counter, inc, 3, ok),

    ok = meck:new(arweave_limiter_time, []),
    ok = meck:expect(arweave_limiter_time, ts_now,
                     fun() ->
                             [{?KEY, Value}] = ets:lookup(?TABLE, ?KEY),
                             Value
                     end),
    0 = arweave_limiter_time:ts_now(),
    {ok, LimiterPid} = ?M:start_link(?TEST_LIMITER, Config),
    LimiterPid.

cleanup(_Config, _LimiterPid) ->
    true = meck:validate(arweave_limiter_time),
    true = meck:validate(prometheus_counter),
    ok = meck:unload([prometheus_counter, arweave_limiter_time]),
    ?M:stop(?TEST_LIMITER),
    true = ets:delete(?TABLE),
    ok.

rate_limiter_process_test_() ->
    {foreachx,
     fun setup/1,
     fun cleanup/2,
     [{#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit => 0,
         concurrency_limit => 5,
         sliding_window_limit => 2,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun(_Config, _LimiterPid) -> {"sliding test", fun simple_sliding_happy/0} end},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit => 5,
         concurrency_limit => 2,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun simple_leaky_happy_path/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit=> 5,
         concurrency_limit => 2,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun rate_limiter_rejected_due_concurrency/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit => 2,
         concurrency_limit => 5,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun rejected_due_leaky_rate/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit => 1,
         concurrency_limit => 10,
         sliding_window_limit => 1,
         sliding_window_duration => 100000,
         leaky_tick_interval_ms => 10000000,
         timestamp_cleanup_expiry => 1000,
         timestamp_cleanup_interval_ms => 1000000},
       fun both_exhausted/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit => 1,
         concurrency_limit => 2,
         sliding_window_limit => 1,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun peer_cleanup/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 1,
         leaky_rate_limit => 5,
         concurrency_limit => 10,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun leaky_manual_reduction/2},
      {#{id => ?TEST_LIMITER,
         is_manual_reduction_disabled => true,
         tick_reduction => 1,
         leaky_rate_limit => 5,
         concurrency_limit => 10,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 1000,
         leaky_tick_interval_ms => 100000},
       fun leaky_manual_reduction_disabled/2}
     ]}.

simple_sliding_happy() ->
    IP = {1,2,3,4},

    Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding}, IP, 1),
    Caller1 ! done,

    timer:sleep(100),
    Info1 = ?M:info(?TEST_LIMITER),
    ?assertMatch(#{sliding_timestamps := #{IP := [1]}}, Info1),
    #{concurrent_requests := ConcurrentReqs1} = Info1,
    ?assertEqual(0, maps:size(ConcurrentReqs1)),

    Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding}, IP, 500),
    Caller2 ! done,

    timer:sleep(100),
    Info2 = ?M:info(?TEST_LIMITER),
    ?assertMatch(#{sliding_timestamps := #{IP := [1,500]}}, Info2),
    #{concurrent_requests := ConcurrentReqs2} = Info2,
    ?assertEqual(0, maps:size(ConcurrentReqs2)),

    Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding}, IP, 2000),
    Caller3 ! done,
    timer:sleep(100),
    %% 2 previous ts expired due to the time elapsed.
    ?assertMatch(#{sliding_timestamps := #{IP := [2000]}}, ?M:info(?TEST_LIMITER)),

    Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding}, IP, 2001),
    Caller4 ! done,
    timer:sleep(100),
    ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, ?M:info(?TEST_LIMITER)),

    Caller5 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _} , IP, 2002),
    Caller5 ! done,
    timer:sleep(100),
    %% Wait a bit for surely have request processed, and observe, no new timestamp
    ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, ?M:info(?TEST_LIMITER)),
    ok.

simple_leaky_happy_path(_Config, LimiterPid) ->
    {"Leaky happy path",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := false}, ?M:config(?TEST_LIMITER)),
             IP = {1,2,3,4},
             %% init state, the ip is not blocked
             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 0),
             timer:sleep(20),
             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 2),

             %% wait a bit so they are surely started.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             Caller1 ! done,
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{IP := [_]},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             Caller2 ! done,
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% Keys deleted
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             %% manually trigger a tick.
             LimiterPid ! {tick, leaky_bucket_reduction},

             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

             %% manually trigger a tick.
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

             %% manually trigger a tick.
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
             #{concurrent_requests := ConcurrentReqs,
               leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
             ?assertEqual(0, maps:size(ConcurrentReqs)),
             ?assertMatch(0, maps:size(LeakyTokens)),
             ok
     end}.

rate_limiter_rejected_due_concurrency(_Config, LimiterPid) ->
    {"rejected due concurrency",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := false}, ?M:config(?TEST_LIMITER)),
             %% init state, the ip is not blocked
             IP = {1,2,3,4},

             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, -1),
             timer:sleep(120),
             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 10),
             timer:sleep(120),
             Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, concurrency, _Data}, IP, 10),

             %% wait a bit so they are surely started.
             timer:sleep(100),

             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),


             Caller1 ! done,
             Caller2 ! done,
             Caller3 ! done,
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% Keys deleted
             %% NOTE: concurrent_requests := #{} matches to any map, so we don't what's in there.
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             %% manually trigger a tick.
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

             %% Concurrency reduced, one handler terminated, will register again
             Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 0),
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             Caller4 ! done,
             %% Keys deleted
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),


             %% manually trigger two ticks.
             LimiterPid ! {tick, leaky_bucket_reduction},
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{},
                            leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

             %% manually trigger a tick.
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
             #{concurrent_requests := ConcurrentReqs,
               leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
             ?assertEqual(0, maps:size(ConcurrentReqs)),
             ?assertEqual(0, maps:size(LeakyTokens)),
             ok
     end}.

rejected_due_leaky_rate(_Config, LimiterPid) ->
    {"rejected due leaky rate",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := false}, ?M:config(?TEST_LIMITER)),
             %% init state, the ip is not blocked
             IP = {1,2,3,4},

             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 1),
             timer:sleep(20),
             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 2),
             timer:sleep(20),
             Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 3),

             %% wait a bit so they are surely started.
             timer:sleep(100),
             %% 2 concurrent, 2 token
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             %% Simulate a tick
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% 2 concurrent, but tokens reduced.
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

             %% Tokens reduced, will register again
             Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 10),
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% 3 concurrent, 2 tokens
             ?assertMatch(#{concurrent_requests := #{IP := [_,_,_]},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             %% manually trigger two ticks.
             LimiterPid ! {tick, leaky_bucket_reduction},
             LimiterPid ! {tick, leaky_bucket_reduction},

             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{IP := [_,_,_]},
                            leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

             %% Clean up
             Caller1 ! done,
             Caller2 ! done,
             Caller3 ! done,
             Caller4 ! done,

             LimiterPid ! {tick, leaky_bucket_reduction},
             %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
             LimiterPid ! {tick, leaky_bucket_reduction},

             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             #{concurrent_requests := ConcurrentReqs,
               leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
             ?assertEqual(0, maps:size(ConcurrentReqs)),
             ?assertEqual(0, maps:size(LeakyTokens)),
             ok
     end}.

both_exhausted(_Config, LimiterPid) ->
    {"Both exhausted",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := false}, ?M:config(?TEST_LIMITER)),
             IP = {1,2,3,4},

             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding}, IP, -1),

             %% wait a bit so they are surely started.
             timer:sleep(100),
             %% 1 concurrent, 0 token
             ?assertMatch(#{concurrent_requests := #{IP := [_]},
                            sliding_timestamps := #{IP := [_]},
                            leaky_tokens := #{}}, ?M:info(?TEST_LIMITER)),

             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 20),

             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% 2 concurrent, but tokens reduced.
             Info = ?M:info(?TEST_LIMITER),
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            sliding_timestamps := #{IP := [_]},
                            leaky_tokens := #{IP := 1}}, Info),

             Caller3 = ?assertHandlerRegisterOrRejectCall(
                          ?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 130),

             %% Tokens reduced, will register again
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% 2 concurrent, 1 token
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            sliding_timestamps := #{IP := [_]},
                            leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

             %% Clean up
             Caller1 ! done,
             Caller2 ! done,
             Caller3 ! done,

             LimiterPid ! {tick, leaky_bucket_reduction},
             %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
             LimiterPid ! {tick, leaky_bucket_reduction},

             %% wait a tiny bit so the tick logic surely runs.
             timer:sleep(100),
             #{concurrent_requests := ConcurrentReqs,
               leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
             ?assertEqual(0, maps:size(ConcurrentReqs)),
             ?assertEqual(0, maps:size(LeakyTokens)),

             ok
     end}.

peer_cleanup(_Config, LimiterPid) ->
    {"Peer cleanup",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := false}, ?M:config(?TEST_LIMITER)),
             %% init state, the ip is not blocked
             IP = {1,2,3,4},

             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, sliding}, IP, 1),

             %% wait a bit so they are surely started.
             timer:sleep(100),
             %% 2 concurrent, 2 token
             ?assertMatch(#{concurrent_requests := #{IP := [_]},
                            sliding_timestamps := #{IP := [_]},
                            leaky_tokens := #{}}, ?M:info(?TEST_LIMITER)),

             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 20),

             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% 2 concurrent, but tokens reduced.
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            sliding_timestamps := #{IP := [_]},
                            leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

             %% further requests are rejected
             Caller3 = ?assertHandlerRegisterOrRejectCall(
                          ?TEST_LIMITER, {reject, concurrency, _Data}, IP, 300),

             %% Tokens reduced, will register again
             %% wait a tiny bit so the logic surely runs.
             timer:sleep(100),
             %% 2 concurrent, 1 token
             ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                            sliding_timestamps := #{IP := [_]},
                            leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

             %% Clean up
             Caller1 ! done,
             Caller2 ! done,
             Caller3 ! done,
             LimiterPid ! {tick, leaky_bucket_reduction},
             %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
             LimiterPid ! {tick, leaky_bucket_reduction},

             %% wait a tiny bit so the tick logic surely runs.
             %% Now we still have timestamps for IP1 in the state.
             timer:sleep(100),
             #{concurrent_requests := ConcurrentReqs,
               sliding_timestamps := SlidingTimestamps,
               leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
             ?assertEqual(0, maps:size(ConcurrentReqs)),
             ?assertEqual(1, maps:size(SlidingTimestamps)),
             ?assertEqual(0, maps:size(LeakyTokens)),

             ?setTsMock(20000),

             timer:sleep(500),
             %% Trigger timestamp cleanup.
             LimiterPid ! {tick, sliding_window_timestamp_cleanup},

             %% wait a tiny bit so the tick logic surely runs.
             %% Now we should have all cleaned up.
             timer:sleep(100),
             #{concurrent_requests := ConcurrentReqs,
               sliding_timestamps := SlidingTimestamps2,
               leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
             ?assertEqual(0, maps:size(ConcurrentReqs)),
             ?assertEqual(0, maps:size(SlidingTimestamps2)),
             ?assertEqual(0, maps:size(LeakyTokens)),

             ok
     end}.

leaky_manual_reduction(_Config, _LimiterPid) ->
    {"Leaky tokens manual peer reduction",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := false}, ?M:config(?TEST_LIMITER)),
             %% init state, the ip is not blocked
             IP = {1,2,3,4},
             NonRecordedIP = {2,3,4,5,1984},

             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 1),
             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 20),
             Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 40),
             Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 60),
             %% wait a bit so they are surely started.
             timer:sleep(100),
             %% 2 concurrent, 2 token
             ?assertMatch(#{concurrent_requests := #{IP := [_, _, _, _]},
                            leaky_tokens := #{IP := 4}}, ?M:info(?TEST_LIMITER)),

             ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),
             ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

             %% call for one that's surely not in the state
             ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, NonRecordedIP)),

             %% 2 concurrent, but tokens reduced.
             ?assertMatch(#{concurrent_requests := #{IP := [_, _, _, _]},
                            leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

             ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),
             ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

             %% 4 concurrent, but tokens reduced.
             ?assertMatch(#{concurrent_requests := #{IP := [_, _, _, _]},
                            leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

             ?assertEqual(ok, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

             %% 4 concurrent, no change, there is nothing to reduce beyond 0
             ?assertMatch(#{concurrent_requests := #{IP := [_, _, _, _]},
                            leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

             %% Clean up
             Caller1 ! done,
             Caller2 ! done,
             Caller3 ! done,
             Caller4 ! done,

             ok
     end}.

leaky_manual_reduction_disabled(_Config, _LimiterPid) ->
    {"Leaky tokens manual peer reduction",
     fun() ->
             ?assertMatch(#{is_manual_reduction_disabled := true}, ?M:config(?TEST_LIMITER)),
             %% init state, the ip is not blocked
             IP = {1,2,3,4},

             Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 1),
             Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 20),
             Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 40),
             Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {register, leaky}, IP, 60),
             %% wait a bit so they are surely started.
             timer:sleep(100),
             ?assertMatch(#{concurrent_requests := #{IP := [_, _, _, _]},
                            leaky_tokens := #{IP := 4}}, ?M:info(?TEST_LIMITER)),

             ?assertEqual(disabled, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

             %% Didn't reduce anything
             ?assertMatch(#{concurrent_requests := #{IP := [_, _, _, _]},
                            leaky_tokens := #{IP := 4}}, ?M:info(?TEST_LIMITER)),

             %% We can repeat this, but still disabled
             ?assertEqual(disabled, ?M:reduce_for_peer(?TEST_LIMITER, IP)),

             %% Clean up
             Caller1 ! done,
             Caller2 ! done,
             Caller3 ! done,
             Caller4 ! done,

             ok
     end}.
