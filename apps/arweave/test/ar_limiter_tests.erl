-module(ar_limiter_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/ar.hrl").

-define(M, ar_limiter).
-define(TEST_LIMITER, test_limiter).

-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer, Now),
 	((fun () ->
                  spawn_link(fun() ->
                                     ?assertMatch(
                                        Pattern,
                                        ?M:register_or_reject_call(LimiterRef, Peer, Now)),
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

do_setup(Config) ->
    meck:new(prometheus_counter, []),
    meck:expect(prometheus_counter, inc, 2, ok),
    meck:expect(prometheus_counter, inc, 3, ok),
    {ok, LimiterPid} = ?M:start_link(?TEST_LIMITER, Config),
    LimiterPid.

cleanup(_LimiterPid) ->
    meck:unload(prometheus_counter),
    ?M:stop(?TEST_LIMITER).

rate_limiter_happy_path_0_leaky_tokens_test_() ->
    {setup,
     fun() ->
             %% Start with 0 leaky_rate, reject when sliding window is exhausted.
             do_setup(#{id => ?TEST_LIMITER,
                        leaky_rate_limit => 0,
                        concurrency_limit => 5,
                        sliding_window_limit => 2,
                        sliding_window_duration => 1000,
                        tick_interval_ms => 100000})
     end,
     fun cleanup/1,
     [fun() ->
              IP = {1,2,3,4},

              Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 1),
              Caller1 ! done,

              timer:sleep(100),
              Info1 = ?M:info(?TEST_LIMITER),
              ?assertMatch(#{sliding_timestamps := #{IP := [1]}}, Info1),
              #{concurrent_requests := ConcurrentReqs1} = Info1,
              ?assertEqual(0, maps:size(ConcurrentReqs1)),

              Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 500),
              Caller2 ! done,

              timer:sleep(100),
              Info2 = ?M:info(?TEST_LIMITER),
              ?assertMatch(#{sliding_timestamps := #{IP := [1,500]}}, Info2),
              #{concurrent_requests := ConcurrentReqs2} = Info2,
              ?assertEqual(0, maps:size(ConcurrentReqs2)),

              Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 2000),
              Caller3 ! done,
              timer:sleep(100),
              %% 2 previous ts expired due to the time elapsed.
              ?assertMatch(#{sliding_timestamps := #{IP := [2000]}}, ?M:info(?TEST_LIMITER)),

              Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 2001),
              Caller4 ! done,
              timer:sleep(100),
              ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, ?M:info(?TEST_LIMITER)),

              Caller5 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _} , IP, 2002),
              Caller5 ! done,
              timer:sleep(100),
              %% Wait a bit for surely have request processed, and observe, no new timestamp
              ?assertMatch(#{sliding_timestamps := #{IP := [2000, 2001]}}, ?M:info(?TEST_LIMITER)),
              ok
      end]}.

rate_limiter_happy_path_0_sliding_window_test_() ->
    {
     setup,
     fun() ->
            do_setup(#{id => ?TEST_LIMITER,
                       leaky_rate_limit => 5,
                       concurrency_limit => 2,
                       sliding_window_limit => 0,
                       tick_interval_ms => 100000})
     end,
     fun cleanup/1,
     fun(LimiterPid) ->
             [fun() ->
                      IP = {1,2,3,4},
                      %% init state, the ip is not blocked
                      Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      timer:sleep(20),
                      Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),

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
                      LimiterPid ! {tick, rate_limit},

                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      ?assertMatch(#{concurrent_requests := #{},
                                     leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

                      %% manually trigger a tick.
                      LimiterPid ! {tick, rate_limit},
                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      ?assertMatch(#{concurrent_requests := #{},
                                     leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

                      %% manually trigger a tick.
                      LimiterPid ! {tick, rate_limit},
                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
                      #{concurrent_requests := ConcurrentReqs,
                        leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
                      ?assertEqual(0, maps:size(ConcurrentReqs)),
                      ?assertMatch(0, maps:size(LeakyTokens)),
                      ok
              end]
     end}.

rate_limiter_rejected_due_concurrency_test_() ->
    {
     setup,
     fun() ->
             do_setup(#{id => ?TEST_LIMITER,
                        leaky_rate_limit=> 5,
                        concurrency_limit => 2,
                        sliding_window_limit => 0,
                        tick_interval_ms => 100000})
     end,
     fun cleanup/1,
     fun(LimiterPid) ->
             [fun() ->
                      %% init state, the ip is not blocked
                      IP = {1,2,3,4},

                      Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      timer:sleep(20),
                      Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      timer:sleep(20),
                      Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, concurrency, _Data}, IP, 0),

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
                      LimiterPid ! {tick, rate_limit},
                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      ?assertMatch(#{concurrent_requests := #{},
                                     leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

                      %% Concurrency reduced, one handler terminated, will register again
                      Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      %% wait a tiny bit so the logic surely runs.
                      timer:sleep(100),
                      Caller4 ! done,
                      %% Keys deleted
                      ?assertMatch(#{concurrent_requests := #{},
                                     leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),


                      %% manually trigger two ticks.
                      LimiterPid ! {tick, rate_limit},
                      LimiterPid ! {tick, rate_limit},
                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      ?assertMatch(#{concurrent_requests := #{},
                                     leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

                      %% manually trigger a tick.
                      LimiterPid ! {tick, rate_limit},
                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
                      #{concurrent_requests := ConcurrentReqs,
                        leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
                      ?assertEqual(0, maps:size(ConcurrentReqs)),
                      ?assertMatch(0, maps:size(LeakyTokens)),
                      ok
              end]
     end}.

rate_limiter_rejected_due_leaky_rate_test_() ->
    {
     setup,
     fun() ->
             do_setup(#{id => ?TEST_LIMITER,
                        leaky_rate_limit => 2,
                        concurrency_limit => 5,
                        sliding_window_limit => 0,
                        tick_interval_ms => 100000})
     end,
     fun cleanup/1,
     fun(LimiterPid) ->
             [fun() ->
                      %% init state, the ip is not blocked
                      IP = {1,2,3,4},

                      Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      timer:sleep(20),
                      Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      timer:sleep(20),
                      Caller3 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 0),

                      %% wait a bit so they are surely started.
                      timer:sleep(100),
                      %% 2 concurrent, 2 token
                      ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                                     leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),

                      %% Simulate a tick
                      LimiterPid ! {tick, rate_limit},
                      %% wait a tiny bit so the logic surely runs.
                      timer:sleep(100),
                      %% 2 concurrent, but tokens reduced.
                      ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                                     leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),


                      %% Tokens reduced, will register again
                      Caller4 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 0),
                      %% wait a tiny bit so the logic surely runs.
                      timer:sleep(100),
                      %% 3 concurrent, 2 tokens
                      ?assertMatch(#{concurrent_requests := #{IP := [_,_,_]},
                                     leaky_tokens := #{IP := 2}}, ?M:info(?TEST_LIMITER)),


                      %% manually trigger two ticks.
                      LimiterPid ! {tick, rate_limit},
                      LimiterPid ! {tick, rate_limit},

                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      ?assertMatch(#{concurrent_requests := #{IP := [_,_,_]},
                                     leaky_tokens := #{IP := 0}}, ?M:info(?TEST_LIMITER)),

                      %% Clean up
                      Caller1 ! done,
                      Caller2 ! done,
                      Caller3 ! done,
                      Caller4 ! done,

                      LimiterPid ! {tick, rate_limit},
                      %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
                      LimiterPid ! {tick, rate_limit},

                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      #{concurrent_requests := ConcurrentReqs,
                        leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
                      ?assertEqual(0, maps:size(ConcurrentReqs)),
                      ?assertMatch(0, maps:size(LeakyTokens)),
                      ok
              end]
     end}.


rate_limiter_exhaust_both_sliding_window_and_leaky_bucket_test_() ->
    {
     setup,
     fun() ->
             do_setup(#{id => ?TEST_LIMITER,
                        leaky_rate_limit=> 1,
                        concurrency_limit => 10,
                        sliding_window_limit => 1,
                        tick_interval_ms => 100000})
     end,
     fun cleanup/1,
     fun(LimiterPid) ->
             [fun() ->
                      %% init state, the ip is not blocked
                      IP = {1,2,3,4},

                      Caller1 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 1),

                      %% wait a bit so they are surely started.
                      timer:sleep(100),
                      %% 2 concurrent, 2 token
                      ?assertMatch(#{concurrent_requests := #{IP := [_]},
                                     leaky_tokens := #{}}, ?M:info(?TEST_LIMITER)),

                      Caller2 = ?assertHandlerRegisterOrRejectCall(?TEST_LIMITER, register, IP, 2),

                      %% wait a tiny bit so the logic surely runs.
                      timer:sleep(100),
                      %% 2 concurrent, but tokens reduced.
                      ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                                     leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),



                      Caller3 = ?assertHandlerRegisterOrRejectCall(
                                   ?TEST_LIMITER, {reject, rate_limit, _Data}, IP, 3),

                      %% Tokens reduced, will register again
                      %% wait a tiny bit so the logic surely runs.
                      timer:sleep(100),
                      %% 3 concurrent, 2 tokens
                      ?assertMatch(#{concurrent_requests := #{IP := [_,_]},
                                     leaky_tokens := #{IP := 1}}, ?M:info(?TEST_LIMITER)),

                      %% Clean up
                      Caller1 ! done,
                      Caller2 ! done,
                      Caller3 ! done,
                      LimiterPid ! {tick, rate_limit},
                      %% Key only deleted from leaky_tokens map, when it reached 0 in the previous tick
                      LimiterPid ! {tick, rate_limit},

                      %% wait a tiny bit so the tick logic surely runs.
                      timer:sleep(100),
                      #{concurrent_requests := ConcurrentReqs,
                        leaky_tokens := LeakyTokens} = ?M:info(?TEST_LIMITER),
                      ?assertEqual(0, maps:size(ConcurrentReqs)),
                      ?assertMatch(0, maps:size(LeakyTokens)),

                      ok
              end]
     end}.
