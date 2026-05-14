-module(arweave_limiter_group_load_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_group).
-define(TEST_LIMITER, test_limiter).

%% SETUP & CLEANUP
setup(Config) ->
    ok = meck:new(arweave_limiter_config, [passthrough]),
    ok = meck:expect(arweave_limiter_config, get_config, 0,
                     [#{id => ?TEST_LIMITER,
                        number_of_workers => 1}]),

    ok = meck:new(prometheus_counter, [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_counter, inc, 3, ok),

    {ok, _LimiterPid0} = ?M:start_link(arweave_limiter_test_limiter_0, Config),
    {ok, _LimiterPid1} = ?M:start_link(arweave_limiter_test_limiter_1, Config),
    {ok, _LimiterPid2} = ?M:start_link(arweave_limiter_test_limiter_2, Config),
    {ok, _LimiterPid3} = ?M:start_link(arweave_limiter_test_limiter_3, Config),
    {ok, _LimiterPid4} = ?M:start_link(arweave_limiter_test_limiter_4, Config),
    ok, CounterPid = spawn_link(fun() -> counter_loop(0, 0, 0) end),
    CounterPid.

cleanup(_Config, CounterPid) ->
    ?M:stop(arweave_limiter_test_limiter_0),
    ?M:stop(arweave_limiter_test_limiter_1),
    ?M:stop(arweave_limiter_test_limiter_2),
    ?M:stop(arweave_limiter_test_limiter_3),
    ?M:stop(arweave_limiter_test_limiter_4),
    CounterPid ! done,

    true = meck:validate(prometheus_counter),
    true = meck:validate(arweave_limiter_config),
    ok = meck:unload([prometheus_counter, arweave_limiter_config]),

    ok.

%% Counter
counter_loop(Register, Reject, Error) ->
    receive
        register ->
            counter_loop(Register + 1, Reject, Error);
        reject ->
            counter_loop(Register, Reject + 1, Error);
        error ->
            counter_loop(Register, Reject, Error + 1);
        {get, Caller} ->
            Caller ! {Register, Reject, Error},
            counter_loop(Register, Reject, Error);
        done ->
            ok
    end.

%% TEST IMPLEMENTATION
rate_limiter_process_test_() ->
    {foreachx,
     fun setup/1,
     fun cleanup/2,
     [{#{id => ?TEST_LIMITER,
         number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
         leaky_tick_ms => 30000},
       fun leaky_only_single_peer/2},
      {#{id => ?TEST_LIMITER,
         number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
         leaky_tick_ms => 30000},
       fun leaky_only_multi_peer/2},
      {#{id => ?TEST_LIMITER,
         number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
        leaky_tick_ms => 30000},
       fun leaky_only_lot_of_peer_lot_of_calls_each/2}
     ]}.


leaky_only_single_peer(_Config, CounterPid) ->
    {"Test with only Leaky bucket enabled, Single peer",
     fun () ->
             Peer = {1,2,3,4},
             TotalCalls = 2000,
             %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
             %% we operate at lower numbers.
             {Time, Result} = timer:tc(fun() -> spawn_n_calls(CounterPid, Peer, TotalCalls) end),
             ?assert(Result),
             ?debugFmt(">>>> Raised requests in ~p microseconds", [Time]),
             timer:sleep(?DEFAULT_ARWEAVE_LIMITER_CALL_TIMEOUT + 1000),
             CounterPid ! {get, self()},
             receive
                 {Reg, Rej, Err} ->
                     ?assertEqual(TotalCalls, Reg+Rej+Err),
                     ?assertEqual(450, Reg), %% Since all separate
                     ?assertEqual(0, Err)
             after 200 ->
                     exit(timeout)
             end,
             ok
     end}.

leaky_only_multi_peer(_Config, CounterPid) ->
    {"Test with only Leaky bucket enabled, 2000 Peer, each sending a single call",
     fun () ->
             Peer = {1,2,3,1},
             TotalCalls = 2000,
             %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
             %% we operate at lower numbers.
             {Time, Result} = timer:tc(fun() -> spawn_n_calls_n_peers(CounterPid, Peer, TotalCalls) end),
             ?assert(Result),
             ?debugFmt(">>>> Raised requests in ~p microseconds", [Time]),
             timer:sleep(?DEFAULT_ARWEAVE_LIMITER_CALL_TIMEOUT + 1000),
             CounterPid ! {get, self()},
             receive
                 {Reg, Rej, Err} ->
                     ?assertEqual(TotalCalls, Reg+Rej+Err),
                     %% All different peers, they all send a single request, it's all allowed.
                     ?assertEqual(2000, Reg),
                     ?assertEqual(0, Err)
             after 200 ->
                     exit(timeout)
             end,
             ok
     end}.

leaky_only_lot_of_peer_lot_of_calls_each(_Config, CounterPid) ->
    {timeout, 300,
     {"Test with only Leaky bucket enabled, Many peers send many calls",
      fun () ->
              Peer = {1,2,3,0},
              TotalPeers = 150,
              CallsPeer = 600,
              ?debugFmt("Sending ~p request for each of the ~p peers~n", [CallsPeer, TotalPeers]),
              %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
              %% we operate at lower numbers.
              {Time, Result} =
                  timer:tc(fun() ->
                                   spawn_n_calls_per_peers(CounterPid, Peer, TotalPeers, CallsPeer)
                           end),
              ?assert(Result),
              %% Wait a bit more than the timeout value, so surely all processes finish
              timer:sleep(?DEFAULT_ARWEAVE_LIMITER_CALL_TIMEOUT + 2000),

              ?debugFmt(">>>> Raised requests in ~p microseconds >>> Waiting to finish spamming", [Time]),
              CounterPid ! {get, self()},
              receive
                  {Reg, Rej, Err} ->
                      ?assertEqual(TotalPeers * CallsPeer, Reg+Rej+Err),
                      ?assertEqual({450*TotalPeers, TotalPeers*(CallsPeer - 450), 0}, {Reg, Rej, Err}),
                      ?assertEqual(0, Err)
              after 200 ->
                      exit(timeout)
              end,
              Red = process_info(whereis(arweave_limiter_test_limiter_0), [reductions]),
              ?debugFmt("red: ~p", [Red]),
              ok
      end}}.

%%% HELPERS
spawn_n_calls(_CounterPid, _Peer, N) when N =< 0 ->
    true;
spawn_n_calls(CounterPid, Peer, N) ->
    spawn_call(CounterPid, Peer),
    spawn_n_calls(CounterPid, Peer, N-1).

spawn_n_calls_n_peers(_CounterPid, _Peer, N) when N =< 0 ->
    true;
spawn_n_calls_n_peers(CounterPid, {A, B, C, D} = Peer, N) ->
    spawn_call(CounterPid, Peer),
    spawn_n_calls_n_peers(CounterPid, {A, B, C, D + 1}, N-1).

spawn_n_calls_per_peers(_CounterPid, _Peer, N, _CallsPeer) when N =< 0 ->
    true;
spawn_n_calls_per_peers(CounterPid, {A, B, C, D} = Peer, N, CallsPeer) ->
    (D rem 50) == 0 andalso ?debugFmt("~p clients spawned", [D]),
    spawn_n_calls(CounterPid, Peer, CallsPeer),
    spawn_n_calls_per_peers(CounterPid, {A, B, C, D + 1}, N-1, CallsPeer).


spawn_call(CounterPid, Peer) ->
    spawn_link(fun() ->
                       case ?M:register_or_reject_call(?TEST_LIMITER, Peer) of
                           {reject, error, _Data} ->
                               CounterPid ! error,
                               ok;
                           {reject, _Reason, _Data} ->
                               CounterPid ! reject,
                               ok;
                           {register, _}  ->
                               CounterPid ! register,
                               ok
                       end
               end).
