-module(arweave_limiter_group_load_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_group).
-define(TEST_LIMITER, test_limiter).

%% SETUP & CLEANUP
setup(Config) ->
    %?TABLE = ets:new(?TABLE, [named_table, public]),
    %?setTsMock(0),
    %{module, arweave_limiter_time} = code:ensure_loaded(arweave_limiter_time),

    ok = meck:new(prometheus_counter, [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_counter, inc, 3, ok),

    %% ok = meck:new(arweave_limiter_time, []),
    %% ok = meck:expect(arweave_limiter_time, ts_now,
    %%                  fun() ->
    %%                          [{?KEY, Value}] = ets:lookup(?TABLE, ?KEY),
    %%                          Value
    %%                  end),
    %% 0 = arweave_limiter_time:ts_now(),
    {ok, LimiterPid} = ?M:start_link(?TEST_LIMITER, Config),
    ok, CounterPid = spawn_link(fun() -> counter_loop(0, 0, 0) end),
    {LimiterPid, CounterPid}.

cleanup(_Config, {_LimiterPid, CounterPid}) ->
    %true = meck:validate(arweave_limiter_time),
    true = meck:validate(prometheus_counter),
    ok = meck:unload([prometheus_counter]),% arweave_limiter_time]),
    ?M:stop(?TEST_LIMITER),
    CounterPid ! done,
%    true = ets:delete(?TABLE),
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
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
         leaky_tick_ms => 30000},
       fun leaky_only_single_peer/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
         leaky_tick_ms => 30000},
       fun leaky_only_multi_peer/2},
      {#{id => ?TEST_LIMITER,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
        leaky_tick_ms => 300000},
       fun leaky_only_lot_of_peer_lot_of_calls_each/2}
     ]}.


leaky_only_single_peer(_Config, {_LimiterPid, CounterPid}) ->
    {"Test with only Leaky bucket enabled, Single peer",
     fun () ->
             Peer = {1,2,3,4},
             TotalCalls = 2000, 
             %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
             %% we operate at lower numbers.
             {Time, Result} = timer:tc(fun() -> spawn_n_calls(CounterPid, Peer, TotalCalls) end),
             ?assert(Result),
             ?debugFmt(">>>> Raised requests in ~p microseconds", [Time]),
             timer:sleep(1000),
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


leaky_only_multi_peer(_Config, {_LimiterPid, CounterPid}) ->
    {"Test with only Leaky bucket enabled, 2000 Peer, each sending a single call",
     fun () ->
             Peer = {1,2,3,1},
             TotalCalls = 2000, 
             %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
             %% we operate at lower numbers.
             {Time, Result} = timer:tc(fun() -> spawn_n_calls_n_peers(CounterPid, Peer, TotalCalls) end),
             ?assert(Result),
             ?debugFmt(">>>> Raised requests in ~p microseconds", [Time]),
             timer:sleep(1000),
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

leaky_only_lot_of_peer_lot_of_calls_each(_Config, {_LimiterPid, CounterPid}) ->
    {timeout, 300,
     {"Test with only Leaky bucket enabled, Many peers send many calls",
      fun () ->
              Peer = {1,2,3,1},
              TotalPeers = 2000,
              CallsPeer = 200,
              %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
              %% we operate at lower numbers.
              {Time, Result} = 
                  timer:tc(fun() -> 
                                   spawn_n_calls_per_peers(CounterPid, Peer, TotalPeers, CallsPeer)
                           end),
              ?assert(Result),
              timer:sleep(2000), 
              %% Don't need to wait more than 2seconds. If there were blocked processes, they would have timed
              %% out already.
              ?debugFmt(">>>> Raised requests in ~p microseconds >>> Waiting to finish spamming", [Time]),
              CounterPid ! {get, self()},
              receive 
                  {Reg, Rej, Err} ->
                      ?assertEqual(TotalPeers * CallsPeer, Reg+Rej+Err),
                      ?assertEqual(400000, Reg),
                      ?assertEqual(0, Err)
              after 200 ->
                      exit(timeout)
              end,
              Red = process_info(whereis(?TEST_LIMITER), [reductions]),
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

spawn_n_calls_per_peers(CounterPid, Peer, N, CallsPeer) when N =< 0 ->
    true;
spawn_n_calls_per_peers(CounterPid, {A, B, C, D} = Peer, N, CallsPeer) ->
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
