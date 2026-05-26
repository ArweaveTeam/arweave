-module(arweave_limiter_group_load_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_group).
-define(TEST_LIMITER, test_limiter).

%% SETUP & CLEANUP
setup(Config) ->
    put({?MODULE, snapshot}, arweave_config:snapshot()),
    maps:fold(
        fun(id, _, ok) -> ok;
           (Field, Value, ok) ->
                {ok, _} = arweave_config:set(
                    [limiter, ?TEST_LIMITER, Field], Value),
                ok
        end, ok, Config),

    ok = meck:new(prometheus_counter, [passthrough]),
    ok = meck:expect(prometheus_counter, inc, 2, ok),
    ok = meck:expect(prometheus_counter, inc, 3, ok),

    {ok, _LimiterPID0} = ?M:start_link(arweave_limiter_test_limiter_0, ?TEST_LIMITER),
    {ok, _LimiterPID1} = ?M:start_link(arweave_limiter_test_limiter_1, ?TEST_LIMITER),
    {ok, _LimiterPID2} = ?M:start_link(arweave_limiter_test_limiter_2, ?TEST_LIMITER),
    {ok, _LimiterPID3} = ?M:start_link(arweave_limiter_test_limiter_3, ?TEST_LIMITER),
    {ok, _LimiterPID4} = ?M:start_link(arweave_limiter_test_limiter_4, ?TEST_LIMITER),
    CounterPID = spawn_link(fun() -> counter_loop(0, 0, 0) end),
    CounterPID.

cleanup(_Config, CounterPID) ->
    ?M:stop(arweave_limiter_test_limiter_0),
    ?M:stop(arweave_limiter_test_limiter_1),
    ?M:stop(arweave_limiter_test_limiter_2),
    ?M:stop(arweave_limiter_test_limiter_3),
    ?M:stop(arweave_limiter_test_limiter_4),
    CounterPID ! done,

    true = meck:validate(prometheus_counter),
    ok = meck:unload([prometheus_counter]),

    arweave_config:restore(erase({?MODULE, snapshot})),
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
         number_of_workers => 5,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
         leaky_tick_ms => 30000},
       fun leaky_only_single_peer/2},
      {#{id => ?TEST_LIMITER,
         number_of_workers => 5,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
         leaky_tick_ms => 30000},
       fun leaky_only_multi_peer/2},
      {#{id => ?TEST_LIMITER,
         number_of_workers => 5,
         tick_reduction => 450,
         leaky_rate_limit => 450,
         concurrency_limit => 500,
         sliding_window_limit => 0,
         sliding_window_duration => 1000,
         timestamp_cleanup_expiry => 2000,
        leaky_tick_ms => 30000},
       fun leaky_only_lot_of_peer_lot_of_calls_each/2}
     ]}.


leaky_only_single_peer(_Config, CounterPID) ->
    {"Test with only Leaky bucket enabled, Single peer",
     fun () ->
             Peer = {1,2,3,4},
             TotalCalls = 2000,
             %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
             %% we operate at lower numbers.
             {Time, Result} = timer:tc(fun() -> spawn_n_calls(CounterPID, Peer, TotalCalls) end),
             ?assert(Result),
             ?debugFmt(">>>> Raised requests in ~p microseconds", [Time]),
             timer:sleep(1000 + 1000),
             CounterPID ! {get, self()},
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

leaky_only_multi_peer(_Config, CounterPID) ->
    {"Test with only Leaky bucket enabled, 2000 Peer, each sending a single call",
     fun () ->
             Peer = {1,2,3,1},
             TotalCalls = 2000,
             %% Spawning 2000 calls, means pretty much 2000 concurrent cowboy processes
             %% we operate at lower numbers.
             {Time, Result} = timer:tc(fun() -> spawn_n_calls_n_peers(CounterPID, Peer, TotalCalls) end),
             ?assert(Result),
             ?debugFmt(">>>> Raised requests in ~p microseconds", [Time]),
             timer:sleep(1000 + 1000),
             CounterPID ! {get, self()},
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

leaky_only_lot_of_peer_lot_of_calls_each(_Config, CounterPID) ->
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
                                   spawn_n_calls_per_peers(CounterPID, Peer, TotalPeers, CallsPeer)
                           end),
              ?assert(Result),
              %% Wait a bit more than the timeout value, so surely all processes finish
              timer:sleep(1000 + 2000),

              ?debugFmt(">>>> Raised requests in ~p microseconds >>> Waiting to finish spamming", [Time]),
              CounterPID ! {get, self()},
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
spawn_n_calls(_CounterPID, _Peer, N) when N =< 0 ->
    true;
spawn_n_calls(CounterPID, Peer, N) ->
    spawn_call(CounterPID, Peer),
    spawn_n_calls(CounterPID, Peer, N-1).

spawn_n_calls_n_peers(_CounterPID, _Peer, N) when N =< 0 ->
    true;
spawn_n_calls_n_peers(CounterPID, {A, B, C, D} = Peer, N) ->
    spawn_call(CounterPID, Peer),
    spawn_n_calls_n_peers(CounterPID, {A, B, C, D + 1}, N-1).

spawn_n_calls_per_peers(_CounterPID, _Peer, N, _CallsPeer) when N =< 0 ->
    true;
spawn_n_calls_per_peers(CounterPID, {A, B, C, D} = Peer, N, CallsPeer) ->
    (D rem 50) == 0 andalso ?debugFmt("~p clients spawned", [D]),
    spawn_n_calls(CounterPID, Peer, CallsPeer),
    spawn_n_calls_per_peers(CounterPID, {A, B, C, D + 1}, N-1, CallsPeer).


spawn_call(CounterPID, Peer) ->
    spawn_link(fun() ->
                       case ?M:register_or_reject_call(?TEST_LIMITER, Peer) of
                           {reject, error, _Data} ->
                               CounterPID ! error,
                               ok;
                           {reject, _Reason, _Data} ->
                               CounterPID ! reject,
                               ok;
                           {register, _}  ->
                               CounterPID ! register,
                               ok
                       end
               end).
