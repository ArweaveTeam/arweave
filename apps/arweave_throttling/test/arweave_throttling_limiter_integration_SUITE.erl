%%% @doc End-to-end integration between `arweave_limiter' (the
%%% server-side rate limiter) and `arweave_throttling' (the
%%% cooperative client-side throttler).
%%%
%%% Each testcase follows the same lifecycle:
%%% Start config, and set limiter config, start limiter and throttling.
%%% Make requests with throttling, limiting, and updating quota.
%%% Evaluate result either in-flight or collect the results and count different
%%% outcomes.
%%%
%%% @end
-module(arweave_throttling_limiter_integration_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(GROUP_ID, test_limiter).
-define(PEER, {127, 0, 0, 1, 1984}).

-define(CLOCK_KEY, ts_now).
-define(CLOCK_TABLE, arweave_throttling_limiter_integration_clock).
-define(setTSMock(TS), ets:insert(?CLOCK_TABLE, {?CLOCK_KEY, TS})).


-define(
   assertTime(Time, MaxMsPassed), 
   begin
       ((fun () ->
                 case (Time div 1000) of
                     Ms when Ms < MaxMsPassed ->
                         ok;
                     Ms ->
                         erlang:error({assertTimePassed,
                                       [{module, ?MODULE},
                                        {line, ?LINE},
                                        {time_passed, Ms},
                                        {max_ms_passedd, MaxMsPassed}]})
                 end
         end)())
   end).

-define(
   assertShouldFindThrottlingGroup(BoolExpr),
   begin
       ((fun () ->
                 X__T = is_process_alive(self()),  % cheap source of truth
                 case (BoolExpr) of
                     X__T -> ok;
                     X__V -> erlang:error({assert,
                                           [{module, ?MODULE},
                                            {line, ?LINE},
                                            {expression, (??BoolExpr)},
                                            {expected, true},
                                            case not X__T of
                                                X__V -> {value, false};
                                                _ -> {not_boolean, X__V}
                                            end]})
                 end
         end)())
   end).

-define(
   assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer, Now, 
                                     MaxMsPassed, ShouldFindThrottlingGroup),
   begin
       ((fun () ->
                 Parent = self(),
                 ?assert(?setTSMock(Now)),
                 PID = 
                     spawn_link(
                       fun() ->
                               Path = [atom_to_list(LimiterRef)],
                               case arweave_throttling_path:path_to_group_id(Peer, Path) of
                                   {ok, GroupID} ->
                                       ?assertShouldFindThrottlingGroup(ShouldFindThrottlingGroup),
                                       {Time, Value} = 
                                           timer:tc(
                                             fun() ->
                                                     arweave_throttling_group:throttle(GroupID, Peer) 
                                             end),
                                       ?assertMatch(ok, Value),
                                       ?assertTime(Time, MaxMsPassed),
                                       ok;
                                   _ ->
                                       ?assertShouldFindThrottlingGroup(not ShouldFindThrottlingGroup),
                                       ok
                               end,
                               LimiterResult = arweave_limiter_group:register_or_reject_call(
                                                 LimiterRef, Peer),
                               ?assertMatch(Pattern, LimiterResult),
                               Headers = res_to_headers(LimiterResult),
                               ?assertMatch(
                                  ok,
                                  arweave_throttling:update_quota(Peer, Path, Headers)),
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

suite() ->
    [{userdata, [description()]}, {timetrap, {minutes, 2}}].

description() ->
    {description, "arweave_throttling + arweave_limiter integration"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

all() ->
    [
     no_throttling_under_sliding_overflow_with_large_burst,
     do_10_10_none_throttled
    ].

%%% Per-testcase limiter configuration.
limiter_config(no_throttling_under_sliding_overflow_with_large_burst) ->
    BaseConfig = base_config(),
    BaseConfig#{
        sliding_window_limit    => 3,
        sliding_window_duration => 2000,
        leaky_rate_limit        => 45000,
        concurrency_limit       => 500000};
limiter_config(do_10_10_none_throttled) ->
    BaseConfig = base_config(),
    BaseConfig#{
        sliding_window_limit    => 10,
        sliding_window_duration => 1000,
        leaky_rate_limit        => 10,
        concurrency_limit       => 1000000};
limiter_config(tight_limits_throttle_many_requests) ->
    BaseConfig = base_config(),
    BaseConfig#{
        sliding_window_limit    => 1,
        sliding_window_duration => 1000,
        leaky_rate_limit        => 15,
        concurrency_limit       => 1000000}.

base_config() ->
    #{number_of_workers            => 1,
      no_limit                     => false,
      is_manual_reduction_disabled => false,
      %% Disable the automatic leaky/cleanup ticks: the run is driven
      %% purely by the simulated clock, so the leaky bucket behaves as a
      %% fixed-capacity burst (no background drain) over the test window.
      leaky_tick_ms                => 3600000,
      timestamp_cleanup_tick_ms    => 3600000,
      timestamp_cleanup_expiry     => 3600000,
      tick_reduction               => 1}.

init_per_testcase(TestCase, Config) ->
    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],

    ?CLOCK_TABLE = ets:new(?CLOCK_TABLE, [named_table, public]),
    ?setTSMock(0),

    {module, arweave_limiter_time} = code:ensure_loaded(arweave_limiter_time),
    ok = meck:new(arweave_limiter_time, []),
    ok = meck:expect(arweave_limiter_time, ts_now,
                    fun() ->
                            [{?CLOCK_KEY, Value}] = ets:lookup(?CLOCK_TABLE, ?CLOCK_KEY),
                            Value
                    end),

    %% Set config and start both limiter and throttling
    application:ensure_all_started(arweave_config),
    ConfigSnapshot = arweave_config:snapshot(),
    LimiterConfig = limiter_config(TestCase),
    set_limiter_config(?GROUP_ID, LimiterConfig),

    ok = arweave_limiter:start(),
    ok = arweave_throttling:start(),

    [{apps_before, AppsBefore},
     {config_snapshot, ConfigSnapshot},
     {limiter_config, LimiterConfig} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = arweave_config:restore(?config(config_snapshot, Config)),

    AppsBefore = ?config(apps_before, Config),
    AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    lists:foreach(fun application:stop/1, AppsNow -- AppsBefore),

    true = meck:validate(arweave_limiter_time),
    ok = meck:unload(arweave_limiter_time),
    catch ets:delete(?CLOCK_TABLE),
    ok.

%%% Testcases.
%% Make a few request to exhaust sliding windows quota and observe
%% no throttling was applied as switching to leaky bucket.
no_throttling_under_sliding_overflow_with_large_burst(_Config) ->
    ct:pal("Starting load:~n", []),

    Policies = 
        #{id => "test_limiter",
          concurrency => #{limit => 500000},
          leaky_bucket =>
              #{tick_reduction => 1,burst => 45000,
                tick_ms => 3600000},
          sliding_window => #{limit => 3,window_seconds => 2}},
    

    %% 30 requests at 5 req/s, one in flight at a time.
    %Results = run_load(?GROUP_ID, ?PEER, 30, 200, 1),
    Pid0 = ?assertHandlerRegisterOrRejectCall(
             ?GROUP_ID, 
             {register,sliding,
              #{remaining := 2, reset_seconds := 0, expiring_limit := 3, 
                policies := Policies}},
              ?PEER, 0, 0, false),
    Pid0 ! done,
    Pid1 = ?assertHandlerRegisterOrRejectCall(
             ?GROUP_ID, 
             {register,sliding,
              #{remaining := 1, reset_seconds := 1, expiring_limit := 3, 
                policies := Policies}},
              ?PEER, 1, 100, true),
    Pid1 ! done,
    Pid2 = ?assertHandlerRegisterOrRejectCall(
             ?GROUP_ID, 
             {register,sliding,
              #{remaining := 45000, reset_seconds := 1, expiring_limit := 45000, 
                policies := Policies}},
              ?PEER, 2, 100, true),
    Pid2 ! done,
    Pid3 = ?assertHandlerRegisterOrRejectCall(
             ?GROUP_ID, 
             {register,leaky,
              #{remaining := 44999, reset_seconds := 1, expiring_limit := 45000, 
                policies := Policies}},
              ?PEER, 3, 100, true),
    Pid3 ! done,

    ok.

%% Do 10 sliding window, and 10 leaky bucket, none should be throttled.
%% In reality in generates 21 timestamps, and the last one is throttled as it should be.
do_10_10_none_throttled(_Config) ->
    Results = sim_stable_load(?GROUP_ID, ?PEER, 1, 1000, 20), 
    ?assertMatch([], [Timeout || Timeout = {timeout, _} <- Results]),
    OKResults = [OK || {ok, OK} <- Results],
    ?assertEqual(1, length([U || U = {unknown_group, _} <- OKResults])),
    ?assertEqual(19, length([Accepted || Accepted = {accepted, _} <- OKResults])),    
    ok.


%% HELPERS
sim_stable_load(LimiterRef, Peer, StartTS, DurationMs, RPS) ->
    Timestamps = 
        lists:map(fun(TS) -> floor(StartTS + TS) end, 
                   generate_timestamps(DurationMs, RPS, [])),
    lists:map(fun(TS) -> sim_single_call(LimiterRef, Peer, TS) end, lists:sort(Timestamps)).

generate_timestamps(DurationMs, RPS, []) ->
    generate_timestamps(DurationMs, RPS, [0]);
generate_timestamps(DurationMs, RPS, [LastTS| _Rest] = TSs) when LastTS < DurationMs ->
    NewTS = LastTS + 1000/RPS,
    generate_timestamps(DurationMs, RPS, [NewTS|TSs]);
generate_timestamps(_DurationMs, _RPS, TSs) ->
    TSs.

sim_single_call(LimiterRef, Peer, Now) ->
    ?setTSMock(Now),
    Parent = self(),
    PID = 
        spawn_link(
          fun() ->
                  Path = [atom_to_list(LimiterRef)],
                  ThrottleReturn = 
                      case arweave_throttling_path:path_to_group_id(Peer, Path) of
                          {ok, GroupID} ->
                              Name = arweave_throttling_group:registered_name(GroupID),
                              case arweave_throttling_group:try_throttle_call(Name, Peer) of
                                  {queued, Ref} ->
                                      receive
                                          {request_ready, Ref} ->
                                              queued
                                      after 500 ->
                                              gen_server:cast(Name, {cancel_request, Peer, Ref}),
                                              throttle_receive_timeout
                                      end;
                                  Return ->
                                      Return
                              end;
                          _ ->
                              unknown_group
                      end,
                  ct:pal("throttle return:~p :~p~n", [Now, ThrottleReturn]),
                  LimiterResult =
                      arweave_limiter_group:register_or_reject_call(LimiterRef, Peer),
                  Headers = res_to_headers(LimiterResult),
                  ok = arweave_throttling:update_quota(Peer, Path, Headers),
                  Parent ! {call_done, {ThrottleReturn, LimiterResult}},
                  receive
                      done -> ok
                  end
          end),
    receive
        {call_done, {_ThrottleReturn, _LimiterResult} = Result} ->
            %% This looks messy, but we can measure timeouts this way, before terminating the caller.
            PID ! done,
            {ok, Result};
        _ ->
            unexpected_message
    after
        1200 ->
            %% This should never really happen.
            %% The call shouldn't take like a second. And if it crashes,
            %% we expect to spawn_link to take down the test process as well
            {timeout, {throttling_limiter_integration_call, Now}}
    end.

res_to_headers(LimiterResult) ->
    arweave_limiter_http_headers:to_http_headers(LimiterResult).

set_limiter_config(GroupID, ConfigMap) ->
    maps:foreach(
        fun(Field, Value) ->
                ok = arweave_config:set([limiter, GroupID, Field], Value)
        end, ConfigMap).
