-module(arweave_limiter_metrics_collector_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_metrics_collector).

-define(GENERAL, general).
-define(METRICS, metrics).

%% Very similar but not identical to ar_limiter_tests macro
-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer),
    ((fun () ->
                  spawn_link(fun() ->
                                     ?assertMatch(
                                        Pattern,
                                        arweave_limiter:register_or_reject_call(LimiterRef, Peer)),
                                     receive
                                         done -> ok
                                     end
                             end)
      end)())).

suite() -> [{userdata, [description()]}].

description() -> {description, "arweave_limiter_metrics_collector test interface"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

all() ->
    [
     empty_limiters_sanity_check,
     rate_limiter_happy_path_sanity_check
    ].

init_per_testcase(TestCase, Config) ->
    %% Declare the limiter metrics. Ignore the already-declared case so
    %% the suite is robust to other suites having registered them in the
    %% same Common Test run.

    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    {ok, _Started1} = application:ensure_all_started(prometheus),
    {ok, _Started2} = application:ensure_all_started(arweave_config),

    arweave_config:set([limiter, ?GENERAL, number_of_workers], 10),
    arweave_config:set([limiter, ?GENERAL, concurrency_limit], 150000),
    arweave_config:set([limiter, ?METRICS, number_of_workers], 5),

    ok = arweave_limiter:start(),

    Callers = case TestCase of
                  rate_limiter_happy_path_sanity_check -> do_setup_with_data();
                  _ -> []
              end,

    [{before_apps, AppsBefore}, {callers, Callers}] ++ Config.

end_per_testcase(_TestCase, Config) ->
    drain_callers(?config(callers, Config)),

    AppsBefore = ?config(before_apps, Config),
    AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    AppsStartedForTest = AppsNow -- AppsBefore,
    lists:foreach(fun application:stop/1, AppsStartedForTest),
    ok.

do_setup_with_data() ->
    %% Generate IP tuples (up to like 16k peers), but any term can be a peer ID.
    Port = 1984,
    IPs = [{1,2,X div 128, X rem 128, Port} || X <- lists:seq(1, 1000)],

    Callers = lists:foldl(fun(IP, Acc) ->
                                  Acc ++ [?assertHandlerRegisterOrRejectCall(?GENERAL, {register, _, _}, IP) ||
                                             _ <- lists:seq(1,150)]
                          end, [], IPs),
    timer:sleep(1500),

    Callers.

%% @doc Release every spawned caller so it exits normally.
drain_callers(Callers) ->
    [Caller ! done || Caller <- Callers],
    ok.

empty_limiters_sanity_check(_Config) ->
    ?assertMatch(
       [{ar_limiter_tracked_items_total,gauge,
         "tracked requests, timestamps, leaky tokens",
         _},
        {ar_limiter_peers,gauge,
         "The number of peers the limiter is monitoring currently", _}], ?M:metrics()),
    ok.

rate_limiter_happy_path_sanity_check(_Config) ->
    ?assertMatch(
       [{ar_limiter_tracked_items_total,gauge,
         "tracked requests, timestamps, leaky tokens",
         _},
        {ar_limiter_peers,gauge,
         "The number of peers the limiter is monitoring currently", _}], ?M:metrics()),

    Info = arweave_limiter_group:info(?GENERAL),
    ?assertMatch(
       [
        {[{limiter_id, ?GENERAL}, {limiting_type, concurrency}], 150*1000},
        {[{limiter_id, ?GENERAL}, {limiting_type, leaky_bucket_tokens}], 1000},
        {[{limiter_id, ?GENERAL}, {limiting_type, sliding_window_timestamps}], 0}
       ], ?M:tracked_items([{?GENERAL, Info}])),
    ?assertMatch(
       [
        {[{limiter_id, ?GENERAL}, {limiting_type, leaky_bucket_tokens}], 1000},
        {[{limiter_id, ?GENERAL}, {limiting_type, sliding_window_timestamps}], 0}
       ], ?M:peers([{?GENERAL, Info}])),
    ok.
