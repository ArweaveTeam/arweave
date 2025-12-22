-module(ar_limiter_metrics_collector_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/ar.hrl").

-define(M, ar_limiter_metrics_collector).
-define(S, ar_limiter_sup).
-define(L, ar_limiter).
-define(ME, ar_metrics).

-define(GENERAL, general).
-define(METRICS, metrics).

%% FIXME:this IS copied from ar_limiter_tests, either customise it, or move to shared library
-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer, Now),
 	((fun () ->
                  spawn_link(fun() ->
                                     ?assertMatch(
                                        Pattern,
                                        ?L:register_or_reject_call(LimiterRef, Peer, Now)),
                                     receive
                                         done -> ok
                                     end
                             end)
	  end)())).

do_setup() ->
    Configs = [#{id => ?GENERAL,
                 leaky_rate_limit => 50,
                 concurrency_limit => 20,
                 sliding_window_limit => 0,
                 tick_interval_ms => 1000000},
               #{id => ?METRICS,
                 leaky_rate_limit => 50,
                 concurrency_limit => 20,
                 sliding_window_limit => 0,
                 tick_interval_ms => 1000000}
               ],
    {ok, Sup} = ?S:start_link(Configs),
    {ok, Apps} = application:ensure_all_started(prometheus),
    ?ME:register(),
    {Sup, Apps, []}.

do_setup_with_data() ->
    {Sup, Apps, _Callers} = do_setup(),
    IP1 = {1,2,3,4},
    IP2 = {2,3,4,5},
    Caller1 = ?assertHandlerRegisterOrRejectCall(?GENERAL, register, IP1, 0),
    Caller2 = ?assertHandlerRegisterOrRejectCall(?GENERAL, register, IP1, 20),
    Caller3 = ?assertHandlerRegisterOrRejectCall(?GENERAL, register, IP1, 30),
    Caller4 = ?assertHandlerRegisterOrRejectCall(?GENERAL, register, IP2, 40),
    Caller5 = ?assertHandlerRegisterOrRejectCall(?GENERAL, register, IP2, 45),
    Caller6 = ?assertHandlerRegisterOrRejectCall(?METRICS, register, IP1, 60),
    timer:sleep(100),

    {Sup, Apps, [Caller1, Caller2, Caller3, Caller4, Caller5, Caller6]}.

cleanup({Sup, Apps, Callers}) ->
    [Caller ! done || Caller <- Callers],
    timer:sleep(50),
    unlink(Sup),
    exit(Sup, shutdown),
    %% FIXME: DO WE NEED TO CLEAR THE METRICS? DO THEY LEAK INTO OTHER TESTS?
    [application:stop(App) || App <- Apps],
    ok.

empty_limiters_sanity_check_test_() ->
    {
     setup,
     fun do_setup/0,
     fun cleanup/1,
     fun({_Sup, _Apps, _Callers}) ->
             [fun() ->
                      ?assertMatch(
                         [{ar_limiter_tracked_items_total,gauge,
                           "tracked requests, timestamps, leaky tokens",
                           _},
                          {ar_limiter_peers,gauge,[],_}], ?M:metrics())
              end]
     end
    }.


rate_limiter_happy_path_sanity_check_test_() ->
    {
     setup,
     fun do_setup_with_data/0,
     fun cleanup/1,
     fun({_Sup, _Apps, _Callers}) ->
             [fun() ->
                      ?assertMatch(
                         [{ar_limiter_tracked_items_total,gauge,
                           "tracked requests, timestamps, leaky tokens",
                           _},
                          {ar_limiter_peers,gauge,[],_}], ?M:metrics())
                         
              end]
     end}.
