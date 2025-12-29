-module(ar_limiter_metrics_collector_tests).

-include_lib("eunit/include/eunit.hrl").

-include("../include/ar.hrl").

-define(M, ar_limiter_metrics_collector).
-define(S, ar_limiter_sup).
-define(L, ar_limiter).
-define(ME, ar_metrics).

-define(GENERAL, general).
-define(METRICS, metrics).

%% Very similar but not identical to ar_limiter_tests macro
-define(assertHandlerRegisterOrRejectCall(LimiterRef, Pattern, Peer),
 	((fun () ->
                  spawn_link(fun() ->
                                     ?assertMatch(
                                        Pattern,
                                        ?L:register_or_reject_call(LimiterRef, Peer)),
                                     receive
                                         done -> ok
                                     end
                             end)
	  end)())).

do_setup() ->
    Configs = [#{id => ?GENERAL,
                 leaky_rate_limit => 50,
                 concurrency_limit => 150,
                 sliding_window_limit => 100,
                 tick_interval_ms => 1000000},
               #{id => ?METRICS,
                 leaky_rate_limit => 50,
                 concurrency_limit => 150,
                 sliding_window_limit => 100,
                 tick_interval_ms => 1000000}
               ],
    {ok, Sup} = ?S:start_link(Configs),
    {ok, Apps} = application:ensure_all_started(prometheus),
    ?ME:register(),
    {Sup, Apps, []}.

do_setup_with_data() ->
    {Sup, Apps, _Callers} = do_setup(),
    %% Generate IP tuples (up to like 16k peers), but any term can be a peer ID.
    IPs = [{1,2,X div 128, X rem 128} || X <- lists:seq(1, 1000)],

    Callers = lists:foldl(fun(IP, Acc) ->
                                  Acc ++ [?assertHandlerRegisterOrRejectCall(?GENERAL, {register, _}, IP) ||
                                             _ <- lists:seq(1,150)]
                          end, [], IPs),
    timer:sleep(500),

    {Sup, Apps, Callers}.

cleanup({Sup, Apps, Callers}) ->
    [Caller ! done || Caller <- Callers],
    timer:sleep(150),
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
