-module(arweave_limiter_metrics_collector_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_metrics_collector).
-define(S, arweave_limiter_sup).
-define(L, arweave_limiter).
-define(ME, arweave_limiter_metrics).

-define(GENERAL, general_test).
-define(METRICS, metrics_test).

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
    %% It would be tempting to just use what the node has started already,
    %% but we need to start new limiters to control the config, and make
    %% sure these tests don't break with only config change.
    %% It is especially important to increase the interval for the tests.
    Configs = [#{id => ?GENERAL,
                 number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
                 leaky_rate_limit => 50,
                 concurrency_limit => 150,
                 sliding_window_limit => 100,
                 leaky_tick_interval_ms => 1000000},
               #{id => ?METRICS,
                 number_of_workers => ?DEFAULT_ARWEAVE_LIMITER_GROUP_WORKERS,
                 leaky_rate_limit => 50,
                 concurrency_limit => 150,
                 sliding_window_limit => 100,
                 leaky_tick_interval_ms => 1000000}
               ],

    meck:new(arweave_limiter_config,[passthrough]),
    meck:expect(arweave_limiter_config, get_config, 0, Configs),

    lists:foreach(fun(Config) ->
                          Children = ?S:children_spec_per_group(Config),
                          lists:foreach(
                            fun(ChildSpec) ->
                                    {ok, _LimPid} = supervisor:start_child(?S, ChildSpec)
                            end, Children)

                  end, Configs),
    [].

do_setup_with_data() ->
    do_setup(),
    %% Generate IP tuples (up to like 16k peers), but any term can be a peer ID.
    Port = 1984,
    IPs = [{1,2,X div 128, X rem 128, Port} || X <- lists:seq(1, 1000)],

    Callers = lists:foldl(fun(IP, Acc) ->
                                  Acc ++ [?assertHandlerRegisterOrRejectCall(?GENERAL, {register, _}, IP) ||
                                             _ <- lists:seq(1,150)]
                          end, [], IPs),
    timer:sleep(500),

    Callers.

cleanup(Callers) ->
    [Caller ! done || Caller <- Callers],
    timer:sleep(1000),
    meck:unload(arweave_limiter_config),
    Children = supervisor:which_children(?S),
    lists:foreach(
      fun({Id, _Pid, _Type, _Modules}) ->
              supervisor:terminate_child(?S, Id),
              supervisor:delete_child(?S, Id)
      end,
      Children
     ),
    ok.

empty_limiters_sanity_check_test_() ->
    {
     setup,
     fun do_setup/0,
     fun cleanup/1,
     fun(_Callers) ->
             [fun() ->
                      ?assertMatch(
                         [{ar_limiter_tracked_items_total,gauge,
                           "tracked requests, timestamps, leaky tokens",
                           _},
                          {ar_limiter_peers,gauge,
                           "The number of peers the limiter is monitoring currently", _}], ?M:metrics())
              end]
     end
    }.


rate_limiter_happy_path_sanity_check_test_() ->
    {
     setup,
     fun do_setup_with_data/0,
     fun cleanup/1,
     fun(_Callers) ->
             [fun() ->
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
                          {[{limiter_id, ?GENERAL}, {limiting_type, sliding_window_timestamps}], 100*1000}
                         ], ?M:tracked_items([{?GENERAL, Info}])),
                      ?assertMatch(
                         [
                          {[{limiter_id, ?GENERAL}, {limiting_type, concurrency}], 1000},
                          {[{limiter_id, ?GENERAL}, {limiting_type, leaky_bucket_tokens}], 1000},
                          {[{limiter_id, ?GENERAL}, {limiting_type, sliding_window_timestamps}], 1000}
                         ], ?M:peers([{?GENERAL, Info}]))
              end]
     end}.
