%% @ar_test: fast
-module(arweave_limiter_metrics_collector_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

-define(M, arweave_limiter_metrics_collector).
-define(S, arweave_limiter_sup).
-define(L, arweave_limiter).
-define(ME, arweave_limiter_metrics).

-define(GENERAL, test_limiter).
-define(METRICS, test_limiter_2).

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
    Overrides = #{leaky_rate_limit => 50,
                  concurrency_limit => 150,
                  sliding_window_limit => 100,
                  leaky_tick_ms => 1000000,
                  number_of_workers => 5},
    put({?MODULE, snapshot}, arweave_config:snapshot()),
    apply_overrides(?GENERAL, Overrides),
    apply_overrides(?METRICS, Overrides),

    lists:foreach(
        fun(GroupID) ->
            Children = ?S:children_spec_per_group(GroupID),
            lists:foreach(
                fun(ChildSpec) ->
                    {ok, _LimPID} = supervisor:start_child(?S, ChildSpec)
                end, Children)
        end, [?GENERAL, ?METRICS]),
    [].

apply_overrides(GroupID, Overrides) ->
    maps:fold(
        fun(Field, Value, ok) ->
            {ok, _} = arweave_config:set(
                [limiter, GroupID, Field], Value),
            ok
        end, ok, Overrides).

do_setup_with_data() ->
    do_setup(),
    %% Generate IP tuples (up to like 16k peers), but any term can be a peer ID.
    Port = 1984,
    IPs = [{1,2,X div 128, X rem 128, Port} || X <- lists:seq(1, 1000)],

    Callers = lists:foldl(fun(IP, Acc) ->
                                  Acc ++ [?assertHandlerRegisterOrRejectCall(?GENERAL, {register, _, _}, IP) ||
                                             _ <- lists:seq(1,150)]
                          end, [], IPs),
    timer:sleep(500),

    Callers.

cleanup(Callers) ->
    [Caller ! done || Caller <- Callers],
    timer:sleep(1000),
    arweave_config:restore(erase({?MODULE, snapshot})),
    Children = supervisor:which_children(?S),
    lists:foreach(
      fun({ID, _PID, _Type, _Modules}) ->
              supervisor:terminate_child(?S, ID),
              supervisor:delete_child(?S, ID)
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
                          {[{limiter_id, ?GENERAL}, {limiting_type, leaky_bucket_tokens}], 1000},
                          {[{limiter_id, ?GENERAL}, {limiting_type, sliding_window_timestamps}], 1000}
                         ], ?M:peers([{?GENERAL, Info}]))
              end]
     end}.
