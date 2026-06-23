-module(arweave_limiter_metrics_collector_SUITE).
-export([suite/0, description/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([all/0]).
-export([
         empty_limiters_sanity_check/1,
         rate_limiter_happy_path_sanity_check/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_metrics_collector).
-define(S, arweave_limiter_sup).
-define(L, arweave_limiter).
-define(ME, arweave_limiter_metrics).

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

    BeforeApps = application:which_applications(),
    {ok, _Started1} = application:ensure_all_started(prometheus),
    {ok, _Started2} = application:ensure_all_started(arweave_config),

    %% It would be tempting to just use what the node has started already,
    %% but we need to start new limiters to control the config, and make
    %% sure these tests don't break with only config change.
    %% It is especially important to increase the interval for the tests.
    GroupIDs = [?GENERAL,
                ?METRICS],

    arweave_config:set([limiter, ?GENERAL, number_of_workers], 10),
    arweave_config:set([limiter, ?GENERAL, concurrency_limit], 150000),
    arweave_config:set([limiter, ?METRICS, number_of_workers], 5),

    %% Bind strictly: end_per_testcase stops the (named) supervisor
    %% synchronously, so start_link/1 must return {ok, _} here. Adopting
    %% an already_started sup would silently reuse a stale one.
    {ok, SupPid} = ?S:start_link(GroupIDs),

    Callers = case TestCase of
                  rate_limiter_happy_path_sanity_check -> do_setup_with_data();
                  _ -> []
              end,

    [{sup_pid, SupPid}, {before_apps, BeforeApps}, {callers, Callers}] ++ Config.

end_per_testcase(_TestCase, Config) ->
    drain_callers(?config(callers, Config)),
    %% Synchronously stop the supervisor so its registered name is free
    %% before the next testcase runs. Relying on the asynchronous
    %% parent-exit teardown instead races the next start_link/1, which
    %% then returns {already_started, StalePid}.
    stop_sup(?config(sup_pid, Config)),
    ?ME:cleanup(),
    [application:stop(App) || App <- application:which_applications() -- ?config(before_apps, Config)],
    ok.

do_setup_with_data() ->
    %% Generate IP tuples (up to like 16k peers), but any term can be a peer ID.
    Port = 1984,
    IPs = [{1,2,X div 128, X rem 128, Port} || X <- lists:seq(1, 1000)],

    Callers = lists:foldl(fun(IP, Acc) ->
                                  Acc ++ [?assertHandlerRegisterOrRejectCall(?GENERAL, {register, _, _}, IP) ||
                                             _ <- lists:seq(1,150)]
                          end, [], IPs),
    timer:sleep(500),

    Callers.

%% @doc Release every spawned caller so it exits normally.
drain_callers(Callers) ->
    [Caller ! done || Caller <- Callers],
    ok.

%% @doc Synchronously stop the supervisor started in init_per_testcase.
%% Unlink first so the shutdown exit signal isn't propagated back to the
%% test process, then wait for the DOWN so the registered name is gone
%% before returning. The supervisor's one_for_all/shutdown teardown stops
%% its workers, so no explicit child termination is needed.
stop_sup(SupPid) when is_pid(SupPid) ->
    unlink(SupPid),
    Ref = monitor(process, SupPid),
    exit(SupPid, shutdown),
    receive
        {'DOWN', Ref, process, SupPid, _Reason} ->
            ok
    after 5000 ->
            demonitor(Ref, [flush]),
            exit(SupPid, kill),
            ok
    end.

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
