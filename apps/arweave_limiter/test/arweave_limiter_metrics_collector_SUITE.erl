%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% @copyright 2025 (c) Arweave
%%% @doc Common Test suite for arweave_limiter_metrics_collector.
%%%
%%% Unlike the eunit version, which relied on a fully booted node to
%%% provide prometheus and a running arweave_limiter_sup, this suite
%%% bootstraps the minimum environment it needs in init_per_suite:
%%% the prometheus application, the limiter metric declarations, and
%%% an (initially empty) arweave_limiter_sup.
%%% @end
%%%===================================================================
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
    %catch ?ME:register(),

    %% It would be tempting to just use what the node has started already,
    %% but we need to start new limiters to control the config, and make
    %% sure these tests don't break with only config change.
    %% It is especially important to increase the interval for the tests.
    GroupIDs = [?GENERAL,
                ?METRICS],

    arweave_config:set([limiter, ?GENERAL, number_of_workers], 10),
    arweave_config:set([limiter, ?GENERAL, concurrency_limit], 150000),
    arweave_config:set([limiter, ?METRICS, number_of_workers], 5),

    SupPid = case ?S:start_link(GroupIDs) of
                 {ok, Pid} -> Pid;
                 {error, {already_started, Pid}} -> Pid
             end,

    Callers = case TestCase of
                  rate_limiter_happy_path_sanity_check -> do_setup_with_data();
                  _ -> []
              end,

    [{sup_pid, SupPid}, {before_apps, BeforeApps}, {callers, Callers}] ++ Config.

end_per_testcase(_TestCase, Config) ->
    cleanup(?config(callers, Config)),
    %case ?config(sup_pid, Config) of
    %    undefined -> ok;
    %    SupPid -> catch exit(SupPid, shutdown)
    %end,
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

cleanup(Callers) ->
    [Caller ! done || Caller <- Callers],
    timer:sleep(1000),
                                                %meck:unload(arweave_limiter_config),
    Children = supervisor:which_children(?S),
    lists:foreach(
      fun({Id, _Pid, _Type, _Modules}) ->
              supervisor:terminate_child(?S, Id),
              supervisor:delete_child(?S, Id)
      end,
      Children
     ),
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
