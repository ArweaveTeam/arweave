%%% @doc Tests arweave_throttling API functions when process is not alive
%%%
%%% No matter what we do, there will always be a window when we might have
%%% a Pid of a dead process in the process table.
%%%
%%% Example scenario:
%%% 1) Throttling group process crashes
%%% 2) Supervisor restarts the process
%%% 3) Registry still contains the previous (crashed) process' Pid.
%%% 4) any gen_server:call or cast will refer to this Pid.
%%%
%%% @end
-module(arweave_throttling_noproc_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PEER1, {123,123,123,123}).

-define(PATH_GENERAL, "some/path/that/lead/to/general").
-define(PATH_DATA_SYNC, "data_sync_record").

-define(GROUPID_GENERAL, "general").
-define(GROUPID_DATA_SYNC, "data_sync_record").

%% Note: `blocking_call' waits for an exhausted-quota reset of 29s
%% plus client-side overhead, so the timetrap has to clear that.
suite() -> [{userdata, [description()]}, {timetrap, {seconds, 90}}].

description() ->
    {description, "arweave_throttling_group gen_server missing"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
    AppsBefore = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    ok = arweave_config:start(),
    ConfigSnapshot = arweave_config:snapshot(),
    ok = arweave_throttling:start(),


    %% We pretend we saw the path for the general group before
    PathKey = arweave_throttling_path:path_to_path_key(?PATH_GENERAL),
    arweave_throttling_router:update_path(?PEER1, PathKey, list_to_binary(?GROUPID_GENERAL)),
    %% We get a fully valid process that immediately terminate
    TerminatedPid = spawn(fun() -> ok end),
    true = ets:insert(arweave_throttling_process,
                      {list_to_binary(?GROUPID_GENERAL), TerminatedPid}),

    [{apps_before, AppsBefore}, {config_snapshot, ConfigSnapshot} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = arweave_config:restore(?config(config_snapshot, Config)),
    ok = arweave_throttling:stop(),

    arweave_throttling_metrics:cleanup(),

    AppsBefore = proplists:get_value(apps_before, Config),
    AppsNow = [App || {App, _Desc, _Vsn} <- application:which_applications()],
    AppsStartedForTest = AppsNow -- AppsBefore,
    lists:foreach(fun application:stop/1, AppsStartedForTest),
    catch arweave_metrics:cleanup(),
    ok.


all() ->
    [
     no_groups_started_calls_on_not_injected_process,
     no_groups_started_calls_on_injected_process
    ].

%% TESTCASES

%% @doc No groups started, let's call API functions on a group that we didn't inject
%% a terminated Pid.
no_groups_started_calls_on_not_injected_process(_Config) ->
    %% Supervisor is alive, but has no child processes.
    ?assert(is_pid(whereis(arweave_throttling_sup))),
    ?assertEqual([], supervisor:which_children(arweave_throttling_sup)),
    %% No terminated Pid is present for the group
    ?assertMatch({error, group_not_found}, arweave_throttling_process:get("data_sync_record")),

    %% Not throttled, because there isn't even a throttling process alive
    %% for it yet
    ?assertEqual(false, arweave_throttling:is_throttled(?PEER1, ?PATH_DATA_SYNC)),
    %% Throttling won't create processes, just let the request pass through
    ?assertEqual(ok, arweave_throttling:throttle(?PEER1, ?PATH_DATA_SYNC)),
    %% Still isn't throttled
    ?assertEqual(false, arweave_throttling:is_throttled(?PEER1, ?PATH_DATA_SYNC)),
    %% Update quota will start a new process
    ?assertEqual(ok, arweave_throttling:update_quota(?PEER1, ?PATH_DATA_SYNC, headers(?GROUPID_DATA_SYNC, 3, 2))),
    
    %% One throttling process alive.
    ?assertMatch([{_ID, _Child, _Type, _Modules}], supervisor:which_children(arweave_throttling_sup)),
    ok.

%%% @doc No groups started, let's call API functions on a group that we did(!) inject
%% a terminated Pid.
%% This time we think we found a group, but we have noproc when
%% doing a gen_server:call(Pid,...)
no_groups_started_calls_on_injected_process(_Config) ->
    %% Supervisor is alive, but has no child processes.
    ?assert(is_pid(whereis(arweave_throttling_sup))),
    ?assertEqual([], supervisor:which_children(arweave_throttling_sup)),
    %% No terminated Pid is present for the group
    ?assertMatch({error, group_not_found}, arweave_throttling_process:get("data_sync_record")),

    %% Not throttled, because there isn't even a throttling process alive
    %% for it yet
    ?assertEqual(false, arweave_throttling:is_throttled(?PEER1, ?PATH_GENERAL)),
    %% Throttling won't create processes, just let the request pass through
    ?assertMatch({error, {exit, {noproc, _}}}, arweave_throttling:throttle(?PEER1, ?PATH_GENERAL)),
    %% Still isn't throttled
    ?assertEqual(false, arweave_throttling:is_throttled(?PEER1, ?PATH_GENERAL)),
    %% Update quota will start a new process
    ?assertEqual(ok, arweave_throttling:update_quota(?PEER1, ?PATH_GENERAL, headers(?GROUPID_GENERAL, 3, 2))),
    
    %% No throttling process alive. This might be surprising, but it's good news.
    %% We don't start new processes when update quota gets into this sort of inconsistency.
    ?assertMatch([], supervisor:which_children(arweave_throttling_sup)),

    %% We manually start a process through the supervisor - this would happend
    %% automatically as long as we are within the restart frequency.
    {ok, NewPid} = arweave_throttling_sup:start_throttling_group(?GROUPID_GENERAL),
    ?assertMatch([{?GROUPID_GENERAL, _Child, _Type, _Modules}], supervisor:which_children(arweave_throttling_sup)),
    
    ?assertEqual({ok, NewPid}, arweave_throttling_process:get(?GROUPID_GENERAL)),
    ok.

%% HELPERS
headers(GroupID, Total, Remaining) ->
    headers(GroupID, Total, Remaining, Total - Remaining, 0).

headers(GroupID, Total, Remaining, ResetAmount, ResetSeconds) ->
    arweave_limiter_http_headers:to_http_headers(
      {register, leaky,
       #{expiring_limit => Total,
         remaining => Remaining,
         reset_amount => ResetAmount,
         reset_seconds => ResetSeconds,
         policies => policies(GroupID, Total)}
      }).

policies(Group, Total) ->
    #{id => Group,
    concurrency => #{limit => 500},
    sliding_window => #{limit => 0,
                window_seconds => 1},
    leaky_bucket   => #{burst => Total,
                tick_ms => 30000,
                tick_reduction => Total}}.
