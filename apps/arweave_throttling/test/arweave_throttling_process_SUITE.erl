-module(arweave_throttling_process_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(M, arweave_throttling_process).

suite() -> [{userdata, [description()]}, {timetrap, {seconds, 30}}].

description() ->
    {description, "arweave_throttling_process group process registry"}.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = ?M:init(),
    %% Stand in for the supervisor: the registry only cares that it gets back
    %% an `{ok, Pid}', not that a real group worker is running.
    Pid = spawn(fun() -> receive stop -> ok end end),
    ok = meck:new(arweave_throttling_sup, [passthrough]),
    ok = meck:expect(arweave_throttling_sup, start_throttling_group,
                     fun(_GroupID) -> {ok, Pid} end),
    [{group_pid, Pid} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = meck:unload(arweave_throttling_sup),
    proplists:get_value(group_pid, Config) ! stop,
    ok = ?M:cleanup(),
    ok.

all() ->
    [
    get_and_delete_unknown_group,
    store_get_delete,
    list_and_binary_group_ids_are_equivalent,
    cleanup_removes_table
    ].

%%====================================================================
%% Test cases
%%====================================================================

get_and_delete_unknown_group(_Config) ->
    ?assertEqual({error, group_not_found}, ?M:get(<<"unknown">>)),
    %% `ets:delete/2' reports success even when the key was never there.
    ?assertEqual(true, ?M:delete(<<"unknown">>)),
    ?assertEqual({error, group_not_found}, ?M:get(<<"unknown">>)).

store_get_delete(Config) ->
    Pid = proplists:get_value(group_pid, Config),
    ?assertEqual({error, group_not_found}, ?M:get(<<"general">>)),
    ?assertEqual({ok, Pid}, ?M:start_and_store(<<"general">>)),
    ?assertEqual({ok, Pid}, ?M:get(<<"general">>)),
    ?assertEqual(true, ?M:delete(<<"general">>)),
    ?assertEqual({error, group_not_found}, ?M:get(<<"general">>)).

list_and_binary_group_ids_are_equivalent(Config) ->
    Pid = proplists:get_value(group_pid, Config),
    ?assertEqual({ok, Pid}, ?M:start_and_store("general")),
    ?assertEqual({ok, Pid}, ?M:get(<<"general">>)),
    ?assertEqual(true, ?M:delete("general")),
    ?assertEqual({error, group_not_found}, ?M:get("general")).

cleanup_removes_table(_Config) ->
    ?assertNotEqual(undefined, ets:info(?M, size)),
    ok = ?M:cleanup(),
    ?assertEqual(undefined, ets:info(?M, size)),
    %% Cleanup is idempotent, so `end_per_testcase' can call it again.
    ?assertEqual(ok, ?M:cleanup()).
