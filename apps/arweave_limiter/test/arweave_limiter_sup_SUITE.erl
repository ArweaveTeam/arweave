-module(arweave_limiter_sup_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(M, arweave_limiter_sup).

suite() -> [{userdata, [description()]}].

description() -> {description, "arweave_limiter_sup test interface"}.

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, _Config) ->
    BeforeApps = application:which_applications(),
    application:ensure_all_started(arweave_config),

    put({?MODULE, snapshot}, arweave_config:snapshot()),

    arweave_config:set([limiter, test_limiter, number_of_workers], 10),
    arweave_config:set([limiter, test_limiter_2, number_of_workers], 5),
    [{before_apps, BeforeApps}].

end_per_testcase(_TestCase, Config) ->
    BeforeApps = ?config(before_apps, Config),
    [application:stop(App) || App <- (application:which_applications() -- BeforeApps)],
    arweave_config:restore(erase({?MODULE, snapshot})),
    ok.

all() ->
	[
		children_spec,
		child_spec
	].

children_spec(_Config) ->
    GroupIDs = [test_limiter, test_limiter_2],
    ChildSpec = ?M:children_spec(GroupIDs),
    SumWorkers = lists:foldl(fun(GID, AccIn) ->
                                     arweave_config:get([limiter, GID, number_of_workers]) + AccIn
                             end, 0, GroupIDs),

    ?assertEqual(SumWorkers, length(ChildSpec)),
    ok.

child_spec(_Config) ->
    ChildSpec = ?M:children_spec_per_group(test_limiter),
    ?assertEqual(arweave_config:get([limiter, test_limiter, number_of_workers]), length(ChildSpec)),
    ok.
