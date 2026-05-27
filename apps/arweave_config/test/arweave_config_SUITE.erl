%%% @doc
-module(arweave_config_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop().

all() ->
	[
		arweave_config,
		load,
		force_config_runtime_guard,
		clear_aggregate,
		replace_aggregate,
		with_test_config_isolation,
		runtime_rejects_on_validator_error
	].

%%====================================================================
%% Test cases
%%====================================================================

arweave_config(_Config) ->
	_ = arweave_config:get([debug]),

	undefined = arweave_config:get([missing, option]),

	{ok, DebugValue1} = arweave_config:set([debug], true),
	DebugValue1 = arweave_config:get([debug]),

	{ok, DebugValue2} = arweave_config:set([debug], false),
	DebugValue2 = arweave_config:get([debug]),
	ok.

load(_Config) ->
	ok = arweave_config:load(#{
		[data_dir] => <<"/tmp/test_load">>,
		[port] => 1985,
		[debug] => true
	}),
	"/tmp/test_load" = arweave_config:get([data_dir]),
	1985 = arweave_config:get([port]),
	true = arweave_config:get([debug]),

	ok = arweave_config:load(#{
		[network, server, tcp, backlog] => 2048
	}),
	2048 = arweave_config:get([network, server, tcp, backlog]),

	ok = arweave_config:load(#{
		[log_dir] => "/tmp/test_logs",
		[network, server, tcp, max_connections] => 1234
	}),
	"/tmp/test_logs" = arweave_config:get([log_dir]),
	1234 = arweave_config:get(
		[network, server, tcp, max_connections]),

	ok = arweave_config:load(#{}),

	ok.

force_config_runtime_guard(_Config) ->
	false = arweave_config:is_runtime(),
	ok = arweave_config:runtime(),
	true = arweave_config:is_runtime(),

	%% `[data_dir]` is a `runtime => false` spec. Without
	%% `force_config/1` it would be rejected because the lifecycle is
	%% in runtime mode.
	ok = arweave_config:force_config(#{
		[data_dir] => <<"/tmp/test_force_config">>
	}),
	"/tmp/test_force_config" = arweave_config:get([data_dir]),

	true = arweave_config:is_runtime(),

	ok.

clear_aggregate(_Config) ->
	arweave_config:with_test_config(fun() ->
		Peers = [{127,0,0,1,1984}, {127,0,0,2,1984}],
		ok = arweave_config:replace_peers(trusted, Peers),
		2 = length(arweave_config:get_peers(trusted)),

		ok = arweave_config:clear_peers(trusted),
		[] = arweave_config:get_peers(trusted)
	end),
	ok.

replace_aggregate(_Config) ->
	arweave_config:with_test_config(fun() ->
		OldPeers = [{127,0,0,1,1984}, {127,0,0,2,1984}],
		ok = arweave_config:replace_peers(trusted, OldPeers),
		2 = length(arweave_config:get_peers(trusted)),

		NewPeers = [{10,0,0,1,1984}],
		ok = arweave_config:replace_peers(trusted, NewPeers),
		Trusted = arweave_config:get_peers(trusted),
		1 = length(Trusted),
		true = lists:member({10,0,0,1,1984}, Trusted),
		false = lists:member({127,0,0,1,1984}, Trusted),
		false = lists:member({127,0,0,2,1984}, Trusted)
	end),
	ok.

with_test_config_isolation(_Config) ->
	OriginalDebug = arweave_config:get([debug]),

	arweave_config:with_test_config(fun() ->
		{ok, true} = arweave_config:set([debug], true),
		true = arweave_config:get([debug])
	end),

	OriginalDebug = arweave_config:get([debug]),

	ok.

runtime_rejects_on_validator_error(_Config) ->
	false = arweave_config:is_runtime(),

	{ok, true} = arweave_config:set([cm, enabled], true),
	%% Sanity check: api_secret is not_set by default.
	not_set = arweave_config:get([cm, api_secret]),

	{error, _} = arweave_config:runtime(),
	false = arweave_config:is_runtime(),

	{ok, false} = arweave_config:set([cm, enabled], false),

	ok.
