%%% @doc arweave configuration bootstrap test suite.
-module(arweave_config_bootstrap_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	unset_env(),
	ok = arweave_config:stop().

all() ->
	[
		bootstrap_parses_legacy_args,
		bootstrap_parses_args,
		env_only_path,
		env_config_file_json,
		env_overrides_config_file,
		cli_overrides_env_and_config_file,
		config_file_json,
		config_file_yaml,
		config_file_dotted_json,
		config_file_dotted_yaml,
		legacy_config_file_uses_legacy_json,
		config_file_failure_forms,
		legacy_config_file_malformed,
		cli_overrides_config_file_when_flag_after_file,
		cli_overrides_config_file_when_flag_before_file,
		multiple_config_files_rejected,
		env_and_cli_config_files_rejected,
		env_and_legacy_config_files_rejected,
		unknown_cli_option_rejected,
		unknown_option_in_config_file_rejected
	].

%%====================================================================
%% Test cases
%%====================================================================

%% These cases exercise `arweave_config_bootstrap:start/1` dispatch
%% across input sources. Exhaustive config-file format coverage lives
%% in `arweave_config_full_load_SUITE`.
bootstrap_parses_legacy_args(_Config) ->
	ok = arweave_config_bootstrap:start(["init"]),
	true = arweave_config:get([genesis, init]),
	ok.

bootstrap_parses_args(_Config) ->
	ok = arweave_config_bootstrap:start(["--debug"]),
	true = arweave_config:get([debug]),
	ok = arweave_config_bootstrap:start(["--debug", "false"]),
	false = arweave_config:get([debug]),
	ok.

env_only_path(_Config) ->
	true = os:putenv("AR_DEBUG", "true"),
	ok = arweave_config_bootstrap:start([]),
	?assertEqual(true, arweave_config:get([debug])),
	ok.

env_config_file_json(Config) ->
	Path = write_config(Config, "bootstrap_env_config.json",
		<<"{\"debug\": true, \"port\": 54321}">>),
	true = os:putenv("AR_CONFIG_FILE", Path),
	ok = arweave_config_bootstrap:start([]),
	?assertEqual(true, arweave_config:get([debug])),
	?assertEqual(54321, arweave_config:get([port])),
	?assertEqual(abs_path_binary(Path),
		arweave_config:get([config_file])),
	ok.

env_overrides_config_file(Config) ->
	Path = write_config(Config, "env_overrides_config.json",
		<<"{\"debug\": false, \"port\": 1111}">>),
	true = os:putenv("AR_CONFIG_FILE", Path),
	true = os:putenv("AR_DEBUG", "true"),
	true = os:putenv("AR_PORT", "2222"),
	ok = arweave_config_bootstrap:start([]),
	?assertEqual(true, arweave_config:get([debug])),
	?assertEqual(2222, arweave_config:get([port])),
	ok.

cli_overrides_env_and_config_file(Config) ->
	Path = write_config(Config, "cli_overrides_env_and_config.json",
		<<"{\"debug\": false, \"port\": 1111}">>),
	true = os:putenv("AR_CONFIG_FILE", Path),
	true = os:putenv("AR_DEBUG", "false"),
	true = os:putenv("AR_PORT", "2222"),
	ok = arweave_config_bootstrap:start(["--debug", "--port", "3333"]),
	?assertEqual(true, arweave_config:get([debug])),
	?assertEqual(3333, arweave_config:get([port])),
	ok.

config_file_json(Config) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	Path = filename:join(PrivDir, "bootstrap_config.json"),
	ok = file:write_file(Path, <<"{\"debug\": true, \"port\": 12345}">>),
	ok = arweave_config_bootstrap:start(["--config_file", Path]),
	?assertEqual(true, arweave_config:get([debug])),
	?assertEqual(12345, arweave_config:get([port])),
	?assertEqual(abs_path_binary(Path),
		arweave_config:get([config_file])),
	ok.

config_file_yaml(Config) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	Path = filename:join(PrivDir, "bootstrap_config.yaml"),
	ok = file:write_file(Path, <<"debug: false\nport: 1986\n">>),
	ok = arweave_config_bootstrap:start(["--config_file", Path]),
	?assertEqual(false, arweave_config:get([debug])),
	?assertEqual(1986, arweave_config:get([port])),
	?assertEqual(abs_path_binary(Path),
		arweave_config:get([config_file])),
	ok.

config_file_dotted_json(Config) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	Path = filename:join(PrivDir, "bootstrap_dotted_config.json"),
	ok = file:write_file(Path,
		<<"{\"mining.enabled\": true, \"port\": 23456}">>),
	ok = arweave_config_bootstrap:start(["--config_file", Path]),
	?assertEqual(true, arweave_config:get([mining, enabled])),
	?assertEqual(23456, arweave_config:get([port])),
	ok.

config_file_dotted_yaml(Config) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	Path = filename:join(PrivDir, "bootstrap_dotted_config.yaml"),
	ok = file:write_file(Path, <<"
\"mining.hashing_threads\": 9
\"join.workers\": 3
">>),
	ok = arweave_config_bootstrap:start(["--config_file", Path]),
	?assertEqual(9, arweave_config:get([mining, hashing_threads])),
	?assertEqual(3, arweave_config:get([join, workers])),
	ok.

legacy_config_file_uses_legacy_json(Config) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	Path = filename:join(PrivDir, "bootstrap_legacy_config.json"),
	ok = file:write_file(Path, <<"{\"mine\": true, \"init\": true}">>),
	ok = arweave_config_bootstrap:start(["config_file", Path]),
	?assertEqual(true, arweave_config:get([mining, enabled])),
	?assertEqual(true, arweave_config:get([genesis, init])),
	ok.

config_file_failure_forms(Config) ->
	Path = missing_config_file_path(Config),
	_ = file:delete(Path),
	Forms = [
		["config_file", Path],
		["--config_file", Path],
		[<<"--config_file">>, list_to_binary(Path)],
		["--config_file=" ++ Path],
		[list_to_binary("--config_file=" ++ Path)]
	],
	lists:foreach(
		fun(Args) ->
			Result = arweave_config_bootstrap:start(Args),
			?assertMatch({error, _}, Result)
		end,
		Forms),
	ok.

legacy_config_file_malformed(_Config) ->
	%% Use a deterministic path so cleanup is straightforward.
	Path = filename:join("/tmp",
		"arweave_config_bootstrap_malformed.json"),
	ok = file:write_file(Path, <<"{ this is not valid json">>),
	try
		Result = arweave_config_bootstrap:start(
			["config_file", Path]),
		?assertMatch({error, _}, Result)
	after
		_ = file:delete(Path)
	end,
	ok.

cli_overrides_config_file_when_flag_after_file(Config) ->
	Path = write_config(Config, "override_after.json",
		<<"{\"debug\": false, \"port\": 1111}">>),
	ok = arweave_config_bootstrap:start(
		["--config_file", Path, "--debug", "--port", "2222"]),
	?assertEqual(true,  arweave_config:get([debug])),
	?assertEqual(2222,  arweave_config:get([port])),
	ok.

cli_overrides_config_file_when_flag_before_file(Config) ->
	Path = write_config(Config, "override_before.json",
		<<"{\"debug\": false, \"port\": 1111}">>),
	ok = arweave_config_bootstrap:start(
		["--debug", "--port", "2222", "--config_file", Path]),
	?assertEqual(true, arweave_config:get([debug])),
	?assertEqual(2222, arweave_config:get([port])),
	ok.

multiple_config_files_rejected(Config) ->
	PathA = write_config(Config, "multi_a.json",
		<<"{\"debug\": true}">>),
	PathB = write_config(Config, "multi_b.json",
		<<"{\"port\": 3333}">>),
	?assertMatch(
		{error, multiple_config_files},
		arweave_config_bootstrap:start(
			["--config_file", PathA, "--config_file", PathB])),
	ok.

env_and_cli_config_files_rejected(Config) ->
	PathA = write_config(Config, "env_multi_a.json",
		<<"{\"debug\": true}">>),
	PathB = write_config(Config, "env_multi_b.json",
		<<"{\"port\": 3333}">>),
	true = os:putenv("AR_CONFIG_FILE", PathA),
	?assertMatch(
		{error, multiple_config_files},
		arweave_config_bootstrap:start(["--config_file", PathB])),
	ok.

env_and_legacy_config_files_rejected(Config) ->
	PathA = write_config(Config, "env_legacy_multi_a.json",
		<<"{\"debug\": true}">>),
	PathB = write_config(Config, "env_legacy_multi_b.json",
		<<"{\"mine\": true}">>),
	true = os:putenv("AR_CONFIG_FILE", PathA),
	?assertMatch(
		{error, multiple_config_files},
		arweave_config_bootstrap:start(["config_file", PathB])),
	ok.

unknown_cli_option_rejected(_Config) ->
	?assertMatch(
		{error, #{ reason := <<"unknown argument">> }},
		arweave_config_bootstrap:start(["--no_such_option"])).

unknown_option_in_config_file_rejected(Config) ->
	Path = write_config(Config, "unknown_key.json",
		<<"{\"no_such_option\": true}">>),
	?assertMatch(
		{error, _},
		arweave_config_bootstrap:start(["--config_file", Path])).

%%====================================================================
%% Helpers
%%====================================================================

write_config(Config, Name, Body) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	Path = filename:join(PrivDir, Name),
	ok = file:write_file(Path, Body),
	Path.

missing_config_file_path(Config) ->
	PrivDir = proplists:get_value(priv_dir, Config),
	filename:join(PrivDir, "bootstrap_missing_config.json").

abs_path_binary(Path) ->
	unicode:characters_to_binary(filename:absname(Path)).

unset_env() ->
	true = os:unsetenv("AR_DEBUG"),
	true = os:unsetenv("AR_PORT"),
	true = os:unsetenv("AR_CONFIG_FILE").
