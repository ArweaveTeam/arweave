%%% @doc arweave_config_format_env test suite.
-module(arweave_config_format_env_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(_TestCase, Config) ->
	set_environment(),
	ok = arweave_config:start(),
	Config.

end_per_testcase(_TestCase, _Config) ->
	ok = arweave_config:stop(),
	unset_environment().

all() ->
	[ parse_returns_spec_matched_env ].

%%====================================================================
%% Test cases
%%====================================================================

parse_returns_spec_matched_env(_Config) ->
	Map = arweave_config_format_env:parse(),
	true = is_map(Map),

	%% AR_DEBUG should map to the [debug] option_key with the value
	%% we set in the fixture. Unmatched AR_TEST_ENVIRONMENT_VARIABLE
	%% should be absent from the result.
	?assertEqual(<<"true">>, maps:get([debug], Map, undefined)),
	?assertEqual(undefined,
		maps:get([ar_test_environment_variable], Map, undefined)),
	?assertEqual(<<"4">>,
		maps:get([mining, hashing_threads], Map, undefined)),
	?assertEqual(<<"/tmp/arweave-env-data">>,
		maps:get([data_dir], Map, undefined)),

	ok.

%%====================================================================
%% Helpers
%%====================================================================

environment() ->
	[
		{"AR_TEST_ENVIRONMENT_VARIABLE", "test"},
		{"AR_DEBUG", "true"},
		{"AR_MINING_HASHING_THREADS", "4"},
		{"AR_DATA_DIR", "/tmp/arweave-env-data"}
	].

set_environment() ->
	[
		begin
			os:putenv(K,V),
			true = os:getenv(K) =:= V
		end
		|| {K,V} <- environment()
	].

unset_environment() ->
	[
		begin
			os:unsetenv(K)
		end
		|| {K,_} <- environment()
	].
