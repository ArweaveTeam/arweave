%%%
%%% @doc Test runner utilities for running EUnit tests with various granularity.
%%% Supports running all tests, specific modules, or specific test functions.
%%%
-module(ar_test_runner).

-export([run/1, run/2]).
-export([start_shell/1, stop_shell/1]).
-export([list_tests/1, list_tests_json/1]).

-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%% @doc Run all tests for a given test type.
%% TestType is 'test' or 'e2e'.
run(TestType) ->
	Modules = default_modules(TestType),
	run_tests(TestType, {modules, Modules}).

%% @doc Run tests based on CLI arguments.
%% Supports:
%%   - module                    run all tests in module
%%   - module:test               run specific test from module  
%%   - module1 module2           run all tests in multiple modules
%%   - module1 module2:test      mixed mode
run(TestType, Args) when is_list(Args) ->
	Specs = lists:map(fun parse_arg/1, Args),
	run_tests(TestType, {mixed, Specs}).

%% @doc Start the test environment for interactive shell use (without running tests).
start_shell(TestType) ->
	ensure_started(TestType).

%% @doc Stop the test environment started by start_shell/1.
stop_shell(TestType) ->
	ar_test_node:stop_peers(TestType),
	init:stop().

%% Parse a CLI argument into either {module, Mod} or {test, Mod, Test}.
parse_arg(Arg) when is_atom(Arg) ->
	{module, Arg};
parse_arg(Arg) when is_list(Arg) ->
	case string:split(Arg, ":") of
		[Mod, Test] -> {test, list_to_atom(Mod), list_to_atom(Test)};
		[Mod]       -> {module, list_to_atom(Mod)}
	end.

%% @doc List all tests in a module.
%% Returns a list of {Module, Test} tuples.
list_tests(Mod) when is_atom(Mod) ->
	Exports = Mod:module_info(exports),
	Tests = lists:filtermap(
		fun({Name, 0}) ->
			NameStr = atom_to_list(Name),
			case lists:suffix("_test_", NameStr) of
				true -> {true, {Mod, Name}};
				false -> false
			end;
		   (_) -> false
		end,
		Exports
	),
	lists:sort(Tests);
list_tests(Mods) when is_list(Mods) ->
	lists:flatmap(fun list_tests/1, Mods).

%% @doc Output tests as JSON for CI systems.
list_tests_json(Mods) ->
	Tests = list_tests(Mods),
	JsonItems = lists:map(
		fun({Mod, Test}) ->
			io_lib:format("{\"module\":\"~s\",\"test\":\"~s\"}", [Mod, Test])
		end,
		Tests
	),
	JsonArray = "[" ++ string:join(JsonItems, ",") ++ "]",
	io:format("~s~n", [JsonArray]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

default_modules(e2e) ->
	[ar_sync_pack_mine_tests, ar_repack_mine_tests, ar_repack_in_place_mine_tests];
default_modules(test) ->
	%% For regular tests, we don't specify modules - EUnit discovers them
	[].

run_tests(TestType, TestSpec) ->
	ensure_started(TestType),
	TotalTimeout = case TestType of
		e2e -> ?E2E_TEST_SUITE_TIMEOUT;
		_ -> ?TEST_SUITE_TIMEOUT
	end,
	Result =
		try
			EunitSpec = build_eunit_spec(TotalTimeout, TestSpec),
			eunit:test(EunitSpec, [verbose, {print_depth, 100}])
		after
			ar_test_node:stop_peers(TestType)
		end,
	case Result of
		ok -> ok;
		_ -> init:stop(1)
	end.

ensure_started(TestType) ->
	try
		arweave_config:start(),
		start_for_tests(TestType),
		ar_test_node:boot_peers(TestType),
		ar_test_node:wait_for_peers(TestType)
	catch
		Type:Reason:S ->
			io:format("Failed to start the peers due to ~p:~p:~p~n", [Type, Reason, S]),
			init:stop(1)
	end.

build_eunit_spec(Timeout, {modules, []}) ->
	{timeout, Timeout, []};
build_eunit_spec(Timeout, {modules, Mods}) ->
	{timeout, Timeout, Mods};
build_eunit_spec(Timeout, {mixed, Specs}) ->
	EunitSpecs = lists:map(fun spec_to_eunit/1, Specs),
	{timeout, Timeout, EunitSpecs}.

spec_to_eunit({module, Mod}) ->
	Mod;
spec_to_eunit({test, Mod, Test}) ->
	%% Check if it's a generator (_test_) or simple test (_test)
	TestName = atom_to_list(Test),
	case lists:suffix("_test_", TestName) of
		true ->
			%% Generator - returns a test spec
			{generator, fun() -> Mod:Test() end};
		false ->
			%% Simple test function - run directly
			{Mod, Test}
	end.

start_for_tests(TestType) ->
	UniqueName = ar_test_node:get_node_namespace(),
	TestConfig = #config{
		debug = true,
		peers = [],
		data_dir = ".tmp/data_" ++ atom_to_list(TestType) ++ "_main_" ++ UniqueName,
		port = ar_test_node:get_unused_port(),
		disable = [randomx_jit],
		auto_join = false
	},
	ar:start(TestConfig).
