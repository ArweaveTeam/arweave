%%%
%%% @doc Test runner utilities for running EUnit tests with various granularity.
%%% Supports running all tests, specific modules, or specific test functions.
%%%
-module(ar_test_runner).

-export([run/1, run/2]).
-export([start_shell/1, stop_shell/1]).
-export([list_tests/1, list_tests_json/1]).

-include("ar.hrl").

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
	load_default_modules("scripts/full_test_modules.txt").

run_tests(TestType, TestSpec) ->
	ensure_started(TestType),
	Result =
		try
			eunit:test(build_eunit_spec(TestSpec), [verbose, {print_depth, 100}])
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
		ok = arweave_limiter:start(),
		start_for_tests(TestType),
		ar_test_node:boot_peers(TestType),
		ar_test_node:wait_for_peers(TestType)
	catch
		Type:Reason:S ->
			io:format("Failed to start the peers due to ~p:~p:~p~n", [Type, Reason, S]),
			init:stop(1)
	end.

build_eunit_spec({modules, Mods}) ->
	%% Hand eunit the bare list (no outer `{timeout, _, [...]}'
	%% wrapper). When the list itself is wrapped in a timeout, eunit
	%% treats it as one test set and a cancellation in any element
	%% aborts the remaining siblings. Each module already contains its
	%% own per-test `{timeout, ?TEST_NODE_TIMEOUT, fun}' generators,
	%% so we don't need an additional outer guard.
	Mods;
build_eunit_spec({mixed, Specs}) ->
	[spec_to_eunit(S) || S <- Specs].

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

load_default_modules(Path) ->
	case file:read_file(Path) of
		{ok, Bin} ->
			parse_default_modules(binary_to_list(Bin));
		{error, Reason} ->
			erlang:error({failed_to_load_test_modules, Path, Reason})
	end.

parse_default_modules(Content) ->
	Lines = string:split(Content, "\n", all),
	lists:filtermap(fun parse_default_module_line/1, Lines).

parse_default_module_line(Line) ->
	Trimmed = string:trim(Line),
	case Trimmed of
		"" ->
			false;
		[$# | _] ->
			false;
		_ ->
			{true, list_to_atom(Trimmed)}
	end.

start_for_tests(TestType) ->
	UniqueName = ar_test_node:get_node_namespace(),
	DataDir = ".tmp/data_" ++ atom_to_list(TestType) ++ "_main_" ++ UniqueName,
	Port = ar_test_node:get_unused_port(),
	%% Park the boot-time scaffolding in env vars and let
	%% `arweave_config:bootstrap/1' apply it. The same env mirror is
	%% what `ar_test_node:clean_up_and_stop/0' replays after each
	%% `arweave_config:reset/0' — no test-only config code paths.
	true = os:putenv("AR_DATA_DIR", DataDir),
	true = os:putenv("AR_PORT", integer_to_list(Port)),
	true = os:putenv("AR_DEBUG", "true"),
	true = os:putenv("AR_RANDOMX_JIT", "false"),
	true = os:putenv("AR_NETWORK_CLIENT_HTTP_KEEPALIVE", "4000"),
	true = os:putenv("AR_JOIN_AUTO", "false"),
	ok = arweave_config:bootstrap([]),
	ar:start_dependencies().
