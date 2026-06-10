%%%
%%% @doc Test runner utilities for running EUnit tests with various granularity.
%%% Supports running all tests, specific modules, or specific test functions.
%%%
-module(ar_test_runner).

-export([run/1, run/2]).
-export([start_shell/1, stop_shell/1]).
-export([start_for_tests/1]).
-export([list_tests/1, list_tests_json/1]).

-include("ar.hrl").

%% @doc Run all tests for a given test type. TestType is 'test'.
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
%% Interactive shells always get the full peer cluster — we don't know
%% which modules will be touched at the REPL.
start_shell(TestType) ->
	ensure_started(TestType, {modules, [no_skip_marker]}).

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

%% @doc List all eunit tests defined in a module. Matches both
%% simple `*_test/0' and generator `*_test_/0' exports — the same
%% naming convention eunit uses for auto-discovery.
list_tests(Mod) when is_atom(Mod) ->
	Exports = Mod:module_info(exports),
	Tests = lists:filtermap(
		fun({Name, 0}) ->
			case is_eunit_test_export(atom_to_list(Name)) of
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

is_eunit_test_export(Name) ->
	%% Order matters: `_test_' has to be checked before `_test'
	%% because the former is a strict superset suffix.
	lists:suffix("_test_", Name) orelse lists:suffix("_test", Name).

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

default_modules(test) ->
	%% scripts/list_test_modules.sh discovers every eunit-bearing module.
	discover_modules("all").

%% @doc Run `scripts/list_test_modules.sh CATEGORY plain` and parse
%% the output into a list of module atoms.
discover_modules(Category) ->
	Cmd = "bash scripts/list_test_modules.sh " ++ Category ++ " plain",
	Output = os:cmd(Cmd),
	parse_module_names(Output).

parse_module_names(Output) ->
	Lines = string:split(Output, "\n", all),
	lists:filtermap(fun parse_module_line/1, Lines).

parse_module_line(Line) ->
	case string:trim(Line) of
		"" -> false;
		Name -> {true, list_to_atom(Name)}
	end.

run_tests(TestType, TestSpec) ->
	ensure_started(TestType, TestSpec),
	Result =
		try
			%% `exact_execution' tells eunit NOT to auto-run the
			%% sibling `Mod_tests' module when given `Mod'. Without
			%% it, eunit's default convenience pulls in both, which
			%% causes `Mod_tests' to run twice (once via `Mod' and
			%% once via its own matrix shard) and surfaces confusing
			%% cross-module failures in the stack trace.
			eunit:test(build_eunit_spec(TestSpec),
				[verbose, {print_depth, 100}, {exact_execution, true}])
		after
			ar_test_node:stop_peers(TestType)
		end,
	case Result of
		ok -> ok;
		_ -> init:stop(1)
	end.

%% @doc Set up the test environment. For `test' runs whose modules are
%% all tagged `@ar_test: fast', the peer cluster is skipped — those
%% tests don't touch peers and the boot costs ~1-2 minutes that we'd
%% pay for nothing. Any non-fast module in the spec forces the full
%% boot, since one slow test mixed in would need the peers.
ensure_started(TestType, TestSpec) ->
	SkipPeers = TestType =:= test andalso all_modules_are_fast(TestSpec),
	try
		arweave_config:start(),
		ok = arweave_limiter:start(),
		start_for_tests(TestType),
		case SkipPeers of
			true ->
				io:format(
					"All requested modules are tagged `@ar_test: fast' — "
					"skipping peer cluster boot~n");
			false ->
				ar_test_node:boot_peers(TestType),
				ar_test_node:wait_for_peers(TestType)
		end
	catch
		Type:Reason:S ->
			io:format("Failed to start the peers due to ~p:~p:~p~n", [Type, Reason, S]),
			init:stop(1)
	end.

%% @doc Returns true only when every module in TestSpec is tagged
%% `@ar_test: fast'. False on any non-fast module or an empty spec.
%% Conservative: if the discovery script fails for any reason, returns
%% false (boot peers — the safe default).
all_modules_are_fast({modules, []}) ->
	false;
all_modules_are_fast({modules, Mods}) ->
	all_in_fast_set(Mods);
all_modules_are_fast({mixed, []}) ->
	false;
all_modules_are_fast({mixed, Specs}) ->
	all_in_fast_set([spec_module(S) || S <- Specs]).

spec_module({module, M}) -> M;
spec_module({test, M, _}) -> M.

all_in_fast_set(Mods) ->
	try
		FastSet = sets:from_list(discover_modules("fast")),
		lists:all(fun(M) -> sets:is_element(M, FastSet) end, Mods)
	catch
		_:_ -> false
	end.

build_eunit_spec({modules, Mods}) ->
	%% Enumerate each module's individual test functions rather than
	%% passing the bare module list to eunit. Passing the bare module
	%% triggers eunit's auto-discovery which also runs `<Mod>_tests'
	%% if it exists — so running e.g. `ar_tx' would also run
	%% `ar_tx_tests', and then `ar_tx_tests' would run a second time
	%% in its own matrix shard. Per-function enumeration sidesteps
	%% that.
	%%
	%% No outer `{timeout, _, [...]}' wrapper: each module's tests
	%% already declare their own per-test `{timeout, _, fun}'
	%% generators, and wrapping the whole list would cause a single
	%% cancellation to abort all siblings.
	lists:flatmap(fun(Mod) -> spec_to_eunit({module, Mod}) end, Mods);
build_eunit_spec({mixed, Specs}) ->
	[spec_to_eunit(S) || S <- Specs].

spec_to_eunit({module, Mod}) ->
	%% Same rationale as build_eunit_spec({modules, _}) — enumerate
	%% to avoid eunit's auto-discovery of `Mod_tests'.
	[spec_to_eunit({test, Mod, Test}) || {_, Test} <- list_tests(Mod)];
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
	%% Anchor the data dir to project root: CT moves the BEAM's CWD to
	%% its per-run log dir, so a relative path would dump main-BEAM data
	%% under `_build/e2e/logs/...'.
	DataDir = filename:join(ar_test_node:project_root(),
		".tmp/data_" ++ atom_to_list(TestType) ++ "_main_" ++ UniqueName),
	Port = ar_test_node:get_unused_port(),
	%% Park the boot-time scaffolding in env vars and let
	%% `arweave_config:bootstrap/1' apply it. The same env mirror is
	%% what `ar_test_node:clean_up_and_stop/0' replays after each
	%% config store restore — no test-only config code paths.
	true = os:putenv("AR_DATA_DIR", DataDir),
	true = os:putenv("AR_PORT", integer_to_list(Port)),
	true = os:putenv("AR_DEBUG", "true"),
	true = os:putenv("AR_NETWORK_CLIENT_HTTP_KEEPALIVE", "4000"),
	true = os:putenv("AR_JOIN_AUTO", "false"),
	ok = arweave_config:bootstrap([]),
	ar:start_dependencies().
