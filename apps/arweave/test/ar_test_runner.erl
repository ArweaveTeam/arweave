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
%% Interactive shells always get the full peer cluster — we don't know
%% which modules will be touched at the REPL.
start_shell(TestType) ->
	ensure_started(TestType, ar_test_node:all_peers(TestType)).

%% @doc Stop the test environment started by start_shell/1.
stop_shell(TestType) ->
	ar_test_node:stop_peers(ar_test_node:all_peers(TestType)),
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

default_modules(e2e) ->
	[ar_sync_pack_mine_tests, ar_repack_mine_tests, ar_repack_in_place_mine_tests];
default_modules(test) ->
	%% Discovery is driven by scripts/list_test_modules.sh — modules
	%% that have eunit tests are picked up automatically. Categories
	%% (fast / vdf / canary) come from `-test_category([...])'
	%% attributes in the module source. Here we want every
	%% eunit-bearing module.
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
	Peers = required_peers(TestType, TestSpec),
	ensure_started(TestType, Peers),
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
			ar_test_node:stop_peers(Peers)
		end,
	case Result of
		ok -> ok;
		_ -> init:stop(1)
	end.

%% @doc Set up the test environment and boot exactly the peers the run
%% needs. `Peers' is the union of the `-test_peers' declarations of the
%% modules under test (as `{TestType, Node}' pairs). An empty list means
%% a peer-free run — only the local `main' node — which is the default
%% and the common case: most modules touch no peer.
ensure_started(TestType, Peers) ->
	try
		arweave_config:start(),
		ok = arweave_limiter:start(),
		start_for_tests(TestType),
		case Peers of
			[] ->
				io:format("No peers required by the modules under test — "
					"skipping peer cluster boot~n");
			_ ->
				ar_test_node:boot_peers(Peers),
				ar_test_node:wait_for_peers(Peers)
		end
	catch
		Type:Reason:S ->
			io:format("Failed to start the peers due to ~p:~p:~p~n", [Type, Reason, S]),
			init:stop(1)
	end.

%% @doc The peers a run needs, as `{test, Node}' pairs. For a targeted
%% run (`{mixed, _}') it's the union of the `-test_peers' declarations
%% of the modules under test; a module with no attribute contributes no
%% peers, so a targeted run defaults to peer-free.
%%
%% The bare `{modules, _}' spec is produced only by the full default run
%% (`./bin/test' / `run/1' with no args). That exercises every module,
%% whose peers union to the whole cluster anyway, so we skip loading all
%% the modules to read their attributes and just boot all peers. e2e
%% keeps its fixed cluster.
required_peers(e2e, _TestSpec) ->
	ar_test_node:all_peers(e2e);
required_peers(test, {modules, _Mods}) ->
	ar_test_node:all_peers(test);
required_peers(test, {mixed, Specs}) ->
	Mods = [spec_module(S) || S <- Specs],
	Atoms = lists:usort(lists:flatmap(fun module_peers/1, Mods)),
	[{test, P} || P <- Atoms].

spec_module({module, M}) -> M;
spec_module({test, M, _}) -> M.

%% @doc Read a module's `-test_peers([...])' attribute, defaulting to the
%% empty list when absent. The module is loadable from the code path the
%% `erl -pa' test args set up, so `module_info/1' is available here —
%% before any peer is booted.
module_peers(Mod) ->
	_ = code:ensure_loaded(Mod),
	proplists:get_value(test_peers, Mod:module_info(attributes), []).

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
	DataDir = ".tmp/data_" ++ atom_to_list(TestType) ++ "_main_" ++ UniqueName,
	Port = ar_test_node:get_unused_port(),
	%% Park the boot-time scaffolding in env vars and let
	%% `arweave_config:bootstrap/1' apply it. The same env mirror is
	%% what `ar_test_node:clean_up_and_stop/0' replays after each
	%% config store restore — no test-only config code paths.
	true = os:putenv("AR_DATA_DIR", DataDir),
	true = os:putenv("AR_PORT", integer_to_list(Port)),
	true = os:putenv("AR_DEBUG", "true"),
	true = os:putenv("AR_RANDOMX_JIT", "false"),
	true = os:putenv("AR_NETWORK_CLIENT_HTTP_KEEPALIVE", "4000"),
	true = os:putenv("AR_JOIN_AUTO", "false"),
	ok = arweave_config:bootstrap([]),
	ar:start_dependencies().
