-module(ar).
-export([start/0, rebuild/0]).
-export([test/0, test_apps/0]).
-export([report/1]).
-export([scale_time/1]).

%%% ArkChain server entrypoint and basic utilities.

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar_simple_reporter,
		ar_wallet,
		ar_tx,
		ar_weave,
		ar_gossip,
		ar_mine,
		ar_node
	]
).

%% All of the apps that have tests associated with them
-define(APP_TEST_MODS, [app_chirper]).

%% Start an ArkChain node on this BEAM.
start() ->
	%io:format("Starting up node...~n"),
	ok.

%% Run the erlang make system on the project.
rebuild() ->
	make:all([load]).

%% Run all of the tests associated with the core project.
test() ->
	eunit:test(?CORE_TEST_MODS, [verbose]).

%% Run tests on the apps.
test_apps() ->
	eunit:test(?APP_TEST_MODS, [verbose]).

%% Print an informational message to the console.
report(X) ->
	error_logger:info_report(X).

%% A multiplier applied to all simulated time elements in the system.
-ifdef(DEBUG).
scale_time(Time) -> erlang:trunc(?DEBUG_TIME_SCALAR * Time).
-else.
scale_time(Time) -> Time.
-endif.
