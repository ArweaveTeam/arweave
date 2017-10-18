-module(ar).
-export([start/0, rebuild/0]).
-export([test/0, test/1, test_apps/0, test_networks/0]).
-export([docs/0]).
-export([report/1, report_console/1, d/1]).
-export([scale_time/1, timestamp/0]).
-include("ar.hrl").

%%% ArkChain server entrypoint and basic utilities.

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar_serialize,
		ar_http_iface,
		ar_wallet,
		ar_router,
		ar_tx,
		ar_weave,
		ar_gossip,
		ar_mine,
		ar_fork_recovery,
		ar_node,
		ar_simple_reporter,
		ar_retarget
	]
).

%% All of the apps that have tests associated with them
-define(APP_TEST_MODS, [app_chirper]).

%% @doc Start an Archain node on this BEAM.
start() ->
	%io:format("Starting up node...~n"),
	inets:start(),
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),
	report_console([{session_log, Filename}]),
	ar_http_iface:start(),
	ok.

%% @doc Create a name for a session log file.
generate_logfile_name() ->
	{{Yr, Mo, Da}, {Hr, Mi, Se}} = erlang:universaltime(),
	lists:flatten(
		io_lib:format(
			"~s/session_~4..0b-~2..0b-~2..0b_~2..0b-~2..0b-~2..0b.log",
			[?LOG_DIR, Yr, Mo, Da, Hr, Mi, Se]
		)
	).

%% @doc Run the erlang make system on the project.
rebuild() ->
	make:all([load]).

%% @doc Run all of the tests associated with the core project.
test() ->
	start(),
	eunit:test(?CORE_TEST_MODS, [verbose]).

%% @doc Run the tests for a single module.
test(Mod) ->
	eunit:test([Mod], [verbose]).

%% @doc Run tests on the apps.
test_apps() ->
	start(),
	eunit:test(?APP_TEST_MODS, [verbose]).

test_networks() ->
	error_logger:tty(false),
	ar_test_sup:start().

%% @doc Generate the project documentation.
docs() ->
	Mods =
		lists:filter(
			fun(File) -> string:str(File, ".erl") > 0 end,
			element(2, file:list_dir("../src"))
		),
	edoc:files([ "../src/" ++ Mod || Mod <- Mods ]).

%% @doc Print an informational message to the log file.
report(X) ->
	error_logger:info_report(X).

%% @doc Print an information message to the log file and console.
report_console(X) ->
	error_logger:tty(true),
	error_logger:info_report(X),
	error_logger:tty(false).

%% @doc Report a value and return it.
d(X) ->
	report_console(X),
	X.

%% @doc A multiplier applied to all simulated time elements in the system.
-ifdef(DEBUG).
scale_time(Time) ->
	erlang:trunc(?DEBUG_TIME_SCALAR * Time).
-else.
scale_time(Time) -> Time.
-endif.

%% @doc Get the unix timestamp (in seconds).
timestamp() ->
	{MegaSec, Sec, _MilliSec} = os:timestamp(),
	(MegaSec * 1000000) + Sec.
