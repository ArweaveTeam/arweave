-module(ar).
-export([main/0, main/1, start/0, start/1, rebuild/0]).
-export([test/0, test/1, test_coverage/0, test_apps/0, test_networks/0]).
-export([docs/0]).
-export([report/1, report_console/1, d/1]).
-export([scale_time/1, timestamp/0]).
-include("ar.hrl").

%%% Archain server entrypoint and basic utilities.

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar_util,
		ar_meta_db,
		ar_storage,
		ar_serialize,
		ar_http_iface,
		ar_wallet,
		ar_router,
		ar_tx,
		ar_weave,
		ar_gossip,
		ar_mine,
		ar_join,
		ar_fork_recovery,
		ar_node,
		ar_simple_reporter,
		ar_retarget
	]
).

%% All of the apps that have tests associated with them
-define(APP_TEST_MODS, [app_chirper]).

%% Start options
-record(opts, {
	port = ?DEFAULT_HTTP_IFACE_PORT,
	init = false,
	mine = false,
	peers = []
}).

%% @doc Command line program entrypoint. Takes a list of arguments.
main() -> main("").
main("") ->
	io:format("Starts an Archain mining server.~n"),
	io:format("Usage: archain-server [init] [mine] "
   		"[peer ip_addr_and_port]* [port port_number]~n");
main(Args) -> main(Args, #opts{}).
main([], O) -> start(O);
main(["init"|Rest], O) ->
	main(Rest, O#opts { init = true });
main(["mine"|Rest], O) ->
	main(Rest, O#opts { mine = true });
main(["peer", Peer|Rest], O = #opts { peers = Ps }) ->
	main(Rest, O#opts { peers = [ar_util:parse_peer(Peer)|Ps] });
main(["port", Port|Rest], O) ->
	main(Rest, O#opts { port = list_to_integer(Port) });
main([Arg|_Rest], _O) ->
	io:format("Unknown argument: ~s. Terminating.", [Arg]).

%% @doc Start an Archain node on this BEAM.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) -> start(#opts { port = Port });
start(#opts { port = Port, init = Init, peers = Peers, mine = Mine }) ->
	% Start apps which we depend on.
	inets:start(),
	ar_meta_db:start(),
	ar_meta_db:put(port, Port),
	Node = ar_node:start(
		Peers,
		if Init -> ar_weave:init(); true -> undefined end
	),
	% Start the logging system.
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),
	ar:report_console(
		[
			starting_server,
			{session_log, Filename},
			{port, Port},
			{init_new_blockweave, Init},
			{automine, Mine},
			{miner, Node},
			{peers, Peers}
		]
	),
	% Start the first node in the gossip network (with HTTP interface)
	ar_http_iface:start(
		Port,
		Node
	),
	if Mine -> ar_node:automine(Node); true -> do_nothing end.

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
	case ?DEFAULT_DIFF of
		X when X > 17 ->
			ar:report_console(
				[
					diff_too_high_for_tests,
					terminating
				]
			);
		_ ->
			start(),
			eunit:test(?CORE_TEST_MODS, [verbose])
	end.

%% @doc Run the TNT test system, printing coverage results.
test_coverage() ->
	ar_coverage:analyse(fun test/0).

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
