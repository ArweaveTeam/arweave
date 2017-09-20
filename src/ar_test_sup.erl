-module(ar_test_sup).
-export([start/0, start/1]).
-include("ar_network_tests.hrl").

%%% Manages the execution of long-term, full network (with simulated clients)
%%% tests. These tests run indefinitely, only reporting when they fail.

-record(state, {
	tests = [],
	finished = []
}).

-record(test_run, {
	name,
	monitor,
	miners,
	clients,
	start_time = erlang:universaltime(),
	fail_time = undefined,
	log = undefined
}).

%% Starts all or a list of tests for ar_network_tests.hrl.
start() -> start(?NETWORK_TESTS).
start(TestName) when is_atom(TestName) -> start([TestName]);
start(RawTests) ->
	{{Yr, Mo, Da}, {Hr, Mi, Se}} = erlang:universaltime(),
	error_logger:logfile(
		{open,
			LogFile = 
				lists:flatten(
					io_lib:format(
						"~s/test_run_"
							"~4..0b-~2..0b-~2..0b_~2..0b-~2..0b-~2..0b.log",
						[?LOG_DIR, Yr, Mo, Da, Hr, Mi, Se]
					)
				)
		}
	),
	Tests =
	 	lists:map(
			fun(Test) ->
				case is_record(Test, network_test) of
					false ->
						lists:keyfind(Test, #network_test.name, ?NETWORK_TESTS);
					true -> Test
				end
			end,
			RawTests
		),
	ar:report_console([{test_log_file, LogFile}, {test_count, length(Tests)}]),
	spawn(
		fun() ->
			server(#state { tests = lists:map(fun start_test/1, Tests) })
		end
	).

%% Main server loop
server(S = #state { tests = Tests, finished = Finished }) ->
	receive
		{test_report, MonitorPID, stopped, Log} ->
			Test =
				(lists:keyfind(MonitorPID, #test_run.monitor, Tests))#test_run {
					fail_time = erlang:universaltime(),
					log = Log
				},
			ar:report_console(
				[
					{name, Test#test_run.name},
					{start_time, Test#test_run.start_time},
					{failure_time, Test#test_run.fail_time},
					{log_file, save_log(Test)}
				]
			),
			server(
				S#state {
					tests = [start_test(Test#test_run.name)|(Tests -- [Test])],
					finished = [Test|Finished]
				}
			);
		stop ->
			lists:foreach(fun stop_test/1, Tests),
			error_logger:logfile(close)
	end.

%%% Utility functions

%% Start a test, given a #network_test or test name.
%% Returns a #test_run.
start_test(T) when is_record(T, network_test) ->
	Miners =
		ar_network:start(
			T#network_test.num_miners,
			T#network_test.miner_connections,
			T#network_test.miner_loss_probability,
			T#network_test.miner_max_latency,
			T#network_test.miner_xfer_speed,
			T#network_test.miner_delay
		),
	ar_network:automine(Miners),
	#test_run {
		name = T#network_test.name,
		monitor = ar_test_monitor:start(Miners, self(), T#network_test.timeout),
		miners = Miners,
		clients =
			[
				ar_sim_client:start(
					Miners,
					T#network_test.client_action_time,
					T#network_test.client_max_tx_len,
					T#network_test.client_connections
				)
			||
				_ <- lists:seq(1, T#network_test.num_clients)
			]
	};
start_test(Name) ->
	case lists:keyfind(Name, #network_test.name, ?NETWORK_TESTS) of
		false -> not_found;
		Test -> start_test(Test)
	end.

%% Stop a test run (including the clients, miners, and monitor).
stop_test(#test_run{ miners = Miners, clients = Clients, monitor = Monitor }) ->
	% Kill the clients
	lists:foreach(fun ar_sim_client:stop/1, Clients),
	% Kill the miners
	lists:foreach(fun ar_node:stop/1, Miners),
	% Cut the monitor!
	ar_test_monitor:stop(Monitor).

%% Save a log object from a monitor to a file.
save_log(T) ->
	file:write_file(
		Filename = generate_filename(T),
		io_lib:format(
			"Name: ~p~nStart time: ~p~nFail time: ~p~n~n~s~n",
			[
				T#test_run.name,
				T#test_run.start_time,
				T#test_run.fail_time,
				lists:flatten(format_logs(T#test_run.log))
			]
		)
	),
	lists:flatten(Filename).

%% Turn a #test_run into a reasonable file name.
generate_filename(
	#test_run {
		name = Name,
		start_time = {{Yr, Mo, Da}, {Hr, Mi, Se}}
	}) ->
	io_lib:format("~s/~p_~4..0b-~2..0b-~2..0b_~2..0b-~2..0b-~2..0b.log",
		[?LOG_DIR, Name, Yr, Mo, Da, Hr, Mi, Se]
	).

%% Output a string representing a log.
format_logs([]) -> "";
format_logs([[{B, _}]|Logs]) ->
	io_lib:format("No forks. Block height: ~p.~n", [B#block.height])
		++ format_logs(Logs);
format_logs([Log|Logs]) ->
	io_lib:format("Fork detected:~n", []) ++
		string:join(
			lists:map(
				fun({B, Num}) ->
					io_lib:format(
						"\tBlock: ~p~n\t\tHeight: ~p~n\t\tNodes: ~p~n",
						[
							B#block.hash,
							B#block.height,
							Num
						]
					)
				end,
				Log
			),
			[$\n]
		) ++
		format_logs(Logs).
