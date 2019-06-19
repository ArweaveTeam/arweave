-module(ar_miner_log).
-export([start/0]).
-export([joining/0, joined/0]).
-export([started_hashing/0, foreign_block/1, mined_block/1, fork_recovered/1]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").

%%% Runs a logging server that informs the operator of the activities of their
%%% miner. In particular, as well as logging when hashing has started and when
%%% a candidate block is mined, it will also log when candidate blocks appear
%%% to have been accepted by the wider network.

%% @doc The period to wait between checking the state of a block in the BHL.

-ifdef(DEBUG).
-define(BLOCK_CHECK_TIME, 100).
-define(CONFIRMATION_DEPTH, 2).
-define(FOREIGN_BLOCK_ALERT_TIME, 3 * 1000).
-else.
-define(BLOCK_CHECK_TIME, 60 * 1000).
-define(CONFIRMATION_DEPTH, 10).
-define(FOREIGN_BLOCK_ALERT_TIME, 60 * 60 * 1000).
-endif.

%% @doc Start the foreign block watchdog process.
start() ->
	watchdog_start().

watchdog_start() ->
	watchdog_stop(),
	register(miner_connection_watchdog, spawn(fun watchdog/0)).

watchdog_stop() ->
	case whereis(miner_connection_watchdog) of
		undefined -> not_started;
		Pid ->
			exit(Pid, kill),
			wait_while_alive(Pid)
	end.

%% @doc Print a log message if no foreign block is received for
%% FOREIGN_BLOCK_ALERT_TIME ms.
watchdog() ->
	receive
		accepted_foreign_block -> watchdog();
		fork_recovered -> watchdog();
		stop -> ok
	after ?FOREIGN_BLOCK_ALERT_TIME ->
		case ar_meta_db:get(polling_mode) of
			true ->
				log(
					"WARNING: No foreign blocks fetched from the network. "
					"Please check your internet connection and the logs for errors."
				);
			_ ->
				log(
					"WARNING: No foreign blocks received from the network. "
					"Please confirm your node is available on port ~B "
					"on your Internet IP address. E.g. browse to http://[Internet IP address]:~B/info. "
					"If so, please check the logs for errors.",
					[ar_meta_db:get(port), ar_meta_db:get(port)]
				)
		end,
		watchdog()
	end.

%% @doc Log the message for starting joining the network.
joining() ->
	log("Joining the Arweave network...").

%% @doc Log the message for successfully joined the network.
joined() ->
	log("Joined the Arweave network successfully.").

%% @doc Log the message for a foreign block was accepted.
foreign_block(_BH) ->
	case whereis(miner_connection_watchdog) of
		undefined -> ok;
		PID -> PID ! accepted_foreign_block
	end.

%% @doc React to a fork recovery event.
fork_recovered(_BH) ->
	case whereis(miner_connection_watchdog) of
		undefined -> ok;
		PID -> PID ! fork_recovered
	end.

%% @doc Log the message for hasing started.
started_hashing() ->
	log("[Stage 1/3] Successfully proved access to recall block. Starting to hash.").

%% @doc Log the message a valid block was mined by the local node.
mined_block(BH) ->
	start_worker(BH),
	log("[Stage 2/3] Produced candidate block ~s and dispatched to network.", [ar_util:encode(BH)]).

%% @doc Log the message for block mined by the local node got confirmed by the
%% network.
accepted_block(BH) ->
	log("[Stage 3/3] Your block ~s was accepted by the network!", [ar_util:encode(BH)]).

%% @doc Log a message to the mining output.
log(FormatStr, Args) ->
	log(lists:flatten(io_lib:format(FormatStr, Args))).
log(Str) ->
	case ar_meta_db:get(miner_logging) of
		false -> do_nothing;
		_ ->
			{Date, {Hour, Minute, Second}} =
				calendar:now_to_local_time(os:timestamp()),
			ar:console(
				"~s, ~2..0w:~2..0w:~2..0w: ~s~n",
				[
					day(Date),
					Hour, Minute, Second,
					Str
				]
			),
			interceptor_log(Str)
	end.

%% @doc Start a process that checks the state of mined blocks.
start_worker(BH) ->
	spawn(fun() -> worker(BH, current_block_height()) end).

current_block_height() ->
	length(ar_node:get_hash_list(whereis(http_entrypoint_node))).

%% @doc Worker process for checking the status of candidate blocks.
worker(BH, InitBlockHeight) ->
	BHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	case ar_util:index_of(BH, BHL) of
		not_found when length(BHL) >= InitBlockHeight + ?STORE_BLOCKS_BEHIND_CURRENT ->
			ok;
		Depth when is_integer(Depth) andalso Depth >= ?CONFIRMATION_DEPTH ->
			accepted_block(BH),
			ok;
		_ ->
			receive
				stop -> ok % Not used
			after ?BLOCK_CHECK_TIME ->
				worker(BH, InitBlockHeight)
			end
	end.

%% @doc Return a printable day name from a date.
day({Year, Month, Day}) ->
	case calendar:day_of_the_week(Year, Month, Day) of
		1 -> "Monday";
		2 -> "Tuesday";
		3 -> "Wednesday";
		4 -> "Thursday";
		5 -> "Friday";
		6 -> "Saturday";
		7 -> "Sunday"
	end.

%%% Tests

%% @doc Start the miner log inception process.
interception_start() ->
	register(miner_log_debug, spawn(fun interceptor/0)).

%% @doc Stop the miner log inception process.
interception_stop() ->
	Pid = whereis(miner_log_debug),
	exit(Pid, kill),
	true = wait_while_alive(Pid).

%% @doc The inception process main function.
interceptor() ->
	interceptor_loop([]).

%% @doc The inception process recursive loop, where the log buffer is the
%% argument.
interceptor_loop(Log) ->
	receive
		{log, Msg} ->
			interceptor_loop(Log ++ [Msg]);
		clear_log ->
			interceptor_loop([]);
		{pop_all, Sender} ->
			Sender ! {log, Log},
			interceptor_loop([])
	end.

%% @doc Will send the log message to the inception process if it exists.
interceptor_log(Msg) ->
	case whereis(miner_log_debug) of
		undefined -> do_nothing;
		Pid -> Pid ! {log, Msg}
	end.

%% @doc Fetch the buffered log from the inception process and than clear the
%% buffer.
interceptor_pop_all() ->
	whereis(miner_log_debug) ! {pop_all, self()},
	receive
		{log, Log} -> Log
	end.

%% @doc Tests that the inception process can buffer manually triggered log
%% messages.
interception_proc_test() ->
	interception_start(),
	joining(),
	joined(),
	foreign_block("A-BLOCK-HASH"),
	started_hashing(),
	mined_block("A-BLOCK-HASH"),
	accepted_block("A-BLOCK-HASH"),
	Expected = [
		"Joining the Arweave network...",
		"Joined the Arweave network successfully.",
		"[Stage 1/3] Successfully proved access to recall block. Starting to hash.",
		"[Stage 2/3] Produced candidate block QS1CTE9DSy1IQVNI and dispatched to network.",
		"[Stage 3/3] Your block QS1CTE9DSy1IQVNI was accepted by the network!"
	],
	?assertEqual(Expected, interceptor_pop_all()),
	?assertEqual([], interceptor_pop_all()),
	interception_stop().

%% @doc Test the "worker" by asserting the log message for an accepted block
%% exists after enough amount of confirmations.
mined_block_test() ->
	interception_start(),
	ar_storage:clear(),
	[B0] = Bs = ar_weave:init([], ?DEFAULT_DIFF, ?AR(1)),
	ar_storage:write_block(B0),
	Node = ar_node:start([], Bs),
	ar_http_iface_server:reregister(Node),
	timer:sleep(500),
	ar_node:mine(Node),
	timer:sleep(500),
	[MyBH | _] = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	MsgCheck = block_accepted_msg_check(MyBH),
	?assert(not lists:any(MsgCheck, interceptor_pop_all())),
	ar_node:mine(Node),
	timer:sleep(500),
	?assert(lists:any(MsgCheck, interceptor_pop_all())),
	interception_stop().

%% @doc Deliberately trigger and test the 'no foreign blocks' warning.
no_foreign_blocks_test() ->
	interception_start(),
	start(),
	timer:sleep(?FOREIGN_BLOCK_ALERT_TIME + 1000),
	?assert(
		lists:any(
			fun ("WARNING: No foreign blocks received" ++ _) -> true;
				(_) -> false
			end,
			interceptor_pop_all()
		)
	),
	interception_stop().

block_accepted_msg_check(BH) ->
	AcceptedLogMsg = lists:flatten(
		io_lib:format("[Stage 3/3] Your block ~s was accepted by the network!",
						[ar_util:encode(BH)])),
	fun (LogMsg) ->
		LogMsg == AcceptedLogMsg
	end.

%% @doc Wait until the pid is not alive anymore. Returns true | {error,timeout}
wait_while_alive(Pid) ->
	Do = fun () -> not is_process_alive(Pid) end,
	ar_util:do_until(Do, 50, 4000).
