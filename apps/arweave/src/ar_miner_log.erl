%%% @doc Runs a logging server that informs the operator of the activities of their
%%% miner. In particular, as well as logging when hashing has started and when
%%% a candidate block is mined, it will also log when mined blocks have a
%%% certain number of confirmations.
-module(ar_miner_log).

-export([
	start/0,
	joining/0, joined/0,
	started_hashing/0, foreign_block/1, mined_block/2,
	log_no_foreign_blocks/0
]).

-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").


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
%% FOREIGN_BLOCK_ALERT_TIME ms. Also, print a log message when a mined
%% block is confirmed.
watchdog() ->
	watchdog(#{ mined_blocks => maps:new(), foreign_blocks_timer => start_timer() }).

watchdog(#{ mined_blocks := MinedBlocks, foreign_blocks_timer := TimerRef } = State) ->
	receive
		{block_received_n_confirmations, BH, Height} ->
			UpdatedMinedBlocks = case maps:take(Height, MinedBlocks) of
				{BH, Map} ->
					accepted_block(BH),
					Map;
				{_, Map} ->
					Map;
				error ->
					MinedBlocks
			end,
			watchdog(State#{ mined_blocks => UpdatedMinedBlocks });
		{mined_block, BH, Height} ->
			watchdog(State#{ mined_blocks => MinedBlocks#{ Height => BH } });
		accepted_foreign_block ->
			watchdog(State#{ foreign_blocks_timer => refresh_timer(TimerRef) });
		stop ->
			cancel_timer(TimerRef),
			ok
	end.

start_timer() ->
	{ok, TimerRef} =
		timer:apply_after(?FOREIGN_BLOCK_ALERT_TIME, ?MODULE, log_no_foreign_blocks, []),
	TimerRef.

refresh_timer(TimerRef) ->
	cancel_timer(TimerRef),
	start_timer().

cancel_timer(TimerRef) ->
	{ok, cancel} = timer:cancel(TimerRef).

log_no_foreign_blocks() ->
	log(
		"WARNING: No foreign blocks received from the network or found by trusted peers. "
		"Please check your internet connection and the logs for errors."
	).

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

%% @doc Log the message for hasing started.
started_hashing() ->
	log("[Stage 1/3] Successfully proved access to recall block. Starting to hash.").

block_received_n_confirmations(BH, Height) ->
	case whereis(miner_connection_watchdog) of
		undefined -> ok;
		PID -> PID ! {block_received_n_confirmations, BH, Height}
	end.

%% @doc Log the message a valid block was mined by the local node.
mined_block(BH, Height) ->
	case whereis(miner_connection_watchdog) of
		undefined -> ok;
		PID -> PID ! {mined_block, BH, Height}
	end,
	log(
		"[Stage 2/3] Produced candidate block ~s and dispatched to network.",
		[ar_util:encode(BH)]
	).

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

%% @doc Will send the log message to the inception process if it exists.
interceptor_log(Msg) ->
	case whereis(miner_log_debug) of
		undefined -> do_nothing;
		Pid -> Pid ! {log, Msg}
	end.

%% @doc Wait until the pid is not alive anymore. Returns true | {error,timeout}
wait_while_alive(Pid) ->
	Do = fun () -> not is_process_alive(Pid) end,
	ar_util:do_until(Do, 50, 4000).
