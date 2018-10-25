-module(ar_miner_log).
-export([joining/0, joined/0]).
-export([started_hashing/0, foreign_block/1, mined_block/1]).
-export([start_worker/1]).
-include("ar.hrl").

%%% Runs a logging server that informs the operator of the activities of their
%%% miner. In particular, as well as logging when hashing has started and when
%%% a candidate block is mined, it will also log when candidate blocks appear
%%% to have been accepted by the wider network.

%% @doc The period to wait between checking the state of a block in the BHL.

-ifdef(DEBUG).
-define(BLOCK_CHECK_TIME, 100).
-define(CONFIRMATION_DEPTH, 2).
-else.
-define(BLOCK_CHECK_TIME, 60 * 1000).
-define(CONFIRMATION_DEPTH, 10).
-endif.


joining() ->
	log("Joining the Arweave network...").

joined() ->
	log("Joined the Arweave network successfully.").

foreign_block(BH) ->
	log("Accepted foreign block ~s.", [ar_util:encode(BH)]).

started_hashing() ->
	log("Successfully proved access to recall block. Starting to hash.").

mined_block(BH) ->
	log("Produced candidate block ~s and dispatched to network.", [ar_util:encode(BH)]).

accepted_block(BH) ->
	log("Your block ~s was accepted by the network!", [ar_util:encode(BH)]).

%% @doc Log a message to the mining output.
log(FormatStr, Args) ->
	log(lists:flatten(io_lib:format(FormatStr, Args))).
log(Str) ->
	case ar_meta_db:get(miner_logging) of
		false -> do_nothing;
		_ ->
			case whereis(miner_log_debug) of
				undefined -> do_nothing;
				PID -> PID ! {log, Str}
			end,
			{Date, {Hour, Minute, Second}} =
				calendar:now_to_datetime(os:timestamp()),
			io:format(
				"~s, ~2..0w:~2..0w:~2..0w: ~s~n",
				[
					day(Date),
					Hour, Minute, Second,
					Str
				]
			)
	end.

%% @doc Start a process that checks the state of mined blocks.
start_worker(BH) -> spawn(fun() -> worker(BH) end).

%% @doc Worker process for checking the status of candidate blocks.
worker(BH) ->
	BHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	case ar_util:index_of(BH, BHL) of
		not_found -> ok;
		_Depth when _Depth >= ?CONFIRMATION_DEPTH ->
			accepted_block(BH);
		_ ->
			receive
				stop -> ok
			after ?BLOCK_CHECK_TIME ->
				worker(BH)
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

%% @doc Check 'happy' path.