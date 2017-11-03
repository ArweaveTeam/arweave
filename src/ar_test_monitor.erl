-module(ar_test_monitor).
-export([start/1, start/2, start/3, start/4, stop/1]).
-include("ar.hrl").

%%% Represents a monitoring system for test network.
%%% Checks for forward progress on the network and fork events.
%%% Optionally notifies a listener when the network fails.

-record(state, {
	miners,
	listener,
	% How many seconds to waait between network polls.
	check_time,
	% How long should we wait for progression before reporting that
	% it has stopped?
	failure_time,
	time_since_progress = 0,
	log = []
}).

%% The default amount of time to wait before storing network state.
-define(DEFAULT_TIMEOUT, 15 * 60).

%% @doc Start a monitor, given a list of mining nodes.
start(Miners) -> start(Miners, self()).
start(Miners, Listener) -> start(Miners, Listener, ?DEFAULT_TIMEOUT).
start(Miners, Listener, CheckTime) ->
	start(Miners, Listener, CheckTime, ?DEFAULT_TIMEOUT).
start(Miners, Listener, CheckTime, FailureTime) ->
	spawn(
		fun() ->
			server(
				#state {
					miners = Miners,
					listener = Listener,
					check_time = CheckTime,
					failure_time = FailureTime,
					log = [gather_results(Miners)]
				}
			)
		end
	).

%% @doc Stop a monitor process (does not kill nodes or miners).
stop(PID) ->
	PID ! stop.

%% @doc Main server loop
server(
		S = #state {
			miners = Miners,
			check_time = CheckTime,
			failure_time = FailureTime,
			time_since_progress = ProgTime,
			log = [Last|_] = Log
		}
	) ->
	receive stop -> ok
	after CheckTime * 1000 ->
		% Check for progress on the network.
		case gather_results(Miners) of
			Last when (ProgTime + CheckTime) >= FailureTime ->
				% No progress for longer than FailureTime, so reporting
				% that the network has stalled.
				end_test(S);
			Last ->
				% Increment time since progress and recurse.
				server(S#state { time_since_progress = ProgTime + CheckTime });
			New ->
				% A new state has been encountered. Print and store it.
				ar:report_console(ar_logging:format_log(New)),
				server(
					S#state {
						log = [New|Log],
						time_since_progress = 0
					}
				)
		end
	end.

%%% Utility functions

%% @doc Ask all nodes for the current block, count the number of each result.
gather_results(Miners) ->
	lists:foldr(
		fun({B, TXs}, Dict) ->
			case lists:keyfind(B, 1, Dict) of
				false -> [{B, TXs, 1}|Dict];
				{B, OldTXs, Num} ->
					lists:keyreplace(B, 1, Dict, {B, erlang:max(TXs, OldTXs), Num + 1})
			end;
		(unavailable, Dict) ->
			case lists:keyfind(unavailable, 1, Dict) of
				false -> [{unavailable, 1}|Dict];
				{unavailable, N} ->
					lists:keyreplace(unavailable, 1, Dict, {unavailable, N + 1})
			end
		end,
		[],
		lists:map(
			fun(Miner) ->
				case ar_node:get_blocks(Miner) of
					no_response -> unavailable;
					Bs -> {hd(Bs), count_txs(Bs)}
				end
			end,
			Miners
		)
	).

%% @doc Stop all network nodes and report log to listener.
end_test(#state { log = Log, listener = Listener }) ->
	ar:report_console([{test, unknown}, stopping]),
	Listener ! {test_report, self(), stopped, Log},
	ok.

%% @doc Count the number of transactions in a list of blocks.
count_txs(Bs) ->
	lists:sum([ length((ar_storage:read_block(B))#block.txs) || B <- Bs ]).
