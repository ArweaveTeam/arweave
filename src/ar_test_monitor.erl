-module(ar_test_monitor).
-export([start/1, start/2, start/3, stop/1]).

%%% Represents a monitoring system for test network.
%%% Checks for forward progress on the network and fork events.
%%% Optionally notifies a listener when the network fails.

-record(state, {
	miners,
	listener,
	timeout, % In seconds
	log = []
}).

%% The default amount of time to wait before storing network state.
-define(DEFAULT_TIMEOUT, 15 * 60).

%% Start a monitor, given a list of mining nodes.
start(Miners) -> start(Miners, self()).
start(Miners, Listener) -> start(Miners, Listener, ?DEFAULT_TIMEOUT).
start(Miners, Listener, Timeout) ->
	spawn(
		fun() ->
			server(
				#state {
					miners = Miners,
					listener = Listener,
					timeout = Timeout,
					log = [gather_results(Miners)]
				}
			)
		end
	).

%% Stop a monitor process (does not kill nodes or miners).
stop(PID) ->
	PID ! stop.

%% Main server loop
server(
		S = #state {
			miners = Miners,
			timeout = Timeout,
			log = [Last|_] = Log
		}
	) ->
	receive stop -> ok
	after ar:scale_time(Timeout * 1000) ->
		case gather_results(Miners) of
			Last -> end_test(S);
			New ->
				server(
					S#state {
						log = [New|Log]
					}
				)
		end
	end.

%%% Utility functions

%% Ask all nodes for the current block, count the number of each result.
gather_results(Miners) ->
	lists:foldr(
		fun(B, Dict) ->
			case lists:keyfind(B, 1, Dict) of
				false -> [{B, 1}|Dict];
				{B, Num} ->
					lists:keyreplace(B, 1, Dict, {B, Num + 1})
			end
		end,
		[],
		lists:map(fun(Miner) -> hd(ar_node:get_blocks(Miner)) end, Miners)
	).

%% Stop all network nodes and report log to listener.
end_test(#state { log = Log, listener = Listener }) ->
	Listener ! {test_report, self(), stopped, Log},
	ok.
