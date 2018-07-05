-module(ar_benchmark).
-export([run/0]).
-include("src/ar.hrl").

%%% Runs a hashing performance benchmark on a single core of the machine.

%% Execute the benchmark, printing the results to the terminal.
run() ->
	io:format("Running Arweave mining benchmark. Press Control+C twice to quit.~n~n"),
	do_run(5000000).

%% Report on a run, then restart.
do_run(Iterations) ->
	T0 = ms_timestamp(),
	benchmark(crypto:strong_rand_bytes(40), Iterations),
	T1 = ms_timestamp(),
	io:format("Performance rate: ~ph/s/core.~n", [erlang:trunc(Iterations / ((T1 - T0)/1000))]),
	do_run(Iterations).

%% Perform the hashing benchmark.
benchmark(_, 0) -> ok;
benchmark(Data, Remaining) ->
	crypto:hash(?MINING_HASH_ALG, Data),
	benchmark(Data, Remaining - 1).

%% Returns Unix time in milliseconds.
ms_timestamp() ->
	erlang:system_time(millisecond).