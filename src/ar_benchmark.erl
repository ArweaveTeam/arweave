-module(ar_benchmark).
-export([run/0]).
-include("src/ar.hrl").

%%% Runs a never ending mining performance benchmark.

%% @doc Execute the benchmark, printing the results to the terminal.
run() ->
	io:format(
		"~nRunning Arweave mining benchmark with ~B miner(s). Press Control+C twice to quit.~n~n",
		[ar_meta_db:get(max_miners)]
	),
	loop({0, 0}).

loop({TotalHashesTried, TotalTimeSpent}) ->
	{HashesTried, TimeSpent} = mine(20, 10),
	NewTotalHashesTried = TotalHashesTried + HashesTried,
	NewTotalTimeSpent = TotalTimeSpent + TimeSpent,
	io:format(
		"Current estimate: ~s. Since start: ~s~n",
		[
			format_hashes_per_second(HashesTried, TimeSpent),
			format_hashes_per_second(NewTotalHashesTried, NewTotalTimeSpent)
		]
	),
	loop({NewTotalHashesTried, NewTotalTimeSpent}).

mine(Diff, Rounds) ->
	{Time, _} = timer:tc(fun() ->
		Run = fun(_) -> mine(Diff) end,
		lists:foreach(Run, lists:seq(1, Rounds))
	end),
	EstimatedTriedHashes = (math:pow(2, Diff) / 2) * Rounds,
	{EstimatedTriedHashes, Time}.

mine(Diff) ->
	B = #block{
		indep_hash = crypto:hash(sha384, crypto:strong_rand_bytes(40)),
		hash = crypto:hash(sha384, crypto:strong_rand_bytes(40)),
		timestamp = os:system_time(seconds),
		last_retarget = os:system_time(seconds),
		hash_list = []
	},
	ar_mine:start(B, B, [], unclaimed, [], Diff, self()),
	receive
		{work_complete, _, _, _, _, _} ->
			ok
	end.

format_hashes_per_second(Hashes, Time) ->
	TimeSec = Time / 1000000,
	MegaPerSec = (Hashes / TimeSec) / 1000000,
	iolist_to_binary(io_lib:format("~.4f MH/s", [MegaPerSec])).
