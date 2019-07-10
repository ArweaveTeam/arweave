-module(ar_benchmark).
-export([run/1]).
-include("src/ar.hrl").

%%% Runs a never ending mining performance benchmark.

%% @doc Execute the benchmark, printing the results to the terminal.
run(Algorithm) ->
	io:format(
		"~nRunning Arweave mining benchmark with ~B miner(s). Press Control+C twice to quit.~n~n",
		[ar_meta_db:get(max_miners)]
	),
	case Algorithm of
		sha384 ->
			loop({0, 0}, sha384);
		randomx ->
			Key = crypto:strong_rand_bytes(32),
			ar_randomx_state:init(
				whereis(ar_randomx_state),
				ar_randomx_state:swap_height(ar_fork:height_1_7()),
				Key,
				erlang:system_info(schedulers_online)
			),
			loop({0, 0}, randomx);
		UnknownAlgorithm ->
			io:format("Unknown mining algorithm: ~p. Choose from sha384, randomx.~n", [UnknownAlgorithm]),
			erlang:halt()
	end.

loop({TotalHashesTried, TotalTimeSpent}, Algorithm) ->
	Difficulty = case Algorithm of
		randomx -> 20 + ?RANDOMX_DIFF_ADJUSTMENT;
		sha384 -> 20
	end,
	{HashesTried, TimeSpent} = mine(Difficulty, 10, Algorithm),
	NewTotalHashesTried = TotalHashesTried + HashesTried,
	NewTotalTimeSpent = TotalTimeSpent + TimeSpent,
	io:format(
		"Current estimate: ~s. Since start: ~s~n",
		[
			format_hashes_per_second(HashesTried, TimeSpent),
			format_hashes_per_second(NewTotalHashesTried, NewTotalTimeSpent)
		]
	),
	loop({NewTotalHashesTried, NewTotalTimeSpent}, Algorithm).

mine(Diff, Rounds, Algorithm) ->
	{Time, _} = timer:tc(fun() ->
		Run = fun(_) -> mine(Diff, Algorithm) end,
		lists:foreach(Run, lists:seq(1, Rounds))
	end),
	EstimatedTriedHashes = math:pow(2, Diff) * Rounds,
	{EstimatedTriedHashes, Time}.

mine(Diff, Algorithm) ->
	B = #block{
		indep_hash = crypto:hash(sha384, crypto:strong_rand_bytes(40)),
		hash = crypto:hash(sha384, crypto:strong_rand_bytes(40)),
		timestamp = os:system_time(seconds),
		last_retarget = os:system_time(seconds),
		hash_list = [],
		height = case Algorithm of randomx -> ar_fork:height_1_7(); sha384 -> ar_fork:height_1_7() - 2 end
	},
	ar_mine:start(B, B, [], unclaimed, [], Diff, self(), []),
	receive
		{work_complete, _, _, _, _, _} ->
			ok
	end.

format_hashes_per_second(Hashes, Time) ->
	TimeSec = Time / 1000000,
	HashesPerSec = Hashes / TimeSec,
	case HashesPerSec of
		N when N > 10000 ->
			MegaPerSec = HashesPerSec / 1000000,
			iolist_to_binary(io_lib:format("~.4f MH/s", [MegaPerSec]));
		_ ->
			iolist_to_binary(io_lib:format("~.4f H/s", [HashesPerSec]))
	end.
