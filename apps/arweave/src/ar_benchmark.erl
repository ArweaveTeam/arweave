-module(ar_benchmark).

-export([run/0]).

-include("src/ar.hrl").

%%% Runs a never ending mining performance benchmark.

%% @doc Execute the benchmark, printing the results to the terminal.
run() ->
	io:format(
		"~nRunning Arweave mining benchmark with ~B miner(s). "
		"Press Control+C twice to quit.~n~n",
		[ar_meta_db:get(max_miners)]
	),
	Key = crypto:strong_rand_bytes(32),
	{ok, _} = ar_wallets:start_link([{recent_block_index, []}, {peers, []}]),
	ar_randomx_state:init(
		whereis(ar_randomx_state),
		ar_randomx_state:swap_height(ar_fork:height_2_2()),
		Key,
		erlang:system_info(schedulers_online)
	),
	loop({0, 0}, initial_diff()).

initial_diff() ->
	Diff = ar_retarget:switch_to_linear_diff(20 + ?RANDOMX_DIFF_ADJUSTMENT),
	initial_diff(Diff, 0).

initial_diff(Diff, 2) ->
	Diff;
initial_diff(Diff, N) ->
	{_, TimeSpent} = mine(Diff),
	%% The initial difficulty might be too easy
	%% for the machine so we mine some blocks and
	%% calibrate it before reporting the first result.
	initial_diff(switch_diff(Diff, TimeSpent), N + 1).

loop({TotalHashesTried, TotalTimeSpent}, Difficulty) ->
	{HashesTried, TimeSpent} = mine(Difficulty),
	NewDifficulty = switch_diff(Difficulty, TimeSpent),
	NewTotalHashesTried = TotalHashesTried + HashesTried,
	NewTotalTimeSpent = TotalTimeSpent + TimeSpent,
	io:format(
		"Current estimate: ~s. Since start: ~s~n",
		[
			format_hashes_per_second(HashesTried, TimeSpent),
			format_hashes_per_second(NewTotalHashesTried, NewTotalTimeSpent)
		]
	),
	loop({NewTotalHashesTried, NewTotalTimeSpent}, NewDifficulty).

mine(Diff) ->
	{Time, HashesTried} = timer:tc(fun() ->
		B = #block{
			indep_hash = crypto:hash(sha384, crypto:strong_rand_bytes(40)),
			diff = Diff,
			hash = crypto:hash(sha384, crypto:strong_rand_bytes(40)),
			timestamp = os:system_time(seconds),
			last_retarget = os:system_time(seconds),
			hash_list = [],
			height = ar_fork:height_2_2(),
			wallet_list = <<>>
		},
		ar_mine:start(B, #poa{}, [], unclaimed, [], self(), [], []),
		receive
			{work_complete, _, _, _, _, _, HashesTried} ->
				HashesTried
		end
	end),
	{HashesTried, Time}.

switch_diff(Diff, Time) ->
	MaxDiff = ar_mine:max_difficulty(),
	erlang:trunc(MaxDiff - (MaxDiff - Diff) * Time / 10000000).

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
