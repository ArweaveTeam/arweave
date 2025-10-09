-module(ar_bench_hash).

-export([run_benchmark_from_cli/1, run_benchmark/1]).

-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

run_benchmark_from_cli(Args) ->
	RandomX = get_flag_value(Args, "randomx", "512"),
	JIT = list_to_integer(get_flag_value(Args, "jit", "1")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	HardwareAES = list_to_integer(get_flag_value(Args, "hw_aes", "1")),

	RandomXMode = case RandomX of
		"512" -> rx512;
		"4096" -> rx4096;
		"squared" -> rxsquared;
		_ -> show_help()
	end,

	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	RandomXState = ar_mine_randomx:init_fast2(
		RandomXMode, ?RANDOMX_PACKING_KEY, JIT, LargePages, Schedulers),
	{H0, H1} = run_benchmark(RandomXState, JIT, LargePages, HardwareAES),
	H0String = io_lib:format("~.3f", [H0 / 1000]),
	H1String = io_lib:format("~.3f", [H1 / 1000]),
	ar:console("Hashing benchmark~nH0: ~s ms~nH1/H2: ~s ms~n", [H0String, H1String]).

get_flag_value([], _, DefaultValue) ->
    DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
    Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
    get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark-hash [options]~n"),
	io:format("Options:~n"),
	io:format("  randomx <512|4096> (default: 512)~n"),
	io:format("  jit <0|1> (default: 1)~n"),
	io:format("  large_pages <0|1> (default: 1)~n"),
	io:format("  hw_aes <0|1> (default: 1)~n"),
	init:stop(1).

run_benchmark(RandomXState) ->
	run_benchmark(RandomXState, ar_mine_randomx:jit(),
		ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes()).

run_benchmark(RandomXState, JIT, LargePages, HardwareAES) ->
	NonceLimiterOutput = crypto:strong_rand_bytes(32),
	Seed = crypto:strong_rand_bytes(32),
	MiningAddr = crypto:strong_rand_bytes(32),
	Iterations = 1000,
	{H0Time, _} = timer:tc(fun() ->
		lists:foreach(
			fun(I) ->
				Data = << NonceLimiterOutput:32/binary,
				I:256, Seed:32/binary, MiningAddr/binary >>,
				ar_mine_randomx:hash(RandomXState, Data, JIT, LargePages, HardwareAES)
			end,
			lists:seq(1, Iterations))
		end),
	H0Microseconds = H0Time / Iterations,

	H0 = crypto:strong_rand_bytes(32),
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	{H1Time, _} = timer:tc(fun() ->
		lists:foreach(
			fun(_) ->
				Nonce = rand:uniform(1000),
				Preimage = crypto:hash(sha256, << H0:32/binary, Nonce:64, Chunk/binary >>),
				crypto:hash(sha256, << H0:32/binary, Preimage/binary >>)
			end,
			lists:seq(1, Iterations))
		end),
	H1Microseconds = H1Time / Iterations,

	{H0Microseconds, H1Microseconds}.