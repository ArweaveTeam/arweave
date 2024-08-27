-module(ar_bench_hash).

-export([run_benchmark_from_cli/1, run_benchmark/1]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").

run_benchmark_from_cli(Args) ->
	JIT = list_to_integer(get_flag_value(Args, "jit", "1")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	HardwareAES = list_to_integer(get_flag_value(Args, "hw_aes", "1")),

	Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
	{ok, RandomXStateRef} = ar_mine_randomx:init_randomx_nif(
		?RANDOMX_PACKING_KEY, ?RANDOMX_HASHING_MODE_FAST, JIT, LargePages, Schedulers),
	{H0, H1} = run_benchmark(RandomXStateRef, JIT, LargePages, HardwareAES),
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
	io:format("  jit <0|1> (default: 1)~n"),
	io:format("  large_pages <0|1> (default: 1)~n"),
	io:format("  hw_aes <0|1> (default: 1)~n"),
	erlang:halt().

run_benchmark(RandomXStateRef) ->
	run_benchmark(RandomXStateRef, ar_mine_randomx:jit(),
		ar_mine_randomx:large_pages(), ar_mine_randomx:hardware_aes()).

run_benchmark(RandomXStateRef, JIT, LargePages, HardwareAES) ->
	NonceLimiterOutput = crypto:strong_rand_bytes(32),
	Seed = crypto:strong_rand_bytes(32),
	MiningAddr = crypto:strong_rand_bytes(32),
	Iterations = 1000,
	{H0Time, _} = timer:tc(fun() ->
		lists:foreach(
			fun(_) ->
				PartitionNumber = rand:uniform(1000),
				Data = << NonceLimiterOutput:32/binary,
					PartitionNumber:256, Seed:32/binary, MiningAddr/binary >>,
				ar_mine_randomx:hash_nif(RandomXStateRef, Data, JIT, LargePages, HardwareAES)
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