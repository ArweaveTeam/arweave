-module(ar_bench_hash).

-export([run_benchmark/0]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").

run_benchmark() ->
	NonceLimiterOutput = crypto:strong_rand_bytes(32),
	Seed = crypto:strong_rand_bytes(32),
	MiningAddr = crypto:strong_rand_bytes(32),
	Iterations = 1000,
	{H0Time, _} = timer:tc(fun() ->
		lists:foreach(
			fun(_) ->
				ar_block:compute_h0(NonceLimiterOutput, rand:uniform(1000), Seed, MiningAddr)
			end,
			lists:seq(1, Iterations))
		end),
	H0Microseconds = H0Time / Iterations,

	H0 = crypto:strong_rand_bytes(32),
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	{H1Time, _} = timer:tc(fun() ->
		lists:foreach(
			fun(_) ->
				ar_block:compute_h1(H0, rand:uniform(1000), Chunk)
			end,
			lists:seq(1, Iterations))
		end),
	H1Microseconds = H1Time / Iterations,

	{H0Microseconds, H1Microseconds}.