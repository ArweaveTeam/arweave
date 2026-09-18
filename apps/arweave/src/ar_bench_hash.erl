%%% Runtime hashing calibration shared with the command-line benchmark.
-module(ar_bench_hash).

-export([run_benchmark/1, run_benchmark/4]).

-include_lib("arweave_constants/include/arweave_constants.hrl").

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
