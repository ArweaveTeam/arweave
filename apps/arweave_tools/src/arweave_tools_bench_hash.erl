%%% Command-line options for the hashing benchmark.
-module(arweave_tools_bench_hash).

-export([run_benchmark_from_cli/1]).

-include_lib("arweave_constants/include/arweave_constants.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

run_benchmark_from_cli(Args) ->
    RandomX = get_flag_value(Args, "randomx", "512"),
    JIT = list_to_integer(get_flag_value(Args, "jit", "1")),
    LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
    HardwareAES = list_to_integer(get_flag_value(Args, "hw_aes", "1")),

    RandomXMode =
        case RandomX of
            "512" -> rx512;
            "4096" -> rx4096;
            "squared" -> rxsquared;
            _ -> show_help()
        end,

    Schedulers = erlang:system_info(dirty_cpu_schedulers_online),
    RandomXState = ar_mine_randomx:init_fast2(
        RandomXMode, ?RANDOMX_PACKING_KEY, JIT, LargePages, Schedulers
    ),
    {H0, H1} = ar_bench_hash:run_benchmark(
        RandomXState, JIT, LargePages, HardwareAES
    ),
    H0String = io_lib:format("~.3f", [H0 / 1000]),
    H1String = io_lib:format("~.3f", [H1 / 1000]),
    ar:console("Hashing benchmark~nH0: ~s ms~nH1/H2: ~s ms~n", [
        H0String, H1String
    ]).

get_flag_value([], _, DefaultValue) ->
    DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when
    Flag == TargetFlag
->
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
