%%% Command-line options for the VDF benchmark.
-module(arweave_tools_bench_vdf).

-export([run_benchmark_from_cli/1]).

-include_lib("arweave_constants/include/arweave_constants.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

run_benchmark_from_cli(Args) ->
    Mode = list_to_atom(get_flag_value(Args, "mode", "default")),
    Difficulty = list_to_integer(
        get_flag_value(Args, "difficulty", integer_to_list(?VDF_DIFFICULTY))
    ),
    Verify = list_to_atom(get_flag_value(Args, "verify", "false")),

    case Difficulty < ?MIN_VDF_DIFFICULTY of
        true ->
            io:format(
                "~nThe VDF difficulty must be at least ~p, got ~p.~n",
                [?MIN_VDF_DIFFICULTY, Difficulty]
            ),
            show_help();
        false ->
            ar_bench_vdf:run_benchmark(Mode, Difficulty, Verify)
    end.

get_flag_value([], _, DefaultValue) ->
    DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when
    Flag == TargetFlag
->
    Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
    get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
    io:format("~nUsage: benchmark vdf [options]~n"),
    io:format("Options:~n"),
    io:format("  mode <default|openssl|fused|hiopt_m4> (default: default)~n"),
    io:format(
        "  difficulty <vdf_difficulty> (minimum: ~p, default: ~p)~n",
        [?MIN_VDF_DIFFICULTY, ?VDF_DIFFICULTY]
    ),
    io:format("  verify <true|false> (default: false)~n"),
    init:stop(1).
