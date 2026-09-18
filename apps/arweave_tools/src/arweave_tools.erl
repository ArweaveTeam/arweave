%%% Command-line entry point for offline doctor commands and benchmarks.
-module(arweave_tools).

-export([main/0, main/1, run/1]).

%% @doc Run the command supplied through the launcher's plain arguments.
main() ->
    main(init:get_plain_arguments()).

%% @doc Run a command and stop the VM with its exit status.
main(Args) ->
    init:stop(run(Args)).

%% @doc Dispatch a command and return its exit status on completion.
run(["doctor" | Args]) ->
    arweave_tools_doctor:main(Args);
run(["benchmark", "hash" | Args]) ->
    arweave_tools_bench_hash:run_benchmark_from_cli(Args),
    %% Preserve the existing benchmark CLI exit status.
    1;
run(["benchmark", "packing" | Args]) ->
    arweave_tools_bench_packing:run_benchmark_from_cli(Args),
    1;
run(["benchmark", "vdf" | Args]) ->
    arweave_tools_bench_vdf:run_benchmark_from_cli(Args),
    1;
run(_) ->
    ar:console("Usage: arweave doctor <command> [args]~n"),
    ar:console("       arweave benchmark <hash|packing|vdf> [args]~n"),
    1.
