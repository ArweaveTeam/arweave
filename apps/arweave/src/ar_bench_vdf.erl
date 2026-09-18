%%% Runtime VDF calibration shared with the command-line benchmark.
-module(ar_bench_vdf).

-export([run_benchmark/0, run_benchmark/3]).

-include_lib("arweave_constants/include/arweave_constants.hrl").

run_benchmark() ->
    run_benchmark(none, ?VDF_DIFFICULTY, false).

run_benchmark(Mode, Difficulty, Verify) ->
    case Mode of
        none ->
            %% Run as part of startup, use whatever is set in the config
            ok;
        openssl ->
            _ = arweave_config:set([vdf, algorithm], openssl),
            ok;
        fused ->
            _ = arweave_config:set([vdf, algorithm], fused),
            ok;
        hiopt_m4 ->
            _ = arweave_config:set([vdf, algorithm], hiopt_m4),
            ok;
        default ->
            %% Fall back to whatever is currently set; the options registry
            %% already applies the declared default when no explicit
            %% value has been written.
            ok
    end,
    Input = crypto:strong_rand_bytes(32),
    {Time, {ok, Output, Checkpoints}} = timer:tc(fun() ->
                                                         ar_vdf:compute2(1, Input, Difficulty)
                                                 end),
    io:format("~n~n"),
    maybe_verify(Verify, Input, Difficulty, Output, Checkpoints),
    io:format("VDF step computed in ~.2f seconds.~n~n", [Time / 1000000]),
    case Time > 1150000 of
        true ->
            io:format("WARNING: your VDF computation speed is low - consider fetching "
                      "VDF outputs from an external source (see vdf_server_trusted_peer "
                      "and vdf_client_peer command line parameters).~n~n");
        false ->
            ok
    end,
    Time.

maybe_verify(true, Input, Difficulty, Output, Checkpoints) ->
    {ok, VerifyOutput, VerifyCheckpoints} = ar_vdf:debug_sha2(1, Input, Difficulty),
    case Output == VerifyOutput of
        true ->
            io:format("Output matches.~n");
        false ->
            io:format("Output mismatch. Expected: ~p, Got: ~p~n",
                      [arweave_util:encode(Output), arweave_util:encode(VerifyOutput)])
    end,
    case Checkpoints == VerifyCheckpoints of
        true ->
            io:format("Checkpoints match.~n");
        false ->
            io:format("Checkpoints mismatch. Expected: ~p, Got: ~p~n",
                      [Checkpoints, VerifyCheckpoints])
    end;
maybe_verify(false, _Input, _Difficulty, _Output, _Checkpoints) ->
    ok.
