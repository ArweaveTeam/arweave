-module(ar_bench_vdf).

-export([run_benchmark/0, run_benchmark_from_cli/1]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_config.hrl").

run_benchmark_from_cli(Args) ->
	Mode = list_to_atom(get_flag_value(Args, "mode", "default")),
	Difficulty = list_to_integer(get_flag_value(Args, "difficulty", integer_to_list(?VDF_DIFFICULTY))),
	Verify = list_to_atom(get_flag_value(Args, "verify", "false")),

	run_benchmark(Mode, Difficulty, Verify).

get_flag_value([], _, DefaultValue) ->
	DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
	Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
	get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark vdf [options]~n"),
	io:format("Options:~n"),
	io:format("  mode <default|openssl|fused|hiopt_m4> (default: default)~n"),
	io:format("  difficulty <vdf_difficulty> (default: ~p)~n", [?VDF_DIFFICULTY]),
	io:format("  verify <true|false> (default: false)~n"),
	init:stop(1).

run_benchmark() ->
	run_benchmark(none, ?VDF_DIFFICULTY, false).

run_benchmark(Mode, Difficulty, Verify) ->
	case Mode of
		none ->
			%% Run as part of startup, use whatever is set in the config
			ok;
		openssl ->
			arweave_config:set(vdf, openssl);
		fused ->
			arweave_config:set(vdf, fused);
		hiopt_m4 ->
			arweave_config:set(vdf, hiopt_m4);
		default ->
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
				[ar_util:encode(Output), ar_util:encode(VerifyOutput)])
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
