-module(ar_bench_vdf).

-export([run_benchmark/0, run_benchmark_from_cli/1]).

-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_config.hrl").

run_benchmark_from_cli(Args) ->
	Mode = list_to_atom(get_flag_value(Args, "mode", "default")),
	Difficulty = list_to_integer(get_flag_value(Args, "difficulty", integer_to_list(?VDF_DIFFICULTY))),
	Rounds = list_to_integer(get_flag_value(Args, "rounds", "1")),

	run_benchmark(Mode, Difficulty, Rounds).

get_flag_value([], _, DefaultValue) ->
	DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
	Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
	get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark vdf [options]~n"),
	io:format("Options:~n"),
	io:format("  mode <default|experimental> (default: default)~n"),
	io:format("  difficulty <vdf_difficulty> (default: ~p)~n", [?VDF_DIFFICULTY]),
	io:format("  rounds <number> (default: 1)~n"),
	erlang:halt().

run_benchmark() ->
	run_benchmark(none, ?VDF_DIFFICULTY, 1).

run_benchmark(Mode, Difficulty, Rounds) ->
	case Mode of
		none ->
			%% Run as part of startup, use whatever is set in the config
			ok;
		experimental ->
			ok = application:set_env(arweave, config, #config{enable = [ vdf_exp ]});
		default ->
			ok = application:set_env(arweave, config, #config{})
	end,
	Input = crypto:strong_rand_bytes(32),
	{FullTime, _} = timer:tc(fun() -> 
		lists:foreach(fun(_) ->
			ar_vdf:compute2(1, Input, Difficulty)
		end, lists:seq(1, Rounds))
	end),
	Time = FullTime / Rounds,
	io:format("~n~nVDF step computed in ~.2f seconds.~n~n", [Time / 1000000]),
	case Time > 1150000 of
		true ->
			io:format("WARNING: your VDF computation speed is low - consider fetching "
					"VDF outputs from an external source (see vdf_server_trusted_peer "
					"and vdf_client_peer command line parameters).~n~n");
		false ->
			ok
	end,
	Time.