-module(ar_bench_vdf).

-export([run_benchmark/0]).

-include_lib("arweave/include/ar_vdf.hrl").

run_benchmark() ->
	Input = crypto:strong_rand_bytes(32),
	{Time, _} = timer:tc(fun() -> ar_vdf:compute2(1, Input, ?VDF_DIFFICULTY) end),
	io:format("~n~nVDF step computed in ~.2f seconds.~n~n", [Time / 1000000]),
	case Time > 1150000 of
		true ->
			io:format("WARNING: your VDF computation speed is low - consider fetching "
					"VDF outputs from an external source (see vdf_server_trusted_peer "
					"and vdf_client_peer command line parameters).~n~n");
		false ->
			ok
	end.