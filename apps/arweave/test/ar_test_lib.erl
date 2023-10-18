-module(ar_test_lib).

-export([start_test_application/0, start_test_application/1, stop_test_application/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

start_test_application() ->
	start_test_application(unclaimed).

start_test_application(RewardAddress) ->
	{ok, Config} = application:get_env(arweave, config),
	Disable = case os:type() of
		{unix, darwin} -> [randomx_jit];
		_ -> []
	end,
	ok = application:set_env(arweave, config, Config#config{
		mining_addr = RewardAddress,
		disable = Disable,
		enable = [search_in_rocksdb_when_mining, serve_arql, serve_wallet_txs,
			serve_wallet_deposits]
	}),
	ar:start_dependencies(),
	ok.

stop_test_application() ->
	{ok, Config} = application:get_env(arweave, config),
	ok = application:stop(arweave),
	%% Do not stop dependencies.
	os:cmd("rm -r " ++ Config#config.data_dir ++ "/*"),
	ok.
