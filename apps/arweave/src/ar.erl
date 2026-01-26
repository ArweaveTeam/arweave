%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @doc Arweave server entrypoint and basic utilities.
%%% @end
%%%===================================================================
-module(ar).
-behaviour(application).
-compile(warnings_as_errors).
-export([
	benchmark_hash/0,
	benchmark_hash/1,
	benchmark_packing/0,
	benchmark_packing/1,
	benchmark_vdf/0,
	benchmark_vdf/1,
	console/1,
	console/2,
	create_ecdsa_wallet/0,
	create_ecdsa_wallet/1,
	create_wallet/0,
	create_wallet/1,
	docs/0,
	e2e/0,
	e2e/1,
	main/1,
	prep_stop/1,
	shell/0,
	shell_e2e/0,
	shutdown/1,
	start/1,
	start/2,
	start_dependencies/0,
	stop/1,
	stop_dependencies/0,
	stop_shell/0,
	stop_shell_e2e/0,
	tests/0,
	tests/1
]).
-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_verify_chunks.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Supported feature flags (default behaviour)
% http_logging (false)
% disk_logging (false)
% miner_logging (true)
% subfield_queries (false)
% blacklist (true)
% time_syncing (true)

%%--------------------------------------------------------------------
%% @doc Command line program entrypoint. Takes a list of arguments.
%% @end
%%--------------------------------------------------------------------
main("") ->
	ar_cli_parser:show_help(),
	init:stop(1);
main(Args) ->
	% arweave_config must be the first application started, it
	% will keep the configuration for all other arweave
	% applications or processes.
	arweave_config:start(),

	% let parse the arguments and initialize arweave_config. The
	% idea here is to let full control over the configuration to
	% arweave_config and then return the correct configuration
	% file. In case of error, the application is stopped.
	case arweave_config_bootstrap:start(Args) of
		{ok, Config} ->
			start(Config);
		Else ->
			ar_cli_parser:show_help(),
			init:stop(1),
			{error, Else}
	end.

%%--------------------------------------------------------------------
%% @doc Start an Arweave node on this BEAM.
%% @end
%%--------------------------------------------------------------------
start(Port) when is_integer(Port) ->
	start(#config{ port = Port });
start(Config) ->
	%% Start the logging system.
	case os:getenv("TERM") of
		"dumb" ->
			% Set logger to output all levels of logs to the console
			% when running in a dumb terminal.
			logger:add_handler(console, logger_std_h, #{level => all});
		_->
			ok
	end,
	case ar_config:validate_config(Config) of
		true ->
			ok;
		false ->
			timer:sleep(2000),
			init:stop(1)
	end,
	Config2 = ar_config:set_dependent_flags(Config),
	ok = arweave_config:set_env(Config2),
	filelib:ensure_dir(Config2#config.log_dir ++ "/"),
	warn_if_single_scheduler(),
	case Config2#config.nonce_limiter_server_trusted_peers of
		[] ->
			VDFSpeed = ar_bench_vdf:run_benchmark(),
			?LOG_INFO([{event, vdf_benchmark}, {vdf_s, VDFSpeed / 1000000}]);
		_ ->
			ok
	end,
	start_dependencies().

%%--------------------------------------------------------------------
%% @hidden
%% @doc application `start/2' callback. function used to start arweave
%% application using `application:start/1' or while using OTP.
%% @end
%%--------------------------------------------------------------------
start(normal, _Args) ->
	% Load configuration from environment variable, it will
	% impact only feature supporting arweave_config.
	arweave_config_environment:load(),

	% Load the old configuration from arweave_config.
	{ok, Config} = arweave_config:get_env(),

	% arweave_config can now switch in runtime mode. Setting
	% parameters without "runtime" flag set to true will fail now.
	arweave_config:runtime(),

	%% Set erlang socket backend
	persistent_term:put({kernel, inet_backend}, Config#config.'socket.backend'),

	%% Configure logger
	ar_logger:init(Config),

	?LOG_INFO("========== Starting Arweave Node  =========="),
	ar_config:log_config(Config),

	%% Start the Prometheus metrics subsystem.
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),

	%% Register custom metrics.
	ar_metrics:register(),

	%% Start other apps which we depend on.
	set_mining_address(Config),
	ar_chunk_storage:run_defragmentation(),

	%% Start Arweave.
	ar_sup:start_link().

set_mining_address(#config{ mining_addr = not_set } = C) ->
	case ar_wallet:get_or_create_wallet([{?RSA_SIGN_ALG, 65537}]) of
		{error, Reason} ->
			ar:console("~nFailed to create a wallet, reason: ~p.~n",
				[io_lib:format("~p", [Reason])]),
			timer:sleep(500),
			init:stop(1);
		W ->
			Addr = ar_wallet:to_address(W),
			ar:console("~nSetting the mining address to ~s.~n", [ar_util:encode(Addr)]),
			C2 = C#config{ mining_addr = Addr },
			arweave_config:set_env(C2),
			set_mining_address(C2)
	end;
set_mining_address(#config{ mine = false }) ->
	ok;
set_mining_address(#config{ mining_addr = Addr, cm_exit_peer = CmExitPeer,
		is_pool_client = PoolClient }) ->
	case ar_wallet:load_key(Addr) of
		not_found ->
			case {CmExitPeer, PoolClient} of
				{not_set, false} ->
					ar:console("~nThe mining key for the address ~s was not found."
						" Make sure you placed the file in [data_dir]/~s (the node is looking for"
						" [data_dir]/~s/[mining_addr].json or "
						"[data_dir]/~s/arweave_keyfile_[mining_addr].json file)."
						" Do not specify \"mining_addr\" if you want one to be generated.~n~n",
						[ar_util:encode(Addr), ?WALLET_DIR, ?WALLET_DIR, ?WALLET_DIR]),
					init:stop(1);
				_ ->
					ok
			end;
		_Key ->
			ok
	end.

create_wallet([DataDir]) ->
	create_wallet(DataDir, ?RSA_KEY_TYPE);
create_wallet(_) ->
	create_wallet_fail(?RSA_KEY_TYPE).

create_ecdsa_wallet() ->
	create_wallet_fail(?ECDSA_KEY_TYPE).

create_ecdsa_wallet([DataDir]) ->
	create_wallet(DataDir, ?ECDSA_KEY_TYPE);
create_ecdsa_wallet(_) ->
	create_wallet_fail(?ECDSA_KEY_TYPE).

create_wallet(DataDir, KeyType) ->
	case filelib:is_dir(DataDir) of
		false ->
			create_wallet_fail(KeyType);
		true ->
			ok = arweave_config:set_env(#config{ data_dir = DataDir }),
			case ar_wallet:new_keyfile(KeyType) of
				{error, Reason} ->
					ar:console("Failed to create a wallet, reason: ~p.~n~n",
							[io_lib:format("~p", [Reason])]),
					timer:sleep(500),
					init:stop(1);
				W ->
					Addr = ar_wallet:to_address(W),
					ar:console("Created a wallet with address ~s.~n", [ar_util:encode(Addr)]),
					init:stop(1)
			end
	end.

create_wallet() ->
	create_wallet_fail(?RSA_KEY_TYPE).

create_wallet_fail(?RSA_KEY_TYPE) ->
	io:format("Usage: ./bin/create-wallet [data_dir]~n"),
	init:stop(1);
create_wallet_fail(?ECDSA_KEY_TYPE) ->
	io:format("Usage: ./bin/create-ecdsa-wallet [data_dir]~n"),
	init:stop(1).

benchmark_vdf() ->
	benchmark_vdf([]).
benchmark_vdf(Args) ->
	ok = arweave_config:set_env(#config{}),
	ar_bench_vdf:run_benchmark_from_cli(Args),
	init:stop(1).

benchmark_hash() ->
	benchmark_hash([]).
benchmark_hash(Args) ->
	ar_bench_hash:run_benchmark_from_cli(Args),
	init:stop(1).

benchmark_packing() ->
	benchmark_packing([]).
benchmark_packing(Args) ->
	ar_bench_packing:run_benchmark_from_cli(Args),
	init:stop(1).

shutdown([NodeName]) ->
	rpc:cast(NodeName, init, stop, []).

prep_stop(State) ->
	% the service will be stopped, ar_shutdown_manager
	% must be noticed and its state modified.
	_ = ar_shutdown_manager:shutdown(),

	% When arweave is stopped, the first step is to stop
	% accepting connections from other peers, and then
	% start the shutdown procedure.
	ok = ranch:suspend_listener(ar_http_iface_listener),

	% all timers/intervals must be stopped.
	ar_timer:terminate_timers(),
	State.

stop(_State) ->
	?LOG_INFO([{stop, ?MODULE}]).

stop_dependencies() ->
	?LOG_INFO("========== Stopping Arweave Node  =========="),
	application:stop(arweave_limiter),
	{ok, [_Kernel, _Stdlib, _SASL, _OSMon | Deps]} = application:get_key(arweave, applications),
	lists:foreach(fun(Dep) -> application:stop(Dep) end, Deps).

start_dependencies() ->
	ok = arweave_limiter:start(),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	ok.

%% One scheduler => one dirty scheduler => Calculating a RandomX hash, e.g.
%% for validating a block, will be blocked on initializing a RandomX dataset,
%% which takes minutes.
warn_if_single_scheduler() ->
	case erlang:system_info(schedulers_online) of
		1 ->
			?LOG_WARNING(
				"WARNING: Running only one CPU core / Erlang scheduler may cause issues.");
		_ ->
			ok
	end.

shell() ->
	ar_test_runner:start_shell(test).

shell_e2e() ->
	ar_test_runner:start_shell(e2e).

stop_shell() ->
	ar_test_runner:stop_shell(test).

stop_shell_e2e() ->
	ar_test_runner:stop_shell(e2e).

%% @doc Run unit tests.
%% Usage: ./bin/test [module | module:test ...]
tests()     -> ar_test_runner:run(test).
tests(Args) -> ar_test_runner:run(test, Args).

%% @doc Run e2e tests.
%% Usage: ./bin/e2e [module | module:test ...]
e2e()     -> ar_test_runner:run(e2e).
e2e(Args) -> ar_test_runner:run(e2e, Args).

%% @doc Generate the project documentation.
docs() ->
	Mods =
		lists:filter(
			fun(File) -> filename:extension(File) == ".erl" end,
			element(2, file:list_dir("apps/arweave/src"))
		),
	edoc:files(
		["apps/arweave/src/" ++ Mod || Mod <- Mods],
		[
			{dir, "source_code_docs"},
			{hidden, true},
			{private, true}
		]
	).


-ifdef(AR_TEST).
console(Format) ->
	?LOG_INFO(io_lib:format(Format, [])).

console(Format, Params) ->
	?LOG_INFO(io_lib:format(Format, Params)).
-else.
console(Format) ->
	io:format(Format).

console(Format, Params) ->
	io:format(Format, Params).
-endif.
