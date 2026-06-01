%%% @doc Arweave server entrypoint and basic utilities.
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
	main/0,
	main/1,
	prep_stop/1,
	shell/0,
	shell_e2e/0,
	shell_localnet/0,
	shell_localnet/1,
	shutdown/1,
	start/2,
	start_dependencies/0,
	stop/1,
	stop_dependencies/0,
	stop_shell/0,
	stop_shell_e2e/0,
	stop_shell_localnet/0,
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
%% No CLI args — boot purely from `AR_*' env vars (and the config
%% file if one is configured).
main() ->
	main([]).

main(Args) ->
	% arweave_config must be the first application started, it
	% will keep the configuration for all other arweave
	% applications or processes.
	arweave_config:start(),

	%% Parse arguments into the options registry. arweave_config:bootstrap
	%% writes every input into the store directly; consumers read it
	%% back through arweave_config:get/1.
	case arweave_config:bootstrap(Args) of
		ok ->
			start_dependencies();
		Else ->
			arweave_config:show_cli_help(),
			init:stop(1),
			{error, Else}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc application `start/2' callback. Owns the full Arweave node
%% boot sequence. Triggered by `application:ensure_all_started(arweave)`
%% from `start_dependencies/0` once `arweave_config` is populated.
%% @end
%%--------------------------------------------------------------------
start(normal, _Args) ->
	%% Post-parse fixups on the loaded config (drains legacy
	%% enable/disable feature lists, promotes start_from_state, etc.).
	ok = arweave_config:normalize(),

	%% Boot-time prerequisites that must be in place before any
	%% supervisor child starts reading config.
	LogDir = arweave_config:get([log_dir]),
	filelib:ensure_dir(LogDir ++ "/"),
	warn_if_single_scheduler(),
	maybe_run_vdf_benchmark(),
	maybe_install_dumb_term_logger(),

	%% Set erlang socket backend
	SocketBackend = arweave_config:get([network, server, socket_backend]),
	persistent_term:put({kernel, inet_backend}, SocketBackend),

	%% Configure logger
	ar_logger:init(),

	?LOG_INFO("========== Starting Arweave Node  =========="),
	arweave_config:log(),

	%% Start the Prometheus metrics subsystem.
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),

	%% Register custom metrics.
	ar_metrics:register(),

	%% Start other apps which we depend on.
	set_mining_address(),
	ar_chunk_storage:run_defragmentation(),

	%% Start Arweave. Supervisor children may run boot-time validators
	%% in their init/1 callbacks that mutate static config (e.g.,
	%% ar_node_worker filters and rewrites the trusted-peer list). The
	%% supervisor tree's child order guarantees those validators run
	%% before any downstream consumer reads the config.
	Result = ar_sup:start_link(),

	%% All boot-time mutations are done. Flip to runtime mode so any
	%% subsequent write to a static spec is rejected.
	case arweave_config:runtime() of
		ok ->
			ok;
		{error, Reason} ->
			io:format("~nConfiguration validation failed: ~p~n~n", [Reason]),
			timer:sleep(2000),
			init:stop(1)
	end,

	Result.

set_mining_address() ->
	MiningAddr = arweave_config:get([mining, address]),
	case MiningAddr of
		not_set ->
			case ar_wallet:get_or_create_wallet([{?RSA_SIGN_ALG, 65537}]) of
				{error, Reason} ->
					ar:console("~nFailed to create a wallet, reason: ~p.~n",
						[io_lib:format("~p", [Reason])]),
					timer:sleep(500),
					init:stop(1);
				W ->
					Addr = ar_wallet:to_address(W),
					ar:console("~nSetting the mining address to ~s.~n",
						[ar_util:encode(Addr)]),
					_ = arweave_config:set([mining, address], Addr),
					verify_mining_keyfile(Addr)
			end;
		Addr ->
			Mine = arweave_config:get([mining, enabled]),
			case Mine of
				false -> ok;
				true  -> verify_mining_keyfile(Addr)
			end
	end.

verify_mining_keyfile(Addr) ->
	case ar_wallet:load_key(Addr) of
		not_found ->
			CMExitPeer = arweave_config:get([peers, cm_exit]),
			PoolClient = arweave_config:get([pool, is_client]),
			case {CMExitPeer, PoolClient} of
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
			_ = arweave_config:set([data_dir], DataDir),
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

	% Deregister from epmd early, before the slow RocksDB teardown.
	% Otherwise, if the BEAM dies during shutdown, epmd retains a stale
	% registration that blocks restarts. Skipped in test builds —
	% see `maybe_stop_net_kernel/0'.
	maybe_stop_net_kernel(),
	State.

stop(_State) ->
	?LOG_INFO([{stop, ?MODULE}]).

%% Production builds tear down distribution so a stale epmd entry
%% can't block a restart after a BEAM death. Tests do not — between
%% tests `ar_test_node` only stops the `arweave` app and expects to
%% drive the same BEAM via subsequent `remote_call's; stopping
%% distribution here turns those into `{badrpc, nodedown}'.
-ifdef(AR_TEST).
maybe_stop_net_kernel() -> ok.
-else.
maybe_stop_net_kernel() ->
	catch net_kernel:stop(),
	ok.
-endif.

stop_dependencies() ->
	?LOG_INFO("========== Stopping Arweave Node  =========="),
	application:stop(arweave_limiter),
	{ok, [_Kernel, _Stdlib, _SASL, _OSMon | Deps]} = application:get_key(arweave, applications),
	lists:foreach(fun(Dep) -> application:stop(Dep) end, lists:reverse(Deps)).

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

%% Run the VDF benchmark when no trusted VDF peers are configured so
%% the node has a local speed estimate before joining.
maybe_run_vdf_benchmark() ->
	case arweave_config:get([peers, vdf_server]) of
		[] ->
			VDFSpeed = ar_bench_vdf:run_benchmark(),
			?LOG_INFO([{event, vdf_benchmark}, {vdf_s, VDFSpeed / 1000000}]);
		_ ->
			ok
	end.

%% In a dumb terminal (no TTY capabilities) the default logger handler
%% suppresses output; install a console handler so the operator still
%% sees node logs.
maybe_install_dumb_term_logger() ->
	case os:getenv("TERM") of
		"dumb" ->
			logger:add_handler(console, logger_std_h, #{level => all});
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

shell_localnet() ->
	shell_localnet([]).

shell_localnet(Args) ->
	try
		case Args of
			[] ->
				ar_localnet:start(),
				io:format("Shell is ready.~n");
			[SnapshotDir] ->
				ar_localnet:start(SnapshotDir),
				io:format("Shell is ready.~n");
			_ ->
				io:format("Usage: ./bin/localnet_shell [snapshot_dir]~n"),
				erlang:error({invalid_args, Args})
		end
	catch
		Type:Reason:S ->
			io:format("Failed to start localnet due to ~p:~p:~p~n", [Type, Reason, S]),
			init:stop(1)
	end.

stop_shell_localnet() ->
	ar_test_node:stop(),
	init:stop().

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
