%%%
%%% @doc Arweave server entrypoint and basic utilities.
%%%
-module(ar).

-behaviour(application).

-export([
	main/0, main/1, start/0, start/1, start/2, stop/1, stop_dependencies/0,
	tests/0, tests/1, tests/2,
	test_ipfs/0,
	docs/0,
	start_for_tests/0,
	shutdown/1,
	console/1, console/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar,
		ar_chunk_storage,
		ar_poa,
		ar_node_utils,
		ar_meta_db,
		ar_webhook_tests,
		ar_poller_tests,
		ar_kv,
		ar_block_cache,
		ar_unbalanced_merkle,
		ar_intervals,
		ar_ets_intervals,
		ar_patricia_tree,
		ar_diff_dag,
		ar_config_tests,
		ar_deep_hash,
		ar_util,
		ar_base64_compatibility_tests,
		ar_storage,
		ar_merkle,
		ar_semaphore_tests,
		ar_tx_db,
		ar_tx,
		ar_wallet,
		ar_serialize,
		ar_block,
		ar_difficulty_tests,
		ar_retarget,
		ar_weave,
		ar_join,
		ar_tx_blacklist_tests,
		ar_data_sync_tests,
		ar_header_sync_tests,
		ar_node_tests,
		ar_fork_recovery_tests,
		ar_mine,
		ar_tx_replay_pool_tests,
		ar_tx_queue,
		ar_http_iface_tests,
		ar_multiple_txs_per_wallet_tests,
		ar_pricing,
		ar_gateway_middleware_tests,
		ar_http_util_tests,
		ar_inflation,
		ar_mine_randomx_tests
	]
).

%% Supported feature flags (default behaviour)
% http_logging (false)
% disk_logging (false)
% miner_logging (true)
% subfield_queries (false)
% blacklist (true)
% time_syncing (true)

%% @doc Command line program entrypoint. Takes a list of arguments.
main() ->
	show_help().

main("") ->
	show_help();
main(Args) ->
	start(parse_config_file(Args, [], #config{})).

show_help() ->
	io:format("Usage: arweave-server [options]~n"),
	io:format("Compatible with network: ~s~n", [?NETWORK_NAME]),
	io:format("Options:~n"),
	lists:foreach(
		fun({Opt, Desc}) ->
			io:format("\t~s~s~n",
				[
					string:pad(Opt, 40, trailing, $ ),
					Desc
				]
			)
		end,
		[
			{"config_file (path)", "Load configuration from specified file."},
			{"peer (ip:port)", "Join a network on a peer (or set of peers)."},
			{"start_from_block_index", "Start the node from the latest stored block index."},
			{"mine", "Automatically start mining once the netwok has been joined."},
			{"port", "The local port to use for mining. "
						"This port must be accessible by remote peers."},
			{"data_dir",
				"The directory for storing the weave and the wallets (when generated)."},
			{"metrics_dir", "The directory for persisted metrics."},
			{"polling (num)", lists:flatten(
					io_lib:format(
						"Poll peers for new blocks every N seconds. Default is ~p. "
						"Useful in environments where port forwarding is not possible.",
						[?DEFAULT_POLLING_INTERVAL]
					)
			)},
			{"clean", "Clear the block cache before starting."},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"mining_addr (addr)",
				"The address that mining rewards should be credited to."
				" Set 'unclaimed' to send all the rewards to the endowment pool."},
			{"stage_one_hashing_threads (num)",
				io_lib:format(
					"The number of mining processes searching for the SPoRA chunks to read."
					"Default: ~B. If the total number of stage one and stage two processes "
					"exceeds the number of available CPU cores, the excess processes will be "
					"hashing chunks when anything gets queued, and search for chunks "
					"otherwise.",
					[?NUM_STAGE_ONE_HASHING_PROCESSES]
				)},
			{"io_threads (num)",
				io_lib:format(
					"The number of processes reading SPoRA chunks during mining. Default: ~B.",
					[?NUM_IO_MINING_THREADS]
				)},
			{"stage_two_hashing_threads (num)",
				io_lib:format(
					"The number of mining processes hashing SPoRA chunks."
					"Default: ~B. If the total number of stage one and stage two processes "
					"exceeds the number of available CPU cores, the excess processes will be "
					"hashing chunks when anything gets queued, and search for chunks "
					"otherwise.",
					[?NUM_STAGE_TWO_HASHING_PROCESSES]
				)},
			{"max_emitters (num)",
				"The maximum number of transaction propagation processes (default 2)."},
			{"tx_propagation_parallelization (num)",
				"The maximum number of best peers to propagate transactions to at a time "
				"(default 4)."},
			{"max_propagation_peers (num)",
				"The maximum number of best peers to propagate blocks and transactions to. "
				"Default is 50."},
			{"sync_jobs (num)",
				io_lib:format(
					"The number of data syncing jobs to run. Default: ~B."
					" Each job periodically picks a range and downloads it from peers.",
					[?DEFAULT_SYNC_JOBS]
				)},
			{"header_sync_jobs (num)",
				io_lib:format(
					"The number of header syncing jobs to run. Default: ~B."
					" Each job periodically picks the latest not synced block header"
					" and downloads it from peers.",
					[?DEFAULT_HEADER_SYNC_JOBS]
				)},
			{"load_mining_key (file)",
				"Load the address that mining rewards should be credited to from file."},
			{"ipfs_pin", "Pin incoming IPFS tagged transactions on your local IPFS node."},
			{"transaction_blacklist (file)", "A file containing blacklisted transactions. "
											 "One Base64 encoded transaction ID per line."},
			{"transaction_blacklist_url", "An HTTP endpoint serving a transaction blacklist."},
			{"transaction_whitelist (file)", "A file containing whitelisted transactions. "
											 "One Base64 encoded transaction ID per line. "
											 "If a transaction is in both lists, it is "
											 "considered whitelisted."},
			{"transaction_whitelist_url", "An HTTP endpoint serving a transaction whitelist."},
			{"disk_space (num)",
				"Max size (in GB) for the disk partition containing "
				"the Arweave data directory (blocks, txs, etc) when "
				"the miner stops writing files to disk."},
			{"disk_space_check_frequency (num)",
				io_lib:format(
					"The frequency in seconds of requesting the information "
					"about the available disk space from the operating system, "
					"used to decide on whether to continue syncing the historical "
					"data or clean up some space. Default is ~B.",
					[?DISK_SPACE_CHECK_FREQUENCY_MS div 1000]
				)},
			{"init", "Start a new weave."},
			{"internal_api_secret (secret)",
				lists:flatten(
					io_lib:format(
						"Enables the internal API endpoints, only accessible with this secret. Min. ~B chars.",
						[?INTERNAL_API_SECRET_MIN_LEN]
					)
				)
			},
			{"enable (feature)", "Enable a specific (normally disabled) feature. For example, subfield_queries."},
			{"disable (feature)", "Disable a specific (normally enabled) feature."},
			{"gateway (domain)", "Run a gateway on the specified domain"},
			{"custom_domain (domain)", "Add a domain to the list of supported custom domains."},
			{"requests_per_minute_limit (number)", "Limit the maximum allowed number of HTTP requests per IP address per minute. Default is 900."},
			{"max_connections", "The number of connections to be handled concurrently. Its purpose is to prevent your system from being overloaded and ensuring all the connections are handled optimally. Default is 1024."},
			{"max_gateway_connections", "The number of gateway connections to be handled concurrently. Default is 128."},
			{"max_poa_option_depth",
				"The number of PoA alternatives to try until the recall data is "
				"found. Has to be an integer > 1. The mining difficulty grows linearly "
				"as a function of the alternative as (0.75 + 0.25 * number) * diff, "
				"up to (0.75 + 0.25 * max_poa_option_depth) * diff. Default is 500."},
			{"disk_pool_data_root_expiration_time",
				"The time in seconds of how long a pending or orphaned data root is kept in the disk pool. The
				default is 2 * 60 * 60 (2 hours)."},
			{"max_disk_pool_buffer_mb",
				"The max total size in mebibytes of the pending chunks in the disk pool."
				"The default is 2000 (2 GiB)."},
			{"max_disk_pool_data_root_buffer_mb",
				"The max size in mebibytes per data root of the pending chunks in the disk"
				" pool. The default is 50."},
			{"randomx_bulk_hashing_iterations",
				"The number of hashes RandomX generates before reporting the result back"
				" to the Arweave miner. The faster CPU hashes, the higher this value should be."
			},
			{"debug",
				"Enable extended logging."
			}
		]
	),
	erlang:halt().

parse_config_file(["config_file", Path | Rest], Skipped, _) ->
	case read_config_from_file(Path) of
		{ok, Config} ->
			parse_config_file(Rest, Skipped, Config);
		{error, Reason, Item} ->
			io:format("Failed to parse config: ~p: ~p.~n", [Reason, Item]),
			show_help();
		{error, Reason} ->
			io:format("Failed to parse config: ~p.~n", [Reason]),
			show_help()
	end;
parse_config_file([Arg | Rest], Skipped, Config) ->
	parse_config_file(Rest, [Arg | Skipped], Config);
parse_config_file([], Skipped, Config) ->
	Args = lists:reverse(Skipped),
	parse_cli_args(Args, Config).

read_config_from_file(Path) ->
	case file:read_file(Path) of
		{ok, FileData} -> ar_config:parse(FileData);
		{error, _} -> {error, file_unreadable, Path}
	end.

parse_cli_args([], C) -> C;
parse_cli_args(["mine"|Rest], C) ->
	parse_cli_args(Rest, C#config { mine = true });
parse_cli_args(["peer", Peer|Rest], C = #config { peers = Ps }) ->
	case ar_util:safe_parse_peer(Peer) of
		{ok, ValidPeer} ->
			parse_cli_args(Rest, C#config { peers = [ValidPeer|Ps] });
		{error, _} ->
			io:format("Peer ~p invalid ~n", [Peer]),
			parse_cli_args(Rest, C)
	end;
parse_cli_args(["transaction_blacklist", File|Rest], C = #config { transaction_blacklist_files = Files } ) ->
	parse_cli_args(Rest, C#config { transaction_blacklist_files = [File|Files] });
parse_cli_args(["transaction_blacklist_url", URL | Rest], C = #config { transaction_blacklist_urls = URLs} ) ->
	parse_cli_args(Rest, C#config{ transaction_blacklist_urls = [URL | URLs] });
parse_cli_args(["transaction_whitelist", File|Rest], C = #config { transaction_whitelist_files = Files } ) ->
	parse_cli_args(Rest, C#config { transaction_whitelist_files = [File|Files] });
parse_cli_args(["transaction_whitelist_url", URL | Rest], C = #config { transaction_whitelist_urls = URLs} ) ->
	parse_cli_args(Rest, C#config { transaction_whitelist_urls = [URL | URLs] });
parse_cli_args(["port", Port|Rest], C) ->
	parse_cli_args(Rest, C#config { port = list_to_integer(Port) });
parse_cli_args(["data_dir", DataDir|Rest], C) ->
	parse_cli_args(Rest, C#config { data_dir = DataDir });
parse_cli_args(["metrics_dir", MetricsDir|Rest], C) ->
	parse_cli_args(Rest, C#config { metrics_dir = MetricsDir });
parse_cli_args(["polling", Frequency|Rest], C) ->
	parse_cli_args(Rest, C#config { polling = list_to_integer(Frequency) });
parse_cli_args(["clean"|Rest], C) ->
	parse_cli_args(Rest, C#config { clean = true });
parse_cli_args(["no_auto_join"|Rest], C) ->
	parse_cli_args(Rest, C#config { auto_join = false });
parse_cli_args(["mining_addr", "unclaimed"|Rest], C) ->
	parse_cli_args(Rest, C#config { mining_addr = unclaimed });
parse_cli_args(["mining_addr", Addr|Rest], C) ->
	parse_cli_args(Rest, C#config { mining_addr = ar_util:decode(Addr) });
parse_cli_args(["max_miners", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_miners = list_to_integer(Num) });
parse_cli_args(["io_threads", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { io_threads = list_to_integer(Num) });
parse_cli_args(["stage_one_hashing_threads", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { stage_one_hashing_threads = list_to_integer(Num) });
parse_cli_args(["stage_two_hashing_threads", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { stage_two_hashing_threads = list_to_integer(Num) });
parse_cli_args(["max_emitters", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_emitters = list_to_integer(Num) });
parse_cli_args(["disk_space", Size|Rest], C) ->
	parse_cli_args(Rest, C#config { disk_space = (list_to_integer(Size) * 1024 * 1024 * 1024) });
parse_cli_args(["disk_space_check_frequency", Frequency|Rest], C) ->
	parse_cli_args(Rest, C#config{
		disk_space_check_frequency = list_to_integer(Frequency) * 1000
	});
parse_cli_args(["load_mining_key", File|Rest], C) ->
	parse_cli_args(Rest, C#config { load_key = File });
parse_cli_args(["ipfs_pin" | Rest], C) ->
	parse_cli_args(Rest, C#config { ipfs_pin = true });
parse_cli_args(["start_from_block_index"|Rest], C) ->
	parse_cli_args(Rest, C#config { start_from_block_index = true });
parse_cli_args(["init"|Rest], C)->
	parse_cli_args(Rest, C#config { init = true });
parse_cli_args(["internal_api_secret", Secret | Rest], C) when length(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse_cli_args(Rest, C#config { internal_api_secret = list_to_binary(Secret)});
parse_cli_args(["internal_api_secret", _ | _], _) ->
	io:format(
		"~nThe internal_api_secret must be at least ~B characters long.~n~n",
		[?INTERNAL_API_SECRET_MIN_LEN]
	),
	erlang:halt();
parse_cli_args(["enable", Feature | Rest ], C = #config { enable = Enabled }) ->
	parse_cli_args(Rest, C#config { enable = [ list_to_atom(Feature) | Enabled ] });
parse_cli_args(["disable", Feature | Rest ], C = #config { disable = Disabled }) ->
	parse_cli_args(Rest, C#config { disable = [ list_to_atom(Feature) | Disabled ] });
parse_cli_args(["gateway", Domain | Rest ], C) ->
	parse_cli_args(Rest, C#config { gateway_domain = list_to_binary(Domain) });
parse_cli_args(["custom_domain", Domain|Rest], C = #config { gateway_custom_domains = Ds }) ->
	parse_cli_args(Rest, C#config { gateway_custom_domains = [ list_to_binary(Domain) | Ds ] });
parse_cli_args(["requests_per_minute_limit", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { requests_per_minute_limit = list_to_integer(Num) });
parse_cli_args(["max_propagation_peers", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_propagation_peers = list_to_integer(Num) });
parse_cli_args(["sync_jobs", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { sync_jobs = list_to_integer(Num) });
parse_cli_args(["header_sync_jobs", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { header_sync_jobs = list_to_integer(Num) });
parse_cli_args(["tx_propagation_parallelization", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { tx_propagation_parallelization = list_to_integer(Num) });
parse_cli_args(["max_connections", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_connections = list_to_integer(Num) });
parse_cli_args(["max_gateway_connections", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_gateway_connections = list_to_integer(Num) });
parse_cli_args(["max_poa_option_depth", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_poa_option_depth = list_to_integer(Num) });
parse_cli_args(["disk_pool_data_root_expiration_time", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { disk_pool_data_root_expiration_time = list_to_integer(Num) });
parse_cli_args(["max_disk_pool_buffer_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_disk_pool_buffer_mb = list_to_integer(Num) });
parse_cli_args(["max_disk_pool_data_root_buffer_mb", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_disk_pool_data_root_buffer_mb = list_to_integer(Num) });
parse_cli_args(["randomx_bulk_hashing_iterations", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { randomx_bulk_hashing_iterations = list_to_integer(Num) });
parse_cli_args(["debug" | Rest], C) ->
	parse_cli_args(Rest, C#config { debug = true });
parse_cli_args([Arg|_Rest], _O) ->
	io:format("~nUnknown argument: ~s.~n", [Arg]),
	show_help().

%% @doc Start an Arweave node on this BEAM.
start() ->
	start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) ->
	start(#config { port = Port });
start(Config) ->
	%% Start the logging system.
	filelib:ensure_dir(?LOG_DIR ++ "/"),
	warn_if_single_scheduler(),
	ok = application:set_env(arweave, config, Config),
	{ok, _} = application:ensure_all_started(arweave, permanent).

start(normal, _Args) ->
	{ok, Config} = application:get_env(arweave, config),
	%% Configure logging for console output.
	LoggerFormatterConsole = #{
		legacy_header => false,
		single_line => true,
		chars_limit => 2048,
		max_size => 512,
		depth => 64,
		template => [time," [",level,"] ",file,":",line," ",msg,"\n"]
	},
	logger:set_handler_config(default, formatter, {logger_formatter, LoggerFormatterConsole}),
	logger:set_handler_config(default, level, error),
	%% Configure logging to the logfile.
	LoggerConfigDisk = #{
		file => lists:flatten("logs/" ++ atom_to_list(node())),
		type => wrap,
		max_no_files => 10,
		max_no_bytes => 51418800 % 10 x 5MB
	},
	logger:add_handler(
		disk_log,
		logger_disk_log_h,
		#{ config => LoggerConfigDisk, level => info }
	),
	LoggerFormatterDisk = #{
		chars_limit => 512,
		max_size => 512,
		depth => 32,
		legacy_header => false,
		single_line => true,
		template => [time," [",level,"] ",file,":",line," ",msg,"\n"]
	},
	logger:set_handler_config(disk_log, formatter, {logger_formatter, LoggerFormatterDisk}),
	logger:set_application_level(arweave, info),
	%% Start the Prometheus metrics subsystem.
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),
	%% Register custom metrics.
	ar_metrics:register(Config#config.metrics_dir),
	%% Verify port collisions when gateway is enabled.
	case {Config#config.port, Config#config.gateway_domain} of
		{P, D} when is_binary(D) andalso (P == 80 orelse P == 443) ->
			io:format(
				"~nThe port must be different than 80 or 443 when the gateway is enabled.~n~n"),
			erlang:halt();
		_ ->
			do_nothing
	end,
	validate_trusted_peers(Config),
	%% Start other apps which we depend on.
	ok = prepare_graphql(),
	case Config#config.ipfs_pin of
		false -> ok;
		true  -> app_ipfs:start_pinning()
	end,
	set_mining_address(Config),
	%% Start Arweave.
	ar_sup:start_link().

validate_trusted_peers(#config{ peers = [] }) ->
	ok;
validate_trusted_peers(Config) ->
	Peers = Config#config.peers,
	ValidPeers = filter_valid_peers(Peers),
	case ValidPeers of
		[] ->
			erlang:halt();
		_ ->
			application:set_env(arweave, config, Config#config{ peers = ValidPeers }),
			case lists:member(time_syncing, Config#config.disable) of
				false ->
					validate_clock_sync(ValidPeers);
				true ->
					ok
			end
	end.

%% @doc Verify peers are on the same network as us.
filter_valid_peers(Peers) ->
	lists:filter(
		fun(Peer) ->
			case ar_http_iface_client:get_info(Peer, name) of
				info_unavailable ->
					io:format("~n\tPeer ~s is not available.~n~n", [ar_util:format_peer(Peer)]),
					false;
				<<?NETWORK_NAME>> ->
					true;
				_ ->
					io:format(
						"~n\tPeer ~s does not belong to the network ~s.~n~n",
						[ar_util:format_peer(Peer), ?NETWORK_NAME]
					),
					false
			end
		end,
		Peers
	).

%% @doc Validate our clocks are in sync with the trusted peers' clocks.
validate_clock_sync(Peers) ->
	ValidatePeerClock = fun(Peer) ->
		case ar_http_iface_client:get_time(Peer, 5 * 1000) of
			{ok, {RemoteTMin, RemoteTMax}} ->
				LocalT = os:system_time(second),
				Tolerance = ?JOIN_CLOCK_TOLERANCE,
				case LocalT of
					T when T < RemoteTMin - Tolerance ->
						log_peer_clock_diff(Peer, RemoteTMin - Tolerance - T),
						false;
					T when T < RemoteTMin - Tolerance div 2 ->
						log_peer_clock_diff(Peer, RemoteTMin - T),
						true;
					T when T > RemoteTMax + Tolerance ->
						log_peer_clock_diff(Peer, T - RemoteTMax - Tolerance),
						false;
					T when T > RemoteTMax + Tolerance div 2 ->
						log_peer_clock_diff(Peer, T - RemoteTMax),
						true;
					_ ->
						true
				end;
			{error, Err} ->
				ar:console(
					"Failed to get time from peer ~s: ~p.",
					[ar_util:format_peer(Peer), Err]
				),
				false
		end
	end,
	Responses = ar_util:pmap(ValidatePeerClock, [P || P <- Peers, not is_pid(P)]),
	case lists:all(fun(R) -> R end, Responses) of
		true ->
			ok;
		false ->
			io:format(
				"~n\tInvalid peers. A valid peer must be part of the"
				" network ~s and its clock must deviate from ours by no"
				" more than ~B seconds.~n", [?NETWORK_NAME, ?JOIN_CLOCK_TOLERANCE]
			),
			erlang:halt()
	end.

log_peer_clock_diff(Peer, Diff) ->
	Warning = "Your local clock deviates from peer ~s by ~B seconds or more.",
	WarningArgs = [ar_util:format_peer(Peer), Diff],
	io:format(Warning, WarningArgs),
	?LOG_WARNING(Warning, WarningArgs).

set_mining_address(Config) ->
	MiningAddr =
		case {Config#config.mining_addr, Config#config.load_key} of
			{not_set, not_set} ->
				{_, Pub} = ar_wallet:new_keyfile(),
				ar_wallet:to_address(Pub);
			{unclaimed, not_set} ->
				unclaimed;
			{Address, _} when is_binary(Address) ->
				Address;
			{_, Path} when is_binary(Path) ->
				{_, Pub} = ar_wallet:load_keyfile(Path),
				ar_wallet:to_address(Pub)
		end,
	application:set_env(arweave, config, Config#config{ mining_addr = MiningAddr }).

shutdown([NodeName]) ->
	rpc:cast(NodeName, init, stop, []).

stop(_State) ->
	ok.

stop_dependencies() ->
	{ok, [_Kernel, _Stdlib, _SASL, _OSMon | Deps]} = application:get_key(arweave, applications),
	lists:foreach(fun(Dep) -> ok = application:stop(Dep) end, Deps).

prepare_graphql() ->
	ok = ar_graphql:load_schema(),
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

%% @doc Run all of the tests associated with the core project.
tests() ->
	tests(?CORE_TEST_MODS, #config {}).

tests(Mods, Config) when is_list(Mods) ->
	start_for_tests(Config),
	case eunit:test({timeout, ?TEST_TIMEOUT, [Mods]}, [verbose]) of
		ok ->
			ok;
		_ ->
			exit(tests_failed)
	end.

start_for_tests() ->
	start_for_tests(#config { }).

start_for_tests(Config) ->
	start(Config#config {
		peers = [],
		data_dir = "data_test_master",
		metrics_dir = "metrics_master",
		disable = [randomx_jit]
	}).

%% @doc Run the tests for a set of module(s).
%% Supports strings so that it can be trivially induced from a unix shell call.
tests(Mod) when not is_list(Mod) -> tests([Mod]);
tests(Args) ->
	Mods =
		lists:map(
			fun(Mod) when is_atom(Mod) -> Mod;
			   (Str) -> list_to_atom(Str)
			end,
			Args
		),
	tests(Mods, #config {}).

%% @doc Run the tests for the IPFS integration. Requires a running local IPFS node.
test_ipfs() ->
	Mods = [app_ipfs_tests],
	tests(Mods, #config{}).

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

%% @doc Ensure that parsing of core command line options functions correctly.
commandline_parser_test_() ->
	{timeout, 20, fun() ->
		Addr = crypto:strong_rand_bytes(32),
		Tests =
			[
				{"peer 1.2.3.4 peer 5.6.7.8:9", #config.peers, [{5,6,7,8,9},{1,2,3,4,1984}]},
				{"mine", #config.mine, true},
				{"port 22", #config.port, 22},
				{"mining_addr "
					++ binary_to_list(ar_util:encode(Addr)), #config.mining_addr, Addr}
			],
		X = string:split(string:join([ L || {L, _, _} <- Tests ], " "), " ", all),
		C = parse_cli_args(X, #config{}),
		lists:foreach(
			fun({_, Index, Value}) ->
				?assertEqual(element(Index, C), Value)
			end,
			Tests
		)
	end}.

-ifdef(DEBUG).
console(_) ->
	ok.

console(_, _) ->
	ok.
-else.
console(Format) ->
	io:format(Format).

console(Format, Params) ->
	io:format(Format, Params).
-endif.
