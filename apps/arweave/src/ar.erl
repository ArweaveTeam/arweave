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
	err/1, err/2, info/1, info/2, warn/1, warn/2, console/1, console/2,
	report/1, report_console/1, d/1,
	scale_time/1,
	start_for_tests/0,
	fixed_diff_option/0, fixed_delay_option/0,
	shutdown/1
]).

-include("ar.hrl").
-include("ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar,
		ar_meta_db,
		ar_block_cache,
		ar_unbalanced_merkle,
		ar_intervals,
		ar_patricia_tree,
		ar_diff_dag,
		ar_config_tests,
		ar_deep_hash,
		ar_inflation,
		ar_util,
		ar_base64_compatibility_tests,
		ar_storage,
		ar_merkle,
		ar_semaphore_tests,
		ar_tx_db,
		ar_tx,
		ar_wallet,
		ar_gossip,
		ar_serialize,
		ar_block,
		ar_difficulty_tests,
		ar_retarget,
		ar_weave,
		ar_join,
		ar_tx_blacklist_tests,
		ar_data_sync_tests,
		ar_header_sync_tests,
		ar_poa_tests,
		ar_node_tests,
		ar_fork_recovery_tests,
		ar_mine,
		ar_tx_replay_pool_tests,
		ar_tx_queue,
		ar_http_iface_tests,
		ar_multiple_txs_per_wallet_tests,
		ar_tx_perpetual_storage_tests,
		ar_gateway_middleware_tests,
		ar_http_util_tests,
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
			{"data_dir", "The directory for storing the weave and the wallets (when generated)."},
			{"metrics_dir", "The directory for persisted metrics."},
			{"polling", "Poll peers for new blocks every 10 seconds. "
						"Useful in environments where "
						"port forwarding is not possible. "
						"When the flag is not set, the node still polls "
						"if it does not receive blocks for a minute."},
			{"clean", "Clear the block cache before starting."},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"mining_addr (addr)", "The address that mining rewards should be credited to."},
			{"max_miners (num)", "The maximum number of mining processes."},
			{"max_emitters (num)", "The maximum number of transaction propagation processes (default 2)."},
			{"tx_propagation_parallelization (num)", "The maximum number of best peers to propagate transactions to at a time (default 4)."},
			{"max_propagation_peers (number)", "The maximum number of best peers to propagate blocks and transactions to. Default is 50."},
			{"new_mining_key", "Generate a new keyfile, apply it as the reward address"},
			{"load_mining_key (file)", "Load the address that mining rewards should be credited to from file."},
			{"ipfs_pin", "Pin incoming IPFS tagged transactions on your local IPFS node."},
			{"transaction_blacklist (file)", "A file containing blacklisted transactions. "
											 "One Base64 encoded transaction ID per line."},
			{"transaction_blacklist_url", "An HTTP endpoint serving a transaction blacklist."},
			{"transaction_whitelist (file)", "A file containing whitelisted transactions. "
											 "One Base64 encoded transaction ID per line. "
											 "If a transaction is in both lists, it is "
											 "considered whitelisted."},
			{"disk_space (num)", "Max size (in GB) for the disk partition containing the Arweave data directory (blocks, txs, etc) when the miner stops writing files to disk."},
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
			{"max_poa_option_depth", "The number of PoA alternatives to try until the recall data is found. Has to be an integer > 1. The mining difficulty increases exponentially with each subsequent option. Default is 8."},
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
parse_cli_args(["port", Port|Rest], C) ->
	parse_cli_args(Rest, C#config { port = list_to_integer(Port) });
parse_cli_args(["data_dir", DataDir|Rest], C) ->
	parse_cli_args(Rest, C#config { data_dir = DataDir });
parse_cli_args(["metrics_dir", MetricsDir|Rest], C) ->
	parse_cli_args(Rest, C#config { metrics_dir = MetricsDir });
parse_cli_args(["polling"|Rest], C) ->
	parse_cli_args(Rest, C#config { polling = true });
parse_cli_args(["clean"|Rest], C) ->
	parse_cli_args(Rest, C#config { clean = true });
parse_cli_args(["no_auto_join"|Rest], C) ->
	parse_cli_args(Rest, C#config { auto_join = false });
parse_cli_args(["mining_addr", Addr|Rest], C) ->
	parse_cli_args(Rest, C#config { mining_addr = ar_util:decode(Addr) });
parse_cli_args(["max_miners", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_miners = list_to_integer(Num) });
parse_cli_args(["max_emitters", Num|Rest], C) ->
	parse_cli_args(Rest, C#config { max_emitters = list_to_integer(Num) });
parse_cli_args(["new_mining_key"|Rest], C)->
	parse_cli_args(Rest, C#config { new_key = true });
parse_cli_args(["disk_space", Size|Rest], C) ->
	parse_cli_args(Rest, C#config { disk_space = (list_to_integer(Size)*1024*1024*1024) });
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
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),
	warn_if_single_scheduler(),
	ok = application:set_env(arweave, config, Config),
	ok = application:set_env(arweave, logfile, Filename),
	{ok, _} = application:ensure_all_started(arweave, permanent).

start(normal, _Args) ->
	{ok, #config {
		init = Init,
		port = Port,
		data_dir = DataDir,
		metrics_dir = MetricsDir,
		peers = Peers,
		mine = Mine,
		polling = Polling,
		clean = Clean,
		auto_join = AutoJoin,
		diff = Diff,
		mining_addr = Addr,
		max_miners = MaxMiners,
		max_emitters = MaxEmitters,
		tx_propagation_parallelization = TXProp,
		new_key = NewKey,
		load_key = LoadKey,
		disk_space = DiskSpace,
		used_space = UsedSpace,
		start_from_block_index = StartFromBlockIndex,
		internal_api_secret = InternalApiSecret,
		enable = Enable,
		disable = Disable,
		transaction_blacklist_files = TransactionBlacklistFiles,
		transaction_blacklist_urls = TransactionBlacklistURLs,
		transaction_whitelist_files = TransactionWhitelistFiles,
		gateway_domain = GatewayDomain,
		gateway_custom_domains = GatewayCustomDomains,
		requests_per_minute_limit = RequestsPerMinuteLimit,
		max_propagation_peers = MaxPropagationPeers,
		ipfs_pin = IPFSPin,
		webhooks = WebhookConfigs,
		max_connections = MaxConnections,
		max_gateway_connections = MaxGatewayConnections,
		max_poa_option_depth = MaxPOAOptionDepth,
		disk_pool_data_root_expiration_time = DiskPoolExpirationTime,
		max_disk_pool_buffer_mb = MaxDiskPoolBuffer,
		max_disk_pool_data_root_buffer_mb = MaxDiskPoolDataRootBuffer,
		randomx_bulk_hashing_iterations = RandomXBulkHashingIterations
	}} = application:get_env(arweave, config),
	%% Verify port collisions when gateway enabled
	case {Port, GatewayDomain} of
		{P, D} when is_binary(D) andalso (P == 80 orelse P == 443) ->
			io:format("~nThe port must be different than 80 or 443 when the gateway is enabled.~n~n"),
			erlang:halt();
		_ ->
			do_nothing
	end,
	ets:new(ar_data_sync, [set, public, named_table, {read_concurrency, true}]),
	ets:new(ar_tx_blacklist, [set, public, named_table, {read_concurrency, true}]),
	ets:new(
		ar_tx_blacklist_pending_headers,
		[set, public, named_table, {read_concurrency, true}]
	),
	ets:new(ar_tx_blacklist_pending_data,
		[set, public, named_table, {read_concurrency, true}]
	),
	ets:new(ar_tx_blacklist_offsets,
		[ordered_set, public, named_table, {read_concurrency, true}]
	),
	{ok, Supervisor} = ar_sup:start_link(),
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_disksup,
		start => {ar_disksup, start_link, []},
		type => worker,
		shutdown => infinity
	}),
	%% Fill up ar_meta_db.
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_meta_db,
		start => {ar_meta_db, start_link, []},
		type => worker,
		shutdown => infinity
	}),
	ar_meta_db:put(data_dir, DataDir),
	ar_meta_db:put(metrics_dir, MetricsDir),
	ar_meta_db:put(port, Port),
	ar_meta_db:put(disk_space, DiskSpace),
	ar_meta_db:put(used_space, UsedSpace),
	ar_meta_db:put(mine, Mine),
	ar_meta_db:put(max_miners, MaxMiners),
	ar_meta_db:put(max_emitters, MaxEmitters),
	ar_meta_db:put(tx_propagation_parallelization, TXProp),
	ar_meta_db:put(transaction_blacklist_files, TransactionBlacklistFiles),
	ar_meta_db:put(transaction_blacklist_urls, TransactionBlacklistURLs),
	ar_meta_db:put(transaction_whitelist_files, TransactionWhitelistFiles),
	ar_meta_db:put(internal_api_secret, InternalApiSecret),
	ar_meta_db:put(requests_per_minute_limit, RequestsPerMinuteLimit),
	ar_meta_db:put(max_propagation_peers, MaxPropagationPeers),
	ar_meta_db:put(max_poa_option_depth, MaxPOAOptionDepth),
	ar_meta_db:put(disk_pool_data_root_expiration_time_us, DiskPoolExpirationTime * 1000000),
	ar_meta_db:put(max_disk_pool_buffer_mb, MaxDiskPoolBuffer),
	ar_meta_db:put(max_disk_pool_data_root_buffer_mb, MaxDiskPoolDataRootBuffer),
	ar_meta_db:put(randomx_bulk_hashing_iterations, RandomXBulkHashingIterations),
	%% Store enabled features.
	lists:foreach(fun(Feature) -> ar_meta_db:put(Feature, true) end, Enable),
	lists:foreach(fun(Feature) -> ar_meta_db:put(Feature, false) end, Disable),
	%% Prepare the storage for operation.
	ar_storage:start(),
	%% Optionally clear the block cache.
	if Clean -> ar_storage:clear(); true -> do_nothing end,
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),
	% Register custom metrics.
	ar_metrics:register(),
	%% Start other apps which we depend on.
	inets:start(),
	ar_tx_db:start(),
	ar_miner_log:start(),
	{ok, _} = ar_arql_db_sup:start_link([{data_dir, DataDir}]),
	ar_storage:start_update_used_space(),
	%% Determine the mining address.
	case {Addr, LoadKey, NewKey} of
		{false, false, false} ->
			{_, Pub} = ar_wallet:new_keyfile(),
			MiningAddress = ar_wallet:to_address(Pub),
			ar:report_console(
				[
					mining_address_generated,
					{address, MiningAddress}
				]
			);
		{false, false, true} ->
			{_, Pub} = ar_wallet:new_keyfile(),
			MiningAddress = ar_wallet:to_address(Pub),
			ar:report_console(
				[
					mining_address,
					{address, MiningAddress}
				]
			);
		{false, Load, false} ->
			{_, Pub} = ar_wallet:load_keyfile(Load),
			MiningAddress = ar_wallet:to_address(Pub),
			ar:report_console(
				[
					mining_address,
					{address, MiningAddress}
				]
			);
		{Address, false, false} ->
			MiningAddress = Address,
			ar:report_console(
				[
					mining_address,
					{address, MiningAddress}
				]
			);
		_ ->
			{_, Pub} = ar_wallet:new_keyfile(),
			MiningAddress = ar_wallet:to_address(Pub),
			ar:report_console(
				[
					mining_address_generated,
					{address, MiningAddress}
				]
			)
	end,
	ar_randomx_state:start(),
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_tx_blacklist,
		start => {ar_tx_blacklist_sup, start_link, [[]]},
		type => supervisor,
		shutdown => infinity
	}),
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_data_sync,
		start => {ar_data_sync_sup, start_link, [[]]},
		type => supervisor,
		shutdown => infinity
	}),
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_header_sync,
		start => {ar_header_sync_sup, start_link, [[]]},
		type => supervisor,
		shutdown => infinity
	}),
	ValidPeers = ar_join:filter_peer_list(Peers),
	case {Peers, ValidPeers} of
		{[_Peer | _], []} ->
			io:format(
				"~n\tInvalid peers. A valid peer must be part of the"
				" network ~s and its clock must deviate from ours by no"
				" more than ~B seconds.~n", [?NETWORK_NAME, ?JOIN_CLOCK_TOLERANCE]
			),
			erlang:halt();
		_ ->
			ok
	end,
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_node,
		shutdown => infinity,
		start => {ar_node, start_link,
			[[
				ValidPeers,
				case {StartFromBlockIndex, Init} of
					{false, false} ->
						not_joined;
					{true, _} ->
						case ar_storage:read_block_index() of
							{error, enoent} ->
								io:format(
									"~n~n\tBlock index file is not found. "
									"If you want to start from a block index copied "
									"from another node, place it in "
									"<data_dir>/hash_lists/last_block_index.json~n~n"
								),
								erlang:halt();
							BI ->
								BI
						end;
					{false, true} ->
						ar_weave:init(
							ar_util:genesis_wallets(),
							ar_retarget:switch_to_linear_diff(Diff),
							0,
							ar_storage:read_tx(ar_weave:read_v1_genesis_txs())
						)
				end,
				0,
				MiningAddress,
				AutoJoin,
				Diff,
				os:system_time(seconds)
			]]}
		}
	),
	Node = whereis(http_entrypoint_node),
	%% Start a bridge, add it to the node's peer list.
	{ok, Bridge} = supervisor:start_child(
		Supervisor,
		{
			ar_bridge,
			{ar_bridge, start_link, [[ValidPeers, [Node], Port]]},
			permanent,
			infinity,
			worker,
			[ar_bridge]
		}
	),
	ar_node:add_peers(Node, Bridge),
	PrintMiningAddress = case MiningAddress of
			unclaimed -> "unclaimed";
			_ -> binary_to_list(ar_util:encode(MiningAddress))
		end,
	{ok, Logfile} = application:get_env(arweave, logfile),
	ar:info(
		[
			{event, starting_server},
			{session_log, Logfile},
			{port, Port},
			{automine, Mine},
			{miner, Node},
			{mining_address, PrintMiningAddress},
			{peers, [ar_util:format_peer(Peer) || Peer <- ValidPeers]},
			{polling, Polling},
			{target_time, ?TARGET_TIME},
			{retarget_blocks, ?RETARGET_BLOCKS}
		]
	),
	ok = prepare_graphql(),
	%% Start the first node in the gossip network (with HTTP interface).
	ok = ar_http_iface_server:start([
		{http_entrypoint_node, Node},
		{http_bridge_node, Bridge},
		{port, Port},
		{gateway_domain, GatewayDomain},
		{gateway_custom_domains, GatewayCustomDomains},
		{max_connections, MaxConnections},
		{max_gateway_connections, MaxGatewayConnections}
	]),
	ar_randomx_state:start_block_polling(),
	PollingArgs = [{trusted_peers, ValidPeers}] ++ case Polling of
		true ->
			[{polling_interval, 10 * 1000}];
		false ->
			[]
	end,
	{ok, _} = supervisor:start_child(Supervisor, #{
		id => ar_poller,
		start => {ar_poller_sup, start_link, [PollingArgs]},
		type => supervisor,
		shutdown => infinity
	}),
	if Mine -> ar_node:automine(Node); true -> do_nothing end,
	case IPFSPin of
		false -> ok;
		true  -> app_ipfs:start_pinning()
	end,
	ar_node:add_peers(Node, ar_webhook:start(WebhookConfigs)),
	{ok, Supervisor}.

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

%% @doc Create a name for a session log file.
generate_logfile_name() ->
	{{Yr, Mo, Da}, {Hr, Mi, Se}} = erlang:universaltime(),
	lists:flatten(
		io_lib:format(
			"~s/session_~4..0b-~2..0b-~2..0b_~2..0b-~2..0b-~2..0b~s.log",
			[?LOG_DIR, Yr, Mo, Da, Hr, Mi, Se, maybe_node_postfix()]
		)
	).

maybe_node_postfix() ->
	case init:get_argument(name) of
		{ok, [[Name]]} ->
			"-" ++ Name;
		_ ->
			""
	end.

%% One scheduler => one dirty scheduler => Calculating a RandomX hash, e.g.
%% for validating a block, will be blocked on initializing a RandomX dataset,
%% which takes minutes.
warn_if_single_scheduler() ->
	case erlang:system_info(schedulers_online) of
		1 ->
			console("WARNING: Running only one CPU core / Erlang scheduler may cause issues.");
		_ ->
			ok
	end.

-ifdef(FIXED_DIFF).
fixed_diff_option() -> [{d, 'FIXED_DIFF', ?FIXED_DIFF}].
-else.
fixed_diff_option() -> [].
-endif.

-ifdef(FIXED_DELAY).
fixed_delay_option() -> [{d, 'FIXED_DELAY', ?FIXED_DELAY}].
-else.
fixed_delay_option() -> [].
-endif.

%% @doc Run all of the tests associated with the core project.
tests() ->
	tests(?CORE_TEST_MODS, #config {}).

tests(Mods, Config) when is_list(Mods) ->
	error_logger:tty(true),
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
	tests(Mods, #config {}).

%% @doc Generate the project documentation.
docs() ->
	Mods =
		lists:filter(
			fun(File) -> string:str(File, ".erl") > 0 end,
			element(2, file:list_dir("../src"))
		),
	edoc:files([ "../src/" ++ Mod || Mod <- Mods ]).

%% @doc Print an informational message to the log file.
report(X) ->
	error_logger:info_report(X).

%% @deprecated
report_console(X) -> info(X).

%% @doc Report a value and return it.
d(X) ->
	info(X),
	X.

%% @doc Print an information message to the log file (log level INFO) and console.
-ifdef(DEBUG).
console(Report) -> info(Report).
-else.
console(Report) ->
	io:format("~P~n", [Report, 30]),
	info(Report).
-endif.

-ifdef(DEBUG).
console(Format, Data) -> info(Format, Data).
-else.
console(Format, Data) ->
	WithNewLine = iolist_to_binary([Format, "~n"]),
	io:format(WithNewLine, Data),
	info(Format, Data).
-endif.

%% @doc Print an INFO message to the log file.
info(Report) ->
	error_logger:info_report(Report).

info(Format, Data) ->
	info(io_lib:format(Format, Data)).

%% @doc Print a WARNING message to the log file.
warn(Report) ->
	error_logger:warning_report(Report).

warn(Format, Data) ->
	warn(io_lib:format(Format, Data)).

%% @doc Print an error message to the log file (log level ERROR) and console.
err(Report) ->
	case is_list(Report) andalso is_tuple_list(Report) of
		true ->
			io:format("ERROR: ~n" ++ format_list_msg(Report) ++ "~n");
		false ->
			io:format("ERROR: ~n~p~n", [Report])
	end,
	error_logger:error_report(Report).

err(Format, Data) ->
	err(io_lib:format(Format, Data)).

is_tuple_list(List) ->
	lists:all(fun is_tuple/1, List).

format_list_msg(List) ->
	FormatRow = fun
		({Key, Value}) ->
			io_lib:format("\s\s\s\s~p: ~p", [Key, Value]);
		(Item) ->
			io_lib:format("\s\s\s\s~p", [Item])
	end,
	lists:flatten(lists:join("~n", lists:map(FormatRow, List))).

%% @doc A multiplier applied to all simulated time elements in the system.
-ifdef(DEBUG).
scale_time(Time) ->
	erlang:trunc(?DEBUG_TIME_SCALAR * Time).
-else.
scale_time(Time) -> Time.
-endif.

%% @doc Ensure that parsing of core command line options functions correctly.
commandline_parser_test_() ->
	{timeout, 20, fun() ->
		Addr = crypto:strong_rand_bytes(32),
		Tests =
			[
				{"peer 1.2.3.4 peer 5.6.7.8:9", #config.peers, [{5,6,7,8,9},{1,2,3,4,1984}]},
				{"mine", #config.mine, true},
				{"port 22", #config.port, 22},
				{"mining_addr " ++ binary_to_list(ar_util:encode(Addr)), #config.mining_addr, Addr}
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
