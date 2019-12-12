%%%
%%% @doc Arweave server entrypoint and basic utilities.
%%%

-module(ar).

-export([main/0, main/1, start/0, start/1]).
-export([tests/0, tests/1, tests/2]).
-export([test_ipfs/0]).
-export([test_with_coverage/0, test_apps/0, test_networks/0, test_slow/0]).
-export([docs/0]).
-export([err/1, err/2, info/1, info/2, warn/1, warn/2, console/1, console/2]).
-export([report/1, report_console/1, d/1]).
-export([scale_time/1, timestamp/0]).
-export([start_link/0, start_link/1, init/1]).
-export([start_for_tests/0]).

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
		ar_block_index,
		ar_config_tests,
		ar_deep_hash,
		ar_inflation,
		ar_tx_queue,
		ar_node_tests,
		ar_util,
		ar_cleanup,
		ar_merkle,
		ar_storage,
		ar_serialize,
		ar_tx,
		ar_weave,
		ar_wallet,
		ar_firewall,
		ar_gossip,
		ar_mine,
		ar_join,
		ar_fork_recovery,
		ar_http_iface_tests,
		ar_retarget,
		ar_block,
		ar_tx_db,
		ar_firewall_distributed_tests,
		ar_semaphore_tests,
		ar_tx_replay_pool_tests,
		ar_multiple_txs_per_wallet_tests,
		ar_tx_perpetual_storage_tests,
		ar_difficulty_tests,
		ar_gateway_middleware_tests,
		ar_randomx_mining_tests,
		ar_http_util_tests,
		% ar_meta_db must be the last in the list since it resets global configuraiton
		ar_meta_db
	]
).

%% Supported feature flags (default behaviour)
% http_logging (false)
% disk_logging (false)
% miner_logging (true)
% subfield_queries (false)
% partial_fork_recovery (false)
% blacklist (true)
% time_syncing (true)

%% All of the apps that have tests associated with them
-define(APP_TEST_MODS, [app_chirper]).

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
					string:pad(Opt, 30, trailing, $ ),
					Desc
				]
			)
		end,
		[
			{"config_file (path)", "Load configuration from specified file."},
			{"peer (ip:port)", "Join a network on a peer (or set of peers)."},
			{"start_hash_list (hash)", "Start the node from a given block."},
			{"mine", "Automatically start mining once the netwok has been joined."},
			{"port", "The local port to use for mining. "
						"This port must be accessible by remote peers."},
			{"data_dir", "The directory for storing the weave and the wallets (when generated)."},
			{"polling", "Poll peers for new blocks every 10 seconds. "
						"Useful in environments where "
						"port forwarding is not possible. "
						"When the flag is not set, the node still polls "
						"if it does not receive blocks for a minute."},
			{"clean", "Clear the block cache before starting."},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"init", "Start a new blockweave."},
			{"diff (init_diff)", "(For use with 'init':) New blockweave starting difficulty."},
			{"mining_addr (addr)", "The address that mining rewards should be credited to."},
			{"max_miners (num)", "The maximum number of mining processes."},
			{"max_emitters (num)", "The maximum number of message emitter processes (default 8)."},
			{"new_mining_key", "Generate a new keyfile, apply it as the reward address"},
			{"load_mining_key (file)", "Load the address that mining rewards should be credited to from file."},
			{"ipfs_pin", "Pin incoming IPFS tagged transactions on your local IPFS node."},
			{"content_policy (file)", "Load a content policy file for the node."},
			{"transaction_blacklist (file)", "A .txt file containing blacklisted transactions. "
											 "One Base64 encoded transaction ID per line."},
			{"disk_space (num)", "Max size (in GB) for the disk partition containing the Arweave data directory (blocks, txs, etc) when the miner stops writing files to disk."},
			{"benchmark (algorithm)", "Run a mining performance benchmark. Pick an algorithm from sha384, randomx."},
			{"auto_update (false|addr)", "Define the auto-update watch address, or disable it with 'false'."},
			{"internal_api_secret (secret)",
				lists:flatten(
					io_lib:format(
						"Enables the internal API endpoints, only accessible with this secret. Min. ~B chars.",
						[?INTERNAL_API_SECRET_MIN_LEN]
					)
				)
			},
			{"enable (feature)", "Enable a specific (normally disabled) feature. For example, subfield_queries."},
			{"disable (feature)", "Disable a specific (normally enabled) feature. For example, api_compat mode."},
			{"gateway (domain)", "Run a gateway on the specified domain"},
			{"custom_domain (domain)", "Add a domain to the list of supported custom domains."},
			{"requests_per_minute_limit (number)", "Limit the maximum allowed number of HTTP requests per IP address per minute. Default is 900."},
			{"max_propagation_peers (number)", "How many peers to propagate blocks and transactions to. Default is 65."},
			{"max_connections", "The number of connections to be handled concurrently. Its purpose is to prevent your system from being overloaded and ensuring all the connections are handled optimally. Default is 1024."},
			{"max_gateway_connections", "The number of gateway connections to be handled concurrently. Default is 128."}
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
parse_cli_args(["init"|Rest], C) ->
	parse_cli_args(Rest, C#config { init = true });
parse_cli_args(["mine"|Rest], C) ->
	parse_cli_args(Rest, C#config { mine = true });
parse_cli_args(["peer", Peer|Rest], C = #config { peers = Ps }) ->
	parse_cli_args(Rest, C#config { peers = [ar_util:parse_peer(Peer)|Ps] });
parse_cli_args(["content_policy", File|Rest], C = #config { content_policy_files = Files }) ->
	parse_cli_args(Rest, C#config { content_policy_files = [File|Files] });
parse_cli_args(["transaction_blacklist", File|Rest], C = #config { transaction_blacklist_files = Files } ) ->
	parse_cli_args(Rest, C#config { transaction_blacklist_files = [File|Files] });
parse_cli_args(["port", Port|Rest], C) ->
	parse_cli_args(Rest, C#config { port = list_to_integer(Port) });
parse_cli_args(["data_dir", DataDir|Rest], C) ->
	parse_cli_args(Rest, C#config { data_dir = DataDir });
parse_cli_args(["diff", Diff|Rest], C) ->
	parse_cli_args(Rest, C#config { diff = list_to_integer(Diff) });
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
parse_cli_args(["start_hash_list", BHLHash|Rest], C) ->
	parse_cli_args(Rest, C#config { start_hash_list = ar_util:decode(BHLHash) });
parse_cli_args(["benchmark", Algorithm|Rest], C)->
	parse_cli_args(Rest, C#config { benchmark = true, benchmark_algorithm = list_to_atom(Algorithm) });
parse_cli_args(["auto_update", "false" | Rest], C) ->
	parse_cli_args(Rest, C#config { auto_update = false });
parse_cli_args(["auto_update", Addr | Rest], C) ->
	parse_cli_args(Rest, C#config { auto_update = ar_util:decode(Addr) });
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
parse_cli_args(["max_connections", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_connections = list_to_integer(Num) });
parse_cli_args(["max_gateway_connections", Num | Rest], C) ->
	parse_cli_args(Rest, C#config { max_gateway_connections = list_to_integer(Num) });
parse_cli_args([Arg|_Rest], _O) ->
	io:format("~nUnknown argument: ~s.~n", [Arg]),
	show_help().

%% @doc Start an Arweave node on this BEAM.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) -> start(#config { port = Port });
start(#config { benchmark = true, benchmark_algorithm = Algorithm, max_miners = MaxMiners, disable = Disable, enable = Enable }) ->
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),
	ar_meta_db:start(),
	lists:foreach(fun(Feature) -> ar_meta_db:put(Feature, false) end, Disable),
	lists:foreach(fun(Feature) -> ar_meta_db:put(Feature, true) end, Enable),
	ar_meta_db:put(max_miners, MaxMiners),
	ar_meta_db:put(mine, true),
	ar_randomx_state:start(),
	ar_benchmark:run(Algorithm);
start(
	#config {
		port = Port,
		data_dir = DataDir,
		init = Init,
		peers = Peers,
		mine = Mine,
		polling = Polling,
		clean = Clean,
		auto_join = AutoJoin,
		diff = Diff,
		mining_addr = Addr,
		max_miners = MaxMiners,
		max_emitters = MaxEmitters,
		new_key = NewKey,
		load_key = LoadKey,
		pause = Pause,
		disk_space = DiskSpace,
		used_space = UsedSpace,
		start_hash_list = BHL,
		auto_update = AutoUpdate,
		internal_api_secret = InternalApiSecret,
		enable = Enable,
		disable = Disable,
		content_policy_files = ContentPolicyFiles,
		transaction_blacklist_files = TransactionBlacklistFiles,
		gateway_domain = GatewayDomain,
		gateway_custom_domains = GatewayCustomDomains,
		requests_per_minute_limit = RequestsPerMinuteLimit,
		max_propagation_peers = MaxPropagationPeers,
		ipfs_pin = IPFSPin,
		webhooks = WebhookConfigs,
		max_connections = MaxConnections,
		max_gateway_connections = MaxGatewayConnections
	}) ->
	%% Start the logging system.
	filelib:ensure_dir(?LOG_DIR ++ "/"),
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),
	warn_if_single_scheduler(),
	%% Verify port collisions when gateway enabled
	case {Port, GatewayDomain} of
		{P, D} when is_binary(D) andalso (P == 80 orelse P == 443) ->
			io:format("~nThe port must be different than 80 or 443 when the gateway is enabled.~n~n"),
			erlang:halt();
		_ ->
			do_nothing
	end,
	%% Fill up ar_meta_db.
	ar_meta_db:start(),
	ar_meta_db:put(data_dir, DataDir),
	ar_meta_db:put(port, Port),
	ar_meta_db:put(disk_space, DiskSpace),
	ar_meta_db:put(used_space, UsedSpace),
	ar_meta_db:put(mine, Mine),
	ar_meta_db:put(max_miners, MaxMiners),
	ar_meta_db:put(max_emitters, MaxEmitters),
	ar_meta_db:put(content_policy_files, ContentPolicyFiles),
	ar_meta_db:put(transaction_blacklist_files, TransactionBlacklistFiles),
	ar_meta_db:put(internal_api_secret, InternalApiSecret),
	ar_meta_db:put(requests_per_minute_limit, RequestsPerMinuteLimit),
	ar_meta_db:put(max_propagation_peers, MaxPropagationPeers),
	%% Prepare the storage for operation.
	ar_storage:start(),
	%% Optionally clear the block cache.
	if Clean -> ar_storage:clear(); true -> do_nothing end,
	ok = application:ensure_started(prometheus),
	{ok, _} = application:ensure_all_started(prometheus_cowboy),
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),
	% Register custom metrics.
	ar_metrics:register(),
	%% Start Cowboy and its dependencies
	{ok, _} = application:ensure_all_started(cowboy),
	%% Start other apps which we depend on.
	inets:start(),
	ar_tx_db:start(),
	ar_key_db:start(),
	ar_miner_log:start(),
	{ok, _} = ar_sqlite3_sup:start_link([{data_dir, DataDir}]),
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
	{ok, Supervisor} = start_link(
		[
			[
				Peers,
				case BHL of
					undefined ->
						if Init -> ar_weave:init(ar_util:genesis_wallets(), Diff);
						true -> not_joined
						end;
					_ ->
						ar_storage:read_block_hash_list(BHL)
				end,
				0,
				MiningAddress,
				AutoJoin,
				Diff,
				os:system_time(seconds)
			]
		]
	),
	Node = whereis(http_entrypoint_node),
	%% Start a bridge, add it to the node's peer list.
	{ok, Bridge} = supervisor:start_child(
		Supervisor,
		{
			ar_bridge,
			{ar_bridge, start_link, [[Peers, [Node], Port]]},
			permanent,
			brutal_kill,
			worker,
			[ar_bridge]
		}
	),
	ar_node:add_peers(Node, Bridge),
	%% Initialise the auto-updater, if enabled.
	case AutoUpdate of
		false ->
			do_nothing;
		AutoUpdateAddr ->
			AutoUpdateNode = app_autoupdate:start(AutoUpdateAddr),
			ar_node:add_peers(Node, AutoUpdateNode)
	end,
	%% Store enabled features.
	lists:foreach(fun(Feature) -> ar_meta_db:put(Feature, true) end, Enable),
	lists:foreach(fun(Feature) -> ar_meta_db:put(Feature, false) end, Disable),
	PrintMiningAddress = case MiningAddress of
			unclaimed -> "unclaimed";
			_ -> binary_to_list(ar_util:encode(MiningAddress))
		end,
	ar:report_console(
		[
			starting_server,
			{session_log, Filename},
			{port, Port},
			{init_new_blockweave, Init},
			{automine, Mine},
			{miner, Node},
			{mining_address, PrintMiningAddress},
			{peers, Peers},
			{polling, Polling},
			{target_time, ?TARGET_TIME},
			{retarget_blocks, ?RETARGET_BLOCKS}
		]
	),
	ok = start_graphql(),
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
	PollingArgs = [{trusted_peers, Peers}] ++ case Polling of
		true ->
			[{polling_interval, 10 * 1000}];
		false ->
			[]
	end,
	{ok, _} = ar_poller_sup:start_link(PollingArgs),
	if Mine -> ar_node:automine(Node); true -> do_nothing end,
	case IPFSPin of
		false -> ok;
		true  -> app_ipfs:start_pinning()
	end,
	ar_node:add_peers(Node, ar_webhook:start(WebhookConfigs)),
	case Pause of
		false ->
			ok;
		_ ->
			garbage_collect(),
			receive after infinity -> ok end
	end.

start_graphql() ->
	ok = application:ensure_started(graphql),
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
			console("WARNING: Running only one CPU core / Erlang scheduler may cause issues");
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

%% @doc passthrough to supervisor start_link
start_link() ->
	supervisor:start_link(?MODULE, []).
start_link(Args) ->
	supervisor:start_link(?MODULE, Args).

%% @doc init function for supervisor
init(Args) ->
	SupFlags = {one_for_one, 5, 30},
	ChildSpecs =
		[
			{
				ar_node,
				{ar_node, start_link, Args},
				permanent,
				brutal_kill,
				worker,
				[ar_node]
			}
		],
	{ok, {SupFlags, ChildSpecs}}.

%% @doc Run all of the tests associated with the core project.
tests() ->
	tests(?CORE_TEST_MODS, #config {}).

tests(Mods, Config) when is_list(Mods) ->
	case ?DEFAULT_DIFF of
		X when X > 8 ->
			ar:report_console(
				[
					diff_too_high_for_tests,
					terminating
				]
			);
		_ ->
			start_for_tests(Config),
			eunit:test({timeout, ?TEST_TIMEOUT, Mods}, [verbose])
	end.

start_for_tests() ->
	start_for_tests(#config { }).

start_for_tests(Config) ->
	start(Config#config {
		peers = [],
		pause = false,
		data_dir = "data_test_master",
		disable = [randomx_jit]
	}).

%% @doc Run the tests for a single module.
tests(Mod) ->
	ar_storage:ensure_directories(),
	eunit:test({timeout, ?TEST_TIMEOUT, [Mod]}, [verbose]).

%% @doc Run the tests, printing coverage results.
test_with_coverage() ->
	ar_coverage:analyse(fun tests/0).

%% @doc Run tests on the apps.
test_apps() ->
	tests(?APP_TEST_MODS, #config {}).

test_networks() ->
	error_logger:tty(false),
	ar_test_sup:start().

test_slow() ->
	ar_node_test:wallet_transaction_test_slow(),
	ar_node_test:wallet_two_transaction_test_slow(),
	ar_node_test:single_wallet_double_tx_before_mine_test_slow(),
	ar_node_test:single_wallet_double_tx_wrong_order_test_slow(),
	ar_node_test:tx_threading_test_slow(),
	ar_node_test:bogus_tx_thread_test_slow(),
	ar_node_test:large_weakly_connected_blockweave_with_data_test_slow(),
	ar_node_test:large_blockweave_with_data_test_slow(),
	ar_node_test:medium_blockweave_mine_multiple_data_test_slow(),
	ar_http_iface_client:get_txs_by_send_recv_test_slow(),
	ar_http_iface_client:get_full_block_by_hash_test_slow(),
	ar_fork_recovery:multiple_blocks_ahead_with_transaction_recovery_test_slow(),
	ar_tx:check_last_tx_test_slow().

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

%% @doc Get the unix timestamp (in seconds).
timestamp() ->
	{MegaSec, Sec, _MilliSec} = os:timestamp(),
	(MegaSec * 1000000) + Sec.

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
