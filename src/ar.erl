%%%
%%% @doc Arweave server entrypoint and basic utilities.
%%%

-module(ar).

-export([main/0, main/1, start/0, start/1, rebuild/0]).
-export([tests/0, tests/1, test_coverage/0, test_apps/0, test_networks/0, test_slow/0]).
-export([docs/0]).
-export([err/1, err/2, info/1, info/2, warn/1, warn/2, console/1, console/2]).
-export([report/1, report_console/1, d/1]).
-export([scale_time/1, timestamp/0]).
-export([start_link/0, start_link/1, init/1]).
-export([start_for_tests/0]).
-export([parse/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar,
		ar_block_index,
		ar_deep_hash,
		ar_inflation,
		ar_node_tests,
		ar_util,
		ar_cleanup,
		ar_merkle,
		ar_meta_db,
		ar_storage,
		ar_serialize,
		ar_services,
		ar_tx,
		ar_weave,
		ar_wallet,
		ar_firewall,
		ar_gossip,
		ar_mine,
		ar_join,
		ar_fork_recovery,
		ar_http_iface_tests,
		ar_simple_reporter,
		ar_retarget,
		ar_block,
		ar_tx_db
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

%% Start options with default values.
-record(opts, {
	benchmark = false,
	port = ?DEFAULT_HTTP_IFACE_PORT,
	init = false,
	mine = false,
	peers = [],
	polling = false,
	auto_join = true,
	clean = false,
	diff = ?DEFAULT_DIFF,
	mining_addr = false,
	max_miners = ?NUM_MINING_PROCESSES,
	new_key = false,
	load_key = false,
	pause = true,
	disk_space = ar_storage:calculate_disk_space(),
	used_space = ar_storage:calculate_used_space(),
	start_hash_list = undefined,
	auto_update = ar_util:decode(?DEFAULT_UPDATE_ADDR),
	internal_api_secret = not_set,
	enable = [],
	disable = [],
	content_policies = []
}).

%% @doc Command line program entrypoint. Takes a list of arguments.
main() -> main("").
main("") ->
	io:format("Starts an Arweave mining server.~n"),
	io:format("Compatible with network: ~s~n", [?NETWORK_NAME]),
	io:format("Usage: arweave-server [options]~n"),
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
			{"peer (ip:port)", "Join a network on a peer (or set of peers)."},
			{"start_hash_list (file)", "Start the node from a given block."},
			{"mine", "Automatically start mining once the netwok has been joined."},
			{"port", "The local port to use for mining. "
						"This port must be accessible by remote peers."},
			{"polling", "Poll peers for new blocks. Useful in environments where "
						"port forwarding is not possible."},
			{"clean", "Clear the block cache before starting."},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"init", "Start a new blockweave."},
			{"diff (init_diff)", "(For use with 'init':) New blockweave starting difficulty."},
			{"mining_addr (addr)", "The address that mining rewards should be credited to."},
			{"max_miners (num)", "The maximum number of mining processes."},
			{"new_mining_key", "Generate a new keyfile, apply it as the reward address"},
			{"load_mining_key (file)", "Load the address that mining rewards should be credited to from file."},
			{"content_policy (file)", "Load a content policy file for the node."},
			{"disk_space (space)", "Max size (in GB) for Arweave to take up on disk"},
			{"benchmark", "Run a mining performance benchmark."},
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
			{"disable (feature)", "Disable a specific (normally enabled) feature. For example, api_compat mode."}
		]
	),
	erlang:halt();
main(Args) ->
	start(parse(Args)).

parse(Args) -> parse(Args, #opts{}).
parse([], O) -> O;
parse(["init"|Rest], O) ->
	parse(Rest, O#opts { init = true });
parse(["mine"|Rest], O) ->
	parse(Rest, O#opts { mine = true });
parse(["peer", Peer|Rest], O = #opts { peers = Ps }) ->
	parse(Rest, O#opts { peers = [ar_util:parse_peer(Peer)|Ps] });
parse(["content_policy", F|Rest], O = #opts { content_policies = Fs }) ->
	parse(Rest, O#opts { content_policies = [F|Fs] });
parse(["port", Port|Rest], O) ->
	parse(Rest, O#opts { port = list_to_integer(Port) });
parse(["diff", Diff|Rest], O) ->
	parse(Rest, O#opts { diff = list_to_integer(Diff) });
parse(["polling"|Rest], O) ->
	parse(Rest, O#opts { polling = true });
parse(["clean"|Rest], O) ->
	parse(Rest, O#opts { clean = true });
parse(["no_auto_join"|Rest], O) ->
	parse(Rest, O#opts { auto_join = false });
parse(["mining_addr", Addr|Rest], O) ->
	parse(Rest, O#opts { mining_addr = ar_util:decode(Addr) });
parse(["max_miners", Num|Rest], O) ->
	parse(Rest, O#opts { max_miners = list_to_integer(Num) });
parse(["new_mining_key"|Rest], O)->
	parse(Rest, O#opts { new_key = true });
parse(["disk_space", Size|Rest], O) ->
	parse(Rest, O#opts { disk_space = (list_to_integer(Size)*1024*1024*1024) });
parse(["load_mining_key", File|Rest], O)->
	parse(Rest, O#opts { load_key = File });
parse(["start_hash_list", IndepHash|Rest], O)->
	parse(Rest, O#opts { start_hash_list = ar_util:decode(IndepHash) });
parse(["benchmark"|Rest], O)->
	parse(Rest, O#opts { benchmark = true });
parse(["auto_update", "false" | Rest], O) ->
	parse(Rest, O#opts { auto_update = false });
parse(["auto_update", Addr | Rest], O) ->
	parse(Rest, O#opts { auto_update = ar_util:decode(Addr) });
parse(["internal_api_secret", Secret | Rest], O) when length(Secret) >= ?INTERNAL_API_SECRET_MIN_LEN ->
	parse(Rest, O#opts { internal_api_secret = list_to_binary(Secret)});
parse(["internal_api_secret", _ | _], _) ->
	io:format(
		"~nThe internal_api_secret must be at least ~B characters long.~n~n",
		[?INTERNAL_API_SECRET_MIN_LEN]
	),
	erlang:halt();
parse(["enable", Feature | Rest ], O = #opts { enable = Enabled }) ->
	parse(Rest, O#opts { enable = [ list_to_atom(Feature) | Enabled ] });
parse(["disable", Feature | Rest ], O = #opts { disable = Disabled }) ->
	parse(Rest, O#opts { disable = [ list_to_atom(Feature) | Disabled ] });
parse([Arg|_Rest], _O) ->
	io:format("~nUnknown argument: ~s. Terminating.~n~n", [Arg]),
	erlang:halt().

%% @doc Start an Arweave node on this BEAM.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) -> start(#opts { port = Port });
start(#opts { benchmark = true }) ->
	ar_benchmark:run();
start(
	#opts {
		port = Port,
		init = Init,
		peers = Peers,
		mine = Mine,
		polling = Polling,
		clean = Clean,
		auto_join = AutoJoin,
		diff = Diff,
		mining_addr = Addr,
		max_miners = MaxMiners,
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
		content_policies = Policies
	}) ->
	% Start the logging system.
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),

	ar_storage:start(),
	% Optionally clear the block cache
	if Clean -> ar_storage:clear(); true -> do_nothing end,
	%register prometheus stats collector,
	%prometheus collector app is started at cmdline
	application:ensure_started(prometheus),
	prometheus_registry:register_collector(prometheus_process_collector),
	prometheus_registry:register_collector(ar_metrics_collector),
	% Start apps which we depend on.
	inets:start(),
	ar_meta_db:start(),
	ar_tx_db:start(),
	ar_key_db:start(),
	ar_track_tx_db:start(),
	ar_miner_log:start(),
	ar_meta_db:put(port, Port),
	ar_meta_db:put(disk_space, DiskSpace),
	ar_meta_db:put(used_space, UsedSpace),
	ar_meta_db:put(max_miners, MaxMiners),
	ar_meta_db:put(content_policies, Policies),
	ar_meta_db:put(internal_api_secret, InternalApiSecret),
	ar_storage:update_directory_size(),
	% Determine mining address.
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
	{ok, Supervisor} = start_link(
		[
			[
				Peers,
				case BHL of
					undefined ->
						if Init -> ar_weave:init(ar_util:genesis_wallets(), Diff);
						true -> not_joined
						end;
					_ -> ar_storage:read_hash_list(ar_util:decode(BHL))
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
	{ok, SearchNode} = supervisor:start_child(
		Supervisor,
		{
			ar_tx_search,
			{ar_tx_search, start, []},
			permanent,
			brutal_kill,
			worker,
			[ar_tx_search]
		}
	),
	ar_node:add_peers(Node, SearchNode),
	% Start a bridge, add it to the node's peer list.
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
	% Initialise the auto-updater, if enabled
	case AutoUpdate of
		false ->
			do_nothing;
		AutoUpdateAddr ->
			AutoUpdateNode = app_autoupdate:start(AutoUpdateAddr),
			ar_node:add_peers(Node, AutoUpdateNode)
	end,
	% Store enabled features
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
	% Start the first node in the gossip network (with HTTP interface)
	ar_http_iface_server:start(
		Port,
		Node,
		SearchNode,
		undefined,
		Bridge
	),
	case Polling of
		true -> ar_poller:start(Node, Peers);
		false -> do_nothing
	end,
	if Mine -> ar_node:automine(Node); true -> do_nothing end,
	case Pause of
		false -> ok;
		_ -> receive after infinity -> ok end
	end.

%% @doc Create a name for a session log file.
generate_logfile_name() ->
	{{Yr, Mo, Da}, {Hr, Mi, Se}} = erlang:universaltime(),
	lists:flatten(
		io_lib:format(
			"~s/session_~4..0b-~2..0b-~2..0b_~2..0b-~2..0b-~2..0b.log",
			[?LOG_DIR, Yr, Mo, Da, Hr, Mi, Se]
		)
	).

%% @doc Run the erlang make system on the project.
rebuild() ->
	io:format("Rebuilding Arweave...~n"),
	MakeOptions = [
		load,
		{d, 'TARGET_TIME', ?TARGET_TIME},
		{d, 'RETARGET_BLOCKS', ?RETARGET_BLOCKS}
	] ++ fixed_diff_option() ++ fixed_delay_option(),
	case make:all(MakeOptions) of
		error ->
			io:format("~nBuild failed!~n"),
			init:stop(1);
		up_to_date ->
			io:format("~nBuild complete!~n")
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
	ar_storage:ensure_directories(),
	case ?DEFAULT_DIFF of
		X when X > 8 ->
			ar:report_console(
				[
					diff_too_high_for_tests,
					terminating
				]
			);
		_ ->
			start_for_tests(),
			eunit:test({timeout, ?TEST_TIMEOUT, ?CORE_TEST_MODS}, [verbose])
	end.

start_for_tests() ->
	start(#opts { peers = [], pause = false}).

%% @doc Run the TNT test system, printing coverage results.
test_coverage() ->
	ar_coverage:analyse(fun tests/0).

%% @doc Run the tests for a single module.
tests(Mod) ->
	ar_storage:ensure_directories(),
	eunit:test({timeout, ?TEST_TIMEOUT, [Mod]}, [verbose]).

%% @doc Run tests on the apps.
test_apps() ->
	start(),
	eunit:test(?APP_TEST_MODS, [verbose]).

test_networks() ->
	error_logger:tty(false),
	ar_test_sup:start().

test_slow() ->
	ar_node_test:filter_out_of_order_txs_test_slow(),
	ar_node_test:filter_out_of_order_txs_large_test_slow(),
	ar_node_test:filter_all_out_of_order_txs_test_slow(),
	ar_node_test:filter_all_out_of_order_txs_large_test_slow(),
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
-ifdef(SILENT).
report_console(X) -> report(X).
-else.
%% @doc Print an information message to the log file and console.
%% @deprecated Use console/1, console/2 instead.
report_console(X) ->
	error_logger:tty(true),
	error_logger:info_report(X),
	error_logger:tty(false).
-endif.
%% @doc Report a value and return it.
d(X) ->
	report_console(X),
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
	io:format(Format, Data),
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
				{"peer 1.2.3.4 peer 5.6.7.8:9", #opts.peers, [{5,6,7,8,9},{1,2,3,4,1984}]},
				{"mine", #opts.mine, true},
				{"port 22", #opts.port, 22},
				{"mining_addr " ++ binary_to_list(ar_util:encode(Addr)), #opts.mining_addr, Addr}
			],
		X = string:split(string:join([ L || {L, _, _} <- Tests ], " "), " ", all),
		ar:d({x, X}),
		O = parse(X),
		lists:foreach(
			fun({_, Index, Value}) ->
				?assertEqual(element(Index, O), Value)
			end,
			Tests
		)
	end}.
