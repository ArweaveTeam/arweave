-module(ar).
-export([main/0, main/1, start/0, start/1, rebuild/0]).
-export([test/0, test/1, test_coverage/0, test_apps/0, test_networks/0, test_slow/0]).
-export([docs/0]).
-export([report/1, report_console/1, d/1]).
-export([scale_time/1, timestamp/0]).
-export([start_link/0, start_link/1, init/1]).
-include("ar.hrl").

%%% Archain server entrypoint and basic utilities.

%% A list of the modules to test.
%% At some point we might want to make this just test all mods starting with
%% ar_.
-define(
	CORE_TEST_MODS,
	[
		ar_util,
		ar_cleanup,
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
		ar_http_iface,
		ar_node,
		ar_simple_reporter,
		ar_retarget,
		ar_block,
		ar_tx_db
	]
).

%% All of the apps that have tests associated with them
-define(APP_TEST_MODS, [app_chirper]).

%% Start options
-record(opts, {
	port = ?DEFAULT_HTTP_IFACE_PORT,
	init = false,
	mine = false,
	peers = default,
	polling = false,
	auto_join = true,
	clean = false,
	diff = ?DEFAULT_DIFF,
	mining_addr = false,
	new_key = false,
	load_key = false,
	pause = true,
	disk_space = ar_storage:calculate_disk_space(),
	used_space = ar_storage:calculate_used_space()
}).

%% @doc Command line program entrypoint. Takes a list of arguments.
main() -> main("").
main("") ->
	io:format("Starts an Archain mining server.~n"),
	io:format("Compatible with network: ~s~n", [?NETWORK_NAME]),
	io:format("Usage: archain-server [options]~n"),
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
			{"peer ip:port", "Join a network on a peer (or set of peers)."},
			{"mine", "Automatically start mining once the netwok has been joined."},
			{"port", "The local port to use for mining. "
						"This port must be accessible by remote peers."},
			{"polling", "Poll peers for new blocks. Useful in environments where "
			 			"port forwarding is not possible."},
			{"clean", "Clear the block cache before starting."},
			{"no_auto_join", "Do not automatically join the network of your peers."},
			{"init", "Start a new blockweave."},
			{"diff init_diff", "(For use with 'init':) New blockweave starting difficulty."},
			{"mining_addr addr", "The address that mining rewards should be credited to."},
			{"new_mining_key", "Generate a new keyfile, apply it as the reward address"},
			{"load_mining_key file", "Load the address that mining rewards should be credited to from file"},
			{"disk_space space", "Max size (in GB) for Arweave to take up on disk"}
		]
	),
	erlang:halt();
main(Args) -> main(Args, #opts{}).
main([], O) -> start(O);
main(["init"|Rest], O) ->
	main(Rest, O#opts { init = true });
main(["mine"|Rest], O) ->
	main(Rest, O#opts { mine = true });
main(["peer", Peer|Rest], O = #opts { peers = default }) ->
	main(Rest, O#opts { peers = [ar_util:parse_peer(Peer)] });
main(["peer", Peer|Rest], O = #opts { peers = Ps }) ->
	main(Rest, O#opts { peers = [ar_util:parse_peer(Peer)|Ps] });
main(["port", Port|Rest], O) ->
	main(Rest, O#opts { port = list_to_integer(Port) });
main(["diff", Diff|Rest], O) ->
	main(Rest, O#opts { diff = list_to_integer(Diff) });
main(["polling"|Rest], O) ->
	main(Rest, O#opts { polling = true });
main(["clean"|Rest], O) ->
	main(Rest, O#opts { clean = true });
main(["no_auto_join"|Rest], O) ->
	main(Rest, O#opts { auto_join = false });
main(["mining_addr", Addr|Rest], O) ->
	main(Rest, O#opts { mining_addr = ar_util:decode(Addr) });
main(["new_mining_key"|Rest], O)->
	main(Rest, O#opts { new_key = true });
main(["disk_space", Size|Rest], O) ->
	main(Rest, O#opts { disk_space = (list_to_integer(Size)*1024*1024*1024) });
main(["load_mining_key", File|Rest], O)->
	main(Rest, O#opts { load_key = File });
main([Arg|_Rest], _O) ->
	io:format("Unknown argument: ~s. Terminating.", [Arg]).

%% @doc Start an Archain node on this BEAM.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) when is_integer(Port) -> start(#opts { port = Port });
start(
	#opts {
		port = Port,
		init = Init,
		peers = RawPeers,
		mine = Mine,
		polling = Polling,
		clean = Clean,
		auto_join = AutoJoin,
		diff = Diff,
		mining_addr = Addr,
		new_key = NewKey,
		load_key = LoadKey,
		pause = Pause,
		disk_space = DiskSpace,
		used_space = UsedSpace
	}) ->
	% Optionally clear the block cache
	if Clean -> ar_storage:clear(); true -> do_nothing end,
	% Start apps which we depend on.
	inets:start(),
	ar_meta_db:start(),
	ar_tx_db:start(),
	ar_blacklist:start(),
	ar_key_db:start(),
	ar_track_tx_db:start(),
	ar_meta_db:put(port, Port),
	ar_meta_db:put(disk_space, DiskSpace),
	ar_storage:update_directory_size(),
	Peers =
		case RawPeers of
			default -> ?DEFAULT_PEER_LIST;
			_ -> RawPeers
		end,
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
				if Init -> ar_weave:init(ar_util:genesis_wallets(), Diff); true -> not_joined end,
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
			app_search,
			{app_search, start_link, [[[Node|Peers]]]},
			permanent,
			brutal_kill,
			worker,
			[app_search]
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
	% Add self to all remote nodes.
    %lists:foreach(fun ar_http_iface:add_peer/1, Peers),
	% Start the logging system.
	error_logger:logfile({open, Filename = generate_logfile_name()}),
	error_logger:tty(false),
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
			{polling, Polling}
		]
    ),
	% Start the first node in the gossip network (with HTTP interface)
	ar_http_iface:start(
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
	make:all([load]).

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
test() ->
	case ?DEFAULT_DIFF of
		X when X > 8 ->
			ar:report_console(
				[
					diff_too_high_for_tests,
					terminating
				]
			);
		_ ->
			start(#opts { peers = [], pause = false}),
			eunit:test({timeout, ?TEST_TIMEOUT, ?CORE_TEST_MODS}, [verbose])
	end.

%% @doc Run the TNT test system, printing coverage results.
test_coverage() ->
	ar_coverage:analyse(fun test/0).

%% @doc Run the tests for a single module.
test(Mod) ->
	eunit:test({timeout, ?TEST_TIMEOUT, [Mod]}, [verbose]).

%% @doc Run tests on the apps.
test_apps() ->
	start(),
	eunit:test(?APP_TEST_MODS, [verbose]).

test_networks() ->
	error_logger:tty(false),
	ar_test_sup:start().

test_slow() ->
	ar_node:filter_out_of_order_txs_test_slow(),
	ar_node:filter_out_of_order_txs_large_test_slow(),
	ar_node:filter_all_out_of_order_txs_test_slow(),
	ar_node:filter_all_out_of_order_txs_large_test_slow(),
	ar_node:wallet_transaction_test_slow(),
	ar_node:wallet_two_transaction_test_slow(),
	ar_node:single_wallet_double_tx_before_mine_test_slow(),
	ar_node:single_wallet_double_tx_wrong_order_test_slow(),
	ar_node:tx_threading_test_slow(),
	ar_node:bogus_tx_thread_test_slow(),
	ar_node:large_weakly_connected_blockweave_with_data_test_slow(),
	ar_node:large_blockweave_with_data_test_slow(),
	ar_node:medium_blockweave_mine_multiple_data_test_slow(),
	ar_http_iface:get_txs_by_send_recv_test_slow(),
	ar_http_iface:get_full_block_by_hash_test_slow(),
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
report_console(X) ->
	error_logger:tty(true),
	error_logger:info_report(X),
	error_logger:tty(false).
-endif.
%% @doc Report a value and return it.
d(X) ->
	report_console(X),
	X.

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
