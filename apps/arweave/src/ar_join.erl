-module(ar_join).
-export([start/2, start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles creating an initial, minimal
%%% block list to be used by a node joining a network already in progress.

%% @doc Start a process that will attempt to join a network from the last
%% sync block.
start(Peers, NewB) when is_record(NewB, block) ->
	start(self(), Peers, NewB);
start(Node, Peers) ->
	start(Node, Peers, find_current_block(Peers)).
start(_, [], _) ->
	ar:report_console([not_joining, {reason, no_peers}]);
start(Node, Peers, B) when is_atom(B) ->
	ar:report_console(
		[
			could_not_retrieve_current_block,
			{trying_again_in, ?REJOIN_TIMEOUT, seconds}
		]
	),
	timer:apply_after(?REJOIN_TIMEOUT, ar_join, start, [Node, Peers]);
start(Node, RawPeers, NewB) ->
	case whereis(join_server) of
		undefined ->
			ar_storage_migration:run_migrations(NewB#block.hash_list),
			PID = spawn(fun() -> do_join(Node, RawPeers, NewB) end),
			erlang:register(join_server, PID);
		_ -> already_running
	end.

%% @doc Perform the joining process.
do_join(_Node, _RawPeers, NewB) when not ?IS_BLOCK(NewB) ->
	ar:report_console(
		[
			node_not_joining,
			{reason, cannot_get_full_block_from_peer},
			{received_instead, NewB}
		]
	);
do_join(Node, RawPeers, NewB) ->
	case verify_time_sync(RawPeers) of
		false ->
			ar:err(
				[
					node_not_joining,
					{reason, clock_time_in_sync_with_join_peers},
					{
						recommendation,
						"Ensure that your machine's time is up-to-date."
					}
				]
			);
		true ->
			Peers = filter_peer_list(RawPeers),
			ar:report_console(
				[
					joining_network,
					{node, Node},
					{peers, Peers},
					{height, NewB#block.height}
				]
			),
			ar_miner_log:joining(),
			ar_sqlite3:populate_db(NewB#block.hash_list),
			ar_randomx_state:init(NewB#block.hash_list, Peers),
			BlockTXPairs = get_block_and_trail(Peers, NewB, NewB#block.hash_list),
			Node ! {
				fork_recovered,
				[NewB#block.indep_hash|NewB#block.hash_list],
				BlockTXPairs
			},
			join_peers(Peers),
			ar_miner_log:joined(),
			spawn(fun() -> fill_to_capacity(ar_manage_peers:get_more_peers(Peers), NewB#block.hash_list) end)
	end.

%% @doc Verify timestamps of peers.
verify_time_sync(Peers) ->
	%% Ignore this check if time syncing is disabled.
	case ar_meta_db:get(time_syncing) of
		false -> true;
		_ ->
			VerifyPeerClock = fun(Peer) ->
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
						ar:info(
							"Failed to get time from peer ~s: ~p.",
							[ar_util:format_peer(Peer), Err]
						),
						true
				end
			end,
			Responses = ar_util:pmap(VerifyPeerClock, [P || P <- Peers, not is_pid(P)]),
			lists:all(fun(R) -> R end, Responses)
	end.

log_peer_clock_diff(Peer, Diff) ->
	Warning = "Your local clock deviates from peer ~s by ~B seconds or more.",
	WarningArgs = [ar_util:format_peer(Peer), Diff],
	ar:console(Warning, WarningArgs),
	ar:warn(Warning, WarningArgs).

%% @doc Return the current block from a list of peers.
find_current_block([]) ->
	ar:info("Did not manage to fetch current block from any of the peers. Will retry later."),
	unavailable;
find_current_block([Peer|Tail]) ->
	try ar_node:get_hash_list(Peer) of
		[] ->
			find_current_block(Tail);
		BHL ->
			Hash = hd(BHL),
			ar:info([
				"Fetching current block.",
				{peer, Peer},
				{hash, Hash}
			]),
			MaybeB = ar_node_utils:get_full_block(Peer, Hash, BHL),
			case MaybeB of
				Atom when is_atom(Atom) ->
					ar:info([
						"Failed to fetch block from peer. Will retry using a different one.",
						{reply, Atom}
					]),
					Atom;
				B ->
					B
			end
	catch
		Exc:Reason ->
			ar:info([
				"Failed to fetch block from peer. Will retry using a different one.",
				{peer, Peer},
				{exception, Exc},
				{reason, Reason}
			]),
			find_current_block(Tail)
	end.

%% @doc Verify peer(s) are on the same network as the client. Remove any that
%% are not.
-ifdef(DEBUG).
filter_peer_list(Peers) when is_list(Peers) ->
	lists:filter(
		fun(Peer) when is_pid(Peer) -> true;
		   (Peer) -> ar_http_iface_client:get_info(Peer, name) == <<?NETWORK_NAME>>
		end,
	Peers);
filter_peer_list(Peer) -> filter_peer_list([Peer]).
-else.
filter_peer_list(Peers) when is_list(Peers) ->
	lists:filter(
		fun(Peer) when is_pid(Peer) -> false;
		   (Peer) -> ar_http_iface_client:get_info(Peer, name) == <<?NETWORK_NAME>>
		end,
	Peers);
filter_peer_list(Peer) -> filter_peer_list([Peer]).
-endif.


join_peers(Peers) when is_list(Peers) ->
	lists:foreach(
		fun(P) ->
			join_peers(P)
		end,
		Peers
	);
join_peers(Peer) when is_pid(Peer) -> ok;
join_peers(Peer) -> ar_http_iface_client:add_peer(Peer).

%% @doc Get a block, and its ?STORE_BLOCKS_BEHIND_CURRENT previous
%% blocks and recall blocks. Alternatively, if the blocklist is shorter than
%% ?STORE_BLOCKS_BEHIND_CURRENT, simply get all existing blocks and recall blocks
get_block_and_trail(_Peers, NewB, []) ->
	TXIDs = [TX#tx.id || TX <- NewB#block.txs],
	ar_storage:write_block(NewB#block { txs = TXIDs }),
	[{NewB#block.indep_hash, TXIDs}];
get_block_and_trail(Peers, NewB, HashList) ->
	get_block_and_trail(Peers, NewB, ?STORE_BLOCKS_BEHIND_CURRENT, HashList, []).

get_block_and_trail(_, unavailable, _, _, BlockTXPairs) ->
	BlockTXPairs;
get_block_and_trail(Peers, NewB, BehindCurrent, _, BlockTXPairs) when NewB#block.height =< 1 ->
	ar_storage:write_full_block(NewB),
	PreviousBlock = ar_node:get_block(
		Peers,
		NewB#block.previous_block,
		NewB#block.hash_list
	),
	ar_storage:write_block(PreviousBlock),
	TXIDs = [TX#tx.id || TX <- NewB#block.txs],
	NewBlockTXPairs = BlockTXPairs ++ [{NewB#block.indep_hash, TXIDs}],
	case BehindCurrent of
		0 ->
			NewBlockTXPairs;
		1 ->
			NewBlockTXPairs ++ [{PreviousBlock#block.indep_hash, PreviousBlock#block.txs}]
	end;
get_block_and_trail(_, _, 0, _, BlockTXPairs) ->
	BlockTXPairs;
get_block_and_trail(Peers, NewB, BehindCurrent, HashList, BlockTXPairs) ->
	PreviousBlock = ar_node_utils:get_full_block(
		Peers,
		NewB#block.previous_block,
		NewB#block.hash_list
	),
	case ?IS_BLOCK(PreviousBlock) of
		true ->
			RecallBlock = ar_util:get_recall_hash(PreviousBlock, HashList),
			ar_storage:write_full_block(NewB),
			TXIDs = [TX#tx.id || TX <- NewB#block.txs],
			NewBlockTXPairs = BlockTXPairs ++ [{NewB#block.indep_hash, TXIDs}],
			case ar_node_utils:get_full_block(Peers, RecallBlock, NewB#block.hash_list) of
				unavailable ->
					ar:info(
						[
							could_not_retrieve_joining_recall_block,
							retrying
						]
					),
					get_block_and_trail(Peers, NewB, BehindCurrent, HashList, BlockTXPairs);
				RecallB ->
					ar_storage:write_full_block(RecallB),
					ar:info(
						[
							{writing_block, NewB#block.height},
							{writing_recall_block, RecallB#block.height},
							{blocks_written, 2 * (?STORE_BLOCKS_BEHIND_CURRENT - (BehindCurrent-1))},
							{blocks_to_write, 2 * (BehindCurrent-1)}
						]
					),
					get_block_and_trail(Peers, PreviousBlock, BehindCurrent-1, HashList, NewBlockTXPairs)
			end;
		false ->
			ar:info(
				[
					could_not_retrieve_joining_block,
					retrying
				]
			),
			timer:sleep(3000),
			get_block_and_trail(Peers, NewB, BehindCurrent, HashList, BlockTXPairs)
	end.

%% @doc Fills node to capacity based on weave storage limit.
fill_to_capacity(Peers, ToWrite) -> fill_to_capacity(Peers, ToWrite, ToWrite).
fill_to_capacity(_, [], _) -> ok;
fill_to_capacity(Peers, ToWrite, BHL) ->
	timer:sleep(1 * 1000),
	RandHash = lists:nth(rand:uniform(length(ToWrite)), ToWrite),
	case ar_storage:read_block(RandHash, BHL) of
		unavailable ->
			fill_to_capacity2(Peers, RandHash, ToWrite, BHL);
		_ ->
			fill_to_capacity(
				Peers,
				lists:delete(RandHash, ToWrite),
				BHL
			)
	end.

fill_to_capacity2(Peers, RandHash, ToWrite, BHL) ->
	B =
		try
			ar_node_utils:get_full_block(Peers, RandHash, BHL)
		catch _:_ ->
			unavailable
		end,
	case B of
		unavailable ->
			timer:sleep(3000),
			fill_to_capacity(Peers, ToWrite, BHL);
		B ->
			case ar_storage:write_full_block(B) of
				{error, _} -> disk_full;
				_ ->
					fill_to_capacity(
						Peers,
						lists:delete(RandHash, ToWrite),
						BHL
					)
			end
	end.

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		Node1 = ar_node:start([], _B0 = ar_weave:init([])),
		timer:sleep(300),
		ar_node:mine(Node1),
		timer:sleep(300),
		ar_node:mine(Node1),
		timer:sleep(600),
		Node2 = ar_node:start([Node1]),
		timer:sleep(1500),
		[B|_] = ar_node:get_blocks(Node2),
		2 = (ar_storage:read_block(B, ar_node:get_hash_list(Node1)))#block.height
	end}.

%% @doc Ensure that both nodes can mine after a join.
node_join_test() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		Node1 = ar_node:start([], _B0 = ar_weave:init([])),
		timer:sleep(300),
		ar_node:mine(Node1),
		timer:sleep(300),
		ar_node:mine(Node1),
		timer:sleep(300),
		Node2 = ar_node:start([Node1]),
		timer:sleep(600),
		ar_node:mine(Node2),
		timer:sleep(1500),
		[B|_] = ar_node:get_blocks(Node1),
		3 = (ar_storage:read_block(B, ar_node:get_hash_list(Node1)))#block.height
	end}.
