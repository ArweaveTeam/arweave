-module(ar_join).
-export([start/2, start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Represents a process that handles creating an initial, minimal
%%% block list to be used by a node joining a network already in progress.

%%% Define how many recent blocks should have syncing priority.
-define(DOWNLOAD_TOP_PRIORITY_BLOCKS_COUNT, 1000).

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
start(Node, RawPeers, {NewB, BI}) ->
	case whereis(join_server) of
		undefined ->
			PID = spawn(fun() -> do_join(Node, RawPeers, NewB, BI) end),
			erlang:register(join_server, PID);
		_ -> already_running
	end.

%% @doc Perform the joining process.
do_join(_Node, _RawPeers, NewB, _BI) when not ?IS_BLOCK(NewB) ->
	ar:report_console(
		[
			node_not_joining,
			{reason, cannot_get_full_block_from_peer},
			{received_instead, NewB}
		]
	);
do_join(Node, RawPeers, NewB, BI) ->
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
			ar_arql_db:populate_db(?BI_TO_BHL(BI)),
			ar_randomx_state:init(BI, Peers),
			BlockTXPairs = get_block_and_trail(Peers, NewB, BI),
			Node ! {fork_recovered, BI, BlockTXPairs},
			join_peers(Peers),
			ar_miner_log:joined(),
			{Recent, Rest} =
				lists:split(min(length(BI), ?DOWNLOAD_TOP_PRIORITY_BLOCKS_COUNT), BI),
			lists:foreach(
				fun({H, _, TXRoot}) ->
					ar_downloader:enqueue_front({block, {H, TXRoot}})
				end,
				lists:sort(fun(_, _) -> rand:uniform(2) == 1 end, Rest)
			),
			lists:foreach(
				fun({H, _, TXRoot}) ->
					ar_downloader:enqueue_front({block, {H, TXRoot}})
				end,
				lists:reverse(Recent)
			)
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
find_current_block([Peer | Tail]) ->
	try ar_node:get_block_index(Peer) of
		[] ->
			find_current_block(Tail);
		BI ->
			{Hash, _, _} = hd(BI),
			ar:info([
				"Fetching current block.",
				{peer, Peer},
				{hash, Hash}
			]),
			MaybeB = ar_http_iface_client:get_block([Peer], Hash),
			case MaybeB of
				Atom when is_atom(Atom) ->
					ar:info([
						"Failed to fetch block from peer. Will retry using a different one.",
						{reply, Atom}
					]),
					Atom;
				B ->
					{B, BI}
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

%% @doc Get a block, and its 2 * ?MAX_TX_ANCHOR_DEPTH previous blocks.
%% If the block list is shorter than 2 * ?MAX_TX_ANCHOR_DEPTH, simply
%% get all existing blocks.
%%
%% The node needs 2 * ?MAX_TX_ANCHOR_DEPTH block anchors so that it
%% can validate transactions even if it enters a ?MAX_TX_ANCHOR_DEPTH-deep
%% fork recovery (which is the deepest fork recovery possible) immediately after
%% joining the network.
get_block_and_trail(_Peers, NewB, []) ->
	%% Joining on the genesis block.
	TXIDs = [TX#tx.id || TX <- NewB#block.txs],
	ar_storage:write_block(NewB#block{ txs = TXIDs }),
	[{NewB#block.indep_hash, TXIDs}];
get_block_and_trail(Peers, NewB, BI) ->
	get_block_and_trail(Peers, NewB, 2 * ?MAX_TX_ANCHOR_DEPTH, BI, []).

get_block_and_trail(_Peers, NewB, _BehindCurrent, _BI, BlockTXPairs)
		when NewB#block.height == 0 ->
	ar_storage:write_full_block(NewB),
	TXIDs = [TX#tx.id || TX <- NewB#block.txs],
	BlockTXPairs ++ [{NewB#block.indep_hash, TXIDs}];
get_block_and_trail(_, NewB, 0, _, BlockTXPairs) ->
	TXIDs = [TX#tx.id || TX <- NewB#block.txs],
	BlockTXPairs ++ [{NewB#block.indep_hash, TXIDs}];
get_block_and_trail(Peers, NewB, BehindCurrent, BI, BlockTXPairs) ->
	PreviousBlock = ar_http_iface_client:get_block(
		Peers,
		NewB#block.previous_block
	),
	case ?IS_BLOCK(PreviousBlock) of
		true ->
			ar_storage:write_full_block(NewB),
			TXIDs = [TX#tx.id || TX <- NewB#block.txs],
			NewBlockTXPairs = BlockTXPairs ++ [{NewB#block.indep_hash, TXIDs}],
			ar:info(
				[
					{writing_block, NewB#block.height},
					{blocks_written, (2 * ?MAX_TX_ANCHOR_DEPTH - (BehindCurrent - 1))},
					{blocks_to_write, (BehindCurrent - 1)}
				]
			),
			get_block_and_trail(Peers, PreviousBlock, BehindCurrent - 1, BI, NewBlockTXPairs);
		false ->
			ar:info(
				[
					could_not_retrieve_joining_block,
					retrying
				]
			),
			timer:sleep(3000),
			get_block_and_trail(Peers, NewB, BehindCurrent, BI, BlockTXPairs)
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
		2 = (ar_storage:read_block(B))#block.height
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
		3 = (ar_storage:read_block(B))#block.height
	end}.
