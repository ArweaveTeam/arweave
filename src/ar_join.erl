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
	start(Node, Peers, ar_node:get_current_block(Peers)).
start(Node, Peers, B) when is_atom(B) -> 
	ar:report_console(
		[
			could_not_retrieve_current_block,
			{trying_again_in, ?REJOIN_TIMEOUT, seconds}
		]
	),
	timer:apply_after(?REJOIN_TIMEOUT, ar_join, start, [Node, Peers]);
start(_, _, not_found) -> do_nothing;
start(_, _, unavailable) -> do_nothing;
start(_, _, no_response) -> do_nothing;
start(Node, RawPeers, RawNewB) ->
	case whereis(join_server) of
		undefined ->
			PID = spawn(
				fun() ->
					Peers = filter_peer_list(RawPeers),
					NewB = ar_node:get_full_block(Peers, RawNewB#block.indep_hash),
					join_peers(Peers),
					case ?IS_BLOCK(NewB) of
						true ->
							ar:report_console(
								[
									joining_network,
									{node, Node},
									{peers, Peers},
									{height, NewB#block.height}
								]
							),
							get_block_and_trail(Peers, NewB, NewB#block.hash_list),
							Node ! {fork_recovered, [NewB#block.indep_hash|NewB#block.hash_list]},
							fill_to_capacity(Peers, [], NewB#block.hash_list);
						false -> ok
					end
				end
			),
			erlang:register(join_server, PID);
		_ -> already_running
	end.

%% @doc Verify peer(s) are on the same network as the client. Remove any that
%% are not.
-ifdef(DEBUG).
filter_peer_list(Peers) when is_list(Peers) ->
	lists:filter(
		fun(Peer) when is_pid(Peer) -> true;
		   (Peer) -> ar_http_iface:get_info(Peer, name) == <<?NETWORK_NAME>>
		end,
	Peers);
filter_peer_list(Peer) -> filter_peer_list([Peer]).
-else.
filter_peer_list(Peers) when is_list(Peers) ->
	lists:filter(
		fun(Peer) when is_pid(Peer) -> false;
		   (Peer) -> ar_http_iface:get_info(Peer, name) == <<?NETWORK_NAME>>
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
join_peers(Peer) -> ar_http_iface:add_peer(Peer).

%% @doc Get a block, and its ?STORE_BLOCKS_BEHIND_CURRENT previous
%% blocks and recall blocks. Alternatively, if the blocklist is shorter than
%% ?STORE_BLOCKS_BEHIND_CURRENT, simply get all existing blocks and recall blocks
get_block_and_trail(_Peers, NewB, []) ->
	ar_storage:write_block(NewB#block { txs = [T#tx.id || T <- NewB#block.txs] });
get_block_and_trail(Peers, NewB, HashList) ->
	get_block_and_trail(Peers, NewB, ?STORE_BLOCKS_BEHIND_CURRENT, HashList).
get_block_and_trail(_, unavailable, _, _) -> ok;
get_block_and_trail(Peers, NewB, _, _) when NewB#block.height =< 1 ->
	ar_storage:write_block(NewB#block { txs = [T#tx.id || T <- NewB#block.txs] }),
	ar_storage:write_tx(NewB#block.txs),
	PreviousBlock = ar_node:get_block(Peers, NewB#block.previous_block),
	ar_storage:write_block(PreviousBlock);
get_block_and_trail(_, _, 0, _) -> ok;
get_block_and_trail(Peers, NewB, BehindCurrent, HashList) ->
	PreviousBlock = ar_node:get_full_block(Peers, NewB#block.previous_block),
	case ?IS_BLOCK(PreviousBlock) of
		true ->
			RecallBlock = ar_util:get_recall_hash(PreviousBlock, HashList),
			case {NewB, ar_node:get_full_block(Peers, RecallBlock)} of
				{B, unavailable} ->
					ar_storage:write_tx(B#block.txs),
					ar_storage:write_block(B#block { txs = [T#tx.id || T <- B#block.txs] } ),
					ar:report(
						[
							{could_not_retrieve_joining_recall_block},
							{retrying}
						]
					),
					timer:sleep(3000),
					get_block_and_trail(Peers, NewB, BehindCurrent, HashList);
				{B, R} ->				
					ar_storage:write_tx(B#block.txs),
					ar_storage:write_block(B#block { txs = [T#tx.id || T <- B#block.txs] } ),
					ar_storage:write_tx(R#block.txs),
					ar_storage:write_block(R#block { txs = [T#tx.id || T <- R#block.txs] } ),
					ar:report(
						[
							{writing_block, B#block.height},
							{writing_recall_block, R#block.height},
							{blocks_written, (?STORE_BLOCKS_BEHIND_CURRENT - ( BehindCurrent -1 ))},
							{blocks_to_write, (BehindCurrent-1)}
						]
					),	
					get_block_and_trail(Peers, PreviousBlock, BehindCurrent-1, HashList)
			end;
		false ->
			ar:report(
				[
					{could_not_retrieve_joining_block},
					{retrying}
				]
			),
			timer:sleep(3000),
			get_block_and_trail(Peers, NewB, BehindCurrent, HashList)
	end.

%% @doc Fills node to capacity based on weave storage limit.
fill_to_capacity(_, [], _) -> ok;
fill_to_capacity(Peers, Written, ToWrite) ->
	timer:sleep(10000),
	try
		RandBlock = lists:nth(rand:uniform(length(ToWrite)-1), ToWrite),
		case ar_node:get_full_block(Peers, RandBlock) of
			unavailable -> 
				timer:sleep(3000),
				fill_to_capacity(Peers, Written, ToWrite);
			B ->
				ar_storage:write_block( B#block {txs = [TX#tx.id || TX <- B#block.txs] }),
				ar_storage:write_tx(B#block.txs),
				fill_to_capacity(
					Peers,
					[RandBlock|Written],
					lists:delete(RandBlock, ToWrite)
				)
		end
	catch
	throw:Term ->
		ar:report(
			[
				{'EXCEPTION', {Term}}
			]
		),
		fill_to_capacity(Peers, Written, ToWrite);
	exit:Term ->
		ar:report(
			[
				{'EXIT', Term}
			]
		);
	error:Term ->
		ar:report(
			[
				{'EXIT', {Term, erlang:get_stacktrace()}}
			]
		),
		fill_to_capacity(Peers, Written, ToWrite)
	end.

%% @doc Check that nodes can join a running network by using the fork recoverer.
basic_node_join_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([], _B0 = ar_weave:init([])),
	receive after 1000 -> ok end,
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	Node2 = ar_node:start([Node1]),
	receive after 1500 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	2 = (ar_storage:read_block(B))#block.height.

%% @doc Ensure that both nodes can mine after a join.
node_join_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([], _B0 = ar_weave:init([])),
	receive after 1000 -> ok end,
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	Node2 = ar_node:start([Node1]),
	receive after 600 -> ok end,
	ar_node:mine(Node2),
	receive after 1000 -> ok end,
	[B|_] = ar_node:get_blocks(Node1),
	3 = (ar_storage:read_block(B))#block.height.
