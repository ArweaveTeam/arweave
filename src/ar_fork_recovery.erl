-module(ar_fork_recovery).
-export([start/3]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An asynchronous process that asks another node on a different fork
%%% for all of the blocks required to 'catch up' with the network,
%%% verifying each in turn. Once the blocks since divergence have been
%%% verified, the process returns the new state to its parent. Target is
%%% height at which block height ~ought~ to be. Hash lists is forked.

%% Defines the server state
-record(state, {
	parent,
	peers,
	target_block,
	block_list,
	hash_list
}).

%% @doc Start the 'catch up' server.
start(Peers, TargetB, HashList) ->
	Parent = self(),
	ar:report(
		[
			{started_fork_recovery_proc, self()},
			{target_height, TargetB#block.height},
			{peer, Peers}
		]
	),
	PID =
	spawn(
		fun() ->
			server(
				#state {
					parent = Parent,
					peers = Peers,
					block_list = HashList,
					hash_list =
						drop_until_diverge(
							lists:reverse(TargetB#block.hash_list),
							lists:reverse(HashList)
						) ++ [TargetB#block.indep_hash]
				}
			)
		end
	),
	PID ! {apply_next_block},
	PID.

%% @doc Take two lists, drop elements until they do not match.
%% Return the remainder of the _first_ list.
drop_until_diverge([X|R1], [X|R2]) -> drop_until_diverge(R1, R2);
drop_until_diverge(R1, _) -> R1.

%% @doc Subtract the second list from the first. If the first list
%% is not a superset of the second, return the empty list
setminus([X|R1], [X|R2]) -> setminus(R1, R2);
setminus(R1, []) -> R1;
setminus(_, _) -> [].

%% @doc Main server loop
server(#state {block_list = BlockList, hash_list = [], parent = Parent}) ->
	Parent ! {fork_recovered, BlockList};
server(S = #state {block_list = BlockList, peers = Peers, hash_list = [NextH|HashList] }) ->
	ar:d(whereis(http_entrypoint_node)),
	receive
	{update_target_block, Block} ->
		ar:d({updating_target_block, Block#block.indep_hash}),
		server(
			S#state {
				hash_list =
					[NextH|HashList] ++
					setminus(
						lists:reverse([Block#block.indep_hash|Block#block.hash_list]),
						[NextH|HashList] ++ lists:reverse(BlockList)
					)
			}
		);
	{apply_next_block} ->
		ar:d({applying_block, NextH}),
		NextB = ar_node:get_block(Peers, NextH),
		ar:d(whereis(http_entrypoint_node)),
		B = ar_storage:read_block(NextB#block.previous_block),
		ar:d(whereis(http_entrypoint_node)),
		RecallB = ar_node:get_block(Peers, ar_util:get_recall_hash(B, B#block.hash_list)),
		ar:d(whereis(http_entrypoint_node)),
		case try_apply_block([B#block.indep_hash|B#block.hash_list], NextB, B, RecallB) of
			false ->
				ar:d(could_not_validate_fork_block);
			true ->
				self() ! {apply_next_block},
				ar:d(whereis(http_entrypoint_node)),
				ar_storage:write_block(NextB),
				ar:d(whereis(http_entrypoint_node)),
				ar_storage:write_block(RecallB),
				ar:d(whereis(http_entrypoint_node)),
				server(
					S#state {
						block_list = [NextH|BlockList],
						hash_list = HashList
					}
				)
		end;
	_ -> server(S)
	end.

try_apply_block(_, NextB, B, RecallB) when
		(not ?IS_BLOCK(NextB)) or
		(not ?IS_BLOCK(B)) or
		(not ?IS_BLOCK(RecallB)) ->
	false;
try_apply_block(HashList, NextB, B, RecallB) ->
	ar_node:validate(HashList,
		ar_node:apply_txs(B#block.wallet_list, NextB#block.txs),
		NextB,
		B,
		RecallB
	).

%%% Tests

%% @doc Ensure forks that are one block behind will resolve.
three_block_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init([]), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 500 -> ok end,
	ar_node:mine(Node1),
	receive after 500 -> ok end,
	ar_node:mine(Node1),
	receive after 500 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 2000 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	7 = (ar_storage:read_block(B))#block.height.

%% @doc Ensure that nodes on a fork that is far behind will catchup correctly.
multiple_blocks_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init([]), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B))#block.height.

%% @doc Ensure that nodes that have diverged by multiple blocks each can reconcile.
multiple_blocks_since_fork_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init([]), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B))#block.height.

%% @doc Check the logic of setminus will correctly update to a new fork
setminus_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B1 = ar_weave:add(ar_weave:init([]), []),
	B2 = ar_weave:add(B1, []),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	ar_node:mine(Node1),
	receive after 300 -> ok end,
	LengthLong = length(
		setminus(lists:reverse(ar_node:get_blocks(Node1)),
		lists:reverse(ar_node:get_blocks(Node2)))
	),
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	LengthShort = length(
		setminus(lists:reverse(ar_node:get_blocks(Node1)),
		lists:reverse(ar_node:get_blocks(Node2)))
		),
	LengthLong = 2,
	LengthShort = 0.
