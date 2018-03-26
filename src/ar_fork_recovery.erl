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
			{started_fork_recovery_proc, Parent},
			{block, ar_util:encode(TargetB#block.indep_hash)},
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
						) ++ [TargetB#block.indep_hash],
					target_block = TargetB
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
%% TODO: Runs into trouble when recovering from diverged genesis blocks
%% Though, it shouldn't ever get into the situation where it needs to
server(#state {block_list = BlockList, hash_list = [], parent = Parent}) ->
	Parent ! {fork_recovered, BlockList};
server(S = #state {block_list = BlockList, peers = Peers, hash_list = [NextH|HashList], target_block = TargetB }) ->
	receive
	{update_target_block, Block, Peer} ->
		HashListExtra = setminus(
			lists:reverse([Block#block.indep_hash|Block#block.hash_list]),
			[NextH|HashList] ++ lists:reverse(BlockList)
		),
		case HashListExtra of
		[] -> server(S);
		H ->
			ar:d({current_target, TargetB#block.height}),
			ar:d({updating_target_block, Block#block.height}),
			ar:d(H),
			server(
				S#state {
					hash_list = [NextH|HashList] ++ H,
					peers = ar_util:unique(Peer ++ Peers),
					target_block = Block
				}
			)
		end;
	{apply_next_block} ->
		NextB = ar_node:get_block(Peers, NextH),
		case ?IS_BLOCK(NextB) of
			false ->
				BHashList = unavailable,
				B = unavailable,
				RecallB = unavailable;
			true ->
				B = ar_node:get_block(Peers, NextB#block.previous_block),
				case ?IS_BLOCK(B) of
					false ->
						BHashList = unavailable,
						RecallB = unavailable;
					true ->
						BHashList = [B#block.indep_hash|B#block.hash_list],
						RecallB = ar_node:get_block(Peers, ar_util:get_recall_hash(B, B#block.hash_list))
				end
		end,
		case try_apply_block(BHashList, NextB, B, RecallB) of
			false ->
				ar:report(
					[
						could_not_validate_fork_block,
						{next_block, ?IS_BLOCK(NextB)},
						{block, ?IS_BLOCK(B)},
						{recall_block, ?IS_BLOCK(RecallB)}
					]
				);
			true ->
				ar:report(
					[
						{applying_block, ar_util:encode(NextH)},
						{block_height, NextB#block.height}
					]
				),
				self() ! {apply_next_block},
				ar_storage:write_block(NextB),
				ar_storage:write_block(RecallB),
				server(
					S#state {
						block_list = [NextH|BlockList],
						hash_list = HashList
					}
				)
		end;
	_ -> server(S)
	end.

%% @doc Try and apply a block
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
