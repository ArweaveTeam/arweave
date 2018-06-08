-module(ar_fork_recovery).
-export([start/3]).
-export([multiple_blocks_ahead_with_transaction_recovery_test_slow/0]).
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
start(Peers, TargetBShadow, HashList) ->
	Parent = self(),
	case ?IS_BLOCK(TargetBShadow) of
		true ->
			ar:report(
				[
					{started_fork_recovery_proc, Parent},
					{block, ar_util:encode(TargetBShadow#block.indep_hash)},
					{target_height, TargetBShadow#block.height},
					{peer, Peers}
				]
			),
			case TargetBShadow#block.height == length(TargetBShadow#block.hash_list) of
				true ->
					PID =
						spawn(
							fun() ->
								TargetB = TargetBShadow,
								DivergedHashes = drop_until_diverge(
									lists:reverse(TargetB#block.hash_list),
									lists:reverse(HashList)
								) ++ [TargetB#block.indep_hash],
								server(
									#state {
										parent = Parent,
										peers = Peers,
										block_list = (TargetB#block.hash_list -- DivergedHashes),
										hash_list = DivergedHashes,
										target_block = TargetB
									}
								)
							end
						),
					PID ! {apply_next_block},
					PID;
				false ->
					ar:report(
					[
						{could_not_start_fork_recovery},
						{target_block_hash_list_incorrect}
					]
				),
				undefined
			end;
		false ->
			ar:report(
				[
					{could_not_start_fork_recovery},
					{could_not_retrieve_target_block}
				]
			),
			undefined
	end.

%% @doc Take two lists, drop elements until they do not match.
%% Return the remainder of the _first_ list.
drop_until_diverge([X|R1], [X|R2]) -> drop_until_diverge(R1, R2);
drop_until_diverge(R1, _) -> R1.

%% @doc Subtract the second list from the first. If the first list
%% is not a superset of the second, return the empty list
setminus([X|R1], [X|R2]) -> setminus(R1, R2);
setminus(R1, []) -> R1;
setminus(_, _) -> [].

%% @doc Start the fork recovery server loop. Attempt to catch up to the target block
%% by applying each block between the current block and the target block in turn
server(#state{peers = _Peers, parent = _Parent, target_block = _TargetB}, rejoin) ->
	ok.
server(#state {block_list = BlockList, hash_list = [], parent = Parent}) ->
	% lists:foreach(
	% 	fun(B) ->
	% 		ar:d({blocklist_block, ar_util:encode(B)})
	% 	end,
	% 	BlockList
	% ),
	Parent ! {fork_recovered, BlockList};
server(S = #state {block_list = BlockList, peers = Peers, hash_list = [NextH|HashList], target_block = TargetB }) ->
	receive
	{update_target_block, Block, Peer} ->
		NewBHashlist = [Block#block.indep_hash|Block#block.hash_list],
		% If the new retarget blocks hashlist contains the hash of the last retarget
		% should be recovering to the same fork.
		case lists:member(TargetB#block.indep_hash, NewBHashlist) of
			true ->
				HashListExtra =
					setminus(
						lists:reverse([NewBHashlist]),
						lists:reverse(BlockList) ++ [NextH|HashList]
					);
			false -> HashListExtra = []
		end,
		case HashListExtra of
		[] ->
			ar:d(failed_to_update_target_block), 
			server(S);
		H ->
			ar:d({current_target, TargetB#block.height}),
			ar:d({updating_target_block, Block#block.height}),
			server(
				S#state {
					hash_list = [NextH|HashList] ++ H,
					peers = ar_util:unique(Peer ++ Peers),
					target_block = Block
				}
			)
		end;
	{apply_next_block} ->
		NextB = ar_node:get_full_block(Peers, NextH),
		ar:d({applying_fork_recovery, ar_util:encode(NextH)}),
		case ?IS_BLOCK(NextB) of
			false ->
				BHashList = unavailable,
				B = unavailable,
				RecallB = unavailable,
				TXs = [];
			true ->
				case {NextB#block.height, ((TargetB#block.height - NextB#block.height) > ?STORE_BLOCKS_BEHIND_CURRENT)} of
					{0, _} ->
						ar:report(
							[
								{fork_recovery_failed},
								{recovery_block_is_genesis_block}
								% {rejoining_on_trusted_peers}
							]
						),
						BHashList = unavailable,
						B = unavailable,
						RecallB = unavailable,
						TXs = [],
						server(S, rejoin);
					{_, true} ->
						ar:report(
							[
								{fork_recovery_failed},
								{recovery_block_is_too_far_ahead}
								% {rejoining_on_trusted_peers}
							]
						),
						BHashList = unavailable,
						B = unavailable,
						RecallB = unavailable,
						TXs = [],
						server(S, rejoin);
					{_X, _Y} ->
						B = ar_node:get_block(Peers, NextB#block.previous_block),
						case ?IS_BLOCK(B) of
							false ->
								BHashList = unavailable,
								RecallB = unavailable,
								TXs = [];
							true ->
								BHashList = [B#block.indep_hash|B#block.hash_list],
								case B#block.height of
									0 -> RecallB = ar_node:get_full_block(Peers, ar_util:get_recall_hash(B, NextB#block.hash_list));
									_ -> RecallB = ar_node:get_full_block(Peers, ar_util:get_recall_hash(B, B#block.hash_list))
								end,
								%%TODO: Rewrite validate so it also takes recall block txs
								% ar:d({old_block, B#block.indep_hash}),
								% ar:d({new_block, NextB#block.indep_hash}),
								% ar:d({recall_block, RecallB#block.indep_hash}),
								% ar:d({old_block_txs, B#block.txs}),
								% ar:d({new_block_txs, NextB#block.txs}),
								% ar:d({recall_block_txs, RecallB#block.txs}),
								ar_storage:write_tx(RecallB#block.txs),
								TXs = NextB#block.txs
						end
				end
		end,
		case
			(not ?IS_BLOCK(NextB)) or
			(not ?IS_BLOCK(B)) or
			(not ?IS_BLOCK(RecallB))
		of
			false ->
				case try_apply_block(
						BHashList,
						NextB#block {txs = [T#tx.id||T <- NextB#block.txs]},
						TXs,
						B,
						RecallB#block {txs = [T#tx.id||T <- RecallB#block.txs]})
				of
					false ->
						ar:report_console(
							[
								could_not_validate_fork_block,
								{next_block, ?IS_BLOCK(NextB)},
								{block, ?IS_BLOCK(B)},
								{recall_block, ?IS_BLOCK(RecallB)}
							]
						);
					true ->
						ar:report_console(
							[
								{applying_block, ar_util:encode(NextH)},
								{block_height, NextB#block.height}
							]
						),
						self() ! {apply_next_block},
						ar_storage:write_tx(NextB#block.txs),
						ar_storage:write_block(NextB#block {txs = [T#tx.id||T <- NextB#block.txs]}),
						ar_storage:write_block(RecallB#block {txs = [T#tx.id||T <- RecallB#block.txs]}),
						server(
							S#state {
								block_list = [NextH|BlockList],
								hash_list = HashList
							}
						)
				end;
			true -> server(S#state {hash_list = []} )
		end;
	_ -> server(S)
	end.

%% @doc Try and apply a block
try_apply_block(_, NextB, _TXs, B, RecallB) when
		(not ?IS_BLOCK(NextB)) or
		(not ?IS_BLOCK(B)) or
		(not ?IS_BLOCK(RecallB)) ->
	false;
try_apply_block(HashList, NextB, TXs, B, RecallB) ->
	{FinderReward, _} = 
		ar_node:calculate_reward_pool(
			B#block.reward_pool,
			TXs,
			NextB#block.reward_addr,
			ar_node:calculate_proportion(
				RecallB#block.block_size,
				NextB#block.weave_size,
				NextB#block.height
			)
		),
	WalletList =
		ar_node:apply_mining_reward(
			ar_node:apply_txs(B#block.wallet_list, TXs),
			NextB#block.reward_addr,
			FinderReward,
			NextB#block.height
		),
	ar_node:validate(
		HashList,
		WalletList,
		NextB,
		TXs,
		B,
		RecallB,
		NextB#block.reward_addr,
		NextB#block.tags
	).
%%% Tests

%% @doc Ensure forks that are one block behind will resolve.
three_block_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B0 = ar_weave:init([]),
	ar_storage:write_block(hd(B0)),
	B1 = ar_weave:add(B0, []),
	ar_storage:write_block(hd(B1)),
	B2 = ar_weave:add(B1, []),
	ar_storage:write_block(hd(B2)),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(hd(B3)),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	ar:d(break1),
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
	B0 = ar_weave:init([]),
	ar_storage:write_block(hd(B0)),
	B1 = ar_weave:add(B0, []),
	ar_storage:write_block(hd(B1)),
	B2 = ar_weave:add(B1, []),
	ar_storage:write_block(hd(B2)),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(hd(B3)),
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
	receive after 1500 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B))#block.height.

multiple_blocks_ahead_with_transaction_recovery_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B0 = ar_weave:init([]),
	ar_storage:write_block(hd(B0)),
	B1 = ar_weave:add(B0, []),
	ar_storage:write_block(hd(B1)),
	B2 = ar_weave:add(B1, []),
	ar_storage:write_block(hd(B2)),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(hd(B3)),
	Node1 ! Node2 ! {replace_block_list, B3},
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	receive after 300 -> ok end,
	ar_node:add_tx(Node1, SignedTX),
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
	receive after 1500 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B))#block.height.

%% @doc Ensure that nodes that have diverged by multiple blocks each can reconcile.
multiple_blocks_since_fork_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B0 = ar_weave:init([]),
	ar_storage:write_block(hd(B0)),
	B1 = ar_weave:add(B0, []),
	ar_storage:write_block(hd(B1)),
	B2 = ar_weave:add(B1, []),
	ar_storage:write_block(hd(B2)),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(hd(B3)),
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
	receive after 1500 -> ok end,
	[B|_] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B))#block.height.

%% @doc Ensure that nodes that nodes recovering from the first block can reconcile
% fork_from_first_test() ->
% 	ar_storage:clear(),
% 	B1 = ar_weave:init([]),
% 	Node1 = ar_node:start([], B1),
% 	Node2 = ar_node:start(Node1, B1),
% 	ar_node:mine(Node1),
% 	receive after 300 -> ok end,
% 	ar_node:mine(Node1),
% 	receive after 300 -> ok end,
% 	ar_node:add_peers(Node1, Node2),
% 	ar_node:mine(Node1),
% 	receive after 300 -> ok end,
% 	true = (ar_node:get_blocks(Node1) == ar_node:get_blocks(Node2)).

%% @doc Check the logic of setminus will correctly update to a new fork
setminus_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	B0 = ar_weave:init([]),
	ar_storage:write_block(hd(B0)),
	B1 = ar_weave:add(B0, []),
	ar_storage:write_block(hd(B1)),
	B2 = ar_weave:add(B1, []),
	ar_storage:write_block(hd(B2)),
	B3 = ar_weave:add(B2, []),
	ar_storage:write_block(hd(B3)),
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
