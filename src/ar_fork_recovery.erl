-module(ar_fork_recovery).
-export([start/4]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An asynchronous process that asks another node on a different fork
%%% for all of the blocks required to 'catch up' with their state of the
%%% network - each block is verified in turn.
%%% Once all the blocks since the two forks of the netwrok diverged have been
%%% verified, the process returns this new state to its parent.
%%%
%%% Block shadows transmit the last 50 hashes of the hashlist, as such if a
%%% node falls more than this limit behind the fork recovery process will fail.

%% Defines the server state
-record(state, {
	parent, % Fork recoveries parent process (initiator)
	peers, % Lists of the nodes peers to try retrieve blocks
	target_block, % The target block being recovered too
	recovery_hash_list, % Complete hash list for this fork
	block_list, % List of hashes of verified blocks
	hash_list % List of block hashes needing to be verified and applied (lowest to highest)
}).

%% @doc Start the fork recovery 'catch up' server.
start(Peers, TargetBShadow, HashList, Parent) ->
	% TODO: At this stage the target block is not a shadow, it is either
	% a valid block or a block with a malformed hashlist (Outside FR range).
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
			% Ensures that the block is within the recovery range and is has
			% been validly rebuilt from a block shadow.
			case
				TargetBShadow#block.height == length(TargetBShadow#block.hash_list)
			of
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
										block_list =
											(TargetB#block.hash_list -- DivergedHashes),
										hash_list = DivergedHashes,
										target_block = TargetB,
										recovery_hash_list =
											[TargetB#block.indep_hash|TargetB#block.hash_list]
									}
								)
							end
						),
					PID ! apply_next_block,
					PID;
				% target block has invalid hash list
				false ->
					ar:report(
						[
							could_not_start_fork_recovery,
							{reason, target_block_hash_list_incorrect}
						]
				),
				undefined
			end;
		false ->
			ar:report(
				[
					could_not_start_fork_recovery,
					{reason, could_not_retrieve_target_block}
				]
			),
			undefined
	end.

%% @doc Take two lists, drop elements until they do not match.
%% Return the remainder of the _first_ list.
drop_until_diverge([X | R1], [X | R2]) -> drop_until_diverge(R1, R2);
drop_until_diverge(R1, _) -> R1.

%% @doc Subtract the second list from the first. If the first list
%% is not a superset of the second, return the empty list
setminus([X | R1], [X | R2]) -> setminus(R1, R2);
setminus(R1, []) -> R1;
setminus(_, _) -> [].

%% @doc Start the fork recovery server loop. Attempt to catch up to the
%% target block by applying each block between the current block and the
%% target block in turn.
server(#state {
		peers = _Peers,
		parent = _Parent,
		target_block = _TargetB
	}, rejoin) -> ok.
server(#state {
		block_list = BlockList,
		hash_list = [],
		parent = Parent
	}) ->
	Parent ! {fork_recovered, BlockList};
server(S = #state { target_block = TargetB }) ->
	receive
		{parent_accepted_block, B} ->
			if B#block.height > TargetB#block.height ->
				ar:report(
					[
						stopping_fork_recovery,
						{reason, parent_accepted_higher_block_than_target}
					]
				),
				ok;
			true ->
				ar:report(
					[
						continuing_fork_recovery,
						{reason, parent_accepted_lower_block_than_target}
					]
				),
				server(S)
			end
	after 0 ->
		do_fork_recover(S)
	end.

do_fork_recover(S = #state {
		block_list = BlockList,
		peers = Peers,
		hash_list = [NextH | HashList],
		target_block = TargetB,
		recovery_hash_list = BHL,
		parent = Parent
	}) ->
	receive
	{update_target_block, Block, Peer} ->
		NewBHL = [Block#block.indep_hash | Block#block.hash_list],
		% If the new retarget blocks hashlist contains the hash of the last
		% retarget should be recovering to the same fork.
		NewToVerify =
			case lists:member(TargetB#block.indep_hash, NewBHL) of
				true ->
					ar:report([encountered_block_on_same_fork_as_recovery_process]),
					drop_until_diverge(
						lists:reverse(NewBHL),
						lists:reverse(BlockList)
					);
				false ->
					ar:report([encountered_block_on_different_fork_to_recovery_process]),
					[]
			end,
		case NewToVerify =/= [] of
			true ->
				ar:report(
					[
						updating_fork_recovery_target,
						{current_target_height, TargetB#block.height},
						{current_target_hash, ar_util:encode(TargetB#block.indep_hash)},
						{new_target_height, Block#block.height},
						{new_target_hash, ar_util:encode(Block#block.indep_hash)},
						{still_to_verify, length(NewToVerify)}
					]
				),
				NewPeers =
					ar_util:unique(
						Peers ++
						if is_list(Peer) -> Peer;
						true -> [Peer]
						end
					),
				server(
					S#state {
						hash_list = NewToVerify,
						peers = NewPeers,
						target_block = Block,
						recovery_hash_list = NewBHL
					}
				);
			false ->
				ar:report(
					[
						not_updating_target_block,
						{ignored_block, ar_util:encode(Block#block.indep_hash)},
						{height, Block#block.height}
					]
				),
				server(S)
		end;
	apply_next_block ->
		NextB = ar_node_utils:get_full_block(Peers, NextH, BHL),
		ar:report(
			[
				{applying_fork_recovery, ar_util:encode(NextH)}
			]
		),
		case ?IS_BLOCK(NextB) of
			% could not retrieve the next block to be applied
			false ->
				ar:report(
					[
						{fork_recovery_block_retreival_failed, ar_util:encode(NextH)},
						{received_instead, NextB}
					]
				),
				BHashList = unavailable,
				B = unavailable,
				RecallB = unavailable,
				TXs = [];
			% next block retrieved, applying block
			true ->
				% Ensure that block being applied is not the genesis block and
				% is within the range of fork recovery.
				%%
				% TODO: Duplication of check, target block height is checked
				% when fork recovery process starts.
				case
					{
						NextB#block.height,
						((TargetB#block.height - NextB#block.height) >
							?STORE_BLOCKS_BEHIND_CURRENT)
					}
				of
					% Recovering to genesis block
					{0, _} ->
						ar:report(
							[
								fork_recovery_failed,
								recovery_block_is_genesis_block
							]
						),
						BHashList = unavailable,
						B = unavailable,
						RecallB = unavailable,
						TXs = [],
						server(S, rejoin);
					% Target block is too far ahead and cannot be recovered.
					{_, true} ->
						ar:report(
							[
								fork_recovery_failed,
								recovery_block_is_too_far_ahead
							]
						),
						BHashList = unavailable,
						B = unavailable,
						RecallB = unavailable,
						TXs = [],
						server(S, rejoin);
					% Target block is within range and isi attempted to be
					% recovered to.
					{_X, _Y} ->
						B = ar_node:get_block(Peers, NextB#block.previous_block, BHL),
						case ?IS_BLOCK(B) of
							false ->
								BHashList = unavailable,
								RecallB = unavailable,
								TXs = [];
							true ->
								BHashList = [B#block.indep_hash|B#block.hash_list],
								case B#block.height of
									0 -> RecallB = ar_node_utils:get_full_block(Peers, ar_util:get_recall_hash(B, NextB#block.hash_list), BHL);
									_ -> RecallB = ar_node_utils:get_full_block(Peers, ar_util:get_recall_hash(B, B#block.hash_list), BHL)
								end,
								%% TODO: Rewrite validate so it also takes recall block txs
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
		% Ensure next block (NextB) is a block, the previous block (B) is a
		% block and that the nexts blocks recall block (RecallB) is a block.
		case
			(not ?IS_BLOCK(NextB)) or
			(not ?IS_BLOCK(B)) or
			(not ?IS_BLOCK(RecallB))
		of
			false ->
				case
					try_apply_block(
						BHashList,
						NextB#block {txs = [T#tx.id || T <- NextB#block.txs]},
						TXs,
						B,
						RecallB#block {txs = [T#tx.id || T <- RecallB#block.txs]}
					)
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
								{applied_fork_recovery_block, ar_util:encode(NextH)},
								{block_height, NextB#block.height}
							]
						),
						case ar_meta_db:get(partial_fork_recovery) of
							true ->
								ar:report(
									[
										reported_partial_fork_recovery,
										{height, NextB#block.height}
									]
								),	
								Parent ! {fork_recovered, [NextH | BlockList]};
							_ -> do_nothing
						end,
						self() ! apply_next_block,
						ar_storage:write_tx(NextB#block.txs),
						ar_storage:write_block(NextB#block {txs = [T#tx.id || T <- NextB#block.txs]}),
						ar_storage:write_block(RecallB#block {txs = [T#tx.id || T <- RecallB#block.txs]}),
						app_search:update_tag_table(NextB),
						server(
							S#state {
								block_list = [NextH | BlockList],
								hash_list = HashList
							}
						)
				end;
			true -> server(S#state { hash_list = [] } )
		end;
	_ -> server(S)
	end.

%% @doc Try and apply a new block (NextB) to the current block (B).
%% Returns	true if the block can be applied, otherwise false.
try_apply_block(_, NextB, _TXs, B, RecallB) when
		(not ?IS_BLOCK(NextB)) or
		(not ?IS_BLOCK(B)) or
		(not ?IS_BLOCK(RecallB)) ->
	false;
try_apply_block(HashList, NextB, TXs, B, RecallB) ->
	{FinderReward, _} =
		ar_node_utils:calculate_reward_pool(
			B#block.reward_pool,
			TXs,
			NextB#block.reward_addr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				NextB#block.weave_size,
				NextB#block.height
			)
		),
	WalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(B#block.wallet_list, TXs),
			NextB#block.reward_addr,
			FinderReward,
			NextB#block.height
		),
	ar_node_utils:validate(
		HashList,
		WalletList,
		NextB,
		TXs,
		B,
		RecallB,
		NextB#block.reward_addr,
		NextB#block.tags
	).

%%%
%%% Tests: ar_fork_recovery
%%%

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
	timer:sleep(500),
	ar_node:mine(Node1),
	timer:sleep(500),
	ar_node:mine(Node2),
	timer:sleep(500),
	ar_node:mine(Node1),
	timer:sleep(500),
	ar_node:mine(Node1),
	timer:sleep(500),
	ar_node:add_peers(Node1, Node2),
	timer:sleep(500),
	ar_node:mine(Node1),
	timer:sleep(1000),
	?assertEqual(block_hashes_by_node(Node1), block_hashes_by_node(Node2)),
	?assertEqual(8, length(block_hashes_by_node(Node2))).

block_hashes_by_node(Node) ->
	BHs = ar_node:get_blocks(Node),
	Bs = [ar_storage:read_block(BH, ar_node:get_hash_list(Node)) || BH <- BHs],
	[ar_util:encode(B#block.indep_hash) || B <- Bs].

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
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	timer:sleep(1500),
	?assertEqual(block_hashes_by_node(Node1), block_hashes_by_node(Node2)),
	?assertEqual(10, length(block_hashes_by_node(Node2))).

%% @doc Ensure that nodes on a fork that is far behind blocks that contain
%% transactions will catchup correctly.
multiple_blocks_ahead_with_transaction_recovery_test_() ->
	{timeout, 60, fun() ->
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
		?assertEqual(block_hashes_by_node(Node1), block_hashes_by_node(Node2)),
		?assertEqual(10, length(block_hashes_by_node(Node2)))
	end}.

%% @doc Ensure that nodes that have diverged by multiple blocks each can
%% reconcile.
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
	timer:sleep(300),
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	timer:sleep(1500),
	?assertEqual(block_hashes_by_node(Node1), block_hashes_by_node(Node2)),
	?assertEqual(10, length(block_hashes_by_node(Node2))).

%% @doc Ensure that nodes that nodes recovering from the first block can
%% reconcile.
% fork_from_first_test() ->
%	ar_storage:clear(),
%	B1 = ar_weave:init([]),
%	Node1 = ar_node:start([], B1),
%	Node2 = ar_node:start(Node1, B1),
%	ar_node:mine(Node1),
%	receive after 300 -> ok end,
%	ar_node:mine(Node1),
%	receive after 300 -> ok end,
%	ar_node:add_peers(Node1, Node2),
%	ar_node:mine(Node1),
%	receive after 300 -> ok end,
%	true = (ar_node:get_blocks(Node1) == ar_node:get_blocks(Node2)).

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
	timer:sleep(300),
	ar_node:mine(Node1),
	timer:sleep(300),
	LengthLong = length(
		setminus(lists:reverse(ar_node:get_blocks(Node1)),
		lists:reverse(ar_node:get_blocks(Node2)))
	),
	ar_node:mine(Node1),
	ar_node:mine(Node2),
	timer:sleep(300),
	LengthShort = length(
		setminus(
			lists:reverse(ar_node:get_blocks(Node1)),
			lists:reverse(ar_node:get_blocks(Node2)))
		),
	LengthLong = 2,
	LengthShort = 0.
