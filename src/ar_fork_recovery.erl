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
	block_list, % List of block hashes shared between current and target block
	hash_list % List of block hashes needing to be applied
}).

%% @doc Start the fork recovery 'catch up' server.
start(_, TargetBShadow, _, _) when not ?IS_BLOCK(TargetBShadow) ->
	ar:report(
		[
			could_not_start_fork_recovery,
			{reason, could_not_retrieve_target_block}
		]
	),
	undefined;
start(_, TargetBShadow, _, _)
		when TargetBShadow#block.height /= length(TargetBShadow#block.hash_list) ->
	% Ensures that the block is within the recovery range and is has
	% been validly rebuilt from a block shadow.
	ar:report(
		[
			could_not_start_fork_recovery,
			{reason, target_block_hash_list_incorrect}
		]
	),
	undefined;
start(Peers, TargetBShadow, HashList, Parent) ->
	% TODO: At this stage the target block is not a shadow, it is either
	% a valid block or a block with a malformed hashlist (Outside FR range).
	PID =
		spawn(
			fun() ->
				TBHL = TargetBShadow#block.hash_list,
				TBIH = TargetBShadow#block.indep_hash,
				DivergedHashes = drop_until_diverge(
					lists:reverse(TBHL),
					lists:reverse(HashList)
					) ++ [TBIH],
				server(
					#state {
						parent = Parent,
						peers = Peers,
						block_list = (TBHL -- DivergedHashes),
						hash_list = DivergedHashes,
						target_block = TargetBShadow,
						recovery_hash_list = [TBIH | TBHL]
					}
				)
			end
		),
	PID ! apply_next_block,
	PID.

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
		block_list = BlockList,
		hash_list = [],
		parent = Parent
	}) ->
	Parent ! {fork_recovered, BlockList};
server(S = #state {
		block_list = BlockList,
		peers = Peers,
		hash_list = [NextH | HashList],
		target_block = TargetB,
		recovery_hash_list = BHL
	}) ->
	receive
	{update_target_block, Block, Peer} ->
		NewBHashlist = [Block#block.indep_hash | Block#block.hash_list],
		% If the new retarget blocks hashlist contains the hash of the last
		% retarget should be recovering to the same fork.
		case lists:member(TargetB#block.indep_hash, NewBHashlist) of
			true ->
				HashListExtra =
					setminus(
						lists:reverse([NewBHashlist]),
						lists:reverse(BlockList) ++ [NextH | HashList]
					);
			false -> HashListExtra = []
		end,
		case HashListExtra of
			[] ->
				ar:report(failed_to_update_target_block),
				server(S);
			H ->
				ar:report(
					[
						{current_target, TargetB#block.height},
						{updating_target_block, Block#block.height}
					]
				),
				server(
					S#state {
						hash_list = [NextH | HashList] ++ H,
						peers = ar_util:unique(Peer ++ Peers),
						target_block = Block
					}
				)
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
						ok;
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
						ok;
					% Target block is within range and is attempted to be
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
								HLtoUse =
									case B#block.height of
										0 -> NextB#block.hash_list;
										_ -> B#block.hash_list
									end,
								RecallB = ar_node_utils:get_full_block(
									Peers,
									ar_util:get_recall_hash(B, HLtoUse), BHL),
								%% TODO: Rewrite validate so it also takes recall block txs
								ar_storage:write_tx(RecallB#block.txs),
								TXs = NextB#block.txs
						end
				end
		end,
		% Ensure the next block (NextB) is a block, the previous block (B)
		% is a block, and the next block's recall block (RecallB) is a block.
		case
			?IS_BLOCK(NextB) andalso ?IS_BLOCK(B) andalso ?IS_BLOCK(RecallB)
		of
			true ->
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
						self() ! apply_next_block,
						ar_storage:write_tx(NextB#block.txs),
						ar_storage:write_block(NextB#block {txs = [T#tx.id || T <- NextB#block.txs]}),
						ar_storage:write_block(RecallB#block {txs = [T#tx.id || T <- RecallB#block.txs]}),
						server(
							S#state {
								block_list = [NextH | BlockList],
								hash_list = HashList
							}
						)
				end;
			false -> server(S#state { hash_list = [] } )
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
	[B | _] = ar_node:get_blocks(Node2),
	7 = (ar_storage:read_block(B, ar_node:get_hash_list(Node2)))#block.height.

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
	[B | _] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B, ar_node:get_hash_list(Node2)))#block.height.

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
		[B | _] = ar_node:get_blocks(Node2),
		9 = (ar_storage:read_block(B, ar_node:get_hash_list(Node2)))#block.height
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
	[B | _] = ar_node:get_blocks(Node2),
	9 = (ar_storage:read_block(B, ar_node:get_hash_list(Node2)))#block.height.

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
		setminus(lists:reverse(ar_node:get_blocks(Node1)),
		lists:reverse(ar_node:get_blocks(Node2)))
		),
	LengthLong = 2,
	LengthShort = 0.
