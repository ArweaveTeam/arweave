-module(ar_fork_recovery).

-export([start/5]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An asynchronous process that asks another node on a different fork
%%% for all of the blocks required to 'catch up' with their state of
%%% the weave - each block is verified in turn. Once all the blocks since
%%% the two forks diverged have been verified, the process returns this
%%% new state to its parent.

-record(state, {
	parent_pid, % The parent process, sends new verified blocks, if any, and awaits a recovered state.
	peers, % The list of peers to retrieve blocks from.
	target_height, % The height of the last (most recent) block to apply.
	target_hashes, % All the hashes of the blocks to be applied during the fork recovery (lowest to highest).
	target_hashes_to_go, % The hashes of the blocks left to apply (lowest to highest).
	recovered_block_index, % The block index constructed during the fork recovery.
	recovered_block_txs_pairs, % The block anchors required for verifying transactions, updated in process.
	base_hash % The hash of the base block.
}).

%% @doc Start the fork recovery 'catch up' server.
%%
%% Peers - the list of peers to retrieve blocks from.
%% RecoveryHashes - the list of hashes of the blocks to apply (from highest to lowest).
%% BI - the block index where the most recent block is the block to apply the fork on.
%% ParentPID - the PID of the parent process.
%% BlockTXPairs - the block anchors required to verify transactions.
start(Peers, RecoveryHashes, BI, ParentPID, BlockTXPairs) ->
	TargetHashes = lists:reverse(RecoveryHashes),
	TargetHeight = length(BI) - 1 + length(RecoveryHashes),
	ar:info(
		[
			{event, started_fork_recovery},
			{target_height, TargetHeight},
			{target_hashes, lists:map(fun ar_util:encode/1, TargetHashes)},
			{top_peers, lists:sublist(Peers, 5)}
		]
	),
	PID = spawn(
		fun() ->
			server(
				#state {
					parent_pid = ParentPID,
					target_height = TargetHeight,
					peers = Peers,
					target_hashes = TargetHashes,
					target_hashes_to_go = TargetHashes,
					recovered_block_index = BI,
					recovered_block_txs_pairs = BlockTXPairs,
					base_hash = element(1, hd(BI))
				}
			)
		end
	),
	PID ! apply_next_block,
	PID.

%% @doc Start the fork recovery server loop. Attempt to catch up to the
%% target block by applying each block between the current block and the
%% target block in turn.
server(#state {
		recovered_block_index = BI,
		recovered_block_txs_pairs = BlockTXPairs,
		target_hashes_to_go = [],
		parent_pid = ParentPID,
		base_hash = BaseH
	}) ->
	ParentPID ! {fork_recovered, BI, BlockTXPairs, BaseH};
server(S = #state { target_height = TargetHeight }) ->
	receive
		{parent_accepted_block, B} ->
			if B#block.height > TargetHeight ->
				ar:info(
					[
						{event, stopping_fork_recovery},
						{reason, parent_accepted_higher_block_than_target}
					]
				);
			true ->
				ar:info(
					[
						{event, continuing_fork_recovery},
						{reason, parent_accepted_lower_block_than_target}
					]
				),
				server(S)
			end
	after 0 ->
		do_fork_recover(S)
	end.

do_fork_recover(State) ->
	receive
		{update_target_hashes, NewRecoveryHashes, Peer} ->
			update_target_hashes(State, NewRecoveryHashes, Peer);
		apply_next_block ->
			apply_next_block(State);
		_ ->
			server(State)
	end.

update_target_hashes(State, NewRecoveryHashes, Peer) ->
	#state {
		peers = Peers,
		target_height = TargetHeight,
		target_hashes = TargetHashes,
		target_hashes_to_go = TargetHashesToGo
	} = State,
	NewTargetHashes = lists:reverse(NewRecoveryHashes),
	case lists:prefix(TargetHashes, NewTargetHashes) of
		true ->
			NewTargetHeight = TargetHeight + length(NewTargetHashes -- TargetHashes),
			NewTargetHashesToGo = TargetHashesToGo ++ NewTargetHashes -- TargetHashes,
			ar:info(
				[
					{event, updating_fork_recovery_target},
					{current_target_height, TargetHeight},
					{current_target_hashes, lists:map(fun ar_util:encode/1, TargetHashes)},
					{new_target_height, NewTargetHeight},
					{new_target_hashes, lists:map(fun ar_util:encode/1, NewTargetHashes)},
					{target_hashes_to_go, lists:map(fun ar_util:encode/1, NewTargetHashesToGo)}
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
				State#state {
					target_hashes = NewTargetHashes,
					peers = NewPeers,
					target_height = NewTargetHeight,
					target_hashes_to_go = NewTargetHashesToGo
				}
			);
		false ->
			ar:info(
				[
					{event, not_updating_fork_recovery_target},
					{reason, encountered_block_on_different_fork},
					{ignored_target_hashes, lists:map(fun ar_util:encode/1, NewTargetHashes)}
				]
			),
			server(State)
	end.

apply_next_block(State) ->
	#state {
		peers = Peers,
		target_height = TargetHeight,
		target_hashes_to_go = [NextH | _]
	} = State,
	NextB = ar_http_iface_client:get_block(Peers, NextH),
	ar:info(
		[
			{event, applying_fork_recovery},
			{block, ar_util:encode(NextH)}
		]
	),
	case ?IS_BLOCK(NextB) of
		false ->
			ar:warn(
				[
					{event, fork_recovery_failed},
					{reason, failed_to_fetch_block},
					{block, ar_util:encode(NextH)}
				]
			);
		true ->
			%% Ensure that block being applied is not the genesis block and
			%% is within the range of fork recovery.
			%%
			%% The range check is redundant but is made for the early detection
			%% and reporting of the invalid block.
			case
				{NextB#block.height, ((TargetHeight - NextB#block.height) > ?STORE_BLOCKS_BEHIND_CURRENT)}
			of
				%% Recovering to genesis block.
				{0, _} ->
					ar:err(
						[
							{event, fork_recovery_failed},
							{reason, recovery_block_is_genesis_block}
						]
					);
				%% The fetched block is too far ahead.
				{_, true} ->
					ar:err(
						[
							{event, fork_recovery_failed},
							{reason, recovery_block_is_too_far_ahead},
							{block_height, NextB#block.height}
						]
					);
				%% Target block is within the accepted range.
				{_X, _Y} ->
					apply_next_block(State, NextB)
			end
	end.

apply_next_block(State, NextB) ->
	#state {
		recovered_block_index = [{CurrentH, _, _} | _]
	} = State,
	B = ar_storage:read_block(CurrentH),
	case ?IS_BLOCK(B) of
		false ->
			ar:err(
				[
					{event, fork_recovery_failed},
					{reason, failed_to_read_current_block}
				]
			);
		true ->
			apply_next_block(State, NextB, B)
	end.

apply_next_block(State, NextB, B) ->
	#state {
		recovered_block_index = BI,
		recovered_block_txs_pairs = BlockTXPairs,
		parent_pid = ParentPID,
		target_hashes_to_go = [_ | NewTargetHashesToGo],
		target_hashes = TargetHashes,
		base_hash = BaseH
	} = State,
	TXs = NextB#block.txs,
	case
		validate(
			BI,
			NextB#block {
				txs = [TX#tx.id || TX <- TXs]
			},
			TXs,
			B,
			BlockTXPairs
		)
	of
		{error, invalid_block} ->
			ar:err(
				[
					{event, fork_recovery_failed},
					{reason, invalid_block},
					{block, ar_util:encode(NextB#block.indep_hash)},
					{previous_block, ar_util:encode(B#block.indep_hash)}
				]
			);
		{error, tx_replay} ->
			ar:err(
				[
					{event, fork_recovery_failed},
					{reason, tx_replay},
					{block, ar_util:encode(NextB#block.indep_hash)},
					{block_txs, lists:map(fun(TX) -> ar_util:encode(TX#tx.id) end, TXs)},
					{
						block_txs_pairs,
						lists:map(
							fun({BH, SizeTaggedTXs}) ->
								TXIDs = [TXID || {{TXID, _}, _} <- SizeTaggedTXs],
								{ar_util:encode(BH), lists:map(fun ar_util:encode/1, TXIDs)}
							end,
							BlockTXPairs
						)
					},
					{previous_block, ar_util:encode(B#block.indep_hash)}
				]
			);
		ok ->
			ar:info(
				[
					{event, applied_fork_recovery_block},
					{block, ar_util:encode(NextB#block.indep_hash)},
					{block_height, NextB#block.height}
				]
			),
			ar_storage:write_full_block(NextB),
			NewBI = ar_node_utils:update_block_index(NextB, BI),
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(NextB#block.txs),
			BH = NextB#block.indep_hash,
			NewBlockTXPairs =
				ar_node_utils:update_block_txs_pairs(BH, SizeTaggedTXs, BlockTXPairs),
			lists:foreach(
				fun(TX) ->
					ar_downloader:enqueue_random({tx_data, TX}),
					ar_tx_queue:drop_tx(TX)
				end,
				NextB#block.txs
			),
			case ar_meta_db:get(partial_fork_recovery) of
				true ->
					ar:info(
						[
							reported_partial_fork_recovery,
							{height, NextB#block.height}
						]
					),
					ParentPID ! {fork_recovered, NewBI, NewBlockTXPairs, BaseH};
				_ -> do_nothing
			end,
			self() ! apply_next_block,
			prometheus_histogram:observe(fork_recovery_depth, length(TargetHashes) - length(NewTargetHashesToGo)),
			server(
				State#state {
					recovered_block_index = NewBI,
					recovered_block_txs_pairs = NewBlockTXPairs,
					target_hashes_to_go = NewTargetHashesToGo
				}
			)
	end.

%% @doc Validate a new block (NextB) against the current block (B).
%% Returns ok | {error, invalid_block} | {error, tx_replay}.
validate(BI, NextB, TXs, B, BlockTXPairs) ->
	{FinderReward, _} =
		ar_node_utils:calculate_reward_pool(
			B#block.reward_pool,
			TXs,
			NextB#block.reward_addr,
			no_recall,
			NextB#block.weave_size,
			NextB#block.height,
			NextB#block.diff,
			NextB#block.timestamp
		),
	WalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(B#block.wallet_list, TXs, B#block.height),
			NextB#block.reward_addr,
			FinderReward,
			NextB#block.height
		),
	BlockValid = ar_node_utils:validate(
		BI,
		WalletList,
		NextB,
		TXs,
		B
	),
	case BlockValid of
		{invalid, _} ->
			{error, invalid_block};
		valid ->
			TXReplayCheck = ar_tx_replay_pool:verify_block_txs(
				TXs,
				NextB#block.diff,
				B#block.height,
				NextB#block.timestamp,
				B#block.wallet_list,
				BlockTXPairs
			),
			case TXReplayCheck of
				invalid ->
					{error, tx_replay};
				valid ->
					ok
			end
	end.

%%%
%%% Tests: ar_fork_recovery
%%%

%% @doc Ensure forks that are one block behind will resolve.
one_block_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	[B1 | _] = ar_weave:add([B0], []),
	ar_storage:write_block(B1),
	[B2 | _] = ar_weave:add([B1, B0], []),
	ar_storage:write_block(B2),
	[B3 | _] = ar_weave:add([B2, B1, B0], []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, [B3, B2, B1, B0]},
	timer:sleep(500),
	ar_node:mine(Node1),
	timer:sleep(500),
	ar_node:add_peers(Node1, Node2),
	ar_node:mine(Node1),
	timer:sleep(1000),
	?assertEqual(block_hashes_by_node(Node1), block_hashes_by_node(Node2)),
	?assertEqual(6, length(block_hashes_by_node(Node2))).

%% @doc Ensure forks that are three block behind will resolve.
three_block_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	[B1 | _] = ar_weave:add([B0], []),
	ar_storage:write_block(B1),
	[B2 | _] = ar_weave:add([B1, B0], []),
	ar_storage:write_block(B2),
	[B3 | _] = ar_weave:add([B2, B1, B0], []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, [B3, B2, B1, B0]},
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
	Bs = [ar_storage:read_block(BH) || BH <- BHs],
	[ar_util:encode(B#block.indep_hash) || B <- Bs].

%% @doc Ensure that nodes on a fork that is far behind will catchup correctly.
multiple_blocks_ahead_recovery_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start(),
	Node2 = ar_node:start(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	[B1 | _] = ar_weave:add([B0], []),
	ar_storage:write_block(B1),
	[B2 | _] = ar_weave:add([B1, B0], []),
	ar_storage:write_block(B2),
	[B3 | _] = ar_weave:add([B2, B1, B0], []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, [B3, B2, B1, B0]},
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
		SignedTX = ar_tx:sign_v1(TX, Priv1, Pub1),
		Node1 = ar_node:start(),
		Node2 = ar_node:start(),
		[B0] = ar_weave:init([]),
		ar_storage:write_block(B0),
		[B1 | _] = ar_weave:add([B0], []),
		ar_storage:write_block(B1),
		[B2 | _] = ar_weave:add([B1, B0], []),
		ar_storage:write_block(B2),
		[B3 | _] = ar_weave:add([B2, B1, B0], []),
		ar_storage:write_block(B3),
		Node1 ! Node2 ! {replace_block_list, [B3, B2, B1, B0]},
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
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	[B1 | _] = ar_weave:add([B0], []),
	ar_storage:write_block(B1),
	[B2 | _] = ar_weave:add([B1, B0], []),
	ar_storage:write_block(B2),
	[B3 | _] = ar_weave:add([B2, B1, B0], []),
	ar_storage:write_block(B3),
	Node1 ! Node2 ! {replace_block_list, [B3, B2, B1, B0]},
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
