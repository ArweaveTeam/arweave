%%% @doc The server responsible for processing blocks and transactions and
%%% maintaining the node state. Blocks are prioritized over transactions.
%%% The state is synchronized with the ar_node process for non-blocking reads.
-module(ar_node_worker).

-export([start_link/1]).

-export([init/1, handle_cast/2, handle_info/2, terminate/2, tx_mempool_size/1]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include_lib("eunit/include/eunit.hrl").

-ifdef(DEBUG).
-define(PROCESS_TASK_QUEUE_FREQUENCY_MS, 10).
-else.
-define(PROCESS_TASK_QUEUE_FREQUENCY_MS, 200).
-endif.

-define(FILTER_MEMPOOL_CHUNK_SIZE, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(State) ->
	process_flag(trap_exit, true),
	gen_server:cast(self(), process_task_queue),
	{ok, State#{ task_queue => gb_sets:new() }}.

handle_cast(process_task_queue, #{ task_queue := TaskQueue } = State) ->
	case gb_sets:is_empty(TaskQueue) of
		true ->
			timer:apply_after(
				?PROCESS_TASK_QUEUE_FREQUENCY_MS,
				gen_server,
				cast,
				[self(), process_task_queue]
			),
			{noreply, State};
		false ->
			record_metrics(State),
			{{_Priority, Task}, TaskQueue2} = gb_sets:take_smallest(TaskQueue),
			gen_server:cast(self(), process_task_queue),
			handle_task(Task, State#{ task_queue => TaskQueue2 })
	end;

handle_cast(Message, #{ task_queue := TaskQueue } = State) ->
	Task = {priority(Message), Message},
	case gb_sets:is_element(Task, TaskQueue) of
		true ->
			{noreply, State};
		false ->
			{noreply, State#{ task_queue => gb_sets:insert(Task, TaskQueue) }}
	end.

handle_info({'DOWN', _Ref, process, PID, _Info}, State) ->
	#{
		blocks_missing_txs := Set,
		missing_txs_lookup_processes := Map
	} = State,
	BH = maps:get(PID, Map),
	{noreply, State#{
		missing_txs_lookup_processes => maps:remove(PID, Map),
		blocks_missing_txs => sets:del_element(BH, Set)
	}};

handle_info(_Message, State) ->
	{noreply, State}.

terminate(Reason, #{ miner := Miner }) ->
	case Miner of
		undefined -> do_nothing;
		PID -> ar_mine:stop(PID)
	end,
	ar:info([
		{event, ar_node_worker_terminated},
		{reason, Reason}
	]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

record_metrics(State) ->
	#{
		height := Height,
		mempool_size := MempoolSize,
		weave_size := WeaveSize
	} = State,
	prometheus_gauge:set(arweave_block_height, Height),
	record_mempool_size_metrics(MempoolSize),
	prometheus_gauge:set(weave_size, WeaveSize).

record_mempool_size_metrics({HeaderSize, DataSize}) ->
	prometheus_gauge:set(mempool_header_size_bytes, HeaderSize),
	prometheus_gauge:set(mempool_data_size_bytes, DataSize).

handle_task({join, BI, Blocks}, #{ node := Node } = State) ->
	Current = element(1, hd(BI)),
	B = hd(Blocks),
	UpdatedState = reset_miner(
		State#{
			block_index          => BI,
			current              => Current,
			wallet_list          => B#block.wallet_list,
			height               => B#block.height,
			reward_pool          => B#block.reward_pool,
			diff                 => B#block.diff,
			cumulative_diff      => B#block.cumulative_diff,
			last_retarget        => B#block.last_retarget,
			weave_size           => B#block.weave_size,
			block_txs_pairs      => [block_txs_pair(Block) || Block <- Blocks],
			block_cache          => ar_block_cache:from_list(Blocks)
		}
	),
	Node ! {sync_state, UpdatedState},
	{noreply, UpdatedState};

handle_task({gossip_message, Msg}, #{ gossip := GS } = State) ->
	{GS2, Message} = ar_gossip:recv(GS, Msg),
	handle_gossip({GS2, Message}, State#{ gossip => GS2 });

handle_task({add_tx, TX}, State) ->
	handle_add_tx(State, TX, maps:get(gossip, State));

handle_task({move_tx_to_mining_pool, TX}, State) ->
	handle_move_tx_to_mining_pool(State, TX, maps:get(gossip, State));

handle_task({process_new_block, Peer, Height, BShadow, BDS, ReceiveTimestamp}, State) ->
	%% We have a new block. Distribute it to the gossip network. This is only
	%% triggered in the polling mode.
	GS = maps:get(gossip, State),
	ar_gossip:send(GS, {new_block, Peer, Height, BShadow, BDS, ReceiveTimestamp}),
	{noreply, State};

handle_task(apply_block, State) ->
	apply_block(State);

handle_task({cache_missing_txs, BH, TXs}, #{ block_cache := BlockCache } = State) ->
	case ar_block_cache:get_block_and_status(BlockCache, BH) of
		not_found ->
			%% The block should have been pruned while we were fetching the missing txs.
			{noreply, State};
		{B, not_validated} ->
			UpdatedBlockCache = ar_block_cache:add(BlockCache, B#block{ txs = TXs }),
			gen_server:cast(self(), apply_block),
			{noreply, State#{ block_cache => UpdatedBlockCache }};
		{_B, _AnotherStatus} ->
			%% The transactions should have been received and the block validated while
			%% we were looking for previously missing transactions.
			{noreply, State}
	end;

handle_task({work_complete, BaseBH, NewB, MinedTXs, BDS, POA}, State) ->
	#{ block_index := [{CurrentBH, _, _} | _]} = State,
	case BaseBH of
		CurrentBH ->
			handle_block_from_miner(State, NewB, MinedTXs, BDS, POA);
		_ ->
			ar:info([{event, ignore_mined_block}, {reason, accepted_foreign_block}]),
			{noreply, State}
	end;

handle_task(mine, State) ->
	{noreply, start_mining(State)};

handle_task(automine, State) ->
	{noreply, start_mining(State#{ automine => true })};

handle_task({set_reward_addr, Addr}, #{ node := Node } = State) ->
	Node ! {sync_reward_addr, Addr},
	{noreply, State#{ reward_addr => Addr }};

handle_task({set_trusted_peers, Peers}, #{ node := Node } = State) ->
	Node ! {sync_trusted_peers, Peers},
	{noreply, State#{ trusted_peers => Peers }};

handle_task({add_peers, Peers}, #{ gossip := GS } = State) ->
	NewGS = ar_gossip:add_peers(GS, Peers),
	{noreply, State#{ gossip => NewGS }};

handle_task({set_loss_probability, Prob}, #{ gossip := GS } = State) ->
	{noreply, State#{ gossip => ar_gossip:set_loss_probability(GS, Prob) }};

handle_task({filter_mempool, Iterator}, State) ->
	#{
		node := Node,
		txs := TXs,
		diff := Diff,
		height := Height,
		wallet_list := WalletList,
		block_txs_pairs := BlockTXPairs,
		mempool_size := MempoolSize
	} = State,
	{ok, List, NextIterator} = take_mempool_chunk(Iterator, ?FILTER_MEMPOOL_CHUNK_SIZE),
	case List of
		[] ->
			{noreply, State};
		_ ->
			Wallets = ar_wallets:get(WalletList, ar_tx:get_addresses(List)),
			InvalidTXs =
				lists:foldl(
					fun(TX, Acc) ->
						case ar_tx_replay_pool:verify_tx(
							TX,
							Diff,
							Height,
							BlockTXPairs,
							#{},
							Wallets
						) of
							valid ->
								Acc;
							{invalid, _Reason} ->
								case TX#tx.format == 2 of
									true ->
										ar_data_sync:maybe_drop_data_root_from_disk_pool(
											TX#tx.data_root,
											TX#tx.data_size,
											TX#tx.id
										);
								false ->
									nothing_to_drop_from_disk_pool
								end,
								[TX | Acc]
						end
					end,
					[],
					List
				),
			{TXs2, DroppedTXMap, DecreasedMempoolSize} = drop_txs(InvalidTXs, TXs, MempoolSize),
			NewState = State#{ txs => TXs2, mempool_size => DecreasedMempoolSize },
			case NextIterator of
				none ->
					scan_complete;
				_ ->
					gen_server:cast(self(), {filter_mempool, NextIterator})
			end,
			Node ! {sync_dropped_mempool_txs, DroppedTXMap},
			{noreply, NewState}
	end;

handle_task(Msg, State) ->
	ar:err([
		{event, ar_node_worker_received_unknown_message},
		{message, Msg}
	]),
	{noreply, State}.

%% @doc Handle the gossip receive results.
handle_gossip({_NewGS, {new_block, _Peer, _Height, BShadow, _BDS, _Timestamp}}, State) ->
	handle_new_block(State, BShadow);

handle_gossip({NewGS, {add_tx, TX}}, State) ->
	handle_add_tx(State, TX, NewGS);

handle_gossip({NewGS, {add_waiting_tx, TX}}, State) ->
	handle_add_waiting_tx(State, TX, NewGS);

handle_gossip({NewGS, {move_tx_to_mining_pool, TX}}, State) ->
	handle_move_tx_to_mining_pool(State, TX, NewGS);

handle_gossip({NewGS, {drop_waiting_txs, TXs}}, State) ->
	handle_drop_waiting_txs(State, TXs, NewGS);

handle_gossip({_NewGS, ignore}, State) ->
	{noreply, State};

handle_gossip({_NewGS, UnknownMessage}, State) ->
	ar:info([
		{event, ar_node_worker_received_unknown_gossip_message},
		{message, UnknownMessage}
	]),
	{noreply, State}.

%% @doc Add the new transaction to the server state.
handle_add_tx(State, TX, GS) ->
	#{ node := Node, txs := TXs, mempool_size := MS} = State,
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	IncreasedMempoolSize = increase_mempool_size(MS, TX),
	Node ! {sync_mempool_tx, TX#tx.id, {TX, ready_for_mining}},
	{noreply, State#{
		txs => maps:put(TX#tx.id, {TX, ready_for_mining}, TXs),
		gossip => NewGS,
		mempool_size => IncreasedMempoolSize
	}}.

increase_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize + HeaderSize, MempoolDataSize + DataSize}.

tx_mempool_size(#tx{ format = 1, data = Data }) ->
	{?TX_SIZE_BASE + byte_size(Data), 0};
tx_mempool_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

%% @doc Add the new waiting transaction to the server state.
handle_add_waiting_tx(State, #tx{ id = TXID } = TX, GS) ->
	#{ node := Node, txs := WaitingTXs, mempool_size := MS } = State,
	{NewGS, _} = ar_gossip:send(GS, {add_waiting_tx, TX}),
	case maps:is_key(TXID, WaitingTXs) of
		false ->
			IncreasedMempoolSize = increase_mempool_size(MS, TX),
			Node ! {sync_mempool_tx, TXID, {TX, waiting}},
			{noreply, State#{
				txs => maps:put(TXID, {TX, waiting}, WaitingTXs),
				gossip => NewGS,
				mempool_size => IncreasedMempoolSize
			}};
		true ->
			{noreply, State}
	end.

%% @doc Add the transaction to the mining pool, to be included in the mined block.
handle_move_tx_to_mining_pool(State, #tx{ id = TXID } = TX, GS) ->
	#{ node := Node, txs := TXs, mempool_size := MempoolSize } = State,
	{NewGS, _} = ar_gossip:send(GS, {move_tx_to_mining_pool, TX}),
	Node ! {sync_mempool_tx, TX#tx.id, {TX, ready_for_mining}},
	case maps:get(TXID, TXs, not_found) of
		not_found ->
			{noreply, State#{
				txs => maps:put(TX#tx.id, {TX, ready_for_mining}, TXs),
				mempool_size => increase_mempool_size(MempoolSize, TX),
				gossip => NewGS
			}};
		{ExistingTX, _Status} ->
			{noreply, State#{
				txs => maps:put(TXID, {ExistingTX, ready_for_mining}, TXs),
				gossip => NewGS
			}}
	end.

handle_drop_waiting_txs(State, DroppedTXs, GS) ->
	#{ node := Node, txs := TXs, mempool_size := MempoolSize } = State,
	{NewGS, _} = ar_gossip:send(GS, {drop_waiting_txs, DroppedTXs}),
	{UpdatedTXs, DroppedTXMap, DecreasedMempoolSize} = drop_txs(DroppedTXs, TXs, MempoolSize),
	Node ! {sync_dropped_mempool_txs, DroppedTXMap},
	{noreply, State#{
		txs => UpdatedTXs,
		gossip => NewGS,
		mempool_size => DecreasedMempoolSize
	}}.

drop_txs(DroppedTXs, TXs, MempoolSize) ->
	{UpdatedTXs, DroppedTXMap} = lists:foldl(
		fun(TX, {Acc, DroppedAcc}) ->
			case maps:take(TX#tx.id, Acc) of
				{Value, Map} ->
					{Map, maps:put(TX#tx.id, Value, DroppedAcc)};
				error ->
					{Acc, DroppedAcc}
			end
		end,
		{TXs, maps:new()},
		DroppedTXs
	),
	{DroppedHeaderSize, DroppedDataSize} = calculate_mempool_size(DroppedTXMap),
	{MempoolHeaderSize, MempoolDataSize} = MempoolSize,
	DecreasedMempoolSize =
		{MempoolHeaderSize - DroppedHeaderSize, MempoolDataSize - DroppedDataSize},
	{UpdatedTXs, DroppedTXMap, DecreasedMempoolSize}.

take_mempool_chunk(Iterator, Size) ->
	take_mempool_chunk(Iterator, Size, []).

take_mempool_chunk(Iterator, 0, Taken) ->
	{ok, Taken, Iterator};
take_mempool_chunk(Iterator, Size, Taken) ->
	case maps:next(Iterator) of
		none ->
			{ok, Taken, none};
		{_, {TX, _}, NextIterator} ->
			take_mempool_chunk(NextIterator, Size - 1, [TX | Taken])
	end.

%% @doc Record the block in the block cache. Schedule an application of the
%% earliest not validated block from the longest chain, if any.
handle_new_block(#{ block_index := not_joined } = State, BShadow) ->
	ar:info([
		{event, ar_node_worker_ignored_block},
		{reason, not_joined},
		{indep_hash, ar_util:encode(BShadow#block.indep_hash)}
	]),
	{noreply, State};
handle_new_block(State, #block{ indep_hash = H, txs = TXs })
		when length(TXs) > ?BLOCK_TX_COUNT_LIMIT ->
	ar:warn([
		{event, received_block_with_too_many_txs},
		{block, ar_util:encode(H)},
		{txs, length(TXs)}
	]),
	{noreply, State};
handle_new_block(State, BShadow) ->
	#{ node := Node, block_cache := BlockCache } = State,
	case ar_block_cache:get(BlockCache, BShadow#block.indep_hash) of
		not_found ->
			case ar_block_cache:get(BlockCache, BShadow#block.previous_block) of
				not_found ->
					%% The cache should have been just pruned and this block is old.
					{noreply, State};
				_ ->
					UpdatedBlockCache = ar_block_cache:add(BlockCache, BShadow),
					gen_server:cast(self(), apply_block),
					Node ! {sync_block_cache, UpdatedBlockCache},
					{noreply, State#{ block_cache => UpdatedBlockCache }}
			end;
		_ ->
			%% The block's already received from a different peer or
			%% fetched by ar_poller.
			{noreply, State}
	end.

apply_block(#{ block_cache := BlockCache, blocks_missing_txs := BlocksMissingTXs } = State) ->
	case ar_block_cache:get_earliest_not_validated_from_longest_chain(BlockCache) of
		not_found ->
			%% Nothing to do - we are at the longest known chain already.
			{noreply, State};
		{B, PrevBlocks} ->
			case sets:is_element(B#block.indep_hash, BlocksMissingTXs) of
				true ->
					%% We do not have some of the transactions from this block,
					%% searching for them at the moment.
					{noreply, State};
				false ->
					apply_block(State, B, PrevBlocks)
			end
	end.

apply_block(State, BShadow, [PrevB | _] = PrevBlocks) ->
	#{
		node := Node,
		txs := Mempool,
		block_cache := BlockCache,
		block_index := BI,
		block_txs_pairs := BlockTXPairs,
		blocks_missing_txs := BlocksMissingTXs,
		missing_txs_lookup_processes := MissingTXsLookupProcesses
	} = State,
	Timestamp = erlang:timestamp(),
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs, Mempool),
	case MissingTXIDs of
		[] ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
			B = BShadow#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs },
			PrevWalletList = PrevB#block.wallet_list,
			PrevRewardPool = PrevB#block.reward_pool,
			PrevHeight = PrevB#block.height,
			case validate_wallet_list(B, PrevWalletList, PrevRewardPool, PrevHeight) of
				error ->
					BH = B#block.indep_hash,
					UpdatedBlockCache = ar_block_cache:remove(BlockCache, BH),
					Node ! {sync_block_cache, UpdatedBlockCache},
					{noreply, State#{ block_cache => UpdatedBlockCache }};
				{ok, RootHash} ->
					B2 = B#block{ wallet_list = RootHash },
					Wallets =
						ar_wallets:get(
							PrevWalletList,
							[B#block.reward_addr | ar_tx:get_addresses(B#block.txs)]
						),
					BI2 = update_block_index(B, PrevBlocks, BI),
					BlockTXPairs2 = update_block_txs_pairs(B, PrevBlocks, BlockTXPairs),
					case ar_node_utils:validate(tl(BI2), B2, PrevB, Wallets, tl(BlockTXPairs2)) of
						{invalid, Reason} ->
							ar:warn([
								{event, received_invalid_block},
								{validation_error, Reason}
							]),
							BH = B#block.indep_hash,
							UpdatedBlockCache = ar_block_cache:remove(BlockCache, BH),
							Node ! {sync_block_cache, UpdatedBlockCache},
							{noreply, State#{ block_cache => UpdatedBlockCache }};
						valid ->
							UpdatedState =
								apply_validated_block(State, B2, PrevBlocks, BI2, BlockTXPairs2),
							ar_miner_log:foreign_block(B#block.indep_hash),
							record_processing_time(Timestamp),
							{noreply, UpdatedState}
				end
			end;
		_ ->
			Self = self(),
			monitor(
				process,
				PID = spawn(fun() -> get_missing_txs_and_retry(BShadow, Mempool, Self) end)
			),
			BH = BShadow#block.indep_hash,
			{noreply, State#{
				blocks_missing_txs => sets:add_element(BH, BlocksMissingTXs),
				missing_txs_lookup_processes => maps:put(PID, BH, MissingTXsLookupProcesses)
			}}
	end.

pick_txs(TXIDs, TXs) ->
	lists:foldr(
		fun (TX, {Found, Missing}) when is_record(TX, tx) ->
				{[TX | Found], Missing};
			(TXID, {Found, Missing}) ->
				case maps:get(TXID, TXs, tx_not_in_mempool) of
					tx_not_in_mempool ->
						%% This disk read should almost never be useful. Presumably,
						%% the only reason to find some of these transactions on disk
						%% is they had been written prior to the call, what means they are
						%% from an orphaned fork, more than one block behind.
						case ar_storage:read_tx(TXID) of
							unavailable ->
								{Found, [TXID | Missing]};
							TX ->
								{[TX | Found], Missing}
						end;
					{TX, _} ->
						{[TX | Found], Missing}
				end
		end,
		{[], []},
		TXIDs
	).

update_block_index(B, [PrevB, PrevPrevB | PrevBlocks], BI) ->
	[block_index_entry(B) | update_block_index(PrevB, [PrevPrevB | PrevBlocks], BI)];
update_block_index(B, [#block{ indep_hash = H }], BI) ->
	[block_index_entry(B) | lists:dropwhile(fun({Hash, _, _}) -> Hash /= H end, BI)].

block_index_entry(B) ->
	{B#block.indep_hash, B#block.weave_size, B#block.tx_root}.

update_block_txs_pairs(B, PrevBlocks, BlockTXPairs) ->
	lists:sublist(update_block_txs_pairs2(B, PrevBlocks, BlockTXPairs), 2 * ?MAX_TX_ANCHOR_DEPTH).

update_block_txs_pairs2(B, [PrevB, PrevPrevB | PrevBlocks], BP) ->
	[block_txs_pair(B) | update_block_txs_pairs2(PrevB, [PrevPrevB | PrevBlocks], BP)];
update_block_txs_pairs2(B, [#block{ indep_hash = H }], BP) ->
	[block_txs_pair(B) | lists:dropwhile(fun({Hash, _}) -> Hash /= H end, BP)].

block_txs_pair(B) ->
	{B#block.indep_hash, B#block.size_tagged_txs}.

validate_wallet_list(B, WalletList, RewardPool, Height) ->
	case ar_wallets:apply_block(B, WalletList, RewardPool, Height) of
		{error, invalid_reward_pool} ->
			ar:warn([
				{event, received_invalid_block},
				{validation_error, invalid_reward_pool}
			]),
			error;
		{error, invalid_wallet_list} ->
			ar:warn([
				{event, received_invalid_block},
				{validation_error, invalid_wallet_list}
			]),
			error;
		{ok, RootHash} ->
			{ok, RootHash}
	end.

get_missing_txs_and_retry(BShadow, Mempool, Worker) ->
	case whereis(http_bridge_node) of
		undefined ->
			ok;
		BridgePID ->
			Peers = ar_bridge:get_remote_peers(BridgePID),
			case ar_http_iface_client:get_txs(Peers, Mempool, BShadow) of
				{ok, TXs} ->
					gen_server:cast(Worker, {cache_missing_txs, BShadow#block.indep_hash, TXs});
				_ ->
					ar:warn([
						{event, ar_node_worker_could_not_find_block_txs},
						{block, ar_util:encode(BShadow#block.indep_hash)}
					])
			end
	end.

apply_validated_block(#{ cumulative_diff := CDiff } = State, B, _Blocks, _BI, _BlockTXs)
		when B#block.cumulative_diff =< CDiff ->
	%% The block is from the longest fork, but not the latest known block from there.
	#{
		node := Node,
		block_cache := BlockCache
	} = State,
	UpdatedBlockCache = ar_block_cache:add_validated(BlockCache, B),
	gen_server:cast(self(), apply_block),
	log_applied_block(B),
	Node ! {sync_block_cache, UpdatedBlockCache},
	State#{ block_cache => UpdatedBlockCache };
apply_validated_block(State, B, PrevBlocks, BI2, BlockTXPairs2) ->
	#{
		node := Node,
		block_cache := BlockCache,
		txs := TXs,
		mempool_size := MempoolSize
	} = State,
	PruneDepth = ?STORE_BLOCKS_BEHIND_CURRENT,
	BH = B#block.indep_hash,
	UpdatedBlockCache =
		ar_block_cache:prune(
			ar_block_cache:mark_tip(
				%% Overwrite the block to store computed size tagged txs - they
				%% may be needed for reconstructing block_txs_pairs if there is a reorg
				%% off and then back on this fork.
				ar_block_cache:add(BlockCache, B),
				BH
			),
			PruneDepth
		),
	%% We could have missed a few blocks due to networking issues, which would then
	%% be picked by ar_poller and end up waiting for missing transactions to be fetched.
	%% Thefore, it is possible (although not likely) that there are blocks above the new tip,
	%% for which we trigger a block application here, in order not to wait for the next
	%% arrived or fetched block to trigger it.
	gen_server:cast(self(), apply_block),
	log_applied_block(B),
	log_tip(B),
	maybe_report_n_confirmations(B, BI2),
	maybe_store_block_index(B, BI2),
	record_fork_depth(length(PrevBlocks) - 1),
	lists:foldl(
		fun (CurrentB, start) ->
				CurrentB;
			(CurrentB, CurrentPrevB) ->
				PrevWallets = CurrentPrevB#block.wallet_list,
				Wallets = CurrentB#block.wallet_list,
				Addr = CurrentB#block.reward_addr,
				Height = CurrentB#block.height,
				%% Use a twice bigger depth than the depth requested on join to serve
				%% the wallet trees to the joining nodes.
				ok = ar_wallets:set_current(PrevWallets, Wallets, Addr, Height, PruneDepth * 2),
				CurrentB
		end,
		start,
		lists:reverse([B | PrevBlocks])
	),
	RecentBI = lists:sublist(BI2, ?STORE_BLOCKS_BEHIND_CURRENT * 2),
	ar_data_sync:add_tip_block(BlockTXPairs2, RecentBI),
	ar_header_sync:add_tip_block(B, RecentBI),
	lists:foreach(
		fun(PrevB) ->
			ar_header_sync:add_block(PrevB)
		end,
		tl(lists:reverse(PrevBlocks))
	),
	BlockTXs = B#block.txs,
	{TXs2, _DroppedTXMap, DecreasedMempoolSize} = drop_txs(BlockTXs, TXs, MempoolSize),
	gen_server:cast(self(), {filter_mempool, maps:iterator(TXs2)}),
	lists:foreach(fun(TX) -> ar_tx_queue:drop_tx(TX) end, BlockTXs),
	UpdatedState = reset_miner(State#{
		block_index      => BI2,
		current          => B#block.indep_hash,
		txs              => TXs2,
		mempool_size     => DecreasedMempoolSize,
		height           => B#block.height,
		reward_pool      => B#block.reward_pool,
		diff             => B#block.diff,
		cumulative_diff  => B#block.cumulative_diff,
		last_retarget    => B#block.last_retarget,
		weave_size       => B#block.weave_size,
		block_txs_pairs  => BlockTXPairs2,
		wallet_list      => B#block.wallet_list,
		block_cache      => UpdatedBlockCache
	}),
	Node ! {sync_state, UpdatedState},
	UpdatedState.

log_applied_block(B) ->
	ar:info([
		{event, applied_block},
		{indep_hash, ar_util:encode(B#block.indep_hash)},
		{height, B#block.height}
	]).

log_tip(B) ->
	ar:info([
		{event, new_tip_block},
		{indep_hash, ar_util:encode(B#block.indep_hash)},
		{height, B#block.height}
	]).

maybe_report_n_confirmations(B, BI) ->
	N = 10,
	LastNBlocks = lists:sublist(BI, N),
	case length(LastNBlocks) == N of
		true ->
			{H, _, _} = lists:last(LastNBlocks),
			ar_miner_log:block_received_n_confirmations(H, B#block.height - N);
		false ->
			do_nothing
	end.

maybe_store_block_index(B, BI) ->
	case B#block.height rem ?STORE_BLOCKS_BEHIND_CURRENT of
		0 ->
			spawn(fun() -> ar_storage:write_block_index(BI) end);
		_ ->
			ok
	end.

record_fork_depth(0) ->
	ok;
record_fork_depth(Depth) ->
	prometheus_histogram:observe(fork_recovery_depth, Depth).

%% @doc Kill the old miner, optionally start a new miner, depending on the automine setting.
reset_miner(#{ miner := undefined, automine := false } = StateIn) ->
	StateIn;
reset_miner(#{ miner := undefined, automine := true } = StateIn) ->
	start_mining(StateIn);
reset_miner(#{ miner := Pid, automine := false } = StateIn) ->
	ar_mine:stop(Pid),
	StateIn#{ miner => undefined };
reset_miner(#{ miner := Pid, automine := true } = StateIn) ->
	ar_mine:stop(Pid),
	start_mining(StateIn#{ miner => undefined }).

%% @doc Force a node to start mining, update state.
start_mining(#{ block_index := not_joined } = StateIn) ->
	%% We don't have a block index. Wait until we have one before
	%% starting to mine.
	StateIn;
start_mining(StateIn) ->
	#{
		node := Node,
		block_index := BI,
		txs := TXs,
		reward_addr := RewardAddr,
		tags := Tags,
		block_txs_pairs := BlockTXPairs,
		current := Current,
		block_cache := BlockCache,
		height := Height
	} = StateIn,
	POA =
		case Height + 1 >= ar_fork:height_2_3() of
			true ->
				not_set;
			false ->
				ar_poa:generate(BI)
		end,
	case POA of
		unavailable ->
			ar:info(
				[
					{event, could_not_start_mining},
					{reason, data_unavailable_to_generate_poa},
					{generated_options_to_depth, ar_meta_db:get(max_poa_option_depth)}
				]
			),
			StateIn;
		_ ->
			ar_miner_log:started_hashing(),
			B = ar_block_cache:get(BlockCache, Current),
			Miner = ar_mine:start(
				B,
				POA,
				maps:fold(
					fun
						(_, {TX, ready_for_mining}, Acc) ->
							[TX | Acc];
						(_, _, Acc) ->
							Acc
					end,
					[],
					TXs
				),
				RewardAddr,
				Tags,
				Node,
				BlockTXPairs,
				BI
			),
			ar:info([{event, started_mining}]),
			StateIn#{ miner => Miner }
	end.

record_processing_time(StartTimestamp) ->
	ProcessingTime = timer:now_diff(erlang:timestamp(), StartTimestamp) / 1000000,
	prometheus_histogram:observe(block_processing_time, ProcessingTime).

calculate_mempool_size(TXs) ->
	maps:fold(
		fun(_TXID, {TX, _}, {HeaderAcc, DataAcc}) ->
			{HeaderSize, DataSize} = tx_mempool_size(TX),
			{HeaderSize + HeaderAcc, DataSize + DataAcc}
		end,
		{0, 0},
		TXs
	).

%% @doc Integrate the block found by us.
handle_block_from_miner(#{ block_index := not_joined } = State, _B, _TXs, _BDS, _POA) ->
	{noreply, State};
handle_block_from_miner(State, BShadow, MinedTXs, BDS, _POA) ->
	#{
		gossip := GS,
		block_index := BI,
		block_txs_pairs := BlockTXPairs,
		block_cache := BlockCache,
		current := Current
	} = State,
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(MinedTXs),
	B = BShadow#block{ txs = MinedTXs, size_tagged_txs = SizeTaggedTXs },
	ar_miner_log:mined_block(B#block.indep_hash, B#block.height),
	ar:info([
		{event, mined_block},
		{indep_hash, ar_util:encode(B#block.indep_hash)},
		{txs, length(MinedTXs)}
	]),
	GossipMessage = {new_block, self(), B#block.height, B, BDS, erlang:timestamp()},
	{NewGS, _} = ar_gossip:send(GS, GossipMessage),
	PrevBlocks = [ar_block_cache:get(BlockCache, Current)],
	BI2 = [block_index_entry(B) | BI],
	BlockTXPairs2 = [block_txs_pair(B) | BlockTXPairs],
	State2 = State#{ block_cache => ar_block_cache:add(BlockCache, B) },
	State3 = apply_validated_block(State2, B, PrevBlocks, BI2, BlockTXPairs2),
	{noreply, State3#{ gossip => NewGS }}.

%% @doc Assign a priority to the task. 0 corresponds to the highest priority.
priority(apply_block) ->
	0;
priority({gossip_message, #gs_msg{ data = {new_block, _, _, _, _, _} }}) ->
	1;
priority({work_complete, _, _, _, _, _}) ->
	2;
priority({cache_missing_txs, _, _}) ->
	3;
priority(_) ->
	os:system_time(second).
