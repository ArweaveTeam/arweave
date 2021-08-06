%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%

%%% @doc The server responsible for processing blocks and transactions and
%%% maintaining the node state. Blocks are prioritized over transactions.
%%% @end
-module(ar_node_worker).

-export([start_link/0]).

-export([init/1, handle_cast/2, handle_info/2, terminate/2, tx_mempool_size/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
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

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	%% Initialize RandomX.
	ar_randomx_state:start(),
	ar_randomx_state:start_block_polling(),
	%% Read persisted mempool.
	load_mempool(),
	%% Join the network.
	{ok, Config} = application:get_env(arweave, config),
	BI =
		case {Config#config.start_from_block_index, Config#config.init} of
			{false, false} ->
				not_joined;
			{true, _} ->
				case ar_storage:read_block_index() of
					{error, enoent} ->
						io:format(
							"~n~n\tBlock index file is not found. "
							"If you want to start from a block index copied "
							"from another node, place it in "
							"<data_dir>/hash_lists/last_block_index.json~n~n"
						),
						erlang:halt();
					BI2 ->
						BI2
				end;
			{false, true} ->
				Config2 = Config#config{ init = false },
				application:set_env(arweave, config, Config2),
				ar_weave:init(
					ar_util:genesis_wallets(),
					ar_retarget:switch_to_linear_diff(Config#config.diff),
					0,
					ar_storage:read_tx(ar_weave:read_v1_genesis_txs())
				)
		end,
	case {BI, Config#config.auto_join} of
		{not_joined, true} ->
			ar_join:start(Config#config.peers);
		{BI, true} ->
			start_from_block_index(BI);
		{_, false} ->
			do_nothing
	end,
	Gossip = ar_gossip:init([
		whereis(ar_bridge),
		%% Attach webhook listeners to the internal gossip network.
		ar_webhook:start(Config#config.webhooks)
	]),
	ar_bridge:add_local_peer(self()),
	%% Add pending transactions from the persisted mempool to the propagation queue.
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	maps:map(
		fun (_TXID, ready_for_mining) ->
				ok;
			(TXID, waiting) ->
				[{_, TX}] = ets:lookup(node_state, {tx, TXID}),
				ar_bridge:add_tx(TX)
		end,
		Map
	),
	%% May be start mining.
	IOThreads =
		case Config#config.mine of
			true ->
				gen_server:cast(self(), automine),
				start_io_threads();
			_ ->
				[]
		end,
	gen_server:cast(self(), process_task_queue),
	ets:insert(node_state, [
		{is_joined,						false},
		{hash_list_2_0_for_1_0_blocks,	read_hash_list_2_0_for_1_0_blocks()}
	]),
	%% Start the HTTP server.
	ok = ar_http_iface_server:start(),
	{ok, #{
		miner => undefined,
		automine => false,
		tags => [],
		gossip => Gossip,
		reward_addr => Config#config.mining_addr,
		blocks_missing_txs => sets:new(),
		missing_txs_lookup_processes => #{},
		task_queue => gb_sets:new(),
		io_threads => IOThreads
	}}.

load_mempool() ->
	case ar_storage:read_term(mempool) of
		{ok, {TXs, MempoolSize}} ->
			Map =
				maps:map(
					fun(TXID, {TX, Status}) ->
						ets:insert(node_state, {{tx, TXID}, TX}),
						Status
					end,
					TXs
				),
			ets:insert(node_state, [
				{mempool_size, MempoolSize},
				{tx_statuses, Map}
			]);
		not_found ->
			ets:insert(node_state, [
				{mempool_size, {0, 0}},
				{tx_statuses, #{}}
			]);
		{error, Error} ->
			?LOG_ERROR([{event, failed_to_load_mempool}, {reason, Error}]),
			ets:insert(node_state, [
				{mempool_size, {0, 0}},
				{tx_statuses, #{}}
			])
	end.

start_io_threads() ->
	%% Start the IO mining processes. The mining server and the hashing
	%% processes are historically restarted every round, but the IO
	%% processes keep the database files open for better performance so
	%% we do not want to restart them.
	{ok, Config} = application:get_env(arweave, config),
	ets:insert(mining_state, {session, {make_ref(), os:system_time(second), not_set}}),
	SearchInRocksDB = lists:member(search_in_rocksdb_when_mining, Config#config.enable),
	[spawn_link(
		fun() ->
			process_flag(trap_exit, true),
			process_flag(message_queue_data, off_heap),
			ar_chunk_storage:open_files(),
			ar_mine:io_thread(SearchInRocksDB)
		end)
		|| _ <- lists:seq(1, Config#config.io_threads)].

handle_cast(process_task_queue, #{ task_queue := TaskQueue } = State) ->
	RunTask =
		case gb_sets:is_empty(TaskQueue) of
			true ->
				false;
			false ->
				case ets:lookup(node_state, is_joined) of
					[{_, true}] ->
						true;
					_ ->
						false
				end
		end,
	case RunTask of
		true ->
			record_metrics(),
			{{_Priority, Task}, TaskQueue2} = gb_sets:take_smallest(TaskQueue),
			gen_server:cast(self(), process_task_queue),
			handle_task(Task, State#{ task_queue => TaskQueue2 });
		false ->
			timer:apply_after(
				?PROCESS_TASK_QUEUE_FREQUENCY_MS,
				gen_server,
				cast,
				[self(), process_task_queue]
			),
			{noreply, State}
	end;

handle_cast(Message, #{ task_queue := TaskQueue } = State) ->
	Task = {priority(Message), Message},
	case gb_sets:is_element(Task, TaskQueue) of
		true ->
			{noreply, State};
		false ->
			{noreply, State#{ task_queue => gb_sets:insert(Task, TaskQueue) }}
	end.

handle_info(Info, State) when is_record(Info, gs_msg) ->
	gen_server:cast(?MODULE, {gossip_message, Info}),
	{noreply, State};

handle_info({join, BI, Blocks}, State) ->
	{ok, Config} = application:get_env(arweave, config),
	{ok, _} = ar_wallets:start_link([{blocks, Blocks}, {peers, Config#config.peers}]),
	ets:insert(node_state, [
		{block_index,	BI},
		{joined_blocks,	Blocks}
	]),
	{noreply, State};

handle_info(wallets_ready, State) ->
	[{block_index, BI}] = ets:lookup(node_state, block_index),
	[{joined_blocks, Blocks}] = ets:lookup(node_state, joined_blocks),
	ar_header_sync:join(BI, Blocks),
	ar_data_sync:join(BI),
	ar_tx_blacklist:start_taking_down(),
	Current = element(1, hd(BI)),
	B = hd(Blocks),
	ar_block_cache:initialize_from_list(block_cache, Blocks),
	BlockTXPairs = [block_txs_pair(Block) || Block <- Blocks],
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs),
	{Rate, ScheduledRate} =
		case B#block.height >= ar_fork:height_2_5() of
			true ->
				{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
			false ->
				{?USD_TO_AR_INITIAL_RATE, ?USD_TO_AR_INITIAL_RATE}
		end,
	ar:console("Joined the Arweave network successfully.~n"),
	?LOG_INFO([{event, joined_the_network}]),
	ets:insert(node_state, [
		{is_joined,				true},
		{block_index,			BI},
		{current,				Current},
		{wallet_list,			B#block.wallet_list},
		{height,				B#block.height},
		{reward_pool,			B#block.reward_pool},
		{diff,					B#block.diff},
		{cumulative_diff,		B#block.cumulative_diff},
		{last_retarget,			B#block.last_retarget},
		{weave_size,			B#block.weave_size},
		{block_txs_pairs,		BlockTXPairs},
		{block_anchors,			BlockAnchors},
		{recent_txs_map,		RecentTXMap},
		{usd_to_ar_rate,		Rate},
		{scheduled_usd_to_ar_rate, ScheduledRate}
	]),
	{noreply, reset_miner(State)};

handle_info({new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}, State) ->
	gen_server:cast(?MODULE, {process_new_block, Peer, Height, NewB, BDS, ReceiveTimestamp}),
	{noreply, State};

handle_info({work_complete, BaseBH, NewB, MinedTXs, BDS, POA}, State) ->
	gen_server:cast(?MODULE, {work_complete, BaseBH, NewB, MinedTXs, BDS, POA}),
	{noreply, State};

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

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {message, Info}]),
	{noreply, State}.

terminate(Reason, #{ miner := Miner }) ->
	ar_http_iface_server:stop(),
	case ets:lookup(node_state, is_joined) of
		[{_, true}] ->
			case Miner of
				undefined -> do_nothing;
				PID -> ar_mine:stop(PID)
			end,
			[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
			[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
			Mempool =
				maps:map(
					fun(TXID, Status) ->
						[{{tx, TXID}, TX}] = ets:lookup(node_state, {tx, TXID}),
						{TX, Status}
					end,
					Map
				),
			dump_mempool(Mempool, MempoolSize);
		_ ->
			ok
	end,
	?LOG_INFO([
		{event, ar_node_worker_terminated},
		{module, ?MODULE},
		{reason, Reason}
	]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

record_metrics() ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{weave_size, WeaveSize}] = ets:lookup(node_state, weave_size),
	[{height, Height}] = ets:lookup(node_state, height),
	prometheus_gauge:set(arweave_block_height, Height),
	record_mempool_size_metrics(MempoolSize),
	prometheus_gauge:set(weave_size, WeaveSize).

record_mempool_size_metrics({HeaderSize, DataSize}) ->
	prometheus_gauge:set(mempool_header_size_bytes, HeaderSize),
	prometheus_gauge:set(mempool_data_size_bytes, DataSize).

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

handle_task({cache_missing_txs, BH, TXs}, State) ->
	case ar_block_cache:get_block_and_status(block_cache, BH) of
		not_found ->
			%% The block should have been pruned while we were fetching the missing txs.
			{noreply, State};
		{B, not_validated} ->
			ar_block_cache:add(block_cache, B#block{ txs = TXs }),
			gen_server:cast(self(), apply_block),
			{noreply, State};
		{_B, _AnotherStatus} ->
			%% The transactions should have been received and the block validated while
			%% we were looking for previously missing transactions.
			{noreply, State}
	end;

handle_task({work_complete, BaseBH, NewB, MinedTXs, BDS, POA}, State) ->
	[{block_index, [{CurrentBH, _, _} | _]}] = ets:lookup(node_state, block_index),
	case BaseBH of
		CurrentBH ->
			handle_block_from_miner(State, NewB, MinedTXs, BDS, POA);
		_ ->
			?LOG_INFO([{event, ignore_mined_block}, {reason, accepted_foreign_block}]),
			{noreply, State}
	end;

handle_task(mine, #{ io_threads := IOThreads } = State) ->
	IOThreads2 =
		case IOThreads of
			[] ->
				start_io_threads();
			_ ->
				IOThreads
		end,
	{noreply, start_mining(State#{ io_threads => IOThreads2 })};

handle_task(automine, State) ->
	{noreply, start_mining(State#{ automine => true })};

handle_task({add_peers, Peers}, #{ gossip := GS } = State) ->
	NewGS = ar_gossip:add_peers(GS, Peers),
	{noreply, State#{ gossip => NewGS }};

handle_task({set_loss_probability, Prob}, #{ gossip := GS } = State) ->
	{noreply, State#{ gossip => ar_gossip:set_loss_probability(GS, Prob) }};

handle_task({filter_mempool, Iterator}, State) ->
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	[{wallet_list, WalletList}] = ets:lookup(node_state, wallet_list),
	[{height, Height}] = ets:lookup(node_state, height),
	[{usd_to_ar_rate, Rate}] = ets:lookup(node_state, usd_to_ar_rate),
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
	[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
	{ok, List, NextIterator} = take_mempool_chunk(Iterator, ?FILTER_MEMPOOL_CHUNK_SIZE),
	case List of
		[] ->
			{noreply, State};
		_ ->
			Wallets = ar_wallets:get(WalletList, ar_tx:get_addresses(List)),
			InvalidTXs =
				lists:foldl(
					fun(TX, Acc) ->
						case ar_tx_replay_pool:verify_tx({
							TX,
							Rate,
							Height,
							BlockAnchors,
							RecentTXMap,
							#{},
							Wallets
						}) of
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
			drop_txs(InvalidTXs, Map, MempoolSize),
			case NextIterator of
				none ->
					scan_complete;
				_ ->
					gen_server:cast(self(), {filter_mempool, NextIterator})
			end,
			{noreply, State}
	end;

handle_task(Msg, State) ->
	?LOG_ERROR([
		{event, ar_node_worker_received_unknown_message},
		{message, Msg}
	]),
	{noreply, State}.

get_block_anchors_and_recent_txs_map(BlockTXPairs) ->
	lists:foldr(
		fun({BH, L}, {Acc1, Acc2}) ->
			Acc3 =
				lists:foldl(
					fun({{TXID, _}, _}, Acc4) ->
						%% We use a map instead of a set here because it is faster.
						maps:put(TXID, ok, Acc4)
					end,
					Acc2,
					L
				),
			{[BH | Acc1], Acc3}
		end,
		{[], #{}},
		lists:sublist(BlockTXPairs, ?MAX_TX_ANCHOR_DEPTH)
	).

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
	?LOG_INFO([
		{event, ar_node_worker_received_unknown_gossip_message},
		{message, UnknownMessage}
	]),
	{noreply, State}.

%% @doc Add the new transaction to the server state.
handle_add_tx(State, TX, GS) ->
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	add_tx_to_mempool(TX, ready_for_mining),
	{noreply, State#{ gossip => NewGS }}.

add_tx_to_mempool(#tx{ id = TXID } = TX, Status) ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	MempoolSize2 =
		case maps:is_key(TXID, Map) of
			false ->
				increase_mempool_size(MempoolSize, TX);
			true ->
				MempoolSize
		end,
	ets:insert(node_state, [
		{{tx, TX#tx.id}, TX},
		{tx_statuses, maps:put(TX#tx.id, Status, Map)},
		{mempool_size, MempoolSize2}
	]),
	ok.

increase_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize + HeaderSize, MempoolDataSize + DataSize}.

tx_mempool_size(#tx{ format = 1, data = Data }) ->
	{?TX_SIZE_BASE + byte_size(Data), 0};
tx_mempool_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

%% @doc Add the new waiting transaction to the server state.
handle_add_waiting_tx(State, #tx{ id = TXID } = TX, GS) ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	{NewGS, _} = ar_gossip:send(GS, {add_waiting_tx, TX}),
	case maps:is_key(TXID, Map) of
		false ->
			Map2 = maps:put(TX#tx.id, waiting, Map),
			ets:insert(node_state, [
				{{tx, TX#tx.id}, TX},
				{tx_statuses, Map2},
				{mempool_size, increase_mempool_size(MempoolSize, TX)}
			]),
			{noreply, State#{ gossip => NewGS }};
		true ->
			{noreply, State#{ gossip => NewGS }}
	end.

%% @doc Add the transaction to the mining pool, to be included in the mined block.
handle_move_tx_to_mining_pool(State, TX, GS) ->
	{NewGS, _} = ar_gossip:send(GS, {move_tx_to_mining_pool, TX}),
	add_tx_to_mempool(TX, ready_for_mining),
	{noreply, State#{ gossip => NewGS }}.

handle_drop_waiting_txs(State, DroppedTXs, GS) ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	{NewGS, _} = ar_gossip:send(GS, {drop_waiting_txs, DroppedTXs}),
	drop_txs(DroppedTXs, Map, MempoolSize),
	{noreply, State#{ gossip => NewGS }}.

drop_txs(DroppedTXs, TXs, MempoolSize) ->
	{TXs2, DroppedTXMap} =
		lists:foldl(
			fun(TX, {Acc, DroppedAcc}) ->
				case maps:take(TX#tx.id, Acc) of
					{_Value, Map} ->
						ar_tx_queue:drop_tx(TX),
						{Map, maps:put(TX#tx.id, TX, DroppedAcc)};
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
	ets:insert(node_state, [
		{mempool_size, DecreasedMempoolSize},
		{tx_statuses, TXs2}
	]),
	maps:map(
		fun(TXID, _) ->
			ets:delete(node_state, {tx, TXID})
		end,
		DroppedTXMap
	).

take_mempool_chunk(Iterator, Size) ->
	take_mempool_chunk(Iterator, Size, []).

take_mempool_chunk(Iterator, 0, Taken) ->
	{ok, Taken, Iterator};
take_mempool_chunk(Iterator, Size, Taken) ->
	case maps:next(Iterator) of
		none ->
			{ok, Taken, none};
		{TXID, _Status, NextIterator} ->
			case ets:lookup(node_state, {tx, TXID}) of
				[{_, TX}] ->
					take_mempool_chunk(NextIterator, Size - 1, [TX | Taken]);
				[] ->
					take_mempool_chunk(NextIterator, Size, Taken)
			end
	end.

%% @doc Record the block in the block cache. Schedule an application of the
%% earliest not validated block from the longest chain, if any.
%% @end
handle_new_block(State, #block{ indep_hash = H, txs = TXs })
		when length(TXs) > ?BLOCK_TX_COUNT_LIMIT ->
	?LOG_WARNING([
		{event, received_block_with_too_many_txs},
		{block, ar_util:encode(H)},
		{txs, length(TXs)}
	]),
	{noreply, State};
handle_new_block(State, BShadow) ->
	case ar_block_cache:get(block_cache, BShadow#block.indep_hash) of
		not_found ->
			case ar_block_cache:get(block_cache, BShadow#block.previous_block) of
				not_found ->
					%% The cache should have been just pruned and this block is old.
					{noreply, State};
				_ ->
					ar_block_cache:add(block_cache, BShadow),
					ar_ignore_registry:add(BShadow#block.indep_hash),
					gen_server:cast(self(), apply_block),
					{noreply, State}
			end;
		_ ->
			ar_ignore_registry:add(BShadow#block.indep_hash),
			%% The block's already received from a different peer or
			%% fetched by ar_poller.
			{noreply, State}
	end.

apply_block(#{ blocks_missing_txs := BlocksMissingTXs } = State) ->
	case ar_block_cache:get_earliest_not_validated_from_longest_chain(block_cache) of
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
		blocks_missing_txs := BlocksMissingTXs,
		missing_txs_lookup_processes := MissingTXsLookupProcesses
	} = State,
	[{block_txs_pairs, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
	[{block_index, BI}] = ets:lookup(node_state, block_index),
	[{tx_statuses, Mempool}] = ets:lookup(node_state, tx_statuses),
	Timestamp = erlang:timestamp(),
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs, Mempool),
	case MissingTXIDs of
		[] ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
			B = BShadow#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs },
			PrevWalletList = PrevB#block.wallet_list,
			PrevRewardPool = PrevB#block.reward_pool,
			PrevHeight = PrevB#block.height,
			Rate =
				case PrevHeight >= ar_fork:height_2_5() of
					true ->
						PrevB#block.usd_to_ar_rate;
					false ->
						?USD_TO_AR_INITIAL_RATE
				end,
			case validate_wallet_list(B, PrevWalletList, PrevRewardPool, Rate, PrevHeight) of
				error ->
					BH = B#block.indep_hash,
					ar_block_cache:remove(block_cache, BH),
					{noreply, State};
				{ok, RootHash} ->
					B2 = B#block{ wallet_list = RootHash },
					Wallets =
						ar_wallets:get(
							PrevWalletList,
							[B#block.reward_addr | ar_tx:get_addresses(B#block.txs)]
						),
					BI2 = update_block_index(B, PrevBlocks, BI),
					BlockTXPairs2 = update_block_txs_pairs(B, PrevBlocks, BlockTXPairs),
					BlockTXPairs3 = tl(BlockTXPairs2),
					{BlockAnchors, RecentTXMap} =
						get_block_anchors_and_recent_txs_map(BlockTXPairs3),
					case ar_node_utils:validate(
							tl(BI2), B2, PrevB, Wallets, BlockAnchors, RecentTXMap) of
						{invalid, Reason} ->
							?LOG_WARNING([
								{event, received_invalid_block},
								{validation_error, Reason}
							]),
							BH = B#block.indep_hash,
							ar_block_cache:remove(block_cache, BH),
							{noreply, State};
						valid ->
							State2 =
								apply_validated_block(
									State,
									B2,
									PrevBlocks,
									BI2,
									BlockTXPairs2
								),
							ar_watchdog:foreign_block(B#block.indep_hash),
							record_processing_time(Timestamp),
							{noreply, State2}
				end
			end;
		_ ->
			?LOG_INFO([{event, missing_txs_for_block}, {count, length(MissingTXIDs)}]),
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
					_Status ->
						[{{tx, _}, TX}] = ets:lookup(node_state, {tx, TXID}),
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

validate_wallet_list(B, WalletList, RewardPool, Rate, Height) ->
	case ar_wallets:apply_block(B, WalletList, RewardPool, Rate, Height) of
		{error, invalid_reward_pool} ->
			?LOG_WARNING([
				{event, received_invalid_block},
				{validation_error, invalid_reward_pool}
			]),
			error;
		{error, invalid_wallet_list} ->
			?LOG_WARNING([
				{event, received_invalid_block},
				{validation_error, invalid_wallet_list}
			]),
			error;
		{ok, RootHash} ->
			{ok, RootHash}
	end.

get_missing_txs_and_retry(BShadow, Mempool, Worker) ->
	Peers = ar_bridge:get_remote_peers(),
	case ar_http_iface_client:get_txs(Peers, Mempool, BShadow) of
		{ok, TXs} ->
			gen_server:cast(Worker, {cache_missing_txs, BShadow#block.indep_hash, TXs});
		_ ->
			?LOG_WARNING([
				{event, ar_node_worker_could_not_find_block_txs},
				{block, ar_util:encode(BShadow#block.indep_hash)}
			])
	end.

apply_validated_block(State, B, PrevBlocks, BI, BlockTXPairs) ->
	[{_, CDiff}] = ets:lookup(node_state, cumulative_diff),
	case B#block.cumulative_diff =< CDiff of
		true ->
			%% The block is from the longest fork, but not the latest known block from there.
			ar_block_cache:add_validated(block_cache, B),
			gen_server:cast(self(), apply_block),
			log_applied_block(B),
			State;
		false ->
			apply_validated_block2(State, B, PrevBlocks, BI, BlockTXPairs)
	end.

apply_validated_block2(State, B, PrevBlocks, BI, BlockTXPairs) ->
	[{current, CurrentH}] = ets:lookup(node_state, current),
	PruneDepth = ?STORE_BLOCKS_BEHIND_CURRENT,
	BH = B#block.indep_hash,
	%% Overwrite the block to store computed size tagged txs - they
	%% may be needed for reconstructing block_txs_pairs if there is a reorg
	%% off and then back on this fork.
	ar_block_cache:add(block_cache, B),
	ar_block_cache:mark_tip(block_cache, BH),
	ar_block_cache:prune(block_cache, PruneDepth),
	%% We could have missed a few blocks due to networking issues, which would then
	%% be picked by ar_poller and end up waiting for missing transactions to be fetched.
	%% Thefore, it is possible (although not likely) that there are blocks above the new tip,
	%% for which we trigger a block application here, in order not to wait for the next
	%% arrived or fetched block to trigger it.
	gen_server:cast(self(), apply_block),
	log_applied_block(B),
	log_tip(B),
	maybe_report_n_confirmations(B, BI),
	maybe_store_block_index(B, BI),
	record_fork_depth(length(PrevBlocks) - 1),
	return_orphaned_txs_to_mempool(CurrentH, (lists:last(PrevBlocks))#block.indep_hash),
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
				ok =
					ar_wallets:set_current(PrevWallets, Wallets, Addr, Height, PruneDepth * 2),
				CurrentB
		end,
		start,
		lists:reverse([B | PrevBlocks])
	),
	RecentBI = lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT * 2),
	ar_data_sync:add_tip_block(B#block.height, BlockTXPairs, RecentBI),
	ar_header_sync:add_tip_block(B, RecentBI),
	lists:foreach(
		fun(PrevB) ->
			ar_header_sync:add_block(PrevB)
		end,
		tl(lists:reverse(PrevBlocks))
	),
	BlockTXs = B#block.txs,
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	drop_txs(BlockTXs, Map, MempoolSize),
	[{tx_statuses, Map2}] = ets:lookup(node_state, tx_statuses),
	gen_server:cast(self(), {filter_mempool, maps:iterator(Map2)}),
	lists:foreach(fun(TX) -> ar_tx_queue:drop_tx(TX) end, BlockTXs),
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs),
	{Rate, ScheduledRate} =
		case B#block.height >= ar_fork:height_2_5() of
			true ->
				{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
			false ->
				{?USD_TO_AR_INITIAL_RATE, ?USD_TO_AR_INITIAL_RATE}
		end,
	ets:insert(node_state, [
		{block_index,			BI},
		{current,				B#block.indep_hash},
		{wallet_list,			B#block.wallet_list},
		{height,				B#block.height},
		{reward_pool,			B#block.reward_pool},
		{diff,					B#block.diff},
		{cumulative_diff,		B#block.cumulative_diff},
		{last_retarget,			B#block.last_retarget},
		{weave_size,			B#block.weave_size},
		{block_txs_pairs,		BlockTXPairs},
		{block_anchors,			BlockAnchors},
		{recent_txs_map,		RecentTXMap},
		{usd_to_ar_rate,		Rate},
		{scheduled_usd_to_ar_rate, ScheduledRate}
	]),
	reset_miner(State).

log_applied_block(B) ->
	?LOG_INFO([
		{event, applied_block},
		{indep_hash, ar_util:encode(B#block.indep_hash)},
		{height, B#block.height}
	]).

log_tip(B) ->
	?LOG_INFO([
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
			ar_watchdog:block_received_n_confirmations(H, B#block.height - N + 1);
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

return_orphaned_txs_to_mempool(H, H) ->
	ok;
return_orphaned_txs_to_mempool(H, BaseH) ->
	#block{ txs = TXs, previous_block = PrevH } = ar_block_cache:get(block_cache, H),
	lists:foreach(fun(TX) -> add_tx_to_mempool(TX, ready_for_mining) end, TXs),
	return_orphaned_txs_to_mempool(PrevH, BaseH).

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
start_mining(StateIn) ->
	#{
		reward_addr := RewardAddr,
		tags := Tags,
		io_threads := IOThreads
	} = StateIn,
	[{block_index, BI}] = ets:lookup(node_state, block_index),
	[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
	[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
	[{current, Current}] = ets:lookup(node_state, current),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	ar_watchdog:started_hashing(),
	B = ar_block_cache:get(block_cache, Current),
	Miner = ar_mine:start({
		B,
		maps:fold(
			fun
				(TXID, ready_for_mining, Acc) ->
					[{_, TX}] = ets:lookup(node_state, {tx, TXID}),
					[TX | Acc];
				(_, _, Acc) ->
					Acc
			end,
			[],
			Map
		),
		RewardAddr,
		Tags,
		ar_node_worker,
		BlockAnchors,
		RecentTXMap,
		BI,
		IOThreads
	}),
	?LOG_INFO([{event, started_mining}]),
	StateIn#{ miner => Miner }.

record_processing_time(StartTimestamp) ->
	ProcessingTime = timer:now_diff(erlang:timestamp(), StartTimestamp) / 1000000,
	prometheus_histogram:observe(block_processing_time, ProcessingTime).

calculate_mempool_size(TXs) ->
	maps:fold(
		fun(_TXID, TX, {HeaderAcc, DataAcc}) ->
			{HeaderSize, DataSize} = tx_mempool_size(TX),
			{HeaderSize + HeaderAcc, DataSize + DataAcc}
		end,
		{0, 0},
		TXs
	).

%% @doc Integrate the block found by us.
handle_block_from_miner(State, BShadow, MinedTXs, BDS, _POA) ->
	#{ gossip := GS } = State,
	[{block_index, BI}] = ets:lookup(node_state, block_index),
	[{block_txs_pairs, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
	[{current, Current}] = ets:lookup(node_state, current),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(MinedTXs),
	B = BShadow#block{ txs = MinedTXs, size_tagged_txs = SizeTaggedTXs },
	ar_watchdog:mined_block(B#block.indep_hash, B#block.height),
	?LOG_INFO([
		{event, mined_block},
		{indep_hash, ar_util:encode(B#block.indep_hash)},
		{txs, length(MinedTXs)}
	]),
	GossipMessage = {new_block, self(), B#block.height, B, BDS, erlang:timestamp()},
	{NewGS, _} = ar_gossip:send(GS, GossipMessage),
	PrevBlocks = [ar_block_cache:get(block_cache, Current)],
	BI2 = [block_index_entry(B) | BI],
	BlockTXPairs2 = [block_txs_pair(B) | BlockTXPairs],
	ar_block_cache:add(block_cache, B),
	ar_ignore_registry:add(B#block.indep_hash),
	State2 = apply_validated_block(State, B, PrevBlocks, BI2, BlockTXPairs2),
	{noreply, State2#{ gossip => NewGS }}.

%% @doc Assign a priority to the task. 0 corresponds to the highest priority.
priority({gossip_message, #gs_msg{ data = {new_block, _, Height, _, _, _} }}) ->
	{0, Height};
priority(apply_block) ->
	{1, 1};
priority({work_complete, _, _, _, _, _}) ->
	{2, 1};
priority({cache_missing_txs, _, _}) ->
	{3, 1};
priority(_) ->
	{os:system_time(second), 1}.

read_hash_list_2_0_for_1_0_blocks() ->
	Fork_2_0 = ar_fork:height_2_0(),
	case Fork_2_0 > 0 of
		true ->
			File = filename:join(["data", "hash_list_1_0"]),
			{ok, Binary} = file:read_file(File),
			HL = lists:map(fun ar_util:decode/1, jiffy:decode(Binary)),
			Fork_2_0 = length(HL),
			HL;
		false ->
			[]
	end.

start_from_block_index([#block{} = GenesisB]) ->
	BI = [ar_util:block_index_entry_from_block(GenesisB)],
	ar_randomx_state:init(BI, []),
	self() ! {join, BI, [GenesisB]};
start_from_block_index(BI) ->
	ar_randomx_state:init(BI, []),
	self() ! {join, BI, read_recent_blocks(BI)}.

read_recent_blocks(not_joined) ->
	[];
read_recent_blocks(BI) ->
	read_recent_blocks2(lists:sublist(BI, 2 * ?MAX_TX_ANCHOR_DEPTH)).

read_recent_blocks2([]) ->
	[];
read_recent_blocks2([{BH, _, _} | BI]) ->
	B = ar_storage:read_block(BH),
	TXs = ar_storage:read_tx(B#block.txs),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	[B#block{ size_tagged_txs = SizeTaggedTXs, txs = TXs } | read_recent_blocks2(BI)].

dump_mempool(TXs, MempoolSize) ->
	case ar_storage:write_term(mempool, {TXs, MempoolSize}) of
		ok ->
			ok;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_dump_mempool}, {reason, Reason}])
	end.
