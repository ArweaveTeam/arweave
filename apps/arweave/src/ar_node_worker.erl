%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%

%%% @doc The server responsible for processing blocks and transactions and
%%% maintaining the node state. Blocks are prioritized over transactions.
%%% @end
-module(ar_node_worker).

-export([start_link/0, calculate_delay/1, is_mempool_or_block_cache_tx/1]).

-export([init/1, handle_cast/2, handle_info/2, terminate/2, tx_mempool_size/1,
		tx_id_prefix/1]).

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

%% @doc Return the prefix used to inform block receivers about the block's transactions
%% via POST /block_announcement.
tx_id_prefix(TXID) ->
	binary:part(TXID, 0, 8).

%% @doc Return true if the given transaction identifier is found in the mempool or
%% block cache (the last ?STORE_BLOCKS_BEHIND_CURRENT blocks).
is_mempool_or_block_cache_tx(TXID) ->
	ets:match_object(tx_prefixes, {tx_id_prefix(TXID), TXID}) /= [].

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	[ok, ok] = ar_events:subscribe([tx, block]),
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
			ar_join:start(ar_peers:get_trusted_peers());
		{BI, true} ->
			start_from_block_index(BI);
		{_, false} ->
			do_nothing
	end,
	%% Add pending transactions from the persisted mempool to the propagation queue.
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	maps:map(
		fun (_TXID, ready_for_mining) ->
				ok;
			(TXID, waiting) ->
				[{_, TX}] = ets:lookup(node_state, {tx, TXID}),
				start_tx_mining_timer(TX)
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
		reward_addr => Config#config.mining_addr,
		blocks_missing_txs => sets:new(),
		missing_txs_lookup_processes => #{},
		task_queue => gb_sets:new(),
		io_threads => IOThreads
	}}.

load_mempool() ->
	case ar_storage:read_term(mempool) of
		{ok, {TXs, _MempoolSize}} ->
			Map =
				maps:map(
					fun(TXID, {TX, Status}) ->
						ets:insert(node_state, {{tx, TXID}, TX}),
						ets:insert(tx_prefixes, {tx_id_prefix(TXID), TXID}),
						Status
					end,
					TXs
				),
			MempoolSize = maps:fold(
				fun(_, {TX, _}, Acc) ->
					increase_mempool_size(Acc, TX)
				end,
				{0, 0},
				TXs
			),
			Set =
				maps:fold(
					fun(TXID, {TX, Status}, Acc) ->
						Timestamp = get_or_create_tx_timestamp(TXID),
						gb_sets:add_element({{ar_tx:utility(TX), Timestamp}, TXID, Status}, Acc)
					end,
					gb_sets:new(),
					TXs
				),
			PropagationQueue =
				maps:fold(
					fun	(_TXID, {_TX, ready_for_mining}, Acc) ->
							Acc;
						(TXID, {TX, _Status}, Acc) ->
							Timestamp = get_or_create_tx_timestamp(TXID),
							gb_sets:add_element({{ar_tx:utility(TX), Timestamp}, TXID}, Acc)
					end,
					gb_sets:new(),
					TXs
				),
			ets:insert(node_state, [
				{mempool_size, MempoolSize},
				{tx_statuses, Map},
				{tx_priority_set, Set},
				{tx_propagation_queue, PropagationQueue}
			]);
		not_found ->
			ets:insert(node_state, [
				{mempool_size, {0, 0}},
				{tx_statuses, #{}},
				{tx_priority_set, gb_sets:new()},
				{tx_propagation_queue, gb_sets:new()}
			]);
		{error, Error} ->
			?LOG_ERROR([{event, failed_to_load_mempool}, {reason, Error}]),
			ets:insert(node_state, [
				{mempool_size, {0, 0}},
				{tx_statuses, #{}},
				{tx_priority_set, gb_sets:new()},
				{tx_propagation_queue, gb_sets:new()}
			])
	end.

start_tx_mining_timer(TX) ->
	%% Calling with ar_node_worker: allows to mock calculate_delay/1 in tests.
	erlang:send_after(ar_node_worker:calculate_delay(tx_propagated_size(TX)), ?MODULE,
			{tx_ready_for_mining, TX}).

tx_propagated_size(#tx{ format = 2 }) ->
	?TX_SIZE_BASE;
tx_propagated_size(#tx{ format = 1, data = Data }) ->
	?TX_SIZE_BASE + byte_size(Data).

%% @doc Return a delay in milliseconds to wait before including a transaction
%% into a block. The delay is computed as base delay + a function of data size with
%% a conservative estimation of the network speed.
calculate_delay(Bytes) ->
	BaseDelay = (?BASE_TX_PROPAGATION_DELAY) * 1000,
	NetworkDelay = Bytes * 8 div (?TX_PROPAGATION_BITS_PER_SECOND) * 1000,
	BaseDelay + NetworkDelay.

start_io_threads() ->
	%% Start the IO mining processes. The mining server and the hashing
	%% processes are historically restarted every round, but the IO
	%% processes keep the database files open for better performance so
	%% we do not want to restart them.
	{ok, Config} = application:get_env(arweave, config),
	ets:insert(mining_state, {session, {make_ref(), os:system_time(second), not_set, not_set}}),
	SearchInRocksDB = lists:member(search_in_rocksdb_when_mining, Config#config.enable),
	[spawn_link(
		fun() ->
			process_flag(trap_exit, true),
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
			ar_util:cast_after(?PROCESS_TASK_QUEUE_FREQUENCY_MS, ?MODULE, process_task_queue),
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

handle_info({join, BI, Blocks}, State) ->
	{ok, _} = ar_wallets:start_link([{blocks, Blocks},
			{peers, ar_peers:get_trusted_peers()}]),
	ar_block_index:init(BI),
	ets:insert(node_state, [
		{recent_block_index,	lists:sublist(BI, ?STORE_BLOCKS_BEHIND_CURRENT * 3)},
		{joined_blocks,			Blocks}
	]),
	{noreply, State};

handle_info({tx_ready_for_mining, TX}, State) ->
	add_tx_to_mempool(TX, ready_for_mining),
	ar_events:send(tx, {ready_for_mining, TX}),
	{noreply, State};

handle_info({event, block, {new, Block, _Source}}, State)
		when length(Block#block.txs) > ?BLOCK_TX_COUNT_LIMIT ->
	?LOG_WARNING([
		{event, received_block_with_too_many_txs},
		{block, ar_util:encode(Block#block.indep_hash)},
		{txs, length(Block#block.txs)}
	]),
	{noreply, State};

handle_info({event, block, {new, Block, _Source}}, State) ->
	%% Record the block in the block cache. Schedule an application of the
	%% earliest not validated block from the longest chain, if any.
	case ar_block_cache:get(block_cache, Block#block.indep_hash) of
		not_found ->
			case ar_block_cache:get(block_cache, Block#block.previous_block) of
				not_found ->
					%% The cache should have been just pruned and this block is old.
					{noreply, State};
				_ ->
					ar_block_cache:add(block_cache, Block),
					ar_ignore_registry:add(Block#block.indep_hash),
					gen_server:cast(self(), apply_block),
					{noreply, State}
			end;
		_ ->
			ar_ignore_registry:add(Block#block.indep_hash),
			%% The block's already received from a different peer or
			%% fetched by ar_poller.
			{noreply, State}
	end;

handle_info({event, block, {mined, Block, TXs, CurrentBH, RecallByte}}, State) ->
	case ets:lookup(node_state, recent_block_index) of
		[{recent_block_index, [{CurrentBH, _, _} | _] = RecentBI}] ->
			[{block_txs_pairs, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
			[{current, Current}] = ets:lookup(node_state, current),
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs,
					Block#block.height),
			B = Block#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs },
			ar_watchdog:mined_block(B#block.indep_hash, B#block.height),
			?LOG_INFO([{event, mined_block},
					{indep_hash, ar_util:encode(B#block.indep_hash)}, {txs, length(TXs)}]),
			PrevBlocks = [ar_block_cache:get(block_cache, Current)],
			RecentBI2 = [block_index_entry(B) | RecentBI],
			BlockTXPairs2 = [block_txs_pair(B) | BlockTXPairs],
			ar_block_cache:add(block_cache, B),
			ar_ignore_registry:add(B#block.indep_hash),
			State2 = apply_validated_block(State, B, PrevBlocks, 0, RecentBI2, BlockTXPairs2),
			%% Won't be received by itself, but we should let know all "block" subscribers.
			ar_events:send(block, {new, Block#block{ txs = TXs }, #{ source => miner,
					recall_byte => RecallByte }}),
			{noreply, State2};
		_ ->
			?LOG_INFO([{event, ignore_mined_block}, {reason, accepted_foreign_block}]),
			{noreply, State}
	end;

handle_info({event, block, _}, State) ->
	{noreply, State};

%% Add the new waiting transaction to the server state.
handle_info({event, tx, {new, TX, _Source}}, State) ->
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	TXID = TX#tx.id,
	case maps:is_key(TXID, Map) of
		false ->
			[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
			Map2 = maps:put(TX#tx.id, waiting, Map),
			[{tx_priority_set, Set}] = ets:lookup(node_state, tx_priority_set),
			[{tx_propagation_queue, Q}] = ets:lookup(node_state, tx_propagation_queue),
			Timestamp = get_or_create_tx_timestamp(TXID),
			Utility = ar_tx:utility(TX),
			Set2 = gb_sets:add_element({{Utility, Timestamp}, TXID, waiting}, Set),
			Q2 = gb_sets:add_element({{Utility, Timestamp}, TXID}, Q),
			MempoolSize2 = increase_mempool_size(MempoolSize, TX),
			ets:insert(node_state, {{tx, TXID}, TX}),
			ets:insert(tx_prefixes, {tx_id_prefix(TXID), TXID}),
			{Map3, Set3, Q3, MempoolSize3} = may_be_drop_low_priority_txs(Map2, Set2, Q2,
					MempoolSize2),
			ets:insert(node_state, [
				{tx_statuses, Map3},
				{mempool_size, MempoolSize3},
				{tx_priority_set, Set3},
				{tx_propagation_queue, Q3}
			]),
			case maps:is_key(TXID, Map3) of
				true ->
					start_tx_mining_timer(TX);
				false ->
					%% The transaction has been dropped because more valuable transactions
					%% exceed the mempool limit.
					ok
			end,
			{noreply, State};
		true ->
			{noreply, State}
	end;

handle_info({event, tx, {emitting_scheduled, Utility, TXID}}, State) ->
	[{tx_propagation_queue, Q}] = ets:lookup(node_state, tx_propagation_queue),
	ets:insert(node_state, {tx_propagation_queue, gb_sets:del_element({Utility, TXID}, Q)}),
	{noreply, State};

%% Add the transaction to the mining pool, to be included in the mined block.
handle_info({event, tx, {ready_for_mining, TX}}, State) ->
	add_tx_to_mempool(TX, ready_for_mining),
	{noreply, State};

%% Remove dropped transactions.
handle_info({event, tx, {dropped, DroppedTX, Reason}}, State) ->
	?LOG_DEBUG("Drop TX ~p from pool with reason: ~p",
			[ar_util:encode(DroppedTX#tx.id), Reason]),
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	[{tx_priority_set, Set}] = ets:lookup(node_state, tx_priority_set),
	[{tx_propagation_queue, Q}] = ets:lookup(node_state, tx_propagation_queue),
	drop_txs([DroppedTX], Map, Set, Q, MempoolSize),
	{noreply, State};

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info(wallets_ready, State) ->
	[{recent_block_index, RecentBI}] = ets:lookup(node_state, recent_block_index),
	[{joined_blocks, Blocks}] = ets:lookup(node_state, joined_blocks),
	B = hd(Blocks),
	Height = B#block.height,
	ar_disk_cache:write_block(B),
	ar_data_sync:join(RecentBI, B#block.packing_2_5_threshold,
			B#block.strict_data_split_threshold),
	ar_header_sync:join(Height, RecentBI, Blocks),
	ar_tx_blacklist:start_taking_down(),
	Current = element(1, hd(RecentBI)),
	ar_block_cache:initialize_from_list(block_cache, Blocks),
	BlockTXPairs = [block_txs_pair(Block) || Block <- Blocks],
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs),
	{Rate, ScheduledRate} =
		case Height >= ar_fork:height_2_5() of
			true ->
				{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
			false ->
				{?INITIAL_USD_TO_AR((Height + 1))(), ?INITIAL_USD_TO_AR((Height + 1))()}
		end,
	ar:console("Joined the Arweave network successfully.~n"),
	?LOG_INFO([{event, joined_the_network}]),
	ets:insert(node_state, [
		{is_joined,				true},
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

handle_task({filter_mempool, Iterator}, State) ->
	{ok, List, NextIterator} = take_mempool_chunk(Iterator, ?FILTER_MEMPOOL_CHUNK_SIZE),
	case List of
		[] ->
			{noreply, State};
		_ ->
			[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
			[{tx_priority_set, Set}] = ets:lookup(node_state, tx_priority_set),
			[{tx_propagation_queue, Q}] = ets:lookup(node_state, tx_propagation_queue),
			[{wallet_list, WalletList}] = ets:lookup(node_state, wallet_list),
			[{height, Height}] = ets:lookup(node_state, height),
			[{usd_to_ar_rate, Rate}] = ets:lookup(node_state, usd_to_ar_rate),
			[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
			[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
			[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
			Wallets = ar_wallets:get(WalletList, ar_tx:get_addresses(List)),
			InvalidTXs =
				lists:foldl(
					fun(TX, Acc) ->
						case ar_tx_replay_pool:verify_tx({TX, Rate, Height, BlockAnchors,
								RecentTXMap, #{}, Wallets}, do_not_verify_signature) of
							valid ->
								Acc;
							{invalid, _Reason} ->
								may_be_drop_from_disk_pool(TX),
								[TX | Acc]
						end
					end,
					[],
					List
				),
			drop_txs(InvalidTXs, Map, Set, Q, MempoolSize),
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

get_or_create_tx_timestamp(TXID) ->
	case ets:lookup(node_state, {tx_timestamp, TXID}) of
		[] ->
			Timestamp = -os:system_time(microsecond),
			ets:insert(node_state, {{tx_timestamp, TXID}, Timestamp}),
			Timestamp;
		[{_, Timestamp}] ->
			Timestamp
	end.

increase_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize + HeaderSize, MempoolDataSize + DataSize}.

tx_mempool_size(#tx{ format = 1, data = Data }) ->
	{?TX_SIZE_BASE + byte_size(Data), 0};
tx_mempool_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

may_be_drop_low_priority_txs(Map, Set, Q, {MempoolHeaderSize, MempoolDataSize})
		when MempoolHeaderSize > ?MEMPOOL_HEADER_SIZE_LIMIT ->
	{{Utility, TXID, _Status}, Set2} = gb_sets:take_smallest(Set),
	[{_, TX}] = ets:lookup(node_state, {tx, TXID}),
	MempoolSize2 = decrease_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX),
	Map2 = maps:remove(TXID, Map),
	Q2 = gb_sets:del_element({Utility, TXID}, Q),
	may_be_drop_from_disk_pool(TX),
	ets:delete(node_state, {tx, TXID}),
	ets:delete(node_state, {tx_timestamp, TXID}),
	ets:delete_object(tx_prefixes, {tx_id_prefix(TXID), TXID}),
	may_be_drop_low_priority_txs(Map2, Set2, Q2, MempoolSize2);
may_be_drop_low_priority_txs(Map, Set, Q, {MempoolHeaderSize, MempoolDataSize})
		when MempoolDataSize > ?MEMPOOL_DATA_SIZE_LIMIT ->
	may_be_drop_low_priority_txs(Map, gb_sets:iterator(Set), Set, Q,
			{MempoolHeaderSize, MempoolDataSize});
may_be_drop_low_priority_txs(Map, Set, Q, MempoolSize) ->
	{Map, Set, Q, MempoolSize}.

may_be_drop_low_priority_txs(Map, Iterator, Set, Q, {MempoolHeaderSize, MempoolDataSize})
		when MempoolDataSize > ?MEMPOOL_DATA_SIZE_LIMIT ->
	{{Utility, TXID, _Status} = Element, Iterator2} = gb_sets:next(Iterator),
	[{_, TX}] = ets:lookup(node_state, {tx, TXID}),
	case TX#tx.format == 2 andalso byte_size(TX#tx.data) > 0 of
		true ->
			MempoolSize2 = decrease_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX),
			Map2 = maps:remove(TXID, Map),
			Set2 = gb_sets:del_element(Element, Set),
			Q2 = gb_sets:del_element({Utility, TXID}, Q),
			may_be_drop_from_disk_pool(TX),
			ets:delete(node_state, {tx, TXID}),
			ets:delete(node_state, {tx_timestamp, TXID}),
			ets:delete_object(tx_prefixes, {tx_id_prefix(TXID), TXID}),
			may_be_drop_low_priority_txs(Map2, Iterator2, Set2, Q2, MempoolSize2);
		false ->
			may_be_drop_low_priority_txs(Map, Iterator2, Set, Q,
					{MempoolHeaderSize, MempoolDataSize})
	end;
may_be_drop_low_priority_txs(Map, _Iterator, Set, Q, MempoolSize) ->
	{Map, Set, Q, MempoolSize}.

decrease_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize - HeaderSize, MempoolDataSize - DataSize}.

may_be_drop_from_disk_pool(#tx{ format = 1 }) ->
	ok;
may_be_drop_from_disk_pool(TX) ->
	ar_data_sync:maybe_drop_data_root_from_disk_pool(TX#tx.data_root, TX#tx.data_size,
			TX#tx.id).

drop_txs(DroppedTXs, TXs, Set, Q, MempoolSize) ->
	drop_txs(DroppedTXs, TXs, Set, Q, MempoolSize, true).

drop_txs(DroppedTXs, TXs, Set, Q, MempoolSize, RemoveTXPrefixes) ->
	{TXs2, Set2, Q2, DroppedTXMap} =
		lists:foldl(
			fun(TX, {Acc, SetAcc, QAcc, DroppedAcc}) ->
				case maps:take(TX#tx.id, Acc) of
					{Status, Map} ->
						ar_events:send(tx, {dropped, TX, removed_from_mempool}),
						TXID = TX#tx.id,
						Timestamp = get_or_create_tx_timestamp(TXID),
						Utility = ar_tx:utility(TX),
						Priority = {{Utility, Timestamp}, TXID, Status},
						SetAcc2 = gb_sets:del_element(Priority, SetAcc),
						QAcc2 = gb_sets:del_element({{Utility, Timestamp}, TXID}, QAcc),
						{Map, SetAcc2, QAcc2, maps:put(TXID, TX, DroppedAcc)};
					error ->
						{Acc, SetAcc, QAcc, DroppedAcc}
				end
			end,
			{TXs, Set, Q, maps:new()},
			DroppedTXs
		),
	{DroppedHeaderSize, DroppedDataSize} = calculate_mempool_size(DroppedTXMap),
	{MempoolHeaderSize, MempoolDataSize} = MempoolSize,
	DecreasedMempoolSize =
		{MempoolHeaderSize - DroppedHeaderSize, MempoolDataSize - DroppedDataSize},
	ets:insert(node_state, [
		{mempool_size, DecreasedMempoolSize},
		{tx_statuses, TXs2},
		{tx_priority_set, Set2},
		{tx_propagation_queue, Q2}
	]),
	maps:map(
		fun(TXID, _) ->
			ets:delete(node_state, {tx, TXID}),
			ets:delete(node_state, {tx_timestamp, TXID}),
			case RemoveTXPrefixes of
				true ->
					ets:delete_object(tx_prefixes, {tx_id_prefix(TXID), TXID});
				false ->
					ok
			end
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
	#{ blocks_missing_txs := BlocksMissingTXs,
			missing_txs_lookup_processes := MissingTXsLookupProcesses } = State,
	[{block_txs_pairs, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
	[{recent_block_index, RecentBI}] = ets:lookup(node_state, recent_block_index),
	[{tx_statuses, Mempool}] = ets:lookup(node_state, tx_statuses),
	Timestamp = erlang:timestamp(),
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs, Mempool),
	case MissingTXIDs of
		[] ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs,
					BShadow#block.height),
			B = BShadow#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs },
			PrevWalletList = PrevB#block.wallet_list,
			PrevRewardPool = PrevB#block.reward_pool,
			PrevHeight = PrevB#block.height,
			Rate = ar_pricing:usd_to_ar_rate(PrevB),
			case validate_wallet_list(B, PrevWalletList, PrevRewardPool, Rate, PrevHeight) of
				error ->
					BH = B#block.indep_hash,
					ar_block_cache:remove(block_cache, BH),
					{noreply, State};
				{ok, RootHash} ->
					B2 = B#block{ wallet_list = RootHash },
					Wallets = ar_wallets:get(PrevWalletList,
							[B#block.reward_addr | ar_tx:get_addresses(B#block.txs)]),
					{NOrphaned, RecentBI2} = update_block_index(B, PrevBlocks, RecentBI),
					BlockTXPairs2 = update_block_txs_pairs(B, PrevBlocks, BlockTXPairs),
					BlockTXPairs3 = tl(BlockTXPairs2),
					{BlockAnchors, RecentTXMap} =
						get_block_anchors_and_recent_txs_map(BlockTXPairs3),
					RecentBI3 = tl(RecentBI2),
					SearchSpaceUpperBound = ar_mine:get_search_space_upper_bound(RecentBI3),
					case ar_node_utils:validate(B2, PrevB, Wallets, BlockAnchors, RecentTXMap,
							RecentBI3, SearchSpaceUpperBound) of
						{invalid, Reason} ->
							?LOG_WARNING([{event, received_invalid_block},
									{validation_error, Reason}]),
							BH = B#block.indep_hash,
							ar_block_cache:remove(block_cache, BH),
							gen_server:cast(self(), apply_block),
							{noreply, State};
						valid ->
							State2 = apply_validated_block(State, B2, PrevBlocks, NOrphaned,
									RecentBI2, BlockTXPairs2),
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
				PID = spawn(fun() -> process_flag(trap_exit, true), get_missing_txs_and_retry(BShadow, Mempool, Self) end)
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

update_block_index(B, PrevBlocks, BI) ->
	#block{ indep_hash = H } = lists:last(PrevBlocks),
	{NOrphaned, Base} = drop_and_count_orphans(BI, H),
	{NOrphaned, [block_index_entry(B) |
		[block_index_entry(PrevB) || PrevB <- PrevBlocks] ++ Base]}.

drop_and_count_orphans(BI, H) ->
	drop_and_count_orphans(BI, H, 0).

drop_and_count_orphans([{H, _, _} | BI], H, N) ->
	{N, BI};
drop_and_count_orphans([_ | BI], H, N) ->
	drop_and_count_orphans(BI, H, N + 1).

block_index_entry(B) ->
	{B#block.indep_hash, B#block.weave_size, B#block.tx_root}.

update_block_txs_pairs(B, PrevBlocks, BlockTXPairs) ->
	lists:sublist(update_block_txs_pairs2(B, PrevBlocks, BlockTXPairs),
			2 * ?MAX_TX_ANCHOR_DEPTH).

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

get_missing_txs_and_retry(#block{ txs = TXIDs }, _Mempool, _Worker)
		when length(TXIDs) > 1000 ->
	?LOG_WARNING([{event, ar_node_worker_downloaded_txs_count_exceeds_limit}]),
	ok;
get_missing_txs_and_retry(BShadow, Mempool, Worker) ->
	get_missing_txs_and_retry(BShadow#block.indep_hash, BShadow#block.txs, Mempool,
			Worker, ar_peers:get_peers(), [], 0).

get_missing_txs_and_retry(_H, _TXIDs, _Mempool, _Worker, _Peers, _TXs, TotalSize)
		when TotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT ->
	?LOG_WARNING([{event, ar_node_worker_downloaded_txs_exceed_block_size_limit}]),
	ok;
get_missing_txs_and_retry(H, [], _Mempool, Worker, _Peers, TXs, _TotalSize) ->
	gen_server:cast(Worker, {cache_missing_txs, H, lists:reverse(TXs)});
get_missing_txs_and_retry(H, TXIDs, Mempool, Worker, Peers, TXs, TotalSize) ->
	Split = min(5, length(TXIDs)),
	{Bulk, Rest} = lists:split(Split, TXIDs),
	Fetch =
		lists:foldl(
			fun	(TX = #tx{ format = 1, data_size = DataSize }, {Acc1, Acc2}) ->
					{[TX | Acc1], Acc2 + DataSize};
				(TX = #tx{}, {Acc1, Acc2}) ->
					{[TX | Acc1], Acc2};
				(_, failed_to_fetch_tx) ->
					failed_to_fetch_tx;
				(_, _) ->
					failed_to_fetch_tx
			end,
			{TXs, TotalSize},
			ar_util:pmap(
				fun(TXID) ->
					ar_http_iface_client:get_tx(Peers, TXID, Mempool)
				end,
				Bulk
			)
		),
	case Fetch of
		failed_to_fetch_tx ->
			?LOG_WARNING([{event, ar_node_worker_failed_to_fetch_missing_tx}]),
			ok;
		{TXs2, TotalSize2} ->
			get_missing_txs_and_retry(H, Rest, Mempool, Worker, Peers, TXs2, TotalSize2)
	end.

apply_validated_block(State, B, PrevBlocks, NOrphaned, RecentBI, BlockTXPairs) ->
	[{_, CDiff}] = ets:lookup(node_state, cumulative_diff),
	case B#block.cumulative_diff =< CDiff of
		true ->
			%% The block is from the longest fork, but not the latest known block from there.
			ar_block_cache:add_validated(block_cache, B),
			gen_server:cast(self(), apply_block),
			log_applied_block(B),
			State;
		false ->
			apply_validated_block2(State, B, PrevBlocks, NOrphaned, RecentBI, BlockTXPairs)
	end.

apply_validated_block2(State, B, PrevBlocks, NOrphaned, RecentBI, BlockTXPairs) ->
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
	maybe_report_n_confirmations(B, RecentBI),
	record_fork_depth(NOrphaned),
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
	ar_disk_cache:write_block(B),
	BlockTXs = B#block.txs,
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	[{tx_priority_set, Set}] = ets:lookup(node_state, tx_priority_set),
	[{tx_propagation_queue, Q}] = ets:lookup(node_state, tx_propagation_queue),
	drop_txs(BlockTXs, Map, Set, Q, MempoolSize, false),
	[{tx_statuses, Map2}] = ets:lookup(node_state, tx_statuses),
	gen_server:cast(self(), {filter_mempool, maps:iterator(Map2)}),
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs),
	Height = B#block.height,
	{Rate, ScheduledRate} =
		case Height >= ar_fork:height_2_5() of
			true ->
				{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
			false ->
				{?INITIAL_USD_TO_AR((Height + 1))(), ?INITIAL_USD_TO_AR((Height + 1))()}
		end,
	AddedBIElements = tl(lists:reverse([block_index_entry(B)
			| [block_index_entry(PrevB) || PrevB <- PrevBlocks]])),
	ar_block_index:update(AddedBIElements, NOrphaned),
	ar_data_sync:add_tip_block(B#block.packing_2_5_threshold,
			B#block.strict_data_split_threshold, BlockTXPairs,
			lists:sublist(RecentBI, ?STORE_BLOCKS_BEHIND_CURRENT * 2)),
	ar_header_sync:add_tip_block(B, lists:sublist(RecentBI, ?STORE_BLOCKS_BEHIND_CURRENT * 2)),
	lists:foreach(
		fun(PrevB) ->
			ar_header_sync:add_block(PrevB),
			ar_disk_cache:write_block(PrevB)
		end,
		tl(lists:reverse(PrevBlocks))
	),
	maybe_store_block_index(B),
	ets:insert(node_state, [
		{recent_block_index,	lists:sublist(RecentBI, ?STORE_BLOCKS_BEHIND_CURRENT * 3)},
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

maybe_store_block_index(B) ->
	case B#block.height rem ?STORE_BLOCKS_BEHIND_CURRENT of
		0 ->
			BI = ar_node:get_block_index(),
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
	lists:foreach(fun(TX) ->
		ar_events:send(tx, {ready_for_mining, TX}),
		%% Add it to the mempool here even though have triggered an event - processes
		%% do not handle their own events.
		add_tx_to_mempool(TX, ready_for_mining)
	end, TXs),
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
	#{ reward_addr := RewardAddr, tags := Tags, io_threads := IOThreads } = StateIn,
	[{recent_block_index, RecentBI}] = ets:lookup(node_state, recent_block_index),
	[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
	[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
	[{current, Current}] = ets:lookup(node_state, current),
	ar_watchdog:started_hashing(),
	B = ar_block_cache:get(block_cache, Current),
	SearchSpaceUpperBound = ar_mine:get_search_space_upper_bound(RecentBI),
	Miner = ar_mine:start({B, collect_mining_transactions(?BLOCK_TX_COUNT_LIMIT),
			RewardAddr, Tags, BlockAnchors, RecentTXMap, SearchSpaceUpperBound,
			RecentBI, IOThreads}),
	?LOG_INFO([{event, started_mining}]),
	StateIn#{ miner => Miner }.

collect_mining_transactions(Limit) ->
	[{tx_priority_set, Set}] = ets:lookup(node_state, tx_priority_set),
	collect_mining_transactions(Limit, Set, []).

collect_mining_transactions(0, _Set, TXs) ->
	TXs;
collect_mining_transactions(Limit, Set, TXs) ->
	case gb_sets:is_empty(Set) of
		true ->
			TXs;
		false ->
			{{_Utility, TXID, Status}, Set2} = gb_sets:take_largest(Set),
			case Status of
				ready_for_mining ->
					[{_, TX}] = ets:lookup(node_state, {tx, TXID}),
					collect_mining_transactions(Limit - 1, Set2, [TX | TXs]);
				_ ->
					collect_mining_transactions(Limit, Set2, TXs)
			end
	end.

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
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, B#block.height),
	[B#block{ size_tagged_txs = SizeTaggedTXs, txs = TXs } | read_recent_blocks2(BI)].

dump_mempool(TXs, MempoolSize) ->
	case ar_storage:write_term(mempool, {TXs, MempoolSize}) of
		ok ->
			ok;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_dump_mempool}, {reason, Reason}])
	end.

add_tx_to_mempool(#tx{ id = TXID } = TX, Status) ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{tx_statuses, Map}] = ets:lookup(node_state, tx_statuses),
	[{tx_priority_set, Set}] = ets:lookup(node_state, tx_priority_set),
	[{tx_propagation_queue, Q}] = ets:lookup(node_state, tx_propagation_queue),
	Timestamp = get_or_create_tx_timestamp(TXID),
	Utility = {ar_tx:utility(TX), Timestamp},
	{MempoolSize2, Set2, Q2} =
		case maps:get(TXID, Map, not_found) of
			not_found ->
				ets:insert(node_state, {{tx, TXID}, TX}),
				ets:insert(tx_prefixes, {tx_id_prefix(TXID), TXID}),
				{increase_mempool_size(MempoolSize, TX),
						gb_sets:add_element({Utility, TXID, Status}, Set),
						gb_sets:add_element({Utility, TXID}, Q)};
			PrevStatus ->
				{MempoolSize, gb_sets:add_element({Utility, TXID, Status},
						gb_sets:del_element({Utility, TXID, PrevStatus}, Set)), Q}
		end,
	Map2 = maps:put(TX#tx.id, Status, Map),
	{Map3, Set3, Q3, MempoolSize3} = may_be_drop_low_priority_txs(Map2, Set2, Q2,
			MempoolSize2),
	ets:insert(node_state, [
		{tx_statuses, Map3},
		{mempool_size, MempoolSize3},
		{tx_priority_set, Set3},
		{tx_propagation_queue, Q3}
	]),
	ok.
