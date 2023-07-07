-module(ar_header_sync).

-behaviour(gen_server).

-export([start_link/0, join/3, add_tip_block/2, add_block/1, request_tx_removal/1,
		remove_block/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%%% This module syncs block and transaction headers and maintains a persisted record of synced
%%% headers. Headers are synced from latest to earliest.

-record(state, {
	block_index,
	height,
	sync_record,
	retry_queue,
	retry_record,
	is_disk_space_sufficient
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Update the tip after the node joins the network.
join(Height, RecentBI, Blocks) ->
	gen_server:cast(?MODULE, {join, Height, RecentBI, Blocks}).

%% @doc Add a new tip block to the index and storage, record the new recent block index.
add_tip_block(B, RecentBI) ->
	gen_server:cast(?MODULE, {add_tip_block, B, RecentBI}).

%% @doc Add a block to the index and storage.
add_block(B) ->
	gen_server:cast(?MODULE, {add_block, B}).

%% @doc Remove the given transaction.
request_tx_removal(TXID) ->
	gen_server:cast(?MODULE, {remove_tx, TXID}).

%% @doc Remove the block header with the given Height from the record. The process
%% will therefore re-sync it later (if there is available disk space).
remove_block(Height) ->
	gen_server:cast(?MODULE, {remove_block, Height}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_header_sync_start}]),
	process_flag(trap_exit, true),
	[ok, ok] = ar_events:subscribe([tx, disksup]),
	{ok, Config} = application:get_env(arweave, config),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "ar_header_sync_db"), ?MODULE),
	{SyncRecord, Height, CurrentBI} =
		case ar_storage:read_term(header_sync_state) of
			not_found ->
				{ar_intervals:new(), -1, []};
			{ok, StoredState} ->
				StoredState
		end,
	lists:foreach(
		fun(_) ->
			gen_server:cast(?MODULE, process_item)
		end,
		lists:seq(1, Config#config.header_sync_jobs)
	),
	gen_server:cast(?MODULE, store_sync_state),
	ets:insert(?MODULE, {synced_blocks, ar_intervals:sum(SyncRecord)}),
	{ok,
		#state{
			sync_record = SyncRecord,
			height = Height,
			block_index = CurrentBI,
			retry_queue = queue:new(),
			retry_record = ar_intervals:new(),
			is_disk_space_sufficient = true
		}}.

handle_cast({join, Height, RecentBI, Blocks}, State) ->
	#state{ height = PrevHeight, block_index = CurrentBI,
			sync_record = SyncRecord, retry_record = RetryRecord } = State,
	State2 =
		State#state{
			height = Height,
			block_index = RecentBI
		 },
	StartHeight = PrevHeight - length(CurrentBI) + 1,
	State3 =
		case {CurrentBI, ar_block_index:get_intersection(StartHeight, CurrentBI)} of
			{[], _} ->
				State2;
			{_, no_intersection} ->
				throw(last_stored_block_index_has_no_intersection_with_the_new_one);
			{_, {IntersectionHeight, _}} ->
				S = State2#state{
						sync_record = ar_intervals:cut(SyncRecord, IntersectionHeight),
						retry_record = ar_intervals:cut(RetryRecord, IntersectionHeight) },
				ok = store_sync_state(S),
				%% Delete from the kv store only after the sync record is saved - no matter
				%% what happens to the process, if a height is in the record, it must be
				%% present in the kv store.
				ok = ar_kv:delete_range(?MODULE, << (IntersectionHeight + 1):256 >>,
						<< (PrevHeight + 1):256 >>),
				S
		end,
	State4 =
		lists:foldl(
			fun(B, S) ->
				element(2, add_block(B, S))
			end,
			State3,
			Blocks
		),
	ok = store_sync_state(State4),
	{noreply, State4};

handle_cast({add_tip_block, #block{ height = Height } = B, RecentBI}, State) ->
	#state{ sync_record = SyncRecord, retry_record = RetryRecord,
			block_index = CurrentBI, height = PrevHeight } = State,
	BaseHeight = get_base_height(CurrentBI, PrevHeight, RecentBI),
	State2 = State#state{
		sync_record = ar_intervals:cut(SyncRecord, BaseHeight),
		retry_record = ar_intervals:cut(RetryRecord, BaseHeight),
		block_index = RecentBI,
		height = Height
	},
	State3 = element(2, add_block(B, State2)),
	case store_sync_state(State3) of
		ok ->
			%% Delete from the kv store only after the sync record is saved - no matter
			%% what happens to the process, if a height is in the record, it must be present
			%% in the kv store.
			ok = ar_kv:delete_range(?MODULE, << (BaseHeight + 1):256 >>,
					<< (PrevHeight + 1):256 >>),
			{noreply, State3};
		Error ->
			?LOG_WARNING([{event, failed_to_store_state},
					{reason, io_lib:format("~p", [Error])}]),
			{noreply, State}
	end;

handle_cast({add_historical_block, _, _, _, _, _},
		#state{ is_disk_space_sufficient = false } = State) ->
	gen_server:cast(?MODULE, process_item),
	{noreply, State};
handle_cast({add_historical_block, B, H, H2, TXRoot, Backoff}, State) ->
	case add_block(B, State) of
		{ok, State2} ->
			gen_server:cast(?MODULE, process_item),
			{noreply, State2};
		{_Error, State2} ->
			gen_server:cast(?MODULE, {failed_to_get_block, H, H2, TXRoot, B#block.height,
					Backoff}),
			{noreply, State2}
	end;

handle_cast({add_block, B}, State) ->
	{noreply, element(2, add_block(B, State))};

handle_cast(process_item, #state{ is_disk_space_sufficient = false } = State) ->
	ar_util:cast_after(?CHECK_AFTER_SYNCED_INTERVAL_MS, ?MODULE, process_item),
	{noreply, State};
handle_cast(process_item, #state{ retry_queue = Queue, retry_record = RetryRecord } = State) ->
	prometheus_gauge:set(downloader_queue_size, queue:len(Queue)),
	Queue2 = process_item(Queue),
	State2 = State#state{ retry_queue = Queue2 },
	case pick_unsynced_block(State) of
		nothing_to_sync ->
			{noreply, State2};
		Height ->
			case ar_node:get_block_index_entry(Height) of
				not_joined ->
					{noreply, State2};
				not_found ->
					{noreply, State2};
				{H, _WeaveSize, TXRoot} ->
					%% Before 2.0, to compute a block hash, the complete wallet list
					%% and all the preceding hashes were required. Getting a wallet list
					%% and a hash list for every historical block to verify it belongs to
					%% the weave is very costly. Therefore, a list of 2.0 hashes for 1.0
					%% blocks was computed and stored along with the network client.
					H2 =
						case Height < ar_fork:height_2_0() of
							true ->
								ar_node:get_2_0_hash_of_1_0_block(Height);
							false ->
								not_set
						end,
					{noreply, State2#state{
							retry_queue = enqueue({block, {H, H2, TXRoot, Height}}, Queue2),
							retry_record = ar_intervals:add(RetryRecord, Height, Height - 1) }}
			end
	end;

handle_cast({failed_to_get_block, H, H2, TXRoot, Height, Backoff},
		#state{ retry_queue = Queue } = State) ->
	Backoff2 = update_backoff(Backoff),
	Queue2 = enqueue({block, {H, H2, TXRoot, Height}}, Backoff2, Queue),
	gen_server:cast(?MODULE, process_item),
	{noreply, State#state{ retry_queue = Queue2 }};

handle_cast({remove_tx, TXID}, State) ->
	{ok, _Size} = ar_storage:delete_blacklisted_tx(TXID),
	ar_tx_blacklist:notify_about_removed_tx(TXID),
	{noreply, State};

handle_cast({remove_block, Height}, State) ->
	?LOG_INFO([{event, removing_block_record}, {height, Height}]),
	#state{ sync_record = Record } = State,
	ok = ar_kv:delete(?MODULE, << Height:256 >>),
	{noreply, State#state{ sync_record = ar_intervals:delete(Record, Height, Height - 1) }};

handle_cast(store_sync_state, State) ->
	ar_util:cast_after(?STORE_HEADER_STATE_FREQUENCY_MS, ?MODULE, store_sync_state),
	case store_sync_state(State) of
		ok ->
			{noreply, State};
		Error ->
			?LOG_WARNING([{event, failed_to_store_state},
					{reason, io_lib:format("~p", [Error])}]),
			{noreply, State}
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_call(_Msg, _From, State) ->
	{reply, not_implemented, State}.

handle_info({event, tx, {preparing_unblacklisting, TXID}}, State) ->
	#state{ sync_record = SyncRecord, retry_record = RetryRecord } = State,
	case ar_storage:get_tx_confirmation_data(TXID) of
		{ok, {Height, _BH}} ->
			?LOG_DEBUG([{event, mark_block_with_blacklisted_tx_for_resyncing},
					{tx, ar_util:encode(TXID)}, {height, Height}]),
			State2 = State#state{ sync_record = ar_intervals:delete(SyncRecord, Height,
					Height - 1), retry_record = ar_intervals:delete(RetryRecord, Height,
					Height - 1) },
			ok = store_sync_state(State2),
			ok = ar_kv:delete(?MODULE, << Height:256 >>),
			ar_events:send(tx, {ready_for_unblacklisting, TXID}),
			{noreply, State2};
		not_found ->
			ar_events:send(tx, {ready_for_unblacklisting, TXID}),
			{noreply, State};
		{error, Reason} ->
			?LOG_WARNING([{event, failed_to_read_tx_confirmation_index},
					{error, io_lib:format("~p", [Reason])}]),
			{noreply, State}
	end;

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info({event, disksup, {remaining_disk_space, "default", true, _Percentage, Bytes}},
		State) ->
	{ok, Config} = application:get_env(arweave, config),
	DiskPoolSize = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	DiskCacheSize = Config#config.disk_cache_size * 1048576,
	BufferSize = 10_000_000_000,
	case Bytes < DiskPoolSize + DiskCacheSize + BufferSize div 2 of
		true ->
			case State#state.is_disk_space_sufficient of
				true ->
					Msg = "~nThe node has stopped syncing headers. Add more disk space "
							"if you wish to store more block and transaction headers. "
							"The node will keep recording account tree updates and "
							"transaction confirmations - they do not take up a lot of "
							"space but you need to make sure the remaining disk space "
							"stays available for the node.~n~n"
							"The mining performance is not affected.~n",
					ar:console(Msg, []),
					?LOG_INFO([{event, ar_header_sync_stopped_syncing},
							{reason, insufficient_disk_space}]);
				false ->
					ok
			end,
			{noreply, State#state{ is_disk_space_sufficient = false }};
		false ->
			case Bytes > DiskPoolSize + DiskCacheSize + BufferSize of
				true ->
					case State#state.is_disk_space_sufficient of
						true ->
							ok;
						false ->
							Msg = "The available disk space has been detected, "
									"resuming header syncing.~n",
							ar:console(Msg, []),
							?LOG_INFO([{event, ar_header_sync_resumed_syncing}])
					end,
					{noreply, State#state{ is_disk_space_sufficient = true }};
				false ->
					{noreply, State}
			end
	end;
handle_info({event, disksup, _}, State) ->
	{noreply, State};

handle_info({'DOWN', _,  process, _, normal}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, noproc}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, Reason}, State) ->
	?LOG_WARNING([{event, header_sync_job_failed}, {reason, io_lib:format("~p", [Reason])},
			{action, spawning_another_one}]),
	gen_server:cast(?MODULE, process_item),
	{noreply, State};

handle_info({_Ref, _Atom}, State) ->
	%% Some older versions of Erlang OTP have a bug where gen_tcp:close may leak
	%% a message. https://github.com/ninenines/gun/issues/193,
	%% https://bugs.erlang.org/browse/ERL-1049.
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {message, Info}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, ar_header_sync_terminate}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

store_sync_state(State) ->
	#state{ sync_record = SyncRecord, height = LastHeight, block_index = BI } = State,
	SyncedCount = ar_intervals:sum(SyncRecord),
	prometheus_gauge:set(synced_blocks, SyncedCount),
	ets:insert(?MODULE, {synced_blocks, SyncedCount}),
	ar_storage:write_term(header_sync_state, {SyncRecord, LastHeight, BI}).

get_base_height([{H, _, _} | CurrentBI], CurrentHeight, RecentBI) ->
	case lists:search(fun({BH, _, _}) -> BH == H end, RecentBI) of
		false ->
			get_base_height(CurrentBI, CurrentHeight - 1, RecentBI);
		_ ->
			CurrentHeight
	end.

add_block(B, State) ->
	case check_fork(B#block.height, B#block.indep_hash, B#block.tx_root) of
		false ->
			{ok, State};
		true ->
			case B#block.height == 0 andalso ?NETWORK_NAME == "arweave.N.1" of
				true ->
					ok;
				false ->
					ar_data_sync:add_block(B, B#block.size_tagged_txs)
			end,
			add_block2(B, State)
	end.

add_block2(B, #state{ is_disk_space_sufficient = false } = State) ->
	case ar_storage:update_confirmation_index(B) of
		ok ->
			case ar_storage:update_reward_history(B) of
				ok ->
					{ok, State};
				Error ->
					?LOG_ERROR([{event, failed_to_record_reward_history_update},
							{reason, io_lib:format("~p", [Error])}]),
					{Error, State}
			end;
		Error ->
			?LOG_ERROR([{event, failed_to_record_block_confirmations},
				{reason, io_lib:format("~p", [Error])}]),
			{Error, State}
	end;
add_block2(B, #state{ sync_record = SyncRecord, retry_record = RetryRecord } = State) ->
	#block{ indep_hash = H, previous_block = PrevH, height = Height } = B,
	case ar_storage:write_full_block(B, B#block.txs) of
		ok ->
			case ar_intervals:is_inside(SyncRecord, Height) of
				true ->
					{ok, State};
				false ->
					ok = ar_kv:put(?MODULE, << Height:256 >>, term_to_binary({H, PrevH})),
					SyncRecord2 = ar_intervals:add(SyncRecord, Height, Height - 1),
					RetryRecord2 = ar_intervals:delete(RetryRecord, Height, Height - 1),
					{ok, State#state{ sync_record = SyncRecord2, retry_record = RetryRecord2 }}
			end;
		{error, Reason} ->
			?LOG_WARNING([{event, failed_to_store_block}, {block, ar_util:encode(H)},
					{height, Height}, {reason, Reason}]),
			{{error, Reason}, State}
	end.

%% @doc Return the latest height we have not synced or put in the retry queue yet.
%% Return 'nothing_to_sync' if everything is either synced or in the retry queue.
pick_unsynced_block(#state{ height = Height, sync_record = SyncRecord,
		retry_record = RetryRecord }) ->
	Union = ar_intervals:union(SyncRecord, RetryRecord),
	case ar_intervals:is_empty(Union) of
		true ->
			Height;
		false ->
			case ar_intervals:take_largest(Union) of
				{{End, _Start}, _Union2} when Height > End ->
					Height;
				{{_End, -1}, _Union2} ->
					nothing_to_sync;
				{{_End, Start}, _Union2} ->
					Start
			end
	end.

enqueue(Item, Queue) ->
	queue:in({Item, initial_backoff()}, Queue).

initial_backoff() ->
	{os:system_time(seconds), ?INITIAL_BACKOFF_INTERVAL_S}.

process_item(Queue) ->
	Now = os:system_time(second),
	case queue:out(Queue) of
		{empty, _Queue} ->
			ar_util:cast_after(?PROCESS_ITEM_INTERVAL_MS, ?MODULE, process_item),
			Queue;
		{{value, {Item, {BackoffTimestamp, _} = Backoff}}, Queue2}
				when BackoffTimestamp > Now ->
			ar_util:cast_after(?PROCESS_ITEM_INTERVAL_MS, ?MODULE, process_item),
			enqueue(Item, Backoff, Queue2);
		{{value, {{block, {H, H2, TXRoot, Height}}, Backoff}}, Queue2} ->
			case check_fork(Height, H, TXRoot) of
				false ->
					ok;
				true ->
					monitor(process, spawn(
						fun() ->
							process_flag(trap_exit, true),
							case download_block(H, H2, TXRoot) of
								{error, _Reason} ->
									gen_server:cast(?MODULE, {failed_to_get_block, H, H2,
											TXRoot, Height, Backoff});
								{ok, B} ->
									gen_server:cast(?MODULE, {add_historical_block, B, H, H2,
											TXRoot, Backoff})
							end
						end
					))
			end,
			Queue2
	end.

enqueue(Item, Backoff, Queue) ->
	queue:in({Item, Backoff}, Queue).

update_backoff({_Timestamp, Interval}) ->
	Interval2 = min(?MAX_BACKOFF_INTERVAL_S, Interval * 2),
	{os:system_time(second) + Interval2, Interval2}.

check_fork(Height, H, TXRoot) ->
	case Height < ar_fork:height_2_0() of
		true ->
			true;
		false ->
			case ar_node:get_block_index_entry(Height) of
				not_joined ->
					false;
				not_found ->
					false;
				{H, _WeaveSize, TXRoot} ->
					true;
				_ ->
					false
			end
	end.

download_block(H, H2, TXRoot) ->
	Peers = ar_peers:get_peers(),
	case ar_storage:read_block(H) of
		unavailable ->
			download_block(Peers, H, H2, TXRoot);
		B ->
			download_txs(Peers, B, TXRoot)
	end.

download_block(Peers, H, H2, TXRoot) ->
	Fork_2_0 = ar_fork:height_2_0(),
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		unavailable ->
			?LOG_WARNING([
				{event, ar_header_sync_failed_to_download_block_header},
				{block, ar_util:encode(H)}
			]),
			{error, block_header_unavailable};
		{Peer, #block{ height = Height } = B, Time, Size} ->
			BH =
				case Height >= Fork_2_0 of
					true ->
						ar_block:indep_hash(B);
					false ->
						ar_block:indep_hash(
							B#block{ tx_root = TXRoot, txs = lists:sort(B#block.txs) }
						)
				end,
			case BH of
				H when Height >= Fork_2_0 ->
					ar_peers:rate_fetched_data(Peer, block, Time, Size),
					download_txs(Peers, B, TXRoot);
				H2 when Height < Fork_2_0 ->
					ar_peers:rate_fetched_data(Peer, block, Time, Size),
					download_txs(Peers, B, TXRoot);
				_ ->
					?LOG_WARNING([
						{event, ar_header_sync_block_hash_mismatch},
						{block, ar_util:encode(H)},
						{peer, ar_util:format_peer(Peer)}
					]),
					{error, block_hash_mismatch}
			end
	end.

download_txs(Peers, B, TXRoot) ->
	case ar_http_iface_client:get_txs(Peers, B) of
		{ok, TXs} ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, B#block.height),
			SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
			{Root, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
			case Root of
				TXRoot ->
					{ok, B#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs }};
				_ ->
					?LOG_WARNING([
						{event, ar_header_sync_block_tx_root_mismatch},
						{block, ar_util:encode(B#block.indep_hash)}
					]),
					{error, block_tx_root_mismatch}
			end;
		{error, txs_exceed_block_size_limit} ->
			?LOG_WARNING([
				{event, ar_header_sync_block_txs_exceed_block_size_limit},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			{error, txs_exceed_block_size_limit};
		{error, txs_count_exceeds_limit} ->
			?LOG_WARNING([
				{event, ar_header_sync_block_txs_count_exceeds_limit},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			{error, txs_count_exceeds_limit};
		{error, tx_not_found} ->
			?LOG_WARNING([
				{event, ar_header_sync_block_tx_not_found},
				{block, ar_util:encode(B#block.indep_hash)}
			]),
			{error, tx_not_found}
	end.
