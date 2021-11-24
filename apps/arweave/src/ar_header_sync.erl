-module(ar_header_sync).

-behaviour(gen_server).

-export([start_link/0, join/2, add_tip_block/2, add_block/1, request_tx_removal/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_header_sync.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%%% This module syncs block and transaction headers and maintains a persisted record of synced
%%% headers. Headers are synced from latest to earliest. Includes a migration process that
%%% moves data to v2 index for blocks written prior to the 2.1 update.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Update the tip after the node joins the network.
join(BI, Blocks) ->
	gen_server:cast(?MODULE, {join, BI, Blocks}).

%% @doc Add a new tip block to the index and storage, record the new recent block index.
add_tip_block(B, RecentBI) ->
	gen_server:cast(?MODULE, {add_tip_block, B, RecentBI}).

%% @doc Add a block to the index and storage.
add_block(B) ->
	gen_server:cast(?MODULE, {add_block, B}).

%% @doc Remove the given transaction.
request_tx_removal(TXID) ->
	gen_server:cast(?MODULE, {remove_tx, TXID}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_header_sync_start}]),
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(tx),
	{ok, Config} = application:get_env(arweave, config),
	{ok, DB} = ar_kv:open("ar_header_sync_db"),
	{SyncRecord, LastHeight, CurrentBI} =
		case ar_storage:read_term(header_sync_state) of
			not_found ->
				{ar_intervals:new(), -1, []};
			{ok, StoredState} ->
				StoredState
		end,
	gen_server:cast(?MODULE, check_space_alarm),
	gen_server:cast(?MODULE, check_space),
	lists:foreach(
		fun(_) ->
			gen_server:cast(?MODULE, process_item)
		end,
		lists:seq(1, Config#config.header_sync_jobs)
	),
	gen_server:cast(?MODULE, store_sync_state),
	ets:insert(?MODULE, {synced_blocks, ar_intervals:sum(SyncRecord)}),
	{ok,
		#{
			db => DB,
			sync_record => SyncRecord,
			last_height => LastHeight,
			block_index => CurrentBI,
			queue => queue:new(),
			last_picked => LastHeight,
			disk_full => false,
			sync_disk_space => have_free_space()
		}}.

handle_cast({join, BI, Blocks}, State) ->
	#{
		db := DB,
		last_height := LastHeight,
		block_index := CurrentBI,
		sync_record := SyncRecord
	} = State,
	LastHeight2 = length(BI) - 1,
	State2 =
		State#{
			last_height => LastHeight2,
			block_index => lists:sublist(BI, ?HEADER_SYNC_TRACK_CONFIRMATIONS),
			last_picked => LastHeight2
		},
	State3 =
		case {CurrentBI, ar_util:get_block_index_intersection(BI, CurrentBI)} of
			{[], none} ->
				State2;
			{_CurrentBI, none} ->
				throw(last_stored_block_index_has_no_intersection_with_the_new_one);
			{_CurrentBI, {_Entry, Height}} ->
				S = State2#{ sync_record => ar_intervals:cut(SyncRecord, Height) },
				ok = store_sync_state(S),
				%% Delete from the kv store only after the sync record is saved - no matter
				%% what happens to the process, if a height is in the record, it must be present
				%% in the kv store.
				ok = ar_kv:delete_range(DB, << (Height + 1):256 >>, << (LastHeight + 1):256 >>),
				S
		end,
	State4 =
		lists:foldl(
			fun(B, S) ->
				ar_data_sync:add_block(B, B#block.size_tagged_txs),
				add_block(B, S)
			end,
			State3,
			Blocks
		),
	ok = store_sync_state(State4),
	{noreply, State4};

handle_cast({add_tip_block, #block{ height = Height } = B, RecentBI}, State) ->
	#{
		db := DB,
		sync_record := SyncRecord,
		block_index := CurrentBI,
		last_height := CurrentHeight
	} = State,
	BaseHeight = get_base_height(CurrentBI, CurrentHeight, RecentBI),
	State2 = State#{
		sync_record => ar_intervals:cut(SyncRecord, BaseHeight),
		block_index => RecentBI,
		last_height => Height
	},
	State3 = add_block(B, State2),
	case store_sync_state(State3) of
		ok ->
			%% Delete from the kv store only after the sync record is saved - no matter
			%% what happens to the process, if a height is in the record, it must be present
			%% in the kv store.
			ok = ar_kv:delete_range(DB, << (BaseHeight + 1):256 >>,
					<< (CurrentHeight + 1):256 >>),
			{noreply, State3#{ disk_full => false }};
		{error, enospc} ->
			{noreply, State#{ disk_full => true }}
	end;

handle_cast({add_historical_block, _}, #{ sync_disk_space := false } = State) ->
	{noreply, State};
handle_cast({add_historical_block, B}, State) ->
	State2 = add_block(B, State),
	{noreply, State2};

handle_cast({add_block, B}, State) ->
	State2 = add_block(B, State),
	{noreply, State2};

handle_cast(check_space_alarm, State) ->
	case have_free_space() of
		false ->
			Msg =
				"The node has stopped syncing headers - the available disk space is"
				" less than ~s. Add more disk space if you wish to store more headers.~n",
			ar:console(Msg, [ar_util:bytes_to_mb_string(?DISK_HEADERS_BUFFER_SIZE)]),
			?LOG_INFO([
				{event, ar_header_sync_stopped_syncing},
				{reason, little_disk_space_left}
			]);
		true ->
			ok
	end,
	ar_util:cast_after(?DISK_SPACE_WARNING_FREQUENCY, ?MODULE, check_space_alarm),
	{noreply, State};

handle_cast(check_space, State) ->
	ar_util:cast_after(ar_disksup:get_disk_space_check_frequency(), ?MODULE, check_space),
	case have_free_space() of
		true ->
			{noreply, State#{ sync_disk_space => true }};
		false ->
			{noreply, State#{ sync_disk_space => false }}
	end;

handle_cast(process_item, #{ sync_disk_space := false } = State) ->
	ar_util:cast_after(?CHECK_AFTER_SYNCED_INTERVAL_MS, ?MODULE, process_item),
	{noreply, State};
handle_cast(process_item, State) ->
	#{
		queue := Queue,
		sync_record := SyncRecord,
		last_picked := LastPicked,
		last_height := LastHeight
	} = State,
	prometheus_gauge:set(downloader_queue_size, queue:len(Queue)),
	UpdatedQueue = process_item(Queue),
	case pick_unsynced_block(LastPicked, SyncRecord) of
		nothing_to_sync ->
			LastPicked2 =
				case queue:is_empty(UpdatedQueue) of
					true ->
						LastHeight;
					false ->
						LastPicked
				end,
			{noreply, State#{ queue => UpdatedQueue, last_picked => LastPicked2 }};
		Height ->
			case ar_node:get_block_index_entry(Height) of
				not_joined ->
					{noreply, State#{ queue => UpdatedQueue }};
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
					{noreply, State#{
						queue => enqueue({block, {H, H2, TXRoot}}, UpdatedQueue),
						last_picked => Height
					}}
			end
	end;

handle_cast({failed_to_get_block, H, H2, TXRoot, Backoff}, #{ queue := Queue } = State) ->
	Backoff2 = update_backoff(Backoff),
	Queue2 = enqueue({block, {H, H2, TXRoot}}, Backoff2, Queue),
	gen_server:cast(?MODULE, process_item),
	{noreply, State#{ queue => Queue2 }};

handle_cast({remove_tx, TXID}, State) ->
	{ok, _Size} = ar_storage:delete_blacklisted_tx(TXID),
	ar_tx_blacklist:notify_about_removed_tx(TXID),
	{noreply, State};

handle_cast(store_sync_state, State) ->
	ar_util:cast_after(?STORE_HEADER_STATE_FREQUENCY_MS, ?MODULE, store_sync_state),
	case store_sync_state(State) of
		ok ->
			{noreply, State#{ disk_full => false }};
		{error, enospc} ->
			{noreply, State#{ disk_full => true }}
	end;

handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

handle_call(_Msg, _From, State) ->
	{reply, not_implemented, State}.

handle_info({event, tx, {preparing_unblacklisting, TXID}}, State) ->
	#{ db := DB, sync_record := SyncRecord } = State,
	case ar_storage:get_tx_confirmation_data(TXID) of
		{ok, {Height, _BH}} ->
			?LOG_DEBUG([{event, mark_block_with_blacklisted_tx_for_resyncing},
					{tx, ar_util:encode(TXID)}, {height, Height}]),
			State2 = State#{ sync_record => ar_intervals:delete(SyncRecord, Height,
					Height - 1) },
			ok = store_sync_state(State2),
			ok = ar_kv:delete(DB, << Height:256 >>),
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

handle_info({'DOWN', _,  process, _, normal}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, noproc}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, Reason}, State) ->
	?LOG_WARNING([
		{event, header_sync_job_failed},
		{reason, io_lib:format("~p", [Reason])},
		{action, spawning_another_one}
	]),
	gen_server:cast(?MODULE, process_item),
	{noreply, State};

handle_info({_Ref, _Atom}, State) ->
	%% Some older versions of Erlang OTP have a bug where gen_tcp:close may leak
	%% a message. https://github.com/ninenines/gun/issues/193,
	%% https://bugs.erlang.org/browse/ERL-1049.
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {message, Info}]),
	{noreply, State}.

terminate(Reason, State) ->
	?LOG_INFO([{event, ar_header_sync_terminate}, {reason, Reason}]),
	#{ db := DB } = State,
	ar_kv:close(DB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

store_sync_state(State) ->
	#{ sync_record := SyncRecord, last_height := LastHeight, block_index := BI } = State,
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

add_block(B, #{ sync_disk_space := false } = State) ->
	case ar_storage:update_confirmation_index(B) of
		ok ->
			ok;
		Error ->
			?LOG_ERROR([{event, failed_to_record_block_confirmations},
				{reason, io_lib:format("~p", [Error])}])
	end,
	State;
add_block(B, State) ->
	#{ db := DB, sync_record := SyncRecord } = State,
	#block{ indep_hash = H, previous_block = PrevH, height = Height } = B,
	case ar_storage:write_full_block(B, B#block.txs) of
		ok ->
			case ar_intervals:is_inside(SyncRecord, Height) of
				true ->
					State;
				false ->
					ok = ar_kv:put(DB, << Height:256 >>, term_to_binary({H, PrevH})),
					UpdatedSyncRecord = ar_intervals:add(SyncRecord, Height, Height - 1),
					State#{ sync_record => UpdatedSyncRecord }
			end;
		{error, Reason} ->
			?LOG_WARNING([
				{event, failed_to_store_block},
				{block, ar_util:encode(H)},
				{height, Height},
				{reason, Reason}
			]),
			State
	end.

have_free_space() ->
	ar_storage:get_free_space(".") > ?DISK_HEADERS_BUFFER_SIZE
		%% RocksDB and the chunk storage contain v1 data, which is part of the headers.
		andalso ar_storage:get_free_space(?ROCKS_DB_DIR) > ?DISK_HEADERS_BUFFER_SIZE
			andalso ar_storage:get_free_space(?CHUNK_DIR) > ?DISK_HEADERS_BUFFER_SIZE.

%% @doc Pick the biggest height smaller than LastPicked from outside the sync record.
pick_unsynced_block(LastPicked, SyncRecord) ->
	case ar_intervals:is_empty(SyncRecord) of
		true ->
			case LastPicked - 1 >= 0 of
				true ->
					LastPicked - 1;
				false ->
					nothing_to_sync
			end;
		false ->
			case ar_intervals:take_largest(SyncRecord) of
				{{_End, -1}, _SyncRecord2} ->
					nothing_to_sync;
				{{_End, Start}, SyncRecord2} when Start >= LastPicked ->
					pick_unsynced_block(LastPicked, SyncRecord2);
				{{End, _Start}, _SyncRecord2} when LastPicked - 1 > End ->
					LastPicked - 1;
				{{_End, Start}, _SyncRecord2} ->
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
		{{value, {Item, {BackoffTimestamp, _} = Backoff}}, UpdatedQueue}
				when BackoffTimestamp > Now ->
			ar_util:cast_after(?PROCESS_ITEM_INTERVAL_MS, ?MODULE, process_item),
			enqueue(Item, Backoff, UpdatedQueue);
		{{value, {{block, {H, H2, TXRoot}}, Backoff}}, UpdatedQueue} ->
			monitor(process, spawn(
				fun() ->
					case download_block(H, H2, TXRoot) of
						{error, _Reason} ->
							gen_server:cast(?MODULE,
									{failed_to_get_block, H, H2, TXRoot, Backoff});
						{ok, B} ->
							gen_server:cast(?MODULE, {add_historical_block, B}),
							ar_util:cast_after(?PROCESS_ITEM_INTERVAL_MS, ?MODULE, process_item)
					end
				end
			)),
			UpdatedQueue
	end.

enqueue(Item, Backoff, Queue) ->
	queue:in({Item, Backoff}, Queue).

update_backoff({_Timestamp, Interval}) ->
	UpdatedInterval = min(?MAX_BACKOFF_INTERVAL_S, Interval * 2),
	{os:system_time(second) + UpdatedInterval, UpdatedInterval}.

download_block(H, H2, TXRoot) ->
	Peers = ar_bridge:get_remote_peers(),
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
		{Peer, #block{ height = Height } = B} ->
			BH =
				case Height >= Fork_2_0 of
					true ->
						ar_weave:indep_hash(B);
					false ->
						ar_weave:indep_hash(
							B#block{ tx_root = TXRoot, txs = lists:sort(B#block.txs) }
						)
				end,
			case BH of
				H when Height >= Fork_2_0 ->
					download_txs(Peers, B, TXRoot);
				H2 when Height < Fork_2_0 ->
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
	case ar_http_iface_client:get_txs(Peers, #{}, B) of
		{ok, TXs} ->
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, B#block.height),
			SizeTaggedDataRoots =
				[{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
			{Root, _Tree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
			case Root of
				TXRoot ->
					case B#block.height == 0 andalso ?NETWORK_NAME == "arweave.N.1" of
						true ->
							{ok, B#block{ txs = TXs }};
						false ->
							ar_data_sync:add_block(B, SizeTaggedTXs),
							case move_data_to_v2_index(TXs) of
								ok ->
									{ok, B#block{ txs = TXs }};
								{error, Reason} = Error ->
									?LOG_WARNING([
										{event, ar_header_sync_failed_to_migrate_v1_txs},
										{block, ar_util:encode(B#block.indep_hash)},
										{reason, Reason}
									]),
									Error
							end
					end;
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

move_data_to_v2_index(TXs) ->
	%% Migrate the transaction data to the new index for blocks
	%% written prior to this update.
	lists:foldl(
		fun (#tx{ format = 2, data_size = DataSize } = TX, ok) when DataSize > 0 ->
				case ar_storage:read_tx_data(TX) of
					{error, enoent} ->
						ok;
					{ok, Data} ->
						case ar_storage:write_tx_data(
									no_expected_data_root,
									Data,
									write_to_free_space_buffer
								) of
							ok ->
								file:delete(ar_storage:tx_data_filepath(TX));
							Error ->
								Error
						end;
					Error ->
						Error
				end;
			(#tx{ format = 1, id = ID, data_size = DataSize } = TX, ok) when DataSize > 0 ->
				case ar_storage:lookup_tx_filename(ID) of
					unavailable ->
						ok;
					{migrated_v1, _} ->
						ok;
					{ok, _} ->
						case ar_storage:write_tx(TX) of
							ok ->
								case file:delete(ar_storage:tx_filepath(TX)) of
									{error, enoent} ->
										ok;
									ok ->
										ok;
									Error ->
										Error
								end;
							Error ->
								Error
						end
				end;
			(_, Acc) ->
				Acc
		end,
		ok,
		TXs
	).
