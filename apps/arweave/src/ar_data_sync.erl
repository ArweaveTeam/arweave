-module(ar_data_sync).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_cast/2, handle_call/3]).
-export([terminate/2]).

-export([
	add_block/3,
	add_chunk/1,
	add_data_root_to_disk_pool/3,
	maybe_drop_data_root_from_disk_pool/3,
	get_chunk/1,
	get_tx_data/1,
	get_tx_offset/1,
	get_sync_record_etf/0,
	get_sync_record_json/0,
	add_historical_block/1
]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

add_block(SizeTaggedTXs, BI, BlockStartOffset) ->
	gen_server:cast(?MODULE, {add_block, SizeTaggedTXs, BI, BlockStartOffset}).

add_chunk(
	#{
		data_root := DataRoot,
		offset := Offset,
		data_path := DataPath,
		chunk := Chunk,
		data_size := TXSize
	}
) ->
	gen_server:call(?MODULE, {add_chunk, DataRoot, DataPath, Chunk, Offset, TXSize}).

add_data_root_to_disk_pool(<<>>, _, _) ->
	ok;
add_data_root_to_disk_pool(_, 0, _) ->
	ok;
add_data_root_to_disk_pool(DataRoot, TXSize, TXID) ->
	gen_server:cast(?MODULE, {add_data_root_to_disk_pool, {DataRoot, TXSize, TXID}}).

maybe_drop_data_root_from_disk_pool(<<>>, _, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(_, 0, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, TXSize, TXID) ->
	gen_server:cast(?MODULE, {maybe_drop_data_root_from_disk_pool, {DataRoot, TXSize, TXID}}).

get_chunk(Offset) ->
	case catch ets:lookup(?MODULE, chunks_index) of
		[{_, ChunksIndex}] ->
			case catch ar_kv:get_next(ChunksIndex, << Offset:?OFFSET_KEY_BITSIZE >>) of
				{'EXIT', _} ->
					{error, not_joined};
				{error, _} ->
					{error, chunk_not_found};
				{ok, Key, Value} ->
					<< ChunkOffset:?OFFSET_KEY_BITSIZE >> = Key,
					{DataPathHash, TXRoot, _, TXPath, _, ChunkSize} = binary_to_term(Value),
					case ChunkOffset - Offset >= ChunkSize of
						true ->
							{error, chunk_not_found};
						false ->
							case ar_storage:read_chunk(DataPathHash) of
								{ok, {Chunk, DataPath}} ->
									Proof = #{
										tx_root => TXRoot,
										chunk => Chunk,
										data_path => DataPath,
										tx_path => TXPath
									},
									{ok, Proof};
								not_found ->
									{error, chunk_not_found};
								{error, Reason} ->
									ar:err([
										{event, failed_to_read_chunk},
										{reason, Reason}
									]),
									{error, failed_to_read_chunk}
							end
					end
			end;
		_ ->
			{error, not_joined}
	end.

get_tx_data(TXID) ->
	gen_server:call(?MODULE, {get_tx_data, TXID}).

get_tx_offset(TXID) ->
	gen_server:call(?MODULE, {get_tx_offset, TXID}).

get_sync_record_etf() ->
	gen_server:call(?MODULE, get_sync_record_etf).

get_sync_record_json() ->
	gen_server:call(?MODULE, get_sync_record_json).

add_historical_block(BH) ->
	gen_server:cast(?MODULE, {add_historical_block, BH}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ar:info([{event, ar_data_sync_start}]),
	process_flag(trap_exit, true),
	Opts = [
		{cache_index_and_filter_blocks, true},
		{bloom_filter_policy, 10}, % ~1% false positive probability
		{prefix_extractor, {capped_prefix_transform, 28}},
		{optimize_filters_for_hits, true},
		{max_open_files, 100000}
	],
	ColumnFamilies = [
		"default",
		"chunks_index",
		"data_root_index",
		"data_root_offset_index",
		"tx_index",
		"tx_offset_index",
		"disk_pool_chunks_index"
	],
	ColumnFamilyDescriptors = [{Name, Opts} || Name <- ColumnFamilies],
	case ar_meta_db:get(automatic_rocksdb_repair) of
		true ->
			case ar_kv:repair("ar_data_sync_db") of
				{error, E} ->
					ar:err([{event, ar_kv_repair_reported_error}, {error, E}]);
				ok ->
					repair_was_not_needed_or_was_successful
			end;
		_ ->
			do_not_attempt_to_repair
	end,
	{ok, DB, [_, CF1, CF2, CF3, CF4, CF5, CF6]} =
		ar_kv:open("ar_data_sync_db", ColumnFamilyDescriptors),
	ChunksIndex = {DB, CF1},
	DataRootIndex = {DB, CF2},
	DataRootOffsetIndex = {DB, CF3},
	TXIndex = {DB, CF4},
	TXOffsetIndex = {DB, CF5},
	DiskPoolChunksIndex = {DB, CF6},
	ets:new(?MODULE, [set, named_table, {read_concurrency, true}]),
	ets:insert(?MODULE, {chunks_index, ChunksIndex}),
	State = #sync_data_state{
		chunks_index = ChunksIndex,
		data_root_index = DataRootIndex,
		data_root_offset_index = DataRootOffsetIndex,
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_data_roots = #{},
		disk_pool_size = 0,
		block_index = not_set,
		weave_size = not_set,
		sync_record = ar_intervals:new(),
		peer_sync_records = #{},
		block_queue = queue:new(),
		disk_pool_cursor = first,
		status = not_joined
	},
	gen_server:cast(self(), init),
	case ar_storage:read_term(data_sync_state) of
		{ok, {SyncRecord, LastStoredBI, RawDiskPoolDataRoots, DiskPoolSize}} ->
			%% Filter out the keys with the invalid values, if any, produced by a bug in 2.1.0.0.
			DiskPoolDataRoots = maps:filter(
				fun (_, {_, _, _}) ->
						true;
					(_, _) ->
						false
				end,
				RawDiskPoolDataRoots
			),
			{ok, State#sync_data_state{
				disk_pool_data_roots = DiskPoolDataRoots,
				sync_record = SyncRecord,
				block_index = LastStoredBI,
				disk_pool_size = DiskPoolSize,
				weave_size = hd(LastStoredBI)
			}};
		_ ->
			{ok, State}
	end.

handle_cast(init, State) ->
	case whereis(http_entrypoint_node) of
		undefined ->
			timer:sleep(200),
			gen_server:cast(self(), init),
			{noreply, State};
		PID ->
			case ar_node:get_block_index(PID) of
				[] ->
					timer:sleep(200),
					gen_server:cast(self(), init),
					{noreply, State};
				BI ->
					do_init(BI, State)
			end
	end;

handle_cast({add_block, SizeTaggedTXs, BI, BlockStartOffset}, State) ->
	#sync_data_state{
		status = Status,
		block_queue = Q
	} = State,
	UpdatedQ = queue:in({SizeTaggedTXs, BI, BlockStartOffset}, Q),
	case {Status, queue:is_empty(Q)} of
		{joined, true} ->
			gen_server:cast(self(), process_block);
		_ ->
			will_be_scheduled_by_do_init_or_process_block
	end,
	{noreply, State#sync_data_state{ block_queue = UpdatedQ }};

handle_cast(process_block, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		sync_record = SyncRecord,
		block_queue = Q,
		weave_size = CurrentWeaveSize,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize
	} = State,
	{{value, {SizeTaggedTXs, BI, BlockStartOffset}}, UpdatedQ} = queue:out(Q),
	{BH, _, _} = hd(BI),
	EncodedBH = ar_util:encode(BH),
	ar:info([
		{event, ar_data_sync_add_block},
		{start_offset, BlockStartOffset},
		{block, EncodedBH}
	]),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	{ok, AddedDataRoots} = add_block_data_roots(State, SizeTaggedTXs, BlockStartOffset),
	UpdatedDiskPoolSize = sets:fold(
		fun(Key, Acc) -> Acc - element(1, maps:get(Key, DiskPoolDataRoots, {0, noop, noop})) end,
		DiskPoolSize,
		AddedDataRoots
	),
	UpdatedDiskPoolDataRoots =
		reset_orphaned_data_roots_disk_pool_timestamps(
			add_block_data_roots_to_disk_pool(DiskPoolDataRoots, AddedDataRoots),
			OrphanedDataRoots
		),
	ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset),
	WeaveSize = case SizeTaggedTXs of
		[] ->
			BlockStartOffset;
		_ ->
			{_, EndOffset} = lists:last(SizeTaggedTXs),
			BlockStartOffset + EndOffset
	end,
	UpdatedState = State#sync_data_state{
		weave_size = WeaveSize,
		sync_record = ar_intervals:cut(SyncRecord, BlockStartOffset),
		block_index = BI,
		block_queue = UpdatedQ,
		disk_pool_data_roots = UpdatedDiskPoolDataRoots,
		disk_pool_size = UpdatedDiskPoolSize
	},
	ok = store_sync_state(UpdatedState),
	ar:info([
		{event, ar_data_sync_added_block},
		{start_offset, BlockStartOffset},
		{block, EncodedBH}
	]),
	case queue:is_empty(UpdatedQ) of
		false ->
			gen_server:cast(self(), process_block);
		true ->
			will_be_scheduled_by_next_add_block
	end,
	{noreply, UpdatedState};

handle_cast({add_data_root_to_disk_pool, {DataRoot, TXSize, TXID}}, State) ->
	#sync_data_state{ disk_pool_data_roots = DiskPoolDataRoots } = State,
	Key = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	UpdatedDiskPoolDataRoots = maps:update_with(
		Key,
		fun ({_, _, not_set} = V) ->
				V;
			({S, T, TXIDSet}) ->
				{S, T, sets:add_element(TXID, TXIDSet)}
		end,
		{0, os:system_time(microsecond), sets:from_list([TXID])},
		DiskPoolDataRoots
	),
	{noreply, State#sync_data_state{ disk_pool_data_roots = UpdatedDiskPoolDataRoots }};

handle_cast({maybe_drop_data_root_from_disk_pool, {DataRoot, TXSize, TXID}}, State) ->
	#sync_data_state{
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize
	} = State,
	Key = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	{UpdatedDiskPoolDataRoots, UpdatedDiskPoolSize} = case maps:get(Key, DiskPoolDataRoots, not_found) of
		not_found ->
			{DiskPoolDataRoots, DiskPoolSize};
		{_, _, not_set} ->
			{DiskPoolDataRoots, DiskPoolSize};
		{Size, T, TXIDs} ->
			case sets:subtract(TXIDs, sets:from_list([TXID])) of
				TXIDs ->
					{DiskPoolDataRoots, DiskPoolSize};
				UpdatedSet ->
					case sets:size(UpdatedSet) of
						0 ->
							{maps:remove(Key, DiskPoolDataRoots), DiskPoolSize - Size};
						_ ->
							{DiskPoolDataRoots#{ Key => {Size, T, UpdatedSet} }, DiskPoolSize}
					end
			end
	end,
	{noreply, State#sync_data_state{
		disk_pool_data_roots = UpdatedDiskPoolDataRoots,
		disk_pool_size = UpdatedDiskPoolSize
	}};

handle_cast({add_historical_block, BH}, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex
	} = State,
	case ar_storage:read_block(BH) of
		unavailable ->
			ar:err([
				{event, ar_data_sync_did_not_find_block},
				{block, ar_util:encode(BH)}
			]),
			{noreply, State};
		B ->
			BlockStartOffset = B#block.weave_size - B#block.block_size,
			TXs = ar_storage:read_tx(B#block.txs),
			case lists:any(fun(TX) -> TX == unavailable end, TXs) of
				true ->
					ar:err([
						{event, ar_data_sync_did_not_find_block_txs},
						{block, ar_util:encode(BH)}
					]),
					{noreply, State};
				false ->
					SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
					{ok, _} = add_block_data_roots(State, SizeTaggedTXs, BlockStartOffset),
					ok = update_tx_index(
						TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset),
					{noreply, State}
			end
	end;

handle_cast(update_peer_sync_records, State) ->
	case whereis(http_bridge_node) of
		undefined ->
			timer:apply_after(200, gen_server, cast, [self(), update_peer_sync_records]);
		Bridge ->
			Peers = ar_bridge:get_remote_peers(Bridge),
			BestPeers = pick_random_peers(
				Peers,
				?CONSULT_PEER_RECORDS_COUNT,
				?PICK_PEERS_OUT_OF_RANDOM_N
			),
			Self = self(),
			spawn(
				fun() ->
					PeerSyncRecords = lists:foldl(
						fun(Peer, Acc) ->
							case ar_http_iface_client:get_sync_record(Peer) of
								{ok, SyncRecord} ->
									maps:put(Peer, SyncRecord, Acc);
								_ ->
									Acc
							end
						end,
						#{},
						BestPeers
					),
					gen_server:cast(Self, {update_peer_sync_records, PeerSyncRecords})
				end
			)
	end,
	{noreply, State};

handle_cast({update_peer_sync_records, PeerSyncRecords}, State) ->
	timer:apply_after(
		?PEER_SYNC_RECORDS_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), update_peer_sync_records]
	),
	{noreply, State#sync_data_state{
		peer_sync_records = PeerSyncRecords
	}};

%% Pick a random not synced interval and sync it.
handle_cast({sync_random_interval, RecentlyFailedPeers}, State) ->
	#sync_data_state{
		sync_record = SyncRecord,
		weave_size = WeaveSize,
		peer_sync_records = PeerSyncRecords
	} = State,
	FilteredPeerSyncRecords = maps:without(RecentlyFailedPeers, PeerSyncRecords),
	case get_random_interval(SyncRecord, FilteredPeerSyncRecords, WeaveSize) of
		none ->
			timer:apply_after(
				?PAUSE_AFTER_COULD_NOT_FIND_CHUNK_MS,
				gen_server,
				cast,
				[self(), {sync_random_interval, []}]
			),
			{noreply, State};
		{ok, {Peer, LeftBound, RightBound}} ->
			gen_server:cast(self(), {sync_chunk, Peer, LeftBound, RightBound}),
			{noreply, State#sync_data_state{ peer_sync_records = FilteredPeerSyncRecords }}
	end;

handle_cast({sync_chunk, _, LeftBound, RightBound}, State) when LeftBound >= RightBound ->
	gen_server:cast(self(), {sync_random_interval, []}),
	ok = store_sync_state(State),
	{noreply, State};
handle_cast({sync_chunk, Peer, LeftBound, RightBound}, State) ->
	Self = self(),
	spawn(
		fun() ->
			case ar_http_iface_client:get_chunk(Peer, LeftBound + 1) of
				{ok, Proof} ->
					gen_server:cast(
						Self,
						{store_fetched_chunk, Peer, LeftBound, RightBound, Proof}
					);
				{error, _} ->
					gen_server:cast(Self, {sync_random_interval, [Peer]})
			end
		end
	),
	{noreply, State};

handle_cast({store_fetched_chunk, Peer, _, _, #{ data_path := Path, chunk := Chunk }}, State)
		when ?IS_CHUNK_PROOF_RATIO_NOT_ATTRACTIVE(Chunk, Path) ->
	#sync_data_state{
		peer_sync_records = PeerSyncRecords
	} = State,
	gen_server:cast(self(), {sync_random_interval, []}),
	{noreply, State#sync_data_state{
		peer_sync_records = maps:remove(Peer, PeerSyncRecords)
	}};
handle_cast({store_fetched_chunk, Peer, LeftBound, RightBound, Proof}, State) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex,
		peer_sync_records = PeerSyncRecords
	} = State,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk } = Proof,
	{ok, Key, Value} = ar_kv:get_prev(DataRootOffsetIndex, << LeftBound:?OFFSET_KEY_BITSIZE >>),
	<< BlockStartOffset:?OFFSET_KEY_BITSIZE >> = Key,
	{TXRoot, BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
	Offset = LeftBound - BlockStartOffset,
	case validate_proof(TXRoot, TXPath, DataPath, Offset, Chunk, BlockSize) of
		false ->
			gen_server:cast(self(), {sync_random_interval, []}),
			{noreply, State#sync_data_state{
				peer_sync_records = maps:remove(Peer, PeerSyncRecords)
			}};
		{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize} ->
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
			case sets:is_element(DataRootKey, DataRootIndexKeySet) of
				true ->
					do_not_update_data_root_index;
				false ->
					UpdatedValue =
						term_to_binary({
							TXRoot,
							BlockSize,
							sets:add_element(DataRootKey, DataRootIndexKeySet)
						}),
					ok = ar_kv:put(
						DataRootOffsetIndex,
						Key,
						UpdatedValue
					),
					ok = ar_kv:put(
						DataRootIndex,
						DataRootKey,
						term_to_binary(
							#{ TXRoot => #{ AbsoluteTXStartOffset => TXPath } }
						)
					)
			end,
			ChunkSize = byte_size(Chunk),
			gen_server:cast(self(), {sync_chunk, Peer, LeftBound + ChunkSize, RightBound}),
			case store_chunk(
				State,
				AbsoluteTXStartOffset + ChunkEndOffset,
				ChunkEndOffset,
				TXRoot,
				DataRoot,
				DataPath,
				Chunk,
				TXPath,
				TXSize
			) of
				{updated, UpdatedState} ->
					{noreply, UpdatedState};
				_ ->
					{noreply, State}
			end
	end;

handle_cast(process_disk_pool_item, State) ->
	#sync_data_state{
		disk_pool_cursor = Cursor,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	case ar_kv:cyclic_iterator_move(DiskPoolChunksIndex, Cursor) of
		{ok, Key, Value, NextCursor} ->
			timer:apply_after(
				case NextCursor of
					first ->
						?DISK_POOL_SCAN_FREQUENCY_MS;
					_ ->
						0
				end,
				gen_server,
				cast,
				[self(), process_disk_pool_item]
			),
			process_disk_pool_item(State, Key, Value, NextCursor);
		none ->
			%% Since we are not persisting the number of chunks in the disk pool metric on
			%% every update, it can go out of sync with the actual value. Here is one place
			%% where we can reset it if we detect the whole column has been traversed.
			prometheus_gauge:set(disk_pool_chunks_count, 0),
			timer:apply_after(
				?DISK_POOL_SCAN_FREQUENCY_MS,
				gen_server,
				cast,
				[self(), process_disk_pool_item]
			),
			{noreply, State}
	end;

handle_cast(update_disk_pool_data_roots, State) ->
	#sync_data_state{ disk_pool_data_roots = DiskPoolDataRoots } = State,
	Now = os:system_time(microsecond),
	UpdatedDiskPoolDataRoots = maps:filter(
		fun(_, {_, Timestamp, _}) ->
			Timestamp + ar_meta_db:get(disk_pool_data_root_expiration_time_us) > Now
		end,
		DiskPoolDataRoots
	),
	DiskPoolSize = maps:fold(
		fun(_, {Size, _, _}, Acc) ->
			Acc + Size
		end,
		0,
		UpdatedDiskPoolDataRoots
	),
	prometheus_gauge:set(pending_chunks_size, DiskPoolSize),
	timer:apply_after(
		?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS,
		gen_server,
		cast,
		[self(), update_disk_pool_data_roots]
	),
	{noreply, State#sync_data_state{
		disk_pool_data_roots = UpdatedDiskPoolDataRoots,
		disk_pool_size = DiskPoolSize
	}}.

handle_call({add_chunk, DataRoot, DataPath, Chunk, Offset, TXSize}, _From, State) ->
	#sync_data_state{
		data_root_index = DataRootIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_size = DiskPoolSize
	} = State,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ar_kv:get(DataRootIndex, DataRootKey) of
		not_found ->
			case maps:get(DataRootKey, DiskPoolDataRoots, not_found) of
				not_found ->
					{reply, {error, data_root_not_found}, State};
				{Size, Timestamp, TXIDSet} ->
					DataRootLimit =
						ar_meta_db:get(max_disk_pool_data_root_buffer_mb) * 1024 * 1024,
					DiskPoolLimit = ar_meta_db:get(max_disk_pool_buffer_mb) * 1024 * 1024,
					ChunkSize = byte_size(Chunk),
					case Size + ChunkSize > DataRootLimit
							orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
						true ->
							{reply, {error, exceeds_disk_pool_size_limit}, State};
						false ->
							case
								validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk)
							of
								false ->
									{reply, {error, invalid_proof}, State};
								{true, EndOffset} ->
									DataPathHash = crypto:hash(sha256, DataPath),
									Key = << Timestamp:256, DataPathHash/binary >>,
									V = term_to_binary(
										{EndOffset, ChunkSize, DataRoot, TXSize}
									),
									case ar_kv:get(DiskPoolChunksIndex, Key) of
										not_found ->
											ok = ar_kv:put(DiskPoolChunksIndex, Key, V),
											prometheus_gauge:inc(disk_pool_chunks_count),
											ok = write_chunk(DataPathHash, Chunk, DataPath),
											UpdatedDiskPoolDataRooots =
												maps:put(
													DataRootKey,
													{Size + ChunkSize, Timestamp, TXIDSet},
													DiskPoolDataRoots
												),
											UpdatedState = State#sync_data_state{
												disk_pool_size = DiskPoolSize + ChunkSize,
												disk_pool_data_roots = UpdatedDiskPoolDataRooots
											},
											{reply, ok, UpdatedState};
										_ ->
											{reply, ok, State}
									end
							end
					end
			end;
		{ok, Value} ->
			case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
				false ->
					{reply, {error, invalid_proof}, State};
				{true, EndOffset} ->
					store_chunk(
						State,
						data_root_index_iterator(binary_to_term(Value)),
						DataRoot,
						DataPath,
						Chunk,
						EndOffset,
						TXSize
					)
			end
	end;

handle_call({get_tx_data, TXID}, _From, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		chunks_index = ChunksIndex
	} = State,
	case ar_kv:get(TXIndex, TXID) of
		not_found ->
			{reply, {error, not_found}, State};
		{error, Reason} ->
			ar:err([{event, failed_to_get_tx_data}, {reason, Reason}]),
			{reply, {error, failed_to_get_tx_data}, State};
		{ok, Value} ->
			{Offset, Size} = binary_to_term(Value),
			case Size > ?MAX_SERVED_TX_DATA_SIZE of
				true ->
					{reply, {error, tx_data_too_big}, State};
				false ->
					StartKey = << (Offset - Size):?OFFSET_KEY_BITSIZE >>,
					EndKey = << Offset:?OFFSET_KEY_BITSIZE >>,
					case ar_kv:get_range(ChunksIndex, StartKey, EndKey) of
						{error, Reason} ->
							ar:err([
								{event, failed_to_get_chunks_for_tx_data},
								{reason, Reason}
							]),
							{reply, {error, not_found}, State};
						{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
							{reply, {error, not_found}, State};
						{ok, Map} ->
							{reply, get_tx_data_from_chunks(Offset, Size, Map), State}
					end
			end
	end;

handle_call({get_tx_offset, TXID}, _From, State) ->
	#sync_data_state{
		tx_index = TXIndex
	} = State,
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{reply,  {ok, binary_to_term(Value)}, State};
		not_found ->
			{reply, {error, not_found}, State};
		{error, Reason} ->
			ar:err([{event, failed_to_read_tx_offset}, {reason, Reason}]),
			{reply, {error, failed_to_read_offset}, State}
	end;

handle_call(get_sync_record_etf, _From, #sync_data_state{ sync_record = SyncRecord } = State) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	{reply, {ok, ar_intervals:to_etf(SyncRecord, Limit)}, State};

handle_call(get_sync_record_json, _From, #sync_data_state{ sync_record = SyncRecord } = State) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	{reply, {ok, ar_intervals:to_json(SyncRecord, Limit)}, State}.

-ifdef(DEBUG).
terminate(_Reason, State) ->
	#sync_data_state{
		chunks_index = {DB, _}
	} = State,
	ar_kv:close(DB),
	ar_storage:delete_term(data_sync_state),
	ar_kv:destroy("ar_data_sync_db").
-else.
terminate(Reason, State) ->
	ar:info([{event, ar_data_sync_terminate}, {reason, Reason}]),
	#sync_data_state{
		chunks_index = {DB, _}
	} = State,
	ar_kv:close(DB).
-endif.

%%%===================================================================
%%% Private functions.
%%%===================================================================

do_init([{_, WeaveSize, _} | _] = BI, State) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		sync_record = SyncRecord,
		block_index = LastStoredBI,
		disk_pool_data_roots = DiskPoolDataRoots,
		block_queue = BlockQueue
	} = State,
	State2 = State#sync_data_state{
		weave_size = WeaveSize,
		block_index = lists:sublist(BI, ?TRACK_CONFIRMATIONS),
		status = joined
	},
	{State3, NewBI, CurrentWeaveSize} =
		case LastStoredBI of
			not_set ->
				{State2, BI, 0};
			_ ->
				case get_intersection(BI, LastStoredBI) of
					{ok, full_intersection, ExtraBI} ->
						{State2, ExtraBI, element(2, hd(LastStoredBI))};
					{ok, no_intersection} ->
						throw(last_stored_block_index_has_no_intersection_with_the_new_one);
					{ok, Offset, ExtraBI} ->
						PreviousWeaveSize = element(2, hd(LastStoredBI)),
						{ok, OrphanedDataRoots} =
							remove_orphaned_data(State, Offset, PreviousWeaveSize),
						UpdatedDiskPoolDataRoots =
							reset_orphaned_data_roots_disk_pool_timestamps(
								DiskPoolDataRoots,
								OrphanedDataRoots
							),
						{State2#sync_data_state{
							sync_record = ar_intervals:cut(SyncRecord, Offset),
							disk_pool_data_roots = UpdatedDiskPoolDataRoots
						}, ExtraBI, Offset}
				end
		end,
	ok = data_root_offset_index_from_block_index(DataRootOffsetIndex, NewBI, CurrentWeaveSize),
	ok = store_sync_state(State3),
	gen_server:cast(self(), update_peer_sync_records),
	gen_server:cast(self(), {sync_random_interval, []}),
	gen_server:cast(self(), update_disk_pool_data_roots),
	gen_server:cast(self(), process_disk_pool_item),
	case queue:is_empty(BlockQueue) of
		false ->
			gen_server:cast(self(), process_block);
		true ->
			will_be_scheduled_by_next_add_block
	end,
	{noreply, State3}.

get_intersection(BI, [{BH, _, _} | LastStoredBI]) ->
	case block_index_contains_block(BI, BH) of
		true ->
			{ok, full_intersection, lists:takewhile(fun({H, _, _}) -> H /= BH end, BI)};
		false ->
			get_intersection2(BI, LastStoredBI)
	end.

block_index_contains_block([{BH, _, _} | _], BH) ->
	true;
block_index_contains_block([_ | BI], BH) ->
	block_index_contains_block(BI, BH);
block_index_contains_block([], _BH) ->
	false.

get_intersection2(BI, [{BH, WeaveSize, _} | LastStoredBI]) ->
	case block_index_contains_block(BI, BH) of
		true ->
			{ok, WeaveSize, lists:takewhile(fun({H, _, _}) -> H /= BH end, BI)};
		false ->
			get_intersection2(BI, LastStoredBI)
	end;
get_intersection2(_BI, []) ->
	{ok, no_intersection}.

data_root_offset_index_from_block_index(Index, BI, StartOffset) ->
	data_root_offset_index_from_reversed_block_index(Index, lists:reverse(BI), StartOffset).

data_root_offset_index_from_reversed_block_index(
	Index,
	[{_, Offset, _} | BI],
	Offset
) ->
	data_root_offset_index_from_reversed_block_index(Index, BI, Offset);
data_root_offset_index_from_reversed_block_index(
	Index,
	[{_, WeaveSize, TXRoot} | BI],
	StartOffset
) ->
	case ar_kv:put(
		Index,
		<< StartOffset:?OFFSET_KEY_BITSIZE >>,
		term_to_binary({TXRoot, WeaveSize - StartOffset, sets:new()})
	) of
		ok ->
			data_root_offset_index_from_reversed_block_index(Index, BI, WeaveSize);
		Error ->
			Error
	end;
data_root_offset_index_from_reversed_block_index(_Index, [], _StartOffset) ->
	ok.	

remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
	ok = remove_orphaned_txs(State, BlockStartOffset, WeaveSize),
	{ok, OrphanedDataRoots} = remove_orphaned_data_roots(State, BlockStartOffset),
	ok = remove_orphaned_data_root_offsets(State, BlockStartOffset, WeaveSize),
	ok = remove_orphaned_chunks(State, BlockStartOffset, WeaveSize),
	{ok, OrphanedDataRoots}.

remove_orphaned_txs(State, BlockStartOffset, WeaveSize) ->
	#sync_data_state{
		tx_offset_index = TXOffsetIndex,
		tx_index = TXIndex
	} = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			ok;
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, TXID, ok) ->
						ar_kv:delete(TXIndex, TXID)
				end,
				ok,
				Map
			);
		Error ->
			Error
	end,
	ar_kv:delete_range(
		TXOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>
	).

remove_orphaned_chunks(State, BlockStartOffset, WeaveSize) ->
	#sync_data_state{ chunks_index = ChunksIndex } = State,
	EndKey = << (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>,
	ar_kv:delete_range(ChunksIndex, << (BlockStartOffset + 1):?OFFSET_KEY_BITSIZE >>, EndKey).

remove_orphaned_data_roots(State, BlockStartOffset) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex
	} = State,
	case ar_kv:get_range(DataRootOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, sets:new()};
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, {ok, OrphanedDataRoots}) ->
						{TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
						sets:fold(
							fun (_Key, {error, _} = Error) ->
									Error;
								(Key, {ok, Orphaned}) ->
									case remove_orphaned_data_root(
										DataRootIndex,
										Key,
										TXRoot,
										BlockStartOffset
									) of
										removed ->
											{ok, sets:add_element(Key, Orphaned)};
										ok ->
											{ok, Orphaned};
										Error ->
											Error
									end
							end,
							{ok, OrphanedDataRoots},
							DataRootIndexKeySet
						)
				end,
				{ok, sets:new()},
				Map
			);	
		Error ->
			Error
	end.

remove_orphaned_data_root(DataRootIndex, DataRootKey, TXRoot, StartOffset) ->
	case ar_kv:get(DataRootIndex, DataRootKey) of
		not_found ->
			ok;
		{ok, Value} ->
			TXRootMap = binary_to_term(Value),
			case maps:is_key(TXRoot, TXRootMap) of
				false ->
					ok;
				true ->
					OffsetMap = maps:get(TXRoot, TXRootMap),
					UpdatedTXRootMap = case maps:filter(
						fun(TXStartOffset, _) ->
							TXStartOffset < StartOffset
						end,
						OffsetMap
					) of
						EmptyMap when map_size(EmptyMap) == 0 ->
							maps:remove(TXRoot, TXRootMap);
						UpdatedOffsetMap ->
							maps:put(TXRoot, UpdatedOffsetMap, TXRootMap)
					end,
					case UpdatedTXRootMap of
						EmptyTXRootMap when map_size(EmptyTXRootMap) == 0 ->
							case ar_kv:delete(DataRootIndex, DataRootKey) of
								ok ->
									removed;
								Error ->
									Error
							end;
						_ ->
							ar_kv:put(
								DataRootIndex,
								DataRootKey,
								term_to_binary(UpdatedTXRootMap)
							)
					end
			end
	end.

remove_orphaned_data_root_offsets(State, BlockStartOffset, WeaveSize) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	ar_kv:delete_range(
		DataRootOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>
	).

update_tx_index(_TXIndex, _TXOffsetIndex, [], _BlockStartOffset) ->
	ok;
update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset) ->
	lists:foldl(
		fun ({_, Offset}, Offset) ->
				Offset;
			({{TXID, _}, TXEndOffset}, PreviousOffset) ->
				AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
				TXSize = TXEndOffset - PreviousOffset,
				AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
				case ar_kv:put(
					TXOffsetIndex,
					<< AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>,
					TXID
				) of
					ok ->
						case ar_kv:put(
							TXIndex,
							TXID,
							term_to_binary({AbsoluteEndOffset, TXSize})
						) of
							ok ->
								TXEndOffset;
							{error, Reason} ->
								ar:err([{event, failed_to_update_tx_index}, {reason, Reason}]),
								TXEndOffset
						end;
					{error, Reason} ->
						ar:err([{event, failed_to_update_tx_offset_index}, {reason, Reason}]),
						TXEndOffset
				end
		end,
		0,
		SizeTaggedTXs
	),
	ok.

add_block_data_roots(_State, [], _CurrentWeaveSize) ->
	{ok, sets:new()};
add_block_data_roots(State, SizeTaggedTXs, CurrentWeaveSize) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, DataRootIndexKeySet} = lists:foldl(
		fun ({_DataRoot, Offset}, {Offset, _} = Acc) ->
				Acc;
			({DataRoot, TXEndOffset}, {TXStartOffset, CurrentDataRootSet}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = CurrentWeaveSize + TXStartOffset,
				TXSize = TXEndOffset - TXStartOffset,
				DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
				ok = update_data_root_index(
					State,
					DataRootKey,
					TXRoot,
					TXOffset,
					TXPath
				),
				{TXEndOffset, sets:add_element(DataRootKey, CurrentDataRootSet)}
		end,
		{0, sets:new()},
		SizeTaggedDataRoots
	),
	case BlockSize > 0 of
		true ->
			ok = ar_kv:put(
				DataRootOffsetIndex,
				<< CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
				term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})
			);
		false ->
			do_not_update_data_root_offset_index
	end,
	{ok, DataRootIndexKeySet}.

update_data_root_index(State, DataRootKey, TXRoot, AbsoluteTXStartOffset, TXPath) ->
	#sync_data_state{
		data_root_index = DataRootIndex
	} = State,
	TXRootMap = case ar_kv:get(DataRootIndex, DataRootKey) of
		not_found ->
			#{};
		{ok, Value} ->
			binary_to_term(Value)
	end,
	OffsetMap = maps:get(TXRoot, TXRootMap, #{}),
	UpdatedValue = term_to_binary(
		TXRootMap#{ TXRoot => OffsetMap#{ AbsoluteTXStartOffset => TXPath } }
	),
	ar_kv:put(DataRootIndex, DataRootKey, UpdatedValue).

add_block_data_roots_to_disk_pool(DataRoots, DataRootIndexKeySet) ->
	{U, _} = sets:fold(
		fun(R, {M, T}) ->
			{maps:update_with(
				R,
				fun({_, Timeout, _}) -> {0, Timeout, not_set} end,
				{0, T, not_set},
				M
			), T + 1}
		end,
		{DataRoots, os:system_time(microsecond)},
		DataRootIndexKeySet
	),
	U.

reset_orphaned_data_roots_disk_pool_timestamps(DataRoots, DataRootIndexKeySet) ->
	{U, _} = sets:fold(
		fun(R, {M, T}) ->
			{maps:update_with(
				R,
				fun({Size, _, TXIDSet}) -> {Size, T, TXIDSet} end,
				{0, T, not_set},
				M
			), T + 1}
		end,
		{DataRoots, os:system_time(microsecond)},
		DataRootIndexKeySet
	),
	U.

update_chunks_index(
	ChunksIndex,
	DiskPoolChunksIndex,
	DiskPoolDataRoots,
	TXSize,
	SyncRecord,
	AbsoluteChunkOffset,
	ChunkOffset,
	DataPathHash,
	TXRoot,
	DataRoot,
	TXPath,
	ChunkSize
) ->
	case ar_intervals:is_inside(SyncRecord, AbsoluteChunkOffset) of
		true ->
			not_updated;
		false ->
			Value = {DataPathHash, TXRoot, DataRoot, TXPath, ChunkOffset, ChunkSize},
			Key = << AbsoluteChunkOffset:?OFFSET_KEY_BITSIZE >>,
			case ar_kv:put(ChunksIndex, Key, term_to_binary(Value)) of
				ok ->
					DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
					case maps:get(DataRootKey, DiskPoolDataRoots, not_found) of
						{_, Timestamp, _} ->
							DiskPoolChunkKey = << Timestamp:256, DataPathHash/binary >>,
							case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
								not_found ->
									ok = ar_kv:put(
										DiskPoolChunksIndex,
										DiskPoolChunkKey,
										term_to_binary(
											{ChunkOffset, ChunkSize, DataRoot, TXSize}
										)
									),
									prometheus_gauge:inc(disk_pool_chunks_count);
								_ ->
									do_not_update_disk_pool
							end;
						not_found ->
							do_not_update_disk_pool
					end,
					StartOffset = AbsoluteChunkOffset - ChunkSize,
					{ok, ar_intervals:add(SyncRecord, AbsoluteChunkOffset, StartOffset)};
				{error, Reason} ->
					ar:err([
						{event, failed_to_update_chunk_index},
						{reason, Reason}
					]),
					{error, Reason}
			end
	end.

store_sync_state(
	#sync_data_state{
		sync_record = SyncRecord,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		block_index = BI
	}
) ->
	prometheus_gauge:set(v2_index_data_size, ar_intervals:sum(SyncRecord)),
	ar_metrics:store(disk_pool_chunks_count),
	ar_storage:write_term(data_sync_state, {SyncRecord, BI, DiskPoolDataRoots, DiskPoolSize}).

pick_random_peers(Peers, N, M) ->
	lists:sublist(
		lists:sort(fun(_, _) -> rand:uniform() > 0.5 end, lists:sublist(Peers, M)),
		N
	).

get_random_interval(SyncRecord, PeerSyncRecords, WeaveSize) ->
	%% Try keeping no more than ?MAX_SHARED_SYNCED_INTERVALS_COUNT intervals
	%% in the sync record by choosing the appropriate size of continuous
	%% intervals to sync. The motivation is to keep the record size small for
	%% low traffic overhead. When the size increases ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	%% a random subset of the intervals is served to peers.
	SyncSize = WeaveSize div ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	maps:fold(
		fun (_, _, {ok, {Peer, L, R}}) ->
				{ok, {Peer, L, R}};
			(Peer, PeerSyncRecord, none) ->
				PeerSyncRecordBelowWeaveSize = ar_intervals:cut(PeerSyncRecord, WeaveSize),
				I = ar_intervals:outerjoin(SyncRecord, PeerSyncRecordBelowWeaveSize),
				Sum = ar_intervals:sum(I),
				case Sum of
					0 ->
						none;
					_ ->
						RelativeByte = rand:uniform(Sum) - 1,
						{L, Byte, R} =
							ar_intervals:get_interval_by_nth_inner_number(I, RelativeByte),
						LeftBound = max(L, Byte - SyncSize div 2),
						{ok, {Peer, LeftBound, min(R, LeftBound + SyncSize div 2)}}
				end
		end,
		none,
		PeerSyncRecords
	).

validate_proof(_, _, _, _, Chunk, _) when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
	false;
validate_proof(TXRoot, TXPath, DataPath, Offset, Chunk, BlockSize) ->
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			ChunkOffset = Offset - TXStartOffset,
			TXSize = TXEndOffset - TXStartOffset,
			case ar_merkle:validate_path(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					case ar_tx:generate_chunk_id(Chunk) == ChunkID of
						false ->
							false;
						true ->
							case ChunkEndOffset - ChunkStartOffset == byte_size(Chunk) of
								true ->
									{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize};
								false ->
									false
							end
					end
			end
	end.

validate_data_path(_, _, _, _, Chunk) when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
	false;
validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	case ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath) of
		false ->
			false;
		{ChunkID, StartOffset, EndOffset} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					case EndOffset - StartOffset == byte_size(Chunk) of
						true ->
							{true, EndOffset};
						false ->
							false
					end
			end
	end.

store_chunk(State, DataRootIndexIterator, DataRoot, DataPath, Chunk, EndOffset, TXSize) ->
	case next(DataRootIndexIterator) of
		none ->
			{reply, ok, State};
		{{TXRoot, TXStartOffset, TXPath}, UpdatedDataRootIndexIterator} ->
			AbsoluteEndOffset = TXStartOffset + EndOffset,
			case store_chunk(
				State,
				AbsoluteEndOffset,
				EndOffset,
				TXRoot,
				DataRoot,
				DataPath,
				Chunk,
				TXPath,
				TXSize
			) of
				{updated, UpdatedState} ->
					ok = store_sync_state(UpdatedState),
					store_chunk(
						UpdatedState,
						UpdatedDataRootIndexIterator,
						DataRoot,
						DataPath,
						Chunk,
						EndOffset,
						TXSize
					);
				not_updated ->
					store_chunk(
						State,
						UpdatedDataRootIndexIterator,
						DataRoot,
						DataPath,
						Chunk,
						EndOffset,
						TXSize
					);
				{error, _} ->
					{reply, {error, failed_to_store_chunk}, State}
			end
	end.

store_chunk(
	State,
	AbsoluteEndOffset,
	ChunkOffset,
	TXRoot,
	DataRoot,
	DataPath,
	Chunk,
	TXPath,
	TXSize
) ->
	#sync_data_state{
		chunks_index = ChunksIndex,
		sync_record = SyncRecord,
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_data_roots = DiskPoolDataRoots
	} = State,
	DataPathHash = crypto:hash(sha256, DataPath),
	case update_chunks_index(
		ChunksIndex,
		DiskPoolChunksIndex,
		DiskPoolDataRoots,
		TXSize,
		SyncRecord,
		AbsoluteEndOffset,
		ChunkOffset,
		DataPathHash,
		TXRoot,
		DataRoot,
		TXPath,
		byte_size(Chunk)
	) of
		not_updated ->
			not_updated;
		{error, _Reason} = Error ->
			Error;
		{ok, UpdatedSyncRecord} ->
			case write_chunk(DataPathHash, Chunk, DataPath) of
				{error, _Reason} = Error ->
					Error;
				ok ->
					UpdatedState = State#sync_data_state{
						sync_record = UpdatedSyncRecord
					},
					{updated, UpdatedState}
			end
	end.

write_chunk(DataPathHash, Chunk, DataPath) ->
	case ar_storage:has_chunk(DataPathHash) of
		true ->
			%% The chunk may be already stored because the same chunk
			%% might be inside different transactions and different
			%% blocks.
			ok;
		false ->
			ar_storage:write_chunk(DataPathHash, Chunk, DataPath)
	end.

process_disk_pool_item(State, Key, Value, NextCursor) ->
	#sync_data_state{
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		data_root_index = DataRootIndex,
		chunks_index = ChunksIndex,
		sync_record = SyncRecord
	} = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	{Offset, Size, DataRoot, TXSize} = binary_to_term(Value),
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex = ar_kv:get(DataRootIndex, DataRootKey),
	InDiskPool = maps:is_key(DataRootKey, DiskPoolDataRoots),
	case {InDataRootIndex, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% the all chunks belonging to the same data root for now.
			{noreply, State#sync_data_state{
				disk_pool_cursor = {seek, << (Timestamp + 1):256 >>}
			}};
		{not_found, false} ->
			ok = ar_kv:delete(DiskPoolChunksIndex, Key),
			prometheus_gauge:dec(disk_pool_chunks_count),
			ar_storage:delete_chunk(DataPathHash),
			{noreply, State#sync_data_state{ disk_pool_cursor = NextCursor }};
		{{ok, DataRootIndexValue}, _} ->
			{IsUpdated, UpdatedSyncRecord} = maps:fold(
				fun(TXRoot, OffsetMap, Acc) ->
					maps:fold(
						fun(AbsoluteTXStartOffset, TXPath, {_, SR} = Acc2) ->
							AbsoluteChunkOffset = AbsoluteTXStartOffset + Offset,
							case update_chunks_index(
								ChunksIndex,
								DiskPoolChunksIndex,
								DiskPoolDataRoots,
								TXSize,
								SR,
								AbsoluteChunkOffset,
								Offset,
								DataPathHash,
								TXRoot,
								DataRoot,
								TXPath,
								Size
							) of
								not_updated ->
									Acc2;
								{ok, UpdatedSR} ->
									{updated, UpdatedSR}
							end
						end,
						Acc,
						OffsetMap
					)
				end,
				{not_updated, SyncRecord},
				binary_to_term(DataRootIndexValue)
			),
			case InDiskPool of
				false ->
					ok = ar_kv:delete(DiskPoolChunksIndex, Key),
					prometheus_gauge:dec(disk_pool_chunks_count);
				true ->
					do_not_remove_from_disk_pool_chunks_index
			end,
			UpdatedState = case IsUpdated of
				updated ->
					U = State#sync_data_state{ sync_record = UpdatedSyncRecord },
					ok = store_sync_state(U),
					U;
				not_updated ->
					State
			end,
			{noreply, UpdatedState#sync_data_state{ disk_pool_cursor = NextCursor }}
	end.

get_tx_data_from_chunks(Offset, Size, Map) ->
	get_tx_data_from_chunks(Offset, Size, Map, <<>>).

get_tx_data_from_chunks(_Offset, 0, _Map, Data) ->
	{ok, iolist_to_binary(Data)};
get_tx_data_from_chunks(Offset, Size, Map, Data) ->
	case maps:get(<< Offset:?OFFSET_KEY_BITSIZE >>, Map, not_found) of
		not_found ->
			{error, not_found};
		Value ->
			{DataPathHash, _, _, _, _, ChunkSize} = binary_to_term(Value),
			case ar_storage:read_chunk(DataPathHash) of
				not_found ->
					{error, not_found};
				{error, Reason} ->
					ar:err([{event, failed_to_read_chunk_for_tx_data}, {reason, Reason}]),
					{error, not_found};
				{ok, {Chunk, _}} ->
					get_tx_data_from_chunks(
						Offset - ChunkSize, Size - ChunkSize, Map, [Chunk | Data])
			end
	end.

data_root_index_iterator(TXRootMap) ->
	{maps:iterator(TXRootMap), none}.

next({TXRootMapIterator, none}) ->
	case maps:next(TXRootMapIterator) of
		none ->
			none;
		{TXRoot, OffsetMap, UpdatedTXRootMapIterator} ->
			OffsetMapIterator = maps:iterator(OffsetMap),
			{Offset, TXPath, UpdatedOffsetMapIterator} = maps:next(OffsetMapIterator),
			UpdatedIterator = {UpdatedTXRootMapIterator, {TXRoot, UpdatedOffsetMapIterator}},
			{{TXRoot, Offset, TXPath}, UpdatedIterator}
	end;
next({TXRootMapIterator, {TXRoot, OffsetMapIterator}}) ->
	case maps:next(OffsetMapIterator) of
		none ->
			next({TXRootMapIterator, none});
		{Offset, TXPath, UpdatedOffsetMapIterator} ->
			UpdatedIterator = {TXRootMapIterator, {TXRoot, UpdatedOffsetMapIterator}},
			{{TXRoot, Offset, TXPath}, UpdatedIterator}
	end.
