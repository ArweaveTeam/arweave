-module(ar_data_sync).

-behaviour(gen_server).

-export([
	start_link/0,
	join/1,
	add_tip_block/3, add_block/2,
	is_chunk_proof_ratio_attractive/3,
	add_chunk/1, add_chunk/2, add_chunk/3,
	add_data_root_to_disk_pool/3, maybe_drop_data_root_from_disk_pool/3,
	get_chunk/2,
	get_tx_data/1, get_tx_data/2, get_tx_offset/1,
	has_data_root/2,
	request_tx_data_removal/1,
	sync_interval/2
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%% The key to the current progress of the migration restoring missing entries
%% in the chunk storage sync record after a bug in 2.1.
-define(RESTORE_CHUNK_STORAGE_SYNC_RECORD_KEY, <<"restore_chunk_storage_sync_record">>).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Notify the server the node has joined the network on the given block index.
join(BI) ->
	gen_server:cast(?MODULE, {join, BI}).

%% @doc Notify the server about the new tip block.
add_tip_block(Height, BlockTXPairs, RecentBI) ->
	gen_server:cast(?MODULE, {add_tip_block, Height, BlockTXPairs, RecentBI}).

%% @doc The condition which is true if the chunk is too small compared to the proof.
%% Small chunks make syncing slower and increase space amplification. A small chunk
%% is accepted if it is the last chunk of the corresponding transaction - such chunks
%% may be produced by ar_tx:chunk_binary/1, the legacy splitting method used to split
%% v1 data or determine the data root of a v2 tx when data is uploaded via the data field.
%% Due to the block limit we can only get up to 1k such chunks per block.
%% @end
is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) ->
	DataPathSize = byte_size(DataPath),
	case DataPathSize of
		0 ->
			false;
		_ ->
			case catch ar_merkle:extract_note(DataPath) of
				{'EXIT', _} ->
					false;
				Offset ->
					Offset == TXSize orelse DataPathSize =< ChunkSize
			end
	end.

%% @doc Store the given chunk if the proof is valid.
add_chunk(Proof) ->
	add_chunk(Proof, 5000, do_not_write_to_free_space_buffer).

%% @doc Store the given chunk if the proof is valid.
add_chunk(Proof, Timeout) ->
	add_chunk(Proof, Timeout, do_not_write_to_free_space_buffer).

%% @doc Store the given chunk if the proof is valid.
add_chunk(
	#{
		data_root := DataRoot,
		offset := Offset,
		data_path := DataPath,
		chunk := Chunk,
		data_size := TXSize
	},
	Timeout,
	WriteToFreeSpaceBuffer
) ->
	Command = {add_chunk, DataRoot, DataPath, Chunk, Offset, TXSize, WriteToFreeSpaceBuffer},
	case catch gen_server:call(?MODULE, Command, Timeout) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Notify the server about the new pending data root (added to mempool).
%% The server may accept pending chunks and store them in the disk pool.
%% @end
add_data_root_to_disk_pool(<<>>, _, _) ->
	ok;
add_data_root_to_disk_pool(_, 0, _) ->
	ok;
add_data_root_to_disk_pool(DataRoot, TXSize, TXID) ->
	gen_server:cast(?MODULE, {add_data_root_to_disk_pool, {DataRoot, TXSize, TXID}}).

%% @doc Notify the server the given data root has been removed from the mempool.
maybe_drop_data_root_from_disk_pool(<<>>, _, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(_, 0, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, TXSize, TXID) ->
	gen_server:cast(?MODULE, {maybe_drop_data_root_from_disk_pool, {DataRoot, TXSize, TXID}}).

%% @doc Fetch the chunk containing the given global offset, including right bound,
%% excluding left bound, along with its inclusion proof.
%%
%% Options:
%%
%%	packing	aes_256_cbc or unpacked
%%	pack	if false and a packed chunk is requested but stored unpacked or
%%			an unpacked chunk is requested but stored packed, return
%%			{error, chunk_not_found} instead of packing/unpacking; true by default
%%	search_fast_storage_only	if true, do not look for the chunk in RocksDB and return
%%								only the chunk, without the proof; false by default
%%
%% @end
get_chunk(Offset, #{ packing := Packing } = Options) ->
	case ar_sync_record:is_recorded(Offset, ?MODULE) of
		false ->
			{error, chunk_not_found};
		{true, StoredPacking} ->
			Pack = maps:get(pack, Options, true),
			case {Pack, Packing == StoredPacking} of
				{false, false} ->
					{error, chunk_not_found};
				{_, true} ->
					get_chunk(Offset, Pack, Packing, Packing, Options);
				{true, false} ->
					get_chunk(Offset, Pack, Packing, StoredPacking, Options)
			end
	end.

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than ?MAX_SERVED_TX_DATA_SIZE.
get_tx_data(TXID) ->
	get_tx_data(TXID, ?MAX_SERVED_TX_DATA_SIZE).

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than SizeLimit.
get_tx_data(TXID, SizeLimit) ->
	case ets:lookup(ar_data_sync_state, tx_index) of
		[] ->
			{error, not_joined};
		[{_, TXIndex}] ->
			[{_, ChunksIndex}] = ets:lookup(ar_data_sync_state, chunks_index),
			[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state, chunk_data_db),
			get_tx_data(TXIndex, ChunksIndex, ChunkDataDB, TXID, SizeLimit)
	end.

%% @doc Return the global end offset and size for the given transaction.
get_tx_offset(TXID) ->
	case ets:lookup(ar_data_sync_state, tx_index) of
		[] ->
			{error, not_joined};
		[{_, TXIndex}] ->
			get_tx_offset(TXIndex, TXID)
	end.

%% @doc Return true if the given {DataRoot, DataSize} is in the mempool
%% or in the index.
has_data_root(DataRoot, DataSize) ->
	DataRootKey = << DataRoot/binary, DataSize:256 >>,
	case catch gen_server:call(?MODULE, {is_data_root_in_mempool, DataRootKey}) of
		true ->
			true;
		false ->
			[{_, DataRootIndex}] = ets:lookup(ar_data_sync_state, data_root_index),
			case ar_kv:get(DataRootIndex, DataRootKey) of
				not_found ->
					false;
				_ ->
					true
			end;
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout}
	end.

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
	gen_server:cast(?MODULE, {add_block, B, SizeTaggedTXs}).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID) ->
	gen_server:cast(?MODULE, {remove_tx_data, TXID}).

%% @doc Make our best attempt to sync the given interval. There is no
%% guarantee the interval will be eventually synced.
sync_interval(Left, Right) ->
	gen_server:cast(?MODULE, {sync_interval, Left, Right}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_data_sync_start}]),
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	State = init_kv(),
	{CurrentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize} = read_data_sync_state(),
	State2 = State#sync_data_state{
		peer_sync_records = #{},
		block_index = CurrentBI,
		weave_size = WeaveSize,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		disk_pool_cursor = first,
		disk_pool_threshold = get_disk_pool_threshold(CurrentBI)
	},
	repair_genesis_block_index(State2),
	gen_server:cast(?MODULE, check_space),
	gen_server:cast(?MODULE, check_space_warning),
	gen_server:cast(?MODULE, pick_sync_peers),
	lists:foreach(
		fun(_SyncingJobNumber) ->
			gen_server:cast(?MODULE, sync_random_interval)
		end,
		lists:seq(1, Config#config.sync_jobs)
	),
	gen_server:cast(?MODULE, update_disk_pool_data_roots),
	gen_server:cast(?MODULE, process_disk_pool_item),
	gen_server:cast(?MODULE, store_sync_state),
	restore_chunk_storage_sync_record(State2),
	{ok, State2}.

handle_cast({restore_chunk_storage_sync_record, none}, State) ->
	#sync_data_state{
		migrations_index = MigrationsDB
	} = State,
	ok = ar_kv:put(MigrationsDB, ?RESTORE_CHUNK_STORAGE_SYNC_RECORD_KEY, <<"complete">>),
	{noreply, State};
handle_cast({restore_chunk_storage_sync_record, Cursor}, State) ->
	#sync_data_state{
		chunks_index = ChunksIndex,
		migrations_index = MigrationsDB,
		chunk_data_db = ChunkDataDB
	} = State,
	{seek, PrevOffset} = Cursor,
	ok = ar_kv:put(MigrationsDB, ?RESTORE_CHUNK_STORAGE_SYNC_RECORD_KEY, PrevOffset),
	Next =
		case ar_kv:get_next(ChunksIndex, Cursor) of
			none ->
				none;
			{ok, ChunkKey, Value} ->
				<< Offset:?OFFSET_KEY_BITSIZE >> = ChunkKey,
				NextCursor = {seek, << (Offset + ?DATA_CHUNK_SIZE):?OFFSET_KEY_BITSIZE >>},
				case ar_chunk_storage:has_chunk(Offset - 1) of
					true ->
						NextCursor;
					false ->
						{ChunkDataKey, _, _, _, _, ChunkSize} = binary_to_term(Value),
						case ChunkSize < ?DATA_CHUNK_SIZE of
							true ->
								NextCursor;
							false ->
								case ar_kv:get(ChunkDataDB, ChunkDataKey) of
									not_found ->
										NextCursor;
									{ok, ChunkValue} ->
										case binary_to_term(ChunkValue) of
											{_, _} ->
												NextCursor;
											DataPath ->
												case ar_chunk_storage:repair_chunk(
															Offset,
															DataPath
														) of
													{ok, synced} ->
														NextCursor;
													{ok, removed} ->
														ok = ar_sync_record:delete(
															Offset,
															Offset - ?DATA_CHUNK_SIZE,
															?MODULE
														),
														ok = ar_kv:delete(ChunksIndex, ChunkKey),
														NextCursor
												end
										end
								end
						end
				end
		end,
	gen_server:cast(?MODULE, {restore_chunk_storage_sync_record, Next}),
	{noreply, State};

handle_cast({join, BI}, State) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		block_index = CurrentBI
	} = State,
	[{_, WeaveSize, _} | _] = BI,
	DiskPoolDataRoots2 =
		case {CurrentBI, ar_util:get_block_index_intersection(BI, CurrentBI)} of
			{[], _Intersection} ->
				ok = data_root_offset_index_from_block_index(DataRootOffsetIndex, BI, 0),
				DiskPoolDataRoots;
			{_CurrentBI, none} ->
				throw(last_stored_block_index_has_no_intersection_with_the_new_one);
			{_CurrentBI, {{H, Offset, _TXRoot}, _Height}} ->
				PreviousWeaveSize = element(2, hd(CurrentBI)),
				{ok, OrphanedDataRoots} = remove_orphaned_data(State, Offset, PreviousWeaveSize),
				ok = data_root_offset_index_from_block_index(
					DataRootOffsetIndex,
					lists:takewhile(fun({BH, _, _}) -> BH /= H end, BI),
					Offset
				),
				ar_chunk_storage:cut(Offset),
				ar_sync_record:cut(Offset, ?MODULE),
				reset_orphaned_data_roots_disk_pool_timestamps(
					DiskPoolDataRoots,
					OrphanedDataRoots
				)
		end,
	State2 =
		State#sync_data_state{
			disk_pool_data_roots = DiskPoolDataRoots2,
			weave_size = WeaveSize,
			block_index = lists:sublist(BI, ?TRACK_CONFIRMATIONS),
			disk_pool_threshold = get_disk_pool_threshold(BI)
		},
	{noreply, store_sync_state(State2)};

handle_cast({add_tip_block, _Height, BlockTXPairs, BI}, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		weave_size = CurrentWeaveSize,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		block_index = CurrentBI
	} = State,
	{BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	{WeaveSize, AddedDataRoots, DiskPoolSize2} = lists:foldl(
		fun ({_BH, []}, Acc) ->
				Acc;
			({_BH, SizeTaggedTXs}, {StartOffset, CurrentAddedDataRoots, CurrentDiskPoolSize}) ->
				{ok, DataRoots} = add_block_data_roots(State, SizeTaggedTXs, StartOffset),
				ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, StartOffset),
				{StartOffset + element(2, lists:last(SizeTaggedTXs)),
					sets:union(CurrentAddedDataRoots, DataRoots),
					sets:fold(
						fun(Key, Acc) ->
							Acc - element(1, maps:get(Key, DiskPoolDataRoots, {0, noop, noop}))
						end,
						CurrentDiskPoolSize,
						DataRoots
					)}
		end,
		{BlockStartOffset, sets:new(), DiskPoolSize},
		Blocks
	),
	DiskPoolDataRoots2 =
		reset_orphaned_data_roots_disk_pool_timestamps(
			add_block_data_roots_to_disk_pool(DiskPoolDataRoots, AddedDataRoots),
			OrphanedDataRoots
		),
	ar_chunk_storage:cut(BlockStartOffset),
	ar_sync_record:cut(BlockStartOffset, ?MODULE),
	State2 = State#sync_data_state{
		weave_size = WeaveSize,
		block_index = BI,
		disk_pool_data_roots = DiskPoolDataRoots2,
		disk_pool_size = DiskPoolSize2,
		disk_pool_threshold = get_disk_pool_threshold(BI)
	},
	{noreply, store_sync_state(State2)};

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
	{DiskPoolDataRoots2, DiskPoolSize2} = case maps:get(Key, DiskPoolDataRoots, not_found) of
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
		disk_pool_data_roots = DiskPoolDataRoots2,
		disk_pool_size = DiskPoolSize2
	}};

handle_cast({add_block, B, SizeTaggedTXs}, State) ->
	add_block(B, SizeTaggedTXs, State),
	{noreply, State};

handle_cast(pick_sync_peers, State) ->
	#sync_data_state{
		peer_sync_records = PeerSyncRecords,
		weave_size = WeaveSize
	} = State,
	Peers = lists:sublist(ar_bridge:get_remote_peers(), ?PEER_SYNC_RECORD_COUNT),
	{PeerSyncRecords2, NewPeers} =
		lists:foldl(
			fun(Peer, {PeerRecords, NP}) ->
				case maps:get(Peer, PeerSyncRecords, not_found) of
					not_found ->
						{maps:put(Peer, ar_intervals:new(), PeerRecords), [Peer | NP]};
					Record ->
						{maps:put(Peer, Record, PeerRecords), NP}
				end
			end,
			{#{}, []},
			Peers
		),
	lists:foldl(
		fun(Peer, Acc) ->
			RandomCursor = rand:uniform(max(1, WeaveSize)) - 1,
			cast_after(Acc, {collect_peer_sync_record, Peer, RandomCursor, 1, RandomCursor}),
			Acc + 200
		end,
		0,
		NewPeers
	),
	Delay =
		case length(Peers) < 10 of
			true ->
				2000;
			false ->
				?SHUFFLE_PEERS_FREQUENCY_MS
		end,
	cast_after(Delay, pick_sync_peers),
	{noreply, State#sync_data_state{ peer_sync_records = PeerSyncRecords2 }};

handle_cast({remove_peer_sync_record, Peer}, State) ->
	#sync_data_state{ peer_sync_records = PeerSyncRecords } = State,
	{noreply, State#sync_data_state{
		peer_sync_records = maps:remove(Peer, PeerSyncRecords) }};

handle_cast({collect_peer_sync_record, Peer, Cursor, Circle, Target},
		#sync_data_state{ sync_disk_space = false } = State) ->
	cast_after(
		ar_disksup:get_disk_space_check_frequency(),
		{collect_peer_sync_record, Peer, Cursor, Circle, Target}
	),
	{noreply, State};
handle_cast({collect_peer_sync_record, Peer, Cursor, Circle, Target},
		#sync_data_state{ peer_sync_records = PeerSyncRecords } = State)
			when is_map_key(Peer, PeerSyncRecords) ->
	WeaveSize = State#sync_data_state.weave_size,
	case ar_sync_record:get_next_unsynced_interval(Cursor, WeaveSize, ?MODULE) of
		not_found ->
			gen_server:cast(
				?MODULE,
				{update_peer_sync_record, Peer, ar_intervals:new(), Circle, Target}
			);
		{_End, Start} ->
			Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
			spawn(
				fun() ->
					case ar_http_iface_client:get_sync_record(Peer, Start + 1, Limit) of
						{ok, PeerSyncRecord} ->
							gen_server:cast(
								?MODULE,
								{update_peer_sync_record, Peer, PeerSyncRecord, Circle, Target}
							);
						{error, {error, timeout}} ->
							cast_after(
								?PEER_FAILED_TO_SERVE_SYNC_RECORD_DELAY_MS,
								{collect_peer_sync_record, Peer, Cursor, Circle, Target}
							);
						{error, {error, connect_timeout}} ->
							cast_after(
								?PEER_FAILED_TO_SERVE_SYNC_RECORD_DELAY_MS,
								{collect_peer_sync_record, Peer, Cursor, Circle, Target}
							);
						{error, {ok, {{<<"503">>, _}, _, <<"{\"error\":\"timeout\"}">>, _ , _}}} ->
							cast_after(
								?PEER_FAILED_TO_SERVE_SYNC_RECORD_DELAY_MS,
								{collect_peer_sync_record, Peer, Cursor, Circle, Target}
							);
						{error, Reason} ->
							?LOG_WARNING([
								{event, failed_to_fetch_peer_sync_record},
								{peer, ar_util:format_peer(Peer)},
								{reason, io_lib:format("~p", [Reason])}
							]),
							cast_after(
								?PEER_FAILED_TO_SERVE_SYNC_RECORD_DELAY_MS,
								{collect_peer_sync_record, Peer, Cursor, Circle, Target}
							)
					end
				end
			)
	end,
	{noreply, State};
handle_cast({collect_peer_sync_record, _Peer, _Cursor, _CircleNumber, _Target}, State) ->
	%% The peer has been removed from the map.
	{noreply, State};

handle_cast({update_peer_sync_record, Peer, PeerSyncRecord2, CircleNumber, Target},
		#sync_data_state{ peer_sync_records = PeerSyncRecords } = State)
			when is_map_key(Peer, PeerSyncRecords) ->
	PeerSyncRecord = maps:get(Peer, PeerSyncRecords),
	PeerSyncRecord3 = ar_intervals:union(PeerSyncRecord2, PeerSyncRecord),
	case ar_intervals:count(PeerSyncRecord3) > ?MAX_PEER_INTERVALS_COUNT of
		true ->
			cast_after(
				?PEER_SYNC_RECORD_UPDATE_DELAY_MS,
				{remove_peer_sync_record, Peer}
			),
			{noreply, State};
		false ->
			PeerSyncRecords2 = maps:put(Peer, PeerSyncRecord3, PeerSyncRecords),
			{CircleNumber2, Cursor} =
				case ar_intervals:is_empty(PeerSyncRecord2) of
					true ->
						{2, 0};
					false ->
						{CircleNumber, element(1, ar_intervals:largest(PeerSyncRecord2))}
				end,
			FullCircleComplete =
				case {CircleNumber2, Cursor >= Target} of
					{2, true} ->
						true;
					_ ->
						false
				end,
			{CircleNumber3, Cursor2, Target2, Delay} =
				case FullCircleComplete of
					true ->
						WeaveSize = State#sync_data_state.weave_size,
						RandomCursor = rand:uniform(max(1, WeaveSize)) - 1,
						{1, RandomCursor, RandomCursor, ?PEER_SYNC_RECORD_UPDATE_DELAY_MS};
					false ->
						{CircleNumber2, Cursor, Target, ?PEER_SYNC_RECORD_NEW_CHUNK_DELAY_MS}
				end,
			cast_after(
				Delay,
				{collect_peer_sync_record, Peer, Cursor2, CircleNumber3, Target2}
			),
			{noreply, State#sync_data_state{ peer_sync_records = PeerSyncRecords2 }}
	end;
handle_cast({update_peer_sync_record, _Peer, _PeerSyncRecord, _CircleNumber, _Target}, State) ->
	%% The peer has been removed from the map.
	{noreply, State};

handle_cast(check_space, State) ->
	cast_after(ar_disksup:get_disk_space_check_frequency(), check_space),
	{noreply, State#sync_data_state{ sync_disk_space = have_free_space() }};

handle_cast(check_space_warning, #sync_data_state{ sync_disk_space = false } = State) ->
	Msg =
		"The node has stopped syncing data - the available disk space is"
		" less than ~s. Add more disk space if you wish to store more data.~n",
	ar:console(Msg, [ar_util:bytes_to_mb_string(?DISK_DATA_BUFFER_SIZE)]),
	?LOG_INFO([
		{event, ar_data_sync_stopped_syncing},
		{reason, little_disk_space_left}
	]),
	cast_after(?DISK_SPACE_WARNING_FREQUENCY, check_space_warning),
	{noreply, State};
handle_cast(check_space_warning, State) ->
	cast_after(?DISK_SPACE_WARNING_FREQUENCY, check_space_warning),
	{noreply, State};

%% Pick a random not synced interval and sync it.
handle_cast(sync_random_interval, #sync_data_state{ sync_disk_space = false } = State) ->
	cast_after(ar_disksup:get_disk_space_check_frequency(), sync_random_interval),
	{noreply, State};
handle_cast(sync_random_interval, State) ->
	#sync_data_state{
		weave_size = WeaveSize,
		peer_sync_records = PeerSyncRecords
	} = State,
	case get_random_interval(PeerSyncRecords, WeaveSize) of
		[] ->
			cast_after(?SEARCH_FOR_SYNC_INTERVAL_DELAY_MS, sync_random_interval),
			{noreply, State};
		SubIntervals ->
			gen_server:cast(?MODULE, {sync_chunk, SubIntervals, loop}),
			{noreply, State}
	end;

handle_cast({sync_interval, _, _}, #sync_data_state{ sync_disk_space = false } = State) ->
	{noreply, State};
handle_cast({sync_interval, Left, Right}, State) ->
	#sync_data_state{
		peer_sync_records = PeerSyncRecords
	} = State,
	case get_interval(PeerSyncRecords, Left, Right) of
		[] ->
			{noreply, State};
		SubIntervals ->
			gen_server:cast(?MODULE, {sync_chunk, SubIntervals, do_not_loop}),
			{noreply, State}
	end;

handle_cast({sync_chunk, [{Byte, RightBound, _} | SubIntervals], Loop}, State)
		when Byte >= RightBound ->
	gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
	{noreply, State};
handle_cast({sync_chunk, [], loop}, State) ->
	gen_server:cast(?MODULE, sync_random_interval),
	{noreply, State};
handle_cast({sync_chunk, [], do_not_loop}, State) ->
	{noreply, State};
handle_cast({sync_chunk, [{_Byte, _RightBound, Peer} | SubIntervals], Loop},
		#sync_data_state{ peer_sync_records = PeerSyncRecords } = State)
			when not is_map_key(Peer, PeerSyncRecords) ->
	gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
	{noreply, State};
handle_cast({sync_chunk, _, _} = Cast,
		#sync_data_state{ sync_disk_space = false } = State) ->
	cast_after(ar_disksup:get_disk_space_check_frequency(), Cast),
	{noreply, State};
handle_cast({sync_chunk, [{Byte, RightBound, Peer} | SubIntervals], Loop}, State) ->
	case ar_sync_record:get_interval(Byte + 1, ?MODULE) of
		{End, _Start} ->
			%% Multiple jobs sync random intervals, which may intersect each other.
			%% This check minimizes the amount of times when an already chunk requested
			%% chunk is requested again.
			gen_server:cast(
				?MODULE,
				{sync_chunk, [{End, RightBound, Peer} | SubIntervals], Loop}
			);
		not_found ->
			Byte2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Byte + 1),
			case Byte2 >= RightBound of
				true ->
					gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop});
				false ->
					Self = self(),
					spawn(
						fun() ->
							case ar_http_iface_client:get_chunk(Peer, Byte2) of
								{ok, Proof} ->
									gen_server:cast(
										Self,
										{
											store_fetched_chunk,
											Peer,
											Byte2 - 1,
											RightBound,
											Proof,
											SubIntervals,
											Loop
										}
									);
								{error, {error, timeout}} ->
									gen_server:cast(Self, {remove_peer_sync_record, Peer}),
									gen_server:cast(Self, {sync_chunk, SubIntervals, Loop});
								{error, {error, connect_timeout}} ->
									gen_server:cast(Self, {remove_peer_sync_record, Peer}),
									gen_server:cast(Self, {sync_chunk, SubIntervals, Loop});
								{error, {ok, {{<<"503">>, _}, _,
										<<"{\"error\":\"timeout\"}">>, _ , _}}} ->
									gen_server:cast(Self, {remove_peer_sync_record, Peer}),
									gen_server:cast(Self, {sync_chunk, SubIntervals, Loop});
								{error, Reason} ->
									?LOG_WARNING([
										{event, failed_to_sync_chunk},
										{byte, Byte2},
										{peer, ar_util:format_peer(Peer)},
										{reason, io_lib:format("~p", [Reason])}
									]),
									gen_server:cast(Self, {remove_peer_sync_record, Peer}),
									gen_server:cast(Self, {sync_chunk, SubIntervals, Loop})
							end
						end
					)
			end
	end,
	{noreply, State};

handle_cast({store_fetched_chunk, Peer, Byte, RightBound, Proof, SubIntervals, Loop}, State) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		peer_sync_records = PeerSyncRecords,
		weave_size = WeaveSize
	} = State,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	{ok, Key, Value} = ar_kv:get_prev(DataRootOffsetIndex, << Byte:?OFFSET_KEY_BITSIZE >>),
	<< BlockStartOffset:?OFFSET_KEY_BITSIZE >> = Key,
	{TXRoot, BlockSize, _DataRootIndexKeySet} = binary_to_term(Value),
	Offset = Byte - BlockStartOffset,
	case validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof) of
		false ->
			?LOG_WARNING([
				{event, got_invalid_proof_from_peer},
				{peer, ar_util:format_peer(Peer)},
				{byte, Byte},
				{weave_size, WeaveSize}
			]),
			%% Not necessarily a malicious peer, it might happen
			%% if the chunk is recent and from a different fork.
			gen_server:cast(?MODULE, {remove_peer_sync_record, Peer}),
			gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
			{noreply, State#sync_data_state{
				peer_sync_records = maps:remove(Peer, PeerSyncRecords)
			}};
		{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkSize, UnpackedChunk} ->
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			case is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) of
				false ->
					?LOG_WARNING([
						{event, got_too_big_proof_from_peer},
						{peer, ar_util:format_peer(Peer)}
					]),
					gen_server:cast(?MODULE, {remove_peer_sync_record, Peer}),
					gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
					{noreply, State#sync_data_state{
						peer_sync_records = maps:remove(Peer, PeerSyncRecords)
					}};
				true ->
					Byte2 = Byte + ChunkSize,
					Cast = {sync_chunk, [{Byte2, RightBound, Peer} | SubIntervals], Loop},
					gen_server:cast(?MODULE, Cast),
					case ar_sync_record:is_recorded(Byte + 1, ?MODULE) of
						{true, _} ->
							%% The chunk has been synced by another job already.
							{noreply, State};
						false ->
							store_fetched_chunk(
								{
									DataRoot,
									AbsoluteTXStartOffset,
									TXPath,
									TXRoot,
									TXSize,
									DataPath,
									Packing,
									ChunkEndOffset,
									ChunkSize,
									Chunk,
									UnpackedChunk
								},
								State
							)
					end
			end
	end;

handle_cast(process_disk_pool_item, #sync_data_state{ disk_full = true } = State) ->
	cast_after(?DISK_POOL_SCAN_FREQUENCY_MS, process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, State) ->
	#sync_data_state{
		disk_pool_cursor = Cursor,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	case ar_kv:get_next(DiskPoolChunksIndex, Cursor) of
		{ok, Key, Value} ->
			gen_server:cast(?MODULE, process_disk_pool_item),
			process_disk_pool_item(State, Key, Value);
		none ->
			case ar_kv:get_next(DiskPoolChunksIndex, first) of
				none ->
					%% The disk_pool_chunks_count metric may be slightly incorrect
					%% if the node is stopped abnormally so the recent changes are
					%% not recorded. Here we correct it every time the disk pool is
					%% cleared.
					prometheus_gauge:set(disk_pool_chunks_count, 0);
				_ ->
					ok
			end,
			cast_after(?DISK_POOL_SCAN_FREQUENCY_MS, process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = first }}
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
	cast_after(?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, update_disk_pool_data_roots),
	{noreply, State#sync_data_state{
		disk_pool_data_roots = UpdatedDiskPoolDataRoots,
		disk_pool_size = DiskPoolSize
	}};

handle_cast({remove_tx_data, TXID}, State) ->
	#sync_data_state{ tx_index = TXIndex } = State,
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{End, Size} = binary_to_term(Value),
			Start = End - Size,
			gen_server:cast(?MODULE, {remove_tx_data, TXID, Size, End, Start + 1}),
			{noreply, State};
		not_found ->
			?LOG_WARNING([
				{event, tx_offset_not_found},
				{tx, ar_util:encode(TXID)}
			]),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_fetch_blacklisted_tx_offset},
				{tx, ar_util:encode(TXID)},
				{reason, Reason}
			]),
			{noreply, State}
	end;

handle_cast({remove_tx_data, TXID, TXSize, End, Cursor}, State) when Cursor > End ->
	ar_tx_blacklist:notify_about_removed_tx_data(TXID, End, End - TXSize),
	{noreply, State};
handle_cast({remove_tx_data, TXID, TXSize, End, Cursor}, State) ->
	#sync_data_state{
		chunks_index = ChunksIndex
	} = State,
	case get_chunk_by_byte(ChunksIndex, Cursor) of
		{ok, Key, Chunk} ->
			<< AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >> = Key,
			case AbsoluteEndOffset > End of
				true ->
					ar_tx_blacklist:notify_about_removed_tx_data(TXID, End, End - TXSize),
					{noreply, State};
				false ->
					{_, _, _, _, _, ChunkSize} = binary_to_term(Chunk),
					AbsoluteStartOffset = AbsoluteEndOffset - ChunkSize,
					%% 1) store updated sync record
					%% 2) remove chunk
					%% 3) update chunks_index
					%%
					%% The order is important - in case the VM crashes,
					%% we will not report false positives to peers,
					%% and the chunk can still be removed upon retry.
					ok = ar_sync_record:delete(AbsoluteEndOffset, AbsoluteStartOffset, ?MODULE),
					ok = ar_chunk_storage:delete(AbsoluteEndOffset),
					ok = ar_kv:delete(ChunksIndex, Key),
					gen_server:cast(
						?MODULE,
						{remove_tx_data, TXID, TXSize, End, AbsoluteEndOffset + 1}
					),
					{noreply, State}
			end;
		{error, invalid_iterator} ->
			%% get_chunk_by_byte looks for a key with the same prefix or the next prefix.
			%% Therefore, if there is no such key, it does not make sense to look for any
			%% key smaller than the prefix + 2 in the next iteration.
			PrefixSpaceSize =
				trunc(math:pow(2, ?OFFSET_KEY_BITSIZE - ?OFFSET_KEY_PREFIX_BITSIZE)),
			NextCursor = ((Cursor div PrefixSpaceSize) + 2) * PrefixSpaceSize,
			gen_server:cast(?MODULE, {remove_tx_data, TXID, TXSize, End, NextCursor}),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([
				{event, tx_data_removal_aborted_since_failed_to_query_chunk},
				{offset, Cursor},
				{reason, Reason}
			]),
			{noreply, State}
	end;

handle_cast(store_sync_state, State) ->
	State2 = store_sync_state(State),
	cast_after(?STORE_STATE_FREQUENCY_MS, store_sync_state),
	{noreply, State2};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_call({add_chunk, _, _, _, _, _, _}, _, #sync_data_state{ disk_full = true } = State) ->
	{reply, {error, disk_full}, State};
handle_call({add_chunk, _, _, _, _, _, _} = Msg, _From, State) ->
	{add_chunk, DataRoot, DataPath, Chunk, Offset, TXSize, WriteToFreeSpaceBuffer} = Msg,
	case WriteToFreeSpaceBuffer == write_to_free_space_buffer orelse have_free_space() of
		false ->
			{reply, {error, disk_full}, State};
		true ->
			add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize, State)
	end;

handle_call({is_data_root_in_mempool, DataRootKey}, _From, State) ->
	#sync_data_state{
		disk_pool_data_roots = DiskPoolDataRoots
	} = State,
	{reply, maps:is_key(DataRootKey, DiskPoolDataRoots), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, State) ->
	#sync_data_state{ chunks_index = {DB, _}, chunk_data_db = ChunkDataDB } = State,
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	store_sync_state(State),
	ar_kv:close(DB),
	ar_kv:close(ChunkDataDB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_chunk(Offset, Pack, Packing, StoredPacking, Options) ->
	case maps:get(search_fast_storage_only, Options, false) of
		true ->
			get_chunk_from_fast_storage(Offset, Pack, Packing, StoredPacking);
		false ->
			case ets:lookup(ar_data_sync_state, chunks_index) of
				[] ->
					{error, not_joined};
				[{_, ChunksIndex}] ->
					[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state, chunk_data_db),
					get_chunk(Offset, Pack, Packing, StoredPacking, ChunksIndex, ChunkDataDB)
			end
	end.

get_chunk_from_fast_storage(Offset, Pack, Packing, StoredPacking) ->
	case ar_chunk_storage:get(Offset - 1) of
		not_found ->
			{error, chunk_not_found};
		{_EndOffset, Chunk} ->
			case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE) of
				false ->
					%% The chunk should have been re-packed
					%% in the meantime - very unlucky timing.
					{error, chunk_not_found};
				true ->
					case {Pack, Packing == StoredPacking} of
						{false, true} ->
							{ok, Chunk};
						{false, false} ->
							{error, chunk_not_found};
						{true, true} ->
							{ok, Chunk};
						{true, false} ->
							{error, option_set_not_supported}
					end
			end
	end.

get_chunk(Offset, Pack, Packing, StoredPacking, ChunksIndex, ChunkDataDB) ->
	case get_chunk_by_byte(ChunksIndex, Offset) of
		{error, _} ->
			{error, chunk_not_found};
		{ok, Key, Value} ->
			<< ChunkOffset:?OFFSET_KEY_BITSIZE >> = Key,
			{ChunkDataDBKey, TXRoot, _, TXPath, _, ChunkSize} = binary_to_term(Value),
			case ChunkOffset - Offset >= ChunkSize of
				true ->
					{error, chunk_not_found};
				false ->
					case read_chunk(ChunkOffset, ChunkDataDB, ChunkDataDBKey) of
						{ok, {Chunk, DataPath}} ->
							case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE) of
								false ->
									%% The chunk should have been re-packed
									%% in the meantime - very unlucky timing.
									{error, chunk_not_found};
								true ->
									PackResult =
										case {Pack, Packing == StoredPacking} of
											{false, true} ->
												{ok, Chunk};
											{false, false} ->
												{error, chunk_not_found};
											{true, true} ->
												{ok, Chunk};
											{true, false} ->
												Unpacked =
													ar_poa:unpack(
														StoredPacking,
														ChunkOffset,
														TXRoot,
														Chunk,
														ChunkSize
													),
												Packed =
													ar_poa:pack(
														Packing,
														ChunkOffset,
														TXRoot,
														Unpacked
													),
												{ok, Packed}
										end,
									case PackResult of
										{ok, PackedChunk} ->
											Proof = #{
												tx_root => TXRoot,
												chunk => PackedChunk,
												data_path => DataPath,
												tx_path => TXPath
											},
											{ok, Proof};
										Error ->
											Error
									end
							end;
						not_found ->
							{error, chunk_not_found};
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_read_chunk},
								{reason, Reason}
							]),
							{error, failed_to_read_chunk}
					end
			end
	end.

get_chunk_by_byte(ChunksIndex, Byte) ->
	ar_kv:get_next_by_prefix(
		ChunksIndex,
		?OFFSET_KEY_PREFIX_BITSIZE,
		?OFFSET_KEY_BITSIZE,
		<< Byte:?OFFSET_KEY_BITSIZE >>
	).

read_chunk(Offset, ChunkDataDB, ChunkDataDBKey) ->
	case ar_kv:get(ChunkDataDB, ChunkDataDBKey) of
		not_found ->
			not_found;
		{ok, Value} ->
			case binary_to_term(Value) of
				{Chunk, DataPath} ->
					{ok, {Chunk, DataPath}};
				DataPath ->
					case ar_chunk_storage:get(Offset - 1) of
						not_found ->
							not_found;
						{_EndOffset, Chunk} ->
							{ok, {Chunk, DataPath}}
					end
			end;
		Error ->
			Error
	end.

get_tx_data(TXIndex, ChunksIndex, ChunkDataDB, TXID, SizeLimit) ->
	case ar_kv:get(TXIndex, TXID) of
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_get_tx_data}, {reason, Reason}]),
			{error, failed_to_get_tx_data};
		{ok, Value} ->
			{Offset, Size} = binary_to_term(Value),
			case Size > SizeLimit of
				true ->
					{error, tx_data_too_big};
				false ->
					StartKey = << (Offset - Size):?OFFSET_KEY_BITSIZE >>,
					EndKey = << Offset:?OFFSET_KEY_BITSIZE >>,
					case ar_kv:get_range(ChunksIndex, StartKey, EndKey) of
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_get_chunks_for_tx_data},
								{reason, Reason}
							]),
							{error, not_found};
						{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
							{error, not_found};
						{ok, Map} ->
							get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map)
					end
			end
	end.

get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map) ->
	get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map, <<>>).

get_tx_data_from_chunks(_ChunkDataDB, _Offset, 0, _Map, Data) ->
	{ok, iolist_to_binary(Data)};
get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map, Data) ->
	case maps:get(<< Offset:?OFFSET_KEY_BITSIZE >>, Map, not_found) of
		not_found ->
			{error, not_found};
		Value ->
			{ChunkDataKey, TXRoot, _, _, _, ChunkSize} = binary_to_term(Value),
			Packing = get_packing(ChunkDataKey),
			case read_chunk(Offset, ChunkDataDB, ChunkDataKey) of
				not_found ->
					{error, not_found};
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_read_chunk_for_tx_data}, {reason, Reason}]),
					{error, not_found};
				{ok, {Chunk, _}} ->
					case ar_sync_record:is_recorded(Offset, Packing, ?MODULE) of
						false ->
							%% The chunk should have been repacked
							%% in the meantime - very unlucky timing.
							{error, not_found};
						true ->
							Unpacked = ar_poa:unpack(Packing, Offset, TXRoot, Chunk, ChunkSize),
							get_tx_data_from_chunks(
								ChunkDataDB,
								Offset - ChunkSize,
								Size - ChunkSize,
								Map,
								[Unpacked | Data]
							)
					end
			end
	end.

get_packing(<< _:256, _:256 >>) ->
	unpacked;
get_packing(<< _:256, _:256, Packing/binary >>) ->
	binary_to_term(Packing);
get_packing(_) ->
	unpacked.

get_tx_offset(TXIndex, TXID) ->
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_read_tx_offset}, {reason, Reason}]),
			{error, failed_to_read_offset}
	end.

init_kv() ->
	BasicOpts = [
		{max_open_files, 1000000}
	],
	BloomFilterOpts = [
		{block_based_table_options, [
			{cache_index_and_filter_blocks, true}, % Keep bloom filters in memory.
			{bloom_filter_policy, 10} % ~1% false positive probability.
		]},
		{optimize_filters_for_hits, true}
	],
	PrefixBloomFilterOpts =
		BloomFilterOpts ++ [
			{prefix_extractor, {capped_prefix_transform, ?OFFSET_KEY_PREFIX_BITSIZE div 8}}],
	ColumnFamilyDescriptors = [
		{"default", BasicOpts},
		{"chunks_index", BasicOpts ++ PrefixBloomFilterOpts},
		{"data_root_index", BasicOpts ++ BloomFilterOpts},
		{"data_root_offset_index", BasicOpts},
		{"tx_index", BasicOpts ++ BloomFilterOpts},
		{"tx_offset_index", BasicOpts},
		{"disk_pool_chunks_index", BasicOpts ++ BloomFilterOpts},
		{"migrations_index", BasicOpts}
	],
	{ok, DB, [_, CF1, CF2, CF3, CF4, CF5, CF6, CF7]} =
		ar_kv:open("ar_data_sync_db", ColumnFamilyDescriptors),
	{ok, ChunkDataDB} =
		ar_kv:open_without_column_families(
			"ar_data_sync_chunk_db", [
				{max_open_files, 1000000},
				{max_background_compactions, 8},
				{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
				{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
				%% 10 files in L1 to make L1 == L0 as recommended by the
				%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
				{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
			]
		),
	State = #sync_data_state{
		chunks_index = {DB, CF1},
		data_root_index = {DB, CF2},
		data_root_offset_index = {DB, CF3},
		tx_index = {DB, CF4},
		tx_offset_index = {DB, CF5},
		disk_pool_chunks_index = {DB, CF6},
		migrations_index = {DB, CF7},
		chunk_data_db = ChunkDataDB
	},
	ets:insert(ar_data_sync_state, [
		{chunks_index, {DB, CF1}},
		{data_root_index, {DB, CF2}},
		{data_root_offset_index, {DB, CF3}},
		{tx_index, {DB, CF4}},
		{tx_offset_index, {DB, CF5}},
		{disk_pool_chunks_index, {DB, CF6}},
		{chunk_data_db, ChunkDataDB}
	]),
	State.

read_data_sync_state() ->
	case ar_storage:read_term(data_sync_state) of
		{ok, {SyncRecord, RecentBI, DiskPoolDataRoots, DiskPoolSize}} ->
			ok = ar_sync_record:set(SyncRecord, unpacked, ?MODULE),
			WeaveSize = case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
			{RecentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize};
		{ok, {RecentBI, DiskPoolDataRoots, DiskPoolSize}} ->
			WeaveSize = case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
			{RecentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize};
		not_found ->
			{[], #{}, 0, 0}
	end.

get_disk_pool_threshold([]) ->
	0;
get_disk_pool_threshold(BI) ->
	ar_mine:get_search_space_upper_bound(BI).

%% @doc Fix the issue related to the mainnet quirk where the node could have incorrectly
%% synced the very beginning of the weave. The genesis block on mainnet has weave_size=0
%% although it has transactions with data. The first non-genesis block with a data
%% transaction (also the first block with non-zero weave_size)
%% is 6OAy50Jx7O7JxHkG8SbGenvX_aHQ-6klsc7gOhLtDF1ebleir2sSJ1_MI3VKSv7N,
%% height 82, size 12364, tx_root P_OiqMNN1s4ltcaq0HXb9VFos_Zz6LFjM8ogUG0vJek,
%% and a single transaction with data_root kuMLOSJKG7O4NmSBY9KZ2PjU-5O4UBNFl_-kF9FnW7w.
%% The nodes that removed the genesis block to free up disk space but later re-synced it,
%% registered its data as the first data of the weave whereas it has to be data from block 82.
%% Fresh nodes would not replicate the incorrect data because the proofs won't be valid,
%% but the affected nodes won't correct themselves either thus we do it here.
%% @end
repair_genesis_block_index(State) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		chunks_index = ChunksIndex,
		data_root_index = DataRootIndex,
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex
	} = State,
	case ?NETWORK_NAME == "arweave.N.1" of
		false ->
			%% Not the mainnet.
			ok;
		true ->
			case ar_kv:get(DataRootOffsetIndex, << 0:256 >>) of
				not_found ->
					ok;
				{ok, Value} ->
					{TXRoot, _, DataRootIndexKeySet} = binary_to_term(Value),
					case ar_util:encode(TXRoot) of
						<<"P_OiqMNN1s4ltcaq0HXb9VFos_Zz6LFjM8ogUG0vJek">> ->
							ok = ar_sync_record:delete(12364, 0, ?MODULE),
							lists:foreach(
								fun(DataRootIndexKey) ->
									ok = ar_kv:delete(DataRootIndex, DataRootIndexKey)
								end,
								sets:to_list(DataRootIndexKeySet)
							),
							ok = ar_kv:delete_range(ChunksIndex, << 0:256 >>, << 12365:256 >>),
							lists:foreach(
								fun(TXID) ->
									ok = ar_kv:delete(TXIndex, TXID)
								end,
								ar_weave:read_v1_genesis_txs()
							),
							ok = ar_kv:delete_range(TXOffsetIndex, << 0:256 >>, << 12364:256 >>),
							DataRoot =
								ar_util:decode(<<"kuMLOSJKG7O4NmSBY9KZ2PjU-5O4UBNFl_-kF9FnW7w">>),
							DataSize = 599058,
							DataRootKey2 = << DataRoot/binary, DataSize:256 >>,
							TXRoot2 =
								ar_util:encode(<<"MzrD8OItolyWnLw9YOheDsAxO5tJeSLAy5QbCYrNJR8">>),
							V2 = {TXRoot2, DataSize, sets:from_list([DataRootKey2])},
							ok =
								ar_kv:put(
									DataRootOffsetIndex,
									<< 0:256 >>,
									term_to_binary(V2)
								),
							ok;
						_ ->
							ok
					end
			end
	end.

restore_chunk_storage_sync_record(State) ->
	%% Restore missing entries, if any, in the chunk storage record.
	%% There was a bug in 2.1 where when a node would stop, the sync
	%% record updates related to the recently stored chunks would not
	%% be persisted because the server was not configured to trap exits.
	#sync_data_state{
		migrations_index = MigrationsDB
	} = State,
	case ar_kv:get(MigrationsDB, ?RESTORE_CHUNK_STORAGE_SYNC_RECORD_KEY) of
		not_found ->
			Cursor = {seek, << (?DATA_CHUNK_SIZE):?OFFSET_KEY_BITSIZE >>},
			gen_server:cast(?MODULE, {restore_chunk_storage_sync_record, Cursor});
		{ok, <<"complete">>} ->
			ok;
		{ok, Cursor} ->
			gen_server:cast(?MODULE, {restore_chunk_storage_sync_record, {seek, Cursor}})
	end.

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
						ar_kv:delete(TXIndex, TXID),
						ar_tx_blacklist:norify_about_orphaned_tx(TXID)
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

have_free_space() ->
	ar_storage:get_free_space(?ROCKS_DB_DIR) > ?DISK_DATA_BUFFER_SIZE
		andalso ar_storage:get_free_space(?CHUNK_DIR) > ?DISK_DATA_BUFFER_SIZE.

add_block(B, SizeTaggedTXs, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex
	} = State,
	BlockStartOffset = B#block.weave_size - B#block.block_size,
	{ok, _} = add_block_data_roots(State, SizeTaggedTXs, BlockStartOffset),
	ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset).

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
								ar_tx_blacklist:notify_about_added_tx(
									TXID,
									AbsoluteEndOffset,
									AbsoluteStartOffset
								),
								TXEndOffset;
							{error, Reason} ->
								?LOG_ERROR([
									{event, failed_to_update_tx_index},
									{reason, Reason}
								]),
								TXEndOffset
						end;
					{error, Reason} ->
						?LOG_ERROR([
							{event, failed_to_update_tx_offset_index},
							{reason, Reason}
						]),
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
	case maps:is_key(AbsoluteTXStartOffset, OffsetMap) of
		true ->
			ok;
		false ->
			UpdatedValue = term_to_binary(
				TXRootMap#{ TXRoot => OffsetMap#{ AbsoluteTXStartOffset => TXPath } }
			),
			ar_kv:put(DataRootIndex, DataRootKey, UpdatedValue)
	end.

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

store_sync_state(State) ->
	#sync_data_state{
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		block_index = BI
	} = State,
	ar_metrics:store(disk_pool_chunks_count),
	StoredState = {BI, DiskPoolDataRoots, DiskPoolSize},
	case ar_storage:write_term(data_sync_state, StoredState) of
		{error, enospc} ->
			?LOG_WARNING([
				{event, failed_to_dump_state},
				{reason, disk_full}
			]),
			State#sync_data_state{ disk_full = true };
		ok ->
			State#sync_data_state{ disk_full = false }
	end.

get_random_interval(_PeerSyncRecords, 0) ->
	[];
get_random_interval(PeerSyncRecords, WeaveSize) ->
	%% Try keeping no more than ?SYNCED_INTERVALS_TARGET intervals
	%% in the sync record by choosing the appropriate size of continuous
	%% intervals to sync. The motivation is to have a syncing heuristic
	%% that syncs data quickly, keeps the record size small for low traffic
	%% overhead, and at the same time maintains synced data reasonably spread out.
	%% When the size increases ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	%% a random subset of the intervals is served to peers.
	%% Include at least one byte - relevant for tests where the weave can be very small.
	SyncSize = max(1, WeaveSize div ?SYNCED_INTERVALS_TARGET),
	N = rand:uniform(WeaveSize) - 1,
	case ar_sync_record:get_next_unsynced_interval(N, WeaveSize, ?MODULE) of
		not_found ->
			[];
		{Right, Left} ->
			Right2 = min(Right, Left + SyncSize),
			get_interval(PeerSyncRecords, Left, Right2)
	end.

get_interval(PeerSyncRecords, Left, Right) ->
	{_, _, PeerIntervals} =
		maps:fold(
			fun pick_intervals_from_peer/3,
			{Left, Right, gb_sets:new()},
			PeerSyncRecords
		),
	get_interval(PeerIntervals, Left, Right, []).

pick_intervals_from_peer(Peer, PeerSyncRecord, {Left, Right, Intervals}) ->
	Iterator = ar_intervals:iterator_from({Left, Left}, PeerSyncRecord),
	pick_intervals_from_peer(Peer, Iterator, Left, Right, Intervals).

pick_intervals_from_peer(Peer, Iterator, Left, Right, Intervals) ->
	case ar_intervals:next(Iterator) of
		none ->
			{Left, Right, Intervals};
		{{_End, Start}, _} when Start >= Right ->
			{Left, Right, Intervals};
		{{End, Start}, _} when End > Right ->
			{Left, Right, gb_sets:add_element({Start, Right, Peer}, Intervals)};
		{{End, Start}, Iterator2} ->
			Intervals2 = gb_sets:add_element({Start, End, Peer}, Intervals),
			pick_intervals_from_peer(Peer, Iterator2, Left, Right, Intervals2)
	end.

get_interval(Intervals, Left, Right, Interval) ->
	case gb_sets:is_empty(Intervals) of
		true ->
			Interval;
		false ->
			case gb_sets:take_smallest(Intervals) of
				{{_Start, End, _Peer}, Intervals2} when Left >= End ->
					get_interval(Intervals2, Left, Right, Interval);
				{{Start, End, Peer}, Intervals2} when Right > End ->
					Left2 = max(Left, Start),
					get_interval(Intervals2, Left2, Right, [{Left2, End, Peer} | Interval]);
				{{Start, _End, Peer}, _Intervals2} ->
					Left2 = max(Left, Start),
					[{Left2, Right, Peer} | Interval]
			end
	end.

validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof) ->
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			ChunkOffset = Offset - TXStartOffset,
			TXSize = TXEndOffset - TXStartOffset,
			case ar_merkle:validate_path_strict(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
					ChunkSize = ChunkEndOffset - ChunkStartOffset,
					UnpackedChunk =
						ar_poa:unpack(
							Packing,
							AbsoluteEndOffset,
							TXRoot,
							Chunk,
							ChunkSize
						),
					case ar_tx:generate_chunk_id(UnpackedChunk) == ChunkID of
						false ->
							false;
						true ->
							case ChunkSize == byte_size(UnpackedChunk) of
								true ->
									{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize,
										ChunkSize, UnpackedChunk};
								false ->
									false
							end
					end
			end
	end.

validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	case ar_merkle:validate_path_strict(DataRoot, Offset, TXSize, DataPath) of
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

%% @doc Process the chunk pushed to the node via POST /chunk.
%% The chunk is placed in the disk pool. The periodic process
%% scanning the disk pool will later record it as synced.
%% The item is removed from the disk pool when chunk's offset
%% drops below the "disk pool threshold".
%% @end
add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize, State) ->
	#sync_data_state{
		data_root_index = DataRootIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_size = DiskPoolSize,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootMapReply = ar_kv:get(DataRootIndex, DataRootKey),
	DataRootInDiskPool = maps:is_key(DataRootKey, DiskPoolDataRoots),
	ChunkSize = byte_size(Chunk),
	CheckDiskPool =
		case {DataRootMapReply, DataRootInDiskPool} of
			{not_found, false} ->
				{error, data_root_not_found};
			{not_found, true} ->
				{ok, Config} = application:get_env(arweave, config),
				DataRootLimit = Config#config.max_disk_pool_data_root_buffer_mb * 1024 * 1024,
				DiskPoolLimit = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
				{Size, Timestamp, TXIDSet} = maps:get(DataRootKey, DiskPoolDataRoots),
				case Size + ChunkSize > DataRootLimit
						orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						{error, exceeds_disk_pool_size_limit};
					false ->
						DiskPoolDataRoots2 =
							maps:put(
								DataRootKey,
								{Size + ChunkSize, Timestamp, TXIDSet},
								DiskPoolDataRoots
							),
						DiskPoolSize2 = DiskPoolSize + ChunkSize,
						State2 =
							State#sync_data_state{
								disk_pool_data_roots = DiskPoolDataRoots2,
								disk_pool_size = DiskPoolSize2
							},
						{ok, {Timestamp, State2}}
				end;
			_ ->
				{ok, {os:system_time(microsecond), State}}
		end,
	ValidateProof =
		case CheckDiskPool of
			{error, _} = Error ->
				Error;
			{ok, PassedState} ->
				case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
					false ->
						{error, invalid_proof};
					{true, EndOffset} ->
						{ok, {EndOffset, PassedState}}
				end
		end,
	CheckSynced =
		case ValidateProof of
			{error, _} = Error2 ->
				Error2;
			{ok, {EndOffset2, {Timestamp2, _}} = PassedState2} ->
				DataPathHash = crypto:hash(sha256, DataPath),
				DiskPoolChunkKey = << Timestamp2:256, DataPathHash/binary >>,
				case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
					{ok, _DiskPoolChunk} ->
						%% The chunk is already in disk pool.
						synced;
					not_found ->
						case DataRootMapReply of
							not_found ->
								{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}};
							_ ->
								DataRootMap = binary_to_term(element(2, DataRootMapReply)),
								case all_offsets_synced(DataRootMap, EndOffset2, State) of
									true ->
										synced;
									false ->
										{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}}
								end
						end;
					{error, Reason} = Error3 ->
						?LOG_ERROR([
							{event, failed_to_read_chunk_from_disk_pool},
							{reason, io_lib:format("~p", [Reason])}
						]),
						Error3
				end
		end,
	case CheckSynced of
		synced ->
			{reply, ok, State};
		{error, _} = Error4 ->
			{reply, Error4, State};
		{ok, {DataPathHash2, DiskPoolChunkKey2, {EndOffset3, {_, State4}}}} ->
			ChunkDataKey = generate_chunk_data_db_key(DataPathHash2, unpacked),
			case write_disk_pool_chunk(
				ChunkDataKey,
				Chunk,
				DataPath,
				State4
			) of
				{error, Reason2} ->
					?LOG_ERROR([
						{event, failed_to_store_chunk_in_disk_pool},
						{reason, io_lib:format("~p", [Reason2])}
					]),
					{reply, {error, failed_to_store_chunk}, State4};
				ok ->
					DiskPoolChunkValue =
						term_to_binary({
							EndOffset3,
							ChunkSize,
							DataRoot,
							TXSize,
							ChunkDataKey
						}),
					case ar_kv:put(
						DiskPoolChunksIndex,
						DiskPoolChunkKey2,
						DiskPoolChunkValue
					) of
						{error, Reason3} ->
							?LOG_ERROR([
								{event, failed_to_record_chunk_in_disk_pool},
								{reason, io_lib:format("~p", [Reason3])}
							]),
							{reply, {error, failed_to_store_chunk}, State};
						ok ->
							prometheus_gauge:inc(disk_pool_chunks_count),
							{reply, ok, State4}
					end
			end
	end.

all_offsets_synced(DataRootIndex, Offset, State) ->
	Iterator = data_root_index_iterator(DataRootIndex),
	all_offsets_synced2(Iterator, Offset, State).

all_offsets_synced2(Iterator, Offset, State) ->
	case next(Iterator) of
		none ->
			true;
		{{_, TXStartOffset, _}, Iterator2} ->
			case ar_sync_record:is_recorded(TXStartOffset + Offset, ?MODULE) of
				{true, _} ->
					all_offsets_synced2(Iterator2, Offset, State);
				false ->
					false
			end
	end.

generate_chunk_data_db_key(DataPathHash, Packing) ->
	Timestamp = os:system_time(microsecond),
	<< Timestamp:256, DataPathHash/binary, (term_to_binary(Packing))/binary >>.

write_disk_pool_chunk(ChunkDataKey, Chunk, DataPath, State) ->
	#sync_data_state{
		chunk_data_db = ChunkDataDB
	} = State,
	ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath})).

write_chunk(Offset, ChunkDataKey, ChunkSize, Chunk, DataPath, State) ->
	case ar_tx_blacklist:is_byte_blacklisted(Offset) of
		true ->
			ok;
		false ->
			write_not_blacklisted_chunk(Offset, ChunkDataKey, ChunkSize, Chunk, DataPath, State)
	end.

write_not_blacklisted_chunk(Offset, ChunkDataKey, ChunkSize, Chunk, DataPath, State) ->
	#sync_data_state{
		chunk_data_db = ChunkDataDB
	} = State,
	Result =
		case ChunkSize == ?DATA_CHUNK_SIZE of
			true ->
				%% 256 KiB chunks are stored in the blob storage optimized for read speed.
				ar_chunk_storage:put(Offset, Chunk);
			false ->
				ok
		end,
	case Result of
		ok ->
			case ChunkSize < ?DATA_CHUNK_SIZE of
				true ->
					ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath}));
				false ->
					ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary(DataPath))
			end;
		_ ->
			Result
	end.

update_chunks_index(Args, State) ->
	{
		AbsoluteChunkOffset,
		ChunkOffset,
		DiskPoolChunkKey,
		ChunkDataKey,
		TXRoot,
		DataRoot,
		TXPath,
		ChunkSize,
		Packing
	} = Args,
	case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
		true ->
			ok;
		false ->
			update_chunks_index2(
				{
					AbsoluteChunkOffset,
					ChunkOffset,
					DiskPoolChunkKey,
					ChunkDataKey,
					TXRoot,
					DataRoot,
					TXPath,
					ChunkSize,
					Packing
				},
				State
			)
	end.

update_chunks_index2(Args, State) ->
	{
		AbsoluteOffset,
		Offset,
		DiskPoolChunkKey,
		ChunkDataDBKey,
		TXRoot,
		DataRoot,
		TXPath,
		ChunkSize,
		Packing
	} = Args,
	#sync_data_state{
		chunks_index = ChunksIndex
	} = State,
	Key = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
	Value = {ChunkDataDBKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
	{Delete, Write} =
		case ar_kv:get(ChunksIndex, Key) of
			{ok, PrevV} ->
				case binary_to_term(PrevV) of
					{DiskPoolChunkKey, _, _, _, _, _} ->
						%% We get here when the chunk falls below disk_pool_threshold.
						%% The same chunk may still be written under offsets above the
						%% threshold so we do not remove it from the disk pool yet.
						{false, true};
					{ChunkDataDBKey, _, _, _, _, _} ->
						%% In practice this should never happen because we
						%% make ar_sync_record:is_recorded checks everywhere
						%% where we run this function.
						{false, false};
					{PreviousKey, _, _, _, _, _} ->
						%% We get here when the chunk is present under offsets
						%% both above and below disk_pool_threshold. Since
						%% PrevousKey /= DiskPoolChunkKey we know that the previously
						%% written chunk is unique so we need to remove it.
						{{true, PreviousKey}, true}
				end;
			not_found ->
				{false, true}
		end,
	WriteResult =
		case Write of
			true ->
				case ar_kv:put(ChunksIndex, Key, term_to_binary(Value)) of
					ok ->
						StartOffset = AbsoluteOffset - ChunkSize,
						ok = ar_sync_record:add(AbsoluteOffset, StartOffset, Packing, ?MODULE),
						ok;
					{error, Reason} ->
						?LOG_ERROR([
							{event, failed_to_update_chunk_index},
							{reason, Reason},
							{chunk, ar_util:encode(ChunkDataDBKey)},
							{offset, AbsoluteOffset}
						]),
						{error, Reason}
				end;
			false ->
				ok
		end,
	case WriteResult of
		ok ->
			case Delete of
				{true, PrevKey} ->
					ok = delete_disk_pool_chunk(PrevKey, State),
					ok;
				false ->
					ok
			end;
		Error ->
			Error
	end.

add_chunk_to_disk_pool(Args, State) ->
	{DataRoot, TXSize, DataPathHash, ChunkDataKey, ChunkOffset, ChunkSize} = Args,
	#sync_data_state{
		disk_pool_data_roots = DiskPoolDataRoots,
		disk_pool_chunks_index = DiskPoolChunksIndex
	} = State,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	Timestamp =
		case maps:get(DataRootKey, DiskPoolDataRoots, not_found) of
			not_found ->
				os:system_time(microsecond);
			{_, DataRootTimestamp, _} ->
				DataRootTimestamp
		end,
	DiskPoolChunkKey = << Timestamp:256, DataPathHash/binary >>,
	case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
		not_found ->
			Value = {ChunkOffset, ChunkSize, DataRoot, TXSize, ChunkDataKey},
			case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey, term_to_binary(Value)) of
				ok ->
					prometheus_gauge:inc(disk_pool_chunks_count),
					ok;
				Error ->
					Error
			end;
		_ ->
			ok
	end.

pick_missing_blocks([{H, WeaveSize, _} | CurrentBI], BlockTXPairs) ->
	{After, Before} = lists:splitwith(fun({BH, _}) -> BH /= H end, BlockTXPairs),
	case Before of
		[] ->
			pick_missing_blocks(CurrentBI, BlockTXPairs);
		_ ->
			{WeaveSize, lists:reverse(After)}
	end.

cast_after(Delay, Message) ->
	%% Not using timer:apply_after here because send_after is more efficient:
	%% http://erlang.org/doc/efficiency_guide/commoncaveats.html#timer-module.
	erlang:send_after(Delay, ?MODULE, {'$gen_cast', Message}).

store_fetched_chunk(Args, State) ->
	{
		DataRoot,
		AbsoluteTXStartOffset,
		TXPath,
		TXRoot,
		TXSize,
		DataPath,
		_Packing,
		Offset,
		ChunkSize,
		_Chunk,
		UnpackedChunk
	} = Args,
	#sync_data_state{
		disk_pool_threshold = DiskPoolThreshold
	} = State,
	AbsoluteOffset = AbsoluteTXStartOffset + Offset,
	DataPathHash = crypto:hash(sha256, DataPath),
	case AbsoluteOffset > DiskPoolThreshold of
		true ->
			ChunkDataKey = generate_chunk_data_db_key(DataPathHash, unpacked),
			Write =
				write_disk_pool_chunk(ChunkDataKey, UnpackedChunk, DataPath, State),
			case Write of
				ok ->
					Args2 = {DataRoot, TXSize, DataPathHash, ChunkDataKey, Offset, ChunkSize},
					case add_chunk_to_disk_pool(Args2, State) of
						ok ->
							case update_chunks_index(
								{
									AbsoluteOffset,
									Offset,
									ChunkDataKey,
									ChunkDataKey,
									TXRoot,
									DataRoot,
									TXPath,
									ChunkSize,
									unpacked
								},
								State
							) of
								ok ->
									{noreply, State};
								{error, Reason} ->
									?LOG_ERROR([
										{event, failed_to_store_fetched_chunk},
										{reason, io_lib:format("~p", [Reason])},
										{relative_chunk_offset, Offset},
										{tx_size, TXSize},
										{data_root, ar_util:encode(DataRoot)}
									]),
									{noreply, State}
							end;
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_store_fetched_chunk},
								{reason, io_lib:format("~p", [Reason])},
								{relative_chunk_offset, Offset},
								{tx_size, TXSize},
								{data_root, ar_util:encode(DataRoot)}
							]),
							{noreply, State}
					end;
				{error, Reason} ->
					?LOG_ERROR([
						{event, failed_to_store_fetched_chunk},
						{reason, io_lib:format("~p", [Reason])},
						{relative_chunk_offset, Offset},
						{tx_size, TXSize},
						{data_root, ar_util:encode(DataRoot)}
					]),
					{noreply, State}
			end;
		false ->
			{StorePacking, StoredChunk} = {unpacked, UnpackedChunk},
			ChunkDataKey = generate_chunk_data_db_key(DataPathHash, StorePacking),
			Write =
				write_chunk(
					AbsoluteOffset, ChunkDataKey, ChunkSize, StoredChunk, DataPath, State),
			case Write of
				ok ->
					case update_chunks_index(
						{
							AbsoluteOffset,
							Offset,
							not_set,
							ChunkDataKey,
							TXRoot,
							DataRoot,
							TXPath,
							ChunkSize,
							StorePacking
						},
						State
					) of
						ok ->
							{noreply, State};
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_store_fetched_chunk},
								{reason, io_lib:format("~p", [Reason])},
								{relative_chunk_offset, Offset},
								{tx_size, TXSize},
								{data_root, ar_util:encode(DataRoot)}
							]),
							{noreply, State}
					end;
				{error, Reason} ->
					?LOG_ERROR([
						{event, failed_to_store_fetched_chunk},
						{reason, io_lib:format("~p", [Reason])},
						{relative_chunk_offset, Offset},
						{tx_size, TXSize},
						{data_root, ar_util:encode(DataRoot)}
					]),
					{noreply, State}
			end
	end.

process_disk_pool_item(State, Key, Value) ->
	#sync_data_state{
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_data_roots = DiskPoolDataRoots,
		data_root_index = DataRootIndex
	} = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = binary_to_term(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} = DiskPoolChunk,
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex =
		case ar_kv:get(DataRootIndex, DataRootKey) of
			not_found ->
				not_found;
			{ok, DataRootIndexValue} ->
				binary_to_term(DataRootIndexValue)
		end,
	InDiskPool = maps:is_key(DataRootKey, DiskPoolDataRoots),
	case {InDataRootIndex, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			NextCursor = {seek, << (Timestamp + 1):256 >>},
			{noreply, State#sync_data_state{ disk_pool_cursor = NextCursor }};
		{not_found, false} ->
			%% The chunk never made it to the chain, the data root has expired in disk pool.
			ok = ar_kv:delete(DiskPoolChunksIndex, Key),
			prometheus_gauge:dec(disk_pool_chunks_count),
			ok = delete_disk_pool_chunk(ChunkDataKey, State),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			{noreply, State#sync_data_state{ disk_pool_cursor = NextCursor }};
		{DataRootMap, _} ->
			DataRootIndexIterator = data_root_index_iterator(DataRootMap),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			State2 = State#sync_data_state{ disk_pool_cursor = NextCursor },
			Args = {Offset, InDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key},
			process_disk_pool_chunk_offsets(DataRootIndexIterator, 0, Args, State2)
	end.

delete_disk_pool_chunk(ChunkDataKey, State) ->
	#sync_data_state{
		chunk_data_db = ChunkDataDB
	} = State,
	ar_kv:delete(ChunkDataDB, ChunkDataKey).

process_disk_pool_chunk_offsets(Iterator, LatestOffset, Args, State) ->
	#sync_data_state{
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_threshold = DiskPoolThreshold,
		chunk_data_db = ChunkDataDB
	} = State,
	{Offset, DataRootInDiskPool, _, _, _, ChunkDataKey, Key} = Args,
	case next(Iterator) of
		none ->
			case (LatestOffset =< DiskPoolThreshold andalso (not DataRootInDiskPool))
					orelse ar_kv:get(ChunkDataDB, ChunkDataKey) == not_found of
				true ->
					ok = ar_kv:delete(DiskPoolChunksIndex, Key),
					prometheus_gauge:dec(disk_pool_chunks_count),
					ok = delete_disk_pool_chunk(ChunkDataKey, State);
				false ->
					ok
			end,
			{noreply, State};
		{{TXRoot, TXStartOffset, TXPath}, Iterator2} ->
			AbsoluteOffset = TXStartOffset + Offset,
			process_disk_pool_chunk_offset(
				Iterator2,
				TXRoot,
				TXPath,
				AbsoluteOffset,
				LatestOffset,
				Args,
				State
			)
	end.

process_disk_pool_chunk_offset(
	Iterator, TXRoot, TXPath, AbsoluteOffset, LatestOffset, Args, State
) ->
	#sync_data_state{
		disk_pool_threshold = DiskPoolThreshold,
		chunk_data_db = ChunkDataDB
	} = State,
	{Offset, _DataRootInDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, _Key} = Args,
	case AbsoluteOffset > DiskPoolThreshold of
		true ->
			case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE) of
				{true, _} ->
					process_disk_pool_chunk_offsets(
						Iterator,
						max(LatestOffset, AbsoluteOffset),
						Args,
						State
					);
				false ->
					case update_chunks_index(
							{
								AbsoluteOffset,
								Offset,
								ChunkDataKey,
								ChunkDataKey,
								TXRoot,
								DataRoot,
								TXPath,
								ChunkSize,
								unpacked
							},
							State
					) of
						ok ->
							process_disk_pool_chunk_offsets(
								Iterator,
								max(LatestOffset, AbsoluteOffset),
								Args,
								State
							);
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_update_chunks_index},
								{reason, io_lib:format("~p", [Reason])}
							]),
							{noreply, State}
					end
			end;
		false ->
			case ar_sync_record:is_recorded(AbsoluteOffset, aes_256_cbc, ?MODULE) of
				true ->
					process_disk_pool_chunk_offsets(
						Iterator,
						max(LatestOffset, AbsoluteOffset),
						Args,
						State
					);
				false ->
					case read_chunk(AbsoluteOffset, ChunkDataDB, ChunkDataKey) of
						not_found ->
							process_disk_pool_chunk_offsets(
								Iterator,
								max(LatestOffset, AbsoluteOffset),
								Args,
								State
							);
						{error, Reason} ->
							?LOG_ERROR([
								{event, failed_to_read_disk_pool_chunk},
								{reason, io_lib:format("~p", [Reason])}
							]),
							{noreply, State};
						{ok, {Chunk, DataPath}} ->
							Packing = get_packing(ChunkDataKey),
							{StorePacking, StoredChunk} =
								{unpacked,
									ar_poa:unpack(
										Packing,
										AbsoluteOffset,
										TXRoot,
										Chunk,
										ChunkSize
									)},
							PackedChunkDataKey =
								generate_chunk_data_db_key(DataPathHash, StorePacking),
							Write =
								write_chunk(
									AbsoluteOffset,
									PackedChunkDataKey,
									ChunkSize,
									StoredChunk,
									DataPath,
									State
								),
							case Write of
								{error, Reason} ->
									?LOG_ERROR([
										{event, failed_to_write_disk_pool_chunk},
										{reason, io_lib:format("~p", [Reason])}
									]),
									{noreply,
										State#sync_data_state{
											disk_full = Reason == enospc
										}};
								ok ->
									case update_chunks_index(
											{
												AbsoluteOffset,
												Offset,
												ChunkDataKey,
												PackedChunkDataKey,
												TXRoot,
												DataRoot,
												TXPath,
												ChunkSize,
												StorePacking
											},
											State
									) of
										ok ->
											process_disk_pool_chunk_offsets(
												Iterator,
												max(LatestOffset, AbsoluteOffset),
												Args,
												State
											);
										{error, Reason} ->
											?LOG_ERROR([
												{event, failed_to_update_disk_pool_chunk_index},
												{reason, io_lib:format("~p", [Reason])}
											]),
											{noreply, State}
									end
							end
					end
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
