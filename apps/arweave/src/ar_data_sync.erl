-module(ar_data_sync).

-behaviour(gen_server).

-export([start_link/2, join/1, add_tip_block/2, add_block/2, is_chunk_proof_ratio_attractive/3,
		add_chunk/5, add_data_root_to_disk_pool/3, maybe_drop_data_root_from_disk_pool/3,
		get_chunk/2, get_tx_data/1, get_tx_data/2, get_tx_offset/1, has_data_root/2,
		request_tx_data_removal/3, request_data_removal/4, record_disk_pool_chunks_count/0,
		record_chunk_cache_size_metric/0, is_chunk_cache_full/0, is_disk_space_sufficient/1,
		get_chunk_by_byte/2, read_chunk/4, decrement_chunk_cache_size/0,
		increment_chunk_cache_size/0, get_chunk_padded_offset/1, get_chunk_metadata_range/3]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).
-export([collect_all_peers_per_chunk/3, aligned_intervals/3, enqueue_intervals/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_sync_buckets.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

-ifdef(DEBUG).
-define(COLLECT_SYNC_INTERVALS_FREQUENCY_MS, 5000).
-else.
-define(COLLECT_SYNC_INTERVALS_FREQUENCY_MS, 300000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Notify the server the node has joined the network on the given block index.
join(RecentBI) ->
	gen_server:cast(ar_data_sync_default, {join, RecentBI}).

%% @doc Notify the server about the new tip block.
add_tip_block(BlockTXPairs, RecentBI) ->
	gen_server:cast(ar_data_sync_default, {add_tip_block, BlockTXPairs, RecentBI}).

%% @doc The condition which is true if the chunk is too small compared to the proof.
%% Small chunks make syncing slower and increase space amplification. A small chunk
%% is accepted if it is the last chunk of the corresponding transaction - such chunks
%% may be produced by ar_tx:chunk_binary/1, the legacy splitting method used to split
%% v1 data or determine the data root of a v2 tx when data is uploaded via the data field.
%% Due to the block limit we can only get up to 1k such chunks per block.
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
%% Called when a chunk is pushed to the node via POST /chunk.
%% The chunk is placed in the disk pool. The periodic process
%% scanning the disk pool will later record it as synced.
%% The item is removed from the disk pool when the chunk's offset
%% drops below the disk pool threshold.
add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize) ->
	DataRootIndex = {data_root_index, "default"},
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	DiskPoolChunksIndex = {disk_pool_chunks_index, "default"},
	ChunkDataDB = {chunk_data_db, "default"},
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootOffsetReply = get_data_root_offset(DataRootKey, "default"),
	DataRootInDiskPool = ets:lookup(ar_disk_pool_data_roots, DataRootKey),
	ChunkSize = byte_size(Chunk),
	{ok, Config} = application:get_env(arweave, config),
	DataRootLimit = Config#config.max_disk_pool_data_root_buffer_mb * 1024 * 1024,
	DiskPoolLimit = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	CheckDiskPool =
		case {DataRootOffsetReply, DataRootInDiskPool} of
			{not_found, []} ->
				{error, data_root_not_found};
			{not_found, [{_, {Size, Timestamp, TXIDSet}}]} ->
				case Size + ChunkSize > DataRootLimit
						orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						{error, exceeds_disk_pool_size_limit};
					false ->
						{ok, {Size + ChunkSize, Timestamp, TXIDSet}}
				end;
			_ ->
				case DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						{error, exceeds_disk_pool_size_limit};
					false ->
						Timestamp =
							case DataRootInDiskPool of
								[] ->
									os:system_time(microsecond);
								[{_, {_, Timestamp2, _}}] ->
									Timestamp2
							end,
						{ok, {ChunkSize, Timestamp, not_set}}
				end
		end,
	ValidateProof =
		case CheckDiskPool of
			{error, _} = Error ->
				Error;
			{ok, DiskPoolDataRootValue} ->
				case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
					false ->
						{error, invalid_proof};
					{true, PassesStrict, EndOffset} ->
						{ok, {EndOffset, PassesStrict, DiskPoolDataRootValue}}
				end
		end,
	CheckSynced =
		case ValidateProof of
			{error, _} = Error2 ->
				Error2;
			{ok, {EndOffset2, _PassesStrict2, {_, Timestamp3, _}} = PassedState2} ->
				DataPathHash = crypto:hash(sha256, DataPath),
				DiskPoolChunkKey = << Timestamp3:256, DataPathHash/binary >>,
				case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
					{ok, _DiskPoolChunk} ->
						%% The chunk is already in disk pool.
						synced;
					not_found ->
						case DataRootOffsetReply of
							not_found ->
								{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}};
							{ok, {TXStartOffset, _}} ->
								case chunk_offsets_synced(DataRootIndex, DataRootKey,
										%% The same data may be uploaded several times.
										%% Here we only accept the chunk if any of the
										%% last 5 instances of this data is not filled in
										%% yet.
										EndOffset2, TXStartOffset, 5) of
									true ->
										synced;
									false ->
										{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}}
								end
						end;
					{error, Reason} ->
						?LOG_WARNING([{event, failed_to_read_chunk_from_disk_pool},
								{reason, io_lib:format("~p", [Reason])},
								{data_path_hash, ar_util:encode(DataPathHash)},
								{data_root, ar_util:encode(DataRoot)},
								{relative_offset, EndOffset2}]),
						{error, failed_to_store_chunk}
				end
		end,
	case CheckSynced of
		synced ->
			ok;
		{error, _} = Error4 ->
			Error4;
		{ok, {DataPathHash2, DiskPoolChunkKey2, {EndOffset3, PassesStrict3,
				DiskPoolDataRootValue2}}} ->
			ChunkDataKey = get_chunk_data_key(DataPathHash2),
			case ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath})) of
				{error, Reason2} ->
					?LOG_WARNING([{event, failed_to_store_chunk_in_disk_pool},
						{reason, io_lib:format("~p", [Reason2])},
						{data_path_hash, ar_util:encode(DataPathHash2)},
						{data_root, ar_util:encode(DataRoot)},
						{relative_offset, EndOffset3}]),
					{error, failed_to_store_chunk};
				ok ->
					DiskPoolChunkValue = term_to_binary({EndOffset3, ChunkSize, DataRoot,
							TXSize, ChunkDataKey, PassesStrict3}),
					case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey2,
							DiskPoolChunkValue) of
						{error, Reason3} ->
							?LOG_WARNING([{event, failed_to_record_chunk_in_disk_pool},
								{reason, io_lib:format("~p", [Reason3])},
								{data_path_hash, ar_util:encode(DataPathHash2)},
								{data_root, ar_util:encode(DataRoot)},
								{relative_offset, EndOffset3}]),
							{error, failed_to_store_chunk};
						ok ->
							?LOG_DEBUG([{event, stored_chunk_in_disk_pool},
									{data_path_hash, ar_util:encode(DataPathHash2)},
									{data_root, ar_util:encode(DataRoot)},
									{relative_offset, EndOffset3}]),
							ets:insert(ar_disk_pool_data_roots,
									{DataRootKey, DiskPoolDataRootValue2}),
							ets:update_counter(ar_data_sync_state, disk_pool_size,
									{2, ChunkSize}),
							prometheus_gauge:inc(pending_chunks_size, ChunkSize),
							ok
					end
			end
	end.

%% @doc Notify the server about the new pending data root (added to mempool).
%% The server may accept pending chunks and store them in the disk pool.
add_data_root_to_disk_pool(_, 0, _) ->
	ok;
add_data_root_to_disk_pool(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
add_data_root_to_disk_pool(DataRoot, TXSize, TXID) ->
	Key = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ets:lookup(ar_disk_pool_data_roots, Key) of
		[] ->
			ets:insert(ar_disk_pool_data_roots, {Key,
					{0, os:system_time(microsecond), sets:from_list([TXID])}});
		[{_, {_, _, not_set}}] ->
			ok;
		[{_, {Size, Timestamp, TXIDSet}}] ->
			ets:insert(ar_disk_pool_data_roots,
					{Key, {Size, Timestamp, sets:add_element(TXID, TXIDSet)}})
	end,
	ok.

%% @doc Notify the server the given data root has been removed from the mempool.
maybe_drop_data_root_from_disk_pool(_, 0, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, TXSize, TXID) ->
	Key = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ets:lookup(ar_disk_pool_data_roots, Key) of
		[] ->
			ok;
		[{_, {_, _, not_set}}] ->
			ok;
		[{_, {Size, Timestamp, TXIDs}}] ->
			case sets:subtract(TXIDs, sets:from_list([TXID])) of
				TXIDs ->
					ok;
				TXIDs2 ->
					case sets:size(TXIDs2) of
						0 ->
							ets:delete(ar_disk_pool_data_roots, Key);
						_ ->
							ets:insert(ar_disk_pool_data_roots, {Key,
									{Size, Timestamp, TXIDs2}})
					end
			end
	end,
	ok.

%% @doc Fetch the chunk corresponding to Offset. When Offset is less than or equal to
%% the strict split data threshold, the chunk returned contains the byte with the given
%% Offset (the indexing is 1-based). Otherwise, the chunk returned ends in the same 256 KiB
%% bucket as Offset counting from the first 256 KiB after the strict split data threshold.
%% The strict split data threshold is weave_size of the block preceding the fork 2.5 block.
%%
%% Options:
%%	_________________________________________________________________________________________
%%	packing				| required; spora_2_5 or unpacked or {spora_2_6, <Mining Address>};
%%	_________________________________________________________________________________________
%%	pack				| if false and a packed chunk is requested but stored unpacked or
%%						| an unpacked chunk is requested but stored packed, return
%%						| {error, chunk_not_found} instead of packing/unpacking;
%%						| true by default;
%%	_________________________________________________________________________________________
%%	bucket_based_offset	| does not play a role for the offsets before
%%						| strict_data_split_threshold (weave_size of the block preceding
%%						| the fork 2.5 block); if true, return the chunk which ends in
%%						| the same 256 KiB bucket starting from
%%						| strict_data_split_threshold where borders belong to the
%%						| buckets on the left; true by default.
get_chunk(Offset, #{ packing := Packing } = Options) ->
	Pack = maps:get(pack, Options, true),
	IsRecorded =
		case Pack of
			false ->
				ar_sync_record:is_recorded(Offset, {?MODULE, Packing});
			true ->
				ar_sync_record:is_recorded(Offset, ?MODULE)
		end,
	SeekOffset =
		case maps:get(bucket_based_offset, Options, true) of
			true ->
				get_chunk_seek_offset(Offset);
			false ->
				Offset
		end,
	case IsRecorded of
		{{true, StoredPacking}, StoreID} ->
			get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID);
		_ ->
			{error, chunk_not_found}
	end.

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than ?MAX_SERVED_TX_DATA_SIZE, unless the limitation
%% is disabled in the configuration.
get_tx_data(TXID) ->
	{ok, Config} = application:get_env(arweave, config),
	SizeLimit =
		case lists:member(serve_tx_data_without_limits, Config#config.enable) of
			true ->
				infinity;
			false ->
				?MAX_SERVED_TX_DATA_SIZE
		end,
	get_tx_data(TXID, SizeLimit).

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than SizeLimit.
get_tx_data(TXID, SizeLimit) ->
	case get_tx_offset(TXID) of
		{error, not_joined} ->
			{error, not_joined};
		{error, not_found} ->
			{error, not_found};
		{error, failed_to_read_tx_offset} ->
			{error, failed_to_read_tx_data};
		{ok, {Offset, Size}} ->
			case Size > SizeLimit of
				true ->
					{error, tx_data_too_big};
				false ->
					get_tx_data(Offset - Size, Offset, [])
			end
	end.

%% @doc Return the global end offset and size for the given transaction.
get_tx_offset(TXID) ->
	TXIndex = {tx_index, "default"},
	get_tx_offset(TXIndex, TXID).

%% @doc Return true if the given {DataRoot, DataSize} is in the mempool
%% or in the index.
has_data_root(DataRoot, DataSize) ->
	DataRootKey = << DataRoot:32/binary, DataSize:256 >>,
	case ets:member(ar_disk_pool_data_roots, DataRootKey) of
		true ->
			true;
		false ->
			case get_data_root_offset(DataRootKey, "default") of
				{ok, _} ->
					true;
				_ ->
					false
			end
	end.

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
	gen_server:call(ar_data_sync_default, {add_block, B, SizeTaggedTXs}, infinity).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID, Ref, ReplyTo) ->
	TXIndex = {tx_index, "default"},
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{End, Size} = binary_to_term(Value),
			remove_range(End - Size, End, Ref, ReplyTo);
		not_found ->
			?LOG_WARNING([{event, tx_offset_not_found}, {tx, ar_util:encode(TXID)}]),
			ok;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_fetch_blacklisted_tx_offset},
					{tx, ar_util:encode(TXID)}, {reason, Reason}]),
			ok
	end.

%% @doc Request the removal of the given byte range.
request_data_removal(Start, End, Ref, ReplyTo) ->
	remove_range(Start, End, Ref, ReplyTo).

%% @doc Return true if the in-memory data chunk cache is full. Return not_initialized
%% if there is no information yet.
is_chunk_cache_full() ->
	case ets:lookup(ar_data_sync_state, chunk_cache_size_limit) of
		[{_, Limit}] ->
			case ets:lookup(ar_data_sync_state, chunk_cache_size) of
				[{_, Size}] when Size > Limit ->
					true;
				_ ->
					false
			end;
		_ ->
			not_initialized
	end.

%% @doc Return true if we have sufficient disk space to write new data for the
%% given StoreID. Return not_initialized if there is no information yet.
is_disk_space_sufficient(StoreID) ->
	case ets:lookup(ar_data_sync_state, {is_disk_space_sufficient, StoreID}) of
		[{_, false}] ->
			false;
		[{_, true}] ->
			true;
		_ ->
			not_initialized
	end.

get_chunk_by_byte(ChunksIndex, Byte) ->
	Chunk = ar_kv:get_next_by_prefix(ChunksIndex, ?OFFSET_KEY_PREFIX_BITSIZE,
		?OFFSET_KEY_BITSIZE, << Byte:?OFFSET_KEY_BITSIZE >>),
	case Chunk of
		{error, Reason} ->
			{error, Reason};
		{ok, Key, MetaData} ->
			<< AbsoluteOffset:?OFFSET_KEY_BITSIZE >> = Key,
			{
				ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize
			} = binary_to_term(MetaData),
			FullMetaData = {AbsoluteOffset, ChunkDataKey, TXRoot, DataRoot, TXPath,
				RelativeOffset, ChunkSize},
			{ok, Key, FullMetaData}
	end.

read_chunk(Offset, ChunkDataDB, ChunkDataKey, StoreID) ->
	case ar_kv:get(ChunkDataDB, ChunkDataKey) of
		not_found ->
			not_found;
		{ok, Value} ->
			case binary_to_term(Value) of
				{Chunk, DataPath} ->
					{ok, {Chunk, DataPath}};
				DataPath ->
					case ar_chunk_storage:get(Offset - 1, StoreID) of
						not_found ->
							not_found;
						{_EndOffset, Chunk} ->
							{ok, {Chunk, DataPath}}
					end
			end;
		Error ->
			Error
	end.

decrement_chunk_cache_size() ->
	ets:update_counter(ar_data_sync_state, chunk_cache_size, {2, -1}, {chunk_cache_size, 0}).

increment_chunk_cache_size() ->
	ets:update_counter(ar_data_sync_state, chunk_cache_size, {2, 1}, {chunk_cache_size, 1}).

%% @doc Return Offset if it is smaller than or equal to ?STRICT_DATA_SPLIT_THRESHOLD.
%% Otherwise, return the offset of the last byte of the chunk + the size of the padding.
get_chunk_padded_offset(Offset) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			ar_poa:get_padded_offset(Offset, ?STRICT_DATA_SPLIT_THRESHOLD);
		false ->
			Offset
	end.

%% @doc Return {ok, Map} | {error, Error} where
%% Map is
%% AbsoluteEndOffset => {ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize}
%% map with all the chunk metadata found within the given range AbsoluteEndOffset >= Start,
%% AbsoluteEndOffset =< End. Return the empty map if no metadata is found.
get_chunk_metadata_range(Start, End, StoreID) ->
	case ar_kv:get_range({chunks_index, StoreID},
			<< Start:?OFFSET_KEY_BITSIZE >>, << End:?OFFSET_KEY_BITSIZE >>) of
		{ok, Map} ->
			{ok, maps:fold(
					fun(K, V, Acc) ->
						<< Offset:?OFFSET_KEY_BITSIZE >> = K,
						maps:put(Offset, binary_to_term(V), Acc)
					end,
					#{},
					Map)};
		Error ->
			Error
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init("default" = StoreID) ->
	?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID}]),
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	[ok, ok, ok] = ar_events:subscribe([node_state, chunk, disksup]),
	State = init_kv(StoreID),
	move_disk_pool_index(State),
	move_data_root_index(State),
	timer:apply_interval(?RECORD_DISK_POOL_CHUNKS_COUNT_FREQUENCY_MS, ar_data_sync,
			record_disk_pool_chunks_count, []),
	StateMap = read_data_sync_state(),
	CurrentBI = maps:get(block_index, StateMap),
	%% Maintain a map of pending, recently uploaded, and orphaned data roots.
	%% << DataRoot:32/binary, TXSize:256 >> => {Size, Timestamp, TXIDSet}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% New chunks for these data roots are accepted until the corresponding size reaches
	%% #config.max_disk_pool_data_root_buffer_mb or the total size of added pending and
	%% seeded chunks reaches #config.max_disk_pool_buffer_mb. When a data root is orphaned,
	%% its timestamp is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they do not belong to any storage module - from storage.
	%% TXIDSet keeps track of pending transaction identifiers - if all pending transactions
	%% with the << DataRoot:32/binary, TXSize:256 >> key are dropped from the mempool,
	%% the corresponding entry is removed from DiskPoolDataRoots. When a data root is
	%% confirmed, TXIDSet is set to not_set - from this point on, the key is only dropped
	%% after expiration.
	DiskPoolDataRoots = maps:get(disk_pool_data_roots, StateMap),
	recalculate_disk_pool_size(DiskPoolDataRoots, State),
	DiskPoolThreshold = maps:get(disk_pool_threshold, StateMap),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 = State#sync_data_state{
		block_index = CurrentBI,
		weave_size = maps:get(weave_size, StateMap),
		disk_pool_cursor = first,
		disk_pool_threshold = DiskPoolThreshold,
		mining_address = Config#config.mining_addr,
		store_id = StoreID
	},
	timer:apply_interval(?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, ?MODULE,
			remove_expired_disk_pool_data_roots, []),
	lists:foreach(
		fun(_DiskPoolJobNumber) ->
			gen_server:cast(self(), process_disk_pool_item)
		end,
		lists:seq(1, Config#config.disk_pool_jobs)
	),
	gen_server:cast(self(), store_sync_state),
	{ok, Config} = application:get_env(arweave, config),
	Limit =
		case Config#config.data_cache_size_limit of
			undefined ->
				Free = proplists:get_value(free_memory, memsup:get_system_memory_data(),
						2000000000),
				Limit2 = min(1000, erlang:ceil(Free * 0.9 / 3 / 262144)),
				Limit3 = Limit2 - Limit2 rem 100 + 100,
				Limit3;
			Limit2 ->
				Limit2
		end,
	ar:console("~nSetting the data chunk cache size limit to ~B chunks.~n", [Limit]),
	ets:insert(ar_data_sync_state, {chunk_cache_size_limit, Limit}),
	ets:insert(ar_data_sync_state, {chunk_cache_size, 0}),
	timer:apply_interval(200, ?MODULE, record_chunk_cache_size_metric, []),
	gen_server:cast(self(), process_store_chunk_queue),
	{ok, State2};
init(StoreID) ->
	?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID}]),
	process_flag(trap_exit, true),
	[ok, ok, ok] = ar_events:subscribe([node_state, chunk, disksup]),
	State = init_kv(StoreID),
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	gen_server:cast(self(), process_store_chunk_queue),
	gen_server:cast(self(), sync_intervals),
	State2 = State#sync_data_state{
		store_id = StoreID,
		range_start = RangeStart,
		range_end = RangeEnd
	},
	{ok, may_be_start_syncing(State2)}.

handle_cast({move_data_root_index, Cursor, N}, State) ->
	move_data_root_index(Cursor, N, State),
	{noreply, State};

handle_cast(process_store_chunk_queue, State) ->
	ar_util:cast_after(200, self(), process_store_chunk_queue),
	{noreply, process_store_chunk_queue(State)};

handle_cast({join, RecentBI}, State) ->
	#sync_data_state{ block_index = CurrentBI, store_id = StoreID } = State,
	[{_, WeaveSize, _} | _] = RecentBI,
	case {CurrentBI, ar_block_index:get_intersection(CurrentBI)} of
		{[], _} ->
			ok;
		{_, no_intersection} ->
			throw(last_stored_block_index_has_no_intersection_with_the_new_one);
		{_, {_H, Offset, _TXRoot}} ->
			PreviousWeaveSize = element(2, hd(CurrentBI)),
			{ok, OrphanedDataRoots} = remove_orphaned_data(State, Offset, PreviousWeaveSize),
			{ok, Config} = application:get_env(arweave, config),
			[gen_server:cast(list_to_atom("ar_data_sync_" ++ ar_storage_module:id(Module)),
					{cut, Offset}) || Module <- Config#config.storage_modules],
			ok = ar_chunk_storage:cut(Offset, StoreID),
			ok = ar_sync_record:cut(Offset, ?MODULE, StoreID),
			ar_events:send(data_sync, {cut, Offset}),
			reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots)
	end,
	BI = ar_block_index:get_list_by_hash(element(1, lists:last(RecentBI))),
	repair_data_root_offset_index(BI, State),
	DiskPoolThreshold = get_disk_pool_threshold(RecentBI),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 =
		State#sync_data_state{
			weave_size = WeaveSize,
			block_index = RecentBI,
			disk_pool_threshold = DiskPoolThreshold
		},
	store_sync_state(State2),
	{noreply, State2};

handle_cast({cut, Start}, #sync_data_state{ store_id = StoreID,
		range_end = End } = State) ->
	case ar_sync_record:get_next_synced_interval(Start, End, ?MODULE, StoreID) of
		not_found ->
			ok;
		_Interval ->
			{ok, Config} = application:get_env(arweave, config),
			case lists:member(remove_orphaned_storage_module_data, Config#config.enable) of
				false ->
					ar:console("The storage module ~s contains some orphaned data above the "
							"weave offset ~B. Make sure you are joining the network through "
							"trusted in-sync peers and restart with "
							"`enable remove_orphaned_storage_module_data`.~n",
							[StoreID, Start]),
					timer:sleep(2000),
					erlang:halt();
				true ->
					ok = remove_chunks_index_range(Start, End, State),
					ok = ar_chunk_storage:cut(Start, StoreID),
					ok = ar_sync_record:cut(Start, ?MODULE, StoreID)
			end
	end,
	{noreply, State};

handle_cast({add_tip_block, BlockTXPairs, BI}, State) ->
	#sync_data_state{ store_id = StoreID, weave_size = CurrentWeaveSize,
			block_index = CurrentBI } = State,
	{BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	{WeaveSize, AddedDataRoots} = lists:foldl(
		fun ({_BH, []}, Acc) ->
				Acc;
			({_BH, SizeTaggedTXs}, {StartOffset, CurrentAddedDataRoots}) ->
				{ok, DataRoots} = add_block_data_roots(SizeTaggedTXs, StartOffset, StoreID),
				ok = update_tx_index(SizeTaggedTXs, StartOffset, StoreID),
				{StartOffset + element(2, lists:last(SizeTaggedTXs)),
					sets:union(CurrentAddedDataRoots, DataRoots)}
		end,
		{BlockStartOffset, sets:new()},
		Blocks
	),
	add_block_data_roots_to_disk_pool(AddedDataRoots),
	reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots),
	ok = ar_chunk_storage:cut(BlockStartOffset, StoreID),
	ok = ar_sync_record:cut(BlockStartOffset, ?MODULE, StoreID),
	ar_events:send(data_sync, {cut, BlockStartOffset}),
	DiskPoolThreshold = get_disk_pool_threshold(BI),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 = State#sync_data_state{ weave_size = WeaveSize,
			block_index = BI, disk_pool_threshold = DiskPoolThreshold },
	store_sync_state(State2),
	{noreply, State2};

handle_cast(sync_data, State) ->
	#sync_data_state{ store_id = OriginStoreID, range_start = RangeStart, range_end = RangeEnd,
			disk_pool_threshold = DiskPoolThreshold } = State,
	%% See if any of OriginStoreID's unsynced intervals can be found in the "default"
	%% storage_module
	Intervals = get_unsynced_intervals_from_other_storage_modules(OriginStoreID, "default",
			RangeStart, min(RangeEnd, DiskPoolThreshold)),
	gen_server:cast(self(), sync_data2),
	%% Find all neighboring storage_modules (those that might share a 100MB overlap with
	%% StoreId)
	StorageModules = [ar_storage_module:id(Module)
			|| Module <- ar_storage_module:get_all(RangeStart, RangeEnd),
			ar_storage_module:id(Module) /= OriginStoreID],
	{noreply, State#sync_data_state{
			unsynced_intervals_from_other_storage_modules = Intervals,
			other_storage_modules_with_unsynced_intervals = StorageModules }};

%% @doc No unsynced overlap intervals, proceed with syncing
handle_cast(sync_data2, #sync_data_state{
		unsynced_intervals_from_other_storage_modules = [],
		other_storage_modules_with_unsynced_intervals = [] } = State) ->
	ar_util:cast_after(2000, self(), collect_peer_intervals),
	{noreply, State};
%% @doc Check to see if a neighboring storge_module may have already synced one of our
%% unsynced intervals
handle_cast(sync_data2, #sync_data_state{
		store_id = OriginStoreID, range_start = RangeStart, range_end = RangeEnd,
		unsynced_intervals_from_other_storage_modules = [],
		other_storage_modules_with_unsynced_intervals = [StoreID | StoreIDs] } = State) ->
	Intervals = get_unsynced_intervals_from_other_storage_modules(OriginStoreID, StoreID,
			RangeStart, RangeEnd),
	gen_server:cast(self(), sync_data2),
	{noreply, State#sync_data_state{
			unsynced_intervals_from_other_storage_modules = Intervals,
			other_storage_modules_with_unsynced_intervals = StoreIDs }};
%% @doc Read an unsynced interval from the disk of a neighboring storage_module
handle_cast(sync_data2, #sync_data_state{
		store_id = OriginStoreID,
		unsynced_intervals_from_other_storage_modules = [{StoreID, {Start, End}} | Intervals]
		} = State) ->
	gen_server:cast(ar_data_sync_worker_master,
			{read_range, {Start, End, StoreID, OriginStoreID, false}}),
	ar_util:cast_after(50, self(), sync_data2),
	{noreply, State#sync_data_state{
			unsynced_intervals_from_other_storage_modules = Intervals }};

handle_cast({invalidate_bad_data_record, Args}, State) ->
	invalidate_bad_data_record(Args),
	{noreply, State};

handle_cast({pack_and_store_chunk, Args} = Cast,
			#sync_data_state{ store_id = StoreID } = State) ->
	case is_disk_space_sufficient(StoreID) of
		true ->
			pack_and_store_chunk(Args, State);
		_ ->
			ar_util:cast_after(30000, self(), Cast),
			{noreply, State}
	end;

%% Schedule syncing of the unsynced intervals. Choose a peer for each of the intervals.
handle_cast(collect_peer_intervals, State) ->
	#sync_data_state{ range_start = Start, range_end = End, sync_intervals_queue = Q,
			store_id = StoreID, disk_pool_threshold = DiskPoolThreshold } = State,
	IsJoined =
		case ar_node:is_joined() of
			false ->
				ar_util:cast_after(1000, self(), collect_peer_intervals),
				false;
			true ->
				true
		end,
	IsDiskSpaceSufficient =
		case IsJoined of
			false ->
				false;
			true ->
				case is_disk_space_sufficient(StoreID) of
					true ->
						true;
					_ ->
						ar_util:cast_after(30000, self(), collect_peer_intervals),
						false
				end
		end,
	IsSyncQueueBusy =
		case IsDiskSpaceSufficient of
			false ->
				true;
			true ->
				case gb_sets:size(Q) > ?SYNC_INTERVALS_MAX_QUEUE_SIZE of
					true ->
						ar_util:cast_after(500, self(), collect_peer_intervals),
						true;
					false ->
						false
				end
		end,
	IsBelowDiskPoolThreshold =
		case IsSyncQueueBusy of
			true ->
				false;
			false ->
				case Start >= DiskPoolThreshold of
					true ->
						ar_util:cast_after(500, self(), collect_peer_intervals),
						false;
					false ->
						true
				end
		end,
	case IsBelowDiskPoolThreshold of
		false ->
			ok;
		true ->
			{ok, Config} = application:get_env(arweave, config),
			SyncWorkers = Config#config.sync_jobs,
			%% We do not want to bother peers if we still have a lot of data scheduled for
			%% local copying and repacking.
			case ar_data_sync_worker_master:get_scheduled_task_count() > SyncWorkers * 2 of
				true ->
					ar_util:cast_after(1000, self(), collect_peer_intervals);
				false ->
					Self = self(),
					monitor(process, spawn(
						fun() ->
							find_peer_intervals(Start, min(End, DiskPoolThreshold), StoreID,
									Self, #{}),
							ar_util:cast_after(?COLLECT_SYNC_INTERVALS_FREQUENCY_MS, Self,
									collect_peer_intervals)
						end
					))
			end
	end,
	{noreply, State};

handle_cast({enqueue_intervals, #{}}, State) ->
	{noreply, State};
handle_cast({enqueue_intervals, Intervals}, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals } = State,
	PeersPerChunk = collect_all_peers_per_chunk(Intervals, QIntervals, #{}),
	{Q2, QIntervals2} = enqueue_intervals(PeersPerChunk, {Q, QIntervals}),
	% {Q2, QIntervals2} = lists:foldl(fun enqueue_intervals/2, {Q, QIntervals}, Intervals),
	{noreply, State#sync_data_state{ sync_intervals_queue = Q2,
			sync_intervals_queue_intervals = QIntervals2 }};
handle_cast(sync_intervals, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals, store_id = StoreID } = State,
	IsQueueEmpty =
		case gb_sets:is_empty(Q) of
			true ->
				ar_util:cast_after(500, self(), sync_intervals),
				true;
			false ->
				false
		end,
	IsDiskSpaceSufficient =
		case IsQueueEmpty of
			true ->
				false;
			false ->
				case is_disk_space_sufficient(StoreID) of
					false ->
						ar_util:cast_after(30000, self(), sync_intervals),
						false;
					true ->
						true
				end
		end,
	IsChunkCacheFull =
		case IsDiskSpaceSufficient of
			false ->
				true;
			true ->
				case is_chunk_cache_full() of
					true ->
						ar_util:cast_after(1000, self(), sync_intervals),
						true;
					false ->
						false
				end
		end,
	AreSyncWorkersBusy =
		case IsChunkCacheFull of
			true ->
				true;
			false ->
				{ok, Config} = application:get_env(arweave, config),
				SyncWorkers = Config#config.sync_jobs,
				ScheduledTasks = ar_data_sync_worker_master:get_scheduled_task_count(),
				case ScheduledTasks > SyncWorkers * 100 of
					true ->
						ar_util:cast_after(200, self(), sync_intervals),
						true;
					false ->
						false
				end
		end,
	case AreSyncWorkersBusy of
		true ->
			{noreply, State};
		false ->
			gen_server:cast(self(), sync_intervals),
			{{Start, End, Peer}, Q2} = gb_sets:take_smallest(Q),
			I2 = ar_intervals:delete(QIntervals, End, Start),
			gen_server:cast(ar_data_sync_worker_master,
					{sync_range, {Start, End, Peer, StoreID}}),
			{noreply, State#sync_data_state{ sync_intervals_queue = Q2,
					sync_intervals_queue_intervals = I2 }}
	end;

handle_cast({store_fetched_chunk, Peer, Time, TransferSize, Byte, Proof} = Cast, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	SeekByte = get_chunk_seek_offset(Byte + 1) - 1,
	{BlockStartOffset, BlockEndOffset, TXRoot} = ar_block_index:get_block_bounds(SeekByte),
	BlockSize = BlockEndOffset - BlockStartOffset,
	Offset = SeekByte - BlockStartOffset,
	{Strict, ValidateDataPathFun} =
		case BlockStartOffset >= ?STRICT_DATA_SPLIT_THRESHOLD of
			true ->
				{true, fun ar_merkle:validate_path_strict_data_split/4};
			false ->
				{false, fun ar_merkle:validate_path_strict_borders/4}
		end,
	case validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof,
			ValidateDataPathFun) of
		{need_unpacking, AbsoluteOffset, ChunkArgs, VArgs} ->
			{Packing, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkID} = VArgs,
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Strict, Peer, Byte},
			case maps:is_key({AbsoluteOffset, unpacked}, PackingMap) of
				true ->
					decrement_chunk_cache_size(),
					{noreply, State};
				false ->
					case ar_packing_server:is_buffer_full() of
						true ->
							ar_util:cast_after(1000, self(), Cast),
							{noreply, State};
						false ->
							ar_events:send(peer, {served_chunk, Peer, Time, TransferSize}),
							ar_events:send(chunk, {unpack_request, AbsoluteOffset, ChunkArgs}),
							?LOG_DEBUG([{event, requested_fetched_chunk_unpacking},
									{data_path_hash, ar_util:encode(crypto:hash(sha256,
											DataPath))},
									{data_root, ar_util:encode(DataRoot)},
									{absolute_end_offset, AbsoluteOffset}]),
							ar_util:cast_after(600000, self(),
									{expire_unpack_fetched_chunk_request,
									{AbsoluteOffset, unpacked}}),
							{noreply, State#sync_data_state{
									packing_map = PackingMap#{
										{AbsoluteOffset, unpacked} => {unpack_fetched_chunk,
												Args} } }}
					end
			end;
		false ->
			decrement_chunk_cache_size(),
			process_invalid_fetched_chunk(Peer, Byte, State);
		{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkSize, ChunkID} ->
			ar_events:send(peer, {served_chunk, Peer, Time, TransferSize}),
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			AbsoluteEndOffset = AbsoluteTXStartOffset + ChunkEndOffset,
			ChunkArgs = {unpacked, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Strict, Peer, Byte},
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end;

handle_cast(process_disk_pool_item, #sync_data_state{ disk_pool_scan_pause = true } = State) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, State) ->
	#sync_data_state{ disk_pool_cursor = Cursor, disk_pool_chunks_index = DiskPoolChunksIndex,
			disk_pool_full_scan_start_key = FullScanStartKey,
			disk_pool_full_scan_start_timestamp = Timestamp,
			currently_processed_disk_pool_keys = CurrentlyProcessedDiskPoolKeys } = State,
	NextKey =
		case ar_kv:get_next(DiskPoolChunksIndex, Cursor) of
			{ok, Key1, Value1} ->
				case sets:is_element(Key1, CurrentlyProcessedDiskPoolKeys) of
					true ->
						none;
					false ->
						{ok, Key1, Value1}
				end;
			none ->
				case ar_kv:get_next(DiskPoolChunksIndex, first) of
					none ->
						none;
					{ok, Key2, Value2} ->
						case sets:is_element(Key2, CurrentlyProcessedDiskPoolKeys) of
							true ->
								none;
							false ->
								{ok, Key2, Value2}
						end
				end
		end,
	case NextKey of
		none ->
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), resume_disk_pool_scan),
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = first,
					disk_pool_full_scan_start_key = none, disk_pool_scan_pause = true }};
		{ok, Key3, Value3} ->
			case FullScanStartKey of
				none ->
					process_disk_pool_item(State#sync_data_state{
							disk_pool_full_scan_start_key = Key3,
							disk_pool_full_scan_start_timestamp = erlang:timestamp() },
							Key3, Value3);
				Key3 ->
					TimePassed = timer:now_diff(erlang:timestamp(), Timestamp),
					case TimePassed < (?DISK_POOL_SCAN_DELAY_MS) * 1000 of
						true ->
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(),
									resume_disk_pool_scan),
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(),
									process_disk_pool_item),
							{noreply, State#sync_data_state{ disk_pool_cursor = first,
									disk_pool_full_scan_start_key = none,
									disk_pool_scan_pause = true }};
						false ->
							process_disk_pool_item(State, Key3, Value3)
					end;
				_ ->
					process_disk_pool_item(State, Key3, Value3)
			end
	end;

handle_cast(resume_disk_pool_scan, State) ->
	{noreply, State#sync_data_state{ disk_pool_scan_pause = false }};

handle_cast({process_disk_pool_chunk_offsets, Iterator, MayConclude, Args}, State) ->
	{Offset, _, _, _, _, _, Key, _} = Args,
	%% Place the chunk under its last 10 offsets in the weave (the same data
	%% may be uploaded several times).
	case data_root_index_next_v2(Iterator, 10) of
		none ->
			State2 =
				case MayConclude of
					true ->
						Iterator2 = data_root_index_reset(Iterator),
						delete_disk_pool_chunk(Iterator2, Args, State),
						may_be_reset_disk_pool_full_scan_key(Key, State);
					false ->
						State
				end,
			gen_server:cast(self(), process_disk_pool_item),
			{noreply, deregister_currently_processed_disk_pool_key(Key, State2)};
		{TXArgs, Iterator2} ->
			State2 = register_currently_processed_disk_pool_key(Key, State),
			{TXStartOffset, TXRoot, TXPath} = TXArgs,
			AbsoluteOffset = TXStartOffset + Offset,
			process_disk_pool_chunk_offset(Iterator2, TXRoot, TXPath, AbsoluteOffset,
					MayConclude, Args, State2)
	end;

handle_cast({remove_range, End, Cursor, Ref, PID}, State) when Cursor > End ->
	PID ! {removed_range, Ref},
	{noreply, State};
handle_cast({remove_range, End, Cursor, Ref, PID}, State) ->
	#sync_data_state{ chunks_index = ChunksIndex, store_id = StoreID } = State,
	case get_chunk_by_byte(ChunksIndex, Cursor) of
		{ok, _Key, {AbsoluteOffset, _, _, _, _, _, _}} 
				when AbsoluteOffset > End ->
			PID ! {removed_range, Ref},
			{noreply, State};
		{ok, Key, {AbsoluteOffset, _, _, _, _, _, ChunkSize}} ->
			PaddedStartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
			PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
			%% 1) store updated sync record
			%% 2) remove chunk
			%% 3) update chunks_index
			%%
			%% The order is important - in case the VM crashes,
			%% we will not report false positives to peers,
			%% and the chunk can still be removed upon retry.
			RemoveFromSyncRecord = ar_sync_record:delete(PaddedOffset,
					PaddedStartOffset, ?MODULE, StoreID),
			RemoveFromChunkStorage =
				case RemoveFromSyncRecord of
					ok ->
						ar_chunk_storage:delete(PaddedOffset, StoreID);
					Error ->
						Error
				end,
			RemoveFromChunksIndex =
				case RemoveFromChunkStorage of
					ok ->
						ar_kv:delete(ChunksIndex, Key);
					Error2 ->
						Error2
				end,
			case RemoveFromChunksIndex of
				ok ->
					NextCursor = AbsoluteOffset + 1,
					gen_server:cast(self(), {remove_range, End, NextCursor, Ref, PID});
				{error, Reason} ->
					?LOG_ERROR([{event,
							data_removal_aborted_since_failed_to_remove_chunk},
							{offset, Cursor},
							{reason, io_lib:format("~p", [Reason])}])
			end,
			{noreply, State};
		{error, invalid_iterator} ->
			%% get_chunk_by_byte looks for a key with the same prefix or the next prefix.
			%% Therefore, if there is no such key, it does not make sense to look for any
			%% key smaller than the prefix + 2 in the next iteration.
			PrefixSpaceSize =
					trunc(math:pow(2, ?OFFSET_KEY_BITSIZE - ?OFFSET_KEY_PREFIX_BITSIZE)),
			NextCursor = ((Cursor div PrefixSpaceSize) + 2) * PrefixSpaceSize,
			gen_server:cast(self(), {remove_range, End, NextCursor, Ref, PID}),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([{event, data_removal_aborted_since_failed_to_query_chunk},
					{offset, Cursor}, {reason, io_lib:format("~p", [Reason])}]),
			{noreply, State}
	end;

handle_cast({expire_repack_chunk_request, Key}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{pack_chunk, {_, DataPath, Offset, DataRoot, _, _, _}} ->
			decrement_chunk_cache_size(),
			DataPathHash = crypto:hash(sha256, DataPath),
			?LOG_DEBUG([{event, expired_repack_chunk_request},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_root, ar_util:encode(DataRoot)},
					{relative_offset, Offset}]),
			{noreply, State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast({expire_unpack_fetched_chunk_request, Key}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Key, PackingMap, not_found) of
		{unpack_fetched_chunk, _Args} ->
			decrement_chunk_cache_size(),
			{noreply, State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast(store_sync_state, State) ->
	store_sync_state(State),
	ar_util:cast_after(?STORE_STATE_FREQUENCY_MS, self(), store_sync_state),
	{noreply, State};

handle_cast({remove_recently_processed_disk_pool_offset, Offset, ChunkDataKey}, State) ->
	{noreply, remove_recently_processed_disk_pool_offset(Offset, ChunkDataKey, State)};

handle_cast({request_default_storage_2_5_repacking, Cursor, RightBound}, State) ->
	case ar_sync_record:get_next_synced_interval(Cursor, RightBound, spora_2_5, ?MODULE,
			"default") of
		not_found ->
			ok;
		{End, Start} ->
			gen_server:cast(ar_data_sync_worker_master, {read_range, {Start, End,
					"default", "default", true}}),
			gen_server:cast(ar_data_sync_default, {request_default_storage_2_5_repacking,
					End, RightBound})
	end,
	{noreply, State};

handle_cast({request_default_unpacked_packing, Cursor, RightBound}, State) ->
	case ar_sync_record:get_next_synced_interval(Cursor, RightBound, unpacked, ?MODULE,
			"default") of
		not_found ->
			ok;
		{End, Start} when End - Start < ?DATA_CHUNK_SIZE,
				End =< ?STRICT_DATA_SPLIT_THRESHOLD ->
			gen_server:cast(ar_data_sync_default, {request_default_unpacked_packing, End,
					RightBound});
		{End, Start} ->
			gen_server:cast(ar_data_sync_default, {read_range, {Start, End, "default",
					"default", true}}),
			gen_server:cast(ar_data_sync_default, {request_default_unpacked_packing, End,
					RightBound})
	end,
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_call({add_block, B, SizeTaggedTXs}, _From, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{reply, add_block(B, SizeTaggedTXs, StoreID), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_info({event, node_state, {initialized, _B}},
		#sync_data_state{ store_id = "default",
				disk_pool_threshold = DiskPoolThreshold } = State) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(legacy_storage_repacking, Config#config.enable)
			orelse lists:member(legacy_storage_unpacked_packing, Config#config.enable) of
		true ->
			Addr = Config#config.mining_addr,
			Packing = {spora_2_6, Addr},
			ar:console("Initiating the default storage repacking. The upper bound: ~B. "
					"The new packing: ~s.~n", [DiskPoolThreshold, ar_util:encode(Addr)]),
			gen_server:cast(ar_chunk_storage_default, {repack, DiskPoolThreshold, Packing});
		false ->
			ok
	end,
	{noreply, State};
handle_info({event, node_state, {initialized, _B}}, State) ->
	{noreply, may_be_start_syncing(State)};

handle_info({event, node_state, {search_space_upper_bound, Bound}}, State) ->
	{noreply, State#sync_data_state{ disk_pool_threshold = Bound }};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info({event, chunk, {unpacked, Offset, ChunkArgs}}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	Key = {Offset, unpacked},
	case maps:get(Key, PackingMap, not_found) of
		{unpack_fetched_chunk, Args} ->
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			process_unpacked_chunk(ChunkArgs, Args, State2);
		_ ->
			{noreply, State}
	end;

handle_info({event, chunk, {packed, Offset, ChunkArgs}}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	Packing = element(1, ChunkArgs),
	Key = {Offset, Packing},
	case maps:get(Key, PackingMap, not_found) of
		{pack_chunk, Args} when element(1, Args) == Packing ->
			State2 = State#sync_data_state{ packing_map = maps:remove(Key, PackingMap) },
			{noreply, store_chunk(ChunkArgs, Args, State2)};
		_ ->
			{noreply, State}
	end;

handle_info({event, chunk, _}, State) ->
	{noreply, State};

handle_info({event, disksup, {remaining_disk_space, StoreID, false, Percentage, _Bytes}},
		#sync_data_state{ store_id = StoreID } = State) ->
	case Percentage < 0.01 of
		true ->
			case is_disk_space_sufficient(StoreID) of
				false ->
					ok;
				_ ->
					log_insufficient_disk_space(StoreID)
			end,
			ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, false});
		false ->
			case Percentage > 0.05 of
				true ->
					case is_disk_space_sufficient(StoreID) of
						false ->
							log_sufficient_disk_space(StoreID);
						_ ->
							ok
					end,
					ets:insert(ar_data_sync_state,
							{{is_disk_space_sufficient, StoreID}, true});
				false ->
					ok
			end
	end,
	{noreply, State};
handle_info({event, disksup, {remaining_disk_space, StoreID, true, _Percentage, Bytes}},
		#sync_data_state{ store_id = StoreID } = State) ->
	{ok, Config} = application:get_env(arweave, config),
	%% Default values:
	%% max_disk_pool_buffer_mb = ?DEFAULT_MAX_DISK_POOL_BUFFER_MB = 100_000
	%% disk_cache_size = ?DISK_CACHE_SIZE = 5_120
	%% DiskPoolSize = ~100GB
	%% DisckCacheSize = ~5GB
	%% BufferSize = ~10GB
	DiskPoolSize = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	DiskCacheSize = Config#config.disk_cache_size * 1024 * 1024,
	BufferSize = 10_000_000_000,
	case Bytes < DiskPoolSize + DiskCacheSize + (BufferSize div 2) of
		true ->
			case is_disk_space_sufficient(StoreID) of
				false ->
					ok;
				_ ->
					log_insufficient_disk_space(StoreID)
			end,
			ets:insert(ar_data_sync_state, {{is_disk_space_sufficient, StoreID}, false});
		false ->
			case Bytes > DiskPoolSize + DiskCacheSize + BufferSize of
				true ->
					case is_disk_space_sufficient(StoreID) of
						false ->
							log_sufficient_disk_space(StoreID);
						_ ->
							ok
					end,
					ets:insert(ar_data_sync_state,
							{{is_disk_space_sufficient, StoreID}, true});
				false ->
					ok
			end
	end,
	{noreply, State};

handle_info({event, disksup, _}, State) ->
	{noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
	{noreply, State};

handle_info({'DOWN', _,  process, _, normal}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, noproc}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, Reason}, State) ->
	?LOG_WARNING([{event, collect_intervals_job_failed},
			{reason, io_lib:format("~p", [Reason])}, {action, spawning_another_one}]),
	gen_server:cast(self(), collect_peer_intervals),
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, State) ->
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	store_sync_state(State).

%%%===================================================================
%%% Private functions.
%%%===================================================================

remove_expired_disk_pool_data_roots() ->
	Now = os:system_time(microsecond),
	{ok, Config} = application:get_env(arweave, config),
	ExpirationTime = Config#config.disk_pool_data_root_expiration_time * 1000000,
	ets:foldl(
		fun({Key, {_Size, Timestamp, _TXIDSet}}, _Acc) ->
			case Timestamp + ExpirationTime > Now of
				true ->
					ok;
				false ->
					ets:delete(ar_disk_pool_data_roots, Key),
					ok
			end
		end,
		ok,
		ar_disk_pool_data_roots
	).

get_chunk(Offset, SeekOffset, Pack, Packing, StoredPacking, StoreID) ->
	case read_chunk_with_metadata(Offset, SeekOffset, StoredPacking, StoreID) of
		{error, Reason} ->
			{error, Reason};
		{ok, {Chunk, DataPath}, AbsoluteOffset, TXRoot, ChunkSize, TXPath} ->
			ChunkID =
				case validate_served_chunk({AbsoluteOffset, DataPath, TXPath, TXRoot,
						ChunkSize, StoreID}) of
					{true, ID} ->
						ID;
					false ->
						error
				end,
			PackResult =
				case {ChunkID, Packing == StoredPacking, Pack} of
					{error, _, _} ->
						%% Chunk was read but could not be validated.
						{error, chunk_not_found};
					{_, false, false} ->
						%% Requested and stored chunk are in different formats,
						%% and repacking is disabled.
						{error, chunk_not_found};
					_ ->
						ar_packing_server:repack(
							Packing, StoredPacking, AbsoluteOffset, TXRoot, Chunk, ChunkSize)
				end,
			case {PackResult, ChunkID} of
				{{error, Reason}, _} ->
					{error, Reason};
				{{ok, PackedChunk, none}, _} ->
					%% PackedChunk is the requested format.
					Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
							data_path => DataPath, tx_path => TXPath },
					{ok, Proof};
				{{ok, PackedChunk, _}, none} ->
					%% PackedChunk is the requested format, but the ChunkID could
					%% not be determined
					Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
							data_path => DataPath, tx_path => TXPath,
							end_offset => AbsoluteOffset },
					{ok, Proof};
				{{ok, PackedChunk, MaybeUnpackedChunk}, _} ->
					case ar_tx:generate_chunk_id(MaybeUnpackedChunk) == ChunkID of
						true ->
							Proof = #{ tx_root => TXRoot, chunk => PackedChunk,
									data_path => DataPath, tx_path => TXPath },
							{ok, Proof};
						false ->
							invalidate_bad_data_record({AbsoluteOffset - ChunkSize,
								AbsoluteOffset, {chunks_index, StoreID}, StoreID, 4}),
							{error, chunk_not_found}
					end
			end
	end.

%% @doc Read the chunk as well as its metadata.
%% Response is of the format:
%% {ok, {Chunk, DataPath}, AbsoluteOffset, TXRoot, ChunkSize, TXPath}
read_chunk_with_metadata(
		Offset, SeekOffset, StoredPacking, StoreID) ->
	case get_chunk_by_byte({chunks_index, StoreID}, SeekOffset) of
		{error, _} ->
			{error, chunk_not_found};
		{ok, _, {AbsoluteOffset, _, _, _, _, _, ChunkSize}} 
				when AbsoluteOffset - SeekOffset >= ChunkSize ->
			case ar_sync_record:delete(AbsoluteOffset - ChunkSize, SeekOffset - 1, ?MODULE,
					StoreID) of
				ok ->
					?LOG_DEBUG([{event, clean_up_sync_record_false_positive},
							{left, SeekOffset - 1}, {right, AbsoluteOffset - ChunkSize}]);
				Err ->
					?LOG_WARNING([{event, failed_to_clean_up_sync_record},
							{error, io_lib:format("~p", [Err])}])
			end,
			{error, chunk_not_found};
		{ok, _, {AbsoluteOffset, ChunkDataKey, TXRoot, _, TXPath, _, ChunkSize}} ->
			case read_chunk(AbsoluteOffset, {chunk_data_db, StoreID}, ChunkDataKey, StoreID) of
				not_found ->
					invalidate_bad_data_record({SeekOffset - 1, AbsoluteOffset,
							{chunks_index, StoreID}, StoreID, 1}),
					{error, chunk_not_found};
				{error, Error} ->
					?LOG_ERROR([{event, failed_to_read_chunk},
							{reason, io_lib:format("~p", [Error])},
							{chunk_data_key, ar_util:encode(ChunkDataKey)},
							{absolute_end_offset, Offset}]),
					{error, failed_to_read_chunk};
				{ok, {Chunk, DataPath}} ->
					case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE,
							StoreID) of
						false ->
							%% The chunk should have been re-packed
							%% in the meantime - very unlucky timing.
							{error, chunk_not_found};
						true ->
							{ok, {Chunk, DataPath}, AbsoluteOffset, TXRoot, ChunkSize, TXPath}
					end
			end
	end.

invalidate_bad_data_record({Start, End, ChunksIndex, StoreID, Case}) ->
	[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
	case End > T of
		true ->
			%% Do not invalidate fresh records - a reorg may be in progress.
			ok;
		false ->
			PaddedEnd = get_chunk_padded_offset(End),
			PaddedStart = get_chunk_padded_offset(Start),
			PaddedStart2 =
				case PaddedStart == PaddedEnd of
					true ->
						PaddedEnd - ?DATA_CHUNK_SIZE;
					false ->
						PaddedStart
				end,
			?LOG_WARNING([{event, invalidating_bad_data_record}, {type, Case},
					{range_start, PaddedStart2}, {range_end, PaddedEnd}]),
			case ar_sync_record:delete(PaddedEnd, PaddedStart2, ?MODULE, StoreID) of
				ok ->
					case ar_kv:delete(ChunksIndex, << End:?OFFSET_KEY_BITSIZE >>) of
						ok ->
							ok;
						Error2 ->
							?LOG_WARNING([{event, failed_to_remove_chunks_index_key},
									{absolute_end_offset, End},
									{error, io_lib:format("~p", [Error2])}])
					end;
				Error ->
					?LOG_WARNING([{event, failed_to_remove_sync_record_range},
							{range_end, PaddedEnd}, {range_start, PaddedStart2},
							{error, io_lib:format("~p", [Error])}])
			end
	end.

validate_served_chunk(Args) ->
	{Offset, DataPath, TXPath, TXRoot, ChunkSize, StoreID} = Args,
	[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
	case Offset > T orelse not ar_node:is_joined() of
		true ->
			{true, none};
		false ->
			case ar_block_index:get_block_bounds(Offset - 1) of
				{BlockStart, BlockEnd, TXRoot} ->
					{_Strict, ValidateDataPathFun} =
						case BlockStart >= ?STRICT_DATA_SPLIT_THRESHOLD of
							true ->
								{true, fun ar_merkle:validate_path_strict_data_split/4};
							false ->
								{false, fun ar_merkle:validate_path_strict_borders/4}
						end,
					BlockSize = BlockEnd - BlockStart,
					ChunkOffset = Offset - BlockStart - 1,
					case validate_proof2({TXRoot, ChunkOffset, BlockSize, DataPath, TXPath,
							ChunkSize, ValidateDataPathFun}) of
						{true, ChunkID} ->
							{true, ChunkID};
						false ->
							StartOffset = Offset - ChunkSize,
							invalidate_bad_data_record({StartOffset, Offset, {chunks_index, StoreID},
									StoreID, 2}),
							false
					end;
				{_BlockStart, _BlockEnd, TXRoot2} ->
					?LOG_WARNING([{event, stored_chunk_invalid_tx_root},
							{byte, Offset - 1}, {tx_root, ar_util:encode(TXRoot2)},
							{stored_tx_root, ar_util:encode(TXRoot)}]),
					invalidate_bad_data_record({Offset - ChunkSize, Offset,  {chunks_index, StoreID},
							StoreID, 3}),
					false
			end
	end.

%% @doc Return Offset if it is smaller than or equal to ?STRICT_DATA_SPLIT_THRESHOLD.
%% Otherwise, return the offset of the first byte of the chunk + 1. The function
%% returns an offset the chunk can be found under even if Offset is inside padding.
get_chunk_seek_offset(Offset) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			ar_poa:get_padded_offset(Offset, ?STRICT_DATA_SPLIT_THRESHOLD)
					- (?DATA_CHUNK_SIZE)
					+ 1;
		false ->
			Offset
	end.

get_tx_offset(TXIndex, TXID) ->
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_read_tx_offset},
					{reason, io_lib:format("~p", [Reason])},
					{tx, ar_util:encode(TXID)}]),
			{error, failed_to_read_offset}
	end.

get_tx_data(Start, End, Chunks) when Start >= End ->
	{ok, iolist_to_binary(Chunks)};
get_tx_data(Start, End, Chunks) ->
	case get_chunk(Start + 1, #{ pack => true, packing => unpacked,
			bucket_based_offset => false }) of
		{ok, #{ chunk := Chunk }} ->
			get_tx_data(Start + byte_size(Chunk), End, [Chunks | Chunk]);
		{error, chunk_not_found} ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_get_tx_data},
					{reason, io_lib:format("~p", [Reason])}]),
			{error, failed_to_get_tx_data}
	end.

get_data_root_offset(DataRootKey, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	DataRootIndex = {data_root_index, StoreID},
	case ar_kv:get_prev(DataRootIndex, << DataRoot:32/binary,
			(ar_serialize:encode_int(TXSize, 8))/binary, <<"a">>/binary >>) of
		none ->
			not_found;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				OffsetSize:8, Offset:(OffsetSize * 8) >>, TXPath} ->
			{ok, {Offset, TXPath}};
		{ok, _, _} ->
			not_found;
		{error, _} = Error ->
			Error
	end.

remove_range(Start, End, Ref, ReplyTo) ->
	ReplyFun =
		fun(Fun, StorageRefs) ->
			case sets:is_empty(StorageRefs) of
				true ->
					ReplyTo ! {removed_range, Ref},
					ar_events:send(data_sync, {remove_range, Start, End});
				false ->
					receive
						{removed_range, StorageRef} ->
							Fun(Fun, sets:del_element(StorageRef, StorageRefs))
					after 10000 ->
						?LOG_DEBUG([{event,
								waiting_for_data_range_removal_longer_than_ten_seconds}]),
						Fun(Fun, StorageRefs)
					end
			end
		end,
	StorageModules = ar_storage_module:get_all(Start, End),
	StoreIDs = ["default" | [ar_storage_module:id(M) || M <- StorageModules]],
	RefL = [make_ref() || _ <- StoreIDs],
	PID = spawn(fun() -> ReplyFun(ReplyFun, sets:from_list(RefL)) end),
	lists:foreach(
		fun({StoreID, R}) ->
			GenServerID = list_to_atom("ar_data_sync_" ++ StoreID),
			gen_server:cast(GenServerID, {remove_range, End, Start + 1, R, PID})
		end,
		lists:zip(StoreIDs, RefL)
	).

init_kv(StoreID) ->
	BasicOpts = [{max_open_files, 10000}],
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
	Dir =
		case StoreID of
			"default" ->
				?ROCKS_DB_DIR;
			_ ->
				filename:join(["storage_modules", StoreID, ?ROCKS_DB_DIR])
		end,
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_db"), ColumnFamilyDescriptors, [],
			[{?MODULE, StoreID}, {chunks_index, StoreID}, {data_root_index_old, StoreID},
			{data_root_offset_index, StoreID}, {tx_index, StoreID}, {tx_offset_index, StoreID},
			{disk_pool_chunks_index_old, StoreID}, {migrations_index, StoreID}]),
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_chunk_db"), [{max_open_files, 10000},
			{max_background_compactions, 8},
			{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
			{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}], {chunk_data_db, StoreID}),
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_disk_pool_chunks_index_db"), [
			{max_open_files, 1000}, {max_background_compactions, 8},
			{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
			{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}] ++ BloomFilterOpts,
			{disk_pool_chunks_index, StoreID}),
	ok = ar_kv:open(filename:join(Dir, "ar_data_sync_data_root_index_db"), [
			{max_open_files, 100}, {max_background_compactions, 8},
			{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
			{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}] ++ BloomFilterOpts,
			{data_root_index, StoreID}),
	#sync_data_state{
		chunks_index = {chunks_index, StoreID},
		data_root_index = {data_root_index, StoreID},
		data_root_index_old = {data_root_index_old, StoreID},
		data_root_offset_index = {data_root_offset_index, StoreID},
		chunk_data_db = {chunk_data_db, StoreID},
		tx_index = {tx_index, StoreID},
		tx_offset_index = {tx_offset_index, StoreID},
		disk_pool_chunks_index = {disk_pool_chunks_index, StoreID},
		disk_pool_chunks_index_old = {disk_pool_chunks_index_old, StoreID},
		migrations_index = {migrations_index, StoreID}
	}.

move_disk_pool_index(State) ->
	move_disk_pool_index(first, State).

move_disk_pool_index(Cursor, State) ->
	#sync_data_state{ disk_pool_chunks_index_old = Old,
			disk_pool_chunks_index = New } = State,
	case ar_kv:get_next(Old, Cursor) of
		none ->
			ok;
		{ok, Key, Value} ->
			ok = ar_kv:put(New, Key, Value),
			ok = ar_kv:delete(Old, Key),
			move_disk_pool_index(Key, State)
	end.

move_data_root_index(#sync_data_state{ migrations_index = MI,
		data_root_index_old = DI } = State) ->
	case ar_kv:get(MI, <<"move_data_root_index">>) of
		{ok, <<"complete">>} ->
			ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
			ok;
		{ok, Cursor} ->
			move_data_root_index(Cursor, 1, State);
		not_found ->
			case ar_kv:get_next(DI, last) of
				none ->
					ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
					ok;
				{ok, Key, _} ->
					move_data_root_index(Key, 1, State)
			end
	end.

move_data_root_index(Cursor, N, State) ->
	#sync_data_state{ migrations_index = MI, data_root_index_old = Old,
			data_root_index = New } = State,
	case N rem 50000 of
		0 ->
			?LOG_DEBUG([{event, moving_data_root_index}, {moved_keys, N}]),
			ok = ar_kv:put(MI, <<"move_data_root_index">>, Cursor),
			gen_server:cast(self(), {move_data_root_index, Cursor, N + 1});
		_ ->
			case ar_kv:get_prev(Old, Cursor) of
				none ->
					ok = ar_kv:put(MI, <<"move_data_root_index">>, <<"complete">>),
					ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
					ok;
				{ok, << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Value} ->
					M = binary_to_term(Value),
					move_data_root_index(DataRoot, TXSize, data_root_index_iterator(M), New),
					PrevKey = << DataRoot:32/binary, (TXSize - 1):?OFFSET_KEY_BITSIZE >>,
					move_data_root_index(PrevKey, N + 1, State);
				{ok, Key, _} ->
					%% The empty data root key (from transactions without data) was
					%% unnecessarily recorded in the index.
					PrevKey = binary:part(Key, 0, byte_size(Key) - 1),
					move_data_root_index(PrevKey, N + 1, State)
			end
	end.

move_data_root_index(DataRoot, TXSize, Iterator, DB) ->
	case data_root_index_next(Iterator, infinity) of
		none ->
			ok;
		{{Offset, _TXRoot, TXPath}, Iterator2} ->
			Key = data_root_key_v2(DataRoot, TXSize, Offset),
			ok = ar_kv:put(DB, Key, TXPath),
			move_data_root_index(DataRoot, TXSize, Iterator2, DB)
	end.

data_root_key_v2(DataRoot, TXSize, Offset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(Offset, 8))/binary >>.

record_disk_pool_chunks_count() ->
	DB = {disk_pool_chunks_index, "default"},
	case ar_kv:count(DB) of
		Count when is_integer(Count) ->
			prometheus_gauge:set(disk_pool_chunks_count, Count);
		Error ->
			?LOG_WARNING([{event, failed_to_read_disk_pool_chunks_count},
					{error, io_lib:format("~p", [Error])}])
	end.

read_data_sync_state() ->
	case ar_storage:read_term(data_sync_state) of
		{ok, #{ block_index := RecentBI } = M} ->
			maps:merge(M, #{
				weave_size => case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
				disk_pool_threshold => maps:get(disk_pool_threshold, M,
						get_disk_pool_threshold(RecentBI)) });
		not_found ->
			#{ block_index => [], disk_pool_data_roots => #{}, disk_pool_size => 0,
					weave_size => 0, packing_2_5_threshold => infinity,
					disk_pool_threshold => 0 }
	end.

may_be_start_syncing(#sync_data_state{ started_syncing = StartedSyncing } = State) ->
	case ar_node:is_joined() of
		false ->
			State;
		true ->
			case StartedSyncing of
				true ->
					State;
				false ->
					{ok, Config} = application:get_env(arweave, config),
					case Config#config.sync_jobs > 0 of
						true ->
							gen_server:cast(self(), sync_data),
							State#sync_data_state{ started_syncing = true };
						false ->
							State
					end
			end
	end.

recalculate_disk_pool_size(DataRootMap, State) ->
	#sync_data_state{ disk_pool_chunks_index = Index } = State,
	DataRootMap2 = maps:map(fun(_DataRootKey, {_Size, Timestamp, TXIDSet}) ->
			{0, Timestamp, TXIDSet} end, DataRootMap),
	recalculate_disk_pool_size(Index, DataRootMap2, first, 0).

recalculate_disk_pool_size(Index, DataRootMap, Cursor, Sum) ->
	case ar_kv:get_next(Index, Cursor) of
		none ->
			prometheus_gauge:set(pending_chunks_size, Sum),
			maps:map(fun(DataRootKey, V) -> ets:insert(ar_disk_pool_data_roots,
					{DataRootKey, V}) end, DataRootMap),
			ets:insert(ar_data_sync_state, {disk_pool_size, Sum});
		{ok, Key, Value} ->
			DecodedValue = binary_to_term(Value),
			ChunkSize = element(2, DecodedValue),
			DataRoot = element(3, DecodedValue),
			TXSize = element(4, DecodedValue),
			DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
			DataRootMap2 =
				case maps:get(DataRootKey, DataRootMap, not_found) of
					not_found ->
						DataRootMap;
					{Size, Timestamp, TXIDSet} ->
						maps:put(DataRootKey, {Size + ChunkSize, Timestamp, TXIDSet},
								DataRootMap)
				end,
			Cursor2 = << Key/binary, <<"a">>/binary >>,
			recalculate_disk_pool_size(Index, DataRootMap2, Cursor2, Sum + ChunkSize)
	end.

get_disk_pool_threshold([]) ->
	0;
get_disk_pool_threshold(BI) ->
	ar_node:get_partition_upper_bound(BI).

remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
	ok = remove_tx_index_range(BlockStartOffset, WeaveSize, State),
	{ok, OrphanedDataRoots} = remove_data_root_index_range(BlockStartOffset, WeaveSize, State),
	ok = remove_data_root_offset_index_range(BlockStartOffset, WeaveSize, State),
	ok = remove_chunks_index_range(BlockStartOffset, WeaveSize, State),
	{ok, OrphanedDataRoots}.

remove_tx_index_range(Start, End, State) ->
	#sync_data_state{ tx_offset_index = TXOffsetIndex, tx_index = TXIndex } = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
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
	ar_kv:delete_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< End:?OFFSET_KEY_BITSIZE >>).

remove_chunks_index_range(Start, End, State) ->
	#sync_data_state{ chunks_index = ChunksIndex } = State,
	ar_kv:delete_range(ChunksIndex, << (Start + 1):?OFFSET_KEY_BITSIZE >>,
			<< (End + 1):?OFFSET_KEY_BITSIZE >>).

remove_data_root_index_range(Start, End, State) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex,
			data_root_index = DataRootIndex } = State,
	case ar_kv:get_range(DataRootOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, sets:new()};
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, {ok, RemovedDataRoots}) ->
						{_TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
						sets:fold(
							fun (_Key, {error, _} = Error) ->
									Error;
								(<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = Key,
										{ok, Removed}) ->
									case remove_data_root(DataRootIndex, Key, Start, End) of
										removed ->
											{ok, sets:add_element(Key, Removed)};
										ok ->
											{ok, Removed};
										Error ->
											Error
									end;
								(_, Acc) ->
									Acc
							end,
							{ok, RemovedDataRoots},
							DataRootIndexKeySet
						)
				end,
				{ok, sets:new()},
				Map
			);
		Error ->
			Error
	end.

remove_data_root(DataRootIndex, DataRootKey, Start, End) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = data_root_key_v2(DataRoot, TXSize, Start),
	EndKey = data_root_key_v2(DataRoot, TXSize, End),
	case ar_kv:delete_range(DataRootIndex, StartKey, EndKey) of
		ok ->
			case ar_kv:get_prev(DataRootIndex, StartKey) of
				{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
						_Rest/binary >>, _} ->
					ok;
				{ok, _, _} ->
					removed;
				none ->
					removed;
				{error, _} = Error ->
					Error
			end;
		Error ->
			Error
	end.

remove_data_root_offset_index_range(Start, End, State) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex } = State,
	ar_kv:delete_range(DataRootOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< End:?OFFSET_KEY_BITSIZE >>).

repair_data_root_offset_index(BI, State) ->
	#sync_data_state{ migrations_index = DB } = State,
	case ar_kv:get(DB, <<"repair_data_root_offset_index">>) of
		not_found ->
			?LOG_INFO([{event, starting_data_root_offset_index_scan}]),
			ReverseBI = lists:reverse(BI),
			ResyncBlocks = repair_data_root_offset_index(ReverseBI, <<>>, 0, [], State),
			[ar_header_sync:remove_block(Height) || Height <- ResyncBlocks],
			ok = ar_kv:put(DB, <<"repair_data_root_offset_index">>, <<>>),
			?LOG_INFO([{event, data_root_offset_index_scan_complete}]);
		_ ->
			ok
	end.

repair_data_root_offset_index(BI, Cursor, Height, ResyncBlocks, State) ->
	#sync_data_state{ data_root_offset_index = DRI } = State,
	case ar_kv:get_next(DRI, Cursor) of
		none ->
			ResyncBlocks;
		{ok, Key, Value} ->
			<< BlockStart:?OFFSET_KEY_BITSIZE >> = Key,
			{TXRoot, BlockSize, _DataRootKeys} = binary_to_term(Value),
			BlockEnd = BlockStart + BlockSize,
			case shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks, BI) of
				{ok, {Height2, BI2}} ->
					Cursor2 = << (BlockStart + 1):?OFFSET_KEY_BITSIZE >>,
					repair_data_root_offset_index(BI2, Cursor2, Height2, ResyncBlocks, State);
				{bad_key, []} ->
					ResyncBlocks;
				{bad_key, ResyncBlocks2} ->
					?LOG_INFO([{event, removing_data_root_index_range},
							{range_start, BlockStart}, {range_end, BlockEnd}]),
					ok = remove_tx_index_range(BlockStart, BlockEnd, State),
					{ok, _} = remove_data_root_index_range(BlockStart, BlockEnd, State),
					ok = remove_data_root_offset_index_range(BlockStart, BlockEnd, State),
					repair_data_root_offset_index(BI, Cursor, Height, ResyncBlocks2, State)
			end
	end.

shift_block_index(_TXRoot, _BlockStart, _BlockEnd, _Height, ResyncBlocks, []) ->
	{bad_key, ResyncBlocks};
shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks,
		[{_H, WeaveSize, _TXRoot} | BI]) when BlockEnd > WeaveSize ->
	ResyncBlocks2 = case BlockStart < WeaveSize of true -> [Height | ResyncBlocks];
			_ -> ResyncBlocks end,
	shift_block_index(TXRoot, BlockStart, BlockEnd, Height + 1, ResyncBlocks2, BI);
shift_block_index(TXRoot, _BlockStart, WeaveSize, Height, _ResyncBlocks,
		[{_H, WeaveSize, TXRoot} | BI]) ->
	{ok, {Height + 1, BI}};
shift_block_index(_TXRoot, _BlockStart, _WeaveSize, Height, ResyncBlocks, _BI) ->
	{bad_key, [Height | ResyncBlocks]}.

add_block(B, SizeTaggedTXs, StoreID) ->
	#block{ indep_hash = H, weave_size = WeaveSize, tx_root = TXRoot } = B,
	case ar_block_index:get_element_by_height(B#block.height) of
		{H, WeaveSize, TXRoot} ->
			BlockStart = B#block.weave_size - B#block.block_size,
			case ar_kv:get({data_root_offset_index, StoreID},
					<< BlockStart:?OFFSET_KEY_BITSIZE >>) of
				not_found ->
					{ok, _} = add_block_data_roots(SizeTaggedTXs, BlockStart, StoreID),
					ok = update_tx_index(SizeTaggedTXs, BlockStart, StoreID),
					ok;
				_ ->
					ok
			end;
		_ ->
			ok
	end.

update_tx_index([], _BlockStartOffset, _StoreID) ->
	ok;
update_tx_index(SizeTaggedTXs, BlockStartOffset, StoreID) ->
	lists:foldl(
		fun ({_, Offset}, Offset) ->
				Offset;
			({{padding, _}, Offset}, _) ->
				Offset;
			({{TXID, _}, TXEndOffset}, PreviousOffset) ->
				AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
				TXSize = TXEndOffset - PreviousOffset,
				AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
				case ar_kv:put({tx_offset_index, StoreID},
						<< AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>, TXID) of
					ok ->
						case ar_kv:put({tx_index, StoreID}, TXID,
								term_to_binary({AbsoluteEndOffset, TXSize})) of
							ok ->
								ar_tx_blacklist:notify_about_added_tx(TXID, AbsoluteEndOffset,
										AbsoluteStartOffset),
								TXEndOffset;
							{error, Reason} ->
								?LOG_ERROR([{event, failed_to_update_tx_index},
										{reason, io_lib:format("~p", [Reason])},
										{tx, ar_util:encode(TXID)}]),
								TXEndOffset
						end;
					{error, Reason} ->
						?LOG_ERROR([{event, failed_to_update_tx_offset_index},
								{reason, io_lib:format("~p", [Reason])},
								{tx, ar_util:encode(TXID)}]),
						TXEndOffset
				end
		end,
		0,
		SizeTaggedTXs
	),
	ok.

add_block_data_roots([], _CurrentWeaveSize, _StoreID) ->
	{ok, sets:new()};
add_block_data_roots(SizeTaggedTXs, CurrentWeaveSize, StoreID) ->
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, DataRootIndexKeySet, Args} = lists:foldl(
		fun ({_, Offset}, {Offset, _, _} = Acc) ->
				Acc;
			({{padding, _}, Offset}, {_, Acc1, Acc2}) ->
				{Offset, Acc1, Acc2};
			({{_, DataRoot}, Offset}, {_, Acc1, Acc2}) when byte_size(DataRoot) < 32 ->
				{Offset, Acc1, Acc2};
			({{_, DataRoot}, TXEndOffset}, {PrevOffset, CurrentDataRootSet, CurrentArgs}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = CurrentWeaveSize + PrevOffset,
				TXSize = TXEndOffset - PrevOffset,
				DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
				{TXEndOffset, sets:add_element(DataRootKey, CurrentDataRootSet),
						[{DataRoot, TXSize, TXOffset, TXPath} | CurrentArgs]}
		end,
		{0, sets:new(), []},
		SizeTaggedTXs
	),
	case BlockSize > 0 of
		true ->
			ok = ar_kv:put({data_root_offset_index, StoreID},
					<< CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
					term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})),
			lists:foreach(
				fun({DataRoot, TXSize, TXOffset, TXPath}) ->
					ok = update_data_root_index(DataRoot, TXSize, TXOffset, TXPath, StoreID)
				end,
				Args
			);
		false ->
			do_not_update_data_root_offset_index
	end,
	{ok, DataRootIndexKeySet}.

update_data_root_index(DataRoot, TXSize, AbsoluteTXStartOffset, TXPath, StoreID) ->
	ar_kv:put({data_root_index, StoreID},
			data_root_key_v2(DataRoot, TXSize, AbsoluteTXStartOffset), TXPath).

add_block_data_roots_to_disk_pool(DataRootKeySet) ->
	sets:fold(
		fun(R, T) ->
			case ets:lookup(ar_disk_pool_data_roots, R) of
				[] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, T, not_set}});
				[{_, {Size, Timeout, _}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {Size, Timeout, not_set}})
			end,
			T + 1
		end,
		os:system_time(microsecond),
		DataRootKeySet
	).

reset_orphaned_data_roots_disk_pool_timestamps(DataRootKeySet) ->
	sets:fold(
		fun(R, T) ->
			case ets:lookup(ar_disk_pool_data_roots, R) of
				[] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, T, not_set}});
				[{_, {Size, _, TXIDSet}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {Size, T, TXIDSet}})
			end,
			T + 1
		end,
		os:system_time(microsecond),
		DataRootKeySet
	).

store_sync_state(#sync_data_state{ store_id = "default" } = State) ->
	#sync_data_state{ block_index = BI } = State,
	DiskPoolDataRoots = ets:foldl(
			fun({DataRootKey, V}, Acc) -> maps:put(DataRootKey, V, Acc) end, #{},
			ar_disk_pool_data_roots),
	StoredState = #{ block_index => BI, disk_pool_data_roots => DiskPoolDataRoots,
			%% Storing it for backwards-compatibility.
			strict_data_split_threshold => ?STRICT_DATA_SPLIT_THRESHOLD },
	case ar_storage:write_term(data_sync_state, StoredState) of
		{error, enospc} ->
			?LOG_WARNING([{event, failed_to_dump_state}, {reason, disk_full},
					{store_id, "default"}]),
			ok;
		ok ->
			ok
	end;
store_sync_state(_State) ->
	ok.

get_unsynced_intervals_from_other_storage_modules(TargetStoreID, StoreID, RangeStart,
		RangeEnd) ->
	get_unsynced_intervals_from_other_storage_modules(TargetStoreID, StoreID, RangeStart,
			RangeEnd, []).
get_unsynced_intervals_from_other_storage_modules(_TargetStoreID, _StoreID, RangeStart,
		RangeEnd, Intervals) when RangeStart >= RangeEnd ->
	Intervals;
get_unsynced_intervals_from_other_storage_modules(TargetStoreID, StoreID, RangeStart,
		RangeEnd, Intervals) ->
	FindNextMissing =
		case ar_sync_record:get_next_synced_interval(RangeStart, RangeEnd, ?MODULE,
				TargetStoreID) of
			not_found ->
				{request, {RangeStart, RangeEnd}};
			{End, Start} when Start =< RangeStart ->
				{skip, End};
			{_End, Start} ->
				{request, {RangeStart, Start}}
		end,
	case FindNextMissing of
		{skip, End2} ->
			get_unsynced_intervals_from_other_storage_modules(TargetStoreID, StoreID, End2,
					RangeEnd, Intervals);
		{request, {Cursor, RightBound}} ->
			case ar_sync_record:get_next_synced_interval(Cursor, RightBound, ?MODULE,
					StoreID) of
				not_found ->
					get_unsynced_intervals_from_other_storage_modules(TargetStoreID, StoreID,
							RightBound, RangeEnd, Intervals);
				{End2, Start2} ->
					Intervals2 = [{StoreID, {Start2, End2}} | Intervals],
					get_unsynced_intervals_from_other_storage_modules(TargetStoreID, StoreID,
							End2, RangeEnd, Intervals2)
			end
	end.

find_peer_intervals(Start, End, _StoreID, _Self, _AllPeersIntervals) when Start >= End ->
	ok;
find_peer_intervals(Start, End, StoreID, Self, AllPeersIntervals) ->
	Start2 = Start - Start rem ?NETWORK_DATA_BUCKET_SIZE,
	End2 = min(Start2 + ?NETWORK_DATA_BUCKET_SIZE, End),
	UnsyncedIntervals = get_unsynced_intervals(Start, End2, StoreID),
	AllPeersIntervals2 =
		case ar_intervals:is_empty(UnsyncedIntervals) of
			true ->
				AllPeersIntervals;
			false ->
				find_peer_intervals2(Start, UnsyncedIntervals, Self, AllPeersIntervals)
		end,
	find_peer_intervals(End2, End, StoreID, Self, AllPeersIntervals2).

%% @doc Collect the unsynced intervals between Start and End excluding the blocklisted
%% intervals.
get_unsynced_intervals(Start, End, StoreID) ->
	UnsyncedIntervals = get_unsynced_intervals(Start, End, ar_intervals:new(), StoreID),
	BlacklistedIntervals = ar_tx_blacklist:get_blacklisted_intervals(Start, End),
	ar_intervals:outerjoin(BlacklistedIntervals, UnsyncedIntervals).

get_unsynced_intervals(Start, End, Intervals, _StoreID) when Start >= End ->
	Intervals;
get_unsynced_intervals(Start, End, Intervals, StoreID) ->
	case ar_sync_record:get_next_synced_interval(Start, End, ?MODULE, StoreID) of
		not_found ->
			ar_intervals:add(Intervals, End, Start);
		{End2, Start2} ->
			case Start2 > Start of
				true ->
					End3 = min(Start2, End),
					get_unsynced_intervals(End2, End,
							ar_intervals:add(Intervals, End3, Start), StoreID);
				_ ->
					get_unsynced_intervals(End2, End, Intervals, StoreID)
			end
	end.

find_peer_intervals2(Start, UnsyncedIntervals, Self, AllPeersIntervals) ->
	Bucket = Start div ?NETWORK_DATA_BUCKET_SIZE,
	{ok, Config} = application:get_env(arweave, config),
	Peers =
		case Config#config.sync_from_local_peers_only of
			true ->
				Config#config.local_peers;
			false ->
				ar_data_discovery:get_bucket_peers(Bucket)
		end,
	case ar_intervals:is_empty(UnsyncedIntervals) of
		true ->
			AllPeersIntervals;
		false ->
			find_peer_intervals3(Start, UnsyncedIntervals, Self, AllPeersIntervals, Peers)
	end.

find_peer_intervals3(Start, UnsyncedIntervals, Self, AllPeersIntervals, Peers) ->
	Intervals =
		ar_util:pmap(
			fun(Peer) ->
				case get_peer_intervals(Peer, Start, UnsyncedIntervals, AllPeersIntervals) of
					{ok, SoughtIntervals, PeerIntervals, Left} ->
						{Peer, SoughtIntervals, PeerIntervals, Left};
					{error, Reason} ->
						?LOG_DEBUG([{event, failed_to_fetch_peer_intervals},
								{peer, ar_util:format_peer(Peer)},
								{reason, io_lib:format("~p", [Reason])}]),
						ok
				end
			end,
			Peers
		),
	{AllPeersSoughtIntervals, AllPeersIntervals2} =
		lists:foldl(
			fun	(
				{Peer, SoughtIntervals, PeerIntervals, Left},
				{AllPeersSoughtIntervals, AllPeersIntervals2}
			) ->
					case ar_intervals:is_empty(PeerIntervals) of
						true ->
							{AllPeersSoughtIntervals, AllPeersIntervals2};
						false ->
							Right = element(1, ar_intervals:largest(PeerIntervals)),
							{
								maps:put(Peer, SoughtIntervals, AllPeersSoughtIntervals),
								maps:put(Peer, {Right, Left, PeerIntervals}, AllPeersIntervals2)
							}
					end;
				(_, Acc) ->
					Acc
			end,
			{#{}, AllPeersIntervals},
			Intervals
		),
	EnqueueIntervals =
		lists:foldl(
			fun	({Peer, SoughtIntervals, _, _}, Acc) ->
					case ar_intervals:is_empty(SoughtIntervals) of
						true ->
							Acc;
						false ->
							[{Peer, SoughtIntervals} | Acc]
					end;
				(_, Acc) ->
					Acc
			end,
			[],
			Intervals
		),
	gen_server:cast(Self, {enqueue_intervals, EnqueueIntervals}),
	AllPeersIntervals2.

%% @doc
%% @return {ok, Intervals, PeerIntervals, Left} | Error
%% Intervals: the intersection of the intervals we are looking for and the intervals that
%%            the peer advertises
%% PeerIntervals: all of the intervals that the peer advertises between
%%                offset Left and Left+?MAX_SHARED_SYNCED_INTERVALS_COUNT
get_peer_intervals(Peer, Left, SoughtIntervals, CachedIntervals) ->
	Limit = ?MAX_SHARED_SYNCED_INTERVALS_COUNT,
	Right = element(1, ar_intervals:largest(SoughtIntervals)),
	case maps:get(Peer, CachedIntervals, not_found) of
		{Right2, Left2, PeerIntervals} when Right2 >= Right, Left2 =< Left ->
			{ok, ar_intervals:intersection(PeerIntervals, SoughtIntervals), PeerIntervals,
					Left2};
		_ ->
			case ar_http_iface_client:get_sync_record(Peer, Left + 1, Limit) of
				{ok, PeerIntervals2} ->
					{ok, ar_intervals:intersection(PeerIntervals2, SoughtIntervals),
							PeerIntervals2, Left};
				Error ->
					Error
			end
	end.

%% @doc Flatten the intervals into chunks and find the list of Peers advertising each chunk.
%% Note: There may be occasional overlaps/duplicates when peers advertise partial chunks.
%% Example without overlap/duplicate (assuming ?DATA_CHUNK_SIZE = 5):
%%   [{peer1, [{30, 20}, {10, 5}]}, {peer2, [{30, 20}]}]
%%   Resulting map:
%%   #{
%%     {5, 10} => [peer1],
%%     {20, 25} => [peer2, peer1],
%%     {25, 30} => [peer2, peer1]
%%   }
%%
%% Example with overlap/duplicate:
%%   [{peer1, [{30, 20}, {10, 5}]}, {peer2, [{30, 23}]}]
%%   Resulting map:
%%   #{
%%     {5, 10} => [peer1],
%%     {20, 25} => [peer1],
%%     {20, 23} => [peer2],
%%     {25, 30} => [peer2, peer1]
%%   }
%%
%% The {20,23} partial chunk exists in the map twice, which will cause that data to be
%% requested twice. I've deferred handling this case for now - I think the number of
%% redundant requests will be minimal and handling the case better introduces
%% complexity that I'm not sure is warranted yet.
collect_all_peers_per_chunk([], _QIntervals, PeersPerChunk) ->
	PeersPerChunk;
collect_all_peers_per_chunk([{Peer, Intervals} | Rest], QIntervals, PeersPerChunk) ->
	OuterJoin = ar_intervals:outerjoin(QIntervals, Intervals),
	PeersPerChunk2 = ar_intervals:fold(
		fun({End, Start}, Acc) ->
			lists:foldl(
				fun({AlignedStart, AlignedEnd}, InnerAcc) ->
					Peers = maps:get({AlignedStart, AlignedEnd}, InnerAcc, []),
					maps:put({AlignedStart, AlignedEnd}, [Peer | Peers], InnerAcc)
				end,
				Acc,
				aligned_intervals(Start, End, ?DATA_CHUNK_SIZE)
			)
		end,
		PeersPerChunk,
		OuterJoin
	),
	collect_all_peers_per_chunk(Rest, QIntervals, PeersPerChunk2).

aligned_intervals(Start, End, StepSize) ->
    AlignedStart = Start - (Start rem StepSize),
	aligned_intervals(AlignedStart, Start, End, StepSize, []).

aligned_intervals(AlignedStart, _Start, End, _StepSize, AlignedIntervals) when AlignedStart >= End ->
	AlignedIntervals;
aligned_intervals(AlignedStart, Start, End, StepSize, AlignedIntervals) ->
	AlignedEnd = min(End, AlignedStart + StepSize),
	Interval = {max(AlignedStart, Start), AlignedEnd},
	aligned_intervals(AlignedStart + StepSize, AlignedEnd, End, StepSize, AlignedIntervals ++ [Interval]).


enqueue_intervals(PeersPerChunk, {Q, QIntervals}) ->
	maps:fold(
		fun({Start, End}, Peers, {QAcc, QIntervalsAcc}) ->
			SelectedPeer = lists:nth(rand:uniform(length(Peers)), Peers),
			QUpdated = gb_sets:add_element({Start, End, SelectedPeer}, QAcc),
			QIntervalsUpdated = ar_intervals:add(QIntervalsAcc, End, Start),
			{QUpdated, QIntervalsUpdated}
		end,
		{Q, QIntervals},
		PeersPerChunk
	).

% enqueue_intervals({Peer, Intervals}, {Q, QIntervals}) ->
% 	%% The outerjoin keeps only unique intervals - only Intervals
% 	%% for this Peer that haven't already been added to the queue of
% 	%% intervals to sync (QInterval2). This means that the earlier
% 	%% a peer is processed, the more likely it is to have its intervals
% 	%% synced. This also means our syncing will be lumpy - we'll sync
% 	%% a bunch of intervals from Peer1 and then move onto Peer2. It would
% 	%% probably be better to sync from a bunch of peers simultaneously to
% 	%% limit the load on any 1 peer and also to mitigate the impact of
% 	%% a peer going offline or timing out.
% 	%%
% 	%% We may get some duplicates for two reasons:
% 	%% 1) find_peer_intervals might choose the same interval several
% 	%%    times in a row even when there are other unsynced intervals
% 	%%    to pick because it is probabilistic.
% 	%% 2) We ask many peers simultaneously about the same interval
% 	%%    to make finding of the relatively rare intervals quicker.
% 	OuterJoin = ar_intervals:outerjoin(QIntervals, Intervals),
% 	ar_intervals:fold(
% 		fun({End, Start}, {Acc, QIAcc}) ->
% 			?LOG_DEBUG([{event, add_interval_to_sync_queue}, {right, End}, {left, Start},
% 					{peer, ar_util:format_peer(Peer)}]),
% 			{lists:foldl(
% 				fun(Start2, Acc2) ->
% 					End2 = min(Start2 + ?DATA_CHUNK_SIZE, End),
% 					gb_sets:add_element({Start2, End2, Peer}, Acc2)
% 				end,
% 				Acc,
% 				lists:seq(Start, End - 1, ?DATA_CHUNK_SIZE)
% 			), ar_intervals:add(QIAcc, End, Start)}
% 		end,
% 		{Q, QIntervals},
% 		OuterJoin
% 	).


validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof, ValidateDataPathFun) ->
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			TXSize = TXEndOffset - TXStartOffset,
			ChunkOffset = Offset - TXStartOffset,
			case ValidateDataPathFun(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
					ChunkSize = ChunkEndOffset - ChunkStartOffset,
					case Packing of
						unpacked ->
							case ar_tx:generate_chunk_id(Chunk) == ChunkID of
								false ->
									false;
								true ->
									case ChunkSize == byte_size(Chunk) of
										true ->
											{true, DataRoot, TXStartOffset, ChunkEndOffset,
												TXSize, ChunkSize, ChunkID};
										false ->
											false
									end
							end;
						_ ->
							ChunkArgs = {Packing, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
							Args = {Packing, DataRoot, TXStartOffset, ChunkEndOffset, TXSize,
									ChunkID},
							{need_unpacking, AbsoluteEndOffset, ChunkArgs, Args}
					end
			end
	end.

validate_proof2(Args) ->
	{TXRoot, Offset, BlockSize, DataPath, TXPath, ChunkSize, ValidateDataPathFun} = Args,
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			TXSize = TXEndOffset - TXStartOffset,
			ChunkOffset = Offset - TXStartOffset,
			case ValidateDataPathFun(DataRoot, ChunkOffset, TXSize, DataPath) of
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					case ChunkEndOffset - ChunkStartOffset == ChunkSize of
						false ->
							false;
						true ->
							{true, ChunkID}
					end;
				_ ->
					false
			end
	end.

validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	ValidatePathResult =
		case ar_merkle:validate_path_strict_data_split(DataRoot, Offset, TXSize, DataPath) of
			false ->
				case ar_merkle:validate_path_strict_borders(DataRoot, Offset, TXSize,
						DataPath) of
					false ->
						false;
					R ->
						{false, R}
				end;
			R ->
				{true, R}
		end,
	case ValidatePathResult of
		false ->
			false;
		{Strict, {ChunkID, StartOffset, EndOffset}} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					case EndOffset - StartOffset == byte_size(Chunk) of
						true ->
							{true, Strict, EndOffset};
						false ->
							false
					end
			end
	end.

chunk_offsets_synced(_, _, _, _, N) when N == 0 ->
	true;
chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset, TXStartOffset, N) ->
	case ar_sync_record:is_recorded(TXStartOffset + ChunkOffset, ?MODULE) of
		{{true, _}, _StoreID} ->
			case TXStartOffset of
				0 ->
					true;
				_ ->
					<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
					Key = data_root_key_v2(DataRoot, TXSize, TXStartOffset - 1),
					case ar_kv:get_prev(DataRootIndex, Key) of
						none ->
							true;
						{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
								TXStartOffset2Size:8,
								TXStartOffset2:(TXStartOffset2Size * 8) >>, _} ->
							chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset,
									TXStartOffset2, N - 1);
						{ok, _, _} ->
							true;
						_ ->
							false
					end
			end;
		false ->
			false
	end.

%% @doc Return a storage reference to the chunk proof (and possibly the chunk itself).
get_chunk_data_key(DataPathHash) ->
	Timestamp = os:system_time(microsecond),
	<< Timestamp:256, DataPathHash/binary >>.

write_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, State) ->
	case ar_tx_blacklist:is_byte_blacklisted(Offset) of
		true ->
			ok;
		false ->
			write_not_blacklisted_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath,
					Packing, State)
	end.

write_not_blacklisted_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing,
		State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB, store_id = StoreID } = State,
	ShouldStoreInChunkStorage = should_store_in_chunk_storage(Offset, ChunkSize, Packing),
	Result =
		case ShouldStoreInChunkStorage of
			true ->
				PaddedOffset = get_chunk_padded_offset(Offset),
				ar_chunk_storage:put(PaddedOffset, Chunk, StoreID);
			false ->
				ok
		end,
	case Result of
		ok ->
			case ShouldStoreInChunkStorage of
				false ->
					ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath}));
				true ->
					ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary(DataPath))
			end;
		_ ->
			Result
	end.

%% @doc 256 KiB chunks are stored in the blob storage optimized for read speed.
%% Return true if we want to place the chunk there.
should_store_in_chunk_storage(Offset, ChunkSize, Packing) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			%% All chunks above ?STRICT_DATA_SPLIT_THRESHOLD are placed in 256 KiB buckets
			%% so technically can be stored in ar_chunk_storage. However, to avoid
			%% managing padding in ar_chunk_storage for unpacked chunks smaller than 256 KiB
			%% (we do not need fast random access to unpacked chunks after
			%% ?STRICT_DATA_SPLIT_THRESHOLD anyways), we put them to RocksDB.
			Packing /= unpacked orelse ChunkSize == (?DATA_CHUNK_SIZE);
		false ->
			ChunkSize == (?DATA_CHUNK_SIZE)
	end.

update_chunks_index(Args, State) ->
	AbsoluteChunkOffset = element(1, Args),
	case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
		true ->
			ok;
		false ->
			update_chunks_index2(Args, State)
	end.

update_chunks_index2(Args, State) ->
	{AbsoluteOffset, Offset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
			Packing} = Args,
	#sync_data_state{ chunks_index = ChunksIndex, store_id = StoreID } = State,
	Key = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
	Value = {ChunkDataKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
	case ar_kv:put(ChunksIndex, Key, term_to_binary(Value)) of
		ok ->
			StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
			PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
			case ar_sync_record:add(PaddedOffset, StartOffset, Packing, ?MODULE, StoreID) of
				ok ->
					ar_events:send(data_sync, {add_range, StartOffset, PaddedOffset, StoreID}),
					ok;
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_update_sync_record}, {reason, Reason},
							{chunk, ar_util:encode(ChunkDataKey)},
							{absolute_end_offset, AbsoluteOffset},
							{data_root, ar_util:encode(DataRoot)},
							{store_id, StoreID}]),
					{error, Reason}
			end;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_update_chunk_index}, {reason, Reason},
					{chunk_data_key, ar_util:encode(ChunkDataKey)},
					{data_root, ar_util:encode(DataRoot)},
					{absolute_end_offset, AbsoluteOffset}, {store_id, StoreID}]),
			{error, Reason}
	end.

pick_missing_blocks([{H, WeaveSize, _} | CurrentBI], BlockTXPairs) ->
	{After, Before} = lists:splitwith(fun({BH, _}) -> BH /= H end, BlockTXPairs),
	case Before of
		[] ->
			pick_missing_blocks(CurrentBI, BlockTXPairs);
		_ ->
			{WeaveSize, lists:reverse(After)}
	end.

process_invalid_fetched_chunk(Peer, Byte, State) ->
	#sync_data_state{ weave_size = WeaveSize } = State,
	?LOG_WARNING([{event, got_invalid_proof_from_peer}, {peer, ar_util:format_peer(Peer)},
			{byte, Byte}, {weave_size, WeaveSize}]),
	%% Not necessarily a malicious peer, it might happen
	%% if the chunk is recent and from a different fork.
	{noreply, State}.

process_valid_fetched_chunk(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{Packing, UnpackedChunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
	{AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot, Chunk, _ChunkID,
			ChunkEndOffset, _Strict, Peer, Byte} = Args,
	case is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) of
		false ->
			?LOG_WARNING([{event, got_too_big_proof_from_peer},
					{peer, ar_util:format_peer(Peer)}]),
			decrement_chunk_cache_size(),
			{noreply, State};
		true ->
			#sync_data_state{ store_id = StoreID } = State,
			case ar_sync_record:is_recorded(Byte + 1, ?MODULE, StoreID) of
				{true, _} ->
					%% The chunk has been synced by another job already.
					decrement_chunk_cache_size(),
					{noreply, State};
				false ->
					true = AbsoluteEndOffset == AbsoluteTXStartOffset + ChunkEndOffset,
					pack_and_store_chunk({DataRoot, AbsoluteEndOffset, TXPath, TXRoot,
							DataPath, Packing, ChunkEndOffset, ChunkSize, Chunk,
							UnpackedChunk, none, none}, State)
			end
	end.

pack_and_store_chunk({_, AbsoluteOffset, _, _, _, _, _, _, _, _, _, _},
		#sync_data_state{ disk_pool_threshold = DiskPoolThreshold } = State)
		when AbsoluteOffset > DiskPoolThreshold ->
	%% We do not put data into storage modules unless it is well confirmed.
	decrement_chunk_cache_size(),
	{noreply, State};
pack_and_store_chunk(Args, State) ->
	{DataRoot, AbsoluteOffset, TXPath, TXRoot, DataPath, Packing, Offset, ChunkSize, Chunk,
			UnpackedChunk, OriginStoreID, OriginChunkDataKey} = Args,
	#sync_data_state{ packing_map = PackingMap } = State,
	DataPathHash = crypto:hash(sha256, DataPath),
	RequiredPacking = get_required_chunk_packing(AbsoluteOffset, ChunkSize, State),
	PackingStatus =
		case {RequiredPacking, Packing} of
			{Packing, Packing} ->
				{ready, {Packing, Chunk}};
			{DifferentPacking, _} ->
				{need_packing, DifferentPacking}
		end,
	case PackingStatus of
		{ready, {StoredPacking, StoredChunk}} ->
			ChunkArgs = {StoredPacking, StoredChunk, AbsoluteOffset, TXRoot, ChunkSize},
			{noreply, store_chunk(ChunkArgs, {StoredPacking, DataPath, Offset, DataRoot,
					TXPath, OriginStoreID, OriginChunkDataKey}, State)};
		{need_packing, RequiredPacking} ->
			case maps:is_key({AbsoluteOffset, RequiredPacking}, PackingMap) of
				true ->
					decrement_chunk_cache_size(),
					{noreply, State};
				false ->
					case ar_packing_server:is_buffer_full() of
						true ->
							ar_util:cast_after(1000, self(), {pack_and_store_chunk, Args}),
							{noreply, State};
						false ->
							{Packing2, Chunk2} =
								case UnpackedChunk of
									none ->
										{Packing, Chunk};
									_ ->
										{unpacked, UnpackedChunk}
								end,
							ar_events:send(chunk, {repack_request, AbsoluteOffset,
									{RequiredPacking, Packing2, Chunk2, AbsoluteOffset,
									TXRoot, ChunkSize}}),
							?LOG_DEBUG([{event, requested_chunk_repacking},
									{data_path_hash, ar_util:encode(DataPathHash)},
									{data_root, ar_util:encode(DataRoot)},
									{absolute_end_offset, AbsoluteOffset},
									{relative_offset, Offset}]),
							ar_util:cast_after(600000, self(),
									{expire_repack_chunk_request,
											{AbsoluteOffset, RequiredPacking}}),
							PackingArgs = {pack_chunk, {RequiredPacking, DataPath,
									Offset, DataRoot, TXPath, OriginStoreID,
									OriginChunkDataKey}},
							{noreply, State#sync_data_state{
								packing_map = PackingMap#{
									{AbsoluteOffset, RequiredPacking} => PackingArgs }}}
					end
			end
	end.

process_store_chunk_queue(#sync_data_state{ store_chunk_queue_len = 0 } = State) ->
	State;
process_store_chunk_queue(State) ->
	#sync_data_state{ store_chunk_queue = Q, store_chunk_queue_len = Len,
			store_chunk_queue_threshold = Threshold } = State,
	Timestamp = element(2, gb_sets:smallest(Q)),
	Now = os:system_time(millisecond),
	Threshold2 =
		case Threshold < ?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD of
			true ->
				Threshold;
			false ->
				case Len > Threshold of
					true ->
						0;
					false ->
						Threshold
				end
		end,
	case Len > Threshold2
			orelse Now - Timestamp > ?STORE_CHUNK_QUEUE_FLUSH_TIME_THRESHOLD of
		true ->
			{{_Offset, _Timestamp, _Ref, ChunkArgs, Args}, Q2} = gb_sets:take_smallest(Q),
			store_chunk2(ChunkArgs, Args, State),
			decrement_chunk_cache_size(),
			State2 = State#sync_data_state{ store_chunk_queue = Q2,
					store_chunk_queue_len = Len - 1,
					store_chunk_queue_threshold = min(Threshold2 + 1,
							?STORE_CHUNK_QUEUE_FLUSH_SIZE_THRESHOLD) },
			process_store_chunk_queue(State2);
		false ->
			State
	end.

store_chunk(ChunkArgs, Args, State) ->
	%% Let at least N chunks stack up, then write them in the ascending order,
	%% to reduce out-of-order disk writes causing fragmentation.
	#sync_data_state{ store_chunk_queue = Q, store_chunk_queue_len = Len } = State,
	Now = os:system_time(millisecond),
	Offset = element(3, ChunkArgs),
	Q2 = gb_sets:add_element({Offset, Now, make_ref(), ChunkArgs, Args}, Q),
	State2 = State#sync_data_state{ store_chunk_queue = Q2, store_chunk_queue_len = Len + 1 },
	process_store_chunk_queue(State2).

store_chunk2(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = ChunkArgs,
	{_Packing, DataPath, Offset, DataRoot, TXPath, OriginStoreID, OriginChunkDataKey} = Args,
	PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
	StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
	DataPathHash = crypto:hash(sha256, DataPath),
	case ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE, StoreID) of
		{error, Reason} ->
			log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot, DataPathHash,
					StoreID),
			{error, Reason};
		ok ->
			DataPathHash = crypto:hash(sha256, DataPath),
			ChunkDataKey =
				case StoreID == OriginStoreID of
					true ->
						OriginChunkDataKey;
					_ ->
						get_chunk_data_key(DataPathHash)
				end,
			case write_chunk(AbsoluteOffset, ChunkDataKey, Chunk, ChunkSize, DataPath,
					Packing, State) of
				ok ->
					case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey, TXRoot,
							DataRoot, TXPath, ChunkSize, Packing}, State) of
						ok ->
							?LOG_DEBUG([{event, stored_chunk},
									{absolute_end_offset, AbsoluteOffset},
									{relative_offset, Offset},
									{data_path_hash, ar_util:encode(DataPathHash)},
									{data_root, ar_util:encode(DataRoot)},
									{store_id, StoreID}]),	
							ok;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot,
									DataPathHash, StoreID),
							{error, Reason}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot,
							DataPathHash, StoreID),
					{error, Reason}
			end
	end.

log_failed_to_store_chunk(Reason, AbsoluteOffset, Offset, DataRoot, DataPathHash, StoreID) ->
	?LOG_ERROR([{event, failed_to_store_chunk},
			{reason, io_lib:format("~p", [Reason])},
			{absolute_end_offset, AbsoluteOffset, Offset},
			{relative_offset, Offset},
			{data_path_hash, ar_util:encode(DataPathHash)},
			{data_root, ar_util:encode(DataRoot)},
			{store_id, StoreID}]).

get_required_chunk_packing(Offset, ChunkSize, #sync_data_state{ store_id = StoreID }) ->
	case Offset =< ?STRICT_DATA_SPLIT_THRESHOLD andalso ChunkSize < ?DATA_CHUNK_SIZE of
		true ->
			unpacked;
		false ->
			case StoreID of
				"default" ->
					{ok, Config} = application:get_env(arweave, config),
					{spora_2_6, Config#config.mining_addr};
				_ ->
					ar_storage_module:get_packing(StoreID)
			end
	end.

process_disk_pool_item(State, Key, Value) ->
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex,
			data_root_index = DataRootIndex, store_id = StoreID,
			chunk_data_db = ChunkDataDB } = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = parse_disk_pool_chunk(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey,
			PassedStrictValidation} = DiskPoolChunk,
	DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex = get_data_root_offset(DataRootKey, StoreID),
	InDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	case {InDataRootIndex, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			NextCursor = {seek, << (Timestamp + 1):256 >>},
			gen_server:cast(self(), process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = NextCursor }};
		{not_found, false} ->
			%% The chunk was either orphaned or never made it to the chain.
			case ets:member(ar_data_sync_state, move_data_root_index_migration_complete) of
				true ->
					ok = ar_kv:delete(DiskPoolChunksIndex, Key),
					?LOG_DEBUG([{event, removed_chunk_from_disk_pool},
							{reason, disk_pool_chunk_data_root_expired},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_doot, ar_util:encode(DataRoot)},
							{relative_offset, Offset}]),
					ok = ar_kv:delete(ChunkDataDB, ChunkDataKey),
					decrease_occupied_disk_pool_size(ChunkSize, DataRootKey);
				false ->
					%% Do not remove the chunk from the disk pool until the data root index
					%% migration is complete, because the data root might still exist in the
					%% old index.
					ok
			end,
			NextCursor = << Key/binary, <<"a">>/binary >>,
			gen_server:cast(self(), process_disk_pool_item),
			State2 = may_be_reset_disk_pool_full_scan_key(Key, State),
			{noreply, State2#sync_data_state{ disk_pool_cursor = NextCursor }};
		{{ok, {TXStartOffset, _TXPath}}, _} ->
			DataRootIndexIterator = data_root_index_iterator_v2(DataRootKey, TXStartOffset + 1,
					DataRootIndex),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			State2 = State#sync_data_state{ disk_pool_cursor = NextCursor },
			Args = {Offset, InDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key,
					PassedStrictValidation},
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, DataRootIndexIterator,
					true, Args}),
			{noreply, State2}
	end.

decrease_occupied_disk_pool_size(Size, DataRootKey) ->
	ets:update_counter(ar_data_sync_state, disk_pool_size, {2, -Size}),
	prometheus_gauge:dec(pending_chunks_size, Size),
	case ets:lookup(ar_disk_pool_data_roots, DataRootKey) of
		[] ->
			ok;
		[{_, {Size2, Timestamp, TXIDSet}}] ->
			ets:insert(ar_disk_pool_data_roots, {DataRootKey,
					{Size2 - Size, Timestamp, TXIDSet}}),
			ok
	end.

may_be_reset_disk_pool_full_scan_key(Key,
		#sync_data_state{ disk_pool_full_scan_start_key = Key } = State) ->
	State#sync_data_state{ disk_pool_full_scan_start_key = none };
may_be_reset_disk_pool_full_scan_key(_Key, State) ->
	State.

parse_disk_pool_chunk(Bin) ->
	case binary_to_term(Bin) of
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, false};
		R ->
			R
	end.

delete_disk_pool_chunk(Iterator, Args, State) ->
	#sync_data_state{ chunks_index = ChunksIndex, chunk_data_db = ChunkDataDB,
			disk_pool_chunks_index = DiskPoolChunksIndex, store_id = StoreID } = State,
	{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, DiskPoolKey, _} = Args,
	case data_root_index_next_v2(Iterator, 10) of
		none ->
			ok = ar_kv:delete(DiskPoolChunksIndex, DiskPoolKey),
			ok = ar_kv:delete(ChunkDataDB, ChunkDataKey),
			?LOG_DEBUG([{event, removed_chunk_from_disk_pool},
					{reason, rotation},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_doot, ar_util:encode(DataRoot)},
					{relative_offset, Offset}]),
			DataRootKey = data_root_index_get_key(Iterator),
			decrease_occupied_disk_pool_size(ChunkSize, DataRootKey);
		{TXArgs, Iterator2} ->
			{TXStartOffset, _TXRoot, _TXPath} = TXArgs,
			AbsoluteOffset = TXStartOffset + Offset,
			ChunksIndexKey = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
			case ar_kv:get(ChunksIndex, ChunksIndexKey) of
				not_found ->
					ok;
				{ok, V} ->
					ChunkArgs = binary_to_term(V),
					case element(1, ChunkArgs) of
						ChunkDataKey ->
							PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
							StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
							ok = ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE,
									StoreID),
							ok = ar_kv:delete(ChunksIndex, ChunksIndexKey);
						_ ->
							%% The entry has been written by the 2.5 version thus has
							%% a different key. We do not want to remove chunks from
							%% the existing 2.5 dataset.
							ok
					end
			end,
			delete_disk_pool_chunk(Iterator2, Args, State)
	end.

register_currently_processed_disk_pool_key(Key, State) ->
	#sync_data_state{ currently_processed_disk_pool_keys = Keys } = State,
	Keys2 = sets:add_element(Key, Keys),
	State#sync_data_state{ currently_processed_disk_pool_keys = Keys2 }.

deregister_currently_processed_disk_pool_key(Key, State) ->
	#sync_data_state{ currently_processed_disk_pool_keys = Keys } = State,
	Keys2 = sets:del_element(Key, Keys),
	State#sync_data_state{ currently_processed_disk_pool_keys = Keys2 }.

process_disk_pool_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude, Args,
		State) ->
	#sync_data_state{ disk_pool_threshold = DiskPoolThreshold } = State,
	{Offset, _, _, DataRoot, DataPathHash, _, _, PassedStrictValidation} = Args,
	case AbsoluteOffset =< ?STRICT_DATA_SPLIT_THRESHOLD orelse PassedStrictValidation of
		false ->
			%% When we accept chunks into the disk pool, we do not know where they will
			%% end up on the weave. Therefore, we cannot require all Merkle proofs pass
			%% the strict validation rules taking effect only after
			%% ?STRICT_DATA_SPLIT_THRESHOLD.
			%% Instead we note down whether the chunk passes the strict validation and take it
			%% into account here where the chunk is associated with a global weave offset.
			?LOG_INFO([{event, disk_pool_chunk_from_bad_split},
					{absolute_end_offset, AbsoluteOffset},
					{relative_offset, Offset},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_root, ar_util:encode(DataRoot)}]),
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
					MayConclude, Args}),
			{noreply, State};
		true ->
			case AbsoluteOffset > DiskPoolThreshold of
				true ->
					process_disk_pool_immature_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteOffset, Args, State);
				false ->
					process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteOffset, MayConclude, Args, State)
			end
	end.

process_disk_pool_immature_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, Args,
		State) ->
	#sync_data_state{ store_id = StoreID } = State,
	case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE, StoreID) of
		{true, unpacked} ->
			%% Pass MayConclude as false because we have encountered an offset
			%% above the disk pool threshold => we need to keep the chunk in the
			%% disk pool for now and not pack and move to the offset-based storage.
			%% The motivation is to keep chain reorganisations cheap.
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false, Args}),
			{noreply, State};
		false ->
			{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key, _} = Args,
			case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey,
					TXRoot, DataRoot, TXPath, ChunkSize, unpacked}, State) of
				ok ->
					?LOG_DEBUG([{event, indexed_disk_pool_chunk},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false,
							Args}),
					{noreply, State};
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_index_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason])},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)}
			end
	end.

process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude,
		Args, State) ->
	%% The chunk has received a decent number of confirmations so we put it in a storage
	%% module. If the have no storage modules configured covering this offset, proceed to
	%% the next offset. If there are several suitable storage modules, we choose one.
	%% The other modules will either sync the chunk themselves or copy it over from the
	%% other module the next time the node is restarted.
	#sync_data_state{ chunk_data_db = ChunkDataDB, store_id = DefaultStoreID } = State,
	{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key, _StrictSplit} = Args,
	FindStorageModule =
		case find_storage_module_for_disk_pool_chunk(AbsoluteOffset) of
			not_found ->
				gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
						MayConclude, Args}),
				{noreply, State};
			StoreID ->
				StoreID
		end,
	IsBlacklisted =
		case FindStorageModule of
			{noreply, State2} ->
				{noreply, State2};
			StoreID2 ->
				case ar_tx_blacklist:is_byte_blacklisted(AbsoluteOffset) of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, remove_recently_processed_disk_pool_offset(AbsoluteOffset,
								ChunkDataKey, State)};
					false ->
						StoreID2
				end
		end,
	IsSynced =
		case IsBlacklisted of
			{noreply, State3} ->
				{noreply, State3};
			StoreID3 ->
				case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE, StoreID3) of
					{true, _Packing} ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, remove_recently_processed_disk_pool_offset(AbsoluteOffset,
								ChunkDataKey, State)};
					false ->
						StoreID3
				end
		end,
	IsProcessed =
		case IsSynced of
			{noreply, State4} ->
				{noreply, State4};
			StoreID4 ->
				case is_recently_processed_offset(AbsoluteOffset, ChunkDataKey, State) of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, State};
					false ->
						StoreID4
				end
		end,
	IsChunkCacheFull =
		case IsProcessed of
			{noreply, State5} ->
				{noreply, State5};
			StoreID5 ->
				case is_chunk_cache_full() of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, State};
					false ->
						StoreID5
				end
		end,
	case IsChunkCacheFull of
		{noreply, State6} ->
			{noreply, State6};
		StoreID6 ->
			case read_chunk(AbsoluteOffset, ChunkDataDB, ChunkDataKey, DefaultStoreID) of
				not_found ->
					?LOG_ERROR([{event, disk_pool_chunk_not_found},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							MayConclude, Args}),
					{noreply, State};
				{error, Reason2} ->
					?LOG_ERROR([{event, failed_to_read_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason2])},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)};
				{ok, {Chunk, DataPath}} ->
					increment_chunk_cache_size(),
					Args2 = {DataRoot, AbsoluteOffset, TXPath, TXRoot, DataPath, unpacked,
							Offset, ChunkSize, Chunk, Chunk, none, none},
					gen_server:cast(list_to_atom("ar_data_sync_" ++ StoreID6),
							{pack_and_store_chunk, Args2}),
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							false, Args}),
					{noreply, cache_recently_processed_offset(AbsoluteOffset, ChunkDataKey,
							State)}
			end
	end.

find_storage_module_for_disk_pool_chunk(Offset) ->
	case ar_storage_module:get_all(Offset) of
		[] ->
			not_found;
		[Module | _] ->
			ar_storage_module:id(Module)
	end.

remove_recently_processed_disk_pool_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	case maps:get(Offset, Map, not_found) of
		not_found ->
			State;
		Set ->
			Set2 = sets:del_element(ChunkDataKey, Set),
			Map2 =
				case sets:is_empty(Set2) of
					true ->
						maps:remove(Offset, Map);
					false ->
						maps:put(Offset, Set2, Map)
				end,
			State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }
	end.

is_recently_processed_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	sets:is_element(ChunkDataKey, Set).

cache_recently_processed_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	Map2 =
		case sets:is_element(ChunkDataKey, Set) of
			false ->
				ar_util:cast_after(?CACHE_RECENTLY_PROCESSED_DISK_POOL_OFFSET_LIFETIME_MS,
						self(), {remove_recently_processed_disk_pool_offset, Offset,
						ChunkDataKey}),
				maps:put(Offset, sets:add_element(ChunkDataKey, Set), Map);
			true ->
				Map
		end,
	State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }.

process_unpacked_chunk(ChunkArgs, Args, State) ->
	{_AbsoluteTXStartOffset, _TXSize, _DataPath, _TXPath, _DataRoot, _Chunk, ChunkID,
			_ChunkEndOffset, _Strict, Peer, Byte} = Args,
	{_Packing, Chunk, AbsoluteEndOffset, _TXRoot, ChunkSize} = ChunkArgs,
	case validate_chunk_id_size(Chunk, ChunkID, ChunkSize) of
		false ->
			?LOG_DEBUG([{event, invalid_unpacked_fetched_chunk},
					{absolute_end_offset, AbsoluteEndOffset}]),
			decrement_chunk_cache_size(),
			process_invalid_fetched_chunk(Peer, Byte, State);
		true ->
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end.

validate_chunk_id_size(Chunk, ChunkID, ChunkSize) ->
	case ar_tx:generate_chunk_id(Chunk) == ChunkID of
		false ->
			false;
		true ->
			ChunkSize == byte_size(Chunk)
	end.

log_sufficient_disk_space(StoreID) ->
	ar:console("~nThe node has detected available disk space and resumed syncing data "
			"into the storage module ~s.~n", [StoreID]),
	?LOG_INFO([{event, storage_module_resumed_syncing}, {storage_module, StoreID}]).

log_insufficient_disk_space(StoreID) ->
	ar:console("~nThe node has stopped syncing data into the storage module ~s due to "
			"the insufficient disk space.~n", [StoreID]),
	?LOG_INFO([{event, storage_module_stopped_syncing},
			{reason, insufficient_disk_space}, {storage_module, StoreID}]),
	ar_util:print_stacktrace().

data_root_index_iterator_v2(DataRootKey, TXStartOffset, DataRootIndex) ->
	{DataRootKey, TXStartOffset, TXStartOffset, DataRootIndex, 1}.

data_root_index_next_v2({_, _, _, _, Count}, Limit) when Count > Limit ->
	none;
data_root_index_next_v2({_, 0, _, _, _}, _Limit) ->
	none;
data_root_index_next_v2(Args, _Limit) ->
	{DataRootKey, TXStartOffset, LatestTXStartOffset, DataRootIndex, Count} = Args,
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = data_root_key_v2(DataRoot, TXSize, TXStartOffset - 1),
	case ar_kv:get_prev(DataRootIndex, Key) of
		none ->
			none;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				TXStartOffset2Size:8, TXStartOffset2:(TXStartOffset2Size * 8) >>, TXPath} ->
			{ok, TXRoot} = ar_merkle:extract_root(TXPath),
			{{TXStartOffset2, TXRoot, TXPath},
					{DataRootKey, TXStartOffset2, LatestTXStartOffset, DataRootIndex,
							Count + 1}};
		{ok, _, _} ->
			none
	end.

data_root_index_reset({DataRootKey, _, TXStartOffset, DataRootIndex, _}) ->
	{DataRootKey, TXStartOffset, TXStartOffset, DataRootIndex, 1}.

data_root_index_get_key(Iterator) ->
	element(1, Iterator).

data_root_index_iterator(TXRootMap) ->
	{maps:fold(
		fun(TXRoot, Map, Acc) ->
			maps:fold(
				fun(Offset, TXPath, Acc2) ->
					gb_sets:insert({Offset, TXRoot, TXPath}, Acc2)
				end,
				Acc,
				Map
			)
		end,
		gb_sets:new(),
		TXRootMap
	), 0}.

data_root_index_next({_Index, Count}, Limit) when Count >= Limit ->
	none;
data_root_index_next({Index, Count}, _Limit) ->
	case gb_sets:is_empty(Index) of
		true ->
			none;
		false ->
			{Element, Index2} = gb_sets:take_largest(Index),
			{Element, {Index2, Count + 1}}
	end.

record_chunk_cache_size_metric() ->
	case ets:lookup(ar_data_sync_state, chunk_cache_size) of
		[{_, Size}] ->
			prometheus_gauge:set(chunk_cache_size, Size);
		_ ->
			ok
	end.
