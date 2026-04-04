-module(ar_disk_pool).

-export([add_chunk/5, add_data_root/3, maybe_drop_data_root/3,
		get_threshold/0, set_threshold/1, init_threshold/1,
		get_unconfirmed_chunk/2, has_data_root/2,
		record_chunks_count/0, remove_expired_data_roots/0,
		debug_get_chunks/0]).

-export([recalculate_size/2, add_block_data_roots/1,
		reset_orphaned_data_roots_timestamps/1,
		init_state/0, init_state/2, move_index/1]).

-export([process_next_item/2, process_chunk_offsets/5,
		remove_recently_processed_offset/3, resume_scan/1]).

-include("ar.hrl").
-include("ar_disk_pool.hrl").
-include("ar_data_sync.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-ifdef(AR_TEST).
-define(MIN_CHUNK_PERSISTENCE_ESTIMATION_VICINITY, (262144 div 2)).
-else.
-define(MIN_CHUNK_PERSISTENCE_ESTIMATION_VICINITY, (10 * ?GiB)).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Store the given chunk if the proof is valid.
%% Called when a chunk is pushed to the node via POST /chunk.
%% The chunk is placed in the disk pool. The periodic process
%% scanning the disk pool will later record it as synced.
%% The item is removed from the disk pool when the chunk's offset
%% drops below the disk pool threshold.
add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize) ->
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	DiskPoolChunksIndex = index_db(?DEFAULT_MODULE),
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootEntry = ar_data_roots:get_entry(DataRootKey, ?DEFAULT_MODULE),
	DataRootInDiskPool = ets:lookup(ar_disk_pool_data_roots, DataRootKey),
	ChunkSize = byte_size(Chunk),
	{ok, Config} = arweave_config:get_env(),
	DataRootLimit = Config#config.max_disk_pool_data_root_buffer_mb * ?MiB,
	DiskPoolLimit = Config#config.max_disk_pool_buffer_mb * ?MiB,
	CheckDiskPool =
		case {DataRootEntry, DataRootInDiskPool} of
			{not_found, []} ->
				?LOG_INFO([{event, failed_to_add_chunk_to_disk_pool},
					{reason, data_root_not_found}, {offset, Offset},
					{data_root, ar_util:encode(DataRoot)}]),
				{error, data_root_not_found};
			{not_found, [{_, {Size, Timestamp, TXIDSet}}]} ->
				case Size + ChunkSize > DataRootLimit
						orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						?LOG_INFO([{event, failed_to_add_chunk_to_disk_pool},
							{reason, exceeds_disk_pool_size_limit1}, {offset, Offset},
							{data_root_size, Size}, {chunk_size, ChunkSize},
							{data_root_limit, DataRootLimit}, {disk_pool_size, DiskPoolSize},
							{disk_pool_limit, DiskPoolLimit}]),
						{error, exceeds_disk_pool_size_limit};
					false ->
						{ok, {Size + ChunkSize, Timestamp, TXIDSet}}
				end;
			_ ->
				case DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						?LOG_INFO([{event, failed_to_add_chunk_to_disk_pool},
							{reason, exceeds_disk_pool_size_limit2}, {offset, Offset},
							{chunk_size, ChunkSize}, {disk_pool_size, DiskPoolSize},
							{disk_pool_limit, DiskPoolLimit}]),
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
						?LOG_INFO([{event, failed_to_add_chunk_to_disk_pool},
							{reason, invalid_proof}, {offset, Offset}]),
						{error, invalid_proof};
					{true, PassesBase, PassesStrict, PassesRebase, EndOffset} ->
						{ok, {EndOffset, PassesBase, PassesStrict, PassesRebase,
								DiskPoolDataRootValue}}
				end
		end,
	CheckSynced =
		case ValidateProof of
			{error, _} = Error2 ->
				Error2;
			{ok, {EndOffset2, _PassesBase2, _PassesStrict2, _PassesRebase2,
					{_, Timestamp3, _}} = PassedState2} ->
				DataPathHash = crypto:hash(sha256, DataPath),
				DiskPoolChunkKey = << Timestamp3:256, DataPathHash/binary >>,
				case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
					{ok, _DiskPoolChunk} ->
						{synced_disk_pool, EndOffset2};
					not_found ->
						case DataRootEntry of
							not_found ->
								{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}};
							{ok, {_DataRoot, _TXSize, TXStartOffset, _TXPath}} ->
								case chunk_offsets_synced(DataRootKey,
										EndOffset2, TXStartOffset) of
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
		{synced_disk_pool, EndOffset4} ->
			case is_estimated_long_term_chunk(DataRootEntry, EndOffset4) of
				false ->
					temporary;
				true ->
					ok
			end;
		{error, _} = Error4 ->
			Error4;
		{ok, {DataPathHash2, DiskPoolChunkKey2, {EndOffset3, PassesBase3, PassesStrict3,
				PassesRebase3, DiskPoolDataRootValue2}}} ->
			ChunkDataKey = get_chunk_data_key(DataPathHash2),
			case ar_data_sync:put_chunk_data(ChunkDataKey, ?DEFAULT_MODULE,
					{Chunk, DataPath}) of
				{error, Reason2} ->
					?LOG_WARNING([{event, failed_to_store_chunk_in_disk_pool},
						{reason, io_lib:format("~p", [Reason2])},
						{data_path_hash, ar_util:encode(DataPathHash2)},
						{data_root, ar_util:encode(DataRoot)},
						{relative_offset, EndOffset3}]),
					{error, failed_to_store_chunk};
				ok ->
					DiskPoolChunkValue = term_to_binary({EndOffset3, ChunkSize, DataRoot,
							TXSize, ChunkDataKey, PassesBase3, PassesStrict3, PassesRebase3}),
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
							ets:insert(ar_disk_pool_data_roots,
									{DataRootKey, DiskPoolDataRootValue2}),
							ets:update_counter(ar_data_sync_state, disk_pool_size,
									{2, ChunkSize}),
							prometheus_gauge:inc(pending_chunks_size, ChunkSize),
							case DiskPoolDataRootValue2 of
								{_, _, not_set} ->
									ok;
								{_, _, TXIDSet2} ->
									cache_chunk(TXIDSet2, EndOffset3,
											DiskPoolChunkKey2, DataPathHash2)
							end,
							case is_estimated_long_term_chunk(DataRootEntry, EndOffset3) of
								false ->
									temporary;
								true ->
									ok
							end
					end
			end
	end.

%% @doc Notify the server about the new pending data root (added to mempool).
%% The server may accept pending chunks and store them in the disk pool.
add_data_root(_, 0, _) ->
	ok;
add_data_root(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
add_data_root(DataRoot, TXSize, TXID) ->
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
maybe_drop_data_root(_, 0, _) ->
	ok;
maybe_drop_data_root(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
maybe_drop_data_root(DataRoot, TXSize, TXID) ->
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

%% @doc Return true if the given {DataRoot, DataSize} is in the mempool
%% or in the index.
has_data_root(DataRoot, DataSize) ->
	DataRootKey = << DataRoot:32/binary, DataSize:256 >>,
	case ets:member(ar_disk_pool_data_roots, DataRootKey) of
		true ->
			true;
		false ->
			ar_data_roots:is_synced(DataRootKey, ?DEFAULT_MODULE)
	end.

get_unconfirmed_chunk(TXID, RelativeEndOffset) ->
	case ets:lookup(ar_disk_pool_chunks_cache, {TXID, RelativeEndOffset}) of
		[{_, DiskPoolChunkKey}] ->
			get_unconfirmed_chunk_from_disk_pool(TXID, RelativeEndOffset,
					DiskPoolChunkKey);
		[] ->
			get_unconfirmed_chunk_from_tx_index(TXID, RelativeEndOffset)
	end.

%% @doc Return the disk pool threshold, a byte offset where
%% the disk pool begins - the data above this offset is considered
%% to belong to the disk pool. For example, we do not store the
%% disk pool data in the storage modules due to the risk of orphans.
get_threshold() ->
	case ets:lookup(ar_data_sync_state, disk_pool_threshold) of
		[] ->
			0;
		[{_, DiskPoolThreshold}] ->
			DiskPoolThreshold
	end.

set_threshold(DiskPoolThreshold) ->
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	DiskPoolThreshold.

%% @doc Compute the current disk pool threshold from the block index,
%% cache it in ETS, and return it.
init_threshold(BI) ->
	DiskPoolThreshold = ar_node:get_partition_upper_bound(BI),
	set_threshold(DiskPoolThreshold).

record_chunks_count() ->
	DB = index_db(?DEFAULT_MODULE),
	case ar_kv:count(DB) of
		Count when is_integer(Count) ->
			prometheus_gauge:set(disk_pool_chunks_count, Count);
		Error ->
			?LOG_WARNING([{event, failed_to_read_disk_pool_chunks_count},
					{error, io_lib:format("~p", [Error])}])
	end.

remove_expired_data_roots() ->
	Now = os:system_time(microsecond),
	{ok, Config} = arweave_config:get_env(),
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

debug_get_chunks() ->
	debug_get_chunks(first).

%%%===================================================================
%%% Functions called by ar_data_sync during init/join/add_tip_block.
%%%===================================================================

recalculate_size(DataRootMap, StoreID) ->
	Index = index_db(StoreID),
	DataRootMap2 = maps:map(fun(_DataRootKey, {_Size, Timestamp, TXIDSet}) ->
			{0, Timestamp, TXIDSet} end, DataRootMap),
	recalculate_size2(Index, DataRootMap2, first, 0).

add_block_data_roots(DataRootKeySet) ->
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

reset_orphaned_data_roots_timestamps(DataRootKeySet) ->
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

init_state() ->
	#disk_pool_state{}.

init_state(StateMap, StoreID) ->
	DiskPoolDataRoots = maps:get(disk_pool_data_roots, StateMap),
	recalculate_size(DiskPoolDataRoots, StoreID),
	case StateMap of
		#{ disk_pool_threshold := T } ->
			set_threshold(T);
		_ ->
			init_threshold(maps:get(block_index, StateMap))
	end,
	init_state().

move_index(StoreID) ->
	move_index2(first, StoreID).

%%%===================================================================
%%% Disk pool scan functions (called from ar_data_sync handle_cast).
%%%===================================================================

process_next_item(#disk_pool_state{ scan_pause = true } = DiskPool, _StoreID) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
	DiskPool;
process_next_item(
		#disk_pool_state{
			cursor = Cursor,
			full_scan_start_key = FullScanStartKey,
			full_scan_start_timestamp = Timestamp,
			currently_processed_keys = CurrentlyProcessedDiskPoolKeys
		} = DiskPool,
		StoreID) ->
	NextKey = get_next_item(StoreID, Cursor, CurrentlyProcessedDiskPoolKeys),
	case NextKey of
		none ->
			pause_scan(DiskPool);
		{ok, Key, Value} ->
			case FullScanStartKey of
				none ->
					process_item(
						DiskPool#disk_pool_state{
							full_scan_start_key = Key,
							full_scan_start_timestamp = erlang:timestamp()
						},
						StoreID,
						Key,
						Value
					);
				Key ->
					TimePassed = timer:now_diff(erlang:timestamp(), Timestamp),
					case TimePassed < (?DISK_POOL_SCAN_DELAY_MS) * 1000 of
						true ->
							pause_scan(DiskPool);
						false ->
							process_item(DiskPool, StoreID, Key, Value)
					end;
				_ ->
					process_item(DiskPool, StoreID, Key, Value)
			end
	end.

resume_scan(DiskPool) ->
	DiskPool#disk_pool_state{ scan_pause = false }.

process_item(DiskPool, StoreID, Key, Value) ->
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = parse_chunk(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey,
			PassedBaseValidation, PassedStrictValidation,
			PassedRebaseValidation} = DiskPoolChunk,
	DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootEntry = ar_data_roots:get_entry(DataRootKey, StoreID),
	InDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	case {DataRootEntry, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			NextCursor = {seek, << (Timestamp + 1):256 >>},
			gen_server:cast(self(), process_disk_pool_item),
			DiskPool#disk_pool_state{ cursor = NextCursor };
		{not_found, false} ->
			%% The chunk was either orphaned or never made it to the chain.
			DiskPoolChunksIndex = index_db(StoreID),
			ok = ar_kv:delete(DiskPoolChunksIndex, Key),
			ok = ar_data_sync:delete_chunk_data(ChunkDataKey, StoreID),
			remove_chunk_from_cache(DataPathHash),
			decrease_occupied_size(ChunkSize, DataRootKey),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			gen_server:cast(self(), process_disk_pool_item),
			DiskPool2 = maybe_reset_full_scan_key(Key, DiskPool),
			DiskPool2#disk_pool_state{ cursor = NextCursor };
		{{ok, {_DataRoot, _TXSize, TXStartOffset, _TXPath}}, _} ->
			DataRootIndexIterator = ar_data_roots:iterator(DataRootKey, TXStartOffset + 1,
					StoreID),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			DiskPool2 = DiskPool#disk_pool_state{ cursor = NextCursor },
			Args = {Offset, InDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key,
					PassedBaseValidation, PassedStrictValidation, PassedRebaseValidation},
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, DataRootIndexIterator,
					true, Args}),
			DiskPool2
	end.

process_chunk_offsets(Iterator, MayConclude, Args, StoreID, DiskPool) ->
	{Offset, _, _, _, _, _, Key, _, _, _} = Args,
	%% Place the chunk under its last configured offsets in the weave (the same data
	%% may be uploaded several times).
	case ar_data_roots:next(Iterator) of
		{ok, DataRootEntry, Iterator2} ->
			DiskPool2 = register_currently_processed_key(Key, DiskPool),
			{_DataRoot, _TXSize, TXStartOffset, TXPath} = DataRootEntry,
			{ok, TXRoot} = ar_merkle:extract_root(TXPath),
			AbsoluteEndOffset = TXStartOffset + Offset,
			process_chunk_offset(Iterator2, TXRoot, TXPath, AbsoluteEndOffset,
					MayConclude, Args, StoreID, DiskPool2);
		_ ->
			DiskPool2 =
				case MayConclude of
					true ->
						Iterator2 = ar_data_roots:reset(Iterator),
						delete_chunk(Iterator2, Args, StoreID, DiskPool),
						maybe_reset_full_scan_key(Key, DiskPool);
					false ->
						DiskPool
				end,
			gen_server:cast(self(), process_disk_pool_item),
			deregister_currently_processed_key(Key, DiskPool2)
	end.

remove_recently_processed_offset(Offset, ChunkDataKey, DiskPool) ->
	#disk_pool_state{ recently_processed_offsets = Map } = DiskPool,
	case maps:get(Offset, Map, not_found) of
		not_found ->
			DiskPool;
		Set ->
			Set2 = sets:del_element(ChunkDataKey, Set),
			Map2 =
				case sets:is_empty(Set2) of
					true ->
						maps:remove(Offset, Map);
					false ->
						maps:put(Offset, Set2, Map)
				end,
			DiskPool#disk_pool_state{ recently_processed_offsets = Map2 }
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

cache_chunk(TXIDSet, RelativeEndOffset, DiskPoolChunkKey, DataPathHash) ->
	sets:fold(
		fun(TXID, ok) ->
			ets:insert(ar_disk_pool_chunks_cache,
					{{TXID, RelativeEndOffset}, DiskPoolChunkKey}),
			ets:insert(ar_disk_pool_chunks_cache_reverse,
					{DataPathHash, {TXID, RelativeEndOffset}}),
			ok
		end,
		ok,
		TXIDSet
	).

remove_chunk_from_cache(DataPathHash) ->
	Entries = ets:lookup(ar_disk_pool_chunks_cache_reverse, DataPathHash),
	lists:foreach(
		fun({_, {TXID, RelativeEndOffset}}) ->
			ets:delete(ar_disk_pool_chunks_cache, {TXID, RelativeEndOffset})
		end,
		Entries
	),
	ets:delete(ar_disk_pool_chunks_cache_reverse, DataPathHash).

get_unconfirmed_chunk_from_disk_pool(TXID, RelativeEndOffset, DiskPoolChunkKey) ->
	DiskPoolChunksIndex = index_db(?DEFAULT_MODULE),
	case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
		not_found ->
			get_unconfirmed_chunk_from_tx_index(TXID, RelativeEndOffset);
		{error, _} = Error ->
			Error;
		{ok, Value} ->
			DiskPoolChunk = parse_chunk(Value),
			{RelEndOffset, _ChunkSize, DataRoot, TXSize, ChunkDataKey,
					_PassesBase, _PassesStrict, _PassesRebase} = DiskPoolChunk,
			case ar_data_sync:get_chunk_data(ChunkDataKey, ?DEFAULT_MODULE) of
				not_found ->
					get_unconfirmed_chunk_from_tx_index(TXID, RelativeEndOffset);
				{error, _} = Error ->
					Error;
				{ok, Bin} ->
					{Chunk, DataPath} = binary_to_term(Bin),
					DataRootKey = << DataRoot/binary,
							TXSize:?OFFSET_KEY_BITSIZE >>,
					DataRootOffsetReply = ar_data_roots:get_entry(DataRootKey,
							?DEFAULT_MODULE),
					IsStoredLongTerm = is_estimated_long_term_chunk(
							DataRootOffsetReply, RelEndOffset),
					{ok, {Chunk, DataPath, IsStoredLongTerm}}
			end
	end.

get_unconfirmed_chunk_from_tx_index(TXID, RelativeEndOffset) ->
	TXIndex = {tx_index, ?DEFAULT_MODULE},
	case ar_data_sync:get_tx_offset(TXIndex, TXID) of
		{error, _} ->
			{error, not_found};
		{ok, {AbsTXEndOffset, TXSize}} ->
			TXStartOffset = AbsTXEndOffset - TXSize,
			AbsoluteChunkEndOffset = TXStartOffset + RelativeEndOffset,
			case ar_data_sync:get_chunk_by_byte(AbsoluteChunkEndOffset, ?DEFAULT_MODULE) of
				{error, _} ->
					{error, not_found};
				{ok, _Key, {_AbsEndOffset, ChunkDataKey, _TXRoot, _DataRoot, TXPath,
						_RelativeOffset, _ChunkSize}} ->
					case ar_data_sync:get_chunk_data(ChunkDataKey, ?DEFAULT_MODULE) of
						not_found ->
							{error, not_found};
						{error, _} = Error ->
							Error;
						{ok, Bin} ->
							{Chunk, DataPath} = binary_to_term(Bin),
							IsStoredLongTerm = is_estimated_long_term_chunk(
									{ok, {TXStartOffset, TXPath}},
									RelativeEndOffset),
							{ok, {Chunk, DataPath, IsStoredLongTerm}}
					end
			end
	end.

is_estimated_long_term_chunk(DataRootEntry, EndOffset) ->
	WeaveSize = ar_node:get_current_weave_size(),
	case DataRootEntry of
		not_found ->
			%% A chunk from a pending transaction.
			is_offset_vicinity_covered(WeaveSize);
		{ok, {_DataRoot, _TXSize, TXStartOffset, _TXPath}} ->
			Size = ar_node:get_recent_max_block_size(),
			AbsoluteEndOffset = TXStartOffset + EndOffset,
			case AbsoluteEndOffset > WeaveSize - Size * 4 of
				true ->
					%% A relatively recent offset - do not expect this chunk to be
					%% persisted unless we have some storage modules configured for
					%% the space ahead (the data may be rearranged during after a reorg).
					is_offset_vicinity_covered(AbsoluteEndOffset);
				false ->
					ar_storage_module:has_any(AbsoluteEndOffset)
			end
	end.

is_offset_vicinity_covered(Offset) ->
	Size = max(?MIN_CHUNK_PERSISTENCE_ESTIMATION_VICINITY,
			ar_node:get_recent_max_block_size()),
	ar_storage_module:has_range(max(0, Offset - Size * 2), Offset + Size * 2).

validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	Base = ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath, strict_borders_ruleset),
	Strict = ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath,
			strict_data_split_ruleset),
	Rebase = ar_merkle:validate_path(DataRoot, Offset, TXSize, DataPath,
			offset_rebase_support_ruleset),
	Result =
		case {Base, Strict, Rebase} of
			{false, false, false} ->
				false;
			{_, {_, _, _} = StrictResult, _} ->
				StrictResult;
			{_, _, {_, _, _} = RebaseResult} ->
				RebaseResult;
			{{_, _, _} = BaseResult, _, _} ->
				BaseResult
		end,
	case Result of
		false ->
			false;
		{ChunkID, StartOffset, EndOffset} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					case EndOffset - StartOffset == byte_size(Chunk) of
						true ->
							PassesBase = not (Base == false),
							PassesStrict = not (Strict == false),
							PassesRebase = not (Rebase == false),
							{true, PassesBase, PassesStrict, PassesRebase, EndOffset};
						false ->
							false
					end
			end
	end.

chunk_offsets_synced(DataRootKey, ChunkOffset, TXStartOffset) ->
	case ar_sync_record:is_recorded(TXStartOffset + ChunkOffset, ar_data_sync) of
		{{true, _}, _StoreID} ->
			Iterator = ar_data_roots:iterator(DataRootKey, TXStartOffset, ?DEFAULT_MODULE),
			chunk_offsets_synced2(ChunkOffset, Iterator);
		false ->
			false
	end.

chunk_offsets_synced2(ChunkOffset, Iterator) ->
	case ar_data_roots:next(Iterator) of
		{ok, {_, _, TXStartOffset, _}, Iterator2} ->
			case ar_sync_record:is_recorded(TXStartOffset + ChunkOffset, ar_data_sync) of
				{{true, _}, _StoreID} ->
					chunk_offsets_synced2(ChunkOffset, Iterator2);
				false ->
					false
			end;
		{error, _} ->
			false;
		none ->
			true
	end.

get_chunk_data_key(DataPathHash) ->
	Timestamp = os:system_time(microsecond),
	<< Timestamp:256, DataPathHash/binary >>.

index_db(StoreID) ->
	{disk_pool_chunks_index, StoreID}.

old_index_db(StoreID) ->
	{disk_pool_chunks_index_old, StoreID}.

debug_get_chunks(Cursor) ->
	case ar_kv:get_next(index_db(?DEFAULT_MODULE), Cursor) of
		none ->
			[];
		{ok, K, V} ->
			K2 = << K/binary, <<"a">>/binary >>,
			[{K, V} | debug_get_chunks(K2)]
	end.

recalculate_size2(Index, DataRootMap, Cursor, Sum) ->
	case ar_kv:get_next(Index, Cursor) of
		none ->
			prometheus_gauge:set(pending_chunks_size, Sum),
			maps:map(fun(DataRootKey, V) -> ets:insert(ar_disk_pool_data_roots,
					{DataRootKey, V}) end, DataRootMap),
			ets:insert(ar_data_sync_state, {disk_pool_size, Sum});
		{ok, Key, Value} ->
			DecodedValue = binary_to_term(Value, [safe]),
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
			recalculate_size2(Index, DataRootMap2, Cursor2, Sum + ChunkSize)
	end.

move_index2(Cursor, StoreID) ->
	Old = old_index_db(StoreID),
	New = index_db(StoreID),
	case ar_kv:get_next(Old, Cursor) of
		none ->
			ok;
		{ok, Key, Value} ->
			ok = ar_kv:put(New, Key, Value),
			ok = ar_kv:delete(Old, Key),
			move_index2(Key, StoreID)
	end.

get_next_item(StoreID, Cursor, CurrentlyProcessedDiskPoolKeys) ->
	DiskPoolChunksIndex = index_db(StoreID),
	case ar_kv:get_next(DiskPoolChunksIndex, Cursor) of
		{ok, Key, Value} ->
			case sets:is_element(Key, CurrentlyProcessedDiskPoolKeys) of
				true ->
					none;
				false ->
					{ok, Key, Value}
			end;
		none ->
			case ar_kv:get_next(DiskPoolChunksIndex, first) of
				{ok, Key, Value} ->
					case sets:is_element(Key, CurrentlyProcessedDiskPoolKeys) of
						true ->
							none;
						false ->
							{ok, Key, Value}
					end;
				none ->
					none
			end
	end.

parse_chunk(Bin) ->
	case binary_to_term(Bin, [safe]) of
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, true, false, false};
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, PassesStrict} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, true, PassesStrict, false};
		R ->
			R
	end.

decrease_occupied_size(Size, DataRootKey) ->
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

maybe_reset_full_scan_key(Key,
		#disk_pool_state{ full_scan_start_key = Key } = DiskPool) ->
	DiskPool#disk_pool_state{ full_scan_start_key = none };
maybe_reset_full_scan_key(_Key, DiskPool) ->
	DiskPool.

pause_scan(DiskPool) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), resume_disk_pool_scan),
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
	DiskPool#disk_pool_state{
		cursor = first,
		full_scan_start_key = none,
		scan_pause = true
	}.

register_currently_processed_key(Key, DiskPool) ->
	#disk_pool_state{ currently_processed_keys = Keys } = DiskPool,
	Keys2 = sets:add_element(Key, Keys),
	DiskPool#disk_pool_state{ currently_processed_keys = Keys2 }.

deregister_currently_processed_key(Key, DiskPool) ->
	#disk_pool_state{ currently_processed_keys = Keys } = DiskPool,
	Keys2 = sets:del_element(Key, Keys),
	DiskPool#disk_pool_state{ currently_processed_keys = Keys2 }.

process_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteEndOffset, MayConclude, Args,
		StoreID, DiskPool) ->
	DiskPoolThreshold = get_threshold(),
	{Offset, _, _, DataRoot, DataPathHash, _, _,
			PassedBase, PassedStrictValidation, PassedRebaseValidation} = Args,
	PassedValidation =
		case {AbsoluteEndOffset >= ar_data_sync:get_merkle_rebase_threshold(),
				AbsoluteEndOffset >= ar_block:strict_data_split_threshold(),
				PassedBase, PassedStrictValidation, PassedRebaseValidation} of
			%% At the rebase threshold we relax some of the validation rules so the strict
			%% validation may fail.
			{true, true, _, _, true} ->
				true;
			%% Between the "strict" and "rebase" thresholds the "base" and "strict split"
			%% rules must be followed.
			{false, true, true, true, _} ->
				true;
			%% Before the strict threshold only the base (most relaxed) validation must
			%% pass.
			{false, false, true, _, _} ->
				true;
			_ ->
				false
		end,
	case PassedValidation of
		false ->
			%% When we accept chunks into the disk pool, we do not know where they will
			%% end up on the weave. Therefore, we cannot require all Merkle proofs pass
			%% the strict validation rules taking effect only after
			%% ar_block:strict_data_split_threshold() or allow the merkle tree offset rebases
			%% supported after the yet another special weave threshold.
			%% Instead we note down whether the chunk passes the strict and rebase validations
			%% and take it into account here where the chunk is associated with a global weave
			%% offset.
			?LOG_INFO([{event, disk_pool_chunk_from_bad_split},
					{absolute_end_offset, AbsoluteEndOffset},
					{merkle_rebase_threshold, ar_data_sync:get_merkle_rebase_threshold()},
					{strict_data_split_threshold, ar_block:strict_data_split_threshold()},
					{passed_base, PassedBase}, {passed_strict, PassedStrictValidation},
					{passed_rebase, PassedRebaseValidation},
					{relative_offset, Offset},
					{data_path_hash, ar_util:encode(DataPathHash)},
					{data_root, ar_util:encode(DataRoot)}]),
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
					MayConclude, Args}),
			DiskPool;
		true ->
			case AbsoluteEndOffset > DiskPoolThreshold of
				true ->
					process_immature_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteEndOffset, Args, StoreID, DiskPool);
				false ->
					process_matured_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteEndOffset, MayConclude, Args, StoreID, DiskPool)
			end
	end.

process_immature_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteEndOffset, Args,
		StoreID, DiskPool) ->
	case ar_sync_record:is_recorded(AbsoluteEndOffset, ar_data_sync, StoreID) of
		{true, unpacked} ->
			%% Pass MayConclude as false because we have encountered an offset
			%% above the disk pool threshold => we need to keep the chunk in the
			%% disk pool for now and not pack and move to the offset-based storage.
			%% The motivation is to keep chain reorganisations cheap.
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false, Args}),
			DiskPool;
		false ->
			{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key, _, _, _} = Args,
			case ar_data_sync:update_chunks_index({AbsoluteEndOffset, Offset, ChunkDataKey,
					TXRoot, DataRoot, TXPath, ChunkSize, unpacked}, false, StoreID) of
				ok ->
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false,
							Args}),
					DiskPool;
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_index_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason])},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteEndOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), process_disk_pool_item),
					deregister_currently_processed_key(Key, DiskPool)
			end
	end.

process_matured_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteEndOffset, MayConclude,
		Args, DefaultStoreID, DiskPool) ->
	%% The chunk has received a decent number of confirmations so we put it in storage
	%% module(s). If we have no storage modules configured covering this offset, proceed to
	%% the next offset. If there are several suitable storage modules, send the chunk
	%% to those modules who have not have it synced yet.
	{Offset, _, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key, _PassedBaseValidation,
			_PassedStrictValidation, _PassedRebaseValidation} = Args,
	FindStorageModules =
		case ar_storage_module:get_all(AbsoluteEndOffset - ChunkSize, AbsoluteEndOffset) of
			[] ->
				gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
						MayConclude, Args}),
				{noreply, DiskPool};
			Modules ->
				[ar_storage_module:id(Module) || Module <- Modules]
		end,
	IsBlacklisted =
		case FindStorageModules of
			{noreply, State2} ->
				{noreply, State2};
			StoreIDs ->
				case ar_tx_blacklist:is_byte_blacklisted(AbsoluteEndOffset) of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, remove_recently_processed_offset(AbsoluteEndOffset,
								ChunkDataKey, DiskPool)};
					false ->
						StoreIDs
				end
		end,
	IsSynced =
		case IsBlacklisted of
			{noreply, State3} ->
				{noreply, State3};
			StoreIDs2 ->
				case filter_storage_modules_by_synced_offset(AbsoluteEndOffset, StoreIDs2) of
					[] ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, remove_recently_processed_offset(AbsoluteEndOffset,
								ChunkDataKey, DiskPool)};
					StoreIDs3 ->
						StoreIDs3
				end
		end,
	IsProcessed =
		case IsSynced of
			{noreply, State4} ->
				{noreply, State4};
			StoreIDs4 ->
				case is_recently_processed_offset(AbsoluteEndOffset, ChunkDataKey, DiskPool) of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, DiskPool};
					false ->
						StoreIDs4
				end
		end,
	IsChunkCacheFull =
		case IsProcessed of
			{noreply, State5} ->
				{noreply, State5};
			StoreIDs5 ->
				case ar_data_sync:is_chunk_cache_full() of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, DiskPool};
					false ->
						StoreIDs5
				end
		end,
	case IsChunkCacheFull of
		{noreply, DiskPool6} ->
			DiskPool6;
		StoreIDs6 ->
			case ar_data_sync:read_chunk(AbsoluteEndOffset, ChunkDataKey, DefaultStoreID) of
				not_found ->
					?LOG_ERROR([{event, disk_pool_chunk_not_found},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteEndOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							MayConclude, Args}),
					DiskPool;
				{error, Reason2} ->
					?LOG_ERROR([{event, failed_to_read_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason2])},
							{data_path_hash, ar_util:encode(DataPathHash)},
							{data_root, ar_util:encode(DataRoot)},
							{absolute_end_offset, AbsoluteEndOffset},
							{relative_offset, Offset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), process_disk_pool_item),
					deregister_currently_processed_key(Key, DiskPool);
				{ok, {Chunk, DataPath}} ->
					ar_data_sync:increment_chunk_cache_size(),
					Args2 = {DataRoot, AbsoluteEndOffset, TXPath, TXRoot, DataPath, unpacked,
							Offset, ChunkSize, Chunk, Chunk, none, none},
					[gen_server:cast(ar_data_sync:name(StoreID6),
						{pack_and_store_chunk, Args2})
						|| StoreID6 <- StoreIDs6],
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							false, Args}),
					cache_recently_processed_offset(AbsoluteEndOffset, ChunkDataKey, DiskPool)
			end
	end.

is_recently_processed_offset(Offset, ChunkDataKey, DiskPool) ->
	#disk_pool_state{ recently_processed_offsets = Map } = DiskPool,
	Set = maps:get(Offset, Map, sets:new()),
	sets:is_element(ChunkDataKey, Set).

cache_recently_processed_offset(Offset, ChunkDataKey, DiskPool) ->
	#disk_pool_state{ recently_processed_offsets = Map } = DiskPool,
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
	DiskPool#disk_pool_state{ recently_processed_offsets = Map2 }.

filter_storage_modules_by_synced_offset(AbsoluteEndOffset, [StoreID | StoreIDs]) ->
	case ar_sync_record:is_recorded(AbsoluteEndOffset, ar_data_sync, StoreID) of
		{true, _Packing} ->
			filter_storage_modules_by_synced_offset(AbsoluteEndOffset, StoreIDs);
		false ->
			[StoreID | filter_storage_modules_by_synced_offset(AbsoluteEndOffset, StoreIDs)]
	end;
filter_storage_modules_by_synced_offset(_, []) ->
	[].

delete_chunk(Iterator, Args, StoreID, DiskPool) ->
	DiskPoolChunksIndex = index_db(StoreID),
	{Offset, _, ChunkSize, _, _, ChunkDataKey, DiskPoolKey, _, _, _} = Args,
	case ar_data_roots:next(Iterator) of
		{ok, DataRootEntry, Iterator2} ->
			{_, _, TXStartOffset, _} = DataRootEntry,
			AbsoluteEndOffset = TXStartOffset + Offset,
			case ar_data_sync:get_chunk_metadata(AbsoluteEndOffset, StoreID) of
				not_found ->
					ok;
				{ok, ChunkArgs} ->
					case element(1, ChunkArgs) of
						ChunkDataKey ->
							PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),
							StartOffset = ar_block:get_chunk_padded_offset(
									AbsoluteEndOffset - ChunkSize),
							ok = ar_footprint_record:delete(PaddedOffset, StoreID),
							ok = ar_sync_record:delete(PaddedOffset, StartOffset, ar_data_sync,
									StoreID),
							case ar_sync_record:is_recorded(PaddedOffset, ar_data_sync) of
								false ->
									ar_events:send(sync_record,
											{global_remove_range, StartOffset, PaddedOffset});
								{{true, {replica_2_9, _}}, _StoreID} ->
									ar_events:send(sync_record,
											{global_remove_range, StartOffset, PaddedOffset});
								_ ->
									ok
							end,
							ok = ar_data_sync:delete_chunk_metadata(AbsoluteEndOffset, StoreID);
						_ ->
							ok
					end
			end,
			delete_chunk(Iterator2, Args, StoreID, DiskPool);
		_ ->
			ok = ar_kv:delete(DiskPoolChunksIndex, DiskPoolKey),
			ok = ar_data_sync:delete_chunk_data(ChunkDataKey, StoreID),
			<< _Timestamp:256, DataPathHash2/binary >> = DiskPoolKey,
			remove_chunk_from_cache(DataPathHash2),
			DataRootKey = ar_data_roots:key(Iterator),
			decrease_occupied_size(ChunkSize, DataRootKey)
	end.
