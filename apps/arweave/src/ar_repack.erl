-module(ar_repack).

-export([read_cursor/3, store_cursor/3, repack/5, chunk_repacked/5]).

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").
-include("../include/ar_config.hrl").

-moduledoc """
	This module handles the repack-in-place logic. This logic is orchestrated by the
	ar_chunk_storage gen_servers.
""".

read_cursor(StoreID, TargetPacking, RangeStart) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	DefaultCursor = ar_chunk_storage:get_chunk_bucket_start(RangeStart + 1),
	case file:read_file(Filepath) of
		{ok, Bin} ->
			case catch binary_to_term(Bin) of
				{Cursor, TargetPacking} when is_integer(Cursor) ->
					Cursor;
				_ ->
					DefaultCursor
			end;
		_ ->
			DefaultCursor
	end.

store_cursor(none, _StoreID, _TargetPacking) ->
	ok;
store_cursor(Cursor, StoreID, TargetPacking) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	file:write_file(Filepath, term_to_binary({Cursor, TargetPacking})).

%% @doc Advance the repack cursor in SectorSize increments. This will optimize the use of
%% any entropy that has been generated. Once we hit the end of the range, restart at the
%% beginning, but offset by the repack interval size. Repeat this loop back process until
%% the looped back position is greater than the sector size.
advance_cursor(Cursor, RangeStart, RangeEnd) ->
	SectorSize = ar_replica_2_9:get_sector_size(),
	Cursor2 = ar_chunk_storage:get_chunk_bucket_start(Cursor + SectorSize + ?DATA_CHUNK_SIZE),
	case Cursor2 > ar_chunk_storage:get_chunk_bucket_start(RangeEnd) of
		true ->
			RepackIntervalSize = get_repack_interval_size(Cursor, RangeStart),
			RangeStart2 = ar_chunk_storage:get_chunk_bucket_start(RangeStart + 1),
			RelativeSectorOffset = (Cursor - RangeStart2) rem SectorSize,
			Cursor3 = RangeStart2
				+ RelativeSectorOffset
				+ RepackIntervalSize,
			case Cursor3 >= RangeStart2 + SectorSize of
				true ->
					none;
				false ->
					Cursor3
			end;
		false ->
			Cursor2
	end.

repack(none, _RangeStart, _RangeEnd, _Packing, StoreID) ->
	ar_chunk_storage:set_repacking_complete(StoreID);
repack(Cursor, RangeStart, RangeEnd, Packing, StoreID) ->
	RepackIntervalSize = get_repack_interval_size(Cursor, RangeStart),
	RightBound = Cursor + RepackIntervalSize,
	?LOG_DEBUG([{event, repacking_in_place},
			{tags, [repack_in_place]},
			{pid, self()},
			{store_id, StoreID},
			{s, Cursor},
			{e, RightBound},
			{repack_interval_size, RepackIntervalSize},
			{range_start, RangeStart},
			{range_end, RangeEnd},
			{packing, ar_serialize:encode_packing(Packing, true)}]),
	case ar_sync_record:get_next_synced_interval(Cursor, RightBound,
			ar_data_sync, StoreID) of
		not_found ->
			?LOG_DEBUG([{event, repack_in_place_no_synced_interval},
					{tags, [repack_in_place]},
					{pid, self()},
					{store_id, StoreID},
					{s, Cursor},
					{e, RightBound},
					{range_start, RangeStart},
					{range_end, RangeEnd}]),
			Server = ar_chunk_storage:name(StoreID),
			Cursor2 = advance_cursor(Cursor, RangeStart, RangeEnd),
			gen_server:cast(Server, {repack, Cursor2, RangeStart, RangeEnd, Packing});
		{_End, _Start} ->
			repack_batch(Cursor, RangeStart, RangeEnd, Packing, StoreID)
	end.

repack_batch(Cursor, RangeStart, RangeEnd, RequiredPacking, StoreID) ->
	RepackIntervalSize = get_repack_interval_size(Cursor, RangeStart),
	Server = ar_chunk_storage:name(StoreID),
	Cursor2 = advance_cursor(Cursor, RangeStart, RangeEnd),
	RepackFurtherArgs = {repack, Cursor2, RangeStart, RangeEnd, RequiredPacking},
	CheckPackingBuffer =
		case ar_packing_server:is_buffer_full() of
			true ->
				?LOG_DEBUG([{event, repack_in_place_buffer_full},
						{tags, [repack_in_place]},
						{pid, self()},
						{store_id, StoreID},
						{s, Cursor},
						{range_start, RangeStart},
						{range_end, RangeEnd},
						{required_packing, ar_serialize:encode_packing(RequiredPacking, true)}]),
				ar_util:cast_after(200, Server,
						{repack, Cursor, RangeStart, RangeEnd, RequiredPacking}),
				continue;
			false ->
				ok
		end,
	OffsetToChunkMap =
		case CheckPackingBuffer of
			continue ->
				continue;
			ok ->
				read_chunk_range(Cursor, RepackIntervalSize, StoreID, RepackFurtherArgs)
		end,
	OffsetToMetadataMap =
		case OffsetToChunkMap of
			continue ->
				continue;
			_ ->
				read_chunk_metadata_range(Cursor, RepackIntervalSize, RangeEnd,
						StoreID, RepackFurtherArgs)
		end,
	case OffsetToMetadataMap of
		continue ->
			ok;
		_ ->
			Args = {StoreID, RequiredPacking, OffsetToChunkMap},
			send_chunks_for_repacking(OffsetToMetadataMap, Args),
			repack_further(StoreID, RepackFurtherArgs)
	end.

repack_further(StoreID, RepackFurtherArgs) ->
	{repack, Cursor, RangeStart, RangeEnd, RequiredPacking} = RepackFurtherArgs,
	?LOG_DEBUG([{event, repack_further},
		{tags, [repack_in_place]},
		{pid, self()},
		{store_id, StoreID},
		{s, Cursor},
		{range_start, RangeStart},
		{range_end, RangeEnd},
		{required_packing, ar_serialize:encode_packing(RequiredPacking, true)}]),
	gen_server:cast(ar_chunk_storage:name(StoreID), RepackFurtherArgs).

read_chunk_range(Start, Size, StoreID, RepackFurtherArgs) ->
	case catch ar_chunk_storage:get_range(Start, Size, StoreID) of
		[] ->
			?LOG_DEBUG([{event, repack_in_place_no_chunks_to_repack},
					{tags, [repack_in_place]},
					{pid, self()},
					{store_id, StoreID},
					{start, Start},
					{size, Size}]),
			#{};
		{'EXIT', _Exc} ->
			?LOG_ERROR([{event, failed_to_read_chunk_range},
					{tags, [repack_in_place]},
					{pid, self()},
					{store_id, StoreID},
					{start, Start},
					{size, Size}]),
			repack_further(StoreID, RepackFurtherArgs),
			continue;
		Range ->
			{_, _, Map} = chunk_offset_list_to_map(Range),
			Map
	end.

read_chunk_metadata_range(Start, Size, End, StoreID, RepackFurtherArgs) ->
	Server = ar_chunk_storage:name(StoreID),
	End2 = min(Start + Size, End),
	
	case ar_data_sync:get_chunk_metadata_range(Start+1, End2, StoreID) of
		{ok, MetadataMap} ->
			MetadataMap;
		{error, Error} ->
			?LOG_ERROR([{event, failed_to_read_chunk_metadata_range},
					{store_id, StoreID},
					{error, io_lib:format("~p", [Error])}]),
			gen_server:cast(Server, RepackFurtherArgs),
			continue
	end.

send_chunks_for_repacking(MetadataMap, Args) ->
	maps:fold(send_chunks_for_repacking(Args), ok, MetadataMap).

send_chunks_for_repacking(Args) ->
	fun	(AbsoluteOffset, {_, _TXRoot, _, _, _, ChunkSize}, ok)
				when ChunkSize /= ?DATA_CHUNK_SIZE,
						AbsoluteOffset =< ?STRICT_DATA_SPLIT_THRESHOLD ->
			{StoreID, _RequiredPacking, _ChunkMap} = Args,
			?LOG_DEBUG([{event, skipping_small_chunk},
					{tags, [repack_in_place]},
					{store_id, StoreID},
					{offset, AbsoluteOffset},
					{chunk_size, ChunkSize}]),
			ok;
		(AbsoluteOffset, ChunkMeta, ok) ->
			send_chunk_for_repacking(AbsoluteOffset, ChunkMeta, Args)
	end.

send_chunk_for_repacking(AbsoluteOffset, ChunkMeta, Args) ->
	{StoreID, RequiredPacking, ChunkMap} = Args,
	Server = ar_chunk_storage:name(StoreID),
	PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
	{ChunkDataKey, TXRoot, DataRoot, TXPath,
			RelativeOffset, ChunkSize} = ChunkMeta,
	case ar_sync_record:is_recorded(PaddedOffset, ar_data_sync, StoreID) of
		{true, unpacked_padded} ->
			%% unpacked_padded is a special internal packing used
			%% for temporary storage of unpacked and padded chunks
			%% before they are enciphered with the 2.9 entropy.
			?LOG_WARNING([
				{event, repack_in_place_found_unpacked_padded},
				{tags, [repack_in_place]},
				{store_id, StoreID},
				{packing,
					ar_serialize:encode_packing(RequiredPacking,true)},
				{offset, AbsoluteOffset}]),
			ok;
		{true, RequiredPacking} ->
			?LOG_WARNING([{event, repack_in_place_found_already_repacked},
					{tags, [repack_in_place]},
					{store_id, StoreID},
					{packing,
						ar_serialize:encode_packing(RequiredPacking, true)},
					{offset, AbsoluteOffset}]),
			ok;
		{true, Packing} ->
			ChunkMaybeDataPath =
				case maps:get(PaddedOffset, ChunkMap, not_found) of
					not_found ->
						read_chunk_and_data_path(StoreID,
								ChunkDataKey, AbsoluteOffset, no_chunk);
					Chunk3 ->
						case ar_chunk_storage:is_storage_supported(AbsoluteOffset,
								ChunkSize, RequiredPacking) of
							false ->
								%% We are going to move this chunk to
								%% RocksDB after repacking so we read
								%% its DataPath here to pass it later on
								%% to store_chunk.
								read_chunk_and_data_path(StoreID,
										ChunkDataKey, AbsoluteOffset, Chunk3);
							true ->
								%% We are going to repack the chunk and keep it
								%% in the chunk storage - no need to make an
								%% extra disk access to read the data path.
								{Chunk3, none}
						end
				end,
			case ChunkMaybeDataPath of
				not_found ->
					ok;
				{Chunk, MaybeDataPath} ->
					RequiredPacking2 =
						case RequiredPacking of
							{replica_2_9, _} ->
								unpacked_padded;
							Packing2 ->
								Packing2
						end,
					Ref = make_ref(),
					RepackArgs = {Packing, MaybeDataPath, RelativeOffset,
							DataRoot, TXPath, none, none},
					gen_server:cast(Server,
							{register_packing_ref, Ref, RepackArgs}),
					ar_util:cast_after(300000, Server,
							{expire_repack_request, Ref}),
					ar_packing_server:request_repack(Ref, whereis(Server),
							{RequiredPacking2, Packing, Chunk,
									AbsoluteOffset, TXRoot, ChunkSize})
			end;
		true ->
			?LOG_WARNING([{event, repack_in_place_found_no_packing_information},
					{tags, [repack_in_place]},
					{store_id, StoreID},
					{offset, PaddedOffset}]),
			ok;
		false ->
			?LOG_WARNING([{event, repack_in_place_chunk_not_found},
					{tags, [repack_in_place]},
					{store_id, StoreID},
					{offset, PaddedOffset}]),
			ok
	end.

chunk_repacked(ChunkArgs, Args, StoreID, FileIndex, EntropyContext) ->
	{Packing, Chunk, Offset, _, ChunkSize} = ChunkArgs,
	PaddedEndOffset = ar_block:get_chunk_padded_offset(Offset),
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	IsStorageSupported =
		ar_chunk_storage:is_storage_supported(PaddedEndOffset, ChunkSize, Packing),

	RemoveFromSyncRecordResult = ar_sync_record:delete(PaddedEndOffset,
			StartOffset, ar_data_sync, StoreID),
	RemoveFromSyncRecordResult2 =
		case RemoveFromSyncRecordResult of
			ok ->
				ar_sync_record:delete(PaddedEndOffset,
					StartOffset, ar_chunk_storage, StoreID);
			Error ->
				Error
		end,

	case {RemoveFromSyncRecordResult2, IsStorageSupported} of
		{ok, false} ->
			gen_server:cast(ar_data_sync:name(StoreID), {store_chunk, ChunkArgs, Args}),
			{ok, FileIndex};
		{ok, true} ->
			StoreResults = ar_chunk_storage:store_chunk(PaddedEndOffset, Chunk, Packing,
					StoreID, FileIndex, EntropyContext),
			case StoreResults of
				{ok, FileIndex2, NewPacking} ->
					ar_sync_record:add_async(repacked_chunk,
							PaddedEndOffset, StartOffset,
							NewPacking, ar_data_sync, StoreID),
					{ok, FileIndex2};
				Error3 ->
					?LOG_ERROR([{event, failed_to_store_repacked_chunk},
							{tags, [repack_in_place]},
							{store_id, StoreID},
							{padded_end_offset, PaddedEndOffset},
							{requested_packing, ar_serialize:encode_packing(Packing, true)},
							{error, io_lib:format("~p", [Error3])}]),
					{ok, FileIndex}
			end;
		{Error4, _} ->
			?LOG_ERROR([{event, failed_to_store_repacked_chunk},
					{tags, [repack_in_place]},
					{store_id, StoreID},
					{padded_end_offset, PaddedEndOffset},
					{requested_packing, ar_serialize:encode_packing(Packing, true)},
					{error, io_lib:format("~p", [Error4])}]),
			{ok, FileIndex}
	end.

read_chunk_and_data_path(StoreID, ChunkDataKey, AbsoluteOffset, MaybeChunk) ->
	case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			?LOG_WARNING([{event, chunk_not_found_in_chunk_data_db},
					{tags, [repack_in_place]},
					{store_id, StoreID},
					{offset, AbsoluteOffset}]),
			not_found;
		{ok, V} ->
			case binary_to_term(V) of
				{Chunk, DataPath} ->
					{Chunk, DataPath};
				DataPath when MaybeChunk /= no_chunk ->
					{MaybeChunk, DataPath};
				_ ->
					?LOG_WARNING([{event, chunk_not_found2},
						{tags, [repack_in_place]},
						{store_id, StoreID},
						{offset, AbsoluteOffset}]),
					not_found
			end
	end.

%% @doc By default we repack `repack_batch_size` chunks at a time. However we don't want
%% a single repack batch to cross a sector boundary.
get_repack_interval_size(Cursor, RangeStart) ->
	{ok, Config} = application:get_env(arweave, config),
	SectorSize = ar_replica_2_9:get_sector_size(),
	RepackBatchSize = ?DATA_CHUNK_SIZE * Config#config.repack_batch_size,
	RangeStart2 = ar_chunk_storage:get_chunk_bucket_start(RangeStart + 1),
	RelativeSectorOffset = (Cursor - RangeStart2) rem SectorSize,
	min(RepackBatchSize, SectorSize - RelativeSectorOffset).

chunk_offset_list_to_map(ChunkOffsets) ->
	chunk_offset_list_to_map(ChunkOffsets, infinity, 0, #{}).

chunk_offset_list_to_map([], Min, Max, Map) ->
	{Min, Max, Map};
chunk_offset_list_to_map([{Offset, Chunk} | ChunkOffsets], Min, Max, Map) ->
	chunk_offset_list_to_map(ChunkOffsets, min(Min, Offset), max(Max, Offset),
			maps:put(Offset, Chunk, Map)).