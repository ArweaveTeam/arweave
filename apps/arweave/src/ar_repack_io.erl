-module(ar_repack_io).

-behaviour(gen_server).

-export([name/1, read_footprint/4, write_queue/3]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_repack.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
	This module handles disk IO for the repack-in-place process.
""".

-record(state, {
	store_id = undefined,
	read_batch_size = ?DEFAULT_REPACK_BATCH_SIZE
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE,  StoreID, []).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
	list_to_atom("ar_repack_io_" ++ ar_storage_module:label(StoreID)).

init(StoreID) ->
	{ok, Config} = application:get_env(arweave, config),
	ReadBatchSize = Config#config.repack_batch_size,
	State = #state{ 
		store_id = StoreID,
		read_batch_size = ReadBatchSize
	},
	log_info(ar_repack_io_init, State, [
		{name, name(StoreID)},
		{read_batch_size, ReadBatchSize}
	]),
	
    {ok, State}.

%% @doc Read all the chunks covered by the given footprint.
%% The footprint covers:
%% - A list of offsets determined by the replica.2.9 entropy footprint pattern.
%% - A set of consecutive chunks following each offset. The number of consecutive chunks
%%   read for each footprint offset is determined by the repack_batch_size config.
-spec read_footprint(
	[non_neg_integer()], non_neg_integer(), non_neg_integer(), ar_storage_module:store_id()) ->
	ok.
read_footprint(FootprintOffsets, FootprintStart, FootprintEnd, StoreID) ->
	gen_server:cast(name(StoreID),
		{read_footprint, FootprintOffsets, FootprintStart, FootprintEnd}).

write_queue(WriteQueue, Packing, StoreID) ->
	gen_server:cast(name(StoreID), {write_queue, WriteQueue, Packing}).
	

%%%===================================================================
%%% Gen server callbacks.
%%%===================================================================

handle_call(Request, _From, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(
		{read_footprint, FootprintOffsets, FootprintStart, FootprintEnd}, #state{} = State) ->
	do_read_footprint(FootprintOffsets, FootprintStart, FootprintEnd, State),
	{noreply, State};

handle_cast({write_queue, WriteQueue, Packing}, #state{} = State) ->
	process_write_queue(WriteQueue, Packing, State),
	{noreply, State};

handle_cast(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

terminate(Reason, #state{} = State) ->
	log_debug(terminate, State, [
		{module, ?MODULE},
		{reason, ar_util:safe_format(Reason)}
	]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

do_read_footprint([], _FootprintStart, _FootprintEnd, #state{}) ->
	ok;
do_read_footprint([
	BucketEndOffset | FootprintOffsets], FootprintStart, FootprintEnd, #state{} = State) 
		when BucketEndOffset < FootprintStart ->
	%% Advance until we hit a chunk covered by the current storage module
	do_read_footprint(FootprintOffsets, FootprintStart, FootprintEnd, State);
do_read_footprint(
	[BucketEndOffset | _FootprintOffsets], _FootprintStart, FootprintEnd, #state{} ) 
		when BucketEndOffset > FootprintEnd ->
	ok;
do_read_footprint(
	[BucketEndOffset | FootprintOffsets], FootprintStart, FootprintEnd, #state{} = State) ->
	#state{ 
		store_id = StoreID,
		read_batch_size = ReadBatchSize
	} = State,

	StartTime = erlang:monotonic_time(),
	{ReadRangeStart, ReadRangeEnd, _ReadRangeOffsets} = ar_repack:get_read_range(
		BucketEndOffset, FootprintEnd, ReadBatchSize),
	ReadRangeSizeInBytes = ReadRangeEnd - ReadRangeStart,
	OffsetChunkMap = 
		case catch ar_chunk_storage:get_range(ReadRangeStart, ReadRangeSizeInBytes, StoreID) of
			[] ->
				#{};
			{'EXIT', _Exc} ->
				log_error(failed_to_read_chunk_range, State, [
					{read_range_start, ReadRangeStart},
					{read_range_end, ReadRangeEnd},
					{read_range_size_bytes, ReadRangeSizeInBytes}
				]),
				#{};
			Range ->
				maps:from_list(Range)
		end,

	
	OffsetMetadataMap =
		case ar_data_sync:get_chunk_metadata_range(ReadRangeStart+1, ReadRangeEnd, StoreID) of
			{ok, MetadataMap} ->
				MetadataMap;
			{error, invalid_iterator} ->
				#{};
			{error, Reason} ->
				log_warning(failed_to_read_chunk_metadata, State, [
					{read_range_start, ReadRangeStart},
					{read_range_end, ReadRangeEnd},
					{reason, Reason}
				]),
				#{}
		end,

	ChunkReadSizeInBytes = maps:fold(
		fun(_Key, Value, Acc) -> Acc + byte_size(Value) end, 
		0, 
		OffsetChunkMap
	),
	ar_metrics:record_rate_metric(
		StartTime, ChunkReadSizeInBytes,
		chunk_read_rate_bytes_per_second, [ar_storage_module:label(StoreID), repack]),

	EndTime = erlang:monotonic_time(),
	ElapsedTime =  max(1, erlang:convert_time_unit(EndTime - StartTime, native, millisecond)),
	log_debug(read_footprint, State, [
		{bucket_end_offset, BucketEndOffset},
		{read_range_start, ReadRangeStart},
		{read_range_end, ReadRangeEnd},
		{read_range_size_bytes, ReadRangeSizeInBytes},
		{chunk_read_size_bytes, ChunkReadSizeInBytes},
		{chunks_read, maps:size(OffsetChunkMap)},
		{metadata_read, maps:size(OffsetMetadataMap)},
		{footprint_start, FootprintStart},
		{footprint_end, FootprintEnd},
		{remaining_offsets, length(FootprintOffsets)},
		{time_taken, ElapsedTime},
		{rate, (ChunkReadSizeInBytes / ?MiB / ElapsedTime) * 1000}
	]),

	ar_repack:chunk_range_read(
		BucketEndOffset, OffsetChunkMap, OffsetMetadataMap, State#state.store_id),
	read_footprint(FootprintOffsets, FootprintStart, FootprintEnd, StoreID).

process_write_queue(WriteQueue, Packing, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	StartTime = erlang:monotonic_time(),
    gb_sets:fold(
        fun({_BucketEndOffset, RepackChunk}, _) ->
			write_repack_chunk(RepackChunk, Packing, State)
        end,
        ok,
        WriteQueue
    ),
	ar_metrics:record_rate_metric(
		StartTime, gb_sets:size(WriteQueue) * ?DATA_CHUNK_SIZE,
		chunk_write_rate_bytes_per_second, [ar_storage_module:label(StoreID), repack]),
	EndTime = erlang:monotonic_time(),
	ElapsedTime =  max(1, erlang:convert_time_unit(EndTime - StartTime, native, millisecond)),
	log_debug(process_write_queue, State, [
		{write_queue_size, gb_sets:size(WriteQueue)},
		{time_taken, ElapsedTime},
		{rate, (gb_sets:size(WriteQueue) / 4 / ElapsedTime) * 1000}
	]).

write_repack_chunk(RepackChunk, Packing, #state{} = State) ->
	#state{ 
		store_id = StoreID
	} = State,
	
	case RepackChunk#repack_chunk.state of
		write_entropy ->
			{replica_2_9, RewardAddr} = Packing,
			Entropy = RepackChunk#repack_chunk.target_entropy,
			BucketEndOffset = RepackChunk#repack_chunk.offsets#chunk_offsets.bucket_end_offset,
			ar_entropy_storage:store_entropy(Entropy, BucketEndOffset, StoreID, RewardAddr);
		write_chunk ->
			write_chunk(RepackChunk, Packing, State);
		_ ->
			log_error(unexpected_chunk_state, State, format_logs(RepackChunk))
	end.

write_chunk(RepackChunk, TargetPacking, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	#repack_chunk{
		offsets = Offsets,
		metadata = Metadata,
		chunk = Chunk
	} = RepackChunk,
	#chunk_offsets{
		absolute_offset = AbsoluteOffset
	} = Offsets,

	IsBlacklisted = ar_tx_blacklist:is_byte_blacklisted(AbsoluteOffset),
	
	case remove_from_sync_record(Offsets, StoreID) of
		ok when IsBlacklisted == true ->
			ok;
		ok when IsBlacklisted == false ->
			WriteResult = 
				ar_data_sync:write_chunk(
					AbsoluteOffset, Metadata, Chunk, TargetPacking, StoreID),
			case WriteResult of
				{ok, TargetPacking} ->
					add_to_sync_record(Offsets, Metadata, TargetPacking, StoreID);
				{ok, WrongPacking} ->
					%% This shouldn't ever happen - the only time write_chunk should change
					%% the packing is when writing to unpacked_padded.
					log_error(repacked_chunk_stored_with_wrong_packing, State, [
						{requested_packing, ar_serialize:encode_packing(TargetPacking, true)},
						{stored_packing, ar_serialize:encode_packing(WrongPacking, true)}
					]);
				Error ->
					log_error(failed_to_store_repacked_chunk, State, [
						{requested_packing, ar_serialize:encode_packing(TargetPacking, true)},
						{error, io_lib:format("~p", [Error])}
					])
			end;
		Error ->
			log_error(failed_to_remove_from_sync_record, State, [
				{error, io_lib:format("~p", [Error])}
			])
	end.

remove_from_sync_record(Offsets, StoreID) ->
	#chunk_offsets{
		padded_end_offset = PaddedEndOffset
	} = Offsets,

	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	
	case ar_sync_record:delete(PaddedEndOffset, StartOffset, ar_data_sync, StoreID) of
		ok ->
			ar_sync_record:delete(PaddedEndOffset, StartOffset, ar_chunk_storage, StoreID);
		Error ->
			Error
	end.

add_to_sync_record(Offsets, Metadata, Packing, StoreID) ->
	#chunk_offsets{
		padded_end_offset = PaddedEndOffset,
		bucket_end_offset = BucketEndOffset
	} = Offsets,
	#chunk_metadata{
		chunk_size = ChunkSize
	} = Metadata,
	
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	ar_sync_record:add(PaddedEndOffset, StartOffset, Packing, ar_data_sync, StoreID),

	IsStorageSupported =
		ar_chunk_storage:is_storage_supported(PaddedEndOffset, ChunkSize, Packing),
	IsReplica29 = case Packing of
		{replica_2_9, _} -> true;
		_ -> false
	end,

	case IsStorageSupported andalso IsReplica29 of
		true ->
			ar_entropy_storage:add_record(BucketEndOffset, Packing, StoreID);
		_ -> ok
	end.

log_error(Event, #state{} = State, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, State, ExtraLogs)).

log_warning(Event, #state{} = State, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, State, ExtraLogs)).

log_info(Event, #state{} = State, ExtraLogs) ->
	?LOG_INFO(format_logs(Event, State, ExtraLogs)).
	
log_debug(Event, #state{} = State, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, State, ExtraLogs)).

format_logs(Event, #state{} = State, ExtraLogs) ->
	[
		{event, Event},
		{tags, [repack_in_place, ar_repack_io]},
		{pid, self()},
		{store_id, State#state.store_id}
		| ExtraLogs
	].

format_logs(#repack_chunk{} = RepackChunk) ->
	#repack_chunk{
		state = ChunkState,
		offsets = Offsets,
		metadata = Metadata,
		chunk = Chunk,
		source_packing = SourcePacking,
		target_packing = TargetPacking,
		target_entropy = TargetEntropy,
		source_entropy = SourceEntropy
	} = RepackChunk,
	#chunk_offsets{	
		absolute_offset = AbsoluteOffset,
		bucket_end_offset = BucketEndOffset,
		padded_end_offset = PaddedEndOffset
	} = Offsets,
	ChunkSize = case Metadata of
		#chunk_metadata{chunk_size = Size} -> Size;
		_ -> Metadata
	end,
	[
		{state, ChunkState},
		{bucket_end_offset, BucketEndOffset},
		{absolute_offset, AbsoluteOffset},
		{padded_end_offset, PaddedEndOffset},
		{chunk_size, ChunkSize},
		{chunk, atom_or_binary(Chunk)},
		{source_packing, ar_serialize:encode_packing(SourcePacking, false)},
		{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
		{source_entropy, atom_or_binary(SourceEntropy)},
		{target_entropy, atom_or_binary(TargetEntropy)}
	].

atom_or_binary(Atom) when is_atom(Atom) -> Atom;
atom_or_binary(Bin) when is_binary(Bin) -> binary:part(Bin, {0, min(10, byte_size(Bin))}).	
