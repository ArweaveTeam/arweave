-module(ar_repack_io).

-behaviour(gen_server).

-export([name/1, init/1, read_batch/4, write_queue/4]).

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
	read_batch_size = ?DEFAULT_REPACK_BATCH_SIZE,
	module_start = 0,
	module_end = 0
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE,  StoreID, []).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
	list_to_atom("ar_repack_io_" ++ ar_storage_module:label_by_id(StoreID)).

init(StoreID) ->
	{ModuleStart, ModuleEnd} = ar_storage_module:get_range(StoreID),
	{ok, Config} = application:get_env(arweave, config),
	ReadBatchSize = Config#config.repack_batch_size,
	State = #state{ 
		store_id = StoreID,
		module_start = ModuleStart,
		module_end = ModuleEnd,
		read_batch_size = ReadBatchSize
	},
	log_info(ar_repack_io_init, State, [
		{name, name(StoreID)},
		{read_batch_size, ReadBatchSize}
	]),
	
    {ok, State}.

read_batch(FootprintOffsets, BatchStart, BatchEnd, StoreID) ->
	gen_server:cast(name(StoreID), {read_batch, FootprintOffsets, BatchStart, BatchEnd}).

write_queue(WriteQueue, Packing, RewardAddr, StoreID) ->
	gen_server:cast(name(StoreID), {write_queue, WriteQueue, Packing, RewardAddr}).
	

%%%===================================================================
%%% Gen server callbacks.
%%%===================================================================

handle_call(Request, _From, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({read_batch, FootprintOffsets, BatchStart, BatchEnd}, #state{} = State) ->
	do_read_batch(FootprintOffsets, BatchStart, BatchEnd, State),
	{noreply, State};

handle_cast({write_queue, WriteQueue, Packing, RewardAddr}, #state{} = State) ->
	process_write_queue(WriteQueue, Packing, RewardAddr, State),
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

do_read_batch([], _BatchStart, _BatchEnd, #state{}) ->
	ok;
do_read_batch([BucketEndOffset | FootprintOffsets], BatchStart, BatchEnd, #state{} = State) 
		when BucketEndOffset < BatchStart ->
	%% Advance until we hit a chunk covered by the current storage module
	do_read_batch(FootprintOffsets, BatchStart, BatchEnd, State);
do_read_batch([BucketEndOffset | _FootprintOffsets], _BatchStart, BatchEnd, #state{} ) 
		when BucketEndOffset > BatchEnd ->
	ok;
do_read_batch([BucketEndOffset | FootprintOffsets], BatchStart, BatchEnd, #state{} = State) ->
	#state{ 
		store_id = StoreID,
		module_start = ModuleStart,
		read_batch_size = ReadBatchSize
	} = State,
	StartTime = erlang:monotonic_time(),
	{ReadRangeStart, ReadRangeEnd, _ReadRangeOffsets} = ar_repack:get_read_range(
		BucketEndOffset, ModuleStart, BatchEnd, ReadBatchSize),
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
	EndTime = erlang:monotonic_time(),
	log_debug(read_batch, State, [
		{read_range_start, ReadRangeStart},
		{read_range_end, ReadRangeEnd},
		{read_range_size_bytes, ReadRangeSizeInBytes},
		{time_taken, erlang:convert_time_unit(EndTime - StartTime, native, millisecond)}
	]),

	ar_repack:chunk_range_read(
		BucketEndOffset, OffsetChunkMap, OffsetMetadataMap, State#state.store_id),
	read_batch(FootprintOffsets, BatchStart, BatchEnd, StoreID).


process_write_queue(WriteQueue, Packing, RewardAddr, #state{} = State) ->
	StartTime = erlang:monotonic_time(),
    gb_sets:fold(
        fun({_BucketEndOffset, ChunkInfo}, _) ->
			write_chunk_info(ChunkInfo, Packing, RewardAddr, State)
        end,
        ok,
        WriteQueue
    ),
	EndTime = erlang:monotonic_time(),
	log_debug(process_write_queue, State, [
		{write_queue_size, gb_sets:size(WriteQueue)},
		{time_taken, erlang:convert_time_unit(EndTime - StartTime, native, millisecond)}
	]).

write_chunk_info(ChunkInfo, Packing, RewardAddr, #state{} = State) ->
	#state{ 
		store_id = StoreID
	} = State,
	
	case ChunkInfo#chunk_info.state of
		write_entropy ->
			Entropy = ChunkInfo#chunk_info.entropy,
			BucketEndOffset = ChunkInfo#chunk_info.offsets#chunk_offsets.bucket_end_offset,
			ar_entropy_storage:store_entropy(Entropy, BucketEndOffset, StoreID, RewardAddr);
		write_chunk ->
			wite_chunk(ChunkInfo, Packing, State);
		_ ->
			log_error(unexpected_chunk_state, State, [ format_logs(ChunkInfo) ])
	end.

wite_chunk(ChunkInfo, TargetPacking, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	#chunk_info{
		offsets = Offsets,
		metadata = Metadata,
		chunk = Chunk,
		data_path = DataPath
	} = ChunkInfo,
	#chunk_offsets{
		absolute_offset = AbsoluteOffset,
		padded_end_offset = PaddedEndOffset,
		relative_offset = RelativeOffset
	} = Offsets,
	#chunk_metadata{
		tx_root = TXRoot,
		data_root = DataRoot,
		tx_path = TXPath,
		chunk_data_key = ChunkDataKey,
		chunk_size = ChunkSize
	} = Metadata,
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	IsStorageSupported =
		ar_chunk_storage:is_storage_supported(PaddedEndOffset, ChunkSize, TargetPacking),

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
			ChunkArgs = {TargetPacking, Chunk, AbsoluteOffset, TXRoot, ChunkSize},
			Args = {
				TargetPacking, DataPath, RelativeOffset, 
				DataRoot, TXPath, StoreID, ChunkDataKey
			},
			gen_server:cast(ar_data_sync:name(StoreID), {store_chunk, ChunkArgs, Args});
		{ok, true} ->
			update_chunk(TargetPacking, ChunkInfo, State);
		{Error2, _} ->
			log_error(failed_to_update_sync_record_for_repacked_chunk, State, [
				format_logs(ChunkInfo) ++ [{error, io_lib:format("~p", [Error2])}]
			])
	end.

update_chunk(Packing, ChunkInfo, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	#chunk_info{
		offsets = Offsets,
		chunk = Chunk
	} = ChunkInfo,
	#chunk_offsets{
		absolute_offset = AbsoluteOffset
	} = Offsets,
	PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(AbsoluteOffset),
	case ar_chunk_storage:put(PaddedEndOffset, Chunk, Packing, StoreID) of
		{ok, NewPacking} ->
			case NewPacking of
				{replica_2_9, _} ->
					BucketStartOffset = BucketEndOffset - ?DATA_CHUNK_SIZE,
					ar_sync_record:add_async(repacked_chunk,
						BucketEndOffset, BucketStartOffset,
						ar_chunk_storage_replica_2_9_1_entropy, StoreID);
				_ -> ok
			end,
			StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
			ar_sync_record:add_async(repacked_chunk,
					PaddedEndOffset, StartOffset,
					NewPacking, ar_data_sync, StoreID);
		Error ->
			log_error(failed_to_store_repacked_chunk, State, [
				format_logs(ChunkInfo) ++ 
				[
					{requested_packing, ar_serialize:encode_packing(Packing, true)},
					{error, io_lib:format("~p", [Error])}
				]
			])
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
		{tags, [repack_in_place]},
		{pid, self()},
		{store_id, State#state.store_id}
		| ExtraLogs
	].

format_logs(#chunk_info{} = ChunkInfo) ->
	#chunk_info{
		state = ChunkState,
		offsets = Offsets,
		metadata = Metadata,
		chunk = Chunk,
		entropy = Entropy
	} = ChunkInfo,
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
		{entropy, atom_or_binary(Entropy)}
	].

atom_or_binary(Atom) when is_atom(Atom) -> Atom;
atom_or_binary(Bin) when is_binary(Bin) -> binary:part(Bin, {0, min(10, byte_size(Bin))}).	
