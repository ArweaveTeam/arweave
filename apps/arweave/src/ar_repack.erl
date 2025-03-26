-module(ar_repack).

-behaviour(gen_server).

-export([name/1, register_workers/0]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_sup.hrl").
-include("ar_config.hrl").
-include("ar_repack.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
	This module handles the repack-in-place logic.
""".

-record(state, {
	store_id = undefined,
	read_batch_size = ?DEFAULT_REPACK_BATCH_SIZE,
	write_batch_size = 400,
	range_start = 0,
	range_end = 0,
	iteration_start = 0,
	iteration_end = 0,
	entropy_start = 0,
	entropy_end = 0,
	next_cursor = 0, 
	target_packing = undefined,
	reward_addr = undefined,
	repack_status = undefined,
	chunk_info_map = #{},
	write_queue = gb_sets:new()
}).

-ifdef(AR_TEST).
-define(DEVICE_LOCK_WAIT, 100).
-else.
-define(DEVICE_LOCK_WAIT, 5_000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, Packing}) ->
	gen_server:start_link({local, Name}, ?MODULE,  {StoreID, Packing}, []).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
	list_to_atom("ar_repack_" ++ ar_storage_module:label_by_id(StoreID)).

register_workers() ->
    {ok, Config} = application:get_env(arweave, config),
    
    RepackInPlaceWorkers = lists:filtermap(
        fun({StorageModule, Packing}) ->
            StoreID = ar_storage_module:id(StorageModule),
            %% Note: the config validation will prevent a StoreID from being used in both
            %% `storage_modules` and `repack_in_place_storage_modules`, so there's
            %% no risk of a `Name` clash with the workers spawned above.
            case ar_entropy_gen:is_entropy_packing(Packing) of
                true ->
                    Worker = ?CHILD_WITH_ARGS(
                        ar_repack, worker, name(StoreID),
                        [name(StoreID), {StoreID, Packing}]),
                    {true, Worker};
                false ->
                    false
            end
        end,
        Config#config.repack_in_place_storage_modules
    ),

    RepackInPlaceWorkers.

init({StoreID, ToPacking}) ->
	FromPacking = ar_storage_module:get_packing(StoreID),
	?LOG_INFO([{event, ar_repack_init},
        {name, name(StoreID)}, {store_id, StoreID},
		{from_packing, ar_serialize:encode_packing(FromPacking, false)},
        {to_packing, ar_serialize:encode_packing(ToPacking, false)}]),

    %% Sanity checks
	%% We curently only support repacking in place to replica_2_9 from non-replica_2_9
	case FromPacking of
		{replica_2_9, _} ->
			?LOG_ERROR([{event, repack_in_place_from_replica_2_9_not_supported}]),
			timer:sleep(5_000),
			erlang:halt();
		_ ->
			ok
	end,
    %% End sanity checks
	
	{replica_2_9, RewardAddr} = ToPacking,

    {RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	PaddedRangeEnd = ar_chunk_storage:get_chunk_bucket_end(RangeEnd),
    Cursor = read_cursor(StoreID, ToPacking, RangeStart),

	{ok, Config} = application:get_env(arweave, config),
	BatchSize = Config#config.repack_batch_size,
	gen_server:cast(self(), repack),
	ar_device_lock:set_device_lock_metric(StoreID, repack, paused),
	State = #state{ 
		store_id = StoreID,
		read_batch_size = BatchSize,
		range_start = RangeStart,
		range_end = PaddedRangeEnd,
		iteration_end = ar_entropy_gen:iteration_end(Cursor, PaddedRangeEnd),
		next_cursor = Cursor, 
		target_packing = ToPacking,
		reward_addr = RewardAddr,
		repack_status = paused
	},
	log_info(starting_repack_in_place, State, [
		{name, name(StoreID)},
		{read_batch_size, BatchSize},
		{write_batch_size, State#state.write_batch_size},
		{from_packing, ar_serialize:encode_packing(FromPacking, false)},
        {to_packing, ar_serialize:encode_packing(ToPacking, false)},
		{padded_range_end, PaddedRangeEnd},
		{next_cursor, Cursor}]),
    {ok, State}.

%%%===================================================================
%%% Gen server callbacks.
%%%===================================================================

handle_call(Request, _From, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(repack, #state{} = State) ->
	#state{ store_id = StoreID } = State,
	store_cursor(State),
	NewStatus = ar_device_lock:acquire_lock(repack, StoreID, State#state.repack_status),
	State2 = State#state{ repack_status = NewStatus },
	State3 = case NewStatus of
		active ->
			repack(State2);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), repack),
			State2;
		_ ->
			State2
	end,
	{noreply, State3};

handle_cast({repack_range, EntropyOffsets}, #state{} = State) ->
	State2 = repack_range(EntropyOffsets, State),
	{noreply, State2};

handle_cast({expire_repack_request, {BucketEndOffset, BatchID}},
		#state{iteration_start = IterationStart} = State) 
		when BatchID == IterationStart ->
	#state{
		chunk_info_map = Map
	} = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been repacked and processed.
			State;
		ChunkInfo ->
			log_debug(repack_request_expired, ChunkInfo, State, []),
			remove_chunk_info(BucketEndOffset, State)
	end,
	{noreply, State2};
handle_cast({expire_repack_request, _Ref}, #state{} = State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast({expire_encipher_request, {BucketEndOffset, BatchID}},
		#state{iteration_start = IterationStart} = State) 
		when BatchID == IterationStart ->
	#state{
		chunk_info_map = Map
	} = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been processed.
			State;
		ChunkInfo ->
			log_debug(encipher_request_expired, ChunkInfo, State, []),
			remove_chunk_info(BucketEndOffset, State)
	end,
	{noreply, State2};
handle_cast({expire_encipher_request, _Ref}, #state{} = State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info({entropy, BucketEndOffset, Entropies}, #state{} = State) ->
	#state{ 
		iteration_start = IterationStart,
		iteration_end = IterationEnd,
		reward_addr = RewardAddr
	} = State,

	%% Generate the next entropy in the batch if needed.
	generate_batch_entropy(BucketEndOffset + ?DATA_CHUNK_SIZE, State),

	EntropyKeys = ar_entropy_gen:generate_entropy_keys(RewardAddr, BucketEndOffset),
	EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset),

	State2 = ar_entropy_gen:map_entropies(
		Entropies, 
		EntropyOffsets,
		IterationStart,
		IterationEnd,
		EntropyKeys, 
		RewardAddr,
		fun entropy_generated/3, [], State),
	
	{noreply, State2};

%% @doc This is only called during repack_in_place. Called when a chunk has been repacked
%% from the old format to the new format and is ready to be stored in the chunk storage.
handle_info({chunk, {packed, {BucketEndOffset, _}, ChunkArgs}}, #state{} = State) ->
	#state{
		chunk_info_map = Map
	} = State,
	
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			{Packing, _, AbsoluteOffset, _, ChunkSize} = ChunkArgs,
			log_warning(chunk_repack_request_not_found, State, [
				{bucket_end_offset, BucketEndOffset},
				{absolute_offset, AbsoluteOffset},
				{chunk_size, ChunkSize},
				{packing, ar_serialize:encode_packing(Packing, false)},
				{chunk_info_map, maps:size(Map)}
			]),
			State;
		ChunkInfo ->
			%% sanity checks
			true = ChunkInfo#chunk_info.state == needs_repack,
			%% end sanity checks

			{_, Chunk, _, _, _} = ChunkArgs,
			ChunkInfo2 = ChunkInfo#chunk_info{
				chunk = Chunk,
				source_packing = unpacked_padded
			},
			update_chunk_state(ChunkInfo2, State)
	end,
	{noreply, State2};

handle_info({chunk, {enciphered, {BucketEndOffset, _}, PackedChunk}}, #state{} = State) ->
	#state{ chunk_info_map = Map } = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			log_warning(chunk_encipher_request_not_found, State, [
				{bucket_end_offset, BucketEndOffset},
				{chunk_info_map, maps:size(Map)}
			]),
			State;
		ChunkInfo ->
			%% sanity checks
			true = ChunkInfo#chunk_info.state == needs_encipher,
			%% end sanity checks

			ChunkInfo2 = ChunkInfo#chunk_info{
				chunk = PackedChunk,
				entropy = <<>>,
				source_packing = ChunkInfo#chunk_info.target_packing
			},
			update_chunk_state(ChunkInfo2, State)
	end,
	{noreply, State2};

handle_info(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

terminate(Reason, #state{} = State) ->
	ReasonStr = io_lib:format("~P", [Reason, 20]),  % Depth limited to 5
        TruncatedStr = if
            length(ReasonStr) > 2000 -> 
                string:slice(ReasonStr, 0, 2000) ++ "... (truncated)";
            true -> 
                ReasonStr
        end,
	log_debug(terminate, State, [{reason, TruncatedStr}]),
	store_cursor(State),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Outer repack loop. Called via `gen_server:cast(self(), repack)`. Each call
%% repacks another batch of chunks. A batch of chunks is N entropy footprints where N
%% is the repack batch size.
repack(#state{ next_cursor = Cursor, range_end = RangeEnd } = State)
		when Cursor > RangeEnd ->
	#state{ chunk_info_map = Map, store_id = StoreID, target_packing = TargetPacking } = State,

	case maps:size(Map) of
		0 ->
			ar_device_lock:release_lock(repack, StoreID),
			ar_device_lock:set_device_lock_metric(StoreID, repack, complete),
			State2 = State#state{ repack_status = complete },
			ar:console("~n~nRepacking of ~s is complete! "
					"We suggest you stop the node, rename "
					"the storage module folder to reflect "
					"the new packing, and start the "
					"node with the new storage module.~n", [StoreID]),
			?LOG_INFO([{event, repacking_complete},
					{store_id, StoreID},
					{target_packing, ar_serialize:encode_packing(TargetPacking, false)}]),
			State2;
		_ ->
			log_debug(repacking_complete_but_waiting, State, [
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)}]),
			ar_util:cast_after(5000, self(), repack),
			State
	end;

repack(#state{} = State) ->
	#state{ next_cursor = Cursor, target_packing = TargetPacking } = State,

	case ar_packing_server:is_buffer_full() of
		true ->
			log_debug(waiting_for_repack_buffer, State, [
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)}]),
			ar_util:cast_after(200, self(), repack),
			State;
		false ->
			repack_batch(Cursor, State)
	end.

repack_batch(Cursor, #state{} = State) ->
	#state{ range_start = RangeStart, range_end = RangeEnd,
		target_packing = TargetPacking, store_id = StoreID,
		read_batch_size = BatchSize } = State,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),

	%% sanity checks
	true = BucketEndOffset == BucketStartOffset + ?DATA_CHUNK_SIZE,
	%% end sanity checks

	IterationStart = BucketStartOffset+1,
	IterationEnd = ar_entropy_gen:iteration_end(BucketEndOffset, RangeEnd),
	EntropyStart = BucketEndOffset,
	EntropyEnd = BucketEndOffset + (BatchSize - 1) * ?DATA_CHUNK_SIZE,
	State2 = State#state{ 
		iteration_start = IterationStart,
		iteration_end = IterationEnd,
		entropy_start = EntropyStart,
		entropy_end = min(EntropyEnd, IterationEnd),
		next_cursor = BucketEndOffset + ?DATA_CHUNK_SIZE
	},
	IsRecorded = ar_sync_record:is_recorded(
		BucketStartOffset+1, TargetPacking, ar_data_sync, StoreID),

	case IsRecorded orelse IterationStart < RangeStart orelse IterationStart > RangeEnd of
		true ->
			%% Skip this BucketEndOffset for one of these reasons:
			%% 1. BucketEndOffset has already been repacked.
			%%    Note: we expect this to happen a lot since we iterate through all
			%%    chunks in the partition, but for each chunk we will repack N
			%%    entropy footprints.
			%% 2. The iteration range of this batch ends before the start of the
			%%    storage module.
			%% 3. The iteration range of this batch starts after the end of the
			%%    storage module.
			gen_server:cast(self(), repack),
			State2;
		_ ->
			%% sanity checks
			true = IterationStart < IterationEnd,
			%% end sanity checks

			EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset),
			log_debug(repack_batch_start, State2, [
				{cursor, Cursor},
				{bucket_end_offset, BucketEndOffset},
				{bucket_start_offset, BucketStartOffset},
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
				{entropy_start, EntropyStart},
				{entropy_end, EntropyEnd},
				{read_batch_size, BatchSize},
				{write_batch_size, State2#state.write_batch_size},
				{entropy_offsets, length(EntropyOffsets)}
			]),
			State3 = init_chunk_info_map(EntropyOffsets, State2),
			generate_batch_entropy(BucketEndOffset, State3),
			gen_server:cast(self(), {repack_range, EntropyOffsets}),
			State3
	end.

generate_batch_entropy(BucketEndOffset,
		#state{ entropy_start = EntropyStart, entropy_end = EntropyEnd }) 
		when BucketEndOffset < EntropyStart orelse BucketEndOffset > EntropyEnd ->
	ok;
generate_batch_entropy(BucketEndOffset, #state{} = State) ->
	#state{ 
		store_id = StoreID,
		entropy_start = EntropyStart,
		entropy_end = EntropyEnd,
		reward_addr = RewardAddr
	} = State,

	ar_entropy_gen:generate_entropies(StoreID, RewardAddr, BucketEndOffset, self()).


init_chunk_info_map([], #state{} = State) ->
	State;
init_chunk_info_map([EntropyOffset | EntropyOffsets], #state{} = State) ->
	#state{ 
		range_start = RangeStart,
		iteration_end = IterationEnd,
		read_batch_size = BatchSize,
		chunk_info_map = Map,
		target_packing = TargetPacking
	} = State,

	{_BatchStart, _BatchEnd, BatchOffsets} = get_batch_range(
		EntropyOffset, RangeStart, IterationEnd, BatchSize),

	Map2 = lists:foldl(
		fun(BucketEndOffset, Acc) ->
			false = maps:is_key(BucketEndOffset, Acc),
			ChunkInfo = #chunk_info{
				offsets = #chunk_offsets{
					bucket_end_offset = BucketEndOffset
				},
				target_packing = TargetPacking
			},
			maps:put(BucketEndOffset, ChunkInfo, Acc)
		end,
	Map, BatchOffsets),

	%% sanity checks
	true = maps:size(Map2) == maps:size(Map) + length(BatchOffsets),
	%% end sanity checks

	init_chunk_info_map(EntropyOffsets, State#state{ chunk_info_map = Map2 }).

repack_range([], #state{} = State) ->
	State;
repack_range([BucketEndOffset | EntropyOffsets],
		#state{ iteration_start = IterationStart } = State) 
			when BucketEndOffset < IterationStart ->
	%% Advance until we hit a chunk covered by the current storage module
	repack_range(EntropyOffsets, State);
repack_range([BucketEndOffset | _EntropyOffsets],
		#state{ iteration_end = IterationEnd } = State) 
			when BucketEndOffset > IterationEnd ->
	State;
repack_range([BucketEndOffset | EntropyOffsets], #state{} = State) ->
	#state{ 
		range_start = RangeStart,
		iteration_end = IterationEnd,
		read_batch_size = BatchSize
	} = State,

	{BatchStart, BatchEnd, BatchOffsets} = get_batch_range(
		BucketEndOffset, RangeStart, IterationEnd, BatchSize),

	OffsetChunkMap = read_chunk_range(BatchStart, BatchEnd, State),
	OffsetMetadataMap = read_chunk_metadata_range(BatchStart, BatchEnd, State),

	State2 = add_range_to_chunk_info_map(OffsetChunkMap, OffsetMetadataMap, State),

	State3 = mark_missing_chunks(BatchOffsets, State2),

	gen_server:cast(self(), {repack_range, EntropyOffsets}),

	State3.

get_batch_range(BucketEndOffset, RangeStart, IterationEnd, BatchSize) ->
	BatchStart = ar_chunk_storage:get_chunk_byte_from_bucket_end(BucketEndOffset),

	SectorSize = ar_replica_2_9:get_sector_size(),
	RangeStart2 = ar_chunk_storage:get_chunk_bucket_start(RangeStart + 1),
	RelativeSectorOffset = (BucketEndOffset - RangeStart2) rem SectorSize,
	SectorEnd = BucketEndOffset + (SectorSize - RelativeSectorOffset),

	FullBatchSize = ?DATA_CHUNK_SIZE * BatchSize,
	BatchEnd = lists:min([BatchStart + FullBatchSize, SectorEnd, IterationEnd]),

	BucketEndOffsets = [BucketEndOffset + (N * ?DATA_CHUNK_SIZE) || 
		N <- lists:seq(0, BatchSize-1),
		BucketEndOffset + (N * ?DATA_CHUNK_SIZE) =< BatchEnd],

	{BatchStart, BatchEnd, BucketEndOffsets}.
	

read_chunk_range(BatchStart, BatchEnd, #state{} = State) ->
	#state{ store_id = StoreID } = State,
	BatchSizeInBytes = BatchEnd - BatchStart,
	case catch ar_chunk_storage:get_range(BatchStart, BatchSizeInBytes, StoreID) of
		[] ->
			#{};
		{'EXIT', _Exc} ->
			log_error(failed_to_read_chunk_range, State, [
				{batch_start, BatchStart},
				{batch_end, BatchEnd},
				{batch_size_bytes, BatchSizeInBytes}
			]),
			#{};
		Range ->
			maps:from_list(Range)
	end.

read_chunk_metadata_range(BatchStart, BatchEnd, #state{} = State) ->
	#state{ store_id = StoreID } = State,
	case ar_data_sync:get_chunk_metadata_range(BatchStart+1, BatchEnd, StoreID) of
		{ok, MetadataMap} ->
			MetadataMap;
		{error, invalid_iterator} ->
			#{};
		{error, Reason} ->
			log_warning(failed_to_read_chunk_metadata, State, [
				{batch_start, BatchStart},
				{batch_end, BatchEnd},
				{reason, Reason}
			]),
			#{}
	end.

add_range_to_chunk_info_map(OffsetChunkMap, OffsetMetadataMap, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	
	maps:fold(
		fun(AbsoluteEndOffset, Metadata, Acc) ->
			#state{
				chunk_info_map = Map
			} = Acc,
			{ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize} = Metadata,
			BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset),
			PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),

			IsRecorded = ar_sync_record:is_recorded(PaddedEndOffset, ar_data_sync, StoreID),
			SourcePacking = case IsRecorded of
				{true, Packing} -> Packing;
				_ -> not_found
			end,

			ChunkInfo = maps:get(BucketEndOffset, Map),

			ChunkInfo2 = ChunkInfo#chunk_info{
				source_packing = SourcePacking,
				offsets = #chunk_offsets{
					absolute_offset = AbsoluteEndOffset,
					bucket_end_offset = BucketEndOffset,
					padded_end_offset = PaddedEndOffset,
					relative_offset = RelativeOffset
				},
				metadata = #chunk_metadata{
					chunk_data_key = ChunkDataKey,
					tx_root = TXRoot,
					data_root = DataRoot,
					tx_path = TXPath,
					chunk_size = ChunkSize
				},
				chunk = maps:get(PaddedEndOffset, OffsetChunkMap, not_found)
			},
			update_chunk_state(ChunkInfo2, Acc)
		end,
		State, OffsetMetadataMap).


%% @doc Mark any chunks that weren't found in either chunk_storage or the chunks_index.
mark_missing_chunks([], #state{} = State) ->
	State;
mark_missing_chunks([BucketEndOffset | BatchOffsets], #state{} = State) ->
	#state{ 
		chunk_info_map = Map
	} = State,
	
	ChunkInfo = maps:get(BucketEndOffset, Map, not_found),

	State2 = case ChunkInfo of
		not_found ->
			State;
		#chunk_info{state = needs_chunk} ->
			%% If we're here and still in the needs_chunk state it means we weren't able
			%% to find the chunk in chunk_storage or the chunks_index.
			ChunkInfo2 = ChunkInfo#chunk_info{
				chunk = not_found,
				metadata = not_found
			},
			update_chunk_state(ChunkInfo2, State);
		_ ->
			State
	end,

	mark_missing_chunks(BatchOffsets, State2).

cache_chunk_info(ChunkInfo, #state{} = State) ->
	#chunk_info{
		offsets = #chunk_offsets{
			bucket_end_offset = BucketEndOffset
		}
	} = ChunkInfo,
	State#state{ chunk_info_map =
		maps:put(BucketEndOffset, ChunkInfo, State#state.chunk_info_map)
	}.

remove_chunk_info(BucketEndOffset, #state{} = State) ->
	State2 = State#state{ chunk_info_map =
		maps:remove(BucketEndOffset, State#state.chunk_info_map)
	},
	maybe_repack_next_batch(State2).

enqueue_chunk_for_writing(ChunkInfo, #state{} = State) ->
	#chunk_info{
		offsets = #chunk_offsets{
			bucket_end_offset = BucketEndOffset
		}
	} = ChunkInfo,
	State2 = State#state{
		write_queue = gb_sets:add_element(
			{BucketEndOffset, ChunkInfo}, State#state.write_queue)
	},

	case gb_sets:size(State2#state.write_queue) >= State2#state.write_batch_size of
		true ->
			process_write_queue(State2);
		false ->
			State2
	end.

chunk_repacked(ChunkInfo, TargetPacking, #state{} = State) ->
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
			log_error(failed_to_update_sync_record_for_repacked_chunk, ChunkInfo, State, [
				{error, io_lib:format("~p", [Error2])}
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
			log_error(failed_to_store_repacked_chunk, ChunkInfo, State, [
				{requested_packing, ar_serialize:encode_packing(Packing, true)},
				{error, io_lib:format("~p", [Error])}
			])
	end.

entropy_generated(Entropy, BucketEndOffset, #state{} = State) ->
	#state{
		chunk_info_map = Map
	} = State,
	
	case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% This should never happen.
			log_error(entropy_generated_chunk_not_found, State, [
				{bucket_end_offset, BucketEndOffset}
			]),
			State;
		ChunkInfo ->
			ChunkInfo2 = ChunkInfo#chunk_info{
				entropy = Entropy
			},

			% log_debug(entropy_generated, ChunkInfo2, State, []),

			update_chunk_state(ChunkInfo2, State)
	end.

write_chunk_info(ChunkInfo, #state{} = State) ->
	#state{ 
		store_id = StoreID,
		reward_addr = RewardAddr,
		target_packing = TargetPacking
	} = State,
	
	case ChunkInfo#chunk_info.state of
		write_entropy ->
			Entropy = ChunkInfo#chunk_info.entropy,
			BucketEndOffset = ChunkInfo#chunk_info.offsets#chunk_offsets.bucket_end_offset,
			ar_entropy_storage:store_entropy(Entropy, BucketEndOffset, StoreID, RewardAddr);
		write_chunk ->
			chunk_repacked(ChunkInfo, TargetPacking, State);
		_ ->
			log_error(unexpected_chunk_state, ChunkInfo, State, [])
	end.

maybe_repack_next_batch(#state{} = State) ->
	#state{ chunk_info_map = Map } = State,
	case maps:size(Map) of
		0 ->
			State2 = process_write_queue(State),
			gen_server:cast(self(), repack),
			State2;
		_ ->
			State
	end.

process_write_queue(#state{} = State) ->
    #state{ write_queue = WriteQueue } = State,
	StartTime = erlang:monotonic_time(),
    gb_sets:fold(
        fun({_BucketEndOffset, ChunkInfo}, _) ->
			write_chunk_info(ChunkInfo, State)
        end,
        ok,
        WriteQueue
    ),
	EndTime = erlang:monotonic_time(),
	log_debug(process_write_queue, State, [
		{time_taken, erlang:convert_time_unit(EndTime - StartTime, native, millisecond)}
	]),
    State#state{ write_queue = gb_sets:new() }.

read_chunk_and_data_path(ChunkInfo, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	#chunk_info{
		metadata = Metadata,
		chunk = MaybeChunk
	} = ChunkInfo,
	#chunk_metadata{
		chunk_data_key = ChunkDataKey
	} = Metadata,
	case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			log_warning(chunk_not_found_in_chunk_data_db, ChunkInfo, State, []),
			ChunkInfo#chunk_info{ data_path = not_found };
		{ok, V} ->
			case binary_to_term(V) of
				{Chunk, DataPath} ->
					ChunkInfo#chunk_info{ 
						data_path = DataPath,
						chunk = Chunk
					};
				DataPath when MaybeChunk /= not_found ->
					ChunkInfo#chunk_info{ 
						data_path = DataPath,
						chunk = MaybeChunk
					};
				_ ->
					log_warning(chunk_not_found, ChunkInfo, State, []),
					ChunkInfo#chunk_info{ data_path = not_found }
			end
	end.

update_chunk_state(ChunkInfo, #state{} = State) ->
	ChunkInfo2 = ar_repack_fsm:crank_state(ChunkInfo),

	case ChunkInfo == ChunkInfo2 of
		true ->
			%% Cache it anyways, just in case.
			cache_chunk_info(ChunkInfo2, State);
		false ->
			process_state_change(ChunkInfo2, State)
	end.

process_state_change(ChunkInfo, #state{} = State) ->
	#state{
		store_id = StoreID,
		iteration_start = IterationStart
	} = State,
	#chunk_info{
		offsets = #chunk_offsets{
			bucket_end_offset = BucketEndOffset
		},
		chunk = Chunk,
		entropy = Entropy,
		source_packing = Packing
	} = ChunkInfo,

	case ChunkInfo#chunk_info.state of
		invalid ->
			ChunkSize = ChunkInfo#chunk_info.metadata#chunk_metadata.chunk_size,
			ar_data_sync:invalidate_bad_data_record(
				BucketEndOffset, ChunkSize, StoreID, repack_found_stale_indices),
			cache_chunk_info(ChunkInfo, State);
		already_repacked ->
			%% Remove the chunk and entropy to free up memory.
			ChunkInfo2 = ChunkInfo#chunk_info{ chunk = <<>> },
			cache_chunk_info(ChunkInfo2, State);
		needs_data_path ->
			ChunkInfo2 = read_chunk_and_data_path(ChunkInfo, State),
			State2 = cache_chunk_info(ChunkInfo2, State),
			update_chunk_state(ChunkInfo2, State2);
		needs_repack ->
			%% Include IterationStart so that we don't accidentally expire a chunk from some
			%% future batch. Unlikely, but not impossible.
			ChunkSize = ChunkInfo#chunk_info.metadata#chunk_metadata.chunk_size,
			TXRoot = ChunkInfo#chunk_info.metadata#chunk_metadata.tx_root,
			AbsoluteOffset = ChunkInfo#chunk_info.offsets#chunk_offsets.absolute_offset,
			ar_packing_server:request_repack({BucketEndOffset, IterationStart}, self(),
				{unpacked_padded, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize}),
			cache_chunk_info(ChunkInfo, State);
		needs_encipher ->
			%% We now have the unpacked_padded chunk and the entropy, proceed
			%% with enciphering and storing the chunk.
			ar_packing_server:request_encipher(
				{BucketEndOffset, IterationStart}, self(), {Chunk, Entropy}),
			cache_chunk_info(ChunkInfo, State);
		write_entropy ->
			State2 = enqueue_chunk_for_writing(ChunkInfo, State),
			remove_chunk_info(BucketEndOffset, State2);
		write_chunk ->
			State2 = enqueue_chunk_for_writing(ChunkInfo, State),
			remove_chunk_info(BucketEndOffset, State2);
		ignore ->
			%% Chunk was already_repacked.
			remove_chunk_info(BucketEndOffset, State);
		error ->
			%% This should never happen.
			log_error(invalid_chunk_info_state, ChunkInfo, State, []),
			remove_chunk_info(BucketEndOffset, State);
		_ ->
			%% No action to take now, but since the chunk state changed, we need to update
			%% the cache.
			cache_chunk_info(ChunkInfo, State)
	end.

read_cursor(StoreID, TargetPacking, RangeStart) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	DefaultCursor = RangeStart + 1,
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

store_cursor(#state{} = State) ->
	store_cursor(State#state.next_cursor, State#state.store_id, State#state.target_packing).
store_cursor(none, _StoreID, _TargetPacking) ->
	ok;
store_cursor(Cursor, StoreID, TargetPacking) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	file:write_file(Filepath, term_to_binary({Cursor, TargetPacking})).

log_error(Event, #chunk_info{} = ChunkInfo, #state{} = State, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, ChunkInfo, State, ExtraLogs)).

log_error(Event, #state{} = State, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, State, ExtraLogs)).

log_warning(Event, #chunk_info{} = ChunkInfo, #state{} = State, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, ChunkInfo, State, ExtraLogs)).

log_warning(Event, #state{} = State, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, State, ExtraLogs)).

log_info(Event, #state{} = State, ExtraLogs) ->
	?LOG_INFO(format_logs(Event, State, ExtraLogs)).

log_debug(Event, #chunk_info{} = ChunkInfo, #state{} = State, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, ChunkInfo, State, ExtraLogs)).
	
log_debug(Event, #state{} = State, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, State, ExtraLogs)).

format_logs(Event, #state{} = State, ExtraLogs) ->
	[
		{event, Event},
		{tags, [repack_in_place]},
		{pid, self()},
		{store_id, State#state.store_id},
		{next_cursor, State#state.next_cursor},
		{iteration_start, State#state.iteration_start},
		{iteration_end, State#state.iteration_end},
		{range_start, State#state.range_start},
		{range_end, State#state.range_end},
		{chunk_info_map, maps:size(State#state.chunk_info_map)},
		{write_queue, gb_sets:size(State#state.write_queue)}
		| ExtraLogs
	].

format_logs(Event, #chunk_info{} = ChunkInfo, #state{} = State, ExtraLogs) ->
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
	format_logs(Event, State, [
		{state, ChunkState},
		{bucket_end_offset, BucketEndOffset},
		{absolute_offset, AbsoluteOffset},
		{padded_end_offset, PaddedEndOffset},
		{chunk_size, ChunkSize},
		{chunk, atom_or_binary(Chunk)},
		{entropy, atom_or_binary(Entropy)} | ExtraLogs
	]).

count_states(#state{} = State) ->
	#state{
		chunk_info_map = Map,
		write_queue = WriteQueue
	} = State,
	MapCount = maps:fold(
		fun(_BucketEndOffset, ChunkInfo, Acc) ->
			maps:update_with(ChunkInfo#chunk_info.state, fun(Count) -> Count + 1 end, 1, Acc)
		end,
		#{},
		Map
	),
	WriteQueueCount = gb_sets:fold(
		fun({_BucketEndOffset, ChunkInfo}, Acc) ->
			maps:update_with(ChunkInfo#chunk_info.state, fun(Count) -> Count + 1 end, 1, Acc)
		end,
		#{},
		WriteQueue
	),
	log_debug(repack_chunk_state_count, State, [
		{map_states, maps:to_list(MapCount)},
		{write_queue_states, maps:to_list(WriteQueueCount)}
	]).	

atom_or_binary(Atom) when is_atom(Atom) -> Atom;
atom_or_binary(Bin) when is_binary(Bin) -> binary:part(Bin, {0, min(10, byte_size(Bin))}).	

%%%===================================================================
%%% Tests.
%%%===================================================================

% name_test() ->
%     Funs = [{ar_storage_module, label_by_id, fun(_StoreID) -> "testlabel" end}],
%     ar_test_node:test_with_mocked_functions(Funs, fun() ->
%          Expected = list_to_atom("ar_repack_" ++ "testlabel"),
%          ?assertEqual(Expected, name(123))
%     end).

% init_chunk_info_map_test() ->
%     DummyState = #state{store_id = 1, next_cursor = 0, target_packing = dummy, chunk_info_map = #{ }},
%     BucketOffsets = [1000, 2000],
%     NewState = init_chunk_info_map(BucketOffsets, DummyState),
%     Map = NewState#state.chunk_info_map,
%     ?assert(maps:is_key(1000, Map)),
%     ?assert(maps:is_key(2000, Map)),
%     ChunkInfo1 = maps:get(1000, Map),
%     ChunkInfo2 = maps:get(2000, Map),
%     ?assertEqual(no_data, ChunkInfo1#chunk_info.status),
%     ?assertEqual(no_data, ChunkInfo2#chunk_info.status).

% cache_chunk_info_test() ->
%     %% Create a dummy chunk_info record.
%     ChunkInfo = #chunk_info{bucket_end_offset = 123, status = no_data},
%     DummyState = #state{store_id = 1, next_cursor = 0, target_packing = dummy, chunk_info_map = #{ }},
%     NewState = cache_chunk_info(ChunkInfo, DummyState),
%     Map = NewState#state.chunk_info_map,
%     ?assertEqual(ChunkInfo, maps:get(123, Map)).

% remove_chunk_info_test() ->
%     %% Start with a state having two keys.
%     ChunkInfo1 = #chunk_info{bucket_end_offset = 456, status = no_data},
%     ChunkInfo2 = #chunk_info{bucket_end_offset = 789, status = no_data},
%     InitialMap = #{456 => ChunkInfo1, 789 => ChunkInfo2},
%     DummyState = #state{store_id = 1, next_cursor = 0, target_packing = dummy, chunk_info_map = InitialMap},
%     NewState = remove_chunk_info(456, DummyState),
%     Map = NewState#state.chunk_info_map,
%     ?assertEqual(false, maps:is_key(456, Map)),
%     ?assert(maps:is_key(789, Map)).

% store_and_read_cursor_test() ->
%     {ok, Config} = application:get_env(arweave, config),
%     TempFile = filename:join(Config#config.data_dir, "test_repack_cursor"),
%     Funs = [{ar_chunk_storage, get_filepath, fun("repack_in_place_cursor2", _StoreID) -> TempFile end}],
%     ar_test_node:test_with_mocked_functions(Funs, fun() ->
%          %% Create a dummy state record with next_cursor.
%          DummyState = #state{next_cursor = 555, store_id = 42, target_packing = test_packing, chunk_info_map = #{ }},
%          ok = store_cursor(DummyState),
%          {ok, Bin} = file:read_file(TempFile),
%          {Cursor, TargetPacking} = binary_to_term(Bin),
%          ?assertEqual(555, Cursor),
%          ?assertEqual(test_packing, TargetPacking),
%          %% Now test read_cursor/3 when file exists.
%          CursorRead = read_cursor(42, test_packing, 1000),
%          ?assertEqual(555, CursorRead),
%          %% Remove the file and test the default behavior.
%          file:delete(TempFile),
%          DefaultCursor = 1000 + 1,
%          CursorRead2 = read_cursor(42, test_packing, 1000),
%          ?assertEqual(DefaultCursor, CursorRead2)
%     end).
