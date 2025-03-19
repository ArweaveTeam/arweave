-module(ar_repack).

-behaviour(gen_server).

-export([name/1, register_workers/0]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("../include/ar.hrl").
-include("../include/ar_sup.hrl").
-include("../include/ar_consensus.hrl").
-include("../include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
	This module handles the repack-in-place logic.
""".

-record(chunk_info, {
	status :: 
		needs_repack | unpacked_padded | already_repacked | no_data | entropy_only | repacked,
	absolute_offset,
	bucket_end_offset,
	padded_end_offset,
	relative_offset,
	chunk_data_key,
	tx_root,
	data_root,
	tx_path,
	chunk_size,
	data_path,
	source_packing,
	chunk,
	entropy = none
}).

-record(state, {
	store_id = undefined,
	batch_size = ?DEFAULT_REPACK_BATCH_SIZE,
	range_start = 0,
	range_end = 0,
	iteration_start = 0,
	iteration_end = 0,
	next_cursor = 0, 
	target_packing = undefined,
	reward_addr = undefined,
	repack_status = undefined,
	chunk_info_map = #{}
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
	log_debug(reading_cursor, StoreID, [{next_cursor, Cursor}, {target_packing, ToPacking}]),
	{ok, Config} = application:get_env(arweave, config),
	BatchSize = Config#config.repack_batch_size,
	gen_server:cast(self(), repack),
	?LOG_INFO([{event, starting_repack_in_place},
			{tags, [repack_in_place]},
			{range_start, RangeStart},
			{range_end, RangeEnd},
			{batch_size, BatchSize},
			{padded_range_end, PaddedRangeEnd},
			{next_cursor, Cursor},
			{store_id, StoreID},
			{target_packing, ar_serialize:encode_packing(ToPacking, false)}]),
	ar_device_lock:set_device_lock_metric(StoreID, repack, paused),
	State = #state{ 
		store_id = StoreID,
		batch_size = BatchSize,
		range_start = RangeStart,
		range_end = PaddedRangeEnd,
		iteration_end = ar_entropy_gen:iteration_end(Cursor, PaddedRangeEnd),
		next_cursor = Cursor, 
		target_packing = ToPacking,
		reward_addr = RewardAddr,
		repack_status = paused
	},
    {ok, State}.

%%%===================================================================
%%% Gen server callbacks.
%%%===================================================================

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(repack, State) ->
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

handle_cast({expire_repack_request, {BucketEndOffset, BatchID}},
		#state{iteration_start = IterationStart} = State) 
		when BatchID == IterationStart ->
	#state{
		chunk_info_map = Map,
		store_id = StoreID
	} = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been repacked and processed.
			State;
		ChunkInfo ->
			log_debug(repack_request_expired, ChunkInfo, StoreID),
			remove_chunk_info(BucketEndOffset, State)
	end,
	{noreply, State2};
handle_cast({expire_repack_request, _Ref}, State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast({expire_encipher_request, {BucketEndOffset, BatchID}},
		#state{iteration_start = IterationStart} = State) 
		when BatchID == IterationStart ->
	#state{
		chunk_info_map = Map,
		store_id = StoreID
	} = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been processed.
			State;
		ChunkInfo ->
			log_debug(encipher_request_expired, ChunkInfo, StoreID),
			remove_chunk_info(BucketEndOffset, State)
	end,
	{noreply, State2};
handle_cast({expire_encipher_request, _Ref}, State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast(Request, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info({entropy, BucketEndOffset, Entropies}, State) ->
	#state{ 
		iteration_start = IterationStart,
		iteration_end = IterationEnd,
		reward_addr = RewardAddr
	} = State,

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
handle_info({chunk, {packed, {BucketEndOffset, _}, ChunkArgs}}, State) ->
	#state{
		chunk_info_map = Map,
		store_id = StoreID,
		iteration_start = IterationStart
	} = State,
	
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			{Packing, _, AbsoluteOffset, _, ChunkSize} = ChunkArgs,
			log_warning(chunk_repack_request_not_found, StoreID, [
				{bucket_end_offset, BucketEndOffset},
				{absolute_offset, AbsoluteOffset},
				{chunk_size, ChunkSize},
				{packing, ar_serialize:encode_packing(Packing, false)},
				{chunk_info_map, maps:size(Map)}
			]),
			State;
		ChunkInfo ->
			{_, Chunk, _, _, _} = ChunkArgs,
			case ChunkInfo#chunk_info.entropy of
				none ->
					%% Waiting on entropy
					ChunkInfo2 = ChunkInfo#chunk_info{
						status = unpacked_padded,
						chunk = Chunk },
					cache_chunk_info(ChunkInfo2, State);
				Entropy ->
					%% We now have the unpacked_padded chunk and the entropy, proceed
					%% with enciphering and storing the chunk.
					ar_packing_server:request_encipher(
						{BucketEndOffset, IterationStart}, self(), {Chunk, Entropy}),
					ChunkInfo2 = ChunkInfo#chunk_info{
						chunk = Chunk
					},
					cache_chunk_info(ChunkInfo2, State)
			end
	end,
	{noreply, State2};

handle_info({chunk, {enciphered, {BucketEndOffset, _}, PackedChunk}}, State) ->
	#state{
		chunk_info_map = Map, store_id = StoreID, target_packing = TargetPacking } = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			log_warning(chunk_encipher_request_not_found, StoreID, [
				{bucket_end_offset, BucketEndOffset},
				{chunk_info_map, maps:size(Map)}
			]),
			State;
		ChunkInfo ->
			ChunkInfo2 = ChunkInfo#chunk_info{
				status = repacked,
				chunk = PackedChunk
			},
			chunk_repacked(ChunkInfo2, TargetPacking, StoreID),
			remove_chunk_info(BucketEndOffset, State)
	end,
	{noreply, State2};

handle_info(Request, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

terminate(Reason, State) ->
	?LOG_DEBUG([{event, terminate}, {module, ?MODULE}, {reason, Reason}]),
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
			log_debug(repacking_complete_but_waiting, State#state.store_id, [
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
				{chunk_info_map_size, maps:size(Map)}]),
			ar_util:cast_after(5000, self(), repack),
			State
	end;

repack(State) ->
	#state{ next_cursor = Cursor, range_start = RangeStart, range_end = RangeEnd,
		target_packing = TargetPacking, store_id = StoreID } = State,

	case ar_packing_server:is_buffer_full() of
		true ->
			log_debug(waiting_for_repack_buffer, State#state.store_id, [
				{pid, self()},
				{store_id, StoreID},
				{cursor, Cursor},
				{range_start, RangeStart},
				{range_end, RangeEnd},
				{required_packing, ar_serialize:encode_packing(TargetPacking, false)}]),
			ar_util:cast_after(200, self(), repack),
			State;
		false ->
			repack_batch(Cursor, State)
	end.

repack_batch(Cursor, State) ->
	#state{ range_start = RangeStart, range_end = RangeEnd,
		target_packing = TargetPacking, store_id = StoreID,
		batch_size = BatchSize } = State,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),

	%% sanity checks
	true = BucketEndOffset == BucketStartOffset + ?DATA_CHUNK_SIZE,
	%% end sanity checks

	IterationStart = BucketStartOffset+1,
	IterationEnd = ar_entropy_gen:iteration_end(BucketEndOffset, RangeEnd),
	State2 = State#state{ 
		iteration_start = IterationStart,
		iteration_end = IterationEnd,
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
			log_debug(repack_batch_start, StoreID, [
				{cursor, Cursor},
				{next_cursor, State2#state.next_cursor},
				{bucket_end_offset, BucketEndOffset},
				{bucket_start_offset, BucketStartOffset},
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
				{range_start, RangeStart},
				{range_end, RangeEnd},
				{iteration_start, State2#state.iteration_start},
				{iteration_end, State2#state.iteration_end},
				{batch_size, BatchSize},
				{entropy_offsets, length(EntropyOffsets)}
			]),
			%% Generate entropies and repack the corresponding chunks. The number
			%% of 256 MiB entropies handled per batch is determined by BatchSize.
			generate_batch_entropies(BucketEndOffset, State2, BatchSize),
			repack_batch_chunks(EntropyOffsets, State2)
	end.

generate_batch_entropies(_BucketEndOffset, _State, Count)
		when Count =< 0 ->
	ok;
generate_batch_entropies(BucketEndOffset, State, Count) ->
	#state{ store_id = StoreID, reward_addr = RewardAddr } = State,
	ar_entropy_gen:generate_entropies(StoreID, RewardAddr, BucketEndOffset, self()),
	generate_batch_entropies(
		BucketEndOffset + ?DATA_CHUNK_SIZE, State, Count-1).

repack_batch_chunks([], State) ->
	State;
repack_batch_chunks([BucketEndOffset | EntropyOffsets],
		#state{ iteration_start = IterationStart } = State) 
			when BucketEndOffset < IterationStart ->
	%% Advance until we hit a chunk covered by the current storage module
	repack_batch_chunks(EntropyOffsets, State);
repack_batch_chunks([BucketEndOffset | _EntropyOffsets],
		#state{ iteration_end = IterationEnd } = State) 
			when BucketEndOffset > IterationEnd ->
	%% Done. Now we wait for the last batch of chunks to be repacked, enciphered, and stored.
	%% When the last chunk is procssed, it will kick off the next batch.
	State;
repack_batch_chunks([BucketEndOffset | EntropyOffsets], State) ->
	#state{ 
		store_id = StoreID,
		range_start = RangeStart,
		iteration_end = IterationEnd,
		batch_size = BatchSize
	} = State,

	{BatchStart, BatchEnd, BatchOffsets} = get_batch_range(
		BucketEndOffset, RangeStart, IterationEnd, BatchSize),

	State2 = init_chunk_info_map(BatchOffsets, State),

	%% sanity checks
	true = maps:size(State2#state.chunk_info_map) == 
		maps:size(State#state.chunk_info_map) + length(BatchOffsets),
	%% end sanity checks

	OffsetChunkMap = read_chunk_range(BatchStart, BatchEnd, StoreID),
	OffsetMetadataMap = read_chunk_metadata_range(BatchStart, BatchEnd, StoreID),

	State3 = add_range_to_chunk_info_map(OffsetChunkMap, OffsetMetadataMap, State2),

	%% sanity checks
	true = maps:size(State3#state.chunk_info_map) == maps:size(State2#state.chunk_info_map),
	%% end sanity checks

	State4 = repack_chunk_range(BatchOffsets, State3),

	repack_batch_chunks(EntropyOffsets, State4).

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
	
init_chunk_info_map(BucketEndOffsets, State) ->
	#state{ chunk_info_map = Map } = State,
	Map2 = lists:foldl(
		fun(BucketEndOffset, Acc) ->
			false = maps:is_key(BucketEndOffset, Acc),
			ChunkInfo = #chunk_info{
				bucket_end_offset = BucketEndOffset,
				status = no_data
			},
			maps:put(BucketEndOffset, ChunkInfo, Acc)
		end,
	Map, BucketEndOffsets),
	State#state{ chunk_info_map = Map2 }.

read_chunk_range(BatchStart, BatchEnd, StoreID) ->
	BatchSizeInBytes = BatchEnd - BatchStart,
	case catch ar_chunk_storage:get_range(BatchStart, BatchSizeInBytes, StoreID) of
		[] ->
			log_debug(chunk_range_has_no_chunks, StoreID, [
				{batch_start, BatchStart},
				{batch_end, BatchEnd},
				{batch_size_bytes, BatchSizeInBytes}
			]),
			#{};
		{'EXIT', _Exc} ->
			log_error(failed_to_read_chunk_range, StoreID, [
				{batch_start, BatchStart},
				{batch_end, BatchEnd},
				{batch_size_bytes, BatchSizeInBytes}
			]),
			#{};
		Range ->
			maps:from_list(Range)
	end.

read_chunk_metadata_range(BatchStart, BatchEnd, StoreID) ->
	case ar_data_sync:get_chunk_metadata_range(BatchStart+1, BatchEnd, StoreID) of
		{ok, MetadataMap} ->
			MetadataMap;
		{error, invalid_iterator} ->
			#{};
		{error, Reason} ->
			log_warning(failed_to_read_chunk_metadata, StoreID, [
				{batch_start, BatchStart},
				{batch_end, BatchEnd},
				{reason, Reason}
			]),
			#{}
	end.

add_range_to_chunk_info_map(OffsetChunkMap, OffsetMetadataMap, State) ->
	#state{
		target_packing = TargetPacking, store_id = StoreID,
		chunk_info_map = Map
	} = State,
	
	Map2 = maps:fold(
		fun(AbsoluteEndOffset, Metadata, Acc) ->
			{ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize} = Metadata,
			BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset),
			PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),

			IsTooSmall = (
				ChunkSize /= ?DATA_CHUNK_SIZE andalso
				AbsoluteEndOffset =< ?STRICT_DATA_SPLIT_THRESHOLD
			),

			IsRecorded = ar_sync_record:is_recorded(PaddedEndOffset, ar_data_sync, StoreID),

			{Status, Packing} = case {IsTooSmall, IsRecorded} of
				{true, _} -> {entropy_only, none}; %% Small chunks are not written to disk
				{false, {true, unpacked_padded}} -> {unpacked_padded, unpacked_padded};
				{false, {true, TargetPacking}} -> {already_repacked, TargetPacking};
				{false, {true, SourcePacking}} -> {needs_repack, SourcePacking};
				_ -> {entropy_only, none} %% No chunk found, we'll only write entropy
			end,

			ChunkInfo = maps:get(BucketEndOffset, Acc),

			ChunkInfo2 = ChunkInfo#chunk_info{
				status = Status,
				source_packing = Packing,
				absolute_offset = AbsoluteEndOffset,
				bucket_end_offset = BucketEndOffset,
				padded_end_offset = PaddedEndOffset,
				relative_offset = RelativeOffset,
				chunk_data_key = ChunkDataKey,
				tx_root = TXRoot,
				data_root = DataRoot,
				tx_path = TXPath,
				chunk_size = ChunkSize
			},

			case Status of
				entropy_only ->
					log_debug(chunk_not_recorded, ChunkInfo2, StoreID, [
						{is_recorded, IsRecorded},
						{is_too_small, IsTooSmall},
						{status, Status}
					]);
				_ ->
					ok
			end,

			ReadChunk = Status == unpacked_padded orelse Status == needs_repack,
			Chunk =  maps:get(PaddedEndOffset, OffsetChunkMap, not_found),

			ChunkInfo3 = case {ReadChunk, Chunk} of
				{false, _} -> ChunkInfo2;
				{true, not_found} ->
					%% Chunk doesn't exist on disk, try chunk data db.
					read_chunk_and_data_path(ChunkInfo2, no_chunk, StoreID);
				{true, Chunk} ->
					case ar_chunk_storage:is_storage_supported(AbsoluteEndOffset,
							ChunkSize, TargetPacking) of
						false ->
							%% We are going to move this chunk to RocksDB after repacking so
							%% we read its DataPath here to pass it later on to store_chunk.
							read_chunk_and_data_path(ChunkInfo2, Chunk, StoreID);
						true ->
							%% We are going to repack the chunk and keep it in the chunk
							%% storage - no need to make an extra disk access to read
							%% the data path.
							ChunkInfo2#chunk_info{
								chunk = Chunk,
								data_path = none
							}
					end
			end,
			maps:put(BucketEndOffset, ChunkInfo3, Acc)
		end,
		Map, OffsetMetadataMap),

	State#state{ chunk_info_map = Map2 }.


repack_chunk_range([], State) ->
	State;
repack_chunk_range([BucketEndOffset | BatchOffsets], State) ->
	#state{ chunk_info_map = Map, iteration_start = IterationStart } = State,
	
	ChunkInfo = maps:get(BucketEndOffset, Map),

	repack_chunk(ChunkInfo, IterationStart),
	repack_chunk_range(BatchOffsets, State).

repack_chunk(#chunk_info{ status = needs_repack } = ChunkInfo, BatchID) ->
	#chunk_info{
		chunk = Chunk,
		absolute_offset = AbsoluteOffset,
		tx_root = TXRoot,
		chunk_size = ChunkSize,
		bucket_end_offset = BucketEndOffset,
		source_packing = Packing
	} = ChunkInfo,
	%% Include BatchID so that we don't accidentally expire a chunk from some future
	%% batch. Unlikely, but not impossible.
	ar_packing_server:request_repack({BucketEndOffset, BatchID}, self(),
			{unpacked_padded, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize}),
	ChunkInfo;
repack_chunk(ChunkInfo, _BatchID) ->
	ChunkInfo.

cache_chunk_info(ChunkInfo, State) ->
	State#state{ chunk_info_map =
		maps:put(ChunkInfo#chunk_info.bucket_end_offset, ChunkInfo, State#state.chunk_info_map)
	}.

remove_chunk_info(BucketEndOffset, State) ->

	State2 = State#state{ chunk_info_map =
		maps:remove(BucketEndOffset, State#state.chunk_info_map)
	},
	maybe_repack_next_batch(State2).

chunk_repacked(ChunkInfo, TargetPacking, StoreID) ->
	#chunk_info{
		chunk = Chunk,
		absolute_offset = AbsoluteOffset,
		padded_end_offset = PaddedEndOffset,
		relative_offset = RelativeOffset,
		tx_root = TXRoot,
		chunk_size = ChunkSize,
		data_path = DataPath,
		data_root = DataRoot,
		tx_path = TXPath,
		chunk_data_key = ChunkDataKey
	} = ChunkInfo,
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
			ar_data_sync:update_chunk(StoreID, AbsoluteOffset, Chunk, TargetPacking);
		{Error2, _} ->
			log_error(failed_to_store_repacked_chunk, ChunkInfo, StoreID, [
				{error, io_lib:format("~p", [Error2])}
			])
	end.

entropy_generated(Entropy, BucketEndOffset, State) ->
	#state{
		chunk_info_map = Map, store_id = StoreID, reward_addr = RewardAddr,
		iteration_start = IterationStart
	} = State,
	case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% This should never happen.
			log_error(entropy_generated_chunk_not_found, State#state.store_id, [
				{bucket_end_offset, BucketEndOffset},
				{chunk_info_map, maps:size(Map)}
			]),
			State;
		ChunkInfo ->
			#chunk_info{
				status = Status,
				chunk = Chunk,
				absolute_offset = AbsoluteEndOffset,
				chunk_size = ChunkSize
			} = ChunkInfo,

			case Status of
				needs_repack ->
					%% Still waiting for the chunk to be repacked, cache the entropy for now
					ChunkInfo2 = ChunkInfo#chunk_info{
						entropy = Entropy
					},
					cache_chunk_info(ChunkInfo2, State);
				unpacked_padded ->
					%% We now have the unpacked_padded chunk and the entropy, proceed
					%% with enciphering and storing the chunk.
					ar_packing_server:request_encipher(
						{BucketEndOffset, IterationStart}, self(), {Chunk, Entropy}),
					ChunkInfo2 = ChunkInfo#chunk_info{
						entropy = Entropy
					},
					cache_chunk_info(ChunkInfo2, State);
				already_repacked ->
					%% Repacked chunk already exists on disk so don't write anything
					%% (neither entropy nor chunk)
					remove_chunk_info(BucketEndOffset, State);
				no_data ->
					%% We don't have a record of this chunk anywhere, so we'll record and
					%% index the entropy
					ar_entropy_storage:store_entropy(
						Entropy, BucketEndOffset, StoreID, RewardAddr),
					remove_chunk_info(BucketEndOffset, State);
				entropy_only ->
					%% This offset exists in some of the chunk indices, but we don't have
					%% any chunk data. This can happen if there was some corruption at some
					%% point in that past. We'll clean out the bad indices, and then record
					%% the entropy.
					log_debug(repack_found_stale_indices, ChunkInfo, StoreID, [
						{iteration_start, IterationStart}
					]),
					ar_data_sync:invalidate_bad_data_record(
						AbsoluteEndOffset, ChunkSize, StoreID, repack_found_stale_indices),
					ar_entropy_storage:store_entropy(
						Entropy, BucketEndOffset, StoreID, RewardAddr),
					remove_chunk_info(BucketEndOffset, State);
				Unexpected ->
					log_error(unexpected_chunk_status, ChunkInfo, StoreID,
							[{status, Unexpected}]),
					remove_chunk_info(BucketEndOffset, State)
			end
	end.

maybe_repack_next_batch(State) ->
	#state{ chunk_info_map = Map } = State,
	case maps:size(Map) of
		0 ->
			gen_server:cast(self(), repack);
		_ ->
			ok
	end,
	State.

read_chunk_and_data_path(ChunkInfo, MaybeChunk, StoreID) ->
	#chunk_info{ chunk_data_key = ChunkDataKey } = ChunkInfo,
	case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			log_warning(chunk_not_found_in_chunk_data_db, ChunkInfo, StoreID, []),
			ChunkInfo#chunk_info{ status = entropy_only };
		{ok, V} ->
			case binary_to_term(V) of
				{Chunk, DataPath} ->
					ChunkInfo#chunk_info{ 
						data_path = DataPath,
						chunk = Chunk
					};
				DataPath when MaybeChunk /= no_chunk ->
					ChunkInfo#chunk_info{ 
						data_path = DataPath,
						chunk = MaybeChunk };
				_ ->
					log_warning(chunk_not_found, ChunkInfo, StoreID, []),
					ChunkInfo#chunk_info{ status = entropy_only }
			end
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

store_cursor(State) ->
	store_cursor(State#state.next_cursor, State#state.store_id, State#state.target_packing).
store_cursor(none, _StoreID, _TargetPacking) ->
	ok;
store_cursor(Cursor, StoreID, TargetPacking) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	file:write_file(Filepath, term_to_binary({Cursor, TargetPacking})).

log_error(Event, ChunkInfo, StoreID, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, ChunkInfo, StoreID, ExtraLogs)).

log_error(Event, StoreID, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, StoreID, ExtraLogs)).

log_warning(Event, ChunkInfo, StoreID, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, ChunkInfo, StoreID, ExtraLogs)).

log_warning(Event, StoreID, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, StoreID, ExtraLogs)).

log_debug(Event, ChunkInfo, StoreID, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, ChunkInfo, StoreID, ExtraLogs)).
	
log_debug(Event, StoreID, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, StoreID, ExtraLogs)).

format_logs(Event, StoreID, ExtraLogs) ->
	[
		{event, Event},
		{tags, [repack_in_place]},
		{store_id, StoreID} | ExtraLogs
	].

format_logs(Event, ChunkInfo, StoreID, ExtraLogs) ->
	#chunk_info{
		status = Status,
		absolute_offset = AbsoluteOffset,
		bucket_end_offset = BucketEndOffset,
		padded_end_offset = PaddedEndOffset,
		chunk_size = ChunkSize
	} = ChunkInfo,
	format_logs(Event, StoreID, [
		{status, Status},
		{bucket_end_offset, BucketEndOffset},
		{absolute_offset, AbsoluteOffset},
		{padded_end_offset, PaddedEndOffset},
		{chunk_size, ChunkSize} | ExtraLogs
	]).
	

%%%===================================================================
%%% Tests.
%%%===================================================================

name_test() ->
    Funs = [{ar_storage_module, label_by_id, fun(_StoreID) -> "testlabel" end}],
    ar_test_node:test_with_mocked_functions(Funs, fun() ->
         Expected = list_to_atom("ar_repack_" ++ "testlabel"),
         ?assertEqual(Expected, name(123))
    end).

init_chunk_info_map_test() ->
    DummyState = #state{store_id = 1, next_cursor = 0, target_packing = dummy, chunk_info_map = #{ }},
    BucketOffsets = [1000, 2000],
    NewState = init_chunk_info_map(BucketOffsets, DummyState),
    Map = NewState#state.chunk_info_map,
    ?assert(maps:is_key(1000, Map)),
    ?assert(maps:is_key(2000, Map)),
    ChunkInfo1 = maps:get(1000, Map),
    ChunkInfo2 = maps:get(2000, Map),
    ?assertEqual(no_data, ChunkInfo1#chunk_info.status),
    ?assertEqual(no_data, ChunkInfo2#chunk_info.status).

cache_chunk_info_test() ->
    %% Create a dummy chunk_info record.
    ChunkInfo = #chunk_info{bucket_end_offset = 123, status = no_data},
    DummyState = #state{store_id = 1, next_cursor = 0, target_packing = dummy, chunk_info_map = #{ }},
    NewState = cache_chunk_info(ChunkInfo, DummyState),
    Map = NewState#state.chunk_info_map,
    ?assertEqual(ChunkInfo, maps:get(123, Map)).

remove_chunk_info_test() ->
    %% Start with a state having two keys.
    ChunkInfo1 = #chunk_info{bucket_end_offset = 456, status = no_data},
    ChunkInfo2 = #chunk_info{bucket_end_offset = 789, status = no_data},
    InitialMap = #{456 => ChunkInfo1, 789 => ChunkInfo2},
    DummyState = #state{store_id = 1, next_cursor = 0, target_packing = dummy, chunk_info_map = InitialMap},
    NewState = remove_chunk_info(456, DummyState),
    Map = NewState#state.chunk_info_map,
    ?assertEqual(false, maps:is_key(456, Map)),
    ?assert(maps:is_key(789, Map)).

store_and_read_cursor_test() ->
    {ok, Config} = application:get_env(arweave, config),
    TempFile = filename:join(Config#config.data_dir, "test_repack_cursor"),
    Funs = [{ar_chunk_storage, get_filepath, fun("repack_in_place_cursor2", _StoreID) -> TempFile end}],
    ar_test_node:test_with_mocked_functions(Funs, fun() ->
         %% Create a dummy state record with next_cursor.
         DummyState = #state{next_cursor = 555, store_id = 42, target_packing = test_packing, chunk_info_map = #{ }},
         ok = store_cursor(DummyState),
         {ok, Bin} = file:read_file(TempFile),
         {Cursor, TargetPacking} = binary_to_term(Bin),
         ?assertEqual(555, Cursor),
         ?assertEqual(test_packing, TargetPacking),
         %% Now test read_cursor/3 when file exists.
         CursorRead = read_cursor(42, test_packing, 1000),
         ?assertEqual(555, CursorRead),
         %% Remove the file and test the default behavior.
         file:delete(TempFile),
         DefaultCursor = 1000 + 1,
         CursorRead2 = read_cursor(42, test_packing, 1000),
         ?assertEqual(DefaultCursor, CursorRead2)
    end).
