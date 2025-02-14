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
		needs_repack | unpacked_padded | already_repacked | no_chunk | repacked,
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
	cursor = 0, 
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
		{from_packing, ar_serialize:encode_packing(FromPacking, true)},
        {to_packing, ar_serialize:encode_packing(ToPacking, true)}]),

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
	log_debug(reading_cursor, StoreID, [{cursor, Cursor}, {target_packing, ToPacking}]),
	{ok, Config} = application:get_env(arweave, config),
	BatchSize = Config#config.repack_batch_size,
	gen_server:cast(self(), repack),
	?LOG_INFO([{event, starting_repack_in_place},
			{tags, [repack_in_place]},
			{range_start, RangeStart},
			{range_end, RangeEnd},
			{batch_size, BatchSize},
			{padded_range_end, PaddedRangeEnd},
			{cursor, Cursor},
			{store_id, StoreID},
			{target_packing, ar_serialize:encode_packing(ToPacking, true)}]),
	ar_device_lock:set_device_lock_metric(StoreID, repack, paused),
	State = #state{ 
		store_id = StoreID,
		batch_size = BatchSize,
		range_start = RangeStart,
		range_end = PaddedRangeEnd,
		iteration_end = ar_entropy_gen:iteration_end(Cursor, PaddedRangeEnd),
		cursor = Cursor, 
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

handle_cast({expire_repack_request, BucketEndOffset}, State) ->
	#state{ chunk_info_map = Map, store_id = StoreID, reward_addr = RewardAddr } = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been repacked and processed.
			State;
		ChunkInfo ->
			HasEntropy = ChunkInfo#chunk_info.entropy /= none,
			log_debug(repack_request_expired, ChunkInfo, StoreID, [{entropy, HasEntropy}]),
			case ChunkInfo#chunk_info.entropy of
				none ->
					%% Still waiting for entropy to be generated.
					State;
				Entropy ->
					ar_entropy_storage:record_entropy(
						Entropy, BucketEndOffset, StoreID, RewardAddr),
					remove_chunk_info(BucketEndOffset, State)
			end
	end,
	{noreply, State2};

handle_cast(Request, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info({entropy, BucketEndOffset, Entropies}, State) ->
	#state{ 
		iteration_start = IterationStart, iteration_end = IterationEnd,
		reward_addr = RewardAddr, store_id = StoreID } = State,

	EntropyKeys = ar_entropy_gen:generate_entropy_keys(RewardAddr, BucketEndOffset),
	EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset),
	log_debug(repack_batch_entropy_generated, StoreID, [
		{bucket_end_offset, BucketEndOffset},
		{entropies, length(Entropies)},
		{entropy_offsets, EntropyOffsets}
	]),

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
handle_info({chunk, {packed, BucketEndOffset, ChunkArgs}}, State) ->
	#state{
		chunk_info_map = Map, store_id = StoreID, target_packing = TargetPacking } = State,
	
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			{Packing, _, AbsoluteOffset, _, ChunkSize} = ChunkArgs,
			log_warning(chunk_repack_request_not_found, StoreID, [
				{bucket_end_offset, BucketEndOffset},
				{absolute_offset, AbsoluteOffset},
				{chunk_size, ChunkSize},
				{packing, ar_serialize:encode_packing(Packing, true)}
			]),
			State;
		ChunkInfo ->
			log_debug(chunk_repack_request_found, ChunkInfo, StoreID, []),
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
					PackedChunk =
						ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
					ChunkInfo2 = ChunkInfo#chunk_info{
						status = repacked,
						chunk = PackedChunk },
					chunk_repacked(ChunkInfo2, TargetPacking, StoreID),
					remove_chunk_info(BucketEndOffset, State)
			end
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

repack(#state{ cursor = Cursor, range_end = RangeEnd } = State)
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
					{target_packing, ar_serialize:encode_packing(TargetPacking, true)}]),
			State2;
		_ ->
			?LOG_DEBUG([{event, repacking_complete_but_waiting},
					{store_id, StoreID},
					{target_packing, ar_serialize:encode_packing(TargetPacking, true)},
					{chunk_info_map_size, maps:size(Map)}]),
			ar_util:cast_after(5000, self(), repack),
			State
	end;


repack(State) ->
	#state{ cursor = Cursor, range_start = RangeStart, range_end = RangeEnd,
		target_packing = TargetPacking, store_id = StoreID, batch_size = BatchSize } = State,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),

	%% sanity checks
	true = BucketEndOffset == BucketStartOffset + ?DATA_CHUNK_SIZE,
	%% end sanity checks

	case ar_packing_server:is_buffer_full() of
		true ->
			?LOG_DEBUG([{event, repack_in_place_buffer_full},
					{tags, [repack_in_place]},
					{pid, self()},
					{store_id, StoreID},
					{s, Cursor},
					{range_start, RangeStart},
					{range_end, RangeEnd},
					{required_packing, ar_serialize:encode_packing(TargetPacking, true)}]),
			ar_util:cast_after(200, self(), repack),
			State;
		false ->
			State2 = State#state{ 
				iteration_start = BucketStartOffset+1,
				iteration_end = ar_entropy_gen:iteration_end(BucketEndOffset, RangeEnd),
				cursor = BucketEndOffset + ?DATA_CHUNK_SIZE },
			IsRecorded = ar_sync_record:is_recorded(
				BucketStartOffset+1, TargetPacking, ar_data_sync, StoreID),
			case IsRecorded of
				true ->
					log_debug(bucket_end_offset_already_recorded, StoreID, [
						{cursor, Cursor},
						{bucket_end_offset, BucketEndOffset},
						{bucket_start_offset, BucketStartOffset},
						{target_packing, ar_serialize:encode_packing(TargetPacking, true)}
					]),
					%% BucketEndOffset has already been repacked, advance and try again.
					gen_server:cast(self(), repack),
					State2;
				_ ->
					EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset),
					log_debug(repack_start, StoreID, [
						{cursor, Cursor},
						{next_cursor, State2#state.cursor},
						{bucket_end_offset, BucketEndOffset},
						{bucket_start_offset, BucketStartOffset},
						{target_packing, ar_serialize:encode_packing(TargetPacking, true)},
						{range_end, RangeEnd},
						{iteration_end, State2#state.iteration_end},
						{range_start, RangeStart},
						{entropy_offsets, EntropyOffsets}
					]),
					%% Generate BatchSize batches of 256 MiB of entropy and repack the
					%% corresponding chunks.
					generate_entropies(BucketEndOffset, State2, BatchSize),
					repack_chunks(EntropyOffsets, State2)
			end
	end.

get_batch_interval(BucketEndOffset, RangeStart, IterationEnd) ->
	{ok, Config} = application:get_env(arweave, config),
	BatchStart = ar_chunk_storage:get_chunk_bucket_start(BucketEndOffset),

	SectorSize = ar_replica_2_9:get_sector_size(),
	RangeStart2 = ar_chunk_storage:get_chunk_bucket_start(RangeStart + 1),
	RelativeSectorOffset = (BucketEndOffset - RangeStart2) rem SectorSize,
	SectorEnd = BucketEndOffset + (SectorSize - RelativeSectorOffset),

	FullBatchSize = ?DATA_CHUNK_SIZE * Config#config.repack_batch_size,
	BatchEnd = lists:min([BatchStart + FullBatchSize, SectorEnd, IterationEnd]),

	BatchSize = BatchEnd - BatchStart,

	BucketEndOffsets = [BucketEndOffset + (N * ?DATA_CHUNK_SIZE) || 
		N <- lists:seq(0, BatchSize-1),
		BucketEndOffset + (N * ?DATA_CHUNK_SIZE) =< BatchEnd],

	{BatchStart, BatchEnd, BucketEndOffsets}.

remove_recorded_offsets(BucketEndOffsets, Packing, StoreID) ->
    lists:filter(
        fun(BucketEndOffset) ->
            BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(BucketEndOffset),
            not ar_sync_record:is_recorded(
				BucketStartOffset + 1, Packing, ar_data_sync, StoreID)
        end,
        BucketEndOffsets
    ).	

generate_entropies(_BucketEndOffset, _State, Count)
		when Count =< 0 ->
	ok;
generate_entropies(BucketEndOffset, State, Count) ->
	#state{ store_id = StoreID, reward_addr = RewardAddr } = State,
	ar_entropy_gen:generate_entropies(StoreID, RewardAddr, BucketEndOffset, self()),
	generate_entropies(
		BucketEndOffset + ?DATA_CHUNK_SIZE, State, Count-1).

repack_chunks([], State) ->
	log_debug(repack_chunks_batch_done, State#state.store_id, [
		{range_start, State#state.range_start},
		{range_end, State#state.range_end},
		{iteration_end, State#state.iteration_end},
		{cursor, State#state.cursor}
	]),
	State;
repack_chunks([BucketEndOffset | EntropyOffsets],
		#state{ iteration_start = IterationStart } = State) 
			when BucketEndOffset < IterationStart ->
	log_debug(repack_chunks_skipping_offset, State#state.store_id, [
		{bucket_end_offset, BucketEndOffset},
		{range_start, State#state.range_start},
		{range_end, State#state.range_end},
		{iteration_end, State#state.iteration_end}
	]),
	%% Advance until we hit a chunk covered by the current storage module
	repack_chunks(EntropyOffsets, State);
repack_chunks([BucketEndOffset | _EntropyOffsets],
		#state{ iteration_end = IterationEnd } = State) 
			when BucketEndOffset > IterationEnd ->
	log_debug(repack_chunks_batch_done, State#state.store_id, [
		{bucket_end_offset, BucketEndOffset},
		{range_start, State#state.range_start},
		{range_end, State#state.range_end},
		{iteration_end, State#state.iteration_end},
		{cursor, State#state.cursor}
	]),
	%% Done. Now we wait for the last batch of chunks to be repacked, enciphered, and stored.
	%% When the last chunk is procssed, it will kick off the next batch.
	State;
repack_chunks([BucketEndOffset | EntropyOffsets], State) ->
	#state{ range_start = RangeStart, iteration_end = IterationEnd } = State,

	{BatchStart, BatchEnd, BatchOffsets} = get_batch_interval(
		BucketEndOffset, RangeStart, IterationEnd),
	log_debug(repack_chunks_offsets, State#state.store_id, [
		{bucket_end_offset, BucketEndOffset},
		{batch_start, BatchStart},
		{batch_end, BatchEnd},
		{offsets, BatchOffsets}
	]),
	State2 = read_chunk_range(BucketEndOffset, BatchStart, BatchEnd, BatchOffsets, State),
	State3 = repack_chunk_range(BatchOffsets, State2),
	repack_chunks(EntropyOffsets, State3).

read_chunk_range(BucketEndOffset, BatchStart, BatchEnd, BatchOffsets, State) ->
	#state{
		target_packing = TargetPacking, store_id = StoreID,
		chunk_info_map = Map, iteration_end = IterationEnd
	} = State,
	
	BatchSize = BatchEnd - BatchStart,

	Map2 = init_chunk_info_map(BatchOffsets, Map),

	log_debug(read_chunk_range, State#state.store_id, [
		{bucket_end_offset, BucketEndOffset},
		{iteration_end, IterationEnd},
		{batch_start, BatchStart}, {batch_end, BatchEnd}, {batch_size, BatchSize}
	]),

	OffsetChunks =
		case catch ar_chunk_storage:get_range(BatchStart, BatchSize, StoreID) of
			[] ->
				log_debug(no_chunks_to_repack1, StoreID, [
					{batch_start, BatchStart},
					{batch_end, BatchEnd},
					{batch_size, BatchSize}
				]),
				[];
			{'EXIT', _Exc} ->
				log_error(failed_to_read_chunk_range, StoreID, [
					{batch_start, BatchStart},
					{batch_end, BatchEnd},
					{batch_size, BatchSize}
				]),
				[];
			Range ->
				Range
		end,

	OffsetMetadataMap = 
		case ar_data_sync:get_chunk_metadata_range(BatchStart+1, BatchEnd, StoreID) of
			{ok, MetadataMap} ->
				MetadataMap;
			{error, invalid_iterator} ->
				log_debug(no_chunks_to_repack2, StoreID, [
					{batch_start, BatchStart},
					{batch_end, BatchEnd},
					{batch_size, BatchSize}
				]),
				#{};
			{error, Reason} ->
				log_warning(failed_to_read_chunk_metadata, StoreID, [
					{batch_start, BatchStart},
					{batch_end, BatchEnd},
					{batch_size, BatchSize},
					{reason, Reason}
				]),
				#{}
		end,

	Map3 = maps:fold(
		fun(AbsoluteEndOffset, Metadata, Acc) ->
			{ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize} = Metadata,
			BucketEndOffset2 = ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset),
			PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),

			IsTooSmall = (
				ChunkSize /= ?DATA_CHUNK_SIZE andalso
				AbsoluteEndOffset =< ?STRICT_DATA_SPLIT_THRESHOLD
			),

			IsRecorded = ar_sync_record:is_recorded(PaddedEndOffset, ar_data_sync, StoreID),

			{Status, Packing} = case {IsTooSmall, IsRecorded} of
				{true, _} -> {no_chunk, none}; %% Small chunks are not written to disk
				{false, {true, unpacked_padded}} -> {unpacked_padded, unpacked_padded};
				{false, {true, TargetPacking}} -> {already_repacked, TargetPacking};
				{false, {true, SourcePacking}} -> {needs_repack, SourcePacking};
				_ -> {no_chunk, none} %% No chunk found, we'll only write entropy
			end,

			ChunkInfo = maps:get(BucketEndOffset2, Acc),
			
			ChunkInfo2 = ChunkInfo#chunk_info{
				status = Status,
				source_packing = Packing,
				absolute_offset = AbsoluteEndOffset,
				bucket_end_offset = BucketEndOffset2,
				padded_end_offset = PaddedEndOffset,
				relative_offset = RelativeOffset,
				chunk_data_key = ChunkDataKey,
				tx_root = TXRoot,
				data_root = DataRoot,
				tx_path = TXPath,
				chunk_size = ChunkSize
			},

			log_debug(read_chunk_range_offset_metadata, ChunkInfo2, State#state.store_id, []),

			maps:put(BucketEndOffset2, ChunkInfo2, Acc)

		end,
		Map2, OffsetMetadataMap),

	Maps4 = lists:foldl(
		fun({AbsoluteEndOffset, Chunk}, Acc) ->
			PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),
			BucketEndOffset3 = ar_chunk_storage:get_chunk_bucket_end(PaddedEndOffset),
			ChunkInfo = maps:get(BucketEndOffset3, Acc),
			ChunkInfo2 = ChunkInfo#chunk_info{
				chunk = Chunk
			},
			log_debug(read_chunk_range_offset_chunk, ChunkInfo2, State#state.store_id, []),
			maps:put(BucketEndOffset3, ChunkInfo2, Acc)
		end,
		Map3, OffsetChunks),

	State#state{ chunk_info_map = Maps4 }.


repack_chunk_range([], State) ->
	State;
repack_chunk_range([Offset | Offsets], State) ->
	#state{ chunk_info_map = Map, store_id = StoreID, target_packing = TargetPacking } = State,
	ChunkInfo = maps:get(Offset, Map),
	#chunk_info{
		status = Status,
		absolute_offset = AbsoluteOffset,
		chunk_size = ChunkSize
	} = ChunkInfo,

	ReadChunk = Status == unpacked_padded orelse Status == needs_repack,

	ChunkInfo2 = case ReadChunk of
		false ->
			%% Chunk either doesn't exist or has already been repacked.
			ChunkInfo;
		true ->
			case ar_chunk_storage:get(AbsoluteOffset - 1, StoreID) of
				not_found ->
					%% Chunk doesn't exist on disk, try chunk data db.
					read_chunk_and_data_path(ChunkInfo, no_chunk, StoreID);
				{_, Chunk} ->
					case ar_chunk_storage:is_storage_supported(AbsoluteOffset,
							ChunkSize, TargetPacking) of
						false ->
							%% We are going to move this chunk to RocksDB after repacking so
							%% we read its DataPath here to pass it later on to store_chunk.
							read_chunk_and_data_path(ChunkInfo, Chunk, StoreID);
						true ->
							%% We are going to repack the chunk and keep it in the chunk
							%% storage - no need to make an extra disk access to read
							%% the data path.
							ChunkInfo#chunk_info{
								chunk = Chunk,
								data_path = none
							}
					end
			end
	end,
	log_debug(repack_chunk, ChunkInfo2, StoreID, [{offset, Offset}]),
	repack_chunk(ChunkInfo2),
	repack_chunk_range(Offsets, State#state{ chunk_info_map =
		maps:put(Offset, ChunkInfo2, State#state.chunk_info_map)
	}).

repack_chunk(#chunk_info{ status = needs_repack } = ChunkInfo) ->
	#chunk_info{
		chunk = Chunk,
		absolute_offset = AbsoluteOffset,
		tx_root = TXRoot,
		chunk_size = ChunkSize,
		bucket_end_offset = BucketEndOffset,
		source_packing = Packing
	} = ChunkInfo,
	ar_util:cast_after(300000, self(), {expire_repack_request, BucketEndOffset}),
	ar_packing_server:request_repack(BucketEndOffset, self(),
			{unpacked_padded, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize}),
	ChunkInfo;
repack_chunk(ChunkInfo) ->
	ChunkInfo.

init_chunk_info_map(BucketEndOffsets, Map) ->
	lists:foldl(
		fun(Offset, Acc) ->
			false = maps:is_key(Offset, Acc),
			ChunkInfo = #chunk_info{
				bucket_end_offset = Offset,
				status = no_chunk
			},
			?LOG_DEBUG([{event, init_chunk_info_map}, {bucket_end_offset, Offset}]),
			maps:put(Offset, ChunkInfo, Acc)
		end,
	Map, BucketEndOffsets).

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
		bucket_end_offset = BucketEnd,
		relative_offset = RelativeOffset,
		tx_root = TXRoot,
		chunk_size = ChunkSize,
		data_path = DataPath,
		data_root = DataRoot,
		tx_path = TXPath,
		chunk_data_key = ChunkDataKey
	} = ChunkInfo,
	log_debug(chunk_repacked, ChunkInfo, StoreID, []),
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	IsStorageSupported =
		ar_chunk_storage:is_storage_supported(PaddedEndOffset, ChunkSize, TargetPacking),

	RemoveFromSyncRecordResult = ar_sync_record:delete(PaddedEndOffset,
			StartOffset, ar_data_sync, StoreID),
	RemoveFromSyncRecordResult2 =
		case RemoveFromSyncRecordResult of
			ok ->
				ar_sync_record:delete(PaddedEndOffset, StartOffset, ar_chunk_storage, StoreID);
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
			case ar_chunk_storage:put(PaddedEndOffset, Chunk, TargetPacking, StoreID) of
				{ok, NewPacking} ->
					BucketStart = BucketEnd - ?DATA_CHUNK_SIZE,
					ar_sync_record:add_async(repacked_chunk,
						BucketEnd, BucketStart,
						ar_chunk_storage_replica_2_9_1_entropy, StoreID),
					ar_sync_record:add_async(repacked_chunk,
							PaddedEndOffset, StartOffset,
							NewPacking, ar_data_sync, StoreID);
				Error3 ->
					log_error(failed_to_store_repacked_chunk, ChunkInfo, StoreID, [
						{requested_packing, ar_serialize:encode_packing(TargetPacking, true)},
						{error, io_lib:format("~p", [Error3])}
					])
			end;
		{Error4, _} ->
			log_error(failed_to_store_repacked_chunk, ChunkInfo, StoreID, [
				{requested_packing, ar_serialize:encode_packing(TargetPacking, true)},
				{error, io_lib:format("~p", [Error4])}
			])
	end.

entropy_generated(Entropy, BucketEndOffset, State) ->
	#state{
		chunk_info_map = Map, store_id = StoreID, target_packing = TargetPacking,
		reward_addr = RewardAddr
	} = State,
	case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% This should never happen.
			log_error(entropy_generated_no_chunk, State#state.store_id, [
				{bucket_end_offset, BucketEndOffset}
			]),
			State;
		ChunkInfo ->
			log_debug(entropy_generated, ChunkInfo, State#state.store_id, [
				{chunk, binary:part(ChunkInfo#chunk_info.chunk, 0, 10)},
				{entropy, binary:part(Entropy, 0, 10)}
			]),

			case ChunkInfo#chunk_info.status of
				needs_repack ->
					%% Still waiting for the chunk to be repacked, cache the entropy for now
					ChunkInfo2 = ChunkInfo#chunk_info{
						entropy = Entropy
					},
					cache_chunk_info(ChunkInfo2, State);
				unpacked_padded ->
					%% We now have the unpacked_padded chunk and the entropy, proceed
					%% with enciphering and storing the chunk.
					PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(
						ChunkInfo#chunk_info.chunk, Entropy),
					ChunkInfo2 = ChunkInfo#chunk_info{
						status = repacked,
						chunk = PackedChunk,
						entropy = Entropy
					},
					chunk_repacked(ChunkInfo2, TargetPacking, StoreID),
					remove_chunk_info(BucketEndOffset, State);
				already_repacked ->
					%% Repacked chunk already exists on disk so don't write anything
					%% (neither entropy nor chunk)
					remove_chunk_info(BucketEndOffset, State);
				no_chunk ->
					%% We don't have a chunk to write, so just record the entropy
					log_debug(entropy_generated_no_chunk, ChunkInfo, StoreID, []),
					ar_entropy_storage:record_entropy(
						Entropy, BucketEndOffset, StoreID, RewardAddr),
					remove_chunk_info(BucketEndOffset, State);
				Unexpected ->
					log_error(unexpected_chunk_status, ChunkInfo, StoreID,
							[{status, Unexpected}]),
					remove_chunk_info(BucketEndOffset, State)
			end
	end.

maybe_repack_next_batch(State) ->
	#state{ chunk_info_map = Map, store_id = StoreID, cursor = Cursor } = State,
	case maps:size(Map) of
		0 ->
			log_debug(repack_batch_done, StoreID, [{cursor, Cursor}]),
			gen_server:cast(self(), repack);
		_ ->
			ok
	end,
	State.

read_chunk_and_data_path(ChunkInfo, MaybeChunk, StoreID) ->
	#chunk_info{ 
		chunk_data_key = ChunkDataKey } = ChunkInfo,
	case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			log_warning(chunk_not_found_in_chunk_data_db, ChunkInfo, StoreID, []),
			ChunkInfo#chunk_info{ status = no_chunk };
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
					ChunkInfo#chunk_info{ status = no_chunk }
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
	store_cursor(State#state.cursor, State#state.store_id, State#state.target_packing).
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

repack_test_() ->
	[
		fun test_repack_in_place/0
	].

test_repack_in_place() ->
	[B0] = ar_weave:init([]),
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),

	StorageModule = {?PARTITION_SIZE, 0, {replica_2_9, RewardAddr}},
	ar_test_node:start(#{
		b0 => B0,
		addr => RewardAddr,
		storage_modules => [ StorageModule ]
	}),

	StoreID = ar_storage_module:id(StorageModule),
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),

	State = #state{ store_id = StoreID, range_start = RangeStart, range_end = RangeEnd },
	repack(State).
