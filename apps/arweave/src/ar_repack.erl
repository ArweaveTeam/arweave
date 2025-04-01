-module(ar_repack).

-behaviour(gen_server).

-export([name/1, register_workers/0, get_read_range/4, chunk_range_read/4]).

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
	write_batch_size = 1024,
	num_entropy_offsets = 128,
	module_start = 0,
	module_end = 0,
	footprint_start = 0,
	footprint_end = 0,
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

-define(STATE_COUNT_INTERVAL, 10_000).

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
    
    RepackInPlaceWorkers = lists:flatmap(
        fun({StorageModule, Packing}) ->
            StoreID = ar_storage_module:id(StorageModule),
            %% Note: the config validation will prevent a StoreID from being used in both
            %% `storage_modules` and `repack_in_place_storage_modules`, so there's
            %% no risk of a `Name` clash with the workers spawned above.
            case ar_entropy_gen:is_entropy_packing(Packing) of
                true ->
                    RepackWorker = ?CHILD_WITH_ARGS(
                        ar_repack, worker, name(StoreID),
                        [name(StoreID), {StoreID, Packing}]),
                    
                    RepackIOWorker = ?CHILD_WITH_ARGS(
                        ar_repack_io, worker, ar_repack_io:name(StoreID),
                        [ar_repack_io:name(StoreID), StoreID]),
                    
                    [RepackWorker, RepackIOWorker];
                false ->
                    []
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

    {ModuleStart, ModuleEnd} = ar_storage_module:get_range(StoreID),
	PaddedModuleEnd = ar_chunk_storage:get_chunk_bucket_end(ModuleEnd),
    Cursor = read_cursor(StoreID, ToPacking, ModuleStart),

	{ok, Config} = application:get_env(arweave, config),
	BatchSize = Config#config.repack_batch_size,
	gen_server:cast(self(), repack),
	gen_server:cast(self(), count_states),
	ar_device_lock:set_device_lock_metric(StoreID, repack, paused),
	State = #state{ 
		store_id = StoreID,
		read_batch_size = BatchSize,
		module_start = ModuleStart,
		module_end = PaddedModuleEnd,
		next_cursor = Cursor, 
		target_packing = ToPacking,
		reward_addr = RewardAddr,
		repack_status = paused
	},
	log_info(starting_repack_in_place, State, [
		{name, name(StoreID)},
		{read_batch_size, BatchSize},
		{write_batch_size, State#state.write_batch_size},
		{num_entropy_offsets, State#state.num_entropy_offsets},
		{from_packing, ar_serialize:encode_packing(FromPacking, false)},
        {to_packing, ar_serialize:encode_packing(ToPacking, false)},
		{raw_module_end, ModuleEnd},
		{next_cursor, Cursor}]),
    {ok, State}.

chunk_range_read(BucketEndOffset, OffsetChunkMap, OffsetMetadataMap, StoreID) ->
	gen_server:cast(name(StoreID),
		{chunk_range_read, BucketEndOffset, OffsetChunkMap, OffsetMetadataMap}).

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

handle_cast({chunk_range_read, BucketEndOffset, OffsetChunkMap, OffsetMetadataMap}, #state{} = State) ->
	#state{
		module_start = ModuleStart,
		footprint_end = FootprintEnd,
		read_batch_size = BatchSize
	} = State,
	{_ReadRangeStart, _ReadRangeEnd, ReadRangeOffsets} = get_read_range(
		BucketEndOffset, ModuleStart, FootprintEnd, BatchSize),
	State2 = add_range_to_chunk_info_map(OffsetChunkMap, OffsetMetadataMap, State),
	State3 = mark_missing_chunks(ReadRangeOffsets, State2),
	{noreply, State3};

handle_cast({expire_repack_request, {BucketEndOffset, FootprintID}},
		#state{footprint_start = FootprintStart} = State) 
		when FootprintID == FootprintStart ->
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

handle_cast({expire_encipher_request, {BucketEndOffset, FootprintID}},
		#state{footprint_start = FootprintStart} = State) 
		when FootprintID == FootprintStart ->
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

handle_cast(count_states, #state{} = State) ->
	count_states(cache,State),
	ar_util:cast_after(?STATE_COUNT_INTERVAL, self(), count_states),
	{noreply, State};

handle_cast(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info({entropy, BucketEndOffset, Entropies}, #state{} = State) ->
	#state{ 
		footprint_start = FootprintStart,
		footprint_end = FootprintEnd,
		reward_addr = RewardAddr
	} = State,

	EntropyKeys = ar_entropy_gen:generate_entropy_keys(RewardAddr, BucketEndOffset),
	EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset),

	State2 = ar_entropy_gen:map_entropies(
		Entropies, 
		EntropyOffsets,
		FootprintStart,
		FootprintEnd,
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
	log_debug(terminate, State, [{reason, ar_util:safe_format(Reason)}]),
	store_cursor(State),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Outer repack loop. Called via `gen_server:cast(self(), repack)`. Each call
%% repacks another footprint of chunks. A repack footprint is N entropy footprints where N
%% is the repack batch size.
repack(#state{ next_cursor = Cursor, module_end = ModuleEnd } = State)
		when Cursor > ModuleEnd ->
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
			repack_footprint(Cursor, State)
	end.

repack_footprint(Cursor, #state{} = State) ->
	#state{ module_start = ModuleStart, module_end = ModuleEnd, num_entropy_offsets = NumEntropyOffsets,
		target_packing = TargetPacking, store_id = StoreID,
		read_batch_size = BatchSize } = State,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),
	PaddedEndOffset = ar_block:get_chunk_padded_offset(Cursor),

	%% sanity checks
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(PaddedEndOffset),
	%% end sanity checks
	
	FootprintStart = BucketStartOffset+1,
	
	IsChunkRecorded = ar_sync_record:is_recorded(PaddedEndOffset, ar_data_sync, StoreID),
	%% Skip this offset if it's already packed to TargetPacking, or if it's not recorded
	%% at all.
	Skip = case IsChunkRecorded of
		false -> true;
		{true, TargetPacking} -> true;
		_ -> false
	end,

	case Skip orelse FootprintStart < ModuleStart orelse FootprintStart > ModuleEnd of
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
			Interval = ar_sync_record:get_next_unsynced_interval(
				BucketEndOffset, infinity, TargetPacking, ar_data_sync, StoreID),
			NextCursor = case Interval of
				not_found ->
					BucketEndOffset + ?DATA_CHUNK_SIZE;
				{_, Start} ->
					Start + ?DATA_CHUNK_SIZE
			end,
			log_debug(skipping_cursor, State, [
				{cursor, Cursor},
				{next_cursor, NextCursor},
				{padded_end_offset, PaddedEndOffset},
				{bucket_end_offset, BucketEndOffset},
				{is_chunk_recorded, IsChunkRecorded},
				{footprint_start, FootprintStart},
				{module_start, ModuleStart},
				{module_end, ModuleEnd}
			]),
			State#state{ next_cursor = NextCursor };
		_ ->
			FootprintOffsets = footprint_offsets(BucketEndOffset, NumEntropyOffsets, ModuleEnd),
			FootprintEnd = footprint_end(FootprintOffsets, ModuleStart, ModuleEnd, BatchSize),

			State2 = State#state{ 
				footprint_start = FootprintStart,
				footprint_end = FootprintEnd,
				next_cursor = BucketEndOffset + ?DATA_CHUNK_SIZE
			},
			State3 = init_chunk_info_map(FootprintOffsets, State2),

			%% We'll generate BatchSize entropy footprints, one for each bucket end offset
			%% starting at BucketEndOffset and ending at EntropyEnd.
			{_, EntropyEnd, _} = get_read_range(
				BucketEndOffset, ModuleStart, FootprintEnd, BatchSize),
			generate_repack_entropy(
				BucketEndOffset, ar_chunk_storage:get_chunk_bucket_end(EntropyEnd), State3),

			ar_repack_io:read_footprint(FootprintOffsets, FootprintStart, FootprintEnd, StoreID),

			log_debug(repack_footprint_start, State3, [
				{cursor, Cursor},
				{bucket_end_offset, BucketEndOffset},
				{bucket_start_offset, BucketStartOffset},
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
				{module_start, ModuleStart},
				{module_end, ModuleEnd},
				{entropy_end, EntropyEnd},
				{read_batch_size, BatchSize},
				{write_batch_size, State2#state.write_batch_size},
				{num_entropy_offsets, NumEntropyOffsets},
				{footprint_start, FootprintStart},
				{footprint_end, FootprintEnd},
				{footprint_offsets, length(FootprintOffsets)},
				{chunk_info_map, maps:size(State3#state.chunk_info_map)}
			]),
			State3
	end.

%% @doc Generates the set of entropy offsets that will be used during one iteration of
%% repack_footprint.
%% 
%% One footprint of entropy offsets is generated and then filtered such that:
%% - no offset is less than BucketEndOffset
%% - no offset is greater than ModuleEnd
%% - at most NumEntropyOffsets offsets are returned
footprint_offsets(BucketEndOffset, NumEntropyOffsets, ModuleEnd) ->
	EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset),

	FilteredOffsets = lists:filter(
		fun(Offset) -> 
			Offset >= BucketEndOffset andalso Offset =< ModuleEnd
		end,
		EntropyOffsets),
	
	lists:sublist(FilteredOffsets, NumEntropyOffsets).

footprint_end(FootprintOffsets, ModuleStart, ModuleEnd, BatchSize) ->
	FirstOffset = lists:min(FootprintOffsets),
	LastOffset = lists:max(FootprintOffsets),
	%% The final read range of the footprint starts at the last entropy offset
	{_, LastOffsetRangeEnd, _} = get_read_range(LastOffset, ModuleStart, ModuleEnd, BatchSize),
	
	%% ar_entropy_gen:footprint_end makes sure all offsets are in the same entropy partition
	FootprintEnd = ar_entropy_gen:footprint_end(FirstOffset, ModuleEnd),
	min(ar_chunk_storage:get_chunk_bucket_end(LastOffsetRangeEnd), FootprintEnd).

generate_repack_entropy(BucketEndOffset, EntropyEnd, #state{}) 
		when BucketEndOffset > EntropyEnd ->
	ok;
generate_repack_entropy(BucketEndOffset, EntropyEnd, #state{} = State) ->
	#state{ 
		store_id = StoreID,
		reward_addr = RewardAddr
	} = State,

	ar_entropy_gen:generate_entropies(StoreID, RewardAddr, BucketEndOffset, self()),
	generate_repack_entropy(BucketEndOffset + ?DATA_CHUNK_SIZE, EntropyEnd, State).


init_chunk_info_map([], #state{} = State) ->
	State;
init_chunk_info_map([EntropyOffset | EntropyOffsets], #state{} = State) ->
	#state{ 
		module_start = ModuleStart,
		footprint_end = FootprintEnd,
		read_batch_size = BatchSize,
		chunk_info_map = Map,
		target_packing = TargetPacking
	} = State,

	{_ReadRangeStart, _ReadRangeEnd, ReadRangeOffsets} = get_read_range(
		EntropyOffset, ModuleStart, FootprintEnd, BatchSize),

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
	Map, ReadRangeOffsets),

	%% sanity checks
	true = maps:size(Map2) == maps:size(Map) + length(ReadRangeOffsets),
	%% end sanity checks

	init_chunk_info_map(EntropyOffsets, State#state{ chunk_info_map = Map2 }).


get_read_range(BucketEndOffset, ModuleStart, FootprintEnd, BatchSize) ->
	ReadRangeStart = ar_chunk_storage:get_chunk_byte_from_bucket_end(BucketEndOffset),

	SectorSize = ar_replica_2_9:get_sector_size(),
	ModuleStart2 = ar_chunk_storage:get_chunk_bucket_start(ModuleStart + 1),
	RelativeSectorOffset = (BucketEndOffset - ModuleStart2) rem SectorSize,
	SectorEnd = BucketEndOffset + (SectorSize - RelativeSectorOffset),

	FullRangeSize = ?DATA_CHUNK_SIZE * BatchSize,
	ReadRangeEnd = lists:min([ReadRangeStart + FullRangeSize, SectorEnd, FootprintEnd]),

	BucketEndOffsets = [BucketEndOffset + (N * ?DATA_CHUNK_SIZE) || 
		N <- lists:seq(0, BatchSize-1),
		BucketEndOffset + (N * ?DATA_CHUNK_SIZE) =< ReadRangeEnd],

	{ReadRangeStart, ReadRangeEnd, BucketEndOffsets}.

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
mark_missing_chunks([BucketEndOffset | ReadRangeOffsets], #state{} = State) ->
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

	mark_missing_chunks(ReadRangeOffsets, State2).

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
	count_states(State),
	State2 = State#state{ chunk_info_map =
		maps:remove(BucketEndOffset, State#state.chunk_info_map)
	},
	maybe_repack_next_footprint(State2).

enqueue_chunk_for_writing(ChunkInfo, #state{} = State) ->
	#state{
		target_packing = TargetPacking,
		reward_addr = RewardAddr,
		store_id = StoreID
	} = State,
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
			count_states(queue, State2),
			ar_repack_io:write_queue(
				State2#state.write_queue, TargetPacking, RewardAddr, StoreID),
			State2#state{ write_queue = gb_sets:new() };
		false ->
			State2
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
			update_chunk_state(ChunkInfo2, State)
	end.

maybe_repack_next_footprint(#state{} = State) ->
	#state{ 
		chunk_info_map = Map,
		write_queue = WriteQueue,
		target_packing = TargetPacking,
		reward_addr = RewardAddr,
		store_id = StoreID
	} = State,
	case maps:size(Map) of
		0 ->
			count_states(queue, State),
			ar_repack_io:write_queue(WriteQueue, TargetPacking, RewardAddr, StoreID),
			State2 = State#state{ write_queue = gb_sets:new() },
			gen_server:cast(self(), repack),
			State2;
		_ ->
			State
	end.

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
		footprint_start = FootprintStart
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
			ChunkInfo2 = ChunkInfo#chunk_info{ chunk = invalid },
			State2 = cache_chunk_info(ChunkInfo2, State),
			update_chunk_state(ChunkInfo2, State2);
		already_repacked ->
			%% Remove the chunk and entropy to free up memory.
			ChunkInfo2 = ChunkInfo#chunk_info{ chunk = <<>> },
			cache_chunk_info(ChunkInfo2, State);
		needs_data_path ->
			ChunkInfo2 = read_chunk_and_data_path(ChunkInfo, State),
			State2 = cache_chunk_info(ChunkInfo2, State),
			update_chunk_state(ChunkInfo2, State2);
		needs_repack ->
			%% Include BatchStart so that we don't accidentally expire a chunk from some
			%% future batch. Unlikely, but not impossible.
			ChunkSize = ChunkInfo#chunk_info.metadata#chunk_metadata.chunk_size,
			TXRoot = ChunkInfo#chunk_info.metadata#chunk_metadata.tx_root,
			AbsoluteOffset = ChunkInfo#chunk_info.offsets#chunk_offsets.absolute_offset,
			ar_packing_server:request_repack({BucketEndOffset, FootprintStart}, self(),
				{unpacked_padded, Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize}),
			cache_chunk_info(ChunkInfo, State);
		needs_encipher ->
			%% We now have the unpacked_padded chunk and the entropy, proceed
			%% with enciphering and storing the chunk.
			ar_packing_server:request_encipher(
				{BucketEndOffset, FootprintStart}, self(), {Chunk, Entropy}),
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

read_cursor(StoreID, TargetPacking, ModuleStart) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	DefaultCursor = ModuleStart + 1,
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
		{footprint_start, State#state.footprint_start},
		{footprint_end, State#state.footprint_end},
		{module_start, State#state.module_start},
		{module_end, State#state.module_end},
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

count_states(cache, #state{} = State) ->
	#state{
		store_id = StoreID,
		chunk_info_map = Map
	} = State,
	MapCount = maps:fold(
		fun(_BucketEndOffset, ChunkInfo, Acc) ->
			maps:update_with(ChunkInfo#chunk_info.state, fun(Count) -> Count + 1 end, 1, Acc)
		end,
		#{},
		Map
	),
	log_debug(count_cache_states, State, [
		{cache_size, maps:size(Map)},
		{states, maps:to_list(MapCount)}
	]),
	maps:fold(
		fun(ChunkState, Count, Acc) ->
			prometheus_gauge:set(repack_chunk_states, [StoreID, cache, ChunkState], Count),
			Acc
		end,
		ok,
		MapCount
	);
count_states(queue, #state{} = State) ->
	#state{
		store_id = StoreID,
		write_queue = WriteQueue
	} = State,
	WriteQueueCount = gb_sets:fold(
		fun({_BucketEndOffset, ChunkInfo}, Acc) ->
			maps:update_with(ChunkInfo#chunk_info.state, fun(Count) -> Count + 1 end, 1, Acc)
		end,
		#{},
		WriteQueue
	),
	log_debug(count_write_queue_states, State, [
		{queue_size, gb_sets:size(WriteQueue)},
		{states, maps:to_list(WriteQueueCount)}
	]),
	maps:fold(
		fun(ChunkState, Count, Acc) ->
			prometheus_gauge:set(repack_chunk_states, [StoreID, queue, ChunkState], Count),
			Acc
		end,
		ok,
		WriteQueueCount
	).

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
