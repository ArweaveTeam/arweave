-module(ar_repack).

-behaviour(gen_server).

-export([name/1, register_workers/0, get_read_range/3, chunk_range_read/4]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_sup.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include("ar_repack.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
	This module handles the repack-in-place logic.
""".

-define(REPACK_WRITE_BATCH_SIZE, 1024).

-record(state, {
	store_id = undefined,
	read_batch_size = ?DEFAULT_REPACK_BATCH_SIZE,
	write_batch_size = ?REPACK_WRITE_BATCH_SIZE,
	num_entropy_offsets,
	module_start = 0,
	module_end = 0,
	footprint_start = 0,
	%% The highest chunk offset that can be read for this repack footprint.
	footprint_end = 0,
	%% The highest bucket end offset to generate entropy for. Generating entropy for this
	%% bucket may yield entropy offsets higher than this because entropy is generated in
	%% 256 MiB batches.
	entropy_end = 0,
	next_cursor = 0, 
	configured_packing = undefined,
	target_packing = undefined,
	repack_status = undefined,
	repack_chunk_map = #{},
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
	list_to_atom("ar_repack_" ++ ar_storage_module:label(StoreID)).

register_workers() ->
    {ok, Config} = arweave_config:get_env(),
    
    RepackInPlaceWorkers = lists:flatmap(
        fun({StorageModule, Packing}) ->
            StoreID = ar_storage_module:id(StorageModule),
            %% Note: the config validation will prevent a StoreID from being used in both
            %% `storage_modules` and `repack_in_place_storage_modules`, so there's
            %% no risk of a `Name` clash with the workers spawned above.
			RepackWorker = ?CHILD_WITH_ARGS(
				ar_repack, worker, name(StoreID),
				[name(StoreID), {StoreID, Packing}]),

			RepackIOWorker = ?CHILD_WITH_ARGS(
				ar_repack_io, worker, ar_repack_io:name(StoreID),
				[ar_repack_io:name(StoreID), StoreID]),

			[RepackWorker, RepackIOWorker]
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
	
	%% ModuleStart to PaddedModuleEnd is the *chunk* range that will be repacked. Chunk
	%% offsets will later be converted to bucket offsets and entropy offsets - and the 
	%% bucket and entropy ranges may differ from this chunk range.
	Module = ar_storage_module:get_by_id(StoreID),
    {ModuleStart, ModuleEnd} = ar_storage_module:module_range(Module),
	PaddedModuleEnd = ar_block:get_chunk_padded_offset(ModuleEnd),
    Cursor = read_cursor(StoreID, ToPacking, ModuleStart),

	{ok, Config} = arweave_config:get_env(),
	BatchSize = Config#config.repack_batch_size,
	CacheSize = Config#config.repack_cache_size_mb,
	NumEntropyOffsets = calculate_num_entropy_offsets(CacheSize, BatchSize),
	gen_server:cast(self(), repack),
	gen_server:cast(self(), count_states),
	ar_device_lock:set_device_lock_metric(StoreID, repack, paused),
	State = #state{ 
		store_id = StoreID,
		read_batch_size = BatchSize,
		num_entropy_offsets = NumEntropyOffsets,
		module_start = ModuleStart,
		module_end = PaddedModuleEnd,
		next_cursor = Cursor, 
		configured_packing = FromPacking,
		target_packing = ToPacking,
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

%% @doc Gets the start and end offset of the range of chunks to read starting from
%% BucketEndOffset. Also includes the BucketEndOffsets covered by that range.
get_read_range(BucketEndOffset, #state{} = State) ->
	#state{
		module_end = ModuleEnd,
		footprint_end = FootprintEnd,
		read_batch_size = BatchSize
	} = State,
	get_read_range(BucketEndOffset, min(ModuleEnd, FootprintEnd), BatchSize).

-spec get_read_range(
		non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
	{non_neg_integer(), non_neg_integer(), [non_neg_integer()]}.
get_read_range(BucketEndOffset, RangeEnd, BatchSize) ->
	ReadRangeStart = ar_chunk_storage:get_chunk_byte_from_bucket_end(BucketEndOffset),

	Partition = ar_node:get_partition_number(BucketEndOffset),
	{EntropyPartitionStart, EntropyPartitionEnd} =
		ar_replica_2_9:get_entropy_partition_range(Partition),
	SectorSize = ar_block:get_replica_2_9_entropy_sector_size(),
	EntropyPartitionStartBucket = ar_chunk_storage:get_chunk_bucket_start(EntropyPartitionStart),
	Sector = (BucketEndOffset - EntropyPartitionStartBucket) div SectorSize,
	SectorBucketEnd = EntropyPartitionStartBucket + (Sector + 1) * SectorSize,
	SectorChunkEnd =
		ar_chunk_storage:get_chunk_byte_from_bucket_end(SectorBucketEnd) + ?DATA_CHUNK_SIZE,
	
	FullRangeSize = ?DATA_CHUNK_SIZE * BatchSize,
	ReadRangeEnd = lists:min([
		ReadRangeStart + FullRangeSize,
		EntropyPartitionEnd,
		SectorChunkEnd,
		RangeEnd]),

	BucketEndOffsets = [BucketEndOffset + (N * ?DATA_CHUNK_SIZE) || 
		N <- lists:seq(0, BatchSize-1),
		BucketEndOffset + (N * ?DATA_CHUNK_SIZE) =< ReadRangeEnd],

	{ReadRangeStart, ReadRangeEnd, BucketEndOffsets}.

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
	{_, _, ReadRangeOffsets} = get_read_range(BucketEndOffset, State),
	State2 = add_range_to_repack_chunk_map(OffsetChunkMap, OffsetMetadataMap, State),
	State3 = mark_missing_chunks(ReadRangeOffsets, State2),
	{noreply, State3};

handle_cast({expire_repack_request, {BucketEndOffset, FootprintID}},
		#state{footprint_start = FootprintStart} = State) 
		when FootprintID == FootprintStart ->
	#state{
		repack_chunk_map = Map
	} = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been repacked and processed.
			State;
		RepackChunk ->
			log_debug(repack_request_expired, RepackChunk, State, []),
			remove_repack_chunk(BucketEndOffset, State)
	end,
	{noreply, State2};
handle_cast({expire_repack_request, _Ref}, #state{} = State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast({expire_encipher_request, {BucketEndOffset, FootprintID}},
		#state{footprint_start = FootprintStart} = State) 
		when FootprintID == FootprintStart ->
	{noreply, expire_exor_request(BucketEndOffset, State)};
handle_cast({expire_encipher_request, _Ref}, #state{} = State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast({expire_decipher_request, {BucketEndOffset, FootprintID}},
		#state{footprint_start = FootprintStart} = State) 
		when FootprintID == FootprintStart ->
	{noreply, expire_exor_request(BucketEndOffset, State)};
handle_cast({expire_decipher_request, _Ref}, #state{} = State) ->
	%% Request is from an old batch, ignore.
	{noreply, State};

handle_cast(count_states, #state{} = State) ->
	count_states(cache, State),
	ar_util:cast_after(?STATE_COUNT_INTERVAL, self(), count_states),
	{noreply, State};

handle_cast(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

handle_info({entropy, BucketEndOffset, RewardAddr, Entropies}, #state{} = State) ->
	#state{ 
		footprint_start = FootprintStart,
		footprint_end = FootprintEnd
	} = State,

	generate_repack_entropy(
		BucketEndOffset + ?DATA_CHUNK_SIZE,
		{replica_2_9, RewardAddr},
		State),

	EntropyKeys = ar_entropy_gen:generate_entropy_keys(RewardAddr, BucketEndOffset),
	EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset, FootprintEnd),

	State2 = ar_entropy_gen:map_entropies(
		Entropies, 
		EntropyOffsets,
		FootprintStart,
		EntropyKeys, 
		RewardAddr,
		fun entropy_generated/4, [], State),
	{noreply, State2};

handle_info({chunk, {packed, {BucketEndOffset, _}, ChunkArgs}}, #state{} = State) ->
	#state{
		repack_chunk_map = Map
	} = State,

	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			{Packing, _, AbsoluteOffset, _, ChunkSize} = ChunkArgs,
			log_warning(chunk_repack_request_not_found, State, [
				{bucket_end_offset, BucketEndOffset},
				{absolute_offset, AbsoluteOffset},
				{chunk_size, ChunkSize},
				{packing, ar_serialize:encode_packing(Packing, false)},
				{repack_chunk_map, maps:size(Map)}
			]),
			State;
		RepackChunk ->
			{Packing, Chunk, _, _, _} = ChunkArgs,
			%% sanity checks
			true = RepackChunk#repack_chunk.state == needs_repack,
			%% end sanity checks

			RepackChunk2 = RepackChunk#repack_chunk{
				chunk = Chunk,
				source_packing = Packing
			},
			update_chunk_state(RepackChunk2, State)
	end,
	{noreply, State2};

handle_info({chunk, {deciphered, {BucketEndOffset, _}, UnpackedChunk}}, #state{} = State) ->
	#state{
		repack_chunk_map = Map
	} = State,

	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			log_warning(chunk_decipher_request_not_found, State, [
				{bucket_end_offset, BucketEndOffset},
				{repack_chunk_map, maps:size(Map)}
			]),
			State;
		RepackChunk ->
			%% sanity checks
			true = RepackChunk#repack_chunk.state == needs_decipher,
			true = byte_size(UnpackedChunk) == ?DATA_CHUNK_SIZE,
			%% end sanity checks

			RepackChunk2 = RepackChunk#repack_chunk{
				chunk = UnpackedChunk,
				source_entropy = <<>>,
				source_packing = unpacked_padded
			},
			update_chunk_state(RepackChunk2, State)
	end,
	{noreply, State2};

handle_info({chunk, {enciphered, {BucketEndOffset, _}, PackedChunk}}, #state{} = State) ->
	#state{ repack_chunk_map = Map } = State,
	State2 = case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			log_warning(chunk_encipher_request_not_found, State, [
				{bucket_end_offset, BucketEndOffset},
				{repack_chunk_map, maps:size(Map)}
			]),
			State;
		RepackChunk ->
			%% sanity checks
			true = RepackChunk#repack_chunk.state == needs_encipher,
			%% end sanity checks

			RepackChunk2 = RepackChunk#repack_chunk{
				chunk = PackedChunk,
				target_entropy = <<>>,
				source_packing = RepackChunk#repack_chunk.target_packing
			},
			update_chunk_state(RepackChunk2, State)
	end,
	{noreply, State2};

handle_info({entropy_generated, _Ref, _Entropy}, State) ->
	?LOG_WARNING([{event, entropy_generation_timed_out}]),
	{noreply, State};

handle_info(Request, #state{} = State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {request, Request}]),
	{noreply, State}.

terminate(Reason, #state{} = State) ->
	log_debug(terminate, State, [{reason, ar_util:safe_format(Reason)}]),
	store_cursor(State),
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

calculate_num_entropy_offsets(CacheSize, BatchSize) ->
	min(ar_block:get_sub_chunks_per_replica_2_9_entropy(), (CacheSize * 4) div BatchSize).

%% @doc Outer repack loop. Called via `gen_server:cast(self(), repack)`. Each call
%% repacks another footprint of chunks. A repack footprint is N entropy footprints where N
%% is the repack batch size.
repack(#state{ next_cursor = Cursor, module_end = ModuleEnd } = State)
		when Cursor > ModuleEnd ->
	#state{ repack_chunk_map = Map, store_id = StoreID, target_packing = TargetPacking } = State,

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
	#state{ module_end = ModuleEnd,
		num_entropy_offsets = NumEntropyOffsets,
		configured_packing = SourcePacking,
		target_packing = TargetPacking,
		store_id = StoreID,
		read_batch_size = BatchSize } = State,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),
	FootprintOffsets = footprint_offsets(BucketEndOffset, NumEntropyOffsets, ModuleEnd),
	FootprintStart = BucketStartOffset+1,
	FootprintEnd = footprint_end(FootprintOffsets, ModuleEnd, BatchSize),
	case should_repack(Cursor, FootprintStart, FootprintEnd, State) of
		{false, Logs} ->
			%% Skip this Cursor for one of these reasons:
			%% 1. Cursor has already been repacked.
			%%    Note: we expect this to happen a lot since we iterate through all
			%%    chunks in the partition, but for each chunk we will repack N
			%%    entropy footprints.
			%% 2. The iteration range of this batch starts after the end of the
			%%    storage module.
			gen_server:cast(self(), repack),
			Interval = ar_sync_record:get_next_unsynced_interval(
				Cursor, infinity, TargetPacking, ar_data_sync, StoreID),
			NextCursor = case Interval of
				not_found ->
					Cursor + ?DATA_CHUNK_SIZE;
				{_, Start} ->
					Start
			end,
			NextCursor2 = max(NextCursor, Cursor + ?DATA_CHUNK_SIZE),
			log_debug(skipping_cursor, State, [
				{next_cursor, NextCursor2},
				{cursor, Cursor},
				{footprint_start, FootprintStart},
				{footprint_end, FootprintEnd},
				{footprint_offsets, length(FootprintOffsets)}
			] ++ Logs),
			State#state{ next_cursor = NextCursor2 };
		true ->
			State2 = State#state{ 
				footprint_start = FootprintStart,
				footprint_end = FootprintEnd,
				next_cursor = Cursor + ?DATA_CHUNK_SIZE
			},

			{_, EntropyEnd, _} = get_read_range(BucketEndOffset, State2),
			State3 = State2#state{
				entropy_end = ar_chunk_storage:get_chunk_bucket_end(EntropyEnd)
			},
			State4 = init_repack_chunk_map(FootprintOffsets, State3),

			MaxChunkMapOffset = lists:max(maps:keys(State4#state.repack_chunk_map)),
			
			log_info(repack_footprint_start, State4, [
				{cursor, Cursor},
				{bucket_end_offset, BucketEndOffset},
				{source_packing, ar_serialize:encode_packing(SourcePacking, false)},
				{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
				{entropy_end, EntropyEnd},
				{read_batch_size, BatchSize},
				{write_batch_size, State4#state.write_batch_size},
				{num_entropy_offsets, NumEntropyOffsets},
				{footprint_offsets, length(FootprintOffsets)},
				{max_chunk_map_offset, MaxChunkMapOffset}
			]),

			%% sanity checks
			true = MaxChunkMapOffset =< FootprintEnd,
			true = EntropyEnd =< FootprintEnd,
			true = FootprintEnd =< ModuleEnd,
			%% end sanity checks

			%% We'll generate BatchSize entropy footprints, one for each bucket end offset
			%% starting at BucketEndOffset and ending at EntropyEnd.
			generate_repack_entropy(BucketEndOffset, SourcePacking, State4),
			generate_repack_entropy(BucketEndOffset, TargetPacking, State4),

			ar_repack_io:read_footprint(
				FootprintOffsets, FootprintStart, FootprintEnd, StoreID),

			State4
	end.

should_repack(Cursor, FootprintStart, FootprintEnd, State) ->
	#state{ module_start = ModuleStart, module_end = ModuleEnd,
		target_packing = TargetPacking, store_id = StoreID } = State,
	PaddedEndOffset = ar_block:get_chunk_padded_offset(Cursor),
	IsChunkRecorded = ar_sync_record:is_recorded(PaddedEndOffset, ar_data_sync, StoreID),
	IsEntropyRecorded = ar_entropy_storage:is_entropy_recorded(
		PaddedEndOffset, TargetPacking, StoreID),
	%% Skip this offset if it's already packed to TargetPacking, or if it's not recorded
	%% at all.
	Skip = case {IsChunkRecorded, IsEntropyRecorded} of
		%% Chunk is missing and we haven't written entropy yet, so we still want to process
		%% the bucket and write entropy to it.
		{false, false} -> false;
		%% Chunk is missing but entropy has already been written, so we can skip.
		{false, true} -> true;
		%% Skip if chunk is recorded and already packed to TargetPacking
		{{true, TargetPacking}, _} -> true;
		%% Skip if entropy exists for an unpacked chunk as this indicates the chunks
		%% 1. the chunks are small and therefore can't be packed
		%% 2. have already been processed and classified as `entropy_only`
		{{true, unpacked}, true} -> true;
		_ -> false
	end,

	ShouldRepack = (
		not Skip 
		andalso FootprintStart =< ModuleEnd
		andalso FootprintEnd >= ModuleStart
	),
	case ShouldRepack of
		false ->
			Logs = [
				{cursor, Cursor},
				{padded_end_offset, PaddedEndOffset},
				{is_chunk_recorded, IsChunkRecorded},
				{is_entropy_recorded, IsEntropyRecorded},
				{skip, Skip}
			],
			{false, Logs};
		_ ->
			true
	end.

%% @doc Generates the set of entropy offsets that will be used during one iteration of
%% repack_footprint. Expects to be called with a BucketEndOffset. This is to avoid
%% unexpected filtering results when a BucketEndOffset is lower than a PickOffset or
%% an AbsoluteEndOffset.
%% 
%% One footprint of entropy offsets is generated and then filtered such that:
%% - no offset is less than BucketEndOffset
%% - no offset is greater than ModuleEnd
%% - at most NumEntropyOffsets offsets are returned
footprint_offsets(BucketEndOffset, NumEntropyOffsets, ModuleEnd) ->
	%% sanity checks
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(BucketEndOffset),
	%% end sanity checks
	
	EntropyOffsets = ar_entropy_gen:entropy_offsets(BucketEndOffset, ModuleEnd),

	FilteredOffsets = lists:filter(
		fun(Offset) -> Offset >= BucketEndOffset end,
		EntropyOffsets),
	
	lists:sublist(FilteredOffsets, NumEntropyOffsets).

%% @doc Calculates and returns the highest chunk offset that can be read for this 
%% repack footprint. This is the highest chunk offset that maps to the highest bucket in
%% the footprint.
footprint_end(FootprintOffsets, ModuleEnd, BatchSize) ->
	FirstOffset = lists:min(FootprintOffsets),
	LastOffset = lists:max(FootprintOffsets),
	%% The final read range of the footprint starts at the last entropy offset
	{_, LastOffsetRangeEnd, _} = get_read_range(LastOffset, ModuleEnd, BatchSize),

	%% makes sure all offsets are in the same entropy partition
	Partition = ar_replica_2_9:get_entropy_partition(FirstOffset),
	{_, EntropyPartitionEnd} = ar_replica_2_9:get_entropy_partition_range(Partition),

	min(LastOffsetRangeEnd, EntropyPartitionEnd).

generate_repack_entropy(BucketEndOffset, {replica_2_9, _}, #state{ entropy_end = EntropyEnd })
		when BucketEndOffset > EntropyEnd ->
	ok;
generate_repack_entropy(BucketEndOffset, {replica_2_9, RewardAddr}, #state{} = State) ->
	#state{ 
		store_id = StoreID
	} = State,

	ar_entropy_gen:generate_entropies(StoreID, RewardAddr, BucketEndOffset, self());
generate_repack_entropy(_BucketEndOffset, _Packing, #state{}) ->
	%% Only generate entropy for the replica.2.9 packing format.
	ok.

init_repack_chunk_map([], #state{} = State) ->
	State;
init_repack_chunk_map([EntropyOffset | EntropyOffsets], #state{} = State) ->
	#state{ 
		repack_chunk_map = Map,
		configured_packing = SourcePacking,
		target_packing = TargetPacking
	} = State,

	{_ReadRangeStart, _ReadRangeEnd, ReadRangeOffsets} = get_read_range(
		EntropyOffset, State),

	Map2 = lists:foldl(
		fun(BucketEndOffset, Acc) ->
			false = maps:is_key(BucketEndOffset, Acc),
			SourceEntropy = case SourcePacking of
				{replica_2_9, _} ->
					not_set;
				_ ->
					%% Setting to <<>> indicates that source entropy is not needed.
					<<>>
			end,
			TargetEntropy = case TargetPacking of
				{replica_2_9, _} ->
					not_set;
				_ ->
					%% Setting to <<>> indicates that target entropy is not needed.
					<<>>
			end,

			RepackChunk = #repack_chunk{
				offsets = #chunk_offsets{
					bucket_end_offset = BucketEndOffset
				},
				target_packing = TargetPacking,
				source_entropy = SourceEntropy,
				target_entropy = TargetEntropy
			},
			maps:put(BucketEndOffset, RepackChunk, Acc)
		end,
	Map, ReadRangeOffsets),

	%% sanity checks
	true = maps:size(Map2) == maps:size(Map) + length(ReadRangeOffsets),
	%% end sanity checks

	init_repack_chunk_map(EntropyOffsets, State#state{ repack_chunk_map = Map2 }).

add_range_to_repack_chunk_map(OffsetChunkMap, OffsetMetadataMap, #state{} = State) ->
	#state{
		store_id = StoreID,
		configured_packing = ConfiguredPacking,
		target_packing = TargetPacking
	} = State,
	
	maps:fold(
		fun(AbsoluteEndOffset, Metadata, Acc) ->
			#state{
				repack_chunk_map = RepackChunkMap
			} = Acc,

			BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset),
			RepackChunk =  maps:get(BucketEndOffset, RepackChunkMap, not_found),
			RepackChunk2 = assemble_repack_chunk(
				RepackChunk, AbsoluteEndOffset, TargetPacking, 
				Metadata, OffsetChunkMap, ConfiguredPacking, StoreID),

			case RepackChunk2 of
				not_found ->
					Acc;
				_ ->
					update_chunk_state(RepackChunk2, Acc)
			end
		end,
		State, OffsetMetadataMap).

assemble_repack_chunk(
		RepackChunk, AbsoluteEndOffset, TargetPacking, Metadata, OffsetChunkMap,
		ConfiguredPacking, StoreID) ->
	{ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize} = Metadata,

	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset),
	PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteEndOffset),

	SourcePacking = get_chunk_packing(PaddedEndOffset, ConfiguredPacking, StoreID),

	ShouldRepack = (
		ar_chunk_storage:is_storage_supported(
			PaddedEndOffset, ChunkSize, SourcePacking)
		orelse
		ar_chunk_storage:is_storage_supported(
			PaddedEndOffset, ChunkSize, TargetPacking)
	),

	case {ShouldRepack, RepackChunk} of
		{true, not_found} ->
			log_error(chunk_not_found_in_map, [
				{bucket_end_offset, ar_chunk_storage:get_chunk_bucket_end(AbsoluteEndOffset)},
				{absolute_end_offset, AbsoluteEndOffset},
				{padded_end_offset, ar_block:get_chunk_padded_offset(AbsoluteEndOffset)},
				{chunk_size, ChunkSize}
			]),
			not_found;
		{true, _} ->
			RepackChunk#repack_chunk{
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
			};
		{false, _} ->
			not_found
	end.

get_chunk_packing(PaddedEndOffset, ConfiguredPacking, StoreID) ->
	HasConfiguredPacking = ar_sync_record:is_recorded(
		PaddedEndOffset, ConfiguredPacking, ar_data_sync, StoreID),
	case HasConfiguredPacking of
		true -> ConfiguredPacking;
		_ ->
			case ar_sync_record:is_recorded(PaddedEndOffset, ar_data_sync, StoreID) of
				{true, Packing} -> Packing;
				_ -> not_found
			end
	end.

%% @doc Mark any chunks that weren't found in either chunk_storage or the chunks_index.
mark_missing_chunks([], #state{} = State) ->
	State;
mark_missing_chunks([BucketEndOffset | ReadRangeOffsets], #state{} = State) ->
	#state{ 
		repack_chunk_map = Map
	} = State,
	
	RepackChunk = maps:get(BucketEndOffset, Map, not_found),

	State2 = case RepackChunk of
		not_found ->
			State;
		#repack_chunk{state = needs_chunk} ->
			%% If we're here and still in the needs_chunk state it means we weren't able
			%% to find the chunk in chunk_storage or the chunks_index.
			RepackChunk2 = RepackChunk#repack_chunk{
				chunk = not_found,
				metadata = not_found
			},
			update_chunk_state(RepackChunk2, State);
		_ ->
			State
	end,

	mark_missing_chunks(ReadRangeOffsets, State2).

cache_repack_chunk(RepackChunk, #state{} = State) ->
	#repack_chunk{
		offsets = #chunk_offsets{
			bucket_end_offset = BucketEndOffset
		}
	} = RepackChunk,
	State#state{ repack_chunk_map =
		maps:put(BucketEndOffset, RepackChunk, State#state.repack_chunk_map)
	}.

remove_repack_chunk(BucketEndOffset, #state{} = State) ->
	State2 = State#state{ repack_chunk_map =
		maps:remove(BucketEndOffset, State#state.repack_chunk_map)
	},
	maybe_repack_next_footprint(State2).

enqueue_chunk_for_writing(RepackChunk, #state{} = State) ->
	#state{
		target_packing = TargetPacking,
		store_id = StoreID
	} = State,
	#repack_chunk{
		offsets = #chunk_offsets{
			bucket_end_offset = BucketEndOffset
		}
	} = RepackChunk,
	State2 = State#state{
		write_queue = gb_sets:add_element(
			{BucketEndOffset, RepackChunk}, State#state.write_queue)
	},

	case gb_sets:size(State2#state.write_queue) >= State2#state.write_batch_size of
		true ->
			count_states(queue, State2),
			ar_repack_io:write_queue(State2#state.write_queue, TargetPacking, StoreID),
			State2#state{ write_queue = gb_sets:new() };
		false ->
			State2
	end.

entropy_generated(Entropy, BucketEndOffset, RewardAddr, #state{} = State) ->
	#state{
		repack_chunk_map = Map,
		configured_packing = SourcePacking,
		target_packing = TargetPacking
	} = State,

	case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% This should never happen.
			log_error(entropy_generated_chunk_not_found, State, [
				{bucket_end_offset, BucketEndOffset}
			]),
			State;
		RepackChunk ->
			RepackChunk2 = case {replica_2_9, RewardAddr} of
				TargetPacking ->
					RepackChunk#repack_chunk{
						target_entropy = Entropy
					};
				SourcePacking ->
					RepackChunk#repack_chunk{
						source_entropy = Entropy
					}
			end,
			update_chunk_state(RepackChunk2, State)
	end.

maybe_repack_next_footprint(#state{} = State) ->
	#state{ 
		repack_chunk_map = Map,
		write_queue = WriteQueue,
		target_packing = TargetPacking,
		store_id = StoreID
	} = State,
	case maps:size(Map) of
		0 ->
			count_states(queue, State),
			ar_repack_io:write_queue(WriteQueue, TargetPacking, StoreID),
			State2 = State#state{ write_queue = gb_sets:new() },
			gen_server:cast(self(), repack),
			State2;
		_ ->
			State
	end.

read_chunk_and_data_path(RepackChunk, #state{} = State) ->
	#state{
		store_id = StoreID
	} = State,
	#repack_chunk{
		metadata = Metadata,
		chunk = MaybeChunk
	} = RepackChunk,
	#chunk_metadata{
		chunk_data_key = ChunkDataKey
	} = Metadata,
	case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			log_warning(chunk_not_found_in_chunk_data_db, RepackChunk, State, []),
			RepackChunk#repack_chunk{ 
				metadata = Metadata#chunk_metadata{ data_path = not_found } };
		{ok, V} ->
			case binary_to_term(V) of
				{Chunk, DataPath} ->
					RepackChunk#repack_chunk{ 
						metadata = Metadata#chunk_metadata{ data_path = DataPath },
						chunk = Chunk
					};
				DataPath when MaybeChunk /= not_found ->
					RepackChunk#repack_chunk{ 
						metadata = Metadata#chunk_metadata{ data_path = DataPath },
						chunk = MaybeChunk
					};
				_ ->
					log_warning(chunk_not_found, RepackChunk, State, []),
					RepackChunk#repack_chunk{ 
						metadata = Metadata#chunk_metadata{ data_path = not_found }
					}
			end
	end.

update_chunk_state(RepackChunk, #state{} = State) ->
	RepackChunk2 = ar_repack_fsm:crank_state(RepackChunk),

	case RepackChunk == RepackChunk2 of
		true ->
			%% Cache it anyways, just in case.
			cache_repack_chunk(RepackChunk2, State);
		false ->
			process_state_change(RepackChunk2, State)
	end.

process_state_change(RepackChunk, #state{} = State) ->
	#state{
		store_id = StoreID,
		footprint_start = FootprintStart
	} = State,
	#repack_chunk{
		offsets = #chunk_offsets{
			bucket_end_offset = BucketEndOffset,
			absolute_offset = AbsoluteEndOffset
		},
		chunk = Chunk
	} = RepackChunk,

	case RepackChunk#repack_chunk.state of
		invalid ->
			ChunkSize = RepackChunk#repack_chunk.metadata#chunk_metadata.chunk_size,
			ar_data_sync:invalidate_bad_data_record(
				AbsoluteEndOffset, ChunkSize, StoreID, repack_found_stale_indices),
			RepackChunk2 = RepackChunk#repack_chunk{ chunk = invalid },
			State2 = cache_repack_chunk(RepackChunk2, State),
			update_chunk_state(RepackChunk2, State2);
		already_repacked ->
			%% Remove the chunk to free up memory. If we're in the already_repacked state
			%% it means the entropy hasn't been set yet. Once it's set we'll transition to
			%% the ignore state and the RepackChunk will be removed from the cache.
			RepackChunk2 = RepackChunk#repack_chunk{ chunk = <<>> },
			cache_repack_chunk(RepackChunk2, State);
		needs_data_path ->
			RepackChunk2 = read_chunk_and_data_path(RepackChunk, State),
			State2 = cache_repack_chunk(RepackChunk2, State),
			update_chunk_state(RepackChunk2, State2);
		needs_repack ->
			%% Include BatchStart so that we don't accidentally expire a chunk from some
			%% future batch. Unlikely, but not impossible.
			ChunkSize = RepackChunk#repack_chunk.metadata#chunk_metadata.chunk_size,
			TXRoot = RepackChunk#repack_chunk.metadata#chunk_metadata.tx_root,
			SourcePacking = RepackChunk#repack_chunk.source_packing,
			TargetPacking = RepackChunk#repack_chunk.target_packing,

			Packing = case TargetPacking of
				{replica_2_9, _} -> unpacked_padded;
				_ -> TargetPacking
			end,

			ar_packing_server:request_repack({BucketEndOffset, FootprintStart}, self(),
				{Packing, SourcePacking, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize}),
			cache_repack_chunk(RepackChunk, State);
		needs_decipher ->
			%% We now have the unpacked_padded chunk and the entropy, proceed
			%% with enciphering and storing the chunk.
			SourceEntropy = RepackChunk#repack_chunk.source_entropy,
			ar_packing_server:request_decipher(
				{BucketEndOffset, FootprintStart}, self(), {Chunk, SourceEntropy}),
			cache_repack_chunk(RepackChunk, State);
		needs_encipher ->
			%% We now have the unpacked_padded chunk and the entropy, proceed
			%% with enciphering and storing the chunk.
			TargetEntropy = RepackChunk#repack_chunk.target_entropy,
			ar_packing_server:request_encipher(
				{BucketEndOffset, FootprintStart}, self(), {Chunk, TargetEntropy}),
			cache_repack_chunk(RepackChunk, State);
		write_entropy ->
			State2 = enqueue_chunk_for_writing(RepackChunk, State),
			remove_repack_chunk(BucketEndOffset, State2);
		write_chunk ->
			State2 = enqueue_chunk_for_writing(RepackChunk, State),
			remove_repack_chunk(BucketEndOffset, State2);
		ignore ->
			%% Chunk was already_repacked.
			remove_repack_chunk(BucketEndOffset, State);
		error ->
			%% This should never happen.
			log_error(invalid_repack_chunk_state, RepackChunk, State, []),
			remove_repack_chunk(BucketEndOffset, State);
		_ ->
			%% No action to take now, but since the chunk state changed, we need to update
			%% the cache.
			cache_repack_chunk(RepackChunk, State)
	end.

expire_exor_request(BucketEndOffset, State) ->
	#state{
		repack_chunk_map = Map
	} = State,
	case maps:get(BucketEndOffset, Map, not_found) of
		not_found ->
			%% Chunk has already been processed.
			State;
		RepackChunk ->
			log_debug(exor_request_expired, RepackChunk, State, []),
			remove_repack_chunk(BucketEndOffset, State)
	end.

read_cursor(StoreID, TargetPacking, ModuleStart) ->
	Filepath = ar_chunk_storage:get_filepath("repack_in_place_cursor2", StoreID),
	DefaultCursor = case ModuleStart of
		0 -> 0;
		_ -> ModuleStart + 1
	end,
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

log_error(Event, #repack_chunk{} = RepackChunk, #state{} = State, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, RepackChunk, State, ExtraLogs)).

log_error(Event, #state{} = State, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, State, ExtraLogs)).

log_error(Event, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, ExtraLogs)).

log_warning(Event, #repack_chunk{} = RepackChunk, #state{} = State, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, RepackChunk, State, ExtraLogs)).

log_warning(Event, #state{} = State, ExtraLogs) ->
	?LOG_WARNING(format_logs(Event, State, ExtraLogs)).

log_info(Event, #state{} = State, ExtraLogs) ->
	?LOG_INFO(format_logs(Event, State, ExtraLogs)).

log_debug(Event, #repack_chunk{} = RepackChunk, #state{} = State, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, RepackChunk, State, ExtraLogs)).
	
log_debug(Event, #state{} = State, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, State, ExtraLogs)).

format_logs(Event, ExtraLogs) ->
	[
		{event, Event},
		{tags, [repack_in_place]},
		{pid, self()}
		| ExtraLogs
	].

format_logs(Event, #state{} = State, ExtraLogs) ->
	format_logs(Event, [
		{store_id, State#state.store_id},
		{next_cursor, State#state.next_cursor},
		{footprint_start, State#state.footprint_start},
		{footprint_end, State#state.footprint_end},
		{module_start, State#state.module_start},
		{module_end, State#state.module_end},
		{repack_chunk_map, maps:size(State#state.repack_chunk_map)},
		{write_queue, gb_sets:size(State#state.write_queue)}
		| ExtraLogs
	]).

format_logs(Event, #repack_chunk{} = RepackChunk, #state{} = State, ExtraLogs) ->
	#repack_chunk{
		state = ChunkState,
		offsets = Offsets,
		metadata = Metadata,
		chunk = Chunk,
		target_entropy = TargetEntropy,
		source_entropy = SourceEntropy,
		source_packing = SourcePacking,
		target_packing = TargetPacking
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
	format_logs(Event, State, [
		{state, ChunkState},
		{bucket_end_offset, BucketEndOffset},
		{absolute_offset, AbsoluteOffset},
		{padded_end_offset, PaddedEndOffset},
		{chunk_size, ChunkSize},
		{chunk, atom_or_binary(Chunk)},
		{source_packing, ar_serialize:encode_packing(SourcePacking, false)},
		{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
		{source_entropy, atom_or_binary(SourceEntropy)},
		{target_entropy, atom_or_binary(TargetEntropy)} | ExtraLogs
	]).

count_states(cache, #state{} = State) ->
	#state{
		store_id = StoreID,
		repack_chunk_map = Map
	} = State,
	MapCount = maps:fold(
		fun(_BucketEndOffset, RepackChunk, Acc) ->
			maps:update_with(RepackChunk#repack_chunk.state, fun(Count) -> Count + 1 end, 1, Acc)
		end,
		#{},
		Map
	),
	log_debug(count_cache_states, State, [
		{cache_size, maps:size(Map)},
		{states, maps:to_list(MapCount)}
	]),
	StoreIDLabel = ar_storage_module:label(StoreID),
	maps:fold(
		fun(ChunkState, Count, Acc) ->
			prometheus_gauge:set(repack_chunk_states, [StoreIDLabel, cache, ChunkState], Count),
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
		fun({_BucketEndOffset, RepackChunk}, Acc) ->
			maps:update_with(RepackChunk#repack_chunk.state, fun(Count) -> Count + 1 end, 1, Acc)
		end,
		#{},
		WriteQueue
	),
	log_debug(count_write_queue_states, State, [
		{queue_size, gb_sets:size(WriteQueue)},
		{states, maps:to_list(WriteQueueCount)}
	]),	
	StoreIDLabel = ar_storage_module:label(StoreID),
	maps:fold(
		fun(ChunkState, Count, Acc) ->
			prometheus_gauge:set(repack_chunk_states, [StoreIDLabel, queue, ChunkState], Count),
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

cache_size_test_() ->
	ar_test_node:test_with_mocked_functions([
		{ar_block, get_sub_chunks_per_replica_2_9_entropy, fun() -> 3 end}
	],
	fun test_cache_size/0, 30).

test_cache_size() ->
	?assertEqual(1, calculate_num_entropy_offsets(100, 400)),
	?assertEqual(2, calculate_num_entropy_offsets(100, 200)),
	?assertEqual(3, calculate_num_entropy_offsets(300, 400)),
	?assertEqual(3, calculate_num_entropy_offsets(3000, 400)),
	?assertEqual(3, calculate_num_entropy_offsets(3, 4)),
	?assertEqual(3, calculate_num_entropy_offsets(3, 1)),
	?assertEqual(2, calculate_num_entropy_offsets(5, 10)).
	
footprint_offsets_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_block, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
			{ar_block, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
			{ar_block, get_sub_chunks_per_replica_2_9_entropy, fun() -> 3 end},
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end}
		],
		fun test_footprint_offsets_small/0, 30),

		%% Run footprint_offsets tests using the production constant values.
		ar_test_node:test_with_mocked_functions([
			{ar_block, partition_size, fun() -> 3_600_000_000_000 end},
			{ar_block, strict_data_split_threshold, fun() -> 30_607_159_107_830 end},
			{ar_storage_module, get_overlap, fun(_) -> 104_857_600 end},
			{ar_block, get_sub_chunks_per_replica_2_9_entropy, fun() -> 1024 end},
			{ar_block, get_replica_2_9_entropy_sector_size, fun() -> 3_515_875_328 end}
		],
		fun test_footprint_offsets_large/0, 30)
	].

test_footprint_offsets_small() ->
    {Start0, End0} = ar_storage_module:module_range({ar_block:partition_size(), 0, unpacked}),
	{Start1, End1} = ar_storage_module:module_range({ar_block:partition_size(), 1, unpacked}),
	PaddedEnd0 = ar_block:get_chunk_padded_offset(End0),
	PaddedEnd1 = ar_block:get_chunk_padded_offset(End1),

	?assertEqual(3, ar_block:get_sub_chunks_per_replica_2_9_entropy()),
	?assertEqual({0, 2262144}, {Start0, End0}),
	?assertEqual({2000000, 4262144}, {Start1, End1}),
	?assertEqual(2272864, PaddedEnd0),
	?assertEqual(4370016, PaddedEnd1),

	?assertEqual([262144, 1048576, 1835008], footprint_offsets(262144, 3, PaddedEnd0)),
	?assertEqual([262144], footprint_offsets(262144, 3, 1_000_000)),
	?assertEqual([262144, 1048576], footprint_offsets(262144, 3, 1_500_000)),
	?assertEqual([262144, 1048576], footprint_offsets(262144, 2, PaddedEnd0)),
	?assertEqual([262144], footprint_offsets(262144, 1, PaddedEnd0)),

	?assertEqual([786432, 1572864], footprint_offsets(786432, 3, PaddedEnd0)),
	?assertEqual([786432, 1572864], footprint_offsets(786432, 2, PaddedEnd0)),
	?assertEqual([786432], footprint_offsets(786432, 1, PaddedEnd0)),

	?assertEqual([1048576, 1835008], footprint_offsets(1048576, 3, PaddedEnd0)),

	?assertEqual([1572864], footprint_offsets(1572864, 3, PaddedEnd0)),
	?assertEqual([1572864], footprint_offsets(1572864, 2, PaddedEnd0)),
	?assertEqual([1572864], footprint_offsets(1572864, 1, PaddedEnd0)),
	
	?assertEqual([1835008], footprint_offsets(1835008, 3, PaddedEnd0)),
	?assertEqual([2097152], footprint_offsets(2097152, 3, PaddedEnd0)),
	
	%% all offsets should be limited to a single entropy partition
	?assertEqual([2097152], footprint_offsets(2097152, 3, PaddedEnd1)),

	?assertEqual([2359296, 3145728, 3932160], footprint_offsets(2359296, 3, PaddedEnd1)),
	?assertEqual([2621440, 3407872, 4194304], footprint_offsets(2621440, 3, PaddedEnd1)),
	?assertEqual([2883584, 3670016], footprint_offsets(2883584, 3, PaddedEnd1)),
	?assertEqual([3145728, 3932160], footprint_offsets(3145728, 3, PaddedEnd1)),
	?assertEqual([4194304], footprint_offsets(4194304, 3, PaddedEnd1)).

%% @doc run a series of footprint_offsets tests using the production constant values.
test_footprint_offsets_large() ->
	{Start0, End0} = ar_storage_module:module_range({ar_block:partition_size(), 0, unpacked}),
	{Start1, End1} = ar_storage_module:module_range({ar_block:partition_size(), 1, unpacked}),
	{Start30, End30} = ar_storage_module:module_range({ar_block:partition_size(), 30, unpacked}),
	PaddedEnd0 = ar_block:get_chunk_padded_offset(End0),
	PaddedEnd1 = ar_block:get_chunk_padded_offset(End1),
	PaddedEnd30 = ar_block:get_chunk_padded_offset(End30),

	?assertEqual(1024, ar_block:get_sub_chunks_per_replica_2_9_entropy()),
	?assertEqual(3515875328, ar_block:get_replica_2_9_entropy_sector_size()),
	?assertEqual({0, 3600104857600}, {Start0, End0}),
	?assertEqual({3600000000000, 7200104857600}, {Start1, End1}),
	?assertEqual({108000000000000, 111600104857600}, {Start30, End30}),
	?assertEqual(3600104857600, PaddedEnd0),
	?assertEqual(7200104857600, PaddedEnd1),
	?assertEqual(111600104939766, PaddedEnd30),

	TestCases = [
		%% {ExpectedFootprintOffsetsLength, End, BucketEndOffset}
		%% Partition 0 - special case as there is no lower partition
		{1024, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0)},
		{1024, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0 + ?DATA_CHUNK_SIZE)},
		{1024, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0 + (2 * ?DATA_CHUNK_SIZE))},
		{1023, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0 + (ar_block:get_replica_2_9_entropy_sector_size()))},
		{1023, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0 + (ar_block:get_replica_2_9_entropy_sector_size() + ?DATA_CHUNK_SIZE))},
		{1022, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0 + (2 * ar_block:get_replica_2_9_entropy_sector_size()))},
		{1022, PaddedEnd0, ar_chunk_storage:get_chunk_bucket_end(Start0 + (2 * ar_block:get_replica_2_9_entropy_sector_size() + ?DATA_CHUNK_SIZE))},
		%% Partition 1 - before the strict data split threshold
		{1, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1)},
		{1, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1 + ?DATA_CHUNK_SIZE)},
		{1024, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1 + (2 * ?DATA_CHUNK_SIZE))},
		{1023, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1 + (ar_block:get_replica_2_9_entropy_sector_size()))},
		{1023, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1 + (ar_block:get_replica_2_9_entropy_sector_size() + ?DATA_CHUNK_SIZE))},
		{1022, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1 + (2 * ar_block:get_replica_2_9_entropy_sector_size()))},
		{1022, PaddedEnd1, ar_chunk_storage:get_chunk_bucket_end(Start1 + (2 * ar_block:get_replica_2_9_entropy_sector_size() + ?DATA_CHUNK_SIZE))},
		%% Partition 30 - after the strict data split threshold
		{1, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30)},
		{1024, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30 + ?DATA_CHUNK_SIZE)},
		{1024, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30 + (2 * ?DATA_CHUNK_SIZE))},
		{1023, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30 + (ar_block:get_replica_2_9_entropy_sector_size()))},
		{1023, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30 + (ar_block:get_replica_2_9_entropy_sector_size() + ?DATA_CHUNK_SIZE))},
		{1022, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30 + (2 * ar_block:get_replica_2_9_entropy_sector_size()))},
		{1022, PaddedEnd30, ar_chunk_storage:get_chunk_bucket_end(Start30 + (2 * ar_block:get_replica_2_9_entropy_sector_size() + ?DATA_CHUNK_SIZE))}
	],

	lists:foreach(
		fun({ExpectedLength, End, BucketEndOffset}) ->
			?assertEqual(ExpectedLength, length(footprint_offsets(BucketEndOffset, 1024, End)),
				lists:flatten(io_lib:format(
					"Offset: ~p, Expected Length: ~p", [BucketEndOffset, ExpectedLength])))
		end,
		TestCases
	),

	ok.

footprint_end_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_block, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
			{ar_block, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
			{ar_block, get_sub_chunks_per_replica_2_9_entropy, fun() -> 3 end},
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end}
		],
		fun test_footprint_end_small/0, 30)
	].

test_footprint_end_small() ->
	{Start0, End0} = ar_storage_module:module_range({ar_block:partition_size(), 0, unpacked}),
	{Start1, End1} = ar_storage_module:module_range({ar_block:partition_size(), 1, unpacked}),
	PaddedEnd0 = ar_block:get_chunk_padded_offset(End0),
	PaddedEnd1 = ar_block:get_chunk_padded_offset(End1),

	?assertEqual(3, ar_block:get_sub_chunks_per_replica_2_9_entropy()),
	?assertEqual({0, 2262144}, {Start0, End0}),
	?assertEqual({2000000, 4262144}, {Start1, End1}),
	?assertEqual(2272864, PaddedEnd0),
	?assertEqual(4370016, PaddedEnd1),
	?assertEqual({0, 2272864}, ar_replica_2_9:get_entropy_partition_range(0)),

	?assertEqual(2010720,
		footprint_end([262144, 1048576, 1835008], PaddedEnd0, 1)),
	?assertEqual(2272864,
		footprint_end([262144, 1048576, 1835008], PaddedEnd0, 2)),
	?assertEqual(2272864,
		footprint_end([262144, 1048576, 1835008], PaddedEnd0, 3)),
	?assertEqual(2272864,
		footprint_end([262144, 1048576, 1835008], PaddedEnd1, 4)),
	ok.

assemble_repack_chunk_test_() ->
[
	ar_test_node:test_with_mocked_functions([
			{ar_sync_record, is_recorded, fun(_, _, _) -> {true, unpacked} end}
		],
		fun test_assemble_repack_chunk/0, 30),
	ar_test_node:test_with_mocked_functions([
			{ar_sync_record, is_recorded, fun(_, _, _) -> {true, unpacked} end}
		],
		fun test_assemble_repack_chunk_too_small_unpacked/0, 30),
	ar_test_node:test_with_mocked_functions([
			{ar_sync_record, is_recorded, fun(_, _, _) -> {true, {spora_2_6, <<"addr">>}} end}
		],
		fun test_assemble_repack_chunk_too_small_packed/0, 30)
].

test_assemble_repack_chunk() ->
	Addr = <<"addr">>,
	StoreID = "storage_module_100_unpacked",
	ChunkDataKey = <<"chunk_data_key">>,
	TXRoot = <<"tx_root">>,
	DataRoot = <<"data_root">>,
	TXPath = <<"tx_path">>,
	RelativeOffset = 1000,
	ChunkSize = ?DATA_CHUNK_SIZE,
	Chunk = crypto:strong_rand_bytes(ChunkSize),

	Metadata = {ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize},

	% %% Error - BucketEndOffset hasn't been initialized
	?assertEqual(not_found,
		assemble_repack_chunk(not_found, 100, {replica_2_9, Addr}, Metadata, #{}, 
		unpacked, StoreID)),

	ExpectedRepackedChunk = #repack_chunk{
		source_packing = unpacked,
		metadata = #chunk_metadata{
			chunk_data_key = ChunkDataKey,
			tx_root = TXRoot,
			data_root = DataRoot,
			tx_path = TXPath,
			chunk_size = ChunkSize
		},
		chunk = Chunk
	},

	%% Chunk before the strict data split threshold
	%% unpacked -> unpacked
	ExpectedOffsets1 = #chunk_offsets{
		absolute_offset = 100,
		bucket_end_offset = 262144,
		padded_end_offset = 100,
		relative_offset = RelativeOffset
	},
	?assertEqual(
		ExpectedRepackedChunk#repack_chunk{
			offsets = ExpectedOffsets1,
			target_packing = unpacked
		},
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = unpacked
			}, 100, unpacked, Metadata, #{ 100 => Chunk }, unpacked, StoreID)
	),
	%% unpacked -> packed
	?assertEqual(
		ExpectedRepackedChunk#repack_chunk{
			offsets = ExpectedOffsets1,
			target_packing = {replica_2_9, Addr}
		},
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = {replica_2_9, Addr}
			}, 100, {replica_2_9, Addr}, Metadata, #{ 100 => Chunk }, unpacked, StoreID)
	),

	%% Chunk after the strict data split threshold
	%% unpacked -> unpacked
	ExpectedOffsets2 = #chunk_offsets{
		absolute_offset = 10_000_000,
		bucket_end_offset = 10_223_616,
		padded_end_offset = 10_223_616,
		relative_offset = RelativeOffset
	},
	?assertEqual(
		ExpectedRepackedChunk#repack_chunk{
			offsets = ExpectedOffsets2,
			target_packing = unpacked
		},
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = unpacked
			}, 10_000_000, unpacked, Metadata, #{ 10_223_616 => Chunk }, unpacked, StoreID)
	),
	%% unpacked -> packed
	?assertEqual(
		ExpectedRepackedChunk#repack_chunk{
			offsets = ExpectedOffsets2,
			target_packing = {replica_2_9, Addr}
		},
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = {replica_2_9, Addr}
			}, 10_000_000, {replica_2_9, Addr}, Metadata, #{ 10_223_616 => Chunk }, 
			unpacked, StoreID)
	),
	ok.

test_assemble_repack_chunk_too_small_unpacked() ->
	Addr = <<"addr">>,
	StoreID = "storage_module_100_unpacked",
	ChunkDataKey = <<"chunk_data_key">>,
	TXRoot = <<"tx_root">>,
	DataRoot = <<"data_root">>,
	TXPath = <<"tx_path">>,
	RelativeOffset = 1000,
	ChunkSize = 100,

	Metadata = {ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize},

	%% Small chunk before the strict data split threshold
	%% unpacked -> unpacked
	?assertEqual(not_found,
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = unpacked
			}, 100, unpacked, Metadata, #{}, unpacked, StoreID)),
	%% unpacked -> packed
	?assertEqual(not_found,
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = {replica_2_9, Addr}
			}, 100, {replica_2_9, Addr}, Metadata, #{}, unpacked, StoreID)),

	%% Small chunk after the strict data split threshold
	%% unpacked -> unpacked
	?assertEqual(not_found,
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = unpacked
			}, 10_000_000, unpacked, Metadata, #{}, unpacked, StoreID)),
	%% unpacked -> packed
	ExpectedRepackedChunk = #repack_chunk{
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr},
		metadata = #chunk_metadata{
			chunk_data_key = ChunkDataKey,
			tx_root = TXRoot,
			data_root = DataRoot,
			tx_path = TXPath,
			chunk_size = ChunkSize
		},
		offsets = #chunk_offsets{
			absolute_offset = 10_000_000,
			bucket_end_offset = 10_223_616,
			padded_end_offset = 10_223_616,
			relative_offset = RelativeOffset
		},
		chunk = not_found
	},
	?assertEqual(ExpectedRepackedChunk,
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = {replica_2_9, Addr}
			}, 10_000_000, {replica_2_9, Addr}, Metadata, #{}, unpacked, StoreID)),
	ok.

test_assemble_repack_chunk_too_small_packed() ->
	Addr = <<"addr">>,
	StoreID = "storage_module_100_unpacked",
	ChunkDataKey = <<"chunk_data_key">>,
	TXRoot = <<"tx_root">>,
	DataRoot = <<"data_root">>,
	TXPath = <<"tx_path">>,
	RelativeOffset = 1000,
	ChunkSize = 100,

	Metadata = {ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset, ChunkSize},

	%% Small chunk before the strict data split threshold
	%% packed -> unpacked
	?assertEqual(not_found,
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = unpacked
			}, 100, unpacked, Metadata, #{}, {spora_2_6, <<"addr">>}, StoreID)),
	%% packed -> packed
	?assertEqual(not_found,
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = {replica_2_9, Addr}
			}, 100, {replica_2_9, Addr}, Metadata, #{}, {spora_2_6, <<"addr">>}, StoreID)),

	%% Small chunk after the strict data split threshold
	ExpectedRepackedChunk = #repack_chunk{
		source_packing = {spora_2_6, Addr},
		metadata = #chunk_metadata{
			chunk_data_key = ChunkDataKey,
			tx_root = TXRoot,
			data_root = DataRoot,
			tx_path = TXPath,
			chunk_size = ChunkSize
		},
		offsets = #chunk_offsets{
			absolute_offset = 10_000_000,
			bucket_end_offset = 10_223_616,
			padded_end_offset = 10_223_616,
			relative_offset = RelativeOffset
		},
		chunk = not_found
	},
	%% packed -> unpacked
	?assertEqual(ExpectedRepackedChunk#repack_chunk{target_packing = unpacked},
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = unpacked
			}, 10_000_000, unpacked, Metadata, #{}, {spora_2_6, <<"addr">>}, StoreID)),
	%% packed -> packed
	?assertEqual(ExpectedRepackedChunk#repack_chunk{target_packing = {replica_2_9, Addr}},
		assemble_repack_chunk(
			#repack_chunk{
				target_packing = {replica_2_9, Addr}
			}, 10_000_000, {replica_2_9, Addr}, Metadata, #{},
			{spora_2_6, <<"addr">>}, StoreID)),
	ok.

should_repack_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end},
			{ar_sync_record, is_recorded, fun(_, _, _) -> false end},
			{ar_entropy_storage, is_entropy_recorded, fun(_, _, _) -> false end}
		],
		fun test_should_repack_no_chunk_no_entropy/0, 30),
		ar_test_node:test_with_mocked_functions([
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end},
			{ar_sync_record, is_recorded, fun(_, _, _) -> {true, {replica_2_9, <<"addr">>}} end},
			{ar_entropy_storage, is_entropy_recorded, fun(_, _, _) -> true end}
		],
		fun test_should_repack_chunk_and_entropy/0, 30),
		ar_test_node:test_with_mocked_functions([
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end},
			{ar_sync_record, is_recorded, fun(_, _, _) -> false end},
			{ar_entropy_storage, is_entropy_recorded, fun(_, _, _) -> true end}
		],
		fun test_should_repack_entropy_but_no_chunk/0, 30),
		ar_test_node:test_with_mocked_functions([
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end},
			{ar_sync_record, is_recorded, fun(_, _, _) -> {true, unpacked} end},
			{ar_entropy_storage, is_entropy_recorded, fun(_, _, _) -> true end}
		],
		fun test_should_repack_unpacked_chunk_and_entropy/0, 30),
		ar_test_node:test_with_mocked_functions([
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end},
			{ar_sync_record, is_recorded, fun(_, _, _) -> {true, unpacked} end},
			{ar_entropy_storage, is_entropy_recorded, fun(_, _, _) -> false end}
		],
		fun test_should_repack_unpacked_chunk_no_entropy/0, 30)
	].

test_should_repack_no_chunk_no_entropy() ->
	%% No chunk exists to repack however we still want to process the bucket and write
	%% entropy to it.
	?assertEqual(true,
		should_repack(600_000, 200_000, 300_000, #state{
			module_start = 100_000,
			module_end = 2_000_000
		})),
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, false},
			{is_entropy_recorded, false},
			{skip, false}
		]},
		should_repack(600_000, 0, 50_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr">>}
		})),
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, false},
			{is_entropy_recorded, false},
			{skip, false}
		]},
		should_repack(600_000, 2_000_001, 3_000_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr">>}
		})),
	?assertEqual(true,
		should_repack(750_000, 200_000, 300_000, #state{
			module_start = 100_000,
			module_end = 2_000_000
		})).

test_should_repack_chunk_and_entropy() ->
	%% Chunk is already packed to the target packing
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, {true, {replica_2_9, <<"addr">>}}},
			{is_entropy_recorded, true},
			{skip, true}
		]},
		should_repack(600_000, 200_000, 300_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr">>}
		})),
	%% Chunk exists and needs repacking - but footprint start is beyond the end of the module
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, {true, {replica_2_9, <<"addr">>}}},
			{is_entropy_recorded, true},
			{skip, false}
		]},
		should_repack(600_000, 2_000_001, 3_000_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr2">>}
		})),
	%% Chunk exists, needs repacking and falls within the module.
	?assertEqual(
		true, 
		should_repack(600_000, 200_000, 300_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr2">>}
		})),
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, {true, {replica_2_9, <<"addr">>}}},
			{is_entropy_recorded, true},
			{skip, false}
		]},
		should_repack(600_000, 0, 50_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr2">>}
		})),
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, {true, {replica_2_9, <<"addr">>}}},
			{is_entropy_recorded, true},
			{skip, false}
		]},
		should_repack(600_000, 2_000_001, 3_000_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr2">>}
		})).
	
test_should_repack_entropy_but_no_chunk() ->
	%% Entropy exists which means this bucket has been processed, but there is no chunk
	%% to repack.
	?assertEqual({false, [
		{cursor, 600_000},
		{padded_end_offset, 600_000},
		{is_chunk_recorded, false},
		{is_entropy_recorded, true},
		{skip, true}
	]},
	should_repack(600_000, 200_000, 300_000, #state{
		module_start = 100_000,
		module_end = 2_000_000,
		target_packing = {replica_2_9, <<"addr">>}
	})).

test_should_repack_unpacked_chunk_and_entropy() ->
	%% Unpacked chunk and entropy exist, which means:
	%% 1. this bucket has small chunks which can not be written to chunk storage.
	%% 2. this bucket has already been processed so we can skip
	?assertEqual({false, [
		{cursor, 600_000},
		{padded_end_offset, 600_000},
		{is_chunk_recorded, {true, unpacked}},
		{is_entropy_recorded, true},
		{skip, true}
	]},
	should_repack(600_000, 200_000, 300_000, #state{
		module_start = 100_000,
		module_end = 2_000_000,
		target_packing = {replica_2_9, <<"addr">>}
	})).

test_should_repack_unpacked_chunk_no_entropy() ->
	%% Chunk is already packed to the target packing
	?assertEqual({false, [
		{cursor, 600_000},
		{padded_end_offset, 600_000},
		{is_chunk_recorded, {true, unpacked}},
		{is_entropy_recorded, false},
		{skip, true}
	]},
	should_repack(600_000, 200_000, 300_000, #state{
		module_start = 100_000,
		module_end = 2_000_000,
		target_packing = unpacked
	})),
	%% Chunk exists, needs repacking and falls within the module.
	?assertEqual(
		true, 
		should_repack(600_000, 200_000, 300_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr">>}
		})),
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, {true, unpacked}},
			{is_entropy_recorded, false},
			{skip, false}
		]},
		should_repack(600_000, 0, 50_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr">>}
		})),
	?assertEqual({false, [
			{cursor, 600_000},
			{padded_end_offset, 600_000},
			{is_chunk_recorded, {true, unpacked}},
			{is_entropy_recorded, false},
			{skip, false}
		]},
		should_repack(600_000, 2_000_001, 3_000_000, #state{
			module_start = 100_000,
			module_end = 2_000_000,
			target_packing = {replica_2_9, <<"addr">>}
		})).


init_repack_chunk_map_test_() ->
	[
		ar_test_node:test_with_mocked_functions(ar_test_node:mainnet_packing_mocks(),
			fun test_init_repack_chunk_map_a/0, 30),
		ar_test_node:test_with_mocked_functions(ar_test_node:mainnet_packing_mocks(),
			fun test_init_repack_chunk_map_b/0, 30)
	].

%% @doc This tests a specific off-by-one error that occurred in the footprint_end calculation.
%% Previously there was an ar_entropy_gen:footprint_end function which was incorrect. The
%% fix removes the ar_entropy_gen:footprint_end function and has everyone use 
%% ar_replica_2_9:get_entropy_partition_range instead, as that one does the correct end of
%% range calculation.
%% 
%% Keeping this test as it's an easy way to assert no future regressions in this logic.
test_init_repack_chunk_map_a() ->
	Cursor = 18003250911837,
	ModuleStart = 18000000000000,
	ModuleEnd = 21600104857600,
	BatchSize = 100,
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),
	FootprintOffsets = footprint_offsets(BucketEndOffset, 1024, ModuleEnd),
	FootprintStart = BucketStartOffset+1,
	FootprintEnd = footprint_end(FootprintOffsets, ModuleEnd, BatchSize),

	State = #state{
		module_start = ModuleStart,
		module_end = ModuleEnd,
		footprint_start = FootprintStart,
		footprint_end = FootprintEnd,
		read_batch_size = BatchSize,
		repack_chunk_map = #{},
		target_packing = {replica_2_9, <<"addr">>}
	},

	State2 = init_repack_chunk_map(FootprintOffsets, State),
	
	?assertEqual(102334, maps:size(State2#state.repack_chunk_map)),
	ok.

test_init_repack_chunk_map_b() ->
	Cursor = 21564833002875,
	NumEntropyOffsets = 1024,
	ModuleStart = 18000000000000,
	ModuleEnd = 21600104857600,
	BatchSize = 100,
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Cursor),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Cursor),
	FootprintOffsets = footprint_offsets(BucketEndOffset, NumEntropyOffsets, ModuleEnd),
	FootprintStart = BucketStartOffset+1,
	FootprintEnd = footprint_end(FootprintOffsets, ModuleEnd, BatchSize),
	{_, EntropyEnd, _} = get_read_range(BucketEndOffset, FootprintEnd, BatchSize),
	EntropyEnd2 = ar_chunk_storage:get_chunk_bucket_end(EntropyEnd),
	{_ReadRangeStart, _ReadRangeEnd, ReadRangeOffsets} = get_read_range(
		BucketEndOffset, FootprintEnd, BatchSize),
	State = #state{
		module_start = ModuleStart,
		module_end = ModuleEnd,
		footprint_start = FootprintStart,
		footprint_end = FootprintEnd,
		read_batch_size = BatchSize,
		repack_chunk_map = #{},
		target_packing = {replica_2_9, <<"addr">>}
	},
	State2 = init_repack_chunk_map(FootprintOffsets, State),

	MaxChunkMap = lists:max(maps:keys(State2#state.repack_chunk_map)),

	?assertEqual(ar_chunk_storage:get_chunk_bucket_end(FootprintEnd), MaxChunkMap),
	?assertEqual(EntropyEnd2, lists:max(ReadRangeOffsets)),
	ok.
	
get_read_range_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_block, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
			{ar_block, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
			{ar_block, strict_data_split_threshold, fun() -> 5_000_000 end}
		],
			fun test_get_read_range_before_strict/0, 30),
		ar_test_node:test_with_mocked_functions([
			{ar_block, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
			{ar_block, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end}
		],
			fun test_get_read_range_after_strict/0, 30)
	].

test_get_read_range_before_strict() ->
	?assertEqual({2359296, 4456447}, ar_replica_2_9:get_entropy_partition_range(1)),
	?assertEqual(786432, ar_block:get_replica_2_9_entropy_sector_size()),
	%% no limit
	?assertEqual(
		{2097151, 2883583, [2097152, 2359296, 2621440]},
		get_read_range(2097152, 4_000_000, 3)
	),
	%% sector limit
	?assertEqual(
		{2359295, 3145727, [2359296, 2621440, 2883584]},
		get_read_range(2359296, 4_000_000, 4)
	),
	?assertEqual(
		{3407871, 3932159, [3407872, 3670016]},
		get_read_range(3407872, 4_000_000, 3)
	),
	%% range end limit
	?assertEqual(
		{2359295, 2700000, [2359296, 2621440]},
		get_read_range(2359296, 2_700_000, 4)
	),
	%% partition end limit
	?assertEqual(
		{3932159, 4456447, [3932160, 4194304]},
		get_read_range(3932160, 6_000_000, 3)
	),
	ok.

test_get_read_range_after_strict() ->
	?assertEqual({2272865, 4370016}, ar_replica_2_9:get_entropy_partition_range(1)),
	?assertEqual(786432, ar_block:get_replica_2_9_entropy_sector_size()),
	%% no limit
	?assertEqual(
		{2272864, 3059296, [2359296, 2621440, 2883584]},
		get_read_range(2359296, 4_000_000, 3)
	),
	%% sector limit
	?assertEqual(
		{2272864, 3059296, [2359296, 2621440, 2883584]},
		get_read_range(2359296, 4_000_000, 4)
	),
	?assertEqual(
		{3321440, 3845728, [3407872, 3670016]},
		get_read_range(3407872, 4_000_000, 3)
	),
	%% range end limit
	?assertEqual(
		{2272864, 2700000, [2359296, 2621440]},
		get_read_range(2359296, 2_700_000, 4)
	),
	%% partition end limit
	?assertEqual(
		{3845728, 4370016, [3932160, 4194304]},
		get_read_range(3932160, 6_000_000, 3)
	),
	ok.
