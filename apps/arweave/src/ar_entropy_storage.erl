-module(ar_entropy_storage).

-behaviour(gen_server).

-export([name/1, acquire_semaphore/1, release_semaphore/1, is_ready/1,
	is_entropy_recorded/2, delete_record/2, store_entropy/7, record_chunk/7]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	store_id
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, _}) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
	list_to_atom("ar_entropy_storage_" ++ ar_storage_module:label_by_id(StoreID)).

init(StoreID) ->
	?LOG_INFO([{event, ar_entropy_storage_init}, {name, name(StoreID)}, {store_id, StoreID}]),
	{ok, #state{ store_id = StoreID }}.

store_entropy(
		StoreID, Entropies, BucketEndOffset, RangeStart, RangeEnd, Keys, RewardAddr) ->
	BucketEndOffset2 = reset_entropy_offset(BucketEndOffset),
	gen_server:cast(name(StoreID), {store_entropy,
		Entropies, BucketEndOffset2, RangeStart, RangeEnd, Keys, RewardAddr}).

is_ready(StoreID) ->
	case catch gen_server:call(name(StoreID), is_ready, infinity) of
		{'EXIT', {Reason, {gen_server, call, _}}} ->
			?LOG_WARNING([{event, is_ready_error}, {module, ?MODULE},
				{name, name(StoreID)}, {store_id, StoreID}, {reason, Reason}]),
			false;
		Reply ->
			Reply
	end.

handle_cast({store_entropy,
		Entropies, BucketEndOffset, RangeStart, RangeEnd, Keys, RewardAddr}, State) ->
	do_store_entropy(
		Entropies, BucketEndOffset, RangeStart, RangeEnd, Keys, RewardAddr, State),
	{noreply, State};
handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(is_ready, _From, State) ->
	{reply, true, State};
handle_call(Call, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}]),
	{reply, {error, unhandled_call}, State}.

terminate(Reason, State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE},
		{reason, Reason}, {name, name(State#state.store_id)},
		{store_id, State#state.store_id}]),
	ok.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

%% @doc Return true if the 2.9 entropy with the given offset is recorded.
is_entropy_recorded(PaddedEndOffset, StoreID) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_entropy) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_entropy) should not be used.
	ID = ar_chunk_storage_replica_2_9_1_entropy,
	ChunkBucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	ar_sync_record:is_recorded(ChunkBucketStart + 1, ID, StoreID).

update_sync_records(IsComplete, PaddedEndOffset, StoreID, RewardAddr) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_entropy) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_entropy) should not be used.
	ID = ar_chunk_storage_replica_2_9_1_entropy,
	BucketEnd = ar_chunk_storage:get_chunk_bucket_end(PaddedEndOffset),
	BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	ar_sync_record:add_async(replica_2_9_entropy, BucketEnd, BucketStart, ID, StoreID),
	prometheus_counter:inc(replica_2_9_entropy_stored,
		[ar_storage_module:label_by_id(StoreID)], ?DATA_CHUNK_SIZE),
	case IsComplete of
		true ->
			Packing = {replica_2_9, RewardAddr},
			StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
			prometheus_counter:inc(chunks_stored,
				[ar_storage_module:packing_label(Packing),
				ar_storage_module:label_by_id(StoreID)]),
			ar_sync_record:add_async(replica_2_9_entropy_with_chunk,
										PaddedEndOffset,
										StartOffset,
										ar_chunk_storage,
										StoreID),
			ar_sync_record:add_async(replica_2_9_entropy_with_chunk,
										PaddedEndOffset,
										StartOffset,
										{replica_2_9, RewardAddr},
										ar_data_sync,
										StoreID);
		false ->
			ok
	end.

delete_record(PaddedEndOffset, StoreID) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_entropy) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_entropy) should not be used.
	ID = ar_chunk_storage_replica_2_9_1_entropy,
	BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	ar_sync_record:delete(BucketStart + ?DATA_CHUNK_SIZE, BucketStart, ID, StoreID).

generate_missing_entropy(PaddedEndOffset, RewardAddr) ->
	Entropies = ar_entropy_gen:generate_entropies(RewardAddr, PaddedEndOffset),
	case Entropies of
		{error, Reason} ->
			{error, Reason};
		_ ->
			EntropyIndex = ar_replica_2_9:get_slice_index(PaddedEndOffset),
			take_combined_entropy_by_index(Entropies, EntropyIndex)
	end.

do_store_entropy(_Entropies,
			BucketEndOffset,
			_RangeStart,
			RangeEnd,
			_Keys,
			_RewardAddr,
			_State)
		when BucketEndOffset > RangeEnd ->
	%% The amount of entropy generated per partition is slightly more than the amount needed.
	%% So at the end of a partition we will have finished processing chunks, but still have
	%% some entropy left. In this case we stop the recursion early and wait for the writes
	%% to complete.
	ok;
do_store_entropy(Entropies,
			BucketEndOffset,
			RangeStart,
			RangeEnd,
			Keys,
			RewardAddr,
			State) ->
	case take_and_combine_entropy_slices(Entropies) of
		{<<>>, []} ->
			%% We've finished processing all the entropies, wait for the writes to complete.
			ok;
		{ChunkEntropy, Rest} ->
			%% Sanity checks
			true =
				ar_replica_2_9:get_entropy_partition(BucketEndOffset)
				== ar_replica_2_9:get_entropy_partition(RangeEnd),
			sanity_check_replica_2_9_entropy_keys(BucketEndOffset, RewardAddr, Keys),
			%% End sanity checks

			case BucketEndOffset > RangeStart of
				true ->
					#state{ store_id = StoreID } = State,
					record_entropy(
						ChunkEntropy,
						BucketEndOffset,
						StoreID,
						RewardAddr);
				false ->
					%% Don't write entropy before the start of the storage module.
					ok
			end,

			%% Jump to the next sector covered by this entropy.
			BucketEndOffset2 = shift_entropy_offset(BucketEndOffset, 1),
			do_store_entropy(
				Rest,
				BucketEndOffset2,
				RangeStart,
				RangeEnd,
				Keys,
				RewardAddr,
				State)
	end.

record_chunk(
		PaddedEndOffset, Chunk, StoreID,
		StoreIDLabel, PackingLabel, FileIndex, {IsPrepared, RewardAddr}) ->
	%% Sanity checks
	true = PaddedEndOffset == ar_block:get_chunk_padded_offset(PaddedEndOffset),
	%% End sanity checks

	StartOffset = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	{_ChunkFileStart, Filepath, _Position, _ChunkOffset} =
		ar_chunk_storage:locate_chunk_on_disk(PaddedEndOffset, StoreID),
	acquire_semaphore(Filepath),
	CheckIsChunkStoredAlready =
		ar_sync_record:is_recorded(PaddedEndOffset, ar_chunk_storage, StoreID),
	CheckIsEntropyRecorded =
		case CheckIsChunkStoredAlready of
			true ->
				{error, already_stored};
			false ->
				is_entropy_recorded(PaddedEndOffset, StoreID)
		end,
	ReadEntropy =
		case CheckIsEntropyRecorded of
			{error, _} = Error ->
				Error;
			false ->
				case IsPrepared of
					false ->
						no_entropy_yet;
					true ->
						missing_entropy
				end;
			true ->
				ar_chunk_storage:get(StartOffset, StartOffset, StoreID)
		end,
	RecordChunk = case ReadEntropy of
		{error, _} = Error2 ->
			Error2;
		not_found ->
			delete_record(PaddedEndOffset, StoreID),
			{error, not_prepared_yet};
		missing_entropy ->
			Packing = {replica_2_9, RewardAddr},
			?LOG_WARNING([{event, missing_entropy}, {padded_end_offset, PaddedEndOffset},
				{store_id, StoreID}, {packing, ar_serialize:encode_packing(Packing, true)}]),
			Entropy = generate_missing_entropy(PaddedEndOffset, RewardAddr),
			case Entropy of
				{error, Reason} ->
					{error, Reason};
				_ ->
					PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
					ar_chunk_storage:record_chunk(
						PaddedEndOffset, PackedChunk, Packing, StoreID,
						StoreIDLabel, PackingLabel, FileIndex)
			end;
		no_entropy_yet ->
			ar_chunk_storage:record_chunk(
				PaddedEndOffset, Chunk, unpacked_padded, StoreID,
				StoreIDLabel, PackingLabel, FileIndex);
		{_EndOffset, Entropy} ->
			Packing = {replica_2_9, RewardAddr},
			PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
			ar_chunk_storage:record_chunk(
				PaddedEndOffset, PackedChunk, Packing, StoreID,
				StoreIDLabel, PackingLabel, FileIndex)
	end,
	release_semaphore(Filepath),
	RecordChunk.

%% @doc Return the byte (>= ChunkStartOffset, < ChunkEndOffset)
%% that necessarily belongs to the chunk stored
%% in the bucket with the given bucket end offset.
get_chunk_byte_from_bucket_end(BucketEndOffset) ->
	case BucketEndOffset >= ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			RelativeBucketEndOffset = BucketEndOffset - ?STRICT_DATA_SPLIT_THRESHOLD,
			case RelativeBucketEndOffset rem ?DATA_CHUNK_SIZE of
				0 ->
					%% The chunk beginning at this offset is the rightmost possible
					%% chunk that will be routed to this bucket.
					%% The chunk ending at this offset plus one is the leftmost possible
					%% chunk routed to this bucket.
					BucketEndOffset - ?DATA_CHUNK_SIZE;
				_ ->
					?STRICT_DATA_SPLIT_THRESHOLD
							+ ar_util:floor_int(RelativeBucketEndOffset, ?DATA_CHUNK_SIZE)
			end;
		false ->
			BucketEndOffset - 1
	end.

record_entropy(ChunkEntropy, BucketEndOffset, StoreID, RewardAddr) ->
	%% Sanity checks
	true = byte_size(ChunkEntropy) == ?DATA_CHUNK_SIZE,
	%% End sanity checks

	Byte = get_chunk_byte_from_bucket_end(BucketEndOffset),
	CheckUnpackedChunkRecorded = ar_sync_record:get_interval(
		Byte + 1, ar_chunk_storage:sync_record_id(unpacked_padded), StoreID),

	{IsUnpackedChunkRecorded, EndOffset} =
		case CheckUnpackedChunkRecorded of
			not_found ->
				{false, BucketEndOffset};
			{_IntervalEnd, IntervalStart} ->
				EndOffset2 = IntervalStart
					+ ar_util:floor_int(Byte - IntervalStart, ?DATA_CHUNK_SIZE)
					+ ?DATA_CHUNK_SIZE,
				case ar_chunk_storage:get_chunk_bucket_end(EndOffset2) of
					BucketEndOffset ->
						{true, EndOffset2};
					_ ->
						%% This chunk is from a different bucket. It may happen near the
						%% strict data split threshold where there is no single byte
						%% unambiguosly determining the bucket the chunk will be routed to.
						?LOG_INFO([{event, record_entropy_read_chunk_from_another_bucket},
								{bucket_end_offset, BucketEndOffset},
								{chunk_end_offset, EndOffset2}]),
						{false, BucketEndOffset}
				end
		end,

	{ChunkFileStart, Filepath, _Position, _ChunkOffset} =
		ar_chunk_storage:locate_chunk_on_disk(EndOffset, StoreID),

	%% We allow generating and filling it the 2.9 entropy and storing unpacked chunks (to
	%% be enciphered later) asynchronously. Whatever comes first, is stored.
	%% If the other counterpart is stored already, we read it, encipher and store the
	%% packed chunk.
	acquire_semaphore(Filepath),

	Chunk = case IsUnpackedChunkRecorded of
		true ->
			StartOffset = EndOffset - ?DATA_CHUNK_SIZE,
			case ar_chunk_storage:get(Byte, StartOffset, StoreID) of
				not_found ->
					{error, not_found};
				{error, _} = Error ->
					Error;
				{_, UnpackedChunk} ->
					ar_sync_record:delete(EndOffset, StartOffset, ar_data_sync, StoreID),
					ar_packing_server:encipher_replica_2_9_chunk(UnpackedChunk, ChunkEntropy)
			end;
		false ->
			%% The entropy for the first sub-chunk of the chunk.
			%% The zero-offset does not have a real meaning, it is set
			%% to make sure we pass offset validation on read.
			ChunkEntropy
	end,

	Result = case Chunk of
		{error, _} = Error2 ->
			Error2;
		_ ->
			WriteChunkResult = ar_chunk_storage:write_chunk(EndOffset, Chunk, #{}, StoreID),
			case WriteChunkResult of
				{ok, Filepath} ->
					ets:insert(chunk_storage_file_index,
						{{ChunkFileStart, StoreID}, Filepath}),
					update_sync_records(
						IsUnpackedChunkRecorded, EndOffset, StoreID, RewardAddr);
				Error2 ->
					Error2
			end
	end,

	case Result of
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_store_replica_2_9_chunk_entropy},
							{filepath, Filepath},
							{byte, Byte},
							{padded_end_offset, EndOffset},
							{bucket_end_offset, BucketEndOffset},
							{store_id, StoreID},
							{reason, io_lib:format("~p", [Reason])}]);
		_ ->
			ok
	end,

	release_semaphore(Filepath).

%% @doc If we are not at the beginning of the entropy, shift the offset to
%% the left. store_entropy will traverse the entire 2.9 partition shifting
%% the offset by sector size.
reset_entropy_offset(BucketEndOffset) ->
	%% Sanity checks
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(BucketEndOffset),
	%% End sanity checks
	SliceIndex = ar_replica_2_9:get_slice_index(BucketEndOffset),
	shift_entropy_offset(BucketEndOffset, -SliceIndex).
	
%% @doc Take the first slice of each entropy and combine into a single binary. This binary
%% can be used to encipher a single chunk.
-spec take_and_combine_entropy_slices(Entropies :: [binary()]) ->
										 {ChunkEntropy :: binary(),
										  RemainingSlicesOfEachEntropy :: [binary()]}.
take_and_combine_entropy_slices(Entropies) ->
	true = ?COMPOSITE_PACKING_SUB_CHUNK_COUNT == length(Entropies),
	take_and_combine_entropy_slices(Entropies, [], []).

take_and_combine_entropy_slices([], Acc, RestAcc) ->
	{iolist_to_binary(Acc), lists:reverse(RestAcc)};
take_and_combine_entropy_slices([<<>> | Entropies], _Acc, _RestAcc) ->
	true = lists:all(fun(Entropy) -> Entropy == <<>> end, Entropies),
	{<<>>, []};
take_and_combine_entropy_slices([<<EntropySlice:?COMPOSITE_PACKING_SUB_CHUNK_SIZE/binary,
								   Rest/binary>>
								 | Entropies],
								Acc,
								RestAcc) ->
	take_and_combine_entropy_slices(Entropies, [Acc, EntropySlice], [Rest | RestAcc]).

take_combined_entropy_by_index(Entropies, Index) ->
	take_combined_entropy_by_index(Entropies, Index, []).

take_combined_entropy_by_index([], _Index, Acc) ->
	iolist_to_binary(Acc);
take_combined_entropy_by_index([Entropy | Entropies], Index, Acc) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	take_combined_entropy_by_index(
		Entropies,
		Index,
		[Acc, binary:part(Entropy, Index * SubChunkSize, SubChunkSize)]).

sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, Keys) ->
	sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, 0, Keys).

sanity_check_replica_2_9_entropy_keys(
		_PaddedEndOffset, _RewardAddr, _SubChunkStartOffset, []) ->
	ok;
sanity_check_replica_2_9_entropy_keys(
		PaddedEndOffset, RewardAddr, SubChunkStartOffset, [Key | Keys]) ->
 	Key = ar_replica_2_9:get_entropy_key(RewardAddr, PaddedEndOffset, SubChunkStartOffset),
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	sanity_check_replica_2_9_entropy_keys(PaddedEndOffset,
										RewardAddr,
										SubChunkStartOffset + SubChunkSize,
										Keys).

shift_entropy_offset(Offset, SectorCount) ->
	SectorSize = ar_replica_2_9:get_sector_size(),
	ar_chunk_storage:get_chunk_bucket_end(Offset + SectorSize * SectorCount).

acquire_semaphore(Filepath) ->
	case ets:insert_new(ar_entropy_storage, {{semaphore, Filepath}}) of
		false ->
			?LOG_DEBUG([
				{event, details_store_chunk}, {section, waiting_on_semaphore}, {filepath, Filepath}]),
			timer:sleep(20),
			acquire_semaphore(Filepath);
		true ->
			ok
	end.

release_semaphore(Filepath) ->
	ets:delete(ar_entropy_storage, {semaphore, Filepath}).

%%%===================================================================
%%% Tests.
%%%===================================================================

reset_entropy_offset_test() ->
	?assertEqual(786432, ar_replica_2_9:get_sector_size()),
	?assertEqual(786432, ?STRICT_DATA_SPLIT_THRESHOLD),
	%% Slice index of 0 means no shift (all offsets at or below the strict data split
	%% threshold are not padded)
	assert_reset_entropy_offset(262144, 0),
	assert_reset_entropy_offset(262144, 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE - 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE + 1),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 2),
	assert_reset_entropy_offset(786432, ?DATA_CHUNK_SIZE * 3),
	%% Slice index of 1 shift down a sector
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 3 + 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 4 - 1),
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 4),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 4 + 1),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 5 - 1),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 5),
	assert_reset_entropy_offset(786432, ?DATA_CHUNK_SIZE * 5 + 1),
	assert_reset_entropy_offset(786432, ?DATA_CHUNK_SIZE * 6),
	%% Slice index of 2 shift down 2 sectors
	assert_reset_entropy_offset(262144, ?DATA_CHUNK_SIZE * 7),
	assert_reset_entropy_offset(524288, ?DATA_CHUNK_SIZE * 8),
	%% First chunk of new partition, restart slice index at 0, so no shift
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 8 + 1),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 9 - 1),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 9),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 9 + 1),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 10),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 11, ?DATA_CHUNK_SIZE * 11),
	%% Slice index of 1 shift down a sector
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 12),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 13),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 11, ?DATA_CHUNK_SIZE * 14),
	%% Slice index of 2 shift down 2 sectors
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 9, ?DATA_CHUNK_SIZE * 15),
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 10, ?DATA_CHUNK_SIZE * 16),
	%% First chunk of new partition, restart slice index at 0, so no shift
	assert_reset_entropy_offset(?DATA_CHUNK_SIZE * 17, ?DATA_CHUNK_SIZE * 17).
assert_reset_entropy_offset(ExpectedShiftedOffset, Offset) ->
	BucketEndOffset = ar_chunk_storage:get_chunk_bucket_end(Offset),
	?assertEqual(
		ExpectedShiftedOffset,
		reset_entropy_offset(BucketEndOffset),
		iolist_to_binary(io_lib:format("Offset: ~p, BucketEndOffset: ~p",
			[Offset, BucketEndOffset]))
	).

replica_2_9_test_() ->
	{timeout, 20, fun test_replica_2_9/0}.

test_replica_2_9() ->
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	Packing = {replica_2_9, RewardAddr},
	StorageModules = [
			{?PARTITION_SIZE, 0, Packing},
			{?PARTITION_SIZE, 1, Packing}
	],
	{ok, Config} = application:get_env(arweave, config),
	try
		ar_test_node:start(#{ reward_addr => RewardAddr, storage_modules => StorageModules }),
		StoreID1 = ar_storage_module:id(lists:nth(1, StorageModules)),
		StoreID2 = ar_storage_module:id(lists:nth(2, StorageModules)),
		C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		%% The replica_2_9 storage does not support updates and three chunks are written
		%% into the first partition when the test node is launched.
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),

		%% Store the new chunk.
		?assertEqual({ok, {replica_2_9, RewardAddr}},
				ar_chunk_storage:put(4 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		{ok, P1, _Entropy} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 4 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P1, 4 * ?DATA_CHUNK_SIZE, StoreID1),

		assert_get(not_found, 8 * ?DATA_CHUNK_SIZE, StoreID1),
		?assertEqual({ok, {replica_2_9, RewardAddr}},
				ar_chunk_storage:put(8 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		{ok, P2, _} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 8 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P2, 8 * ?DATA_CHUNK_SIZE, StoreID1),

		%% Store chunks in the second partition.
		?assertEqual({ok, {replica_2_9, RewardAddr}},
				ar_chunk_storage:put(12 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID2)),
		{ok, P3, Entropy3} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 12 * ?DATA_CHUNK_SIZE, C1),

		assert_get(P3, 12 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertEqual({ok, {replica_2_9, RewardAddr}},
				ar_chunk_storage:put(15 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID2)),
		{ok, P4, Entropy4} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 15 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P4, 15 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertNotEqual(P3, P4),
		?assertNotEqual(Entropy3, Entropy4),

		?assertEqual({ok, {replica_2_9, RewardAddr}},
				ar_chunk_storage:put(16 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID2)),
		{ok, P5, Entropy5} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 16 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P5, 16 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertNotEqual(Entropy4, Entropy5)
	after
		ok = application:set_env(arweave, config, Config)
	end.
	

assert_get(Expected, Offset) ->
	assert_get(Expected, Offset, "default").

assert_get(Expected, Offset, StoreID) ->
	ExpectedResult =
		case Expected of
			not_found ->
				not_found;
			_ ->
				{Offset, Expected}
		end,
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - 1, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - 2, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 1, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 2, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 + 1, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 - 1, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 3, StoreID)).