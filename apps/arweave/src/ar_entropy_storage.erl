-module(ar_entropy_storage).

-export([is_entropy_packing/1, acquire_semaphore/1, release_semaphore/1, is_recorded/2,
	is_sub_chunk_recorded/3, delete_record/2, generate_entropies/3, generate_missing_entropy/2,
	generate_entropy_keys/3, shift_entropy_offset/2, store_entropy/8, record_chunk/6]).

-include("../include/ar.hrl").

-include_lib("eunit/include/eunit.hrl").

-spec is_entropy_packing(ar_chunk_storage:packing()) -> boolean().
is_entropy_packing(unpacked_padded) ->
	true;
is_entropy_packing({replica_2_9, _}) ->
	true;
is_entropy_packing(_) ->
	false.

%% @doc Return true if the given sub-chunk bucket contains the 2.9 entropy.
is_sub_chunk_recorded(PaddedEndOffset, SubChunkBucketStartOffset, StoreID) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_entropy) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_entropy) should not be used.
	ID = ar_chunk_storage_replica_2_9_1_entropy,
	ChunkBucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	SubChunkBucketStart = ChunkBucketStart + SubChunkBucketStartOffset,
	ar_sync_record:is_recorded(SubChunkBucketStart + 1, ID, StoreID).

%% @doc Return true if the 2.9 entropy for every sub-chunk of the chunk with the
%% given offset (> start offset, =< end offset) is recorded.
%% We check every sub-chunk because the entropy is written on the sub-chunk level.
is_recorded(PaddedEndOffset, StoreID) ->
	ChunkBucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	is_recorded2(ChunkBucketStart,
									 ChunkBucketStart + ?DATA_CHUNK_SIZE,
									 StoreID).

is_recorded2(Cursor, BucketEnd, _StoreID) when Cursor >= BucketEnd ->
	true;
is_recorded2(Cursor, BucketEnd, StoreID) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_entropy) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_entropy) should not be used.
	ID = ar_chunk_storage_replica_2_9_1_entropy,
	case ar_sync_record:is_recorded(Cursor + 1, ID, StoreID) of
		false ->
			false;
		true ->
			SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
			is_recorded2(Cursor + SubChunkSize, BucketEnd, StoreID)
	end.

update_sync_records(IsComplete, PaddedEndOffset, StoreID, RewardAddr) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_entropy) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_entropy) should not be used.
	ID = ar_chunk_storage_replica_2_9_1_entropy,
	BucketEnd = ar_chunk_storage:get_chunk_bucket_end(PaddedEndOffset),
	BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	ar_sync_record:add_async(replica_2_9_entropy, BucketEnd, BucketStart, ID, StoreID),
	prometheus_counter:inc(replica_2_9_entropy_stored, [StoreID], ?DATA_CHUNK_SIZE),
	case IsComplete of
		true ->
			StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
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
	Entropies = generate_entropies(RewardAddr, PaddedEndOffset, 0),
	EntropyIndex = ar_replica_2_9:get_slice_index(PaddedEndOffset),
	take_combined_entropy_by_index(Entropies, EntropyIndex).

%% @doc Returns all the entropies needed to encipher the chunk at PaddedEndOffset.
%% ar_packing_server:get_replica_2_9_entropy/3 will query a cached entropy, or generate it
%% if it is not cached.
generate_entropies(_RewardAddr, _PaddedEndOffset, SubChunkStart)
	when SubChunkStart == ?DATA_CHUNK_SIZE ->
	[];
generate_entropies(RewardAddr, PaddedEndOffset, SubChunkStart) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	[ar_packing_server:get_replica_2_9_entropy(RewardAddr, PaddedEndOffset, SubChunkStart)
	 | generate_entropies(RewardAddr, PaddedEndOffset, SubChunkStart + SubChunkSize)].

generate_entropy_keys(_RewardAddr, _Offset, SubChunkStart)
	when SubChunkStart == ?DATA_CHUNK_SIZE ->
	[];
generate_entropy_keys(RewardAddr, Offset, SubChunkStart) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	[ar_replica_2_9:get_entropy_key(RewardAddr, Offset, SubChunkStart)
	 | generate_entropy_keys(RewardAddr, Offset, SubChunkStart + SubChunkSize)].

store_entropy(_Entropies,
			  PaddedEndOffset,
			  _SubChunkStartOffset,
			  RangeEnd,
			  _Keys,
			  _RewardAddr,
			  N,
			  WaitN)
	when PaddedEndOffset > RangeEnd ->
	%% The amount of entropy generated per partition is slightly more than the amount needed.
	%% So at the end of a partition we will have finished processing chunks, but still have
	%% some entropy left. In this case we stop the recursion early and wait for the writes
	%% to complete.
	wait_store_entropy_processes(WaitN),
	{ok, N};
store_entropy(Entropies,
			  PaddedEndOffset,
			  SubChunkStartOffset,
			  RangeEnd,
			  Keys,
			  RewardAddr,
			  N,
			  WaitN) ->
	case take_and_combine_entropy_slices(Entropies) of
		{<<>>, []} ->
			%% We've finished processing all the entropies, wait for the writes to complete.
			wait_store_entropy_processes(WaitN),
			{ok, N};
		{ChunkEntropy, Rest} ->
			true =
				ar_replica_2_9:get_entropy_partition(PaddedEndOffset)
				== ar_replica_2_9:get_entropy_partition(RangeEnd),
			sanity_check_replica_2_9_entropy_keys(PaddedEndOffset,
												  RewardAddr,
												  SubChunkStartOffset,
												  Keys),
			FindModules =
				case ar_storage_module:get_all_packed(PaddedEndOffset, {replica_2_9, RewardAddr}) of
					[] ->
						?LOG_WARNING([{event, failed_to_find_storage_modules_for_2_9_entropy},
									  {padded_end_offset, PaddedEndOffset}]),
						not_found;
					StoreIDs ->
						{ok, StoreIDs}
				end,
			case FindModules of
				not_found ->
					PaddedEndOffset2 = shift_entropy_offset(PaddedEndOffset, 1),
					store_entropy(Rest,
								  PaddedEndOffset2,
								  SubChunkStartOffset,
								  RangeEnd,
								  Keys,
								  RewardAddr,
								  N,
								  WaitN);
				{ok, StoreIDs2} ->
					From = self(),
					WaitN2 = lists:foldl(fun(StoreID2, WaitNAcc) ->
							spawn_link(fun() ->
									StartTime = erlang:monotonic_time(),

									record_entropy(ChunkEntropy,
													PaddedEndOffset,
													StoreID2,
													RewardAddr),

									EndTime = erlang:monotonic_time(),
									ElapsedTime =
										erlang:convert_time_unit(EndTime - StartTime,
																native,
																microsecond),
									%% bytes per second
									WriteRate =
										case ElapsedTime > 0 of
											true -> 1000000 * byte_size(ChunkEntropy) div ElapsedTime;
											false -> 0
										end,
									prometheus_gauge:set(replica_2_9_entropy_store_rate,
														[StoreID2],
														WriteRate),
									From ! {store_entropy_sub_chunk_written, WaitNAcc + 1}
								end),
							WaitNAcc + 1
						end,
						WaitN,
						StoreIDs2
					),
					PaddedEndOffset2 = shift_entropy_offset(PaddedEndOffset, 1),
					store_entropy(Rest,
								  PaddedEndOffset2,
								  SubChunkStartOffset,
								  RangeEnd,
								  Keys,
								  RewardAddr,
								  N + length(Keys),
								  WaitN2)
			end
	end.

record_chunk(PaddedEndOffset, Chunk, RewardAddr, StoreID, FileIndex, IsPrepared) ->
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	{_ChunkFileStart, Filepath, _Position, _ChunkOffset} =
		ar_chunk_storage:locate_chunk_on_disk(PaddedEndOffset, StoreID),
	acquire_semaphore(Filepath),
	CheckIsStoredAlready =
		ar_sync_record:is_recorded(PaddedEndOffset, ar_chunk_storage, StoreID),
	CheckIsEntropyRecorded =
		case CheckIsStoredAlready of
			true ->
				{error, already_stored};
			false ->
				is_recorded(PaddedEndOffset, StoreID)
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
	case ReadEntropy of
		{error, _} = Error2 ->
			?LOG_DEBUG([{event, details_failed_to_store_chunk},
				{context, error_recording_chunk_to_entropy_storage},
				{error, io_lib:format("~p", [Error2])},
				{padded_offset, PaddedEndOffset},
				{start_offset, StartOffset},
				{check_is_stored_already, CheckIsStoredAlready},
				{check_is_entropy_recorded, CheckIsEntropyRecorded},
				{store_id, StoreID},
				{filepath, Filepath}
			]),
			release_semaphore(Filepath),
			Error2;
		not_found ->
			release_semaphore(Filepath),
			{error, not_prepared_yet2};
		missing_entropy ->
			Packing = {replica_2_9, RewardAddr},
			Entropy = generate_missing_entropy(PaddedEndOffset, RewardAddr),
			PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
			Result = ar_chunk_storage:record_chunk(
				PaddedEndOffset, PackedChunk, Packing, StoreID, FileIndex),
			release_semaphore(Filepath),
			Result;
		no_entropy_yet ->
			Result = ar_chunk_storage:record_chunk(
				PaddedEndOffset, Chunk, unpacked_padded, StoreID, FileIndex),
			release_semaphore(Filepath),
			Result;
		{_EndOffset, Entropy} ->
			Packing = {replica_2_9, RewardAddr},
			release_semaphore(Filepath),
			PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
			ar_chunk_storage:record_chunk(
				PaddedEndOffset, PackedChunk, Packing, StoreID, FileIndex)
	end.

record_entropy(ChunkEntropy, PaddedEndOffset, StoreID, RewardAddr) ->

	true = byte_size(ChunkEntropy) == ?DATA_CHUNK_SIZE,

	IsUnpackedChunkRecorded = ar_sync_record:is_recorded(
		PaddedEndOffset, ar_chunk_storage:sync_record_id(unpacked_padded), StoreID),

	{ChunkFileStart, Filepath, _Position, _ChunkOffset} =
		ar_chunk_storage:locate_chunk_on_disk(PaddedEndOffset, StoreID),

	%% We allow generating and filling it the 2.9 entropy and storing unpacked chunks (to
	%% be enciphered later) asynchronously. Whatever comes first, is stored.
	%% If the other counterpart is stored already, we read it, encipher and store the
	%% packed chunk.
	acquire_semaphore(Filepath),

	Chunk = case IsUnpackedChunkRecorded of
		true ->
			StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
			case ar_chunk_storage:get(StartOffset, StartOffset, StoreID) of
				not_found ->
					?LOG_DEBUG([{event, details_failed_to_store_chunk},
						{context, unpacked_padded_chunk_not_found},
						{padded_offset, PaddedEndOffset},
						{start_offset, StartOffset},
						{store_id, StoreID},
						{filepath, Filepath},
						{is_unpacked_chunk_recorded, IsUnpackedChunkRecorded}
					]),
					{error, not_found};
				{error, _} = Error ->
					?LOG_DEBUG([{event, details_failed_to_store_chunk},
						{context, unpacked_padded_chunk_error},
						{error, io_lib:format("~p", [Error])},
						{padded_offset, PaddedEndOffset},
						{start_offset, StartOffset},
						{store_id, StoreID},
						{filepath, Filepath},
						{is_unpacked_chunk_recorded, IsUnpackedChunkRecorded}
					]),
					Error;
				{_, UnpackedChunk} ->
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
			case ar_chunk_storage:write_chunk(PaddedEndOffset, Chunk, #{}, StoreID) of
				{ok, Filepath} ->
					ets:insert(chunk_storage_file_index,
						{{ChunkFileStart, StoreID}, Filepath}),
					update_sync_records(
						IsUnpackedChunkRecorded, PaddedEndOffset, StoreID, RewardAddr);
				Error2 ->
					Error2
			end
	end,

	case Result of
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_store_replica_2_9_sub_chunk_entropy},
							{filepath, Filepath},
							{padded_end_offset, PaddedEndOffset},
							{store_id, StoreID},
							{reason, io_lib:format("~p", [Reason])}]);
		_ ->
			ok
	end,

	release_semaphore(Filepath).
	

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
	take_combined_entropy_by_index(Entropies,
								   Index,
								   [Acc, binary:part(Entropy, Index * SubChunkSize, SubChunkSize)]).

sanity_check_replica_2_9_entropy_keys(_PaddedEndOffset,
									  _RewardAddr,
									  _SubChunkStartOffset,
									  []) ->
	ok;
sanity_check_replica_2_9_entropy_keys(PaddedEndOffset,
									  RewardAddr,
									  SubChunkStartOffset,
									  [Key | Keys]) ->
	Key = ar_replica_2_9:get_entropy_key(RewardAddr, PaddedEndOffset, SubChunkStartOffset),
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	sanity_check_replica_2_9_entropy_keys(PaddedEndOffset,
										  RewardAddr,
										  SubChunkStartOffset + SubChunkSize,
										  Keys).

wait_store_entropy_processes(0) ->
	ok;
wait_store_entropy_processes(N) ->
	receive
		{store_entropy_sub_chunk_written, N} ->
			wait_store_entropy_processes(N - 1)
	end.

shift_entropy_offset(Offset, SectorCount) ->
	SectorSize = ar_replica_2_9:get_sector_size(),
	ar_chunk_storage:get_chunk_bucket_end(ar_block:get_chunk_padded_offset(Offset + SectorSize * SectorCount)).

acquire_semaphore(Filepath) ->
	case ets:insert_new(ar_entropy_storage, {{semaphore, Filepath}}) of
		false ->
			timer:sleep(20),
			acquire_semaphore(Filepath);
		true ->
			ok
	end.

release_semaphore(Filepath) ->
	ets:delete(ar_entropy_storage, {semaphore, Filepath}).
