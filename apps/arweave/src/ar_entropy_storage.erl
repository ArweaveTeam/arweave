-module(ar_entropy_storage).

-behaviour(gen_server).

-export([name/1, acquire_semaphore/1, release_semaphore/1, is_ready/1,
	sync_record_id/0, is_entropy_recorded/3, get_next_unsynced_interval/3,
	add_record/3, add_record_async/4, delete_record/2, delete_record/3,
	store_entropy_footprint/6, store_entropy/4, record_chunk/5]).

-export([start_link/2, init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").

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
	list_to_atom("ar_entropy_storage_" ++ ar_storage_module:label(StoreID)).

init(StoreID) ->
	?LOG_INFO([{event, ar_entropy_storage_init}, {name, name(StoreID)}, {store_id, StoreID}]),
	{ok, #state{ store_id = StoreID }}.

sync_record_id() ->
	ar_chunk_storage_replica_2_9_5_entropy.

%% @doc Write all of the entropies in a full 256 MiB entropy footprint to disk.
-spec store_entropy_footprint(	
	StoreID :: ar_storage_module:store_id(),
	Entropies :: [binary()],
	EntropyOffsets :: [non_neg_integer()],
	RangeStart :: non_neg_integer(),
	Keys :: [binary()],
	RewardAddr :: ar_wallet:address()) -> ok.
store_entropy_footprint(
	StoreID, Entropies, EntropyOffsets, RangeStart, Keys, RewardAddr) ->
	gen_server:cast(name(StoreID), {store_entropy_footprint,
		Entropies, EntropyOffsets, RangeStart, Keys, RewardAddr}).

store_entropy(ChunkEntropy, BucketEndOffset, StoreID, RewardAddr) ->
	case catch gen_server:call(
			name(StoreID), 
			{store_entropy, ChunkEntropy, BucketEndOffset, StoreID, RewardAddr},
			?DEFAULT_CALL_TIMEOUT) of
		{'EXIT', {Reason, {gen_server, call, _}}} ->
			?LOG_WARNING([{event, store_entropy}, {module, ?MODULE},
				{name, name(StoreID)}, {store_id, StoreID},
				{bucket_end_offset, BucketEndOffset}, {reason, Reason}]),
			false;
		Reply ->
			Reply
	end.

is_ready(StoreID) ->
	case catch gen_server:call(name(StoreID), is_ready, ?DEFAULT_CALL_TIMEOUT) of
		{'EXIT', {Reason, {gen_server, call, _}}} ->
			?LOG_WARNING([{event, is_ready_error}, {module, ?MODULE},
				{name, name(StoreID)}, {store_id, StoreID}, {reason, Reason}]),
			false;
		Reply ->
			Reply
	end.

handle_cast({store_entropy_footprint,
		Entropies, EntropyOffsets, RangeStart, Keys, RewardAddr}, State) ->
	#state{ store_id = StoreID } = State,
	ar_entropy_gen:map_entropies(
		Entropies,
		EntropyOffsets,
		RangeStart,
		Keys,
		RewardAddr,
		fun do_store_entropy/5,
		[StoreID],
		ok),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(is_ready, _From, State) ->
	{reply, true, State};


handle_call({store_entropy, ChunkEntropy, BucketEndOffset, StoreID, RewardAddr},
		_From, State) ->
	#state{ store_id = StoreID } = State,
	do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID),
	{reply, ok, State};

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
is_entropy_recorded(PaddedEndOffset, {replica_2_9, _} = Packing, StoreID) ->
	ChunkBucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	IsRecorded = ar_sync_record:is_recorded(
		ChunkBucketStart + 1, Packing, sync_record_id(), StoreID),
	case IsRecorded of
		false ->
			%% Included for backwards compatibility with entropy written prior to 2.9.5.
			ar_sync_record:is_recorded(
				ChunkBucketStart + 1, ar_chunk_storage_replica_2_9_1_entropy, StoreID);
		_ ->
			true
	end;
is_entropy_recorded(_PaddedEndOffset, _Packing, _StoreID) ->
	false.

get_next_unsynced_interval(Offset, Packing, StoreID) ->
	case ar_sync_record:get_next_unsynced_interval(
			Offset, infinity, Packing, sync_record_id(), StoreID) of
		not_found ->
			%% Included for backwards compatibility with entropy written prior to 2.9.5.
			case ar_sync_record:get_next_unsynced_interval(
					Offset, infinity, ar_chunk_storage_replica_2_9_1_entropy, StoreID) of
				not_found ->
					not_found;
				Interval ->
					Interval
			end;
		Interval ->
			Interval
	end.

update_sync_records(IsComplete, PaddedEndOffset, StoreID, RewardAddr) ->
	BucketEnd = ar_chunk_storage:get_chunk_bucket_end(PaddedEndOffset),
	add_record_async(replica_2_9_entropy, BucketEnd, {replica_2_9, RewardAddr}, StoreID),
	prometheus_counter:inc(replica_2_9_entropy_stored,
		[ar_storage_module:label(StoreID)], ?DATA_CHUNK_SIZE),
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	case IsComplete of
		true ->
			Packing = {replica_2_9, RewardAddr},
			
			prometheus_counter:inc(chunks_stored,
				[ar_storage_module:packing_label(Packing),
				ar_storage_module:label(StoreID)]),
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

add_record(BucketEndOffset, {replica_2_9, _} = Packing, StoreID) ->
	BucketStartOffset = BucketEndOffset - ?DATA_CHUNK_SIZE,
	ar_sync_record:add(BucketEndOffset, BucketStartOffset, Packing, sync_record_id(), StoreID).

add_record_async(Event, BucketEndOffset, {replica_2_9, _} = Packing, StoreID) ->
	BucketStartOffset = BucketEndOffset - ?DATA_CHUNK_SIZE,
	ar_sync_record:add_async(Event,
		BucketEndOffset, BucketStartOffset, Packing, sync_record_id(), StoreID).

delete_record(PaddedEndOffset, StoreID) ->
	BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	delete_record(BucketStart + ?DATA_CHUNK_SIZE, BucketStart, StoreID).

delete_record(EndOffset, StartOffset, StoreID) ->
	case ar_sync_record:delete(EndOffset, StartOffset, sync_record_id(), StoreID) of
		ok ->
			%% Included for backwards compatibility with entropy written prior to 2.9.5.
			ar_sync_record:delete(
				EndOffset, StartOffset, ar_chunk_storage_replica_2_9_1_entropy, StoreID);
		Error ->
			Error
	end.
		

generate_missing_entropy(PaddedEndOffset, RewardAddr) ->
	Entropies = ar_entropy_gen:generate_entropies(RewardAddr, PaddedEndOffset),
	case Entropies of
		{error, Reason} ->
			{error, Reason};
		_ ->
			EntropyIndex = ar_replica_2_9:get_slice_index(PaddedEndOffset),
			take_combined_entropy_by_index(Entropies, EntropyIndex)
	end.

record_chunk(
		PaddedEndOffset, Chunk, StoreID, FileIndex, {IsPrepared, RewardAddr}) ->
	%% Sanity checks
	PaddedEndOffset = ar_block:get_chunk_padded_offset(PaddedEndOffset),
	%% End sanity checks

	Packing = {replica_2_9, RewardAddr},
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
				is_entropy_recorded(PaddedEndOffset, Packing, StoreID)
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
			?LOG_WARNING([{event, missing_entropy}, {padded_end_offset, PaddedEndOffset},
				{store_id, StoreID}, {packing, ar_serialize:encode_packing(Packing, true)}]),
			Entropy = generate_missing_entropy(PaddedEndOffset, RewardAddr),
			case Entropy of
				{error, Reason} ->
					{error, Reason};
				_ ->
					PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
					ar_chunk_storage:record_chunk(
						PaddedEndOffset, PackedChunk, Packing, StoreID, FileIndex)
			end;
		no_entropy_yet ->
			ar_chunk_storage:record_chunk(
				PaddedEndOffset, Chunk, unpacked_padded, StoreID,  FileIndex);
		{_EndOffset, Entropy} ->
			PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
			ar_chunk_storage:record_chunk(
				PaddedEndOffset, PackedChunk, Packing, StoreID, FileIndex)
	end,
	release_semaphore(Filepath),
	RecordChunk.

do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID, ok) ->
	do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID).

do_store_entropy(ChunkEntropy, BucketEndOffset, RewardAddr, StoreID) ->
	%% Sanity checks
	true = byte_size(ChunkEntropy) == ?DATA_CHUNK_SIZE,
	%% End sanity checks

	Byte = ar_chunk_storage:get_chunk_byte_from_bucket_end(BucketEndOffset),
	CheckUnpackedChunkRecorded = ar_sync_record:get_interval(
		Byte + 1, ar_chunk_storage:sync_record_id(unpacked_padded), StoreID),

	{IsUnpackedChunkRecorded, PaddedEndOffset} =
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
		ar_chunk_storage:locate_chunk_on_disk(PaddedEndOffset, StoreID),

	%% We allow generating and filling it the 2.9 entropy and storing unpacked chunks (to
	%% be enciphered later) asynchronously. Whatever comes first, is stored.
	%% If the other counterpart is stored already, we read it, encipher and store the
	%% packed chunk.
	acquire_semaphore(Filepath),

	Chunk = case IsUnpackedChunkRecorded of
		true ->
			StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
			case ar_chunk_storage:get(Byte, StartOffset, StoreID) of
				not_found ->
					{error, not_found};
				{error, _} = Error ->
					Error;
				{_, UnpackedChunk} ->
					ar_sync_record:delete(PaddedEndOffset, StartOffset, ar_data_sync, StoreID),
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
			WriteChunkResult = ar_chunk_storage:write_chunk(
				PaddedEndOffset, Chunk, #{}, StoreID),
			case WriteChunkResult of
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
			?LOG_ERROR([{event, failed_to_store_replica_2_9_chunk_entropy},
							{filepath, Filepath},
							{byte, Byte},
							{padded_end_offset, PaddedEndOffset},
							{bucket_end_offset, BucketEndOffset},
							{store_id, StoreID},
							{reason, io_lib:format("~p", [Reason])}]);
		_ ->
			ok
	end,

	release_semaphore(Filepath),
	ok.
	
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

replica_2_9_test_() ->
	{timeout, 60, fun test_replica_2_9/0}.

test_replica_2_9() ->
	case ar_block:strict_data_split_threshold() of
		786432 ->
			ok;
		_ ->
			throw(unexpected_strict_data_split_threshold)
	end,

	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	Packing = {replica_2_9, RewardAddr},
	StorageModules = [
			{ar_block:partition_size(), 0, Packing},
			{ar_block:partition_size(), 1, Packing}
	],
	try
		ar_test_node:start(#{ reward_addr => RewardAddr, storage_modules => StorageModules }),
		StoreID1 = ar_storage_module:id(lists:nth(1, StorageModules)),
		StoreID2 = ar_storage_module:id(lists:nth(2, StorageModules)),
		C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		%% ar_chunk_storage does not allow overwriting a chunk
		%% with an unpacked_padded chunk.
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		assert_get(C1, ?DATA_CHUNK_SIZE, StoreID1),
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		assert_get(C1, 2 * ?DATA_CHUNK_SIZE, StoreID1),
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
		assert_get(C1, 3 * ?DATA_CHUNK_SIZE, StoreID1),

		%% Store the new unpacked_padded chunk. Expect it to be enciphered with
		%% the its entropy.
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(4 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
		{ok, P1, _Entropy} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 4 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P1, 4 * ?DATA_CHUNK_SIZE, StoreID1),

		assert_get(not_found, 8 * ?DATA_CHUNK_SIZE, StoreID1),
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(8 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
		{ok, P2, _} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 8 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P2, 8 * ?DATA_CHUNK_SIZE, StoreID1),

		%% Store chunks in the second partition.
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(12 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2)),
		{ok, P3, Entropy3} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 12 * ?DATA_CHUNK_SIZE, C1),

		assert_get(P3, 12 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertEqual({ok, Packing},
				ar_chunk_storage:put(15 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2)),
		{ok, P4, Entropy4} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 15 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P4, 15 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertNotEqual(P3, P4),
		?assertNotEqual(Entropy3, Entropy4),

		?assertEqual({ok, Packing},
				ar_chunk_storage:put(16 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2)),
		{ok, P5, Entropy5} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 16 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P5, 16 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertNotEqual(Entropy4, Entropy5)
	after
		ok
	end.

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
