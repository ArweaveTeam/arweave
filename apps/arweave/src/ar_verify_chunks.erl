-module(ar_verify_chunks).

-behaviour(gen_server).

-export([start_link/2, name/1]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_poa.hrl").
-include("ar_config.hrl").
-include("ar_consensus.hrl").
-include("ar_chunk_storage.hrl").
-include("ar_verify_chunks.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	mode = log :: purge | log,
	store_id :: string(),
	packing :: term(),
	start_offset :: non_neg_integer(),
	end_offset :: non_neg_integer(),
	cursor :: non_neg_integer(),
	ready = false :: boolean(),
	chunk_samples = ?SAMPLE_CHUNK_COUNT :: non_neg_integer(),
	verify_report = #verify_report{} :: #verify_report{}
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, StorageModule) ->
	gen_server:start_link({local, Name}, ?MODULE, StorageModule, []).

-spec name(binary()) -> atom().
name(StoreID) ->
	list_to_atom("ar_verify_chunks_" ++ ar_storage_module:label(StoreID)).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(StoreID) ->
	{ok, Config} = arweave_config:get_env(),
	?LOG_INFO([{event, verify_chunk_storage_started},
		{store_id, StoreID}, {mode, Config#config.verify},
		{chunk_samples, Config#config.verify_samples}]),
	{StartOffset, EndOffset} = ar_storage_module:get_range(StoreID),
	gen_server:cast(self(), sample),
	{ok, #state{
		mode = Config#config.verify,
		store_id = StoreID,
		packing = ar_storage_module:get_packing(StoreID),
		start_offset = StartOffset,
		end_offset = EndOffset,
		cursor = StartOffset,
		ready = is_ready(EndOffset),
		chunk_samples = Config#config.verify_samples,
		verify_report = #verify_report{
			start_time = erlang:system_time(millisecond)
		}
	}}.

handle_cast(sample, #state{ready = false, end_offset = EndOffset} = State) ->
	ar_util:cast_after(1000, self(), sample),
	{noreply, State#state{ready = is_ready(EndOffset)}};
handle_cast(sample,
		#state{cursor = Cursor, end_offset = EndOffset} = State) when Cursor >= EndOffset ->
	ar:console("Done!~n"),
	{noreply, State};
handle_cast(sample, State) ->
	%% Sample ?SAMPLE_CHUNK_COUNT random chunks, read them, unpack them and verify them.
	%% Report the collected statistics and continue with the "verify" procedure.
	io:format("Sampling ~p chunks from ~p to ~p~n",
		[State#state.chunk_samples, State#state.start_offset, State#state.end_offset]),
	MaxSamples = case State#state.chunk_samples of
		all ->
			(State#state.end_offset - State#state.start_offset) div ?DATA_CHUNK_SIZE;
		Count ->
			Count
	end,
	
	sample_chunks(
		State#state.chunk_samples, sets:new(), #sample_report{samples = MaxSamples}, State),
	gen_server:cast(self(), verify),
	{noreply, State};

handle_cast(verify, #state{ready = false, end_offset = EndOffset} = State) ->
	ar_util:cast_after(1000, self(), verify),
	{noreply, State#state{ready = is_ready(EndOffset)}};
handle_cast(verify,
		#state{cursor = Cursor, end_offset = EndOffset} = State) when Cursor >= EndOffset ->
	ar:console("Done!~n"),
	{noreply, State};
handle_cast(verify, State) ->
	State2 = verify(State),
	State3 = report_progress(State2),
	{noreply, State3};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(Call, From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}, {from, From}]),
	{reply, ok, State}.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

is_ready(EndOffset) ->
	case ar_block_index:get_last() of
		'$end_of_table' ->
			false;
		{WeaveSize, _Height, _H, _TXRoot}  ->
			WeaveSize >= EndOffset
	end.

%% @doc verify runs through a series of checks:
%% 1. All chunks covered by the ar_chunk_storage or ar_data_sync sync records exist.
%% 2. All chunks in the ar_data_sync sync record are also in the ar_chunk_storage sync record.
%% 3. All chunks have valid proofs.
%% 4. The ar_data_sync record has the expected packing format.
%% 5. All chunks in the ar_chunk_storage sync record are also in the ar_data_sync sync record.
%% 
%% For any chunk that fails one of the above checks: invalidate it so that it can be resynced.
verify(State) ->
	#state{store_id = StoreID} = State,
	{UnionInterval, Intervals} = query_intervals(State),
	State2 = verify_chunks(UnionInterval, Intervals, State),
	case State2#state.cursor >= State2#state.end_offset of
		true ->
			ar:console("Done verifying ~s!~n", [StoreID]),
			?LOG_INFO([{event, verify_chunk_storage_verify_chunks_done}, {store_id, StoreID}]);
		false ->
			gen_server:cast(self(), verify)
	end,
	State2.

verify_chunks(not_found, _Intervals, State) ->
	State#state{ cursor = State#state.end_offset };
verify_chunks({End, _Start}, _Intervals, #state{cursor = Cursor} = State)
		when Cursor >= End ->
	State;
verify_chunks({IntervalEnd, IntervalStart}, Intervals, State) ->
	#state{ cursor = Cursor } = State,
	Cursor2 = max(IntervalStart, Cursor),
	State3 = case verify_chunks_index(State#state{ cursor = Cursor2 }) of
		{error, State2} ->
			State2;
		{ChunkData, State2} ->
			verify_chunk(ChunkData, Intervals, State2)
	end,
	verify_chunks({IntervalEnd, IntervalStart}, Intervals, State3).

verify_chunks_index(State) ->
	#state{ cursor = Cursor, store_id = StoreID } = State,
	ChunkData = ar_data_sync:get_chunk_by_byte(Cursor, StoreID),
	verify_chunks_index2(ChunkData, State).

verify_chunks_index2({error, Reason}, State) ->
	#state{ cursor = Cursor } = State,
	NextCursor = ar_data_sync:advance_chunks_index_cursor(Cursor),
	State2 = invalidate_sync_record(
		chunks_index_error, Cursor, NextCursor, [{reason, Reason}], State),
	{error, State2#state{ cursor = NextCursor }};
verify_chunks_index2(
	{AbsoluteOffset, _, _, _, _, _, ChunkSize}, #state{cursor = Cursor} = State)
		when AbsoluteOffset - Cursor >= ChunkSize ->
	NextCursor = AbsoluteOffset - ChunkSize,
	State2 = invalidate_sync_record(chunks_index_gap, Cursor, NextCursor, [], State),
	{error, State2#state{ cursor = NextCursor + 1 }};
verify_chunks_index2(ChunkData, State) ->	
	{ChunkData, State}.

verify_chunk({ok, _Key, Metadata}, Intervals, State) ->
	{AbsoluteOffset, _ChunkDataKey, _TXRoot, _DataRoot, _TXPath,
		_TXRelativeOffset, _ChunkSize} = Metadata,
	{ChunkStorageInterval, _DataSyncInterval} = Intervals,

	PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
	
	State2 = verify_chunk_storage(PaddedOffset, Metadata, ChunkStorageInterval, State),

	State3 = verify_proof(Metadata, State2),

	State4 = verify_packing(Metadata, State3),

	State4#state{ cursor = PaddedOffset + 1 };
verify_chunk(_ChunkData, _Intervals, State) ->
	State.

verify_proof(Metadata, State) ->
	#state{ store_id = StoreID } = State,
	{AbsoluteOffset, ChunkDataKey, TXRoot, _DataRoot, TXPath,
		_TXRelativeOffset, ChunkSize} = Metadata,

	case ar_data_sync:read_data_path(ChunkDataKey, StoreID) of
		{ok, DataPath} ->
			ChunkMetadata = #chunk_metadata{
				tx_root = TXRoot,
				tx_path = TXPath,
				data_path = DataPath
			},
			ChunkProof = ar_poa:chunk_proof(ChunkMetadata, AbsoluteOffset - 1),
			case ar_poa:validate_paths(ChunkProof) of
				{false, _} ->
					invalidate_chunk(validate_paths_error, AbsoluteOffset, ChunkSize, State);
				{true, _} ->
					State
			end;
		Error ->
			invalidate_chunk(
				read_data_path_error, AbsoluteOffset, ChunkSize, [{reason, Error}], State)
	end.

%% @doc Verify that the ar_data_sync record is configured correctly - namely that it has
%% entry in the expected packing format. This also indirectly detects the case where an
%% interval exists in the ar_chunk_storage record, but not the ar_data_sync record.
verify_packing(Metadata, State) ->
	#state{packing = Packing, store_id = StoreID} = State,
	{AbsoluteOffset, _ChunkDataKey, _TXRoot, _DataRoot, _TXPath,
			_TXRelativeOffset, ChunkSize} = Metadata,
	PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
	StoredPackingCheck = ar_sync_record:is_recorded(AbsoluteOffset, ar_data_sync, StoreID),
	ExpectedPacking =
		case ar_chunk_storage:is_storage_supported(PaddedOffset, ChunkSize, Packing) of
			true ->
				Packing;
			false ->
				unpacked
		end,
	case {StoredPackingCheck, ExpectedPacking} of
		{{true, ExpectedPacking}, _} ->
			%% Chunk is recorded in ar_sync_record under the expected Packing.
			State;
		{{true, StoredPacking}, _} ->
			%% This check will invalidate chunks that are not packed to the expected
			%% *final* packing format. A storage module that is in the process of being
			%% packed to replica_2_8 may have chunks that are stored in the intermediate
			%% unpacked_padded format. This check will invalidate those chunks as well.
			%% Miners should make sure to only run `verify` in the `purge` mode after they
			%% have completed packing.
			invalidate_chunk(unexpected_packing, AbsoluteOffset, ChunkSize, 
				[{stored_packing, ar_serialize:encode_packing(StoredPacking, true)}], State);
		{Reply, _} ->
			invalidate_chunk(missing_packing_info, AbsoluteOffset, ChunkSize,
				[{packing_reply, io_lib:format("~p", [Reply])}], State)
	end.

%% @doc Verify that chunk exists on disk or in chunk_data_db. This also indirectly detects the
%% case where an interval exists in the ar_data_sync record, but not the ar_chunk_storage
%% record.
verify_chunk_storage(PaddedOffset, Metadata, {End, Start}, State)
		when PaddedOffset - ?DATA_CHUNK_SIZE >= Start andalso PaddedOffset =< End ->
	#state{store_id = StoreID} = State,
	{AbsoluteOffset, ChunkDataKey, _TXRoot, _DataRoot, _TXPath,
		_TXRelativeOffset, ChunkSize} = Metadata,
	{_ChunkFileStart, _Filepath, _Position, ExpectedChunkOffset} =
				ar_chunk_storage:locate_chunk_on_disk(PaddedOffset, StoreID),
	case ar_chunk_storage:read_offset(PaddedOffset, StoreID) of
		{ok, << ExpectedChunkOffset:?OFFSET_BIT_SIZE >>} ->
			State;
		{ok, << ActualChunkOffset:?OFFSET_BIT_SIZE >>} ->
			%% The chunk is recorded in the ar_chunk_storage sync record, but not stored.
			invalidate_chunk(
				invalid_chunk_offset, AbsoluteOffset, ChunkSize, [
					{expected_chunk_offset, ExpectedChunkOffset}, 
					{actual_chunk_offset, ActualChunkOffset}
				], State);
		Error ->
			IsChunkStoredInRocksDB =
				case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
					not_found ->
						false;
					{ok, Value} ->
						case binary_to_term(Value) of
							{_Chunk, _DataPath} ->
								true;
							_ ->
								false
						end
				end,
			invalidate_chunk(
				invalid_chunk_offset, AbsoluteOffset, ChunkSize, [
					{expected_chunk_offset, ExpectedChunkOffset}, 
					{error, Error},
					{is_chunk_stored_in_rocksdb, IsChunkStoredInRocksDB}
				], State)
	end;
verify_chunk_storage(PaddedOffset, Metadata, Interval, State) ->
	#state{ packing = Packing, store_id = StoreID } = State,
	{AbsoluteOffset, _ChunkDataKey, _TXRoot, _DataRoot, _TXPath,
		_TXRelativeOffset, ChunkSize} = Metadata,
	case ar_chunk_storage:is_storage_supported(PaddedOffset, ChunkSize, Packing) of
		true ->
			Logs = [
				{ar_data_sync,
					ar_sync_record:is_recorded(AbsoluteOffset, ar_data_sync, StoreID)},
				{ar_chunk_storage,
					ar_sync_record:is_recorded(AbsoluteOffset, ar_chunk_storage, StoreID)},
				{ar_chunk_storage_replica_2_9_1_unpacked,
					ar_sync_record:is_recorded(AbsoluteOffset, ar_chunk_storage_replica_2_9_1_unpacked, StoreID)},
				{unpacked_padded,
					ar_sync_record:is_recorded(AbsoluteOffset, unpacked_padded, StoreID)},
				{is_entropy_recorded, ar_entropy_storage:is_entropy_recorded(
					AbsoluteOffset, Packing, StoreID)},
				{is_blacklisted, ar_tx_blacklist:is_byte_blacklisted(AbsoluteOffset)},
				{interval, Interval},
				{padded_offset, PaddedOffset}
			],
			invalidate_chunk(chunk_storage_gap, AbsoluteOffset, ChunkSize, Logs, State);
		false ->
			verify_chunk_data(Metadata, State)
	end.

verify_chunk_data(Metadata, State) ->
	#state{ store_id = StoreID } = State,
	{AbsoluteOffset, ChunkDataKey, _TXRoot, _DataRoot, _TXPath,
		_TXRelativeOffset, ChunkSize} = Metadata,
	case ar_data_sync:get_chunk_data(ChunkDataKey, StoreID) of
		not_found ->
			invalidate_chunk(chunk_data_not_found, AbsoluteOffset, ChunkSize, [], State);
		{ok, Value} ->
			case binary_to_term(Value) of
				{_Chunk, _DataPath} ->
					State;
				_DataPath ->
					invalidate_chunk(
						chunk_data_no_chunk, AbsoluteOffset, ChunkSize, [], State)
			end;
		Error ->
			invalidate_chunk(
				chunk_data_error, AbsoluteOffset, ChunkSize, [{reason, Error}], State)
	end.

invalidate_chunk(Type, AbsoluteOffset, ChunkSize, State) ->
	invalidate_chunk(Type, AbsoluteOffset, ChunkSize, [], State).

invalidate_chunk(Type, AbsoluteOffset, ChunkSize, Logs, State) ->
	#state{ mode = Mode, store_id = StoreID } = State,
	case Mode of
		purge ->
			ar_data_sync:invalidate_bad_data_record(AbsoluteOffset, ChunkSize, StoreID, Type);
		log ->
			ok
	end,
	log_error(Type, AbsoluteOffset, ChunkSize, Logs, State).

invalidate_sync_record(Type, Cursor, NextCursor, Logs, State) ->
	#state{ mode = Mode, store_id = StoreID } = State,
	case Mode of
		purge ->
			ar_sync_record:delete(NextCursor, Cursor, ar_data_sync, StoreID);
		log ->
			ok
	end,
	Range = NextCursor - Cursor,
	log_error(Type, Cursor, Range, Logs, State).

log_error(Type, AbsoluteOffset, ChunkSize, Logs, State) ->
	#state{ 
		verify_report = Report, store_id = StoreID, cursor = Cursor, packing = Packing 
	} = State,

	LogMessage = [{event, verify_chunk_error},
		{type, Type}, {store_id, StoreID},
		{expected_packing, ar_serialize:encode_packing(Packing, true)},
		{absolute_end_offset, AbsoluteOffset}, {cursor, Cursor}, {chunk_size, ChunkSize}]
		++ Logs,
	?LOG_INFO(LogMessage),
	NewBytes = maps:get(Type, Report#verify_report.error_bytes, 0) + ChunkSize,
	NewChunks = maps:get(Type, Report#verify_report.error_chunks, 0) + 1,

	Report2 = Report#verify_report{
		total_error_bytes = Report#verify_report.total_error_bytes + ChunkSize,
		total_error_chunks = Report#verify_report.total_error_chunks + 1,
		error_bytes = maps:put(Type, NewBytes, Report#verify_report.error_bytes),
		error_chunks = maps:put(Type, NewChunks, Report#verify_report.error_chunks)
	},
	State#state{ verify_report = Report2 }.

%% @doc Returns 3 sets of intervals:
%% 1. ar_chunk_storage: should cover all chunks that have been stored on disk.
%% 2. ar_data_sync, Packing: should cover all chunks of the specified packing that have been
%%                           synced
%% 3. The union of the above two intervals.
%% 
%% We will use these intervals to determine errors in the node state (e.g. a chunk that
%% exists in ar_chunk_storage but not ar_data_sync - or vice versa).
query_intervals(State) ->
	#state{cursor = Cursor, store_id = StoreID} = State,
	ChunkStorageInterval = ar_sync_record:get_next_synced_interval(
		Cursor, infinity, ar_chunk_storage, StoreID),
	DataSyncInterval = ar_sync_record:get_next_synced_interval(
		Cursor, infinity, ar_data_sync, StoreID),
	{ChunkStorageInterval2, DataSyncInterval2} = align_intervals(
		Cursor, ChunkStorageInterval, DataSyncInterval),
	UnionInterval = union_intervals(ChunkStorageInterval2, DataSyncInterval2),
	{UnionInterval, {ChunkStorageInterval2, DataSyncInterval2}}.

align_intervals(_Cursor, not_found, not_found) ->
	{not_found, not_found};
align_intervals(Cursor, not_found, DataSyncInterval) ->
	{not_found, clamp_interval(Cursor, infinity, DataSyncInterval)};
align_intervals(Cursor, ChunkStorageInterval, not_found) ->
	{clamp_interval(Cursor, infinity, ChunkStorageInterval), not_found};
align_intervals(Cursor, ChunkStorageInterval, DataSyncInterval) ->
	{ChunkStorageEnd, _} = ChunkStorageInterval,
	{DataSyncEnd, _} = DataSyncInterval,

	{
		clamp_interval(Cursor, DataSyncEnd, ChunkStorageInterval),
		clamp_interval(Cursor, ChunkStorageEnd, DataSyncInterval)
	}.

union_intervals(not_found, not_found) ->
	not_found;
union_intervals(not_found, B) ->
	B;
union_intervals(A, not_found) ->
	A;
union_intervals({End1, Start1}, {End2, Start2}) ->
	{max(End1, End2), min(Start1, Start2)}.

clamp_interval(ClampMin, ClampMax, {End, Start}) ->
	check_interval({min(End, ClampMax), max(Start, ClampMin)}).

check_interval({End, Start}) when Start > End ->
	not_found;
check_interval(Interval) ->
	Interval.

report_progress(State) ->
	#state{ 
		store_id = StoreID, verify_report = Report, cursor = Cursor,
		start_offset = StartOffset, end_offset = EndOffset
	} = State,

	Status = case Cursor >= EndOffset of
		true -> done;
		false -> running
	end,

	BytesProcessed = Cursor - StartOffset,
	Progress = BytesProcessed * 100 div (EndOffset - StartOffset),
	Report2 = Report#verify_report{
		bytes_processed = BytesProcessed,
		progress = Progress,
		status = Status
	},
	ar_verify_chunks_reporter:update(StoreID, Report2),
	State#state{ verify_report = Report2 }.

%% Generate offset in the range [Start, End]
%% (i.e. offsets greater than or equal to Start and less than or equal to End)
%% Offsets are normalized to a bucket boundary such that if that bucket boundary has
%% been sampled before, it won't be sampled again.
generate_sample_offset(Start, End, SampledOffsets, Retry) when Retry > 0 ->
	Range = End - Start,
	Offset = Start + rand:uniform(Range),
	BucketStartOffset = ar_chunk_storage:get_chunk_bucket_start(Offset),
	SampleOffset = BucketStartOffset + 1,
	case sets:is_element(SampleOffset, SampledOffsets) of
		true ->
			generate_sample_offset(Start, End, SampledOffsets, Retry - 1);
		false ->
			SampleOffset
	end.

sample_chunks(0, _SampledOffsets, SampleReport, _State) ->
	SampleReport;
sample_chunks(all, _SampledOffsets, SampleReport, State) ->
	#state{ store_id = StoreID, start_offset = Start, end_offset = End } = State,
	SampleOffset =  ar_chunk_storage:get_chunk_bucket_start(Start) + 1,

	lists:foldl(
        fun(Offset, Report) ->
            {_IsRecorded, NewReport} = sample_offset(Offset, StoreID, Report),
            ar_verify_chunks_reporter:update(StoreID, NewReport),
            NewReport
        end,
        SampleReport,
        lists:seq(SampleOffset, End, ?DATA_CHUNK_SIZE)
    );
sample_chunks(Count, SampledOffsets, SampleReport, State) ->
	#state{ store_id = StoreID, start_offset = Start, end_offset = End } = State,

	SampleOffset = generate_sample_offset(Start+1, End, SampledOffsets, 100),
	SampledOffsets2 = sets:add_element(SampleOffset, SampledOffsets),

	{IsRecorded, SampleReport2} = sample_offset(SampleOffset, StoreID, SampleReport),
	case IsRecorded of
		true ->
			ar_verify_chunks_reporter:update(StoreID, SampleReport2),
			sample_chunks(Count - 1, SampledOffsets2, SampleReport2, State);
		false ->
			sample_chunks(Count, SampledOffsets2, SampleReport2, State)
	end.

sample_offset(Offset, StoreID, SampleReport) ->
	IsRecorded = case ar_sync_record:is_recorded(Offset, ar_data_sync, StoreID) of
		{true, _} ->
			true;
		true ->
			true;
		false ->
			false
	end,

	SampleReport2 = case IsRecorded of
		true ->
			case ar_data_sync:get_chunk(
				Offset, #{pack => true, packing => unpacked, origin => verify}) of
				{ok, _Proof} ->
					SampleReport#sample_report{
						total = SampleReport#sample_report.total + 1,
						success = SampleReport#sample_report.success + 1
					};
				{error, Reason} ->
					?LOG_INFO([{event, sample_chunk_error}, {offset, Offset}, {status, Reason}]),
					SampleReport#sample_report{
						total = SampleReport#sample_report.total + 1,
						failure = SampleReport#sample_report.failure + 1
					}
			end;
		false ->
			SampleReport
	end,
	{IsRecorded, SampleReport2}.

%%%===================================================================
%%% Tests.
%%%===================================================================

intervals_test_() ->
    [
        {timeout, 30, fun test_align_intervals/0},
		{timeout, 30, fun test_union_intervals/0}
	].

verify_chunk_storage_test_() ->
	[
		ar_test_node:test_with_mocked_functions(
			[{ar_chunk_storage, read_offset,
				fun(_Offset, _StoreID) -> {ok, << ?DATA_CHUNK_SIZE:24 >>} end}],
			fun test_verify_chunk_storage_in_interval/0),
		ar_test_node:test_with_mocked_functions(
			[{ar_chunk_storage, read_offset,
				fun(_Offset, _StoreID) -> {ok, << ?DATA_CHUNK_SIZE:24 >>} end}],
			fun test_verify_chunk_storage_should_store/0),
		ar_test_node:test_with_mocked_functions(
			[{ar_chunk_storage, read_offset,
				fun(_Offset, _StoreID) -> {ok, << ?DATA_CHUNK_SIZE:24 >>} end},
			{ar_data_sync, get_chunk_data,
				fun(_, _) -> {ok, term_to_binary({<<>>, <<>>})} end}],
			fun test_verify_chunk_storage_should_not_store/0)
	].

verify_proof_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_data_sync, read_data_path, fun(_, _) -> not_found end}],
			fun test_verify_proof_no_datapath/0
		),
		ar_test_node:test_with_mocked_functions([
			{ar_data_sync, read_data_path, fun(_, _) -> {ok, <<>>} end},
			{ar_poa, chunk_proof, fun(_, _) -> #chunk_proof{} end},
			{ar_poa, validate_paths, fun(_) -> {true, <<>>} end}
		],
			fun test_verify_proof_valid_paths/0
		),
		ar_test_node:test_with_mocked_functions([
			{ar_data_sync, read_data_path, fun(_, _) -> {ok, <<>>} end},
			{ar_poa, chunk_proof, fun(_, _) -> #chunk_proof{} end},
			{ar_poa, validate_paths, fun(_) -> {false, <<>>} end}
		],
			fun test_verify_proof_invalid_paths/0
		)
	].

verify_chunk_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_data_sync, read_data_path, fun(_, _) -> {ok, <<>>} end},
			{ar_poa, validate_paths, fun(_) -> {true, <<>>} end},
			{ar_poa, chunk_proof, fun(_, _) -> #chunk_proof{} end},
			{ar_chunk_storage, read_offset,
				fun(_Offset, _StoreID) -> {ok, << ?DATA_CHUNK_SIZE:24 >>} end},
			{ar_data_sync, get_chunk_data,
				fun(_, _) -> {ok, term_to_binary({<<>>, <<>>})} end}
		],
			fun test_verify_chunk/0
		)
	].

test_align_intervals() ->
	?assertEqual(
		{not_found, not_found},
		align_intervals(0, not_found, not_found)),
	?assertEqual(
		{{10, 5}, not_found},
		align_intervals(0, {10, 5}, not_found)),
	?assertEqual(
		{{10, 7}, not_found},
		align_intervals(7, {10, 5}, not_found)),
	?assertEqual(
		{not_found, not_found},
		align_intervals(12, {10, 5}, not_found)),
	?assertEqual(
		{not_found, {10, 5}},
		align_intervals(0, not_found, {10, 5})),
	?assertEqual(
		{not_found, {10, 7}},
		align_intervals(7, not_found, {10, 5})),
	?assertEqual(
		{not_found, not_found},
		align_intervals(12, not_found, {10, 5})),

	?assertEqual(
		{{9, 4}, {9, 5}},
		align_intervals(0, {9, 4}, {10, 5})),
	?assertEqual(
		{{9, 7}, {9, 7}},
		align_intervals(7, {9, 4}, {10, 5})),
	?assertEqual(
		{not_found, not_found},
		align_intervals(12, {9, 4}, {10, 5})),
	?assertEqual(
		{{9, 5}, {9, 4}},
		align_intervals(0, {10, 5}, {9, 4})),
	?assertEqual(
		{{9, 7}, {9, 7}},
		align_intervals(7, {10, 5}, {9, 4})),
	?assertEqual(
		{not_found, not_found},
		align_intervals(12, {10, 5}, {9, 4})),
	ok.
		
test_union_intervals() ->
	?assertEqual(
		not_found,
		union_intervals(not_found, not_found)),
	?assertEqual(
		{10, 5},
		union_intervals(not_found, {10, 5})),
	?assertEqual(
		{10, 5},
		union_intervals({10, 5}, not_found)),
	?assertEqual(
		{10, 3},
		union_intervals({10, 7}, {5, 3})),
	ok.


test_verify_chunk_storage_in_interval() ->
	?assertEqual(
		#state{ packing = unpacked },
		verify_chunk_storage(
			10*?DATA_CHUNK_SIZE,
			{10*?DATA_CHUNK_SIZE, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		#state{ packing = unpacked },
		verify_chunk_storage(
			6*?DATA_CHUNK_SIZE,
			{6*?DATA_CHUNK_SIZE - 1, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		#state{ packing = unpacked },
		verify_chunk_storage(
			20*?DATA_CHUNK_SIZE,
			{20*?DATA_CHUNK_SIZE - ?DATA_CHUNK_SIZE div 2, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	ok.

test_verify_chunk_storage_should_store() ->
	Addr = crypto:strong_rand_bytes(32),
	ExpectedState = #state{ 
		packing = unpacked,
		verify_report = #verify_report{
			total_error_bytes = ?DATA_CHUNK_SIZE,
			total_error_chunks = 1,
			error_bytes = #{chunk_storage_gap => ?DATA_CHUNK_SIZE},
			error_chunks = #{chunk_storage_gap => 1}
		} 
	},
	?assertEqual(
		ExpectedState,
		verify_chunk_storage(
			0,
			{0, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		ExpectedState,
		verify_chunk_storage(
			ar_block:strict_data_split_threshold() + 1,
			{ar_block:strict_data_split_threshold() + 1, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		#state{
			packing = {composite, Addr, 1},
			verify_report = #verify_report{
				total_error_bytes = ?DATA_CHUNK_SIZE div 2,
				total_error_chunks = 1,
				error_bytes = #{chunk_storage_gap => ?DATA_CHUNK_SIZE div 2},
				error_chunks = #{chunk_storage_gap => 1}
			} 
		},
		verify_chunk_storage(
			ar_block:strict_data_split_threshold() + 1,
			{ar_block:strict_data_split_threshold() + 1, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = {composite, Addr, 1} })),
	ok.

test_verify_chunk_storage_should_not_store() ->
	ExpectedState = #state{ 
		packing = unpacked
	},
	?assertEqual(
		ExpectedState,
		verify_chunk_storage(
			0,
			{0, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		ExpectedState,
		verify_chunk_storage(
			ar_block:strict_data_split_threshold() + 1,
			{ar_block:strict_data_split_threshold() + 1, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			{20*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	ok.

test_verify_proof_no_datapath() ->
	ExpectedState1 = #state{ 
		packing = unpacked,
		verify_report = #verify_report{
			total_error_bytes = ?DATA_CHUNK_SIZE,
			total_error_chunks = 1,
			error_bytes = #{read_data_path_error => ?DATA_CHUNK_SIZE},
			error_chunks = #{read_data_path_error => 1}
		} 
	},
	ExpectedState2 = #state{ 
		packing = unpacked,
		verify_report = #verify_report{
			total_error_bytes = ?DATA_CHUNK_SIZE div 2,
			total_error_chunks = 1,
			error_bytes = #{read_data_path_error => ?DATA_CHUNK_SIZE div 2},
			error_chunks = #{read_data_path_error => 1}
		} 
	},
	?assertEqual(
		ExpectedState1,
		verify_proof(
			{10, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		ExpectedState2,
		verify_proof(
			{10, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			#state{ packing = unpacked })),
	ok.

test_verify_proof_valid_paths() ->
	?assertEqual(
		#state{},
		verify_proof(
			{10, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE},
			#state{})),
	ok.
	
test_verify_proof_invalid_paths() ->
	ExpectedState1 = #state{ 
		packing = unpacked,
		verify_report = #verify_report{
			total_error_bytes = ?DATA_CHUNK_SIZE,
			total_error_chunks = 1,
			error_bytes = #{validate_paths_error => ?DATA_CHUNK_SIZE},
			error_chunks = #{validate_paths_error => 1}
		} 
	},
	ExpectedState2 = #state{ 
		packing = unpacked,
		verify_report = #verify_report{
			total_error_bytes = ?DATA_CHUNK_SIZE div 2,
			total_error_chunks = 1,
			error_bytes = #{validate_paths_error => ?DATA_CHUNK_SIZE div 2},
			error_chunks = #{validate_paths_error => 1}
		} 
	},
	?assertEqual(
		ExpectedState1,
		verify_proof(
			{10, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE},
			#state{ packing = unpacked })),
	?assertEqual(
		ExpectedState2,
		verify_proof(
			{10, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2},
			#state{ packing = unpacked })),
	ok.

test_verify_chunk() ->
	PreSplitOffset = ar_block:strict_data_split_threshold() - (?DATA_CHUNK_SIZE div 2),
	PostSplitOffset = ar_block:strict_data_split_threshold() + (?DATA_CHUNK_SIZE div 2),
	IntervalStart = ar_block:strict_data_split_threshold() - ?DATA_CHUNK_SIZE,
	IntervalEnd = ar_block:strict_data_split_threshold() + ?DATA_CHUNK_SIZE,
	Interval = {IntervalEnd, IntervalStart},
	?assertEqual(
		#state{ 
			cursor = PreSplitOffset + 1,
			packing = unpacked,
			verify_report = #verify_report{
				total_error_bytes = ?DATA_CHUNK_SIZE div 2,
				total_error_chunks = 1,
				error_bytes = #{missing_packing_info => ?DATA_CHUNK_SIZE div 2},
				error_chunks = #{missing_packing_info => 1}
			}
		},
		verify_chunk(
			{ok, <<>>, {PreSplitOffset, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2}},
			{Interval, not_found},
			#state{packing=unpacked})),
	?assertEqual(
		#state{ 
			cursor = ar_block:strict_data_split_threshold() + ?DATA_CHUNK_SIZE + 1,
			packing = unpacked,
			verify_report = #verify_report{
				total_error_bytes = ?DATA_CHUNK_SIZE div 2,
				total_error_chunks = 1,
				error_bytes = #{missing_packing_info => ?DATA_CHUNK_SIZE div 2},
				error_chunks = #{missing_packing_info => 1}
			}
		},
		verify_chunk(
			{ok, <<>>, {PostSplitOffset, <<>>, <<>>, <<>>, <<>>, <<>>, ?DATA_CHUNK_SIZE div 2}},
			{Interval, not_found},
			#state{packing=unpacked})),
	ExpectedState = #state{ 
		cursor = 33554432, %% = 2 * 2^24. From ar_data_sync:advance_chunks_index_cursor/1
		packing = unpacked,
		verify_report = #verify_report{
			total_error_bytes = 33554432,
			total_error_chunks = 1,
			error_bytes = #{chunks_index_error => 33554432},
			error_chunks = #{chunks_index_error => 1}
		}
	},
	?assertEqual(
		{error, ExpectedState},
		verify_chunks_index2(
			{error, some_error},
			#state{ cursor = 0, packing = unpacked })),
	ok.

%% Verify that generate_sample_offsets/3 samples without replacement.
sample_offsets_loop(Start, End, Count) ->
    %% Compute the number of available unique candidates.
    Candidates = lists:seq(Start + 1, End, ?DATA_CHUNK_SIZE),
    ActualCount = erlang:min(Count, length(Candidates)),
    sample_offsets_loop(Start, End, ActualCount, sets:new()).

sample_offsets_loop(_Start, _End, 0, _SampledSet) ->
    [];
sample_offsets_loop(Start, End, Count, SampledSet) ->
    Offset = generate_sample_offset(Start, End, SampledSet, 100),
    NewSet = sets:add_element(Offset, SampledSet),
    [Offset | sample_offsets_loop(Start, End, Count - 1, NewSet)].

sample_offsets_without_replacement_test() ->
    ChunkSize = ?DATA_CHUNK_SIZE,
    Count = 5,
    %% Use the helper function to generate a list of offsets.
    Offsets = sample_offsets_loop(ChunkSize * 10, ChunkSize * 1000, Count),
    %% Check that exactly Count unique offsets are produced.
    ?assertEqual(Count, length(Offsets)),
    %% For every pair, ensure the absolute difference is at least ?DATA_CHUNK_SIZE.
    lists:foreach(fun(A) ->
        lists:foreach(fun(B) ->
            case {A == B, abs(A - B) < ?DATA_CHUNK_SIZE} of
                {true, _} -> ok;
                {false, true} -> ?assert(false);
                _ -> ok
            end
        end, Offsets)
    end, Offsets),
    %% When the available candidates are fewer than Count,
    %% only one unique offset should be returned.
    Offsets2 = sample_offsets_loop(0, ChunkSize, Count),
    ?assertEqual(1, length(Offsets2)).

%% Verify sample_random_chunks/4 aggregates outcomes correctly.
%%
%% We mock ar_data_sync:get_chunk/2 such that:
%%   - The first call returns {error, chunk_not_found},
%%   - The second call returns {ok, <<"valid_proof">>},
%%   - The third call returns {error, invalid_chunk}.
%% Note: Using atoms for partition borders triggers the fallback in generate_sample_offsets/3.
sample_random_chunks_test_() ->
	[
		ar_test_node:test_with_mocked_functions(
			[{ar_data_sync, get_chunk, fun(_Offset, _Opts) ->
				%% Use process dictionary to simulate sequential responses.
				Counter = case erlang:get(sample_counter) of
					undefined -> 0;
					C -> C
				end,
				erlang:put(sample_counter, Counter + 1),
				case Counter of
					0 -> {error, chunk_not_found};
					1 -> {ok, <<"valid_proof">>};
					2 -> {error, invalid_chunk}
				end
			end},
			{ar_sync_record, is_recorded,
				fun(_, _, _) -> true end}
			],
			fun test_sample_random_chunks/0)
	].

test_sample_random_chunks() ->
	%% Initialize counter.
	erlang:put(sample_counter, 0),
	State = #state{
		packing = unpacked,
		start_offset = 0,
		end_offset = ?DATA_CHUNK_SIZE * 10 ,
		store_id = "test"
	},
	Report = sample_chunks(3, sets:new(), #sample_report{}, State),
	ExpectedReport = #sample_report{total = 3, success = 1, failure = 2},
	?assertEqual(ExpectedReport, Report).
