-module(ar_footprint_record).

-export([add/3, delete/2, get_offset/1, get_padded_offset_from_footprint_offset/1,
		get_footprint/1, get_footprint_bucket/1, get_intervals/3,
		get_intervals/4, get_unsynced_intervals/3,
		get_intervals_from_footprint_intervals/1,
		get_footprint_size/0, get_footprints_per_partition/0]).

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_consensus.hrl").
-include("ar_data_discovery.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
    This module exports functions for maintaining
    a replica 2.9 entropy-aligned record of the synced chunks.
    It differs from the normal record (ar_data_sync) in that it only registers
    the bucket numbers of the synced chunks and records chunks with the same footprint
    next to each other. For example, a record may contain intervals 0-10, 1000-1024,
    1020-2048. This means the node has the first 10 chunks of the first entropy footprint,
    the last 24 chunks of the first entropy footprint and chunks 20-48 of the second
    entropy footprint. These chunks are from the first partition. The offset of the chunks
    from the second partition is shifted by the number of chunks in the replica 2.9
    entropy generated per partition (which is slightly bigger than the number of chunks
    that can fit in the 3.6 TB partition).

    Note that Packing does not have to be replica_2_9. We maintain this record
    for any packing so that it is convenient to serve the data to any client.
""".

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Add a chunk to the footprint record.
-spec add(PaddedOffset :: non_neg_integer(), Packing :: term(), StoreID :: string()) -> ok.
add(PaddedOffset, Packing, StoreID) ->
	Offset = get_offset(PaddedOffset),
	ar_sync_record:add(Offset, Offset - 1, Packing, ar_data_sync_footprints, StoreID).

%% @doc Get the offset of a chunk in the footprint record.
-spec get_offset(Offset :: non_neg_integer()) -> non_neg_integer().
get_offset(Offset) ->
	PaddedOffset = ar_block:get_chunk_padded_offset(Offset),
	FootprintSize = get_footprint_size(),
	FootprintsPerPartition = get_footprints_per_partition(),

	%% Convert byte offset to chunk index (1-based)
	ChunkIndex = (PaddedOffset - 1) div ?DATA_CHUNK_SIZE + 1,

	ChunksPerPartition = FootprintsPerPartition * FootprintSize,
	Partition = (ChunkIndex - 1) div ChunksPerPartition,

	%% Position within partition
	PartitionOffset = (ChunkIndex - 1) rem ChunksPerPartition,
	%% Which footprint within the partition
	Footprint = PartitionOffset div FootprintSize,
	%% Position within the footprint
	FootprintOffset = PartitionOffset rem FootprintSize,

	%% Interleave: chunks are arranged so that consecutive footprints are spread out
	Interleaved = FootprintOffset * FootprintsPerPartition + Footprint,
	%% Final offset (1-based)
	Partition * ChunksPerPartition + Interleaved + 1.

%% @doc Return the largest end offset of the chunk that maps to the given footprint offset.
get_padded_offset_from_footprint_offset(FootprintOffset) ->
	Start = FootprintOffset - 1,
	FootprintSize = get_footprint_size(),
	PartitionSize = FootprintSize * get_footprints_per_partition(),
	PartitionOffset = (Start div PartitionSize) * PartitionSize,
	PartitionRelativeOffset = Start - PartitionOffset,
	SliceSize = get_footprints_per_partition(),
	SliceOffset = (PartitionRelativeOffset rem FootprintSize) * SliceSize,
	InFootprintOffset = PartitionRelativeOffset div FootprintSize,
	ar_block:get_chunk_padded_offset((PartitionOffset + SliceOffset + InFootprintOffset) * ?DATA_CHUNK_SIZE) + ?DATA_CHUNK_SIZE.

%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
-spec get_footprint(Offset :: non_neg_integer()) -> non_neg_integer().
get_footprint(Offset) ->
	EntropyIndex = ar_replica_2_9:get_entropy_index(Offset, 0),
	EntropyIndex div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

%% @doc Get the footprint bucket number of a chunk.
-spec get_footprint_bucket(Offset :: non_neg_integer()) -> non_neg_integer().
get_footprint_bucket(Offset) ->
	get_offset(Offset) div ?NETWORK_FOOTPRINT_BUCKET_SIZE.

%% @doc Get the synced footprint intervals of a chunk.
-spec get_intervals(
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer(),
	StoreID :: string()
) -> term().
get_intervals(Partition, Footprint, StoreID) ->
	get_intervals(Partition, Footprint, any, StoreID).

%% @doc Get the synced footprint intervals of a chunk.
-spec get_intervals(
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer(),
	Packing :: term(),
	StoreID :: string()
) -> term().
get_intervals(Partition, Footprint, Packing, StoreID) ->
	FootprintSize = get_footprint_size(),
	FootprintsPerPartition = ?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
	PartitionStartOffset = Partition * FootprintsPerPartition * FootprintSize,
	FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
	collect_intervals(FootprintStart, FootprintStart + FootprintSize, Packing, StoreID).

%% @doc Get the unsynced footprint intervals of a chunk.
-spec get_unsynced_intervals(
	Partition :: non_neg_integer(),
	Footprint :: non_neg_integer(),
	StoreID :: string()
) -> term().
get_unsynced_intervals(Partition, Footprint, StoreID) ->
	FootprintSize = get_footprint_size(),
	FootprintsPerPartition = ?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
	PartitionStartOffset = Partition * FootprintsPerPartition * FootprintSize,
	FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
	collect_unsynced_intervals(FootprintStart, FootprintStart + FootprintSize, StoreID).

%% @doc Delete a chunk from the footprint record.
-spec delete(Offset :: non_neg_integer(), StoreID :: string()) -> ok.
delete(Offset, StoreID) ->
	FootprintOffset = get_offset(Offset),
	ar_sync_record:delete(FootprintOffset, FootprintOffset - 1, ar_data_sync_footprints, StoreID).

%% @doc Convert a list of footprint intervals to a list of intervals.
-spec get_intervals_from_footprint_intervals(FootprintIntervals :: term()) -> term().
get_intervals_from_footprint_intervals(FootprintIntervals) ->
	get_intervals_from_footprint_intervals(ar_intervals:to_list(FootprintIntervals), ar_intervals:new()).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_footprints_per_partition() ->
	?REPLICA_2_9_ENTROPY_COUNT div ?COMPOSITE_PACKING_SUB_CHUNK_COUNT.

get_footprint_size() ->
	?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

collect_intervals(Start, End, Packing, StoreID) ->
	collect_intervals(Start, End, Packing, StoreID, ar_intervals:new()).

collect_intervals(Start, End, _Packing, _StoreID, Intervals) when Start >= End ->
	Intervals;
collect_intervals(Start, End, Packing, StoreID, Intervals) ->
	Query =
		case Packing of
			any ->
				ar_sync_record:get_next_synced_interval(Start, End,
					ar_data_sync_footprints, StoreID);
			Packing ->
				ar_sync_record:get_next_synced_interval(Start, End,
						Packing, ar_data_sync_footprints, StoreID)
		end,
	case Query of
		not_found ->
			Intervals;
		{End2, Start2} ->
			End3 = min(End2, End),
			collect_intervals(End3, End, Packing, StoreID,
					ar_intervals:add(Intervals, End3, Start2))
	end.

collect_unsynced_intervals(Start, End, StoreID) ->
	collect_unsynced_intervals(Start, End, StoreID, ar_intervals:new()).

collect_unsynced_intervals(Start, End, _StoreID, Intervals) when Start >= End ->
	Intervals;
collect_unsynced_intervals(Start, End, StoreID, Intervals) ->
	Query = ar_sync_record:get_next_unsynced_interval(Start, End, ar_data_sync_footprints, StoreID),
	case Query of
		not_found ->
			Intervals;
		{End2, Start2} ->
			End3 = min(End2, End),
			collect_unsynced_intervals(End3, End, StoreID,
					ar_intervals:add(Intervals, End3, Start2))
	end.

get_intervals_from_footprint_intervals([], Intervals) ->
	Intervals;
get_intervals_from_footprint_intervals([{End, Start} | Rest], Intervals) ->
	Intervals2 = get_intervals_from_footprint_intervals(Start, End, Intervals),
	get_intervals_from_footprint_intervals(Rest, Intervals2).

get_intervals_from_footprint_intervals(Start, End, Intervals) when Start >= End ->
	Intervals;
get_intervals_from_footprint_intervals(Start, End, Intervals) ->
	Offset = get_padded_offset_from_footprint_offset(Start + 1),
	Intervals2 = ar_intervals:add(Intervals, Offset, Offset - ?DATA_CHUNK_SIZE),
	get_intervals_from_footprint_intervals(Start + 1, End, Intervals2).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(TEST).

get_offset_test() ->
	?assertEqual(1, get_offset(?DATA_CHUNK_SIZE)),
	?assertEqual(4, get_offset(?DATA_CHUNK_SIZE * 2)),
	?assertEqual(7, get_offset(?DATA_CHUNK_SIZE * 3)),
	?assertEqual(2, get_offset(?DATA_CHUNK_SIZE * 4)),
	?assertEqual(5, get_offset(?DATA_CHUNK_SIZE * 5)),
	?assertEqual(8, get_offset(?DATA_CHUNK_SIZE * 6)),
	?assertEqual(3, get_offset(?DATA_CHUNK_SIZE * 7)),
	?assertEqual(6, get_offset(?DATA_CHUNK_SIZE * 8)),
	?assertEqual(9, get_offset(?DATA_CHUNK_SIZE * 9)),
	?assertEqual(10, get_offset(?DATA_CHUNK_SIZE * 10)),
	?assertEqual(13, get_offset(?DATA_CHUNK_SIZE * 11)),
	?assertEqual(16, get_offset(?DATA_CHUNK_SIZE * 12)),
	?assertEqual(11, get_offset(?DATA_CHUNK_SIZE * 13)),
	?assertEqual(14, get_offset(?DATA_CHUNK_SIZE * 14)),
	?assertEqual(17, get_offset(?DATA_CHUNK_SIZE * 15)),
	?assertEqual(12, get_offset(?DATA_CHUNK_SIZE * 16)),
	?assertEqual(15, get_offset(?DATA_CHUNK_SIZE * 17)),
	?assertEqual(18, get_offset(?DATA_CHUNK_SIZE * 18)),
	?assertEqual(19, get_offset(?DATA_CHUNK_SIZE * 19)).

offset_reversal_test() ->
	Offsets = [?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE * 2, ?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 4,
			?DATA_CHUNK_SIZE * 8, ?DATA_CHUNK_SIZE * 9],
	[test_offset_reversal(Offset) || Offset <- Offsets].

test_offset_reversal(ByteOffset) ->
	FootprintOffset = get_offset(ByteOffset),

	FootprintInterval = ar_intervals:from_list([{FootprintOffset, FootprintOffset - 1}]),
	ResultingByteIntervals = get_intervals_from_footprint_intervals(FootprintInterval),
	[{GotEnd, GotStart}] = ar_intervals:to_list(ResultingByteIntervals),

	?assertEqual(ByteOffset, GotEnd),
	?assertEqual(ByteOffset - ?DATA_CHUNK_SIZE, GotStart).

get_unsynced_intervals_test() ->
	ar_test_node:test_with_mocked_functions(
		[{ar_storage_module, get_by_id, fun(test_unsynced_store) -> test_unsynced_store end}],
		fun() ->
			%% Set up a test sync record server.
			TestStoreID = test_unsynced_store,
			TestProcessName = list_to_atom("ar_sync_record_" ++ atom_to_list(TestStoreID)),

			%% Initialize sync_records ETS table if it does not exist.
			case ets:info(sync_records) of
				undefined ->
					ets:new(sync_records, [named_table, public, {read_concurrency, true}]);
				_ ->
					%% Clear existing data from previous tests.
					ets:delete_all_objects(sync_records)
			end,

			%% Start the sync record process.
			case whereis(TestProcessName) of
				undefined ->
					{ok, _Pid} = ar_sync_record:start_link(TestProcessName, TestStoreID);
				_ ->
					ok
			end,

			%% Test partition 0, footprint 0. It should have unsynced intervals initially.
			Partition = 0,
			Footprint = 0,

			%% Get unsynced intervals before adding any data.
			UnsyncedBefore = get_unsynced_intervals(Partition, Footprint, TestStoreID),
			UnsyncedBeforeList = ar_intervals:to_list(UnsyncedBefore),

			%% The entire footprint range should be unsynced initially.
			FootprintSize = get_footprint_size(),
			FootprintsPerPartition = get_footprints_per_partition(),
			PartitionStartOffset = Partition * FootprintsPerPartition * FootprintSize,
			FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
			FootprintEnd = FootprintStart + FootprintSize,
			?assertEqual([{FootprintEnd, FootprintStart}], UnsyncedBeforeList),

			%% Add some data to the footprint.
			%% This should map to partition 0, footprint 0.
			PaddedOffset = 0,
			Packing = unpacked,
			ok = add(PaddedOffset, Packing, TestStoreID),

			%% Get unsynced intervals after adding data.
			UnsyncedAfter = get_unsynced_intervals(Partition, Footprint, TestStoreID),
			UnsyncedAfterList = ar_intervals:to_list(UnsyncedAfter),

			%% Assert the exact shape of the unsynced intervals.
			AddedFootprintOffset = get_offset(PaddedOffset),
			ExpectedUnsyncedAfter = [
				{AddedFootprintOffset - 1, FootprintStart},
				{FootprintEnd, AddedFootprintOffset}
			],

			?assertEqual(ExpectedUnsyncedAfter, UnsyncedAfterList)
		end).

get_intervals_from_footprint_intervals_test() ->
	TestCases =
	[
		{[], [], "Empty"},
		{[{1, 0}], [{?DATA_CHUNK_SIZE, 0}], "One chunk"},
		{[{2, 0}], [{?DATA_CHUNK_SIZE, 0}, {?DATA_CHUNK_SIZE * 4, ?DATA_CHUNK_SIZE * 3}], "Two chunks"},
		{[{1, 0}, {3, 2}], [{?DATA_CHUNK_SIZE, 0}, {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}], "Two chunks with a hole"},
		{[{9, 0}], [{?DATA_CHUNK_SIZE * 9, 0}], "Completely covered partition"},
		{[{10, 0}], [{?DATA_CHUNK_SIZE * 10, 0}], "Completely covered partition plus one chunk"},
		{[{11, 0}], [{?DATA_CHUNK_SIZE * 10, 0}, {?DATA_CHUNK_SIZE * 13, ?DATA_CHUNK_SIZE * 12}], "Completely covered partition plus two chunks"}
	],
	test_get_intervals_from_footprint_intervals(TestCases).

test_get_intervals_from_footprint_intervals([]) ->
	ok;
test_get_intervals_from_footprint_intervals([{Input, Expected, Title} | Rest]) ->
	?assertEqual(ar_intervals:from_list(Expected),
			get_intervals_from_footprint_intervals(ar_intervals:from_list(Input)), Title),
	test_get_intervals_from_footprint_intervals(Rest).

-endif.