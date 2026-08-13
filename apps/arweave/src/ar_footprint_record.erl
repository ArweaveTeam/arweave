-module(ar_footprint_record).
-test_category([fast]).

-export([add/3, add_async/4, delete/2, get_offset/1, get_padded_offset_from_footprint_offset/1,
        get_footprint/1, get_location/1, get_footprint_bucket/1,
        get_intervals/3,
         get_intervals/4, get_unsynced_intervals/3,
        footprint_intervals_to_byte_intervals/1,
        footprint_intervals_to_byte_intervals/3,
         max_offset/1, is_recorded/2]).

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_sync.hrl").

-include_lib("eunit/include/eunit.hrl").

-moduledoc """
    This module exports functions for maintaining
a replica 2.9 entropy-aligned record of the synced chunks.
It differs from the normal record (ar_data_sync) in that it only registers
the bucket numbers of the synced chunks and records chunks with the same footprint
next to each other. For example, a record may contain intervals 0-10, 1000-1024,
1028-2048. This means the node has the first 10 chunks of the first entropy footprint,
the last 24 chunks of the first entropy footprint and chunks 4-44 of the second
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
-spec add(Offset :: non_neg_integer(), Packing :: term(), StoreID :: string()) -> ok.
add(Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    ar_sync_record:add(FootprintOffset, FootprintOffset - 1, Packing, ar_data_sync_footprints, StoreID).

%% @doc Add a chunk to the footprint record asynchronously.
-spec add_async(Tag :: term(), Offset :: non_neg_integer(), Packing :: term(), StoreID :: string()) -> ok.
add_async(Tag, Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    ar_sync_record:add_async(Tag, FootprintOffset, FootprintOffset - 1, Packing, ar_data_sync_footprints, StoreID).

%% @doc Get the offset of a chunk in the footprint record.
-spec get_offset(Offset :: non_neg_integer()) -> non_neg_integer().
get_offset(Offset) ->
    PaddedOffset = ar_block:get_chunk_padded_offset(Offset),
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    FootprintsPerPartition = ar_replica_2_9:get_footprints_per_partition(),

    ChunksPerPartition = get_chunks_per_partition(),
    Partition = ar_replica_2_9:get_entropy_partition(PaddedOffset),
    PartitionOffset = (PaddedOffset - Partition * ar_block:partition_size()) div ?DATA_CHUNK_SIZE - 1,

    %% Which footprint within the partition
    Footprint = PartitionOffset rem FootprintsPerPartition,
    %% Position within the footprint
    FootprintOffset = PartitionOffset div FootprintsPerPartition,
    Partition * ChunksPerPartition + Footprint * FootprintSize + FootprintOffset + 1.

%% @doc Return the largest end offset of the chunk that maps to the given footprint offset.
get_padded_offset_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    Partition = Start div ChunksPerPartition,
    FootprintsPerPartition = ar_replica_2_9:get_footprints_per_partition(),
    PartitionStart = Partition * ChunksPerPartition,
    Footprint = (Start - PartitionStart) div FootprintSize,
    InFootprintOffset = (Start - PartitionStart) rem FootprintSize,
    EndOffset = Partition * ar_block:partition_size() + (InFootprintOffset * FootprintsPerPartition + (Footprint + 1)) * ?DATA_CHUNK_SIZE,
    ar_block:get_chunk_padded_offset(EndOffset).

%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
-spec get_footprint(Offset :: non_neg_integer()) -> non_neg_integer().
get_footprint(Offset) ->
    EntropyIndex = ar_replica_2_9:get_entropy_index(Offset, 0),
    EntropyIndex div ?SUB_CHUNK_COUNT.

%% @doc Get the replica 2.9 partition and footprint containing a chunk offset.
-spec get_location(Offset :: non_neg_integer()) ->
        {Partition :: non_neg_integer(), Footprint :: non_neg_integer()}.
get_location(Offset) ->
    {ar_replica_2_9:get_entropy_partition(Offset), get_footprint(Offset)}.

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
get_intervals(Partition, Footprint, any, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    ar_sync_record:collect_synced_intervals(
      Start, End, ar_data_sync_footprints, StoreID);
get_intervals(Partition, Footprint, Packing, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    ar_sync_record:collect_synced_intervals(Start, End, Packing,
        ar_data_sync_footprints, StoreID).

%% @doc Get the unsynced footprint intervals of a chunk.
-spec get_unsynced_intervals(
        Partition :: non_neg_integer(),
        Footprint :: non_neg_integer(),
        StoreID :: string()
       ) -> term().
get_unsynced_intervals(Partition, Footprint, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    ar_sync_record:collect_unsynced_intervals(Start, End, ar_data_sync_footprints, StoreID).

%% @doc Delete a chunk from the footprint record.
-spec delete(Offset :: non_neg_integer(), StoreID :: string()) -> ok.
delete(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    ar_sync_record:delete(FootprintOffset, FootprintOffset - 1, ar_data_sync_footprints, StoreID).

%% @doc Convert footprint intervals to byte intervals.
-spec footprint_intervals_to_byte_intervals(FootprintIntervals :: term()) -> term().
footprint_intervals_to_byte_intervals(FootprintIntervals) ->
    do_footprint_intervals_to_byte_intervals(
        ar_intervals:to_list(FootprintIntervals), ar_intervals:new()).

%% @doc Convert footprint intervals to byte intervals, cut at End, and drop
%% bytes before the chunk containing Start.
-spec footprint_intervals_to_byte_intervals(
    FootprintIntervals :: term(),
    Start :: non_neg_integer(),
    End :: non_neg_integer()
) -> term().
footprint_intervals_to_byte_intervals(FootprintIntervals, Start, End) ->
    ByteIntervals = footprint_intervals_to_byte_intervals(FootprintIntervals),
    ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
    PaddedStart =
        case ar_block:get_chunk_padded_offset(Start) of
            Start -> Start;
            PaddedOffset -> PaddedOffset - ?DATA_CHUNK_SIZE
        end,
    ar_intervals:outerjoin(
        ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2).

%% @doc Return an upper bound on the footprint offsets reachable by a weave of
%% the given byte size: the per-partition footprint capacity times the number
%% of partitions touched by the weave.
-spec max_offset(WeaveSize :: non_neg_integer()) -> non_neg_integer().
max_offset(WeaveSize) when WeaveSize > 0 ->
    NumPartitions = (WeaveSize + ar_block:partition_size() - 1) div ar_block:partition_size(),
    NumPartitions * get_chunks_per_partition();
max_offset(_) ->
    0.

%% @doc Return true if a chunk containing the given Offset (=< EndOffset, > StartOffset)
%% is found in the footprint record.
-spec is_recorded(Offset :: non_neg_integer(), StoreID :: string()) -> boolean().
is_recorded(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    ar_sync_record:is_recorded(FootprintOffset, ar_data_sync_footprints, StoreID).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_chunks_per_partition() ->
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    arweave_util:pad_to_closest_multiple_equal_or_above(ar_block:partition_size(), ?DATA_CHUNK_SIZE * FootprintSize) div ?DATA_CHUNK_SIZE.

footprint_range(Partition, Footprint) ->
    FootprintSize = ar_replica_2_9:get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    PartitionStartOffset = Partition * ChunksPerPartition,
    Start = PartitionStartOffset + Footprint * FootprintSize,
    End = min(Start + FootprintSize, PartitionStartOffset + ChunksPerPartition),
    {Start, End}.

do_footprint_intervals_to_byte_intervals([], Intervals) ->
    Intervals;
do_footprint_intervals_to_byte_intervals([{End, Start} | Rest], Intervals) ->
    Intervals2 = do_footprint_intervals_to_byte_intervals(Start, End, Intervals),
    do_footprint_intervals_to_byte_intervals(Rest, Intervals2).

do_footprint_intervals_to_byte_intervals(Start, End, Intervals) when Start >= End ->
    Intervals;
do_footprint_intervals_to_byte_intervals(Start, End, Intervals) ->
    Offset = get_padded_offset_from_footprint_offset(Start + 1),
    Intervals2 = ar_intervals:add(Intervals, Offset, Offset - ?DATA_CHUNK_SIZE),
    do_footprint_intervals_to_byte_intervals(Start + 1, End, Intervals2).

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).

get_offset_test() ->
    %% The first chunk of the first footprint.
    ?assertEqual(1, get_offset(?DATA_CHUNK_SIZE)),
    %% The first chunk of the second footprint.
    ?assertEqual(5, get_offset(?DATA_CHUNK_SIZE * 2)),
    %% The second chunk of the first footprint.
    ?assertEqual(2, get_offset(?DATA_CHUNK_SIZE * 3)),
    %% The second chunk of the second footprint.
    ?assertEqual(6, get_offset(?DATA_CHUNK_SIZE * 4)),
    %% The third chunk of the first footprint.
    ?assertEqual(3, get_offset(?DATA_CHUNK_SIZE * 5)),
    %% The third chunk of the second footprint.
    ?assertEqual(7, get_offset(?DATA_CHUNK_SIZE * 6)),
    %% The fourth chunk of the first footprint.
    ?assertEqual(4, get_offset(?DATA_CHUNK_SIZE * 7)),
    %% The fourth chunk of the second footprint.
    ?assertEqual(8, get_offset(?DATA_CHUNK_SIZE * 8)),
    %% The first chunk of the first footprint of the second partition.
    ?assertEqual(9, get_offset(?DATA_CHUNK_SIZE * 9)),
    %% The first chunk of the second footprint of the second partition.
    ?assertEqual(13, get_offset(?DATA_CHUNK_SIZE * 10)),
    %% The second chunk of the first footprint of the second partition.
    ?assertEqual(10, get_offset(?DATA_CHUNK_SIZE * 11)),
    %% The second chunk of the second footprint of the second partition.
    ?assertEqual(14, get_offset(?DATA_CHUNK_SIZE * 12)),
    %% The third chunk of the first footprint of the second partition.
    ?assertEqual(11, get_offset(?DATA_CHUNK_SIZE * 13)),
    %% The third chunk of the second footprint of the second partition.
    ?assertEqual(15, get_offset(?DATA_CHUNK_SIZE * 14)),
    %% The fourth chunk of the first footprint of the second partition.
    ?assertEqual(12, get_offset(?DATA_CHUNK_SIZE * 15)),
    %% The fourth chunk of the second footprint of the second partition.
    ?assertEqual(16, get_offset(?DATA_CHUNK_SIZE * 16)),
    %% The first chunk of the first footprint of the third partition.
    ?assertEqual(17, get_offset(?DATA_CHUNK_SIZE * 17)),
    %% The first chunk of the second footprint of the third partition.
    ?assertEqual(21, get_offset(?DATA_CHUNK_SIZE * 18)),
    %% The second chunk of the first footprint of the third partition.
    ?assertEqual(18, get_offset(?DATA_CHUNK_SIZE * 19)).

get_padded_offset_from_footprint_offset_test() ->
    ?assertEqual(262144, get_padded_offset_from_footprint_offset(1)),
    ?assertEqual(786432, get_padded_offset_from_footprint_offset(2)),
    ?assertEqual(1310720, get_padded_offset_from_footprint_offset(3)),
    ?assertEqual(1835008, get_padded_offset_from_footprint_offset(4)),
    ?assertEqual(524288, get_padded_offset_from_footprint_offset(5)),
    ?assertEqual(1048576, get_padded_offset_from_footprint_offset(6)),
    ?assertEqual(1572864, get_padded_offset_from_footprint_offset(7)),
    ?assertEqual(2097152, get_padded_offset_from_footprint_offset(8)),
    ?assertEqual(2359296, get_padded_offset_from_footprint_offset(9)),
    ?assertEqual(2883584, get_padded_offset_from_footprint_offset(10)),

    79280870522880 = ar_block:get_chunk_padded_offset(79280870522880),
    ?assertEqual(317123481, ar_footprint_record:get_offset(79280870522880)),
    ?assertEqual(79280870522880, ar_footprint_record:get_padded_offset_from_footprint_offset(317123481)).

get_offset_footprint_intervals_to_byte_intervals_reversal_test() ->
    Offsets = [?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE * 2, ?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 4,
               ?DATA_CHUNK_SIZE * 8, ?DATA_CHUNK_SIZE * 9],
    [get_offset_footprint_intervals_to_byte_intervals_reversal(Offset) || Offset <- Offsets].

get_offset_footprint_intervals_to_byte_intervals_reversal(ByteOffset) ->
    FootprintOffset = get_offset(ByteOffset),

    FootprintInterval = ar_intervals:from_list([{FootprintOffset, FootprintOffset - 1}]),
    ResultingByteIntervals = footprint_intervals_to_byte_intervals(FootprintInterval),
    [{GotEnd, GotStart}] = ar_intervals:to_list(ResultingByteIntervals),

    ?assertEqual(ByteOffset, GotEnd),
    ?assertEqual(ByteOffset - ?DATA_CHUNK_SIZE, GotStart).

get_unsynced_intervals_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
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

              Partition = 0,
              Footprint = 0,

              %% Get unsynced intervals before adding any data.
              UnsyncedBefore = get_unsynced_intervals(Partition, Footprint, TestStoreID),
              UnsyncedBeforeList = ar_intervals:to_list(UnsyncedBefore),
              ?assertEqual([{4, 0}], UnsyncedBeforeList),

              %% Add some data to the footprint.
              %% This should map to partition 0, footprint 0.
              PaddedOffset = ?DATA_CHUNK_SIZE,
              Packing = unpacked,
              ok = add(PaddedOffset, Packing, TestStoreID),

              UnsyncedAfter = get_unsynced_intervals(Partition, Footprint, TestStoreID),
              UnsyncedAfterList = ar_intervals:to_list(UnsyncedAfter),
              ?assertEqual([{4, 1}], UnsyncedAfterList)
      end).

get_intervals_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
      [{ar_storage_module, get_by_id, fun(test_intervals_store) -> test_intervals_store end}],
      fun() ->
              %% Set up a test sync record server.
              TestStoreID = test_intervals_store,
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

              Packing = unpacked,
              ar_sync_record:add(32, 0,
                                 Packing, ar_data_sync_footprints, TestStoreID),

              Partition = 0,
              Footprint = 0,
              SyncedIntervals = get_intervals(Partition, Footprint, TestStoreID),
              SyncedIntervalsList = ar_intervals:to_list(SyncedIntervals),
              ?assertEqual([{4, 0}], SyncedIntervalsList),
              Partition2 = 0,
              Footprint2 = 1,
              SyncedIntervals2 = get_intervals(Partition2, Footprint2, TestStoreID),
              SyncedIntervalsList2 = ar_intervals:to_list(SyncedIntervals2),
              ?assertEqual([{8, 4}], SyncedIntervalsList2),
              Partition3 = 0,
              Footprint3 = 2,
              SyncedIntervals3 = get_intervals(Partition3, Footprint3, TestStoreID),
              SyncedIntervalsList3 = ar_intervals:to_list(SyncedIntervals3),
              ?assertEqual([], SyncedIntervalsList3),
              Partition4 = 1,
              Footprint4 = 0,
              SyncedIntervals4 = get_intervals(Partition4, Footprint4, TestStoreID),
              SyncedIntervalsList4 = ar_intervals:to_list(SyncedIntervals4),
              ?assertEqual([{12, 8}], SyncedIntervalsList4),
              Partition5 = 1,
              Footprint5 = 1,
              SyncedIntervals5 = get_intervals(Partition5, Footprint5, TestStoreID),
              SyncedIntervalsList5 = ar_intervals:to_list(SyncedIntervals5),
              ?assertEqual([{16, 12}], SyncedIntervalsList5),
              Partition6 = 1,
              Footprint6 = 2,
              SyncedIntervals6 = get_intervals(Partition6, Footprint6, TestStoreID),
              SyncedIntervalsList6 = ar_intervals:to_list(SyncedIntervals6),
              ?assertEqual([], SyncedIntervalsList6),
              Partition7 = 2,
              Footprint7 = 0,
              SyncedIntervals7 = get_intervals(Partition7, Footprint7, TestStoreID),
              SyncedIntervalsList7 = ar_intervals:to_list(SyncedIntervals7),
              ?assertEqual([{20, 16}], SyncedIntervalsList7)
      end).

get_offset_get_padded_offset_from_footprint_offset_reversal_test() ->
    Offsets = [
               ?DATA_CHUNK_SIZE,
               ?DATA_CHUNK_SIZE * 2,
               ?DATA_CHUNK_SIZE * 3,
               ?PARTITION_SIZE,
               ?DATA_CHUNK_SIZE * 8,
               ?DATA_CHUNK_SIZE * 9,
               ?PARTITION_SIZE * 2,
               ?PARTITION_SIZE * 3,
               ?PARTITION_SIZE * 4,
               ?PARTITION_SIZE * 5,
               ?PARTITION_SIZE * 6,
               ?PARTITION_SIZE * 7,
               ?PARTITION_SIZE * 8,
               ?PARTITION_SIZE * 9,
               ?PARTITION_SIZE * 10,
               ?PARTITION_SIZE * 11,
               ?PARTITION_SIZE * 12,
               ?PARTITION_SIZE * 13,
               ?PARTITION_SIZE * 6249,
               ?PARTITION_SIZE * 6250,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE,
               ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 2,
               ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE * 2,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 8,
               ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE * 8,
               ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 9
              ],
    [get_offset_get_padded_offset_from_footprint_offset_reversal(F) || F <- Offsets],
    ok.

get_offset_get_padded_offset_from_footprint_offset_reversal(Offset) ->
    FootprintOffset = get_offset(Offset),
    PaddedEndOffset = get_padded_offset_from_footprint_offset(FootprintOffset),
    ?assertEqual(ar_block:get_chunk_padded_offset(Offset), PaddedEndOffset).

footprint_intervals_to_byte_intervals_test() ->
    TestCases =
        [
         {[], [], "Empty"},
         {[{1, 0}], [{?DATA_CHUNK_SIZE, 0}], "One chunk"},
         {[{2, 0}], [
                     {?DATA_CHUNK_SIZE, 0}, {?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 2}], "Two chunks"},
         {[{4, 0}], [
                     {?DATA_CHUNK_SIZE, 0},
                     {?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 2},
                     {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4},
                     {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}], "Full footprint"},
         {[{5, 0}], [
                     {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4},
                     {?DATA_CHUNK_SIZE * 3, 0},
                     {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}], "Footprint wraparound"},
         {[{6, 3}], [
                     {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6},
                     {?DATA_CHUNK_SIZE * 2, ?DATA_CHUNK_SIZE * 1},
                     {?DATA_CHUNK_SIZE * 4, ?DATA_CHUNK_SIZE * 3}], "Bits of two footprints"},
         {[{1, 0}, {3, 2}], [
                             {?DATA_CHUNK_SIZE, 0},
                             {?DATA_CHUNK_SIZE * 5,
                              ?DATA_CHUNK_SIZE * 4}], "Two chunks with a hole"},
         {[{8, 0}], [
                     {?DATA_CHUNK_SIZE * 8, 0}], "Completely covered partition"},
         {[{9, 0}], [
                     {?DATA_CHUNK_SIZE * 9, 0}], "Completely covered partition plus one chunk"},
         {[{9, 0}, {13, 12}], [
                               {?DATA_CHUNK_SIZE * 10, 0}], "Completely covered partition plus two chunks"},
         {[{9, 0}, {13, 12}, {15, 14}], [
                                         {?DATA_CHUNK_SIZE * 10, 0},
                                         {?DATA_CHUNK_SIZE * 14, ?DATA_CHUNK_SIZE * 13}],
          "Completely covered partition plus three chunks"}
        ],
    test_footprint_intervals_to_byte_intervals(TestCases).

test_footprint_intervals_to_byte_intervals([]) ->
    ok;
test_footprint_intervals_to_byte_intervals([{Input, Expected, Title} | Rest]) ->
    ?assertEqual(ar_intervals:from_list(Expected),
            footprint_intervals_to_byte_intervals(ar_intervals:from_list(Input)), Title),
    test_footprint_intervals_to_byte_intervals(Rest).

bounded_footprint_intervals_to_byte_intervals_test() ->
    ?assertEqual(
        ar_intervals:from_list([{786432, 524288}, {1310720, 1048576}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{4, 0}]), 262144, 1572864),
        "Full Footprint 0, aligned boundaries"),

    ?assertEqual(
        ar_intervals:from_list([{524288, 262144}, {1048576, 786432}, {1572864, 1310720}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{8, 4}]), 262144, 1572864),
        "Full Footprint 1 cut to aligned boundaries"),

    ?assertEqual(
        ar_intervals:from_list([
            {262144, 200000}, {786432, 524288}, {1310720, 1048576}, {1600000, 1572864}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{4, 0}]), 200000, 1600000),
        "Full Footprint 0, unaligned boundaries, pre-strict"),

    ?assertEqual(
        ar_intervals:from_list([{524288, 262144}, {1048576, 786432}, {1572864, 1310720}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{8, 4}]), 200000, 1600000),
        "Full Footprint 1, unaligned boundaries, pre-strict"),

    ?assertEqual(
        ar_intervals:from_list([{2883584, 2621440}, {3407872, 3145728}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{12, 8}]), 2400000, 3500000),
        "Full Footprint 2, unaligned boundaries, post-strict"),

    ?assertEqual(
        ar_intervals:from_list([{2621440, 2359296}, {3145728, 2883584}, {3500000, 3407872}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{16, 12}]), 2400000, 3500000),
        "Full Footprint 3, unaligned boundaries, post-strict"),

    ?assertEqual(
        ar_intervals:from_list([{2621440, 2359296}, {3500000, 3407872}]),
        footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{16, 14}, {13, 12}]), 2400000, 3500000),
        "Partial Footprint 3, unaligned boundaries, post-strict"),

    ok.

-endif.
