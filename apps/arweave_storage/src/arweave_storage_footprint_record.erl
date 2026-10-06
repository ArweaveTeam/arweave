-module(arweave_storage_footprint_record).
-ifdef(AR_TEST).
-export([get_chunks_per_partition/0, collect_intervals/4, collect_intervals/5, collect_unsynced_intervals/3, collect_unsynced_intervals/4, get_intervals_from_footprint_intervals/2, get_intervals_from_footprint_intervals/3, get_offset_get_intervals_from_footprint_intervals_reversal/1, get_offset_get_padded_offset_from_footprint_offset_reversal/1]).
-endif.



-export([add/3, add_async/4, delete/2, get_offset/1, get_padded_offset_from_footprint_offset/1,
         get_footprint/1, get_footprint_bucket/1, get_intervals/3,
         get_intervals/4, get_unsynced_intervals/3,
         get_intervals_from_footprint_intervals/1,
         get_footprint_size/0, get_footprints_per_partition/0,
         max_offset/1, is_recorded/2, get_next_sector_start/1,
         get_sector_bucket_start/2]).


-include_lib("arweave/include/ar.hrl").

-include_lib("arweave/include/ar_consensus.hrl").

-include_lib("arweave/include/ar_data_discovery.hrl").


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
    arweave_storage_sync_record:add(FootprintOffset, FootprintOffset - 1, Packing, ar_data_sync_footprints, StoreID).


%% @doc Add a chunk to the footprint record asynchronously.
-spec add_async(Tag :: term(), Offset :: non_neg_integer(), Packing :: term(), StoreID :: string()) -> ok.

add_async(Tag, Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage_sync_record:add_async(Tag, FootprintOffset, FootprintOffset - 1, Packing, ar_data_sync_footprints, StoreID).


%% @doc Get the offset of a chunk in the footprint record.
-spec get_offset(Offset :: non_neg_integer()) -> non_neg_integer().

get_offset(Offset) ->
    PaddedOffset = arweave_lib_constants:get_chunk_padded_offset(Offset),
    FootprintSize = get_footprint_size(),
    FootprintsPerPartition = get_footprints_per_partition(),

    ChunksPerPartition = get_chunks_per_partition(),
    Partition = arweave_lib_replica_2_9:get_entropy_partition(PaddedOffset),
    PartitionOffset = (PaddedOffset - Partition * ?PARTITION_SIZE) div ?DATA_CHUNK_SIZE - 1,

    %% Which footprint within the partition
    Footprint = PartitionOffset rem FootprintsPerPartition,
    %% Position within the footprint
    FootprintOffset = PartitionOffset div FootprintsPerPartition,
    Partition * ChunksPerPartition + Footprint * FootprintSize + FootprintOffset + 1.


%% @doc Return the largest end offset of the chunk that maps to the given footprint offset.
get_padded_offset_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    FootprintSize = get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    Partition = Start div ChunksPerPartition,
    FootprintsPerPartition = get_footprints_per_partition(),
    PartitionStart = Partition * ChunksPerPartition,
    Footprint = (Start - PartitionStart) div FootprintSize,
    InFootprintOffset = (Start - PartitionStart) rem FootprintSize,
    EndOffset = Partition * ?PARTITION_SIZE + (InFootprintOffset * FootprintsPerPartition + (Footprint + 1)) * ?DATA_CHUNK_SIZE,
    arweave_lib_constants:get_chunk_padded_offset(EndOffset).


%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
-spec get_footprint(Offset :: non_neg_integer()) -> non_neg_integer().

get_footprint(Offset) ->
    EntropyIndex = arweave_lib_replica_2_9:get_entropy_index(Offset, 0),
    EntropyIndex div ?SUB_CHUNK_COUNT.


%% @doc Return the bucket end offset of the first chunk of the sector after
%% the one holding the given chunk.
get_next_sector_start(AbsoluteChunkEndOffset) ->
    get_sector_bucket_start(AbsoluteChunkEndOffset, 1) + ?DATA_CHUNK_SIZE.


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
    ChunksPerPartition = get_chunks_per_partition(),
    PartitionStartOffset = Partition * ChunksPerPartition,
    FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
    End = min(FootprintStart + FootprintSize, PartitionStartOffset + ChunksPerPartition),
    collect_intervals(FootprintStart, End, Packing, StoreID).


%% @doc Get the unsynced footprint intervals of a chunk.
-spec get_unsynced_intervals(
        Partition :: non_neg_integer(),
        Footprint :: non_neg_integer(),
        StoreID :: string()
       ) -> term().

get_unsynced_intervals(Partition, Footprint, StoreID) ->
    FootprintSize = get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    PartitionStartOffset = Partition * ChunksPerPartition,
    FootprintStart = PartitionStartOffset + Footprint * FootprintSize,
    End = min(FootprintStart + FootprintSize, PartitionStartOffset + ChunksPerPartition),
    collect_unsynced_intervals(FootprintStart, End, StoreID).


%% @doc Delete a chunk from the footprint record.
-spec delete(Offset :: non_neg_integer(), StoreID :: string()) -> ok.

delete(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage_sync_record:delete(FootprintOffset, FootprintOffset - 1, ar_data_sync_footprints, StoreID).


%% @doc Convert a list of footprint intervals to a list of intervals.
-spec get_intervals_from_footprint_intervals(FootprintIntervals :: term()) -> term().

get_intervals_from_footprint_intervals(FootprintIntervals) ->
    get_intervals_from_footprint_intervals(arweave_lib_intervals:to_list(FootprintIntervals), arweave_lib_intervals:new()).


%% @doc Get the number of footprints contained in a partition.
-spec get_footprints_per_partition() -> non_neg_integer().

get_footprints_per_partition() ->
    ?REPLICA_2_9_ENTROPY_COUNT div ?SUB_CHUNK_COUNT.


%% @doc Return an upper bound on the footprint offsets reachable by a weave of
%% the given byte size: the per-partition footprint capacity times the number
%% of partitions touched by the weave.
-spec max_offset(WeaveSize :: non_neg_integer()) -> non_neg_integer().

max_offset(WeaveSize) when WeaveSize > 0 ->
    NumPartitions = (WeaveSize + ?PARTITION_SIZE - 1) div ?PARTITION_SIZE,
    NumPartitions * get_chunks_per_partition();
max_offset(_) ->
    0.


%% @doc Return true if a chunk containing the given Offset (=< EndOffset, > StartOffset)
%% is found in the footprint record.
-spec is_recorded(Offset :: non_neg_integer(), StoreID :: string()) -> boolean().

is_recorded(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage_sync_record:is_recorded(FootprintOffset, ar_data_sync_footprints, StoreID).


%%%===================================================================
%%% Private functions.
%%%===================================================================

get_footprint_size() ->
    ?REPLICA_2_9_ENTROPY_SIZE div ?SUB_CHUNK_SIZE.


get_chunks_per_partition() ->
    FootprintSize = get_footprint_size(),
    arweave_lib_util:pad_to_closest_multiple_equal_or_above(?PARTITION_SIZE, ?DATA_CHUNK_SIZE * FootprintSize) div ?DATA_CHUNK_SIZE.


collect_intervals(Start, End, Packing, StoreID) ->
    collect_intervals(Start, End, Packing, StoreID, arweave_lib_intervals:new()).


collect_intervals(Start, End, _Packing, _StoreID, Intervals) when Start >= End ->
    Intervals;
collect_intervals(Start, End, Packing, StoreID, Intervals) ->
    Query =
        case Packing of
            any ->
                arweave_storage_sync_record:get_next_synced_interval(Start, End,
                                                        ar_data_sync_footprints, StoreID);
            Packing ->
                arweave_storage_sync_record:get_next_synced_interval(Start, End,
                                                        Packing, ar_data_sync_footprints, StoreID)
        end,
    case Query of
        not_found ->
            Intervals;
        {End2, Start2} ->
            End3 = min(End2, End),
            Start3 = max(Start2, Start),
            collect_intervals(End3, End, Packing, StoreID,
                              arweave_lib_intervals:add(Intervals, End3, Start3))
    end.


collect_unsynced_intervals(Start, End, StoreID) ->
    collect_unsynced_intervals(Start, End, StoreID, arweave_lib_intervals:new()).


collect_unsynced_intervals(Start, End, _StoreID, Intervals) when Start >= End ->
    Intervals;
collect_unsynced_intervals(Start, End, StoreID, Intervals) ->
    Query = arweave_storage_sync_record:get_next_unsynced_interval(Start, End, ar_data_sync_footprints, StoreID),
    case Query of
        not_found ->
            Intervals;
        {End2, Start2} ->
            End3 = min(End2, End),
            Start3 = max(Start2, Start),
            collect_unsynced_intervals(End3, End, StoreID,
                                       arweave_lib_intervals:add(Intervals, End3, Start3))
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
    Intervals2 = arweave_lib_intervals:add(Intervals, Offset, Offset - ?DATA_CHUNK_SIZE),
    get_intervals_from_footprint_intervals(Start + 1, End, Intervals2).


%% @doc Return the start offset of the first bucket of the sector that is
%% SectorShift sectors after the one holding the given chunk. The last
%% sector of a partition overhangs the next one, so a shift past it lands
%% on the next partition's first bucket.
get_sector_bucket_start(AbsoluteChunkEndOffset, SectorShift) ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    PartitionSize = arweave_lib_constants:partition_size(),
    PartitionRelativeOffset =
        arweave_lib_replica_2_9:get_partition_offset(AbsoluteChunkEndOffset),
    Partition = arweave_lib_replica_2_9:get_entropy_partition(AbsoluteChunkEndOffset),
    Sector = PartitionRelativeOffset div SectorSize + SectorShift,
    SectorStart = min(Partition * PartitionSize + Sector * SectorSize,
        (Partition + 1) * PartitionSize),
    arweave_lib_util:floor_int(
        SectorStart + ?DATA_CHUNK_SIZE - 1, ?DATA_CHUNK_SIZE).


%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).


get_offset_get_intervals_from_footprint_intervals_reversal(ByteOffset) ->
    FootprintOffset = get_offset(ByteOffset),

    FootprintInterval = arweave_lib_intervals:from_list([{FootprintOffset, FootprintOffset - 1}]),
    ResultingByteIntervals = get_intervals_from_footprint_intervals(FootprintInterval),
    [{GotEnd, GotStart}] = arweave_lib_intervals:to_list(ResultingByteIntervals),

    ?assertEqual(ByteOffset, GotEnd),
    ?assertEqual(ByteOffset - ?DATA_CHUNK_SIZE, GotStart).


get_offset_get_padded_offset_from_footprint_offset_reversal(Offset) ->
    FootprintOffset = get_offset(Offset),
    PaddedEndOffset = get_padded_offset_from_footprint_offset(FootprintOffset),
    ?assertEqual(arweave_lib_constants:get_chunk_padded_offset(Offset), PaddedEndOffset).


-endif.



