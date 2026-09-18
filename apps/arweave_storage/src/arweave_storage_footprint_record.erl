-module(arweave_storage_footprint_record).

-export([
    add/3,
    add_async/4,
    delete/2,
    get_offset/1,
    get_padded_offset_from_footprint_offset/1,
    get_footprint/1,
    footprint_range/2,
    get_location/1,
    get_footprint_bucket/1,
    get_intervals/3,
    get_intervals/4,
    get_unsynced_intervals/3,
    footprint_intervals_to_byte_intervals/1,
    footprint_intervals_to_byte_intervals/3,
    max_offset/1,
    is_recorded/2,
    get_next_sector_start/1,
    get_sector_bucket_start/2
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

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
add(Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage:add_sync_record(
        FootprintOffset, FootprintOffset - 1, Packing, {ar_data_sync, footprint}, StoreID
    ).

%% @doc Add a chunk to the footprint record asynchronously.
add_async(Tag, Offset, Packing, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage_sync_record:add_async(
        Tag,
        FootprintOffset,
        FootprintOffset - 1,
        Packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% @doc Get the offset of a chunk in the footprint record.
get_offset(Offset) ->
    PaddedOffset = arweave_constants:get_chunk_padded_offset(Offset),
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    FootprintsPerPartition = arweave_storage_deps:get_footprints_per_partition(),

    ChunksPerPartition = get_chunks_per_partition(),
    Partition = arweave_storage_deps:get_entropy_partition(PaddedOffset),
    PartitionOffset =
        (PaddedOffset - Partition * arweave_constants:partition_size()) div ?DATA_CHUNK_SIZE - 1,

    %% Which footprint within the partition
    Footprint = PartitionOffset rem FootprintsPerPartition,
    %% Position within the footprint
    FootprintOffset = PartitionOffset div FootprintsPerPartition,
    Partition * ChunksPerPartition + Footprint * FootprintSize + FootprintOffset + 1.

%% @doc Return the largest end offset of the chunk that maps to the given footprint offset.
get_padded_offset_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    ChunksPerPartition = get_chunks_per_partition(),
    Partition = Start div ChunksPerPartition,
    FootprintsPerPartition = arweave_storage_deps:get_footprints_per_partition(),
    PartitionStart = Partition * ChunksPerPartition,
    Footprint = (Start - PartitionStart) div FootprintSize,
    InFootprintOffset = (Start - PartitionStart) rem FootprintSize,
    EndOffset =
        Partition * arweave_constants:partition_size() +
            (InFootprintOffset * FootprintsPerPartition + (Footprint + 1)) * ?DATA_CHUNK_SIZE,
    arweave_constants:get_chunk_padded_offset(EndOffset).

%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
get_footprint(Offset) ->
    EntropyIndex = arweave_storage_deps:get_entropy_index(Offset, 0),
    EntropyIndex div ?SUB_CHUNK_COUNT.

%% @doc Return the bucket end offset of the first chunk of the sector after
%% the one holding the given chunk.
get_next_sector_start(AbsoluteChunkEndOffset) ->
    get_sector_bucket_start(AbsoluteChunkEndOffset, 1) + ?DATA_CHUNK_SIZE.

%% @doc Get the replica 2.9 partition and footprint containing a chunk offset.
get_location(Offset) ->
    {arweave_storage_deps:get_entropy_partition(Offset), get_footprint(Offset)}.

%% @doc Get the footprint bucket number of a chunk.
get_footprint_bucket(Offset) ->
    get_offset(Offset) div ?NETWORK_FOOTPRINT_BUCKET_SIZE.

%% @doc Get the synced footprint intervals of a chunk.
get_intervals(Partition, Footprint, StoreID) ->
    get_intervals(Partition, Footprint, any, StoreID).

%% @doc Get the synced footprint intervals of a chunk.
get_intervals(Partition, Footprint, any, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    arweave_storage_sync_record:get_intervals(
        synced, Start, End, any_packing, {ar_data_sync, footprint}, StoreID
    );
get_intervals(Partition, Footprint, Packing, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    arweave_storage_sync_record:get_intervals(
        synced,
        Start,
        End,
        Packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% @doc Get the unsynced footprint intervals of a chunk.
get_unsynced_intervals(Partition, Footprint, StoreID) ->
    {Start, End} = footprint_range(Partition, Footprint),
    arweave_storage:get_intervals(
        unsynced,
        Start,
        End,
        any_packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%% @doc Delete a chunk from the footprint record.
delete(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage:delete_sync_record(
        FootprintOffset, FootprintOffset - 1, {ar_data_sync, footprint}, StoreID
    ).

%% @doc Convert footprint intervals to byte intervals.
footprint_intervals_to_byte_intervals(FootprintIntervals) ->
    do_footprint_intervals_to_byte_intervals(
        ar_intervals:to_list(FootprintIntervals), ar_intervals:new()
    ).

%% @doc Convert footprint intervals to byte intervals, cut at End, and drop
%% bytes before the chunk containing Start.
footprint_intervals_to_byte_intervals(FootprintIntervals, Start, End) ->
    ByteIntervals = footprint_intervals_to_byte_intervals(FootprintIntervals),
    ByteIntervals2 = ar_intervals:cut(ByteIntervals, End),
    PaddedStart =
        case arweave_constants:get_chunk_padded_offset(Start) of
            Start -> Start;
            PaddedOffset -> PaddedOffset - ?DATA_CHUNK_SIZE
        end,
    ar_intervals:outerjoin(
        ar_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2
    ).

%% @doc Return an upper bound on the footprint offsets reachable by a weave of
%% the given byte size: the per-partition footprint capacity times the number
%% of partitions touched by the weave.
max_offset(WeaveSize) when WeaveSize > 0 ->
    NumPartitions =
        (WeaveSize + arweave_constants:partition_size() - 1) div arweave_constants:partition_size(),
    NumPartitions * get_chunks_per_partition();
max_offset(_) ->
    0.

%% @doc Return true if a chunk containing the given Offset (=< EndOffset, > StartOffset)
%% is found in the footprint record.
is_recorded(Offset, StoreID) ->
    FootprintOffset = get_offset(Offset),
    arweave_storage:is_recorded(
        FootprintOffset,
        any_packing,
        {ar_data_sync, footprint},
        StoreID
    ).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_chunks_per_partition() ->
    FootprintSize = arweave_storage_deps:get_footprint_size(),
    arweave_util:pad_to_closest_multiple_equal_or_above(
        arweave_constants:partition_size(), ?DATA_CHUNK_SIZE * FootprintSize
    ) div ?DATA_CHUNK_SIZE.

footprint_range(Partition, Footprint) ->
    FootprintSize = arweave_storage_deps:get_footprint_size(),
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

%% @doc Return the start offset of the first bucket of the sector that is
%% SectorShift sectors after the one holding the given chunk. The last
%% sector of a partition overhangs the next one, so a shift past it lands
%% on the next partition's first bucket.
get_sector_bucket_start(AbsoluteChunkEndOffset, SectorShift) ->
    SectorSize = arweave_storage_deps:get_entropy_sector_size(),
    PartitionSize = arweave_constants:partition_size(),
    PartitionRelativeOffset =
        arweave_storage_deps:get_partition_offset(AbsoluteChunkEndOffset),
    Partition = arweave_storage_deps:get_entropy_partition(AbsoluteChunkEndOffset),
    Sector = PartitionRelativeOffset div SectorSize + SectorShift,
    SectorStart = min(
        Partition * PartitionSize + Sector * SectorSize,
        (Partition + 1) * PartitionSize
    ),
    arweave_util:floor_int(
        SectorStart + ?DATA_CHUNK_SIZE - 1, ?DATA_CHUNK_SIZE
    ).
