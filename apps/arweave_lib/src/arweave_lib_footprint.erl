%%% Public replica.2.9 footprint geometry in the arweave_lib library app: maps
%%% chunk offsets to footprint offsets, locations and buckets, and footprint
%%% intervals to byte intervals.
-module(arweave_lib_footprint).

-export([
    get_footprint_offset/1,
    get_padded_offset_from_footprint_offset/1,
    get_footprint/1,
    get_footprint_location/1,
    get_location_from_footprint_offset/1,
    get_footprint_bucket/1,
    get_footprint_range/2,
    footprint_intervals_to_byte_intervals/1,
    footprint_intervals_to_byte_intervals/3,
    byte_intervals_to_footprint_intervals/3,
    max_footprint_offset/1,
    get_chunks_per_partition/0,
    get_next_sector_start/1,
    get_sector_bucket_start/2
]).

-include_lib("arweave_lib/include/arweave_lib_constants.hrl").

%% @doc Get the offset of a chunk in the footprint record.
get_footprint_offset(Offset) ->
    PaddedOffset = arweave_lib_constants:get_chunk_padded_offset(Offset),
    FootprintSize =
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy(),
    FootprintsPerPartition =
        arweave_lib_constants:get_replica_2_9_footprints_per_partition(),

    ChunksPerPartition = get_chunks_per_partition(),
    Partition =
        arweave_lib_replica_2_9:get_entropy_partition(PaddedOffset),
    PartitionOffset =
        (PaddedOffset - Partition * arweave_lib_constants:partition_size()) div ?DATA_CHUNK_SIZE - 1,

    %% Which footprint within the partition
    Footprint = PartitionOffset rem FootprintsPerPartition,
    %% Position within the footprint
    FootprintOffset = PartitionOffset div FootprintsPerPartition,
    Partition * ChunksPerPartition + Footprint * FootprintSize + FootprintOffset + 1.

%% @doc Return the largest end offset of the chunk that maps to the given footprint offset.
get_padded_offset_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    FootprintSize =
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy(),
    ChunksPerPartition = get_chunks_per_partition(),
    Partition = Start div ChunksPerPartition,
    FootprintsPerPartition =
        arweave_lib_constants:get_replica_2_9_footprints_per_partition(),
    PartitionStart = Partition * ChunksPerPartition,
    Footprint = (Start - PartitionStart) div FootprintSize,
    InFootprintOffset = (Start - PartitionStart) rem FootprintSize,
    EndOffset =
        Partition * arweave_lib_constants:partition_size() +
            (InFootprintOffset * FootprintsPerPartition + (Footprint + 1)) * ?DATA_CHUNK_SIZE,
    arweave_lib_constants:get_chunk_padded_offset(EndOffset).

%% @doc Get the chunk's footprint's number, >= 0, < the maximum number of footprints
%% in a partition.
get_footprint(Offset) ->
    EntropyIndex = arweave_lib_replica_2_9:get_entropy_index(Offset, 0),
    EntropyIndex div ?SUB_CHUNK_COUNT.

%% @doc Return the bucket end offset of the first chunk of the sector after
%% the one holding the given chunk.
get_next_sector_start(AbsoluteChunkEndOffset) ->
    get_sector_bucket_start(AbsoluteChunkEndOffset, 1) + ?DATA_CHUNK_SIZE.

%% @doc Get the replica 2.9 partition and footprint containing a chunk offset.
get_footprint_location(Offset) ->
    {
        arweave_lib_replica_2_9:get_entropy_partition(Offset),
        get_footprint(Offset)
    }.

%% @doc The footprint location of a footprint offset: the
%% {Partition, Footprint} pair get_location/1 derives from a byte offset,
%% computed in footprint space. It cannot go through get_location/1: a
%% partition's entropy covers a few offsets more than the partition has
%% chunks, those map to chunks of the next partition, and telling them apart
%% is what comparing the two answers is for.
get_location_from_footprint_offset(FootprintOffset) ->
    Start = FootprintOffset - 1,
    ChunksPerPartition = get_chunks_per_partition(),
    FootprintSize =
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy(),
    Partition = Start div ChunksPerPartition,
    {Partition, (Start - Partition * ChunksPerPartition) div FootprintSize}.

%% @doc Get the footprint bucket number of a chunk.
get_footprint_bucket(Offset) ->
    get_footprint_offset(Offset) div ?NETWORK_FOOTPRINT_BUCKET_SIZE.

%% @doc Convert footprint intervals to byte intervals.
footprint_intervals_to_byte_intervals(FootprintIntervals) ->
    do_footprint_intervals_to_byte_intervals(
        arweave_lib_intervals:to_list(FootprintIntervals),
        arweave_lib_intervals:new()
    ).

%% @doc Convert footprint intervals to byte intervals, cut at End, and drop
%% bytes before the chunk containing Start.
footprint_intervals_to_byte_intervals(FootprintIntervals, Start, End) ->
    ByteIntervals = footprint_intervals_to_byte_intervals(FootprintIntervals),
    ByteIntervals2 = arweave_lib_intervals:cut(ByteIntervals, End),
    PaddedStart =
        case arweave_lib_constants:get_chunk_padded_offset(Start) of
            Start -> Start;
            PaddedOffset -> PaddedOffset - ?DATA_CHUNK_SIZE
        end,
    arweave_lib_intervals:outerjoin(
        arweave_lib_intervals:from_list([{PaddedStart, -1}]), ByteIntervals2
    ).

%% @doc Return the offsets of the footprint whose chunks overlap ByteIntervals,
%% the inverse of footprint_intervals_to_byte_intervals/1.
byte_intervals_to_footprint_intervals(ByteIntervals, Partition, Footprint) ->
    {Start, End} = get_footprint_range(Partition, Footprint),
    arweave_lib_intervals:fold(
        fun({ByteEnd, ByteStart}, Acc) ->
            %% Each offset maps to one chunk in byte space
            %% (chunk_byte_interval/1), and a footprint's chunks follow the
            %% order of its offsets, so the ones overlapping
            %% (ByteStart, ByteEnd] are the offsets from First up to, not
            %% including, Next.
            First = first_chunk_ending_after(ByteStart, Start, End),
            Next = first_chunk_starting_from(ByteEnd, Start, End),
            case Next > First of
                true -> arweave_lib_intervals:add(Acc, Next - 1, First - 1);
                false -> Acc
            end
        end,
        arweave_lib_intervals:new(),
        ByteIntervals
    ).

%% @doc Return the first offset in (Start, End] whose chunk ends after Byte,
%% or End + 1 when none does.
first_chunk_ending_after(Byte, Start, End) ->
    first_footprint_offset(
        fun(Offset) ->
            {ChunkEnd, _ChunkStart} = chunk_byte_interval(Offset),
            ChunkEnd > Byte
        end,
        Start + 1,
        End + 1
    ).

%% @doc Return the first offset in (Start, End] whose chunk starts at or after
%% Byte, or End + 1 when none does.
first_chunk_starting_from(Byte, Start, End) ->
    first_footprint_offset(
        fun(Offset) ->
            {_ChunkEnd, ChunkStart} = chunk_byte_interval(Offset),
            ChunkStart >= Byte
        end,
        Start + 1,
        End + 1
    ).

%% @doc Return the first offset in [Low, High) satisfying Pred, or High; Pred
%% must stay true once true, as it does for a footprint's chunks.
first_footprint_offset(_Pred, Low, High) when Low >= High ->
    High;
first_footprint_offset(Pred, Low, High) ->
    Mid = (Low + High) div 2,
    case Pred(Mid) of
        true -> first_footprint_offset(Pred, Low, Mid);
        false -> first_footprint_offset(Pred, Mid + 1, High)
    end.

%% @doc Return the byte interval {End, Start} of the chunk a footprint offset
%% maps to: the 256 KiB bucket ending at
%% get_padded_offset_from_footprint_offset/1.
chunk_byte_interval(FootprintOffset) ->
    End = get_padded_offset_from_footprint_offset(FootprintOffset),
    {End, End - ?DATA_CHUNK_SIZE}.

%% @doc Return an upper bound on the footprint offsets reachable by a weave of
%% the given byte size: the per-partition footprint capacity times the number
%% of partitions touched by the weave.
max_footprint_offset(WeaveSize) when WeaveSize > 0 ->
    NumPartitions =
        (WeaveSize + arweave_lib_constants:partition_size() - 1) div arweave_lib_constants:partition_size(),
    NumPartitions * get_chunks_per_partition();
max_footprint_offset(_) ->
    0.

get_chunks_per_partition() ->
    FootprintSize =
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy(),
    arweave_lib_util:pad_to_closest_multiple_equal_or_above(
        arweave_lib_constants:partition_size(), ?DATA_CHUNK_SIZE * FootprintSize
    ) div ?DATA_CHUNK_SIZE.

get_footprint_range(Partition, Footprint) ->
    FootprintSize =
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy(),
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
    {ChunkEnd, ChunkStart} = chunk_byte_interval(Start + 1),
    Intervals2 = arweave_lib_intervals:add(Intervals, ChunkEnd, ChunkStart),
    do_footprint_intervals_to_byte_intervals(Start + 1, End, Intervals2).

%% @doc Return the start offset of the first bucket of the sector that is
%% SectorShift sectors after the one holding the given chunk. The last
%% sector of a partition overhangs the next one, so a shift past it lands
%% on the next partition's first bucket.
get_sector_bucket_start(AbsoluteChunkEndOffset, SectorShift) ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    PartitionSize = arweave_lib_constants:partition_size(),
    PartitionRelativeOffset =
        arweave_lib_replica_2_9:get_partition_offset(
            AbsoluteChunkEndOffset
        ),
    Partition = arweave_lib_replica_2_9:get_entropy_partition(
        AbsoluteChunkEndOffset
    ),
    Sector = PartitionRelativeOffset div SectorSize + SectorShift,
    SectorStart = min(
        Partition * PartitionSize + Sector * SectorSize,
        (Partition + 1) * PartitionSize
    ),
    arweave_lib_util:floor_int(
        SectorStart + ?DATA_CHUNK_SIZE - 1, ?DATA_CHUNK_SIZE
    ).
