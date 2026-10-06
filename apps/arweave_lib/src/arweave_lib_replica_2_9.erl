%%% Public replica.2.9 entropy-mapping API in the arweave_lib library app.
-module(arweave_lib_replica_2_9).

-export([get_entropy_partition/1, get_entropy_partition_range/1, get_entropy_key/3,
    get_slice_index/1, get_partition_offset/1, get_entropy_index/2]).

-include_lib("arweave_lib/include/arweave_lib_constants.hrl").

-moduledoc """
    This module handles mapping the 2.9 replica entropy to chunks and sub-chunks.

    Here's a break down of how entropy is mapped to sub-chunks.

    1. Iterate through each chunk's (e.g. chunk0) sub-chunks (e.g. s0, s1) assigning each one
       to a different entropy. This ensures that all contiguous sub-chunks are assigned to
       different entropies, maximizing the amount of work that an on-demand miner needs to do
       to pack and mine a contiguous recall range.

                   chunk0                          chunk1
                   +-----------------------------+ +-----------------------------+
                   |  s0 |  s1 |  s2 | ... | s31 | |  s0 |  s1 |  s2 | ... | s31 |
                   +-----------------------------+ +-----------------------------+
                      v     v     v           v       v      v     v          v 
    entropy index:   e0    e1    e2          e31     e32    e33   e33        e63

    2. Each 8 MiB entropy contains 1024 8 KiB slices. To finish packing the sub-chunks we
       will encipher them with the appropriate slice. A sub-chunk's slice index is
       determined by its *chunk* - each sub-chunk in a chunk is assigned to a different
       *entropy* but has the same *slice index*. A slice index and sector index are the same
       but are just used in difference contexts (e.g. slices divide up entropy, sectors
       divide up the partition). A chunk in sector 0 of the partition is enciphered with
       slice index 0 from its entropies.

         sector0   sector1  sector2           sector1023        sector0  sector1
         chunk0    c12413   c26825            cXXXXXX           chunk1   c12414
         +-------++-------++-------+         +-------+          +-------++-------+
         | | | | || | | | || | | | |   ...   | | | | |          | | | | || | | | |
         +-------++-------++-------+         +-------+          +-------++-------+
             |        |        |                 |                  |        |
         +-----------------------------------------------+      +--------------------------+
     e0: | slice0 | slice1 | slice2 | ...... | slice1023 | e32: | slice0 | slice1 | ...... 
         +-----------------------------------------------+      +--------------------------+
             |        |        |                 |                  |        |      
         +-----------------------------------------------+      +--------------------------+
     e1: | slice0 | slice1 | slice2 | ...... | slice1023 | e33: | slice0 | slice1 | ...... 
         +-----------------------------------------------+      +--------------------------+
             |        |        |                 |                  |        |      
         +-----------------------------------------------+      +--------------------------+
     e2: | slice0 | slice1 | slice2 | ...... | slice1023 | e34: | slice0 | slice1 | ...... 
         +-----------------------------------------------+      +--------------------------+
     ...     |        |        |                 |                  |        |      
         +-----------------------------------------------+      +--------------------------+
    e31: | slice0 | slice1 | slice2 | ...... | slice1023 | e63: | slice0 | slice1 | ...... 
         +-----------------------------------------------+      +--------------------------+
             |        |        |                 |                  |        |      
             v        v        v                 v                  v        v

    Glossary:

    entropy: An 8 MiB (?REPLICA_2_9_ENTROPY_SIZE) block of entropy that contains the entropy
             for 1024 sub-chunks (?REPLICA_2_9_ENTROPY_SIZE div 
             ?SUB_CHUNK_SIZE.

    slice: The 8192 byte (?SUB_CHUNK_SIZE) range of an 'entropy' that will
           be enciphered with a sub-chunk when packing to the replica_2_9 format.

    entropy partition: contains all the entropies needed to encipher all the chunks in a
                       recall partition. A recall partition is 3.6 TB (arweave_lib_constants:partition_size()),
                       but an entropy partition is slightly larger since enciphering a chunk
                       (256 KiB) requires slices from 32 different entropies (256 MiB).
                       Some of the entropies in a partition can be reused by neighboring
                       recall partitions.

    entropy index: The index of an entropy within an entropy partition. All of a chunk's
                   sub-chunks have a different entropy index.

    slice index: the index of a slice within an entropy. All of a chunk's sub-chunks have
                 the same slice index.

    sector: Each slice of an entropy is distributed to a different sector such that consecutive
            slices map to chunks that are as far as possible from each other within a
            partition. With an entropy size of 8_388_608 bytes and a slice size of 8_192 bytes,
            there are 1024 slices per entropy, which yields 1024 sectors per partition.
""".

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the 2.9 partition number the chunk with the given absolute end offset is
%% mapped to. This partition number is a part of the 2.9 replication key. It is NOT
%% the same as the arweave_lib_constants:partition_size() (3.6 TB) recall partition.
-spec get_entropy_partition(
        AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_entropy_partition(AbsoluteChunkEndOffset) ->
    BucketStart =
        arweave_lib_constants:get_chunk_bucket_start(AbsoluteChunkEndOffset),
    BucketStart div arweave_lib_constants:partition_size().

get_entropy_partition_range(PartitionNumber) ->
    %% The goal of this function is to return the minimum and maximum byte offsets that, when
    %% fed to arweave_lib_replica_2_9:get_entropy_partition/1 will yield the provided PartitinNumber.
    %% 
    %% To do this we do a rough reversal of the steps taken by
    %% arweave_lib_replica_2_9:get_entropy_partition/1:
    %% 
    %% get_entropy_partition(AbsoluteChunkEndOffset) ->
    %%    BucketStart = arweave_lib_constants:get_chunk_bucket_start(AbsoluteChunkEndOffset),
    %%    BucketStart div arweave_lib_constants:partition_size().
    %% 
    %% I say "rough reverseal" because several of the steps are not reversible (e.g. 
    %% rounding down to a bucket boundary discards data). 
    %% 
    %% 1. Reverse BucketStart div arweave_lib_constants:partition_size() to get the pick offsets
    %%    representing the byte boundaries of the recall partition.
    StartRecall = PartitionNumber * arweave_lib_constants:partition_size(),
    EndRecall = (PartitionNumber + 1) * arweave_lib_constants:partition_size(),
    %% 2. The next 3 steps reverse arweave_lib_constants:get_chunk_bucket_start/1 to yield the
    %%    first and last bytes of the entropy partition.
    %% 
    %%    Get the first bucket boundary greater than the recall boundaries. This represents
    %%    the bucket end offset of the bucket which contains the first/last byte of the
    %%    recall partition. 
    %% 
    %%    Note: by passing 0 into get_padded_offset/2 we ignore the strict data split
    %%    threshold and focus on just finding the nearest 256 KiB aligned boundary greater
    %%    than the recall boundaries.
    StartBucket1 = arweave_lib_constants:get_padded_offset(StartRecall, 0),
    EndBucket1 = arweave_lib_constants:get_padded_offset(EndRecall, 0),
    %% 3. arweave_lib_replica_2_9:get_entropy_partition/1 allocates this straddling bucket to the 
    %%    previous partition. So the start of the entropy partition is the first byte which
    %%    falls in the *next* bucket, and the end of the entropy partition is the last byte
    %%    which falls in *this* bucket. To get those bytes we'll advance to the next bucket...
    StartBucket2 = StartBucket1 + ?DATA_CHUNK_SIZE,
    EndBucket2 = EndBucket1 + ?DATA_CHUNK_SIZE,
    %% 4. ... and then get the first byte which falls in that bucket
    StartByte1 =
        arweave_lib_constants:get_chunk_byte_from_bucket_end(StartBucket2) + 1,
    EndByte1 = arweave_lib_constants:get_chunk_byte_from_bucket_end(EndBucket2),

    %% 5. Handle the special case of partition 0. Since it has no preceding partition its
    %%    byte start is 0.
    StartByte2 = case PartitionNumber of
        0 ->
            0;
        _ ->
            StartByte1
    end,

    {StartByte2, EndByte1}.

%% @doc Return the key used to generate the entropy for the 2.9 replication format.
%% RewardAddr: The address of the miner that mined the chunk.
%% AbsoluteEndOffset: The absolute end offset of the chunk.
%% SubChunkStartOffset: The start offset of the sub-chunk within the chunk. 0 is the first
%% sub-chunk of the chunk, (?DATA_CHUNK_SIZE - ?SUB_CHUNK_SIZE) is the
%% last sub-chunk of the chunk.
-spec get_entropy_key(
        RewardAddr :: binary(),
        AbsoluteEndOffset :: non_neg_integer(),
        SubChunkStartOffset :: non_neg_integer()
) -> binary().
get_entropy_key(RewardAddr, AbsoluteEndOffset, SubChunkStartOffset) ->
    Partition = get_entropy_partition(AbsoluteEndOffset),
    %% We use the key to generate a large entropy shared by many chunks.
    EntropyIndex = get_entropy_index(AbsoluteEndOffset, SubChunkStartOffset),
    crypto:hash(sha256, << Partition:256, EntropyIndex:256, RewardAddr/binary >>).

%% @doc Return the 0-based index indicating which area within a 2.9 entropy the
%% given sub-chunk is mapped to (aka slice index). Sub-chunks of the same chunk are mapped to
%% different entropies but all use the same slice index.
-spec get_slice_index(
        AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_slice_index(AbsoluteChunkEndOffset) ->
    PartitionRelativeOffset = get_partition_offset(AbsoluteChunkEndOffset),
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    (PartitionRelativeOffset div SectorSize) rem arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy().

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Return the offset of the chunk within its partition.
-spec get_partition_offset(AbsoluteChunkEndOffset :: non_neg_integer()) -> non_neg_integer().
get_partition_offset(AbsoluteChunkEndOffset) ->
    BucketStart =
        arweave_lib_constants:get_chunk_bucket_start(AbsoluteChunkEndOffset),
    Partition = get_entropy_partition(AbsoluteChunkEndOffset),
    PartitionStart = Partition * arweave_lib_constants:partition_size(),
    BucketStart - PartitionStart.

%% @doc Returns the index of the entropy containing the slice for specified chunk's sub-chunk. 
%% An entropy index is 0-based index used to identify a specific entropy within an entropy
%% partition. It is not unique - the same index will refer to different entropies in different
%% partitions and for different mining addresses. For a unique entropy identifier see
%% get_entropy_key/3.
%% 
%% The entropy index is for the 2.9 replication format.
-spec get_entropy_index(
    AbsoluteChunkEndOffset :: non_neg_integer(),
    SubChunkStartOffset :: non_neg_integer()
) -> non_neg_integer().
get_entropy_index(AbsoluteChunkEndOffset, SubChunkStartOffset) ->
    %% Assert that SubChunkStartOffset is less than ?DATA_CHUNK_SIZE
    true = SubChunkStartOffset < ?DATA_CHUNK_SIZE,
    PartitionRelativeOffset = get_partition_offset(AbsoluteChunkEndOffset),
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    %% Index of this chunk into the sector (i.e. how many chunks into the sector it falls)
    ChunkBucket = (PartitionRelativeOffset rem SectorSize) div ?DATA_CHUNK_SIZE,
    %% Index of this sub-chunk into the chunk (i.e. how many sub-chunks into the chunk it
    %% falls)
    SubChunkBucket = SubChunkStartOffset div ?SUB_CHUNK_SIZE,
    ChunkBucket * ?SUB_CHUNK_COUNT + SubChunkBucket.
