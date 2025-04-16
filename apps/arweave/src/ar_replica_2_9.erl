-module(ar_replica_2_9).

-export([get_entropy_partition/1, get_entropy_partition_range/1, get_entropy_key/3,
    get_sector_size/0, get_slice_index/1, get_partition_offset/1, sub_chunks_per_entropy/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

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
             ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

    slice: The 8192 byte (?COMPOSITE_PACKING_SUB_CHUNK_SIZE) range of an 'entropy' that will
           be enciphered with a sub-chunk when packing to the replica_2_9 format.

    entropy partition: contains all the entropies needed to encipher all the chunks in a
                       recall partition. A recall partition is 3.6 TB (ar_block:partition_size()),
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
%% the same as the ar_block:partition_size() (3.6 TB) recall partition.
-spec get_entropy_partition(
		AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_entropy_partition(AbsoluteChunkEndOffset) ->
    BucketStart = get_entropy_bucket_start(AbsoluteChunkEndOffset),
    ar_node:get_partition_number(BucketStart).

get_entropy_partition_range(PartitionNumber) ->
    %% 1. Get the bounds of the recall partition. This will yield Start/End offsets for bytes
    %%    that may fall anywhere within a chunk or bucket. We refer to these offsets as
    %%    PickOffsets.
    RecallStart = PartitionNumber * ar_block:partition_size(),
    RecallEnd = (PartitionNumber + 1) * ar_block:partition_size(),
    %% 2. Get the first bucket offset greater than PickOffset. This represents the minimum
    %%    and maximum BucketEndOffsets covered by this partition. We pass in 0 to
    %%    get_padded_offset/2 to ignore the strict data split threshold.
    BucketStart = ar_poa:get_padded_offset(RecallStart, 0),
    BucketEnd = ar_poa:get_padded_offset(RecallEnd, 0),
    %% 3. Determine the lowest and highest byte offsets that map to the boundary buckets. 
    %%    These values are the byte boundaries of this entropy partition.
    ByteStart = ar_block:get_chunk_padded_offset(BucketStart) + 1,
    ByteEnd = ar_block:get_chunk_padded_offset(BucketEnd),

    %% 4. Handle the special case of partition 0. Since it has no preceding partition its
    %%    byte start is 0.
    ByteStart2 = case PartitionNumber of
        0 ->
            0;
        _ ->
            ByteStart
    end,

    {ByteStart2, ByteEnd}.

%% @doc Return the key used to generate the entropy for the 2.9 replication format.
%% RewardAddr: The address of the miner that mined the chunk.
%% AbsoluteEndOffset: The absolute end offset of the chunk.
%% SubChunkStartOffset: The start offset of the sub-chunk within the chunk. 0 is the first
%% sub-chunk of the chunk, (?DATA_CHUNK_SIZE - ?COMPOSITE_PACKING_SUB_CHUNK_SIZE) is the
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

%% @doc Return the 2.9 entropy sector size - the largest total size in bytes of the contiguous
%% area where the 2.9 entropy of every chunk is unique.
-spec get_sector_size() -> pos_integer().
get_sector_size() ->
	?REPLICA_2_9_ENTROPY_COUNT * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

%% @doc Return the 0-based index indicating which area within a 2.9 entropy the
%% given sub-chunk is mapped to (aka slice index). Sub-chunks of the same chunk are mapped to
%% different entropies but all use the same slice index.
-spec get_slice_index(
		AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_slice_index(AbsoluteChunkEndOffset) ->
    PartitionRelativeOffset = get_partition_offset(AbsoluteChunkEndOffset),
	SectorSize = get_sector_size(),
	(PartitionRelativeOffset div SectorSize) rem sub_chunks_per_entropy().

%% @doc Return the number of sub-chunks per entropy. We'll generally create 32x entropies
%% in order to fully encipher this many chunks.
-spec sub_chunks_per_entropy() -> pos_integer().
sub_chunks_per_entropy() ->
	?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Return the start offset of the bucket containing the given chunk offset.
%% A chunk bucket is a 0-based, 256-KiB wide, 256-KiB aligned range. A chunk belongs to
%% the bucket that contains the first byte of the chunk.
-spec get_entropy_bucket_start(non_neg_integer()) -> non_neg_integer().
get_entropy_bucket_start(AbsoluteChunkEndOffset) ->
	PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteChunkEndOffset),
	PickOffset = max(0, PaddedEndOffset - ?DATA_CHUNK_SIZE),
	BucketStart = ar_util:floor_int(PickOffset, ?DATA_CHUNK_SIZE),

    true = BucketStart == ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
    
	BucketStart.

%% @doc Return the offset of the chunk within its partition.
-spec get_partition_offset(AbsoluteChunkEndOffset :: non_neg_integer()) -> non_neg_integer().
get_partition_offset(AbsoluteChunkEndOffset) ->
    BucketStart = get_entropy_bucket_start(AbsoluteChunkEndOffset),
    Partition = get_entropy_partition(AbsoluteChunkEndOffset),
    PartitionStart = Partition * ar_block:partition_size(),
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
    SectorSize = get_sector_size(),
    %% Index of this chunk into the sector (i.e. how many chunks into the sector it falls)
    ChunkBucket = (PartitionRelativeOffset rem SectorSize) div ?DATA_CHUNK_SIZE,
    %% Index of this sub-chunk into the chunk (i.e. how many sub-chunks into the chunk it
    %% falls)
    SubChunkBucket = SubChunkStartOffset div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    ChunkBucket * ?COMPOSITE_PACKING_SUB_CHUNK_COUNT + SubChunkBucket.

get_entropy_partition_size() ->
    ?REPLICA_2_9_ENTROPY_COUNT * ?REPLICA_2_9_ENTROPY_SIZE.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_entropy_key_test() ->
    SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    SectorSize = get_sector_size(),
    EntropyPartitionSize = get_entropy_partition_size(),
    Addr = << 0:256 >>,
    ?assertEqual(32, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
    ?assertEqual(?REPLICA_2_9_ENTROPY_COUNT * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE, SectorSize),
    ?assertEqual(?REPLICA_2_9_ENTROPY_COUNT * ?REPLICA_2_9_ENTROPY_SIZE, EntropyPartitionSize),
    ?assertEqual(0, get_entropy_index(1, 0)),
    EntropyKey = ar_util:encode(get_entropy_key(Addr, 1, 0)),
    ?assertEqual(EntropyKey,
            ar_util:encode(get_entropy_key(Addr, 1, 0))),
    ?assertEqual(EntropyKey,
            ar_util:encode(get_entropy_key(Addr, 262144, 0))),
    %% The strict data split threshold in tests is 262144 * 3. Before the strict data
    %% split threshold, the mapping works such that the chunk end offset up to but excluding
    %% the bucket border is mapped to the previous bucket.
    ?assertEqual(EntropyKey,
            ar_util:encode(get_entropy_key(Addr, 262144 * 2 - 1, 0))),
    EntropyKey2 = ar_util:encode(get_entropy_key(Addr, 262144 * 2, 0)),
    ?assertNotEqual(EntropyKey, EntropyKey2),
    ?assertEqual(EntropyKey2,
            ar_util:encode(get_entropy_key(Addr, 262144 * 3 - 1, 0))),
    EntropyKey3 = ar_util:encode(get_entropy_key(Addr, 262144 * 3, 0)),
    ?assertNotEqual(EntropyKey2, EntropyKey3),
    EntropyKey4 = ar_util:encode(get_entropy_key(Addr, 262144 * 3 + 1, 0)),
    %% 262144 * 3 is the strict data split threshold so chunks ending after it are mapped
    %% to the first bucket after the threshold so the key does not equal the one of the
    %% chunk ending exactly at the threshold which is still mapped to the previous bucket.
    ?assertNotEqual(EntropyKey3, EntropyKey4),
    ?assertEqual(EntropyKey4,
            ar_util:encode(get_entropy_key(Addr, 262144 * 4 - 1, 0))),
    ?assertEqual(EntropyKey4,
            ar_util:encode(get_entropy_key(Addr, 262144 * 4, 0))),
    %% The mapping then goes this way indefinitely.
    EntropyKey5 = ar_util:encode(get_entropy_key(Addr, 262144 * 5, 0)),
    ?assertNotEqual(EntropyKey4, EntropyKey5),
    %% Shift by sector size.
    ?assertEqual(EntropyKey4,
            ar_util:encode(get_entropy_key(Addr, 262144 * 3 + 1 + SectorSize, 0))),
    ?assertEqual(EntropyKey4,
            ar_util:encode(get_entropy_key(Addr, 262144 * 4 + SectorSize, 0))),
    ?assertEqual(EntropyKey5,
            ar_util:encode(get_entropy_key(Addr, 262144 * 4 + 1 + SectorSize, 0))),
    ?assertEqual(EntropyKey5,
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + SectorSize, 0))),

    %% Exactly equal to the recall partition size:
    ?assertEqual(0, get_entropy_partition(262144 * 5 + SectorSize)),
    %% One greater than the recall partition size:
    ?assertEqual(1, get_entropy_partition(262144 * 5 + SectorSize + 1)),
    %% Greater than the entropy partition size (shouldn't matter since we map chunks
    %% based on recall partition size)
    ?assertEqual(1, get_entropy_partition(262144 * 6 + SectorSize + 1)),
    %% The new partition => the new entropy.
    EntropyKey6 =
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + 2 * SectorSize, 0)),
    ?assertNotEqual(EntropyKey6, EntropyKey5),
    %% There is, of course, regularity within every partition.
    ?assertEqual(EntropyKey6,
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + 3 * SectorSize, 0))),

    %% Test the edges of recall partition vs. entropy partition.
    ?assertEqual(0, get_entropy_partition(ar_block:partition_size())),    
    ?assertEqual(1, get_entropy_partition(EntropyPartitionSize)),
    ?assertEqual(1, get_entropy_partition(2 * ar_block:partition_size())),
    ?assertEqual(2, get_entropy_partition(ar_block:partition_size() + EntropyPartitionSize)),
    ?assertEqual(2, get_entropy_partition(3 * ar_block:partition_size())),
    ?assertEqual(3, get_entropy_partition(2 * ar_block:partition_size() + EntropyPartitionSize)),
    ?assertEqual(10, get_entropy_partition(11 * ar_block:partition_size())),
    ?assertEqual(11, get_entropy_partition(10 * ar_block:partition_size() + EntropyPartitionSize)),
    %% This sub-chunk offset isn't used in practice, just adding a bounds check.
    ?assertMatch(
        {'EXIT', {{badmatch, false}, _}},  catch get_entropy_index(0, 32 * SubChunkSize)).

get_entropy_partition_range_test_() ->
    ar_test_node:test_with_mocked_functions([
            {ar_block, partition_size, fun() -> 2_000_000 end},
            {ar_block, strict_data_split_threshold, fun() -> 700_000 end}
        ],
        fun test_get_entropy_partition_range/0, 30).

test_get_entropy_partition_range() ->
    %% 2272864 is the highest byte offset that maps to 2091752 which is the first
    %% BucketEndOffset after the p0 recall partition end of 2000000.
	?assertEqual({0, 2272864}, get_entropy_partition_range(0)),
	?assertEqual({2272865, 4370016}, get_entropy_partition_range(1)),
	?assertEqual({4370017, 6205024}, get_entropy_partition_range(2)),
	ok.

%% @doc Walk sequentially through all chunks in a couple partitions and verify their slice
%% indices
slice_index_walk_test() ->
    ?assertEqual(3 * 8192, ?REPLICA_2_9_ENTROPY_SIZE),
    ?assertEqual(3 * 262144, ar_block:strict_data_split_threshold()),
    ?assertEqual(3 * 262144, get_sector_size()),
    ?assertEqual(9 * 262144, get_entropy_partition_size()),

    %% Entropies per partition.
    ?assertEqual(96, ?REPLICA_2_9_ENTROPY_COUNT),
    %% Sub-chunks per entropy.
    ?assertEqual(3, sub_chunks_per_entropy()),

    %% --------------------------------------------------------------------------
    %% Before the strict data split threshold:
    %% --------------------------------------------------------------------------
    
    %% Partition start
    %% Sector start
    %% All sub-chunks in a chunk have the same slice index
    assert_slice_index(0, [
        0
    ]),
    assert_slice_index(0, [
        1, 262144-1, 262144
    ]),
    assert_slice_index(0, [
        262144+1, 2*262144-1
    ]),
    assert_slice_index(0, [
        2*262144, 2*262144+1, 3*262144-1
    ]),

    %% The strict data split threshold:
    %% The end offset exactly at the strict data split threshold is mapped to the
    %% second bucket, therefore it is still the same sector size.
    assert_slice_index(0, [
        3*262144
    ]),

    %% --------------------------------------------------------------------------
    %% After the strict data split threshold, all end offsets are padded to a multiple of
    %% ?DATA_CHUNK_SIZE (i.e. 262144).
    %% --------------------------------------------------------------------------
    
    %% Sector start
    assert_slice_index(1, [
        3*262144+1, 4*262144-1, 4*262144
    ]),
    assert_slice_index(1, [
        4*262144+1, 5*262144-1, 5*262144
    ]),
    assert_slice_index(1, [
        5*262144+1 , 6*262144-1, 6*262144
    ]),

    %% Sector start
    assert_slice_index(2, [
        6*262144+1, 7*262144-1, 7*262144
    ]),
    assert_slice_index(2, [
        7*262144+1, 8*262144-1, 8*262144
    ]),

    %% Recall partition start
    %% Sector start
    assert_slice_index(0, [
        8*262144+1, 9*262144-1, 9*262144
    ]),
    assert_slice_index(0, [
        9*262144+1, 10*262144-1, 10*262144
    ]),
    assert_slice_index(0, [
        10*262144+1, 11*262144-1, 11*262144
    ]),

    %% Sector start
    assert_slice_index(1, [
        11*262144+1, 12*262144-1, 12*262144
    ]),
    assert_slice_index(1, [
        12*262144+1, 13*262144-1, 13*262144
    ]),
    assert_slice_index(1, [
        13*262144+1, 14*262144-1, 14*262144
    ]),
    
    %% Sector start
    assert_slice_index(2, [
        14*262144+1, 15*262144-1, 15*262144
    ]),
    assert_slice_index(2, [
        15*262144+1, 16*262144-1, 16*262144
    ]),

    %% Recall partition start
    %% Sector start
    assert_slice_index(0, [
        16*262144+1, 17*262144-1, 17*262144
    ]),

    ?assertEqual(sub_chunks_per_entropy() - 1,
            get_slice_index(ar_block:partition_size())),
    ?assertEqual(0,
            get_slice_index(ar_block:partition_size() + 1)),

    ok.

assert_slice_index(_ExpectedIndex, []) ->
    ok; 
assert_slice_index(ExpectedIndex, [AbsoluteChunkByteOffset | Rest]) ->
    ?assertEqual(
        ExpectedIndex, get_slice_index(AbsoluteChunkByteOffset),
        lists:flatten(io_lib:format("get_slice_index(~p)", 
            [AbsoluteChunkByteOffset]))
    ),
    assert_slice_index(ExpectedIndex, Rest).


%% @doc Walk through every sub-chunk of each chunk and verify its entropy index and
%% entropy sub-chunk index.
entropy_index_walk_test() ->
    ?assertEqual(3 * 8192, ?REPLICA_2_9_ENTROPY_SIZE),
    ?assertEqual(3 * 262144, ar_block:strict_data_split_threshold()),
    ?assertEqual(3 * 262144, get_sector_size()),
    ?assertEqual(96, ?REPLICA_2_9_ENTROPY_COUNT),
    ?assertEqual(9 * 262144, get_entropy_partition_size()),
    

    %% assert_entropy_index takes a list of chunk end offsets and verifies the entropy
    %% index for each sub-chunk in the chunk. The first argument is the expected entropy
    %% index for the first sub-chunk in the chunk, for each subsequent sub-chunk the
    %% expected index is incremented by 1.
    %% 
    %% The sector size determines the number of entropy indices. During tests the sector
    %% size is 3*262144, so the total number of entropy indices is 3*262144 / 8192 = 96 (one
    %% for each sub-chunk in each sector).
    
    %% In tests the strict data split threshold is 262144 * 3, before that offset chunks
    %% were not padded. So each provided end offset is taken as is. After the threshold each
    %% offset is padded to a multiple of ?DATA_CHUNK_SIZE (i.e. 262144) off of the threshold
    %% value.

    %% --------------------------------------------------------------------------
    %% Before the strict data split threshold:
    %% --------------------------------------------------------------------------
    
    %% Partition start
    %% Sector start
    assert_entropy_index(0, [
        0
    ]),
    assert_entropy_index(0, [
        1, 262144-1, 262144
    ]),
    assert_entropy_index(0, [
        262144+1, 2*262144-1
    ]),
    assert_entropy_index(32, [
        2*262144, 2*262144+1, 3*262144-1
    ]),

    %% The strict data split threshold:
    assert_entropy_index(64, [
        3*262144
    ]),

    %% --------------------------------------------------------------------------
    %% After the strict data split threshold, all end offsets are padded to a multiple of
    %% ?DATA_CHUNK_SIZE (i.e. 262144).
    %% --------------------------------------------------------------------------
    
    %% Sector start
    assert_entropy_index(0, [
        3*262144+1, 4*262144-1, 4*262144
    ]),
    assert_entropy_index(32, [
        4*262144+1, 5*262144-1, 5*262144
    ]),
    assert_entropy_index(64, [
        5*262144+1 , 6*262144-1, 6*262144
    ]),

    %% Sector start
    assert_entropy_index(0, [
        6*262144+1, 7*262144-1, 7*262144
    ]),
    assert_entropy_index(32, [
        7*262144+1, 8*262144-1, 8*262144
    ]),

    %% Partition start
    %% Sector start
    assert_entropy_index(0, [
        8*262144+1, 9*262144-1, 9*262144
    ]),
    assert_entropy_index(32, [
        9*262144+1, 10*262144-1, 10*262144
    ]),
    assert_entropy_index(64, [
        10*262144+1, 11*262144-1, 11*262144
    ]),

    %% Sector start
    assert_entropy_index(0, [
        11*262144+1, 12*262144-1, 12*262144
    ]),
    assert_entropy_index(32, [
        12*262144+1, 13*262144-1, 13*262144
    ]),
    assert_entropy_index(64, [
        13*262144+1, 14*262144-1, 14*262144
    ]),

    %% Sector start
    assert_entropy_index(0, [
        14*262144+1, 15*262144-1, 15*262144
    ]),
    assert_entropy_index(32, [
        15*262144+1, 16*262144-1, 16*262144
    ]),

    %% Partition start
    %% Sector start
    assert_entropy_index(0, [
        16*262144+1, 17*262144-1, 17*262144
    ]),


    ok.

assert_entropy_index(_ExpectedIndex, []) ->
    ok; 
assert_entropy_index(ExpectedIndex, [AbsoluteChunkByteOffset | Rest]) ->
    walk_sub_chunks(ExpectedIndex, AbsoluteChunkByteOffset, 0),
    assert_entropy_index(ExpectedIndex, Rest).

walk_sub_chunks(_ExpectedIndex, _AbsoluteChunkByteOffset, SubChunkStartOffset)
    when SubChunkStartOffset >= ?DATA_CHUNK_SIZE ->
        ok;
walk_sub_chunks(ExpectedIndex, AbsoluteChunkByteOffset, SubChunkStartOffset) ->
    ?assertEqual(
        ExpectedIndex, get_entropy_index(AbsoluteChunkByteOffset, SubChunkStartOffset),
        lists:flatten(io_lib:format("get_entropy_index(~p, ~p)", 
            [AbsoluteChunkByteOffset, SubChunkStartOffset]))
    ),
    ?assertEqual(
        ExpectedIndex, get_entropy_index(AbsoluteChunkByteOffset, SubChunkStartOffset+1),
        lists:flatten(io_lib:format("get_entropy_index(~p, ~p)", 
            [AbsoluteChunkByteOffset, SubChunkStartOffset+1]))
    ),
    ?assertEqual(
        ExpectedIndex, get_entropy_index(AbsoluteChunkByteOffset,  SubChunkStartOffset+8192-1),
        lists:flatten(io_lib:format("get_entropy_index(~p, ~p)", 
            [AbsoluteChunkByteOffset,  SubChunkStartOffset+8192-1]))
    ),
    walk_sub_chunks(ExpectedIndex+1, AbsoluteChunkByteOffset, SubChunkStartOffset+8192).