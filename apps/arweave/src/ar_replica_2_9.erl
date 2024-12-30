-module(ar_replica_2_9).

-export([get_entropy_partition/1, get_entropy_key/3, get_sector_size/0, 
    get_slice_index/1]).

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
       *entropy* but has the same *slice index*.

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
                       recall partition. A recall partition is 3.6 TB (?PARTITION_SIZE),
                       but an entropy partition is slightly larger since enciphering a chunk
                       (256 KiB) requires slices from 32 different entropies (256 MiB).
                       Some of the entropies in a partition can be reused by neighboring
                       recall partitions.

    entropy index: The index of an entropy within an entropy partition. All of a chunk's
                   sub-chunks have a different entropy index.

    slice index: the index of a slice within an entropy. All of a chunk's sub-chunks have
                 the same slice index.

    sector: 1/32 the size of an entropy partition. 
        """.


%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the 2.9 partition number the chunk with the given absolute end offset is
%% mapped to. This partition number is a part of the 2.9 replication key. It is NOT
%% the same as the ?PARTITION_SIZE (3.6 TB) recall partition.
-spec get_entropy_partition(
		AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_entropy_partition(AbsoluteChunkEndOffset) ->
	EntropyPartitionSize = get_entropy_partition_size(),
    BucketStart = get_entropy_bucket_start(AbsoluteChunkEndOffset),
	BucketStart div EntropyPartitionSize.

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
	get_entropy_count() * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE.

%% @doc Return the 0-based index indicating which area within a 2.9 entropy the
%% given sub-chunk is mapped to (aka slice index). Sub-chunks of the same chunk are mapped to
%% different entropies but all use the same slice index.
-spec get_slice_index(
		AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_slice_index(AbsoluteChunkEndOffset) ->
	BucketStart = get_entropy_bucket_start(AbsoluteChunkEndOffset),
    SubChunkCount = ?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	SectorSize = get_sector_size(),
	(BucketStart div SectorSize) rem SubChunkCount.


%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Return the start offset of the bucket containing the given chunk offset.
%% A chunk bucket a 0-based, 256-KiB wide, 256-KiB aligned range that fully contains a chunk.
-spec get_entropy_bucket_start(non_neg_integer()) -> non_neg_integer().
get_entropy_bucket_start(AbsoluteChunkEndOffset) ->
	PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteChunkEndOffset),
	PickOffset = max(0, PaddedEndOffset - ?DATA_CHUNK_SIZE),
	BucketStart = PickOffset - PickOffset rem ?DATA_CHUNK_SIZE,

    true = BucketStart == ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
    
	BucketStart.

%% @doc Return the total number of entropies generated per partition
%% in the 2.9 replication format.
-spec get_entropy_count() -> non_neg_integer().
get_entropy_count() ->
	Count = ?PARTITION_SIZE div ?REPLICA_2_9_ENTROPY_SIZE,
	false = ?PARTITION_SIZE rem ?REPLICA_2_9_ENTROPY_SIZE == 0,
	%% Add some extra entropies so that the total count is multiple of 32. There are
    %% 32 sub-chunks per chunk, and each sub-chunk is enciphered with a different entropy. 
    %% Ensuring we have a multiple of 32 entropies proves very convenient for chunk-by-chunk
    %% syncing.
    %% 
	%% The additional number of entropies (the constant) is chosen depending
	%% on the PARTITION_SIZE and REPLICA_2_9_ENTROPY_SIZE constants
	%% such that the sector size (num entropies * sub-chunk size) is evenly divisible
	%% by ?DATA_CHUNK_SIZE. This proves very convenient for chunk-by-chunk syncing.
	Count + ?REPLICA_2_9_EXTRA_ENTROPY_COUNT.

%% @doc Returns the index of the entropy containing the slice for specified chunk's sub-chunk. 
%% An entropy index is 0-based index used to identify a specific entropy within an entropy
%% partition. It is not unique - the same id will refer to different entropies in differen
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
    BucketStart = get_entropy_bucket_start(AbsoluteChunkEndOffset),
    SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    SectorSize = get_sector_size(),
    %% Index of this chunk into the sector (i.e. how many chunks into the sector it falls)
    ChunkBucket = (BucketStart rem SectorSize) div ?DATA_CHUNK_SIZE,
    %% Index of this sub-chunk into the chunk (i.e. how many sub-chunks into the chunk it
    %% falls)
    SubChunkBucket = SubChunkStartOffset div SubChunkSize,
    ChunkBucket * ?COMPOSITE_PACKING_SUB_CHUNK_COUNT + SubChunkBucket.

get_entropy_partition_size() ->
    get_entropy_count() * ?REPLICA_2_9_ENTROPY_SIZE.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_entropy_key_test() ->
    SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    SectorSize = get_sector_size(),
    PartitionSize = get_entropy_partition_size(),
    Addr = << 0:256 >>,
    ?assertEqual(32, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
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
    %% The partition changes at this point (> 9 chunk sizes.)
    ?assertEqual(0, get_entropy_partition(262144 * 5 + SectorSize)),
    ?assertEqual(0, get_entropy_partition(262144 * 5 + SectorSize + 1)),
    ?assertEqual(1, get_entropy_partition(262144 * 6 + SectorSize + 1)),
    %% The new partition => the new entropy.
    EntropyKey6 =
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + 2 * SectorSize, 0)),
    ?assertNotEqual(EntropyKey6, EntropyKey5),
    %% There is, off course, regularity within every partition.
    ?assertEqual(EntropyKey6,
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + 3 * SectorSize, 0))),
    ?assertEqual(0, get_entropy_partition(PartitionSize)),
    ?assertEqual(1, get_entropy_partition(2 * PartitionSize)),
    ?assertEqual(2, get_entropy_partition(3 * PartitionSize)),
    ?assertEqual(9, get_entropy_partition(10 * PartitionSize)),
    %% This sub-chunk offset isn't used in practice, just adding a bounds check.
    ?assertMatch(
        {'EXIT', {{badmatch, false}, _}},  catch get_entropy_index(0, 32 * SubChunkSize)).

%% @doc Walk sequentially through all chunks in a couple partitions and verify their slice
%% indices
slice_index_walk_test() ->
    ?assertEqual(3 * 8192, ?REPLICA_2_9_ENTROPY_SIZE),
    ?assertEqual(3 * 262144, ?STRICT_DATA_SPLIT_THRESHOLD),
    ?assertEqual(3 * 262144, get_sector_size()),
    ?assertEqual(9 * 262144, get_entropy_partition_size()),

    %% Entropies per partition.
    ?assertEqual(96, get_entropy_count()),
    %% Sub-chunks per entropy.
    SubChunkCount = ?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    ?assertEqual(3, SubChunkCount),

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
    assert_slice_index(2, [
        8*262144+1, 9*262144-1, 9*262144
    ]),

    %% Partition start
    %% Sector start
    assert_slice_index(0, [
        9*262144+1, 10*262144-1, 10*262144
    ]),
    assert_slice_index(0, [
        10*262144+1, 11*262144-1, 11*262144
    ]),
    assert_slice_index(0, [
        11*262144+1, 12*262144-1, 12*262144
    ]),

    %% Sector start
    assert_slice_index(1, [
        12*262144+1, 13*262144-1, 13*262144
    ]),

    ?assertEqual(SubChunkCount - 1,
            get_slice_index(get_entropy_partition_size())),
    ?assertEqual(0,
            get_slice_index(get_entropy_partition_size() + 1)),

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


% @doc Walk through every sub-chunk of each chunk and verify its entropy index and
% entropy sub-chunk index.
entropy_index_walk_test() ->
    ?assertEqual(3 * 8192, ?REPLICA_2_9_ENTROPY_SIZE),
    ?assertEqual(3 * 262144, ?STRICT_DATA_SPLIT_THRESHOLD),
    ?assertEqual(3 * 262144, get_sector_size()),
    ?assertEqual(96, get_entropy_count()),
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
    assert_entropy_index(64, [
        8*262144+1, 9*262144-1, 9*262144
    ]),

    %% Partition start
    %% Sector start
    assert_entropy_index(0, [
        9*262144+1, 10*262144-1, 10*262144
    ]),
    assert_entropy_index(32, [
        10*262144+1, 11*262144-1, 11*262144
    ]),
    assert_entropy_index(64, [
        11*262144+1, 12*262144-1, 12*262144
    ]),

    %% Sector start
    assert_entropy_index(0, [
        12*262144+1, 13*262144-1, 13*262144
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