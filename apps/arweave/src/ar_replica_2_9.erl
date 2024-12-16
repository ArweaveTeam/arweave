-module(ar_replica_2_9).

-export([get_partition/1, get_entropy_key/3, get_entropy_sector_size/0, 
    get_entropy_sub_chunk_index/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
    This module contains functions for the 2.9 replication format.
""".


%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the 2.9 partition number the chunk with the given absolute end offset is
%% mapped to. This partition number is a part of the 2.9 replication key. It is NOT
%% the same as the ?PARTITION_SIZE (3.6 TB) recall partition.
-spec get_partition(
		AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_partition(AbsoluteChunkEndOffset) ->
	EntropyPartitionSize = get_partition_size(),
    PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteChunkEndOffset),
    BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
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
	Partition = get_partition(AbsoluteEndOffset),
	%% We use the key to generate a large entropy shared by many chunks.
	EntropyIndex = get_entropy_id(AbsoluteEndOffset, SubChunkStartOffset),
	crypto:hash(sha256, << Partition:256, EntropyIndex:256, RewardAddr/binary >>).


%% @doc Return the 2.9 entropy sector size - the largest total size in bytes of the contiguous
%% area where the 2.9 entropy of every chunk is unique.
-spec get_entropy_sector_size() -> pos_integer().
get_entropy_sector_size() ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	get_entropy_count() * SubChunkSize.


%% @doc Return the 0-based index indicating which area within the 2.9 entropy the
%% given sub-chunk is mapped to. The given sub-chunk belongs to the chunk with
%% the absolute end offset AbsoluteChunkEndOffset. Different sub-chunks of the same chunk
%% are mapped to different entropies but each has the same index within its entropy.
-spec get_entropy_sub_chunk_index(
		AbsoluteChunkEndOffset :: non_neg_integer()
) -> non_neg_integer().
get_entropy_sub_chunk_index(AbsoluteChunkEndOffset) ->
	PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteChunkEndOffset),
    BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    SubChunkCount = ?REPLICA_2_9_ENTROPY_SIZE div SubChunkSize,
	SectorSize = get_entropy_count() * SubChunkSize,
	(BucketStart div SectorSize) rem SubChunkCount.


%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Return the total number of entropies generated per partition
%% in the 2.9 replication format.
-spec get_entropy_count() -> non_neg_integer().
get_entropy_count() ->
	Count = ?PARTITION_SIZE div ?REPLICA_2_9_ENTROPY_SIZE,
	false = ?PARTITION_SIZE rem ?REPLICA_2_9_ENTROPY_SIZE == 0,
	%% Add some extra entropies. Some entropies will be slightly underused.
	%% The additional number of entropies (the constant) is chosen depending
	%% on the PARTITION_SIZE and REPLICA_2_9_ENTROPY_SIZE constants
	%% such that the sector size (num entropies * sub-chunk size) is evenly divisible
	%% by ?DATA_CHUNK_SIZE. This proves very convenient for chunk-by-chunk syncing.
	Count + ?REPLICA_2_9_EXTRA_ENTROPY_COUNT.

%% @doc Return the 0-based index of the entropy for the sub-chunk with the given
%% absolute end offset of its chunk and its own relative start offset within this chunk.
%% The entropy id is for the 2.9 replication format.
-spec get_entropy_id(
    AbsoluteChunkEndOffset :: non_neg_integer(),
    SubChunkStartOffset :: non_neg_integer()
) -> non_neg_integer().
get_entropy_id(AbsoluteChunkEndOffset, SubChunkStartOffset) ->
    PaddedEndOffset = ar_block:get_chunk_padded_offset(AbsoluteChunkEndOffset),
    BucketStart = ar_chunk_storage:get_chunk_bucket_start(PaddedEndOffset),
    SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    SectorSize = get_entropy_count() * SubChunkSize,
    ChunkBucket = (BucketStart rem SectorSize) div ?DATA_CHUNK_SIZE,
    SubChunkCount = ?COMPOSITE_PACKING_SUB_CHUNK_COUNT,
    ChunkBucket * SubChunkCount + SubChunkStartOffset div SubChunkSize.

get_partition_size() ->
    get_entropy_count() * ?REPLICA_2_9_ENTROPY_SIZE.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_entropy_id_test() ->
    SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    EntropyCount = get_entropy_count(),
    SectorSize = EntropyCount * SubChunkSize,
    PartitionSize = EntropyCount * ?REPLICA_2_9_ENTROPY_SIZE,
    Addr = << 0:256 >>,
    ?assertEqual(32, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
    ?assertEqual(0, get_entropy_id(1, 0)),
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
    ?assertEqual(0, get_partition(262144 * 5 + SectorSize)),
    ?assertEqual(0, get_partition(262144 * 5 + SectorSize + 1)),
    ?assertEqual(1, get_partition(262144 * 6 + SectorSize + 1)),
    %% The new partition => the new entropy.
    EntropyKey6 =
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + 2 * SectorSize, 0)),
    ?assertNotEqual(EntropyKey6, EntropyKey5),
    %% There is, off course, regularity within every partition.
    ?assertEqual(EntropyKey6,
            ar_util:encode(get_entropy_key(Addr, 262144 * 5 + 3 * SectorSize, 0))),
    ?assertEqual(0, get_partition(PartitionSize)),
    ?assertEqual(1, get_partition(2 * PartitionSize)),
    ?assertEqual(2, get_partition(3 * PartitionSize)),
    ?assertEqual(9, get_partition(10 * PartitionSize)),
    %% The first bucket, but different sub-chunk offsets.
    ?assertEqual(1, get_entropy_id(0, SubChunkSize)),
    ?assertEqual(1, get_entropy_id(0, SubChunkSize + 1)),
    ?assertEqual(2, get_entropy_id(0, 2 * SubChunkSize)),
    ?assertEqual(31, get_entropy_id(0, 31 * SubChunkSize)),
    %% 262144 < 3 * 262144 (the strict data split threshold)
    ?assertEqual(31, get_entropy_id(262144, 31 * SubChunkSize)),

    %% The first offset mapped to the second bucket.
    ?assertEqual(32, get_entropy_id(2 * 262144, 0)),
    ?assertEqual(63, get_entropy_id(2 * 262144, 31 * SubChunkSize)),
    %% The first offset mapped to the third bucket.
    ?assertEqual(64, get_entropy_id(3 * 262144, 0)),
    ?assertEqual(65, get_entropy_id(3 * 262144, SubChunkSize)),
    ?assertEqual(95, get_entropy_id(3 * 262144, 31 * SubChunkSize)),
    ?assertEqual(96, EntropyCount),
    ?assertEqual(0, get_entropy_id(3 * 262144 + 1, 0)),
    ?assertEqual(31, get_entropy_id(3 * 262144 + 1, 31 * SubChunkSize)).

get_entropy_sub_chunk_index_test() ->
    SubChunkCount = ?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
    PartitionSize = get_entropy_count() * ?REPLICA_2_9_ENTROPY_SIZE,
    %% Every sub-chunk of the first chunk has index 0 in its own entropy.
    ?assertEqual(0, get_entropy_sub_chunk_index(1)),
    %% The following are buckets of the same sector.
    ?assertEqual(0, get_entropy_sub_chunk_index(262144)),
    ?assertEqual(0, get_entropy_sub_chunk_index(262144 * 2)),
    ?assertEqual(0, get_entropy_sub_chunk_index(262144 * 2 + 1)),
    ?assertEqual(0, get_entropy_sub_chunk_index(262144 * 2 + 8192)),
    %% The end offset exactly at the strict data split threshold is mapped to the
    %% second bucket, therefore it is still the same sector size.
    ?assertEqual(0, get_entropy_sub_chunk_index(262144 * 3)),
    %% This is now the second sector.
    ?assertEqual(1, get_entropy_sub_chunk_index(262144 * 4)),
    ?assertEqual(1, get_entropy_sub_chunk_index(262144 * 5)),
    ?assertEqual(1, get_entropy_sub_chunk_index(262144 * 6)),
    ?assertEqual(2, get_entropy_sub_chunk_index(262144 * 7)),
    ?assertEqual(SubChunkCount - 1,
            get_entropy_sub_chunk_index(PartitionSize)),
    ?assertEqual(0,
            get_entropy_sub_chunk_index(PartitionSize + 1)).
    