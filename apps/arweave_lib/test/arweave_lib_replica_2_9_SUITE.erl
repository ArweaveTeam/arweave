-module(arweave_lib_replica_2_9_SUITE).
-test_category([fast]).
-export([all/0, get_entropy_key/1, get_entropy_partition_range/1, slice_index_walk/1, entropy_index_walk/1, get_next_fetch_offset/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [get_entropy_key, get_entropy_partition_range, slice_index_walk, entropy_index_walk, get_next_fetch_offset].



%%%===================================================================
%%% Tests.
%%%===================================================================

get_entropy_key_test_() ->
    ar_test_util:with_mocked([
        {arweave_lib_constants, partition_size, fun() -> 2_000_000 end},
        {arweave_lib_constants, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
        {arweave_lib_constants, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
        {arweave_lib_constants, get_sub_chunks_per_replica_2_9_entropy, fun() -> 3 end}
    ],
    fun test_get_entropy_key/0, 30).

get_entropy_key(_Config) ->
    get_entropy_key_test_().



test_get_entropy_key() ->
    SubChunkSize = ?SUB_CHUNK_SIZE,
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    EntropyPartitionSize = arweave_lib_constants:get_replica_2_9_entropy_partition_size(),
    Addr = << 0:256 >>,
    ?assertEqual(32, ?SUB_CHUNK_COUNT),
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_index(1, 0)),
    EntropyKey = arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 1, 0)),
    ?assertEqual(EntropyKey,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 1, 0))),
    ?assertEqual(EntropyKey,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144, 0))),
    %% The strict data split threshold in tests is 262144 * 3. Before the strict data
    %% split threshold, the mapping works such that the chunk end offset up to but excluding
    %% the bucket border is mapped to the previous bucket.
    ?assertEqual(EntropyKey,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 2 - 1, 0))),
    EntropyKey2 = arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 2, 0)),
    ?assertNotEqual(EntropyKey, EntropyKey2),
    ?assertEqual(EntropyKey2,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 3 - 1, 0))),
    EntropyKey3 = arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 3, 0)),
    ?assertNotEqual(EntropyKey2, EntropyKey3),
    EntropyKey4 = arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 3 + 1, 0)),
    %% 262144 * 3 is the strict data split threshold so chunks ending after it are mapped
    %% to the first bucket after the threshold so the key does not equal the one of the
    %% chunk ending exactly at the threshold which is still mapped to the previous bucket.
    ?assertNotEqual(EntropyKey3, EntropyKey4),
    ?assertEqual(EntropyKey4,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 4 - 1, 0))),
    ?assertEqual(EntropyKey4,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 4, 0))),
    %% The mapping then goes this way indefinitely.
    EntropyKey5 = arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 5, 0)),
    ?assertNotEqual(EntropyKey4, EntropyKey5),
    %% Shift by sector size.
    ?assertEqual(EntropyKey4,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 3 + 1 + SectorSize, 0))),
    ?assertEqual(EntropyKey4,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 4 + SectorSize, 0))),
    ?assertEqual(EntropyKey5,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 4 + 1 + SectorSize, 0))),
    ?assertEqual(EntropyKey5,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 5 + SectorSize, 0))),

    %% Exactly equal to the recall partition size:
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_partition(262144 * 5 + SectorSize)),
    %% One greater than the recall partition size:
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(262144 * 5 + SectorSize + 1)),
    %% Greater than the entropy partition size (shouldn't matter since we map chunks
    %% based on recall partition size)
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(262144 * 6 + SectorSize + 1)),
    %% The new partition => the new entropy.
    EntropyKey6 =
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 5 + 2 * SectorSize, 0)),
    ?assertNotEqual(EntropyKey6, EntropyKey5),
    %% There is, of course, regularity within every partition.
    ?assertEqual(EntropyKey6,
            arweave_util:encode(arweave_lib_replica_2_9:get_entropy_key(Addr, 262144 * 5 + 3 * SectorSize, 0))),

    %% Test the edges of recall partition vs. entropy partition.
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_partition(arweave_lib_constants:partition_size())),    
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(EntropyPartitionSize)),
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(2 * arweave_lib_constants:partition_size())),
    ?assertEqual(2, arweave_lib_replica_2_9:get_entropy_partition(arweave_lib_constants:partition_size() + EntropyPartitionSize)),
    ?assertEqual(2, arweave_lib_replica_2_9:get_entropy_partition(3 * arweave_lib_constants:partition_size())),
    ?assertEqual(3, arweave_lib_replica_2_9:get_entropy_partition(2 * arweave_lib_constants:partition_size() + EntropyPartitionSize)),
    ?assertEqual(10, arweave_lib_replica_2_9:get_entropy_partition(11 * arweave_lib_constants:partition_size())),
    ?assertEqual(11, arweave_lib_replica_2_9:get_entropy_partition(10 * arweave_lib_constants:partition_size() + EntropyPartitionSize)),
    %% This sub-chunk offset isn't used in practice, just adding a bounds check.
    ?assertMatch(
        {'EXIT', {{badmatch, false}, _}},  catch arweave_lib_replica_2_9:get_entropy_index(0, 32 * SubChunkSize)).


get_entropy_partition_range_test_() ->
    [
        ar_test_util:with_mocked([
                {arweave_lib_constants, strict_data_split_threshold, fun() -> 700_000 end}
            ],
            fun test_get_entropy_partition_range_after_strict/0, 30),
        ar_test_util:with_mocked([
                {arweave_lib_constants, strict_data_split_threshold, fun() -> 5_000_000 end}
            ],
            fun test_get_entropy_partition_range_before_strict/0, 30)
    ].

get_entropy_partition_range(_Config) ->
    get_entropy_partition_range_test_().



test_get_entropy_partition_range_after_strict() ->
    Start0 = 0,
    End0 = 2272864,
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_partition(Start0)),
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_partition(End0)),
    ?assertEqual({Start0, End0}, arweave_lib_replica_2_9:get_entropy_partition_range(0)),

    Start1 = 2272865,
    End1 = 4370016,
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(Start1)),
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(End1)),
    ?assertEqual({Start1, End1}, arweave_lib_replica_2_9:get_entropy_partition_range(1)),

    Start2 = 4370017,
    End2 = 6205024,
    ?assertEqual(2, arweave_lib_replica_2_9:get_entropy_partition(Start2)),
    ?assertEqual(2, arweave_lib_replica_2_9:get_entropy_partition(End2)),
    ?assertEqual({Start2, End2}, arweave_lib_replica_2_9:get_entropy_partition_range(2)),
    ok.


test_get_entropy_partition_range_before_strict() ->
    Start0 = 0,
    End0 = 2359295,
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_partition(Start0)),
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_partition(End0)),
    ?assertEqual({Start0, End0}, arweave_lib_replica_2_9:get_entropy_partition_range(0)),
    
    Start1 = 2359296,
    End1 = 4456447,
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(Start1)),
    ?assertEqual(1, arweave_lib_replica_2_9:get_entropy_partition(End1)),
    ?assertEqual({Start1, End1}, arweave_lib_replica_2_9:get_entropy_partition_range(1)),
    
    Start2 = 4456448,
    End2 = 6048576,
    ?assertEqual(2, arweave_lib_replica_2_9:get_entropy_partition(Start2)),
    ?assertEqual(2, arweave_lib_replica_2_9:get_entropy_partition(End2)),
    ?assertEqual({Start2, End2}, arweave_lib_replica_2_9:get_entropy_partition_range(2)),
    ok.



%% @doc Walk sequentially through all chunks in a couple partitions and verify their slice
%% indices
slice_index_walk_test_() ->
    ar_test_util:with_mocked([
        {arweave_lib_constants, partition_size, fun() -> 8 * 262144 end},
        {arweave_lib_constants, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
        {arweave_lib_constants, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
        {arweave_lib_constants, get_sub_chunks_per_replica_2_9_entropy, fun() -> 3 end},
        {arweave_lib_constants, strict_data_split_threshold, fun() -> 3 * 262144 end}
    ],
    fun test_slice_index_walk/0, 30).

slice_index_walk(_Config) ->
    slice_index_walk_test_().



test_slice_index_walk() ->
    %% --------------------------------------------------------------------------
    %% Before the strict data split threshold:
    %% --------------------------------------------------------------------------
    
    %% Partition start
    %% Sector start
    %% All sub-chunks in a chunk have the same slice index
    arweave_lib_replica_2_9:assert_slice_index(0, [
        0
    ]),
    arweave_lib_replica_2_9:assert_slice_index(0, [
        1, 262144-1, 262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(0, [
        262144+1, 2*262144-1
    ]),
    arweave_lib_replica_2_9:assert_slice_index(0, [
        2*262144, 2*262144+1, 3*262144-1
    ]),

    %% The strict data split threshold:
    %% The end offset exactly at the strict data split threshold is mapped to the
    %% second bucket, therefore it is still the same sector size.
    arweave_lib_replica_2_9:assert_slice_index(0, [
        3*262144
    ]),

    %% --------------------------------------------------------------------------
    %% After the strict data split threshold, all end offsets are padded to a multiple of
    %% ?DATA_CHUNK_SIZE (i.e. 262144).
    %% --------------------------------------------------------------------------
    
    %% Sector start
    arweave_lib_replica_2_9:assert_slice_index(1, [
        3*262144+1, 4*262144-1, 4*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(1, [
        4*262144+1, 5*262144-1, 5*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(1, [
        5*262144+1 , 6*262144-1, 6*262144
    ]),

    %% Sector start
    arweave_lib_replica_2_9:assert_slice_index(2, [
        6*262144+1, 7*262144-1, 7*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(2, [
        7*262144+1, 8*262144-1, 8*262144
    ]),

    %% Recall partition start
    %% Sector start
    arweave_lib_replica_2_9:assert_slice_index(0, [
        8*262144+1, 9*262144-1, 9*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(0, [
        9*262144+1, 10*262144-1, 10*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(0, [
        10*262144+1, 11*262144-1, 11*262144
    ]),

    %% Sector start
    arweave_lib_replica_2_9:assert_slice_index(1, [
        11*262144+1, 12*262144-1, 12*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(1, [
        12*262144+1, 13*262144-1, 13*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(1, [
        13*262144+1, 14*262144-1, 14*262144
    ]),
    
    %% Sector start
    arweave_lib_replica_2_9:assert_slice_index(2, [
        14*262144+1, 15*262144-1, 15*262144
    ]),
    arweave_lib_replica_2_9:assert_slice_index(2, [
        15*262144+1, 16*262144-1, 16*262144
    ]),

    %% Recall partition start
    %% Sector start
    arweave_lib_replica_2_9:assert_slice_index(0, [
        16*262144+1, 17*262144-1, 17*262144
    ]),

    ?assertEqual(arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy() - 1,
            arweave_lib_replica_2_9:get_slice_index(arweave_lib_constants:partition_size())),
    ?assertEqual(0,
            arweave_lib_replica_2_9:get_slice_index(arweave_lib_constants:partition_size() + 1)),

    ok.



%% @doc Walk through every sub-chunk of each chunk and verify its entropy index and
%% entropy sub-chunk index.
entropy_index_walk_test_() ->
    ar_test_util:with_mocked([
        {arweave_lib_constants, get_replica_2_9_entropy_sector_size, fun() -> 786432 end},
        {arweave_lib_constants, get_replica_2_9_entropy_partition_size, fun() -> 2359296 end},
        {arweave_lib_constants, get_sub_chunks_per_replica_2_9_entropy, fun() -> 3 end}
    ],
    fun test_entropy_index_walk/0, 30).

entropy_index_walk(_Config) ->
    entropy_index_walk_test_().



test_entropy_index_walk() ->
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
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        0
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        1, 262144-1, 262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        262144+1, 2*262144-1
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(32, [
        2*262144, 2*262144+1, 3*262144-1
    ]),

    %% The strict data split threshold:
    arweave_lib_replica_2_9:assert_entropy_index(64, [
        3*262144
    ]),

    %% --------------------------------------------------------------------------
    %% After the strict data split threshold, all end offsets are padded to a multiple of
    %% ?DATA_CHUNK_SIZE (i.e. 262144).
    %% --------------------------------------------------------------------------
    
    %% Sector start
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        3*262144+1, 4*262144-1, 4*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(32, [
        4*262144+1, 5*262144-1, 5*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(64, [
        5*262144+1 , 6*262144-1, 6*262144
    ]),

    %% Sector start
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        6*262144+1, 7*262144-1, 7*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(32, [
        7*262144+1, 8*262144-1, 8*262144
    ]),

    %% Partition start
    %% Sector start
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        8*262144+1, 9*262144-1, 9*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(32, [
        9*262144+1, 10*262144-1, 10*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(64, [
        10*262144+1, 11*262144-1, 11*262144
    ]),

    %% Sector start
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        11*262144+1, 12*262144-1, 12*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(32, [
        12*262144+1, 13*262144-1, 13*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(64, [
        13*262144+1, 14*262144-1, 14*262144
    ]),

    %% Sector start
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        14*262144+1, 15*262144-1, 15*262144
    ]),
    arweave_lib_replica_2_9:assert_entropy_index(32, [
        15*262144+1, 16*262144-1, 16*262144
    ]),

    %% Partition start
    %% Sector start
    arweave_lib_replica_2_9:assert_entropy_index(0, [
        16*262144+1, 17*262144-1, 17*262144
    ]),


    ok.


get_next_fetch_offset_test() ->
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    {P0Start, P0End} = arweave_lib_replica_2_9:get_entropy_partition_range(0),
    Chunk = ?DATA_CHUNK_SIZE,

    ?assertEqual(P0Start + Chunk,
        arweave_lib_replica_2_9:get_next_fetch_offset(P0Start, P0Start, P0End),
        "simple advance"),

    ?assertEqual(P0Start + 1000,
        arweave_lib_replica_2_9:get_next_fetch_offset(P0Start, P0Start, P0Start + 1000),
        "simple advance, limited by End"),

    ?assertEqual(P0End,
        arweave_lib_replica_2_9:get_next_fetch_offset(P0Start + SectorSize - 1, P0Start, P0End),
        "jump to PartitionEnd"),

    ?assertEqual(P0Start + SectorSize,
        arweave_lib_replica_2_9:get_next_fetch_offset(P0Start + SectorSize - 1, P0Start, P0Start + SectorSize),
        "jump to PartitionEnd, limited by End"),

    ok.

get_next_fetch_offset(_Config) ->
    get_next_fetch_offset_test().
