-module(arweave_lib_replica_2_9_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_lib/include/arweave_lib_constants.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        entropy_key,
        entropy_partition_range_after_strict,
        entropy_partition_range_before_strict,
        slice_index_walk,
        entropy_index_walk
    ].

init_per_testcase(_, Config) ->
    meck:new(arweave_lib_constants, [passthrough]),
    Config.

end_per_testcase(_, _) ->
    meck:unload(arweave_lib_constants).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Chunks in the same bucket share an entropy key, and the key changes
%% at bucket, strict data split threshold and partition boundaries.
entropy_key(_) ->
    mock_constants([
        {partition_size, 2_000_000},
        {get_replica_2_9_entropy_sector_size, 786432},
        {get_replica_2_9_entropy_partition_size, 2359296},
        {get_sub_chunks_per_replica_2_9_entropy, 3}
    ]),
    SectorSize = arweave_lib_constants:get_replica_2_9_entropy_sector_size(),
    EntropyPartitionSize =
        arweave_lib_constants:get_replica_2_9_entropy_partition_size(),
    PartitionSize = arweave_lib_constants:partition_size(),
    Key = fun(Offset) ->
        arweave_lib_replica_2_9:get_entropy_key(<<0:256>>, Offset, 0)
    end,
    Partition = fun arweave_lib_replica_2_9:get_entropy_partition/1,
    ?assertEqual(32, ?SUB_CHUNK_COUNT),
    ?assertEqual(0, arweave_lib_replica_2_9:get_entropy_index(1, 0)),
    EntropyKey = Key(1),
    ?assertEqual(EntropyKey, Key(1)),
    ?assertEqual(EntropyKey, Key(262144)),
    %% The strict data split threshold in tests is 262144 * 3. Before the
    %% threshold, a chunk end offset up to but excluding the bucket border is
    %% mapped to the previous bucket.
    ?assertEqual(EntropyKey, Key(262144 * 2 - 1)),
    EntropyKey2 = Key(262144 * 2),
    ?assertNotEqual(EntropyKey, EntropyKey2),
    ?assertEqual(EntropyKey2, Key(262144 * 3 - 1)),
    EntropyKey3 = Key(262144 * 3),
    ?assertNotEqual(EntropyKey2, EntropyKey3),
    %% Chunks ending after the threshold are mapped to the first bucket after
    %% it, so their key differs from the key of the chunk ending exactly at
    %% the threshold, which is still mapped to the previous bucket.
    EntropyKey4 = Key(262144 * 3 + 1),
    ?assertNotEqual(EntropyKey3, EntropyKey4),
    ?assertEqual(EntropyKey4, Key(262144 * 4 - 1)),
    ?assertEqual(EntropyKey4, Key(262144 * 4)),
    %% The mapping then continues the same way.
    EntropyKey5 = Key(262144 * 5),
    ?assertNotEqual(EntropyKey4, EntropyKey5),
    %% Shift by the sector size.
    ?assertEqual(EntropyKey4, Key(262144 * 3 + 1 + SectorSize)),
    ?assertEqual(EntropyKey4, Key(262144 * 4 + SectorSize)),
    ?assertEqual(EntropyKey5, Key(262144 * 4 + 1 + SectorSize)),
    ?assertEqual(EntropyKey5, Key(262144 * 5 + SectorSize)),

    %% Exactly equal to the recall partition size.
    ?assertEqual(0, Partition(262144 * 5 + SectorSize)),
    %% One greater than the recall partition size.
    ?assertEqual(1, Partition(262144 * 5 + SectorSize + 1)),
    %% Greater than the entropy partition size. Chunks are mapped by the
    %% recall partition size, so this is still partition 1.
    ?assertEqual(1, Partition(262144 * 6 + SectorSize + 1)),
    %% A new partition uses a new entropy.
    EntropyKey6 = Key(262144 * 5 + 2 * SectorSize),
    ?assertNotEqual(EntropyKey6, EntropyKey5),
    %% The mapping repeats within every partition.
    ?assertEqual(EntropyKey6, Key(262144 * 5 + 3 * SectorSize)),

    %% The edges of the recall partition and the entropy partition.
    ?assertEqual(0, Partition(PartitionSize)),
    ?assertEqual(1, Partition(EntropyPartitionSize)),
    ?assertEqual(1, Partition(2 * PartitionSize)),
    ?assertEqual(2, Partition(PartitionSize + EntropyPartitionSize)),
    ?assertEqual(2, Partition(3 * PartitionSize)),
    ?assertEqual(3, Partition(2 * PartitionSize + EntropyPartitionSize)),
    ?assertEqual(10, Partition(11 * PartitionSize)),
    ?assertEqual(11, Partition(10 * PartitionSize + EntropyPartitionSize)),
    %% This sub-chunk offset isn't used in practice; it checks the bound.
    ?assertMatch({'EXIT', {{badmatch, false}, _}},
        catch arweave_lib_replica_2_9:get_entropy_index(0,
            32 * ?SUB_CHUNK_SIZE)).

%% @doc Each entropy partition range spans the offsets mapped to that
%% partition, when the partitions lie above the strict data split threshold.
entropy_partition_range_after_strict(_) ->
    mock_constants([{strict_data_split_threshold, 700_000}]),
    Partition = fun arweave_lib_replica_2_9:get_entropy_partition/1,
    Range = fun arweave_lib_replica_2_9:get_entropy_partition_range/1,
    ?assertEqual(0, Partition(0)),
    ?assertEqual(0, Partition(2272864)),
    ?assertEqual({0, 2272864}, Range(0)),
    ?assertEqual(1, Partition(2272865)),
    ?assertEqual(1, Partition(4370016)),
    ?assertEqual({2272865, 4370016}, Range(1)),
    ?assertEqual(2, Partition(4370017)),
    ?assertEqual(2, Partition(6205024)),
    ?assertEqual({4370017, 6205024}, Range(2)).

%% @doc Each entropy partition range spans the offsets mapped to that
%% partition, when the partitions lie below the strict data split threshold.
entropy_partition_range_before_strict(_) ->
    mock_constants([{strict_data_split_threshold, 5_000_000}]),
    Partition = fun arweave_lib_replica_2_9:get_entropy_partition/1,
    Range = fun arweave_lib_replica_2_9:get_entropy_partition_range/1,
    ?assertEqual(0, Partition(0)),
    ?assertEqual(0, Partition(2359295)),
    ?assertEqual({0, 2359295}, Range(0)),
    ?assertEqual(1, Partition(2359296)),
    ?assertEqual(1, Partition(4456447)),
    ?assertEqual({2359296, 4456447}, Range(1)),
    ?assertEqual(2, Partition(4456448)),
    ?assertEqual(2, Partition(6048576)),
    ?assertEqual({4456448, 6048576}, Range(2)).

%% @doc Walk through the chunks of a few partitions and check their slice
%% indices. All sub-chunks of a chunk share a slice index.
slice_index_walk(_) ->
    C = ?DATA_CHUNK_SIZE,
    mock_constants([
        {partition_size, 8 * C},
        {get_replica_2_9_entropy_sector_size, 786432},
        {get_replica_2_9_entropy_partition_size, 2359296},
        {get_sub_chunks_per_replica_2_9_entropy, 3},
        {strict_data_split_threshold, 3 * C}
    ]),
    %% Each entry is a slice index and the chunk end offsets that map to it.
    Walk = [
        %% Before the strict data split threshold. Partition and sector
        %% start.
        {0, [0]},
        {0, [1, C - 1, C]},
        {0, [C + 1, 2 * C - 1]},
        {0, [2 * C, 2 * C + 1, 3 * C - 1]},
        %% The end offset exactly at the threshold is mapped to the second
        %% bucket, so it is still in the first sector.
        {0, [3 * C]},
        %% After the threshold, end offsets are padded to a multiple of
        %% ?DATA_CHUNK_SIZE. Sector start.
        {1, [3 * C + 1, 4 * C - 1, 4 * C]},
        {1, [4 * C + 1, 5 * C - 1, 5 * C]},
        {1, [5 * C + 1, 6 * C - 1, 6 * C]},
        %% Sector start.
        {2, [6 * C + 1, 7 * C - 1, 7 * C]},
        {2, [7 * C + 1, 8 * C - 1, 8 * C]},
        %% Recall partition and sector start.
        {0, [8 * C + 1, 9 * C - 1, 9 * C]},
        {0, [9 * C + 1, 10 * C - 1, 10 * C]},
        {0, [10 * C + 1, 11 * C - 1, 11 * C]},
        %% Sector start.
        {1, [11 * C + 1, 12 * C - 1, 12 * C]},
        {1, [12 * C + 1, 13 * C - 1, 13 * C]},
        {1, [13 * C + 1, 14 * C - 1, 14 * C]},
        %% Sector start.
        {2, [14 * C + 1, 15 * C - 1, 15 * C]},
        {2, [15 * C + 1, 16 * C - 1, 16 * C]},
        %% Recall partition and sector start.
        {0, [16 * C + 1, 17 * C - 1, 17 * C]}
    ],
    [?assertEqual(Index, arweave_lib_replica_2_9:get_slice_index(Offset),
            {offset, Offset})
        || {Index, Offsets} <- Walk, Offset <- Offsets],
    PartitionSize = arweave_lib_constants:partition_size(),
    ?assertEqual(
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy() - 1,
        arweave_lib_replica_2_9:get_slice_index(PartitionSize)),
    ?assertEqual(0,
        arweave_lib_replica_2_9:get_slice_index(PartitionSize + 1)).

%% @doc Walk through every sub-chunk of the chunks in a few partitions and
%% check its entropy index. The sub-chunks of a chunk use consecutive indices.
entropy_index_walk(_) ->
    C = ?DATA_CHUNK_SIZE,
    mock_constants([
        {get_replica_2_9_entropy_sector_size, 786432},
        {get_replica_2_9_entropy_partition_size, 2359296},
        {get_sub_chunks_per_replica_2_9_entropy, 3}
    ]),
    %% Each entry is the entropy index of the first sub-chunk and the chunk
    %% end offsets it applies to. The sector size is 3 * C, so there are
    %% 3 * C / 8192 = 96 entropy indices, one per sub-chunk in a sector. The
    %% strict data split threshold is 3 * C; end offsets after it are padded
    %% to a multiple of ?DATA_CHUNK_SIZE.
    Walk = [
        %% Before the strict data split threshold. Partition and sector
        %% start.
        {0, [0]},
        {0, [1, C - 1, C]},
        {0, [C + 1, 2 * C - 1]},
        {32, [2 * C, 2 * C + 1, 3 * C - 1]},
        %% The strict data split threshold.
        {64, [3 * C]},
        %% After the threshold. Sector start.
        {0, [3 * C + 1, 4 * C - 1, 4 * C]},
        {32, [4 * C + 1, 5 * C - 1, 5 * C]},
        {64, [5 * C + 1, 6 * C - 1, 6 * C]},
        %% Sector start.
        {0, [6 * C + 1, 7 * C - 1, 7 * C]},
        {32, [7 * C + 1, 8 * C - 1, 8 * C]},
        %% Partition and sector start.
        {0, [8 * C + 1, 9 * C - 1, 9 * C]},
        {32, [9 * C + 1, 10 * C - 1, 10 * C]},
        {64, [10 * C + 1, 11 * C - 1, 11 * C]},
        %% Sector start.
        {0, [11 * C + 1, 12 * C - 1, 12 * C]},
        {32, [12 * C + 1, 13 * C - 1, 13 * C]},
        {64, [13 * C + 1, 14 * C - 1, 14 * C]},
        %% Sector start.
        {0, [14 * C + 1, 15 * C - 1, 15 * C]},
        {32, [15 * C + 1, 16 * C - 1, 16 * C]},
        %% Partition and sector start.
        {0, [16 * C + 1, 17 * C - 1, 17 * C]}
    ],
    [?assertEqual(First + SubChunk,
            arweave_lib_replica_2_9:get_entropy_index(Offset,
                SubChunk * ?SUB_CHUNK_SIZE + Shift),
            {Offset, SubChunk, Shift})
        || {First, Offsets} <- Walk, Offset <- Offsets,
            SubChunk <- lists:seq(0, ?SUB_CHUNK_COUNT - 1),
            Shift <- [0, 1, ?SUB_CHUNK_SIZE - 1]].

%%====================================================================
%% Helpers
%%====================================================================

mock_constants(Values) ->
    lists:foreach(
        fun({Name, Value}) ->
            meck:expect(arweave_lib_constants, Name, fun() -> Value end)
        end,
        Values
    ).
