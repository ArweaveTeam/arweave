-module(arweave_lib_constants_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_lib/include/arweave_lib_constants.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        build_constants,
        replica_geometry,
        shared_geometry_overrides,
        padding_boundaries,
        chunk_bucket,
        chunk_byte_from_bucket_end,
        packing_geometry,
        fork_heights
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_lib),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(Name, Config)
        when Name =:= chunk_bucket; Name =:= chunk_byte_from_bucket_end ->
    meck:new(arweave_lib_constants, [passthrough]),
    meck:expect(arweave_lib_constants, strict_data_split_threshold,
        fun() -> 700_000 end),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(Name, _)
        when Name =:= chunk_bucket; Name =:= chunk_byte_from_bucket_end ->
    meck:unload(arweave_lib_constants);
end_per_testcase(_, _) ->
    arweave_lib_constants:internal_reset_partition_size_override(),
    arweave_lib_constants:internal_reset_replica_2_9_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Public constant accessors match the values selected by the build
%% profile.
build_constants(_) ->
    ?assertEqual(?PARTITION_SIZE, arweave_lib_constants:partition_size()),
    ?assertEqual(
        ?STRICT_DATA_SPLIT_THRESHOLD,
        arweave_lib_constants:strict_data_split_threshold()
    ),
    ?assertEqual(
        ?MERKLE_REBASE_SUPPORT_THRESHOLD,
        arweave_lib_constants:get_merkle_rebase_support_threshold()
    ),
    ?assertEqual(
        ?STORE_BLOCKS_BEHIND_CURRENT,
        arweave_lib_constants:get_consensus_window_size()
    ),
    ?assertEqual(
        ?STORE_BLOCKS_BEHIND_CURRENT,
        arweave_lib_constants:get_max_tx_anchor_depth()
    ),
    ?assertEqual(
        2 * ?JOIN_CLOCK_TOLERANCE + ?CLOCK_DRIFT_MAX,
        arweave_lib_constants:get_max_timestamp_deviation()
    ).

%% @doc Replica geometry derives consistently from entropy size, count and
%% sub-chunk size.
replica_geometry(_) ->
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE,
        arweave_lib_constants:replica_2_9_entropy_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT,
        arweave_lib_constants:replica_2_9_entropy_count()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT * ?SUB_CHUNK_SIZE,
        arweave_lib_constants:get_replica_2_9_entropy_sector_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT * ?REPLICA_2_9_ENTROPY_SIZE,
        arweave_lib_constants:get_replica_2_9_entropy_partition_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE div ?SUB_CHUNK_SIZE,
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE * ?SUB_CHUNK_COUNT,
        arweave_lib_constants:get_replica_2_9_footprint_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT div ?SUB_CHUNK_COUNT,
        arweave_lib_constants:get_replica_2_9_footprints_per_partition()
    ).

%% @doc Geometry overrides affect derived constants and can be reset
%% independently.
shared_geometry_overrides(_) ->
    arweave_lib_constants:internal_override_partition_size(
        ?MAINNET_PARTITION_SIZE
    ),
    arweave_lib_constants:internal_override_replica_2_9_entropy_size(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE
    ),
    arweave_lib_constants:internal_override_replica_2_9_entropy_count(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT
    ),
    ?assertEqual(
        ?MAINNET_PARTITION_SIZE,
        arweave_lib_constants:partition_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE * ?SUB_CHUNK_COUNT,
        arweave_lib_constants:get_replica_2_9_footprint_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT * ?SUB_CHUNK_SIZE,
        arweave_lib_constants:get_replica_2_9_entropy_sector_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT * ?MAINNET_REPLICA_2_9_ENTROPY_SIZE,
        arweave_lib_constants:get_replica_2_9_entropy_partition_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE div ?SUB_CHUNK_SIZE,
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT div ?SUB_CHUNK_COUNT,
        arweave_lib_constants:get_replica_2_9_footprints_per_partition()
    ),
    %% Resetting partition overrides must not reset entropy overrides.
    arweave_lib_constants:internal_reset_partition_size_override(),
    ?assertEqual(?PARTITION_SIZE, arweave_lib_constants:partition_size()),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE,
        arweave_lib_constants:replica_2_9_entropy_size()
    ),
    arweave_lib_constants:internal_override_partition_size(
        ?MAINNET_PARTITION_SIZE
    ),
    arweave_lib_constants:internal_reset_replica_2_9_overrides(),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE,
        arweave_lib_constants:replica_2_9_entropy_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT,
        arweave_lib_constants:replica_2_9_entropy_count()
    ),
    ?assertEqual(
        ?MAINNET_PARTITION_SIZE,
        arweave_lib_constants:partition_size()
    ).

%% @doc Chunk padding rounds relative to the split threshold without moving
%% aligned offsets.
padding_boundaries(_) ->
    Threshold = arweave_lib_constants:strict_data_split_threshold(),
    ChunkSize = ?DATA_CHUNK_SIZE,
    ?assertEqual(0, arweave_lib_constants:get_chunk_padded_offset(0)),
    ?assertEqual(
        Threshold,
        arweave_lib_constants:get_chunk_padded_offset(Threshold)
    ),
    ?assertEqual(
        Threshold + ChunkSize,
        arweave_lib_constants:get_chunk_padded_offset(Threshold + 1)
    ),
    ?assertEqual(
        Threshold + ChunkSize,
        arweave_lib_constants:get_chunk_padded_offset(Threshold + ChunkSize)
    ),
    ?assertEqual(
        Threshold + 2 * ChunkSize,
        arweave_lib_constants:get_chunk_padded_offset(Threshold + ChunkSize + 1)
    ),
    %% An unaligned threshold distinguishes relative from absolute padding.
    Offset = ChunkSize + 1,
    ?assertEqual(
        Offset,
        arweave_lib_constants:get_padded_offset(Offset, 1)
    ),
    ?assertEqual(
        2 * ChunkSize + 1,
        arweave_lib_constants:get_padded_offset(Offset + 1, 1)
    ).

%% @doc Recall ranges, sub-chunks and nonce bounds match each packing
%% difficulty.
packing_geometry(_) ->
    ?assertEqual(
        ?LEGACY_RECALL_RANGE_SIZE,
        arweave_lib_constants:get_recall_range_size(0)
    ),
    ?assertEqual(?DATA_CHUNK_SIZE, arweave_lib_constants:get_sub_chunk_size(0)),
    ?assertEqual(1, arweave_lib_constants:get_nonces_per_chunk(0)),
    ?assertEqual(
        max(1, ?LEGACY_RECALL_RANGE_SIZE div ?DATA_CHUNK_SIZE),
        arweave_lib_constants:get_nonces_per_recall_range(0)
    ),
    ?assertEqual(
        max(0, ?LEGACY_RECALL_RANGE_SIZE div ?DATA_CHUNK_SIZE - 1),
        arweave_lib_constants:get_max_nonce(0)
    ),
    lists:foreach(
        fun(Difficulty) ->
            RangeSize = ?RECALL_RANGE_SIZE div Difficulty,
            ?assertEqual(
                RangeSize,
                arweave_lib_constants:get_recall_range_size(Difficulty)
            ),
            ?assertEqual(
                ?SUB_CHUNK_SIZE,
                arweave_lib_constants:get_sub_chunk_size(Difficulty)
            ),
            ?assertEqual(
                ?SUB_CHUNK_COUNT,
                arweave_lib_constants:get_nonces_per_chunk(Difficulty)
            ),
            ?assertEqual(
                max(1, RangeSize div ?SUB_CHUNK_SIZE),
                arweave_lib_constants:get_nonces_per_recall_range(Difficulty)
            ),
            ?assertEqual(
                max(
                    ?SUB_CHUNK_COUNT - 1,
                    RangeSize div ?SUB_CHUNK_SIZE - 1
                ),
                arweave_lib_constants:get_max_nonce(Difficulty)
            )
        end,
        [1, ?REPLICA_2_9_PACKING_DIFFICULTY, ?SUB_CHUNK_COUNT]
    ).

%% @doc The regular test profile exposes every protocol fork as active at
%% genesis.
fork_heights(_) ->
    %% The regular test profile activates all forks at genesis.
    Heights = [
        arweave_lib_constants:height_1_6(),
        arweave_lib_constants:height_1_7(),
        arweave_lib_constants:height_1_8(),
        arweave_lib_constants:height_1_9(),
        arweave_lib_constants:height_2_0(),
        arweave_lib_constants:height_2_2(),
        arweave_lib_constants:height_2_3(),
        arweave_lib_constants:height_2_4(),
        arweave_lib_constants:height_2_5(),
        arweave_lib_constants:height_2_6(),
        arweave_lib_constants:height_2_6_8(),
        arweave_lib_constants:height_2_7(),
        arweave_lib_constants:height_2_7_1(),
        arweave_lib_constants:height_2_7_2(),
        arweave_lib_constants:height_2_8(),
        arweave_lib_constants:height_2_9(),
        arweave_lib_constants:height_2_9_6()
    ],
    ?assertEqual(lists:duplicate(length(Heights), 0), Heights).

%% @doc Chunk bucket bounds pad only offsets above the strict data split
%% threshold.
chunk_bucket(_Config) ->
    case arweave_lib_constants:strict_data_split_threshold() of
        700_000 ->
            ok;
        _ ->
            throw(unexpected_strict_data_split_threshold)
    end,

    %% get_chunk_bucket_end pads the provided offset
    %% get_chunk_bucket_start does not pad the provided offset

    %% At and before the STRICT_DATA_SPLIT_THRESHOLD, offsets are not padded.
    ?assertEqual(262144, arweave_lib_constants:get_chunk_bucket_end(0)),
    ?assertEqual(0, arweave_lib_constants:get_chunk_bucket_start(0)),

    ?assertEqual(262144, arweave_lib_constants:get_chunk_bucket_end(1)),
    ?assertEqual(0, arweave_lib_constants:get_chunk_bucket_start(1)),

    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_end(?DATA_CHUNK_SIZE - 1)
    ),
    ?assertEqual(
        0,
        arweave_lib_constants:get_chunk_bucket_start(
            ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_end(?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        0,
        arweave_lib_constants:get_chunk_bucket_start(?DATA_CHUNK_SIZE)
    ),

    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_end(?DATA_CHUNK_SIZE + 1)
    ),
    ?assertEqual(
        0,
        arweave_lib_constants:get_chunk_bucket_start(
            ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_end(2 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_start(
            2 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_end(
            2 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_start(
            2 * ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_end(
            arweave_lib_constants:strict_data_split_threshold() - 1
        )
    ),
    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_start(
            arweave_lib_constants:strict_data_split_threshold() - 1
        )
    ),

    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_end(
            arweave_lib_constants:strict_data_split_threshold()
        )
    ),
    ?assertEqual(
        262144,
        arweave_lib_constants:get_chunk_bucket_start(
            arweave_lib_constants:strict_data_split_threshold()
        )
    ),

    %% After the STRICT_DATA_SPLIT_THRESHOLD, offsets are padded.
    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_end(
            arweave_lib_constants:strict_data_split_threshold() + 1
        )
    ),
    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_start(
            arweave_lib_constants:strict_data_split_threshold() + 1
        )
    ),

    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_end(
            3 * ?DATA_CHUNK_SIZE - 1
        )
    ),
    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_start(
            3 * ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_end(3 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_start(
            3 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_end(
            3 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        524288,
        arweave_lib_constants:get_chunk_bucket_start(
            3 * ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        1048576,
        arweave_lib_constants:get_chunk_bucket_end(
            4 * ?DATA_CHUNK_SIZE - 1
        )
    ),
    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_start(
            4 * ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        1048576,
        arweave_lib_constants:get_chunk_bucket_end(4 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_start(
            4 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        1048576,
        arweave_lib_constants:get_chunk_bucket_end(
            4 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        786432,
        arweave_lib_constants:get_chunk_bucket_start(
            4 * ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        1310720,
        arweave_lib_constants:get_chunk_bucket_end(
            5 * ?DATA_CHUNK_SIZE - 1
        )
    ),
    ?assertEqual(
        1048576,
        arweave_lib_constants:get_chunk_bucket_start(
            5 * ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        1310720,
        arweave_lib_constants:get_chunk_bucket_end(5 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        1048576,
        arweave_lib_constants:get_chunk_bucket_start(
            5 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        1310720,
        arweave_lib_constants:get_chunk_bucket_end(
            5 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        1048576,
        arweave_lib_constants:get_chunk_bucket_start(
            5 * ?DATA_CHUNK_SIZE + 1
        )
    ).

%% @doc The byte taken from a bucket end falls inside that bucket's chunk.
chunk_byte_from_bucket_end(_Config) ->
    ?assertEqual(
        262143,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(262144)
    ),
    ?assertEqual(
        524287,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(524288)
    ),
    ?assertEqual(
        700000,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(786432)
    ),
    ?assertEqual(
        962144,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(1048576)
    ),
    ?assertEqual(
        1224288,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(1310720)
    ),
    ?assertEqual(
        1486432,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(1572864)
    ),
    ?assertEqual(
        1748576,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(1835008)
    ),
    ?assertEqual(
        2010720,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(2097152)
    ),
    ?assertEqual(
        2272864,
        arweave_lib_constants:get_chunk_byte_from_bucket_end(2359296)
    ).
