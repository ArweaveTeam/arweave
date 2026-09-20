-module(arweave_constants_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_constants/include/arweave_constants.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        standalone_library,
        build_constants,
        replica_geometry,
        shared_geometry_overrides,
        padding_boundaries,
        packing_geometry,
        fork_heights
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_constants),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

end_per_testcase(_, _) ->
    arweave_constants:internal_reset_partition_size_override(),
    arweave_constants:internal_reset_replica_2_9_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Protocol constants load without host services or registered processes.
standalone_library(_) ->
    ?assertEqual(
        {ok, [kernel, stdlib]},
        application:get_key(arweave_constants, applications)
    ),
    ?assertEqual(undefined, whereis(ar_sup)),
    ?assertEqual(
        {ok, []},
        application:get_key(arweave_constants, registered)
    ).

%% @doc Public constant accessors match the values selected by the build
%% profile.
build_constants(_) ->
    ?assertEqual(?PARTITION_SIZE, arweave_constants:partition_size()),
    ?assertEqual(
        ?STRICT_DATA_SPLIT_THRESHOLD,
        arweave_constants:strict_data_split_threshold()
    ),
    ?assertEqual(
        ?MERKLE_REBASE_SUPPORT_THRESHOLD,
        arweave_constants:get_merkle_rebase_support_threshold()
    ),
    ?assertEqual(
        ?STORE_BLOCKS_BEHIND_CURRENT,
        arweave_constants:get_consensus_window_size()
    ),
    ?assertEqual(
        ?STORE_BLOCKS_BEHIND_CURRENT,
        arweave_constants:get_max_tx_anchor_depth()
    ),
    ?assertEqual(
        2 * ?JOIN_CLOCK_TOLERANCE + ?CLOCK_DRIFT_MAX,
        arweave_constants:get_max_timestamp_deviation()
    ).

%% @doc Replica geometry derives consistently from entropy size, count and
%% sub-chunk size.
replica_geometry(_) ->
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE,
        arweave_constants:replica_2_9_entropy_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT,
        arweave_constants:replica_2_9_entropy_count()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT * ?SUB_CHUNK_SIZE,
        arweave_constants:get_replica_2_9_entropy_sector_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT * ?REPLICA_2_9_ENTROPY_SIZE,
        arweave_constants:get_replica_2_9_entropy_partition_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE div ?SUB_CHUNK_SIZE,
        arweave_constants:get_sub_chunks_per_replica_2_9_entropy()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE * ?SUB_CHUNK_COUNT,
        arweave_constants:get_replica_2_9_footprint_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT div ?SUB_CHUNK_COUNT,
        arweave_constants:get_replica_2_9_footprints_per_partition()
    ).

%% @doc Geometry overrides affect derived constants and can be reset
%% independently.
shared_geometry_overrides(_) ->
    arweave_constants:internal_override_partition_size(?MAINNET_PARTITION_SIZE),
    arweave_constants:internal_override_replica_2_9_entropy_size(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE
    ),
    arweave_constants:internal_override_replica_2_9_entropy_count(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT
    ),
    ?assertEqual(
        ?MAINNET_PARTITION_SIZE,
        arweave_constants:partition_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE * ?SUB_CHUNK_COUNT,
        arweave_constants:get_replica_2_9_footprint_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT * ?SUB_CHUNK_SIZE,
        arweave_constants:get_replica_2_9_entropy_sector_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT * ?MAINNET_REPLICA_2_9_ENTROPY_SIZE,
        arweave_constants:get_replica_2_9_entropy_partition_size()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE div ?SUB_CHUNK_SIZE,
        arweave_constants:get_sub_chunks_per_replica_2_9_entropy()
    ),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_COUNT div ?SUB_CHUNK_COUNT,
        arweave_constants:get_replica_2_9_footprints_per_partition()
    ),
    %% Resetting partition overrides must not reset entropy overrides.
    arweave_constants:internal_reset_partition_size_override(),
    ?assertEqual(?PARTITION_SIZE, arweave_constants:partition_size()),
    ?assertEqual(
        ?MAINNET_REPLICA_2_9_ENTROPY_SIZE,
        arweave_constants:replica_2_9_entropy_size()
    ),
    arweave_constants:internal_override_partition_size(?MAINNET_PARTITION_SIZE),
    arweave_constants:internal_reset_replica_2_9_overrides(),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_SIZE,
        arweave_constants:replica_2_9_entropy_size()
    ),
    ?assertEqual(
        ?REPLICA_2_9_ENTROPY_COUNT,
        arweave_constants:replica_2_9_entropy_count()
    ),
    ?assertEqual(
        ?MAINNET_PARTITION_SIZE,
        arweave_constants:partition_size()
    ).

%% @doc Chunk padding rounds relative to the split threshold without moving
%% aligned offsets.
padding_boundaries(_) ->
    Threshold = arweave_constants:strict_data_split_threshold(),
    ChunkSize = ?DATA_CHUNK_SIZE,
    ?assertEqual(0, arweave_constants:get_chunk_padded_offset(0)),
    ?assertEqual(
        Threshold,
        arweave_constants:get_chunk_padded_offset(Threshold)
    ),
    ?assertEqual(
        Threshold + ChunkSize,
        arweave_constants:get_chunk_padded_offset(Threshold + 1)
    ),
    ?assertEqual(
        Threshold + ChunkSize,
        arweave_constants:get_chunk_padded_offset(Threshold + ChunkSize)
    ),
    ?assertEqual(
        Threshold + 2 * ChunkSize,
        arweave_constants:get_chunk_padded_offset(Threshold + ChunkSize + 1)
    ),
    %% An unaligned threshold distinguishes relative from absolute padding.
    Offset = ChunkSize + 1,
    ?assertEqual(
        Offset,
        arweave_constants:get_padded_offset(Offset, 1)
    ),
    ?assertEqual(
        2 * ChunkSize + 1,
        arweave_constants:get_padded_offset(Offset + 1, 1)
    ).

%% @doc Recall ranges, sub-chunks and nonce bounds match each packing
%% difficulty.
packing_geometry(_) ->
    ?assertEqual(
        ?LEGACY_RECALL_RANGE_SIZE,
        arweave_constants:get_recall_range_size(0)
    ),
    ?assertEqual(?DATA_CHUNK_SIZE, arweave_constants:get_sub_chunk_size(0)),
    ?assertEqual(1, arweave_constants:get_nonces_per_chunk(0)),
    ?assertEqual(
        max(1, ?LEGACY_RECALL_RANGE_SIZE div ?DATA_CHUNK_SIZE),
        arweave_constants:get_nonces_per_recall_range(0)
    ),
    ?assertEqual(
        max(0, ?LEGACY_RECALL_RANGE_SIZE div ?DATA_CHUNK_SIZE - 1),
        arweave_constants:get_max_nonce(0)
    ),
    lists:foreach(
        fun(Difficulty) ->
            RangeSize = ?RECALL_RANGE_SIZE div Difficulty,
            ?assertEqual(
                RangeSize,
                arweave_constants:get_recall_range_size(Difficulty)
            ),
            ?assertEqual(
                ?SUB_CHUNK_SIZE,
                arweave_constants:get_sub_chunk_size(Difficulty)
            ),
            ?assertEqual(
                ?SUB_CHUNK_COUNT,
                arweave_constants:get_nonces_per_chunk(Difficulty)
            ),
            ?assertEqual(
                max(1, RangeSize div ?SUB_CHUNK_SIZE),
                arweave_constants:get_nonces_per_recall_range(Difficulty)
            ),
            ?assertEqual(
                max(
                    ?SUB_CHUNK_COUNT - 1,
                    RangeSize div ?SUB_CHUNK_SIZE - 1
                ),
                arweave_constants:get_max_nonce(Difficulty)
            )
        end,
        [1, ?REPLICA_2_9_PACKING_DIFFICULTY, ?SUB_CHUNK_COUNT]
    ).

%% @doc The regular test profile exposes every protocol fork as active at
%% genesis.
fork_heights(_) ->
    %% The regular test profile activates all forks at genesis.
    Heights = [
        arweave_constants:height_1_6(),
        arweave_constants:height_1_7(),
        arweave_constants:height_1_8(),
        arweave_constants:height_1_9(),
        arweave_constants:height_2_0(),
        arweave_constants:height_2_2(),
        arweave_constants:height_2_3(),
        arweave_constants:height_2_4(),
        arweave_constants:height_2_5(),
        arweave_constants:height_2_6(),
        arweave_constants:height_2_6_8(),
        arweave_constants:height_2_7(),
        arweave_constants:height_2_7_1(),
        arweave_constants:height_2_7_2(),
        arweave_constants:height_2_8(),
        arweave_constants:height_2_9(),
        arweave_constants:height_2_9_6()
    ],
    ?assertEqual(lists:duplicate(length(Heights), 0), Heights).
