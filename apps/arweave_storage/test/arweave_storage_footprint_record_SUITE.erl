-module(arweave_storage_footprint_record_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        get_offset,
        get_padded_offset_from_footprint_offset,
        get_offset_footprint_intervals_to_byte_intervals_reversal,
        get_unsynced_intervals,
        get_intervals,
        get_offset_get_padded_offset_from_footprint_offset_reversal,
        footprint_intervals_to_byte_intervals,
        bounded_footprint_intervals_to_byte_intervals,
        byte_intervals_to_footprint_intervals,
        footprint_geometry,
        next_sector_start_clamps_to_partition,
        initialize_records_synced_chunks,
        initialize_skips_entropy_tail_offsets,
        initialize_mid_partition_range
    ].

init_per_suite(Config) -> arweave_storage_ct_util:init_suite(Config).

end_per_suite(Config) -> arweave_storage_ct_util:end_suite(Config).

init_per_testcase(Name, Config) ->
    Config2 = arweave_storage_ct_util:init_case(Name, Config),
    case
        lists:member(Name, [
            get_unsynced_intervals,
            get_intervals,
            initialize_records_synced_chunks,
            initialize_skips_entropy_tail_offsets,
            initialize_mid_partition_range
        ])
    of
        true ->
            {ok, _} = arweave_storage:activate();
        false ->
            ok
    end,
    Config2.

end_per_testcase(Name, Config) ->
    arweave_storage_ct_util:stop_record(test_unsynced_store),
    arweave_storage_ct_util:stop_record(test_intervals_store),
    arweave_storage_ct_util:stop_record(test_init_store),
    arweave_storage_ct_util:end_case(Name, Config).

%%====================================================================
%% Test cases
%%====================================================================

get_offset(_Config) ->
    %% The first chunk of the first footprint.
    ?assertEqual(
        1, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE)
    ),
    %% The first chunk of the second footprint.
    ?assertEqual(
        5, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 2)
    ),
    %% The second chunk of the first footprint.
    ?assertEqual(
        2, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 3)
    ),
    %% The second chunk of the second footprint.
    ?assertEqual(
        6, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 4)
    ),
    %% The third chunk of the first footprint.
    ?assertEqual(
        3, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 5)
    ),
    %% The third chunk of the second footprint.
    ?assertEqual(
        7, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 6)
    ),
    %% The fourth chunk of the first footprint.
    ?assertEqual(
        4, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 7)
    ),
    %% The fourth chunk of the second footprint.
    ?assertEqual(
        8, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 8)
    ),
    %% The first chunk of the first footprint of the second partition.
    ?assertEqual(
        9, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 9)
    ),
    %% The first chunk of the second footprint of the second partition.
    ?assertEqual(
        13, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 10)
    ),
    %% The second chunk of the first footprint of the second partition.
    ?assertEqual(
        10, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 11)
    ),
    %% The second chunk of the second footprint of the second partition.
    ?assertEqual(
        14, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 12)
    ),
    %% The third chunk of the first footprint of the second partition.
    ?assertEqual(
        11, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 13)
    ),
    %% The third chunk of the second footprint of the second partition.
    ?assertEqual(
        15, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 14)
    ),
    %% The fourth chunk of the first footprint of the second partition.
    ?assertEqual(
        12, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 15)
    ),
    %% The fourth chunk of the second footprint of the second partition.
    ?assertEqual(
        16, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 16)
    ),
    %% The first chunk of the first footprint of the third partition.
    ?assertEqual(
        17, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 17)
    ),
    %% The first chunk of the second footprint of the third partition.
    ?assertEqual(
        21, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 18)
    ),
    %% The second chunk of the first footprint of the third partition.
    ?assertEqual(
        18, arweave_storage_footprint_record:get_offset(?DATA_CHUNK_SIZE * 19)
    ).

get_padded_offset_from_footprint_offset(_Config) ->
    ?assertEqual(
        262144,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            1
        )
    ),
    ?assertEqual(
        786432,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            2
        )
    ),
    ?assertEqual(
        1310720,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            3
        )
    ),
    ?assertEqual(
        1835008,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            4
        )
    ),
    ?assertEqual(
        524288,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            5
        )
    ),
    ?assertEqual(
        1048576,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            6
        )
    ),
    ?assertEqual(
        1572864,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            7
        )
    ),
    ?assertEqual(
        2097152,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            8
        )
    ),
    ?assertEqual(
        2359296,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            9
        )
    ),
    ?assertEqual(
        2883584,
        arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
            10
        )
    ),

    79280870522880 = arweave_constants:get_chunk_padded_offset(79280870522880),
    ?assertEqual(
        317123481, arweave_storage:get_footprint_offset(79280870522880)
    ),
    ?assertEqual(
        79280870522880,
        arweave_storage:get_padded_offset_from_footprint_offset(317123481)
    ).

get_offset_footprint_intervals_to_byte_intervals_reversal(_Config) ->
    Offsets = [
        ?DATA_CHUNK_SIZE,
        ?DATA_CHUNK_SIZE * 2,
        ?DATA_CHUNK_SIZE * 3,
        ?DATA_CHUNK_SIZE * 4,
        ?DATA_CHUNK_SIZE * 8,
        ?DATA_CHUNK_SIZE * 9
    ],
    lists:foreach(
        fun(ByteOffset) ->
            FootprintOffset = arweave_storage_footprint_record:get_offset(
                ByteOffset
            ),
            Interval = ar_intervals:from_list([
                {FootprintOffset, FootprintOffset - 1}
            ]),
            ?assertEqual(
                [{ByteOffset, ByteOffset - ?DATA_CHUNK_SIZE}],
                ar_intervals:to_list(
                    arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
                        Interval
                    )
                )
            )
        end,
        Offsets
    ).

get_unsynced_intervals(_Config) ->
    TestStoreID = test_unsynced_store,
    arweave_storage_ct_util:with_mocks(
        [
            {arweave_storage_module, [
                {get_by_id, fun
                    (test_unsynced_store) -> test_unsynced_store;
                    (StoreID) -> meck:passthrough([StoreID])
                end}
            ]}
        ],
        fun() ->
            {ok, _PID} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(TestStoreID), TestStoreID
            ),
            Partition = 0,
            Footprint = 0,

            %% Get unsynced intervals before adding any data.
            UnsyncedBefore = arweave_storage_footprint_record:get_unsynced_intervals(
                Partition, Footprint, TestStoreID
            ),
            UnsyncedBeforeList = ar_intervals:to_list(UnsyncedBefore),
            ?assertEqual([{4, 0}], UnsyncedBeforeList),

            %% Add some data to the footprint.
            %% This should map to partition 0, footprint 0.
            PaddedOffset = ?DATA_CHUNK_SIZE,
            Packing = unpacked,
            ok = arweave_storage_footprint_record:add(
                PaddedOffset, Packing, TestStoreID
            ),

            UnsyncedAfter = arweave_storage_footprint_record:get_unsynced_intervals(
                Partition, Footprint, TestStoreID
            ),
            UnsyncedAfterList = ar_intervals:to_list(UnsyncedAfter),
            ?assertEqual([{4, 1}], UnsyncedAfterList)
        end
    ).

get_intervals(_Config) ->
    TestStoreID = test_intervals_store,
    arweave_storage_ct_util:with_mocks(
        [
            {arweave_storage_module, [
                {get_by_id, fun
                    (test_intervals_store) -> test_intervals_store;
                    (StoreID) -> meck:passthrough([StoreID])
                end}
            ]}
        ],
        fun() ->
            {ok, _PID} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(TestStoreID), TestStoreID
            ),
            Packing = unpacked,
            arweave_storage:add_sync_record(
                32,
                0,
                Packing,
                {ar_data_sync, footprint},
                TestStoreID
            ),

            Partition = 0,
            Footprint = 0,
            SyncedIntervals = arweave_storage_footprint_record:get_intervals(
                Partition, Footprint, TestStoreID
            ),
            SyncedIntervalsList = ar_intervals:to_list(SyncedIntervals),
            ?assertEqual([{4, 0}], SyncedIntervalsList),
            Partition2 = 0,
            Footprint2 = 1,
            SyncedIntervals2 = arweave_storage_footprint_record:get_intervals(
                Partition2, Footprint2, TestStoreID
            ),
            SyncedIntervalsList2 = ar_intervals:to_list(SyncedIntervals2),
            ?assertEqual([{8, 4}], SyncedIntervalsList2),
            Partition3 = 0,
            Footprint3 = 2,
            SyncedIntervals3 = arweave_storage_footprint_record:get_intervals(
                Partition3, Footprint3, TestStoreID
            ),
            SyncedIntervalsList3 = ar_intervals:to_list(SyncedIntervals3),
            ?assertEqual([], SyncedIntervalsList3),
            Partition4 = 1,
            Footprint4 = 0,
            SyncedIntervals4 = arweave_storage_footprint_record:get_intervals(
                Partition4, Footprint4, TestStoreID
            ),
            SyncedIntervalsList4 = ar_intervals:to_list(SyncedIntervals4),
            ?assertEqual([{12, 8}], SyncedIntervalsList4),
            Partition5 = 1,
            Footprint5 = 1,
            SyncedIntervals5 = arweave_storage_footprint_record:get_intervals(
                Partition5, Footprint5, TestStoreID
            ),
            SyncedIntervalsList5 = ar_intervals:to_list(SyncedIntervals5),
            ?assertEqual([{16, 12}], SyncedIntervalsList5),
            Partition6 = 1,
            Footprint6 = 2,
            SyncedIntervals6 = arweave_storage_footprint_record:get_intervals(
                Partition6, Footprint6, TestStoreID
            ),
            SyncedIntervalsList6 = ar_intervals:to_list(SyncedIntervals6),
            ?assertEqual([], SyncedIntervalsList6),
            Partition7 = 2,
            Footprint7 = 0,
            SyncedIntervals7 = arweave_storage_footprint_record:get_intervals(
                Partition7, Footprint7, TestStoreID
            ),
            SyncedIntervalsList7 = ar_intervals:to_list(SyncedIntervals7),
            ?assertEqual([{20, 16}], SyncedIntervalsList7)
        end
    ).

get_offset_get_padded_offset_from_footprint_offset_reversal(_Config) ->
    Offsets = [
        ?DATA_CHUNK_SIZE,
        ?DATA_CHUNK_SIZE * 2,
        ?DATA_CHUNK_SIZE * 3,
        ?PARTITION_SIZE,
        ?DATA_CHUNK_SIZE * 8,
        ?DATA_CHUNK_SIZE * 9,
        ?PARTITION_SIZE * 2,
        ?PARTITION_SIZE * 3,
        ?PARTITION_SIZE * 4,
        ?PARTITION_SIZE * 5,
        ?PARTITION_SIZE * 6,
        ?PARTITION_SIZE * 7,
        ?PARTITION_SIZE * 8,
        ?PARTITION_SIZE * 9,
        ?PARTITION_SIZE * 10,
        ?PARTITION_SIZE * 11,
        ?PARTITION_SIZE * 12,
        ?PARTITION_SIZE * 13,
        ?PARTITION_SIZE * 6249,
        ?PARTITION_SIZE * 6250,
        ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE,
        ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE,
        ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 2,
        ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE * 2,
        ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 8,
        ?PARTITION_SIZE * 6250 + ?DATA_CHUNK_SIZE * 8,
        ?PARTITION_SIZE * 6249 + ?DATA_CHUNK_SIZE * 9
    ],
    lists:foreach(
        fun(Offset) ->
            ?assertEqual(
                arweave_constants:get_chunk_padded_offset(Offset),
                arweave_storage_footprint_record:get_padded_offset_from_footprint_offset(
                    arweave_storage_footprint_record:get_offset(Offset)
                )
            )
        end,
        Offsets
    ),
    ok.

footprint_intervals_to_byte_intervals(_Config) ->
    TestCases =
        [
            {[], [], "Empty"},
            {[{1, 0}], [{?DATA_CHUNK_SIZE, 0}], "One chunk"},
            {
                [{2, 0}],
                [
                    {?DATA_CHUNK_SIZE, 0},
                    {?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 2}
                ],
                "Two chunks"
            },
            {
                [{4, 0}],
                [
                    {?DATA_CHUNK_SIZE, 0},
                    {?DATA_CHUNK_SIZE * 3, ?DATA_CHUNK_SIZE * 2},
                    {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4},
                    {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}
                ],
                "Full footprint"
            },
            {
                [{5, 0}],
                [
                    {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4},
                    {?DATA_CHUNK_SIZE * 3, 0},
                    {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6}
                ],
                "Footprint wraparound"
            },
            {
                [{6, 3}],
                [
                    {?DATA_CHUNK_SIZE * 7, ?DATA_CHUNK_SIZE * 6},
                    {?DATA_CHUNK_SIZE * 2, ?DATA_CHUNK_SIZE * 1},
                    {?DATA_CHUNK_SIZE * 4, ?DATA_CHUNK_SIZE * 3}
                ],
                "Bits of two footprints"
            },
            {
                [{1, 0}, {3, 2}],
                [
                    {?DATA_CHUNK_SIZE, 0},
                    {?DATA_CHUNK_SIZE * 5, ?DATA_CHUNK_SIZE * 4}
                ],
                "Two chunks with a hole"
            },
            {
                [{8, 0}],
                [
                    {?DATA_CHUNK_SIZE * 8, 0}
                ],
                "Completely covered partition"
            },
            {
                [{9, 0}],
                [
                    {?DATA_CHUNK_SIZE * 9, 0}
                ],
                "Completely covered partition plus one chunk"
            },
            {
                [{9, 0}, {13, 12}],
                [
                    {?DATA_CHUNK_SIZE * 10, 0}
                ],
                "Completely covered partition plus two chunks"
            },
            {
                [{9, 0}, {13, 12}, {15, 14}],
                [
                    {?DATA_CHUNK_SIZE * 10, 0},
                    {?DATA_CHUNK_SIZE * 14, ?DATA_CHUNK_SIZE * 13}
                ],
                "Completely covered partition plus three chunks"
            }
        ],
    lists:foreach(
        fun({Input, Expected, Title}) ->
            ?assertEqual(
                ar_intervals:from_list(Expected),
                arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
                    ar_intervals:from_list(Input)
                ),
                Title
            )
        end,
        TestCases
    ).

%% @doc Projecting byte intervals onto a footprint selects the footprint's
%% chunks they overlap, the inverse of footprint_intervals_to_byte_intervals/1.
byte_intervals_to_footprint_intervals(_Config) ->
    C = ?DATA_CHUNK_SIZE,
    ToBytes = fun(FootprintIntervals) ->
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list(FootprintIntervals)
        )
    end,
    ToFootprint = fun(ByteIntervals, Partition, Footprint) ->
        arweave_storage_footprint_record:byte_intervals_to_footprint_intervals(
            ar_intervals:from_list(ByteIntervals), Partition, Footprint
        )
    end,
    %% Footprint 0 holds the chunks ending at 1, 3, 5 and 7 chunk sizes,
    %% footprint 1 those ending at 2, 4, 6 and 8, and partition 1 repeats the
    %% pattern from 9 on.
    RoundTrips = [
        {0, 0, [{1, 0}], [{C, 0}], "First chunk of footprint 0"},
        {0, 1, [{8, 4}],
            [{2 * C, C}, {4 * C, 3 * C}, {6 * C, 5 * C}, {8 * C, 7 * C}],
            "All of footprint 1"},
        {0, 1, [{6, 5}, {8, 7}], [{4 * C, 3 * C}, {8 * C, 7 * C}],
            "Two chunks of footprint 1"},
        {1, 0, [{10, 8}], [{9 * C, 8 * C}, {11 * C, 10 * C}],
            "First two chunks of footprint 0 of partition 1"}
    ],
    lists:foreach(
        fun({Partition, Footprint, FootprintIntervals, ByteIntervals, Title}) ->
            ?assertEqual(
                ByteIntervals,
                ar_intervals:to_list(ToBytes(FootprintIntervals)),
                Title
            ),
            ?assertEqual(
                FootprintIntervals,
                ar_intervals:to_list(
                    ToFootprint(ByteIntervals, Partition, Footprint)
                ),
                Title
            )
        end,
        RoundTrips
    ),
    %% Byte intervals that do not line up with chunks.
    OneWay = [
        {0, 1, [{C + 200, C + 100}], [{5, 4}], "A few bytes of a chunk"},
        {0, 0, [{C + 200, C + 100}], [], "A chunk of another footprint"},
        {0, 1, [{C, 0}], [], "An interval ending where a chunk starts"},
        {0, 0, [{8 * C, 0}], [{4, 0}], "The whole partition"}
    ],
    lists:foreach(
        fun({Partition, Footprint, ByteIntervals, FootprintIntervals, Title}) ->
            ?assertEqual(
                FootprintIntervals,
                ar_intervals:to_list(
                    ToFootprint(ByteIntervals, Partition, Footprint)
                ),
                Title
            )
        end,
        OneWay
    ).

bounded_footprint_intervals_to_byte_intervals(_Config) ->
    ?assertEqual(
        ar_intervals:from_list([{786432, 524288}, {1310720, 1048576}]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{4, 0}]), 262144, 1572864
        ),
        "Full Footprint 0, aligned boundaries"
    ),

    ?assertEqual(
        ar_intervals:from_list([
            {524288, 262144}, {1048576, 786432}, {1572864, 1310720}
        ]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{8, 4}]), 262144, 1572864
        ),
        "Full Footprint 1 cut to aligned boundaries"
    ),

    ?assertEqual(
        ar_intervals:from_list([
            {262144, 200000},
            {786432, 524288},
            {1310720, 1048576},
            {1600000, 1572864}
        ]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{4, 0}]), 200000, 1600000
        ),
        "Full Footprint 0, unaligned boundaries, pre-strict"
    ),

    ?assertEqual(
        ar_intervals:from_list([
            {524288, 262144}, {1048576, 786432}, {1572864, 1310720}
        ]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{8, 4}]), 200000, 1600000
        ),
        "Full Footprint 1, unaligned boundaries, pre-strict"
    ),

    ?assertEqual(
        ar_intervals:from_list([{2883584, 2621440}, {3407872, 3145728}]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{12, 8}]), 2400000, 3500000
        ),
        "Full Footprint 2, unaligned boundaries, post-strict"
    ),

    ?assertEqual(
        ar_intervals:from_list([
            {2621440, 2359296}, {3145728, 2883584}, {3500000, 3407872}
        ]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{16, 12}]), 2400000, 3500000
        ),
        "Full Footprint 3, unaligned boundaries, post-strict"
    ),

    ?assertEqual(
        ar_intervals:from_list([{2621440, 2359296}, {3500000, 3407872}]),
        arweave_storage_footprint_record:footprint_intervals_to_byte_intervals(
            ar_intervals:from_list([{16, 14}, {13, 12}]), 2400000, 3500000
        ),
        "Partial Footprint 3, unaligned boundaries, post-strict"
    ),

    ok.

footprint_geometry(_Config) ->
    %% Test geometry: 512 KiB sectors, 2 chunks per sector, 4 sectors per
    %% partition, partitions of 2,000,000 bytes.
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(262144)),
    ?assertEqual(1, arweave_storage_footprint_record:get_footprint(524288)),
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(786432)),
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(1835008)),
    ?assertEqual(1, arweave_storage_footprint_record:get_footprint(2097152)),
    %% Partition 1 starts at 2,000,000, so its buckets sit 97,152 bytes
    %% into each sector.
    ?assertEqual(0, arweave_storage_footprint_record:get_footprint(2359296)),
    ?assertEqual(1, arweave_storage_footprint_record:get_footprint(2621440)),
    ?assertEqual(
        786432, arweave_storage_footprint_record:get_next_sector_start(262144)
    ),
    ?assertEqual(
        786432, arweave_storage_footprint_record:get_next_sector_start(524288)
    ),
    ?assertEqual(
        1310720, arweave_storage_footprint_record:get_next_sector_start(786432)
    ),
    %% Sector 3 of partition 0 is followed by the first bucket of
    %% partition 1.
    ?assertEqual(
        2359296, arweave_storage_footprint_record:get_next_sector_start(1835008)
    ),
    ?assertEqual(
        2883584, arweave_storage_footprint_record:get_next_sector_start(2359296)
    ).

next_sector_start_clamps_to_partition(_Config) ->
    %% Four 512 KiB sectors overhang this smaller partition.
    arweave_storage_ct_util:with_mocks(
        [
            {arweave_constants, [
                {partition_size, fun() -> 1200000 end}
            ]}
        ],
        fun() ->
            ?assertEqual(
                1572864,
                arweave_storage_footprint_record:get_next_sector_start(1310720)
            ),
            ?assertEqual(
                0, arweave_storage_footprint_record:get_footprint(1572864)
            )
        end
    ).

%% @doc The store's sync record server builds its footprint record from the
%% {ar_data_sync, byte} record: each chunk under the packing it is recorded
%% with, the transitional unpacked_padded one left out, and nothing else.
%% Partition 0 is full, partitions 1 and 2 hold a few chunks and partition 3
%% none. Test geometry: 8 chunk offsets per partition in two footprints of
%% four.
initialize_records_synced_chunks(_Config) ->
    Replica = {replica_2_9, <<"addr">>},
    Recorded =
        [{N * ?DATA_CHUNK_SIZE, Replica} || N <- lists:seq(1, 8)] ++
            [
                {10 * ?DATA_CHUNK_SIZE, Replica},
                {12 * ?DATA_CHUNK_SIZE, Replica},
                {19 * ?DATA_CHUNK_SIZE, unpacked}
            ],
    Padded = {11 * ?DATA_CHUNK_SIZE, unpacked_padded},
    Range = {0, 4 * arweave_constants:partition_size()},
    initialize(Range, Recorded ++ [Padded], fun() ->
        assert_recorded(Recorded),
        ?assertEqual(
            false,
            arweave_storage_footprint_record:is_recorded(
                element(1, Padded), test_init_store
            )
        )
    end).

%% @doc A partition's entropy covers more footprint offsets than the partition
%% has chunks; the surplus offsets map to chunks of the next partition and
%% must not be recorded for them.
initialize_skips_entropy_tail_offsets(_Config) ->
    %% Five chunks per partition against eight footprint offsets: three
    %% surplus offsets in every partition.
    arweave_storage_ct_util:with_mocks(
        [{arweave_constants, [{partition_size, fun() -> 1200000 end}]}],
        fun() ->
            Replica = {replica_2_9, <<"addr">>},
            %% Every chunk of partitions 0 and 1.
            Chunks = [{N * ?DATA_CHUNK_SIZE, Replica} || N <- lists:seq(1, 9)],
            initialize({0, 2400000}, Chunks, fun() ->
                assert_recorded(Chunks)
            end)
        end
    ).

%% @doc A store whose range starts on a partition boundary and ends inside a
%% partition - the shapes production modules come in - records exactly its
%% own chunks. The range (2,000,000, 5,000,000] starts at partition 1, yet its
%% first chunk, ending at 2,097,152, has a bucket starting at 1,835,008 and so
%% belongs to entropy partition 0; the chunk ending at 4,194,304 straddles the
%% boundary at 4,000,000 the same way. The chunks ending at 1,835,008 and
%% 5,242,880 lie outside the range and must be left out even though the
%% {ar_data_sync, byte} record holds them.
initialize_mid_partition_range(_Config) ->
    Replica = {replica_2_9, <<"addr">>},
    %% Every chunk end is a multiple of 262,144 in the test geometry, the
    %% strict data split threshold (786,432) being one; 8 through 19 are the
    %% chunks in the range.
    InRange = [{N * ?DATA_CHUNK_SIZE, Replica} || N <- lists:seq(8, 19)],
    OutOfRange = [
        {7 * ?DATA_CHUNK_SIZE, Replica}, {20 * ?DATA_CHUNK_SIZE, Replica}
    ],
    initialize({2_000_000, 5_000_000}, InRange ++ OutOfRange, fun() ->
        assert_recorded(InRange),
        lists:foreach(
            fun({EndOffset, _Packing}) ->
                ?assertEqual(
                    false,
                    arweave_storage_footprint_record:is_recorded(
                        EndOffset, test_init_store
                    )
                )
            end,
            OutOfRange
        )
    end).

%% Build the footprint record of a store covering Range whose
%% {ar_data_sync, byte} record holds Chunks, through its sync record server as
%% in production, then run Assert.
initialize(Range, Chunks, Assert) ->
    StoreID = test_init_store,
    arweave_storage_ct_util:with_disk_stores([StoreID], Range, [], fun() ->
        {ok, _} = arweave_storage_ct_util:start_sync_record(StoreID),
        arweave_storage_ct_util:add_chunks_to_sync_record(Chunks, StoreID),
        ok = arweave_storage:initialize_footprint_record(StoreID),
        ok = ar_test_await:until(footprint_record_initialized, fun() ->
            arweave_storage:is_footprint_record_initialized(StoreID)
        end),
        Assert()
    end).

%% Each chunk is in the footprint record under its packing, and the record
%% holds nothing else.
assert_recorded(Chunks) ->
    lists:foreach(
        fun({EndOffset, Packing}) ->
            ?assertEqual(
                {true, Packing},
                arweave_storage_footprint_record:is_recorded(
                    EndOffset, test_init_store
                )
            )
        end,
        Chunks
    ),
    ?assertEqual(
        length(Chunks),
        ar_intervals:sum(footprint_record(any_packing, test_init_store))
    ).

footprint_record(Packing, StoreID) ->
    arweave_storage_sync_record:get(
        Packing, {ar_data_sync, footprint}, StoreID
    ).
