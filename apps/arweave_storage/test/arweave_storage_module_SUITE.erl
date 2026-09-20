-module(arweave_storage_module_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        label,
        disk_dir_name,
        range_helpers,
        has_any,
        get_unique_sorted_intervals,
        has_range,
        sort_storage_modules_by_left_bound,
        get_cover2
    ].

-define(LABEL_TEST_ADDR_A, <<"label-test-address-a-aaaaaaaaaaa">>).
-define(LABEL_TEST_ADDR_B, <<"label-test-address-b-bbbbbbbbbbb">>).
-define(LABEL_TEST_ADDR_C, <<255, 255, 255, "label-test-c-cccccccccccccccc">>).

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

%%====================================================================
%% Test cases
%%====================================================================

label(_Config) ->
    OldLabels = ets:match_object(arweave_storage, {{label, '_'}, '_'}),
    OldAddrLabels = ets:match_object(arweave_storage, {
        {address_label, '_'}, '_'
    }),
    OldLastLabel = ets:lookup(arweave_storage, last_address_label),
    ets:match_delete(arweave_storage, {{label, '_'}, '_'}),
    ets:match_delete(arweave_storage, {{address_label, '_'}, '_'}),
    ets:delete(arweave_storage, last_address_label),
    try
        arweave_config:internal_with_test_config(fun() ->
            P0 = arweave_constants:partition_size(),
            StorageModules = [
                {0, P0, {spora_2_6, ?LABEL_TEST_ADDR_A}},
                {2 * P0, 3 * P0, {spora_2_6, ?LABEL_TEST_ADDR_A}},
                {0, P0, {spora_2_6, ?LABEL_TEST_ADDR_B}},
                {3 * 524288, 4 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_B}},
                {2 * P0, 3 * P0, unpacked},
                {2 * P0, 3 * P0, {spora_2_6, ?LABEL_TEST_ADDR_C}},
                {2 * 524288, 3 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_C}}
            ],
            ok = arweave_config:internal_force_config(#{
                [storage_modules] => StorageModules
            }),
            P = arweave_constants:partition_size(),
            ?assertEqual(
                "storage_module_0_spora_2_6_1",
                arweave_storage_module:label(
                    arweave_storage_module:id(
                        {0, P, {spora_2_6, ?LABEL_TEST_ADDR_A}}
                    )
                )
            ),
            ?assertEqual(
                "storage_module_2_spora_2_6_1",
                arweave_storage_module:label(
                    arweave_storage_module:id({
                        2 * P, 3 * P, {spora_2_6, ?LABEL_TEST_ADDR_A}
                    })
                )
            ),
            ?assertEqual(
                "storage_module_0_spora_2_6_2",
                arweave_storage_module:label(
                    arweave_storage_module:id(
                        {0, P, {spora_2_6, ?LABEL_TEST_ADDR_B}}
                    )
                )
            ),
            ?assertEqual(
                "storage_module_1572864_2097152_spora_2_6_2",
                arweave_storage_module:label(
                    arweave_storage_module:id({
                        3 * 524288, 4 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_B}
                    })
                )
            ),
            ?assertEqual(
                "storage_module_2_unpacked",
                arweave_storage_module:label(
                    arweave_storage_module:id({2 * P, 3 * P, unpacked})
                )
            ),
            %% force a _ in the encoded address
            ?assertEqual(
                "storage_module_2_spora_2_6_3",
                arweave_storage_module:label(
                    arweave_storage_module:id({
                        2 * P, 3 * P, {spora_2_6, ?LABEL_TEST_ADDR_C}
                    })
                )
            ),
            ?assertEqual(
                "storage_module_1048576_1572864_spora_2_6_3",
                arweave_storage_module:label(
                    arweave_storage_module:id({
                        2 * 524288, 3 * 524288, {spora_2_6, ?LABEL_TEST_ADDR_C}
                    })
                )
            )
        end)
    after
        ets:match_delete(arweave_storage, {{label, '_'}, '_'}),
        ets:match_delete(arweave_storage, {{address_label, '_'}, '_'}),
        ets:delete(arweave_storage, last_address_label),
        ets:insert(arweave_storage, OldLabels),
        ets:insert(arweave_storage, OldAddrLabels),
        ets:insert(arweave_storage, OldLastLabel)
    end.

disk_dir_name(_Config) ->
    arweave_config:internal_with_test_config(fun() ->
        P = arweave_constants:partition_size(),
        BucketModule = {2 * 524288, 3 * 524288, unpacked},
        PartitionModule = {0, P, unpacked},
        ok = arweave_config:internal_force_config(
            #{[storage_modules] => [BucketModule, PartitionModule]}
        ),
        BucketStoreID = arweave_storage_module:id(BucketModule),
        PartitionStoreID = arweave_storage_module:id(PartitionModule),
        ?assertEqual(
            "storage_module_1048576_1572864_unpacked",
            BucketStoreID
        ),
        %% Current-notation launch: the id names the directory.
        ?assertEqual(
            BucketStoreID, arweave_storage_module:disk_dir_name(BucketStoreID)
        ),
        %% Legacy-notation launch: bucket-expressible modules keep the
        %% legacy bucket-notation directory name... (internal_force_config: the
        %% test configuration is in runtime mode, where the load-only
        %% config_dialect option rejects plain sets.)
        ok = arweave_config:internal_force_config(#{[config_dialect] => legacy}),
        ?assertEqual(
            "storage_module_524288_2_unpacked",
            arweave_storage_module:disk_dir_name(BucketStoreID)
        ),
        %% ...and whole partitions keep their (identical) name.
        ?assertEqual(
            PartitionStoreID,
            arweave_storage_module:disk_dir_name(PartitionStoreID)
        )
    end).

range_helpers(_Config) ->
    arweave_config:internal_with_test_config(fun() ->
        %% A one-megabyte range starting at one megabyte, in byte notation.
        Module = {1_000_000, 2_000_000, unpacked},
        StoreID = arweave_storage_module:id(Module),
        ok = arweave_config:internal_force_config(#{
            [storage_modules] => [Module]
        }),
        RawRange = arweave_storage_module:module_range(Module),
        ?assertEqual(
            {1_000_000,
                2_000_000 + arweave_storage_module:get_overlap(unpacked)},
            RawRange
        ),
        ?assertEqual(RawRange, arweave_storage_module:get_range_safe(StoreID)),
        {RangeStart, RangeEnd} = RawRange,
        ?assertEqual(
            {
                max(
                    0,
                    arweave_constants:get_chunk_padded_offset(RangeStart) -
                        ?DATA_CHUNK_SIZE
                ),
                arweave_constants:get_chunk_padded_offset(RangeEnd)
            },
            arweave_storage_module:get_padded_range(StoreID)
        ),
        ?assertEqual(
            not_found, arweave_storage_module:get_range_safe("missing_store")
        ),
        ?assertEqual(
            {-1, -1}, arweave_storage_module:get_padded_range("missing_store")
        ),
        ?assertEqual(
            {0, infinity},
            arweave_storage_module:get_range_safe(?DEFAULT_MODULE)
        ),
        ?assertEqual(
            {-1, -1}, arweave_storage_module:get_padded_range(?DEFAULT_MODULE)
        )
    end).

has_any(_Config) ->
    ?assertEqual(false, arweave_storage_module:has_any(0, [])),
    ?assertEqual(false, arweave_storage_module:has_any(0, [{10, 20, p}])),
    ?assertEqual(false, arweave_storage_module:has_any(10, [{10, 20, p}])),
    ?assertEqual(true, arweave_storage_module:has_any(11, [{10, 20, p}])),
    ?assertEqual(
        true, arweave_storage_module:has_any(11, [{10, 20, {replica_2_9, a}}])
    ),
    ?assertEqual(
        true,
        arweave_storage_module:has_any(
            20 + arweave_storage_module:get_overlap(unpacked), [{10, 20, p}]
        )
    ),
    ?assertEqual(
        true,
        arweave_storage_module:has_any(
            20 + arweave_storage_module:get_overlap(unpacked), [
                {10, 20, {replica_2_9, a}}
            ]
        )
    ),
    %% Unaligned range.
    ?assertEqual(false, arweave_storage_module:has_any(7, [{7, 15, p}])),
    ?assertEqual(true, arweave_storage_module:has_any(8, [{7, 15, p}])),
    ?assertEqual(
        true,
        arweave_storage_module:has_any(
            15 + arweave_storage_module:get_overlap(unpacked), [{7, 15, p}]
        )
    ),
    ?assertEqual(
        false,
        arweave_storage_module:has_any(
            16 + arweave_storage_module:get_overlap(unpacked), [{7, 15, p}]
        )
    ).

get_unique_sorted_intervals(_Config) ->
    ?assertEqual(
        [{0, 24}, {90, 120}],
        arweave_storage_module:get_unique_sorted_intervals([
            {0, 10, p}, {90, 120, p}, {0, 20, p}, {12, 24, p}
        ])
    ),
    %% Unaligned ranges merge the same way.
    ?assertEqual(
        [{3, 17}],
        arweave_storage_module:get_unique_sorted_intervals([
            {3, 11, p}, {7, 17, p}
        ])
    ).

has_range(_Config) ->
    ?assertEqual(false, arweave_storage_module:has_range(0, 10, [])),
    ?assertEqual(false, arweave_storage_module:has_range(0, 10, [{0, 9}])),
    ?assertEqual(true, arweave_storage_module:has_range(0, 10, [{0, 10}])),
    ?assertEqual(true, arweave_storage_module:has_range(0, 10, [{0, 11}])),
    ?assertEqual(
        true, arweave_storage_module:has_range(0, 10, [{0, 9}, {9, 10}])
    ),
    ?assertEqual(
        true, arweave_storage_module:has_range(5, 10, [{0, 9}, {9, 10}])
    ),
    ?assertEqual(
        true, arweave_storage_module:has_range(5, 10, [{0, 2}, {2, 9}, {9, 10}])
    ).

sort_storage_modules_by_left_bound(_Config) ->
    ?assertEqual(
        [], arweave_storage_module:sort_storage_modules_by_left_bound([], none)
    ),
    ?assertEqual(
        [{0, 1, p}],
        arweave_storage_module:sort_storage_modules_by_left_bound(
            [{0, 1, p}], none
        )
    ),
    ?assertEqual(
        [{0, 10, p}, {10, 20, p}, {20, 30, p}],
        arweave_storage_module:sort_storage_modules_by_left_bound(
            [{10, 20, p}, {0, 10, p}, {20, 30, p}], none
        )
    ),
    ?assertEqual(
        [{0, 10, p}, {7, 14, p}, {10, 20, p}, {20, 30, p}],
        arweave_storage_module:sort_storage_modules_by_left_bound(
            [
                {10, 20, p},
                {0, 10, p},
                {20, 30, p},
                {7, 14, p}
            ],
            none
        )
    ),
    ?assertEqual(
        [{0, 10, p}, {10, 20, p}, {10, 20, p2}],
        arweave_storage_module:sort_storage_modules_by_left_bound(
            [{10, 20, p}, {0, 10, p}, {10, 20, p2}], none
        )
    ),
    ?assertEqual(
        [{0, 10, p}, {10, 20, p2}, {10, 20, p}],
        arweave_storage_module:sort_storage_modules_by_left_bound(
            [{10, 20, p}, {0, 10, p}, {10, 20, p2}],
            {10, 20, p2}
        )
    ).

get_cover2(_Config) ->
    ?assertEqual(not_found, arweave_storage_module:get_cover2(0, 1, [])),
    ?assertEqual(
        [{0, 1, "storage_module_0_1_p"}],
        arweave_storage_module:get_cover2(0, 1, [{0, 1, p}])
    ),
    ?assertEqual(
        [{0, 1, "storage_module_0_1_p"}, {1, 2, "storage_module_1_2_p"}],
        arweave_storage_module:get_cover2(0, 2, [{0, 1, p}, {1, 2, p}])
    ),
    ?assertEqual(
        not_found,
        arweave_storage_module:get_cover2(0, 2, [{0, 1, p}, {2, 3, p}])
    ),
    ?assertEqual(
        [{0, 2, "storage_module_0_2_p"}],
        arweave_storage_module:get_cover2(0, 2, [{0, 2, p}, {0, 1, p}])
    ),
    ?assertEqual(
        [{0, 2, "storage_module_0_2_p"}, {2, 3, "storage_module_0_3_p"}],
        arweave_storage_module:get_cover2(0, 3, [{0, 2, p}, {0, 3, p}])
    ),
    ?assertEqual(
        [{0, 1, "storage_module_0_1_p"}, {1, 3, "storage_module_1_3_p"}],
        arweave_storage_module:get_cover2(0, 3, [{0, 1, p}, {1, 3, p}])
    ).
