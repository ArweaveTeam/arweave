-module(arweave_storage_sync_record_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        point_selectors,
        add_selectors,
        record_mutations,
        record_query_selectors,
        existing_point_search_scope,
        single_store_intervals,
        cross_store_intervals,
        cross_store_matches_union,
        footprint_selectors
    ].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

init_per_testcase(_, Config) ->
    ets:new(sync_records, [named_table, public, ordered_set]),
    [{config_snapshot, arweave_config:internal_snapshot()} | Config].

end_per_testcase(_, Config) ->
    arweave_config:internal_restore(proplists:get_value(config_snapshot, Config)).

%%====================================================================
%% Test cases
%%====================================================================

%% @doc Byte and footprint selectors stay distinct, including cross-store
%% footprint searches.
footprint_selectors(_) ->
    %% The same numeric offset identifies unrelated bytes and footprint entries.
    seed_record({ar_data_sync, store_a}, [{10, 0}]),
    seed_record({ar_data_sync_footprints, store_a}, [{20, 10}]),
    seed_record({ar_data_sync_footprints, unpacked, store_a}, [{20, 10}]),
    seed_record({ar_data_sync_footprints, store_b}, [{30, 20}]),
    seed_record({ar_data_sync_footprints, unpacked, store_b}, [{30, 20}]),
    Byte = {ar_data_sync, byte},
    Footprint = {ar_data_sync, footprint},
    ?assertEqual(
        false,
        arweave_storage:is_recorded(15, any_packing, Byte, store_a)
    ),
    ?assertEqual(
        {true, unpacked},
        arweave_storage:is_recorded(15, any_packing, Footprint, store_a)
    ),
    %% No configured module covers byte 15: footprint searches must not use it
    %% as a physical byte to choose candidate stores.
    ok = arweave_config:internal_force_config(#{[storage_modules] => []}),
    ?assertEqual(
        {{true, unpacked}, store_a},
        arweave_storage:is_recorded(15, unpacked, Footprint, any_store)
    ),
    ?assertEqual(
        {30, 10},
        arweave_storage:get_next_interval(
            synced, 0, 40, any_packing, Footprint, any_store
        )
    ),
    ?assertEqual(
        [{25, 15}],
        ar_intervals:to_list(
            arweave_storage:get_intervals(
                synced,
                15,
                25,
                unpacked,
                Footprint,
                any_store
            )
        )
    ),
    ?assertEqual(
        [{10, 0}, {40, 30}],
        ar_intervals:to_list(
            arweave_storage:get_intervals(
                unsynced,
                0,
                40,
                any_packing,
                Footprint,
                any_store
            )
        )
    ),
    ?assertException(
        error,
        function_clause,
        arweave_storage:is_recorded(
            1,
            any_packing,
            {ar_chunk_storage, footprint},
            store_a
        )
    ),
    ?assertException(
        error,
        function_clause,
        arweave_storage:is_recorded(
            1,
            any_packing,
            {ar_data_sync_footprints, byte},
            store_a
        )
    ).

%% @doc Point lookups honor concrete or wildcard store and packing selectors.
point_selectors(_) ->
    %% Two modules cover the same ten bytes but have different packings.
    Packing = {spora_2_6, <<0:256>>},
    Module = {0, 10, Packing},
    ok = arweave_config:internal_force_config(#{[storage_modules] => [Module]}),
    #store_info{id = StoreID} = arweave_storage:store_info(Module),
    seed_record({record_id, StoreID}, [{10, 0}]),
    seed_record({record_id, Packing, StoreID}, [{10, 0}]),
    seed_record({record_id, ?DEFAULT_MODULE}, [{5, 0}]),
    seed_record({record_id, unpacked, ?DEFAULT_MODULE}, [{5, 0}]),
    ?assertEqual(
        {true, Packing},
        arweave_storage:is_recorded(1, any_packing, {record_id, byte}, StoreID)
    ),
    ?assertEqual(
        true,
        arweave_storage:is_recorded(1, Packing, {record_id, byte}, StoreID)
    ),
    ?assertEqual(
        false,
        arweave_storage:is_recorded(1, unpacked, {record_id, byte}, StoreID)
    ),
    ?assertEqual(
        {{true, unpacked}, ?DEFAULT_MODULE},
        arweave_storage:is_recorded(1, any_packing, {record_id, byte}, any_store)
    ),
    %% Explicit candidates exclude the default store unless it is supplied,
    %% and their order takes precedence over the default-store preference.
    ?assertEqual(
        {{true, Packing}, StoreID},
        arweave_storage:is_recorded_any(1, {record_id, byte}, [Module])
    ),
    ?assertEqual(
        {{true, Packing}, StoreID},
        arweave_storage:is_recorded_any(
            1, {record_id, byte}, [Module, ?DEFAULT_MODULE]
        )
    ),
    ?assertEqual(
        false,
        arweave_storage:is_recorded_any(1, {record_id, byte}, [])
    ),
    ?assertEqual(
        {{true, Packing}, StoreID},
        arweave_storage:is_recorded(6, any_packing, {record_id, byte}, any_store)
    ),
    ?assertEqual(
        {{true, Packing}, StoreID},
        arweave_storage:is_recorded(1, Packing, {record_id, byte}, any_store)
    ),
    ?assertEqual(
        {{true, unpacked}, ?DEFAULT_MODULE},
        arweave_storage:is_recorded(1, unpacked, {record_id, byte}, any_store)
    ),
    ?assertEqual(
        false,
        arweave_storage:is_recorded(6, unpacked, {record_id, byte}, any_store)
    ),
    ?assertEqual(
        false,
        arweave_storage:is_recorded(0, any_packing, {record_id, byte}, any_store)
    ),
    ?assertEqual(
        false,
        arweave_storage:is_recorded(11, any_packing, {record_id, byte}, any_store)
    ).

%% @doc Record additions distinguish untyped and packed data and reject invalid
%% selectors.
add_selectors(_) ->
    StoreID = storage_record_test,
    {ok, PID} = arweave_storage_sync_record:start_link(
        arweave_storage_sync_record:name(StoreID), StoreID
    ),
    try
        %% Adjacent ten-byte writes: the first is untyped, the second unpacked.
        ?assertEqual(
            ok,
            arweave_storage:add_sync_record(
                10, 0, any_packing, {record_id, byte}, StoreID
            )
        ),
        ?assertEqual(
            true,
            arweave_storage:is_recorded(5, any_packing, {record_id, byte}, StoreID)
        ),
        ?assertEqual(
            false,
            arweave_storage:is_recorded(5, unpacked, {record_id, byte}, StoreID)
        ),
        ?assertNot(ets:member(sync_records, {record_id, any_packing, StoreID})),
        ?assertEqual(
            ok,
            arweave_storage:add_sync_record(
                20, 10, unpacked, {record_id, byte}, StoreID
            )
        ),
        ?assertEqual(
            {20, 0},
            arweave_storage:get_next_interval(
                synced, 0, 30, any_packing, {record_id, byte}, StoreID
            )
        ),
        ?assertEqual(
            {20, 10},
            arweave_storage:get_next_interval(
                synced, 0, 30, unpacked, {record_id, byte}, StoreID
            )
        ),
        ?assertException(
            error,
            function_clause,
            arweave_storage:add_sync_record(
                30, 20, any_packing, {record_id, byte}, any_store
            )
        ),
        ?assertException(
            error,
            function_clause,
            arweave_storage:add_sync_record(
                30, 20, unpacked, {record_id, byte}, any_store
            )
        )
    after
        gen_server:stop(PID)
    end.

%% @doc Byte and footprint selectors preserve native record IDs across reads,
%% synchronous and asynchronous writes, deletes, and cuts.
record_mutations(_) ->
    StoreID = storage_record_test,
    {ok, PID} = arweave_storage_sync_record:start_link(
        arweave_storage_sync_record:name(StoreID), StoreID
    ),
    try
        lists:foreach(
            fun({Record, ID}) ->
                ?assertNot(arweave_storage:sync_record_exists(any_packing, Record, StoreID)),
                %% Three ten-unit ranges exercise clipping in either index.
                ?assertEqual(
                    ok,
                    arweave_storage:add_sync_record(
                        10, 0, unpacked, Record, StoreID
                    )
                ),
                ?assert(arweave_storage:sync_record_exists(any_packing, Record, StoreID)),
                ?assert(ets:member(sync_records, {ID, StoreID})),
                ?assertNot(ets:member(sync_records, {Record, StoreID})),
                ?assertEqual(
                    [{10, 0}],
                    ar_intervals:to_list(
                        arweave_storage:get_sync_record(any_packing, Record, StoreID)
                    )
                ),
                ?assertEqual(
                    {10, 0},
                    arweave_storage:get_interval(5, any_packing, Record, StoreID)
                ),
                ?assertEqual(
                    5,
                    arweave_storage:get_intersection_size(15, 5, any_packing, Record, StoreID)
                ),
                ?assertEqual(
                    ok,
                    arweave_storage:delete_sync_record(10, 5, Record, StoreID)
                ),
                ?assertEqual(
                    ok,
                    arweave_storage_sync_record:add_async(
                        test_add, 20, 10, Record, StoreID
                    )
                ),
                ?assertEqual(
                    ok,
                    arweave_storage_sync_record:add_async(
                        test_add, 30, 20, unpacked, Record, StoreID
                    )
                ),
                %% The synchronous cut follows both casts from this process.
                ?assertEqual(
                    ok,
                    arweave_storage:cut_sync_record(25, Record, StoreID)
                ),
                ?assertEqual(
                    [{5, 0}, {25, 10}],
                    ar_intervals:to_list(
                        arweave_storage:get_sync_record(any_packing, Record, StoreID)
                    )
                ),
                ?assertEqual(
                    [{5, 0}, {25, 20}],
                    ar_intervals:to_list(
                        arweave_storage_sync_record:get(unpacked, Record, StoreID)
                    )
                )
            end,
            [
                {{ar_data_sync, byte}, ar_data_sync},
                {{ar_data_sync, footprint}, ar_data_sync_footprints}
            ]
        )
    after
        gen_server:stop(PID)
    end.

%% @doc Single-store searches return bounded synced ranges or gaps for the
%% requested packing.
single_store_intervals(_) ->
    %% Two ten-byte ranges with a ten-byte hole between them.
    seed_record({record_id, store_a}, [{10, 0}, {30, 20}]),
    seed_record({record_id, unpacked, store_a}, [{10, 0}]),
    ?assertEqual(
        {10, 0},
        arweave_storage:get_next_interval(
            synced, 5, 40, any_packing, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        {7, 0},
        arweave_storage:get_next_interval(
            synced, 5, 7, any_packing, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        {30, 20},
        arweave_storage:get_next_interval(
            synced, 10, 40, any_packing, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        {20, 10},
        arweave_storage:get_next_interval(
            unsynced, 0, 40, any_packing, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        {40, 10},
        arweave_storage:get_next_interval(
            unsynced, 0, 40, unpacked, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_next_interval(
            synced, 10, 40, unpacked, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_next_interval(
            synced, 0, 40, any_packing, {missing_record, byte}, store_a
        )
    ),
    ?assertEqual(
        {40, 0},
        arweave_storage:get_next_interval(
            unsynced, 0, 40, any_packing, {record_id, byte}, missing_store
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_next_interval(
            unsynced, 40, 40, any_packing, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_next_interval(
            synced, 40, 30, any_packing, {record_id, byte}, store_a
        )
    ),
    ?assertEqual(
        [{20, 10}, {40, 30}],
        ar_intervals:to_list(
            arweave_storage:get_intervals(
                unsynced,
                0,
                40,
                any_packing,
                {record_id, byte},
                store_a
            )
        )
    ).

%% @doc Cross-store searches merge adjacent coverage and report gaps with
%% packing filters.
cross_store_intervals(_) ->
    %% Three stores bridge adjacent ranges into (0, 40], with a gap to (50, 60].
    seed_record({record_id, store_a}, [{10, 0}, {40, 30}]),
    seed_record({record_id, store_b}, [{20, 10}]),
    seed_record({record_id, store_c}, [{30, 20}, {60, 50}]),
    seed_record({record_id, unpacked, store_a}, [{10, 0}]),
    seed_record({record_id, unpacked, store_c}, [{60, 50}]),
    ?assertEqual(
        {40, 0},
        arweave_storage:get_next_interval(
            synced, 25, 70, any_packing, {record_id, byte}, any_store
        )
    ),
    ?assertEqual(
        {27, 0},
        arweave_storage:get_next_interval(
            synced, 25, 27, any_packing, {record_id, byte}, any_store
        )
    ),
    ?assertEqual(
        {50, 40},
        arweave_storage:get_next_interval(
            unsynced, 0, 70, any_packing, {record_id, byte}, any_store
        )
    ),
    ?assertEqual(
        {60, 50},
        arweave_storage:get_next_interval(
            synced, 40, infinity, any_packing, {record_id, byte}, any_store
        )
    ),
    ?assertEqual(
        {infinity, 60},
        arweave_storage:get_next_interval(
            unsynced, 55, infinity, any_packing, {record_id, byte}, any_store
        )
    ),
    ?assertEqual(
        {50, 10},
        arweave_storage:get_next_interval(
            unsynced, 0, 70, unpacked, {record_id, byte}, any_store
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_next_interval(
            synced, 0, 70, any_packing, {missing_record, byte}, any_store
        )
    ),
    ?assertEqual(
        {70, 0},
        arweave_storage:get_next_interval(
            unsynced, 0, 70, any_packing, {missing_record, byte}, any_store
        )
    ).

%% @doc Cross-store interval queries match an explicit union across offsets,
%% bounds and packings.
cross_store_matches_union(_) ->
    %% Overlap, adjacency and nesting across three stores, followed by a gap.
    Records = [
        [{10, 0}, {40, 30}],
        [{20, 5}, {60, 50}],
        [{8, 2}, {30, 20}, {65, 55}]
    ],
    lists:foreach(
        fun({StoreID, Intervals}) ->
            seed_record({record_id, StoreID}, Intervals),
            seed_record({record_id, unpacked, StoreID}, Intervals)
        end,
        lists:zip([store_a, store_b, store_c], Records)
    ),
    Expected = ets:new(expected_union, [ordered_set]),
    lists:foreach(
        fun({End, Start}) ->
            ar_ets_intervals:add(Expected, End, Start)
        end,
        lists:append(Records)
    ),
    %% Every byte through the final end plus one, with bounds inside and outside.
    lists:foreach(
        fun({Offset, End, Packing, Status}) ->
            Interval =
                case Status of
                    synced -> ar_ets_intervals:get_next_interval(Expected, Offset, End);
                    unsynced -> ar_ets_intervals:get_next_interval_outside(Expected, Offset, End)
                end,
            ?assertEqual(
                Interval,
                arweave_storage:get_next_interval(
                    Status, Offset, End, Packing, {record_id, byte}, any_store
                )
            )
        end,
        [
            {Offset, End, Packing, Status}
         || Offset <- lists:seq(0, 66),
            End <- [0, 1, 6, 23, 45, 70, infinity],
            Packing <- [any_packing, unpacked],
            Status <- [synced, unsynced]
        ]
    ).

%% @doc All record reads honor selectors, distinguish empty records, and merge
%% overlapping or adjacent ranges without counting bytes twice.
record_query_selectors(_) ->
    lists:foreach(
        fun({Record, ID}) ->
            %% The union is (0, 40]; unpacked covers (0, 30] and packed (30, 40].
            seed_record({ID, store_a}, [{10, 0}, {40, 30}]),
            seed_record({ID, unpacked, store_a}, [{10, 0}]),
            seed_record({ID, packed, store_a}, [{40, 30}]),
            seed_record({ID, store_b}, [{30, 5}]),
            seed_record({ID, unpacked, store_b}, [{30, 5}]),
            seed_record({ID, store_empty}, []),
            seed_record({ID, unpacked, store_empty}, []),
            lists:foreach(
                fun({Packing, StoreID, Intervals, Exists}) ->
                    Expected = ets:new(expected_query, [ordered_set]),
                    Set = ar_intervals:from_list(Intervals),
                    ar_ets_intervals:init_from_gb_set(Expected, Set),
                    ?assertEqual(
                        Exists,
                        arweave_storage:sync_record_exists(Packing, Record, StoreID)
                    ),
                    ?assertEqual(
                        ar_intervals:to_list(Set),
                        ar_intervals:to_list(
                            arweave_storage:get_sync_record(Packing, Record, StoreID)
                        )
                    ),
                    %% Include both interval boundaries, gaps, and the origin.
                    lists:foreach(
                        fun(Offset) ->
                            ?assertEqual(
                                ar_ets_intervals:get_interval_with_byte(Expected, Offset),
                                arweave_storage:get_interval(
                                    Offset, Packing, Record, StoreID
                                )
                            )
                        end,
                        [0, 1, 5, 10, 15, 30, 35, 40, 41]
                    ),
                    lists:foreach(
                        fun({End, Start}) ->
                            ?assertEqual(
                                ar_ets_intervals:get_intersection_size(Expected, End, Start),
                                arweave_storage:get_intersection_size(
                                    End, Start, Packing, Record, StoreID
                                )
                            )
                        end,
                        [{40, 0}, {35, 5}, {30, 10}, {41, 40}]
                    ),
                    ets:delete(Expected)
                end,
                [
                    {any_packing, store_a, [{10, 0}, {40, 30}], true},
                    {unpacked, store_a, [{10, 0}], true},
                    {packed, store_a, [{40, 30}], true},
                    {any_packing, any_store, [{40, 0}], true},
                    {unpacked, any_store, [{30, 0}], true},
                    {packed, any_store, [{40, 30}], true},
                    {any_packing, store_empty, [], true},
                    {unpacked, store_empty, [], true},
                    {packed, store_empty, [], false},
                    {missing_packing, any_store, [], false},
                    {any_packing, missing_store, [], false}
                ]
            ),
            ?assertNot(
                arweave_storage:sync_record_exists(
                    any_packing, {missing_record, byte}, any_store
                )
            )
        end,
        [
            {{ar_data_sync, byte}, ar_data_sync},
            {{ar_data_sync, footprint}, ar_data_sync_footprints}
        ]
    ).

%% @doc Existing byte point queries keep configured-store selection even though
%% interval queries also see registered records outside that selection.
existing_point_search_scope(_) ->
    ok = arweave_config:internal_force_config(#{[storage_modules] => []}),
    seed_record({record_id, unconfigured_store}, [{10, 0}]),
    seed_record({record_id, unpacked, unconfigured_store}, [{10, 0}]),
    Record = {record_id, byte},
    ?assertEqual(false, arweave_storage:is_recorded(5, any_packing, Record, any_store)),
    ?assertEqual(false, arweave_storage:is_recorded(5, unpacked, Record, any_store)),
    ?assertEqual(
        {10, 0},
        arweave_storage:get_next_interval(synced, 0, 20, any_packing, Record, any_store)
    ),
    ?assertEqual(
        {10, 0},
        arweave_storage:get_interval(5, any_packing, Record, any_store)
    ).

%%====================================================================
%% Helpers
%%====================================================================

seed_record(Key, Intervals) ->
    TID = ets:new(test_record, [ordered_set]),
    ar_ets_intervals:init_from_gb_set(TID, ar_intervals:from_list(Intervals)),
    ets:insert(sync_records, {Key, TID}).
