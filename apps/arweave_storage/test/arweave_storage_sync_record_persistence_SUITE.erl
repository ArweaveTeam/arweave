-module(arweave_storage_sync_record_persistence_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [
    get_after_add,
    persistence_roundtrip,
    snapshots_take_turns,
    data_sizes_published_on_load,
    data_sizes_split_by_partition,
    data_sizes_shared_partition,
    footprint_record_built_by_its_server,
    footprint_initialization_retries_cursor_write,
    footprint_initialization_marked_complete,
    footprint_initialization_resumes_before_first_step,
    footprint_initialization_resumes_from_persisted_cursor
].

init_per_suite(Config) -> arweave_storage_ct_util:init_suite(Config).

end_per_suite(Config) -> arweave_storage_ct_util:end_suite(Config).

init_per_testcase(Name, Config) ->
    Config2 = arweave_storage_ct_util:init_case(Name, Config),
    {ok, _} = arweave_storage:activate(),
    Config2.

end_per_testcase(Name, Config) ->
    arweave_storage_ct_util:stop_record(test_sync_record_get),
    arweave_storage_ct_util:stop_record(test_sync_record_persist),
    arweave_storage_ct_util:stop_record(test_sync_record_turn),
    arweave_storage_ct_util:stop_record(test_sync_record_sizes),
    arweave_storage_ct_util:stop_record(test_sync_record_split),
    arweave_storage_ct_util:stop_record(test_sync_record_left),
    arweave_storage_ct_util:stop_record(test_sync_record_right),
    arweave_storage_ct_util:stop_record(test_sync_record_footprint),
    arweave_storage_ct_util:end_case(Name, Config).

%%====================================================================
%% Test cases
%%====================================================================

get_after_add(_Config) ->
    arweave_storage_ct_util:with_mocks(
        [
            {arweave_storage_module, [
                {get_by_id, fun
                    (test_sync_record_get) -> test_sync_record_get;
                    (StoreID) -> meck:passthrough([StoreID])
                end}
            ]}
        ],
        fun() ->
            StoreID = test_sync_record_get,
            Record = {test_sync_record_get_id, byte},
            {ok, PID} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(StoreID), StoreID
            ),
            ?assertEqual(
                [],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:add(
                2, 0, unpacked, Record, StoreID
            ),
            ?assertEqual(
                [{2, 0}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{2, 0}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:add(
                4, 3, any_packing, Record, StoreID
            ),
            ?assertEqual(
                [{2, 0}, {4, 3}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{2, 0}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:delete(2, 1, Record, StoreID),
            ?assertEqual(
                [{1, 0}, {4, 3}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{1, 0}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:cut(1, Record, StoreID),
            ?assertEqual(
                [{1, 0}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            kill(PID)
        end
    ).

persistence_roundtrip(_Config) ->
    arweave_storage_ct_util:with_mocks(
        [
            {arweave_storage_module, [
                {get_by_id, fun
                    (test_sync_record_persist) ->
                        {0, arweave_lib_constants:partition_size(), unpacked};
                    (StoreID) ->
                        meck:passthrough([StoreID])
                end},
                {info, fun
                    (test_sync_record_persist = StoreID) ->
                        #store_info{
                            id = StoreID,
                            path = filename:join([
                                arweave_config:get([data_dir]),
                                "storage_modules",
                                atom_to_list(StoreID)
                            ])
                        };
                    (StoreID) ->
                        meck:passthrough([StoreID])
                end}
            ]}
        ],
        fun() ->
            StoreID = test_sync_record_persist,
            Record = {test_sync_record_persist_id, byte},
            StateDB = {sync_record, StoreID},
            {ok, PID} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(StoreID), StoreID
            ),
            %% The operations below land in the WAL; kill the server before the next
            %% snapshot so init has to replay them into the ETS tables.
            ok = arweave_storage_sync_record:add(
                2, 1, any_packing, Record, StoreID
            ),
            ok = arweave_storage_sync_record:add(
                4, 3, unpacked, Record, StoreID
            ),
            ok = arweave_storage_sync_record:cut(3, Record, StoreID),
            kill(PID),
            {ok, PID2} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(StoreID), StoreID
            ),
            ?assertEqual(
                [{2, 1}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ?assertEqual(
                true,
                arweave_storage_sync_record:is_recorded(
                    2, any_packing, Record, StoreID
                )
            ),
            %% Mutate again and wait for the periodic snapshot (it resets the WAL
            %% counter) so the next restart loads the state from the snapshot.
            ok = arweave_storage_sync_record:add(
                6, 5, unpacked, Record, StoreID
            ),
            ok = arweave_storage_sync_record:delete(2, 1, Record, StoreID),
            ok = ar_test_await:until(sync_record_snapshot_stored, fun() ->
                case ar_kv:get(StateDB, <<"wal">>) of
                    {ok, V} -> binary:decode_unsigned(V) == 0;
                    _ -> false
                end
            end),
            {ok, Snapshot} = ar_kv:get(StateDB, <<"sync_records">>),
            %% 131 is the external-term version and 80 is its compressed-term tag.
            ?assertMatch(<<131, 80, _/binary>>, Snapshot),
            kill(PID2),
            {ok, PID3} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(StoreID), StoreID
            ),
            ?assertEqual(
                [{6, 5}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{6, 5}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ?assertEqual(
                false,
                arweave_storage_sync_record:is_recorded(
                    2, any_packing, Record, StoreID
                )
            ),
            kill(PID3),
            %% Snapshots written before compression must remain readable on upgrade.
            ok = ar_kv:put(
                StateDB,
                <<"sync_records">>,
                term_to_binary(binary_to_term(Snapshot, [safe]))
            ),
            {ok, PID4} = arweave_storage_sync_record:start_link(
                arweave_storage_sync_record:name(StoreID), StoreID
            ),
            ?assertEqual(
                [{6, 5}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{6, 5}],
                arweave_lib_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            kill(PID4)
        end
    ).

%% @doc Stores snapshot one at a time: a store that finds the snapshot token
%% held by a live process retries after the short token delay, a token left
%% by a dead holder is taken over, a completed snapshot schedules the next one
%% a full period away, and a store whose WAL is empty skips the snapshot. A
%% plain process stands in for the other store holding the token, since a
%% real store releases it within a millisecond in the test profile.
snapshots_take_turns(_Config) ->
    Parent = self(),
    StoreID = test_sync_record_turn,
    Record = {test_sync_record_turn_id, byte},
    StateDB = {sync_record, StoreID},
    with_disk_store(StoreID, [
        {ar_timer, [
            %% Capture the delays the stores schedule instead of running them.
            {apply_after, fun(Delay, _M, _F, _A, _O) ->
                Parent ! {scheduled, self(), Delay},
                {ok, make_ref()}
            end}
        ]},
        %% Count the database writes, running them as usual.
        {ar_kv, []}
    ], fun() ->
        {ok, PID} = arweave_storage_ct_util:start_sync_record(StoreID),
        %% Test profile: snapshots are a second apart and the first one lands
        %% within the second after that.
        Period = 1000,
        ?assert(receive_scheduled(PID) > Period),
        ok = arweave_storage_sync_record:add(2, 0, unpacked, Record, StoreID),
        %% A live process outside this suite holds the token.
        Holder = spawn(fun() -> receive stop -> ok end end),
        true = ets:insert(sync_records, {snapshot_token, Holder}),
        gen_server:cast(PID, store_state),
        _ = sys:get_state(PID),
        %% WAL hasn't been snapshotted since Holder holds the token
        ?assertEqual({ok, 1}, wal_count(StateDB)),
        %% Rescheduled after the token retry delay, 100 ms in the test profile.
        ?assertEqual(100, receive_scheduled(PID)),
        %% A reader reaches this key before the token holder dies.
        [{ReaderKey, Holder}] = ets:lookup(sync_records, snapshot_token),
        %% The holder dies without releasing; the next attempt takes over.
        exit(Holder, kill),
        ok = ar_test_await:until(holder_dead, fun() ->
            not is_process_alive(Holder)
        end),
        gen_server:cast(PID, store_state),
        _ = sys:get_state(PID),
        %% WAL count 0 because it has been snapshotted
        ?assertEqual({ok, 0}, wal_count(StateDB)),
        %% Releasing the token keeps the reader's traversal key valid.
        ?assertEqual(
            [{snapshot_token, undefined}],
            ets:lookup(sync_records, snapshot_token)
        ),
        ?assertNotEqual(ReaderKey, ets:next(sync_records, ReaderKey)),
        ?assertEqual({true, unpacked}, arweave_storage:is_recorded(
            2, any_packing, Record, StoreID
        )),
        ?assertEqual(Period, receive_scheduled(PID)),
        %% Cast again with nothing added since the last snapshot: the empty WAL
        %% means no snapshot is written and the next attempt is a full period out.
        Writes = meck:num_calls(ar_kv, put, [StateDB, <<"sync_records">>, '_']),
        gen_server:cast(PID, store_state),
        _ = sys:get_state(PID),
        ?assertEqual(
            Writes,
            meck:num_calls(ar_kv, put, [StateDB, <<"sync_records">>, '_'])
        ),
        ?assertEqual(Period, receive_scheduled(PID)),
        %% Extend (0, 2] to (0, 4] and snapshot again using the freed token.
        ok = arweave_storage_sync_record:add(4, 2, unpacked, Record, StoreID),
        gen_server:cast(PID, store_state),
        _ = sys:get_state(PID),
        ?assertEqual({ok, 0}, wal_count(StateDB)),
        ?assertEqual(
            [{snapshot_token, undefined}],
            ets:lookup(sync_records, snapshot_token)
        ),
        ?assertEqual(Period, receive_scheduled(PID)),
        kill(PID)
    end).

%% @doc A restarted store publishes the data sizes of its snapshot at once
%% rather than after its first periodic snapshot.
data_sizes_published_on_load(_Config) ->
    StoreID = test_sync_record_sizes,
    Record = {ar_data_sync, byte},
    with_disk_store(StoreID, [], fun() ->
        {ok, PID} = arweave_storage_ct_util:start_sync_record(StoreID),
        ok = arweave_storage_sync_record:add(
            3 * ?DATA_CHUNK_SIZE, 0, unpacked, Record, StoreID
        ),
        %% A graceful stop snapshots the record.
        ok = gen_server:stop(PID),
        ets:delete_all_objects(arweave_storage_data_sizes),
        {ok, PID2} = arweave_storage_ct_util:start_sync_record(StoreID),
        ?assertMatch(
            [{_, unpacked, 0, Size}] when Size == 3 * ?DATA_CHUNK_SIZE,
            arweave_storage_sync_record:get_data_sizes()
        ),
        kill(PID2)
    end).

%% @doc A store spanning several partitions publishes the data it holds in
%% each of them, rather than its whole record under its first partition.
data_sizes_split_by_partition(_Config) ->
    StoreID = test_sync_record_split,
    PartitionSize = arweave_lib_constants:partition_size(),
    %% Two and a half partitions, the span of a 9 TB module on mainnet.
    RangeEnd = 5 * PartitionSize div 2,
    Module = {0, RangeEnd, unpacked},
    arweave_storage_ct_util:with_disk_stores(
        [StoreID], {0, RangeEnd}, [], fun() ->
            {ok, PID} = arweave_storage_ct_util:start_sync_record(StoreID),
            %% The whole range, plus one chunk in the overlap past its end.
            ok = arweave_storage_sync_record:add(
                RangeEnd + ?DATA_CHUNK_SIZE,
                0,
                unpacked,
                {ar_data_sync, byte},
                StoreID
            ),
            gen_server:cast(PID, store_state),
            _ = sys:get_state(PID),
            %% Half a partition, and the overlap chunk past the end.
            LastSize = PartitionSize div 2 + ?DATA_CHUNK_SIZE,
            ?assertEqual(
                [
                    {Module, unpacked, 0, PartitionSize},
                    {Module, unpacked, 1, PartitionSize},
                    {Module, unpacked, 2, LastSize}
                ],
                lists:sort([
                    DataSize
                 || {M, _, _, _} = DataSize <-
                        arweave_storage_sync_record:get_data_sizes(),
                    M == Module
                ])
            ),
            kill(PID)
        end
    ).

%% @doc Two neighbouring stores that meet inside a partition each publish their
%% own share of it, and the shares add up to the whole partition.
data_sizes_shared_partition(_Config) ->
    PartitionSize = arweave_lib_constants:partition_size(),
    %% The stores meet halfway through partition 1: the left one spans
    %% partitions 0 and 1, the right one starts mid-partition 1 and spans 2.
    Boundary = 3 * PartitionSize div 2,
    Half = PartitionSize div 2,
    Left = {test_sync_record_left, {0, Boundary}},
    Right = {test_sync_record_right, {Boundary, 3 * PartitionSize}},
    LeftModule = {0, Boundary, unpacked},
    RightModule = {Boundary, 3 * PartitionSize, unpacked},
    arweave_storage_ct_util:with_disk_stores([Left, Right], [], fun() ->
        lists:foreach(
            fun({StoreID, {RangeStart, RangeEnd}}) ->
                {ok, PID} = arweave_storage_ct_util:start_sync_record(StoreID),
                ok = arweave_storage_sync_record:add(
                    RangeEnd,
                    RangeStart,
                    unpacked,
                    {ar_data_sync, byte},
                    StoreID
                ),
                gen_server:cast(PID, store_state),
                _ = sys:get_state(PID)
            end,
            [Left, Right]
        ),
        DataSizes = arweave_storage_sync_record:get_data_sizes(),
        ?assertEqual(
            [
                {LeftModule, unpacked, 0, PartitionSize},
                {LeftModule, unpacked, 1, Half}
            ],
            lists:sort([D || {M, _, _, _} = D <- DataSizes, M == LeftModule])
        ),
        ?assertEqual(
            [
                {RightModule, unpacked, 1, Half},
                {RightModule, unpacked, 2, PartitionSize}
            ],
            lists:sort([D || {M, _, _, _} = D <- DataSizes, M == RightModule])
        ),
        %% Summed per partition, as the mining report and the dashboards do.
        ?assertEqual(
            PartitionSize,
            lists:sum([
                Size
             || {M, _, 1, Size} <- DataSizes,
                M == LeftModule orelse M == RightModule
            ])
        )
    end).

%% @doc The store's own sync record server builds its footprint record, and a
%% step left over from an overlapping chain - scheduled at a cursor the
%% record has since moved past - neither walks nor writes, so it cannot undo
%% the finished record.
footprint_record_built_by_its_server(_Config) ->
    StoreID = test_sync_record_footprint,
    StateDB = {sync_record, StoreID},
    CursorKey = <<"footprint_record_init_cursor">>,
    %% Count the database writes, running them as usual.
    with_disk_store(StoreID, [{ar_kv, []}], fun() ->
        {ok, PID} = arweave_storage_ct_util:start_sync_record(StoreID),
        Chunks = [{N * ?DATA_CHUNK_SIZE, unpacked} || N <- [1, 2, 3]],
        arweave_storage_ct_util:add_chunks_to_sync_record(Chunks, StoreID),
        ?assertNot(arweave_storage:is_footprint_record_initialized(StoreID)),
        ok = arweave_storage:initialize_footprint_record(StoreID),
        ok = ar_test_await:until(footprint_record_built, fun() ->
            arweave_storage:is_footprint_record_initialized(StoreID)
        end),
        ?assertEqual(
            length(Chunks),
            arweave_lib_intervals:sum(
                arweave_storage_sync_record:get(
                    unpacked, {ar_data_sync, footprint}, StoreID
                )
            )
        ),
        Writes = meck:num_calls(ar_kv, put, [StateDB, CursorKey, '_']),
        %% A step of an older chain, still at the first cursor.
        gen_server:cast(
            PID, {continue_footprint_record_initialization, start, false}
        ),
        _ = sys:get_state(PID),
        ?assertEqual(
            Writes,
            meck:num_calls(ar_kv, put, [StateDB, CursorKey, '_'])
        ),
        ?assert(arweave_storage:is_footprint_record_initialized(StoreID)),
        kill(PID)
    end).

footprint_initialization_retries_cursor_write(_Config) ->
    StoreID = test_footprint_cursor_retry,
    StateDB = {sync_record, StoreID},
    CursorKey = <<"footprint_record_init_cursor">>,
    Attempts = atomics:new(1, []),
    Mocks = [{ar_kv, [{put, fun(DB, Key, Value) ->
        case {DB, Key, Value} of
            {StateDB, CursorKey, <<"complete">>} ->
                case atomics:add_get(Attempts, 1, 1) of
                    1 -> {error, enospc};
                    _ -> meck:passthrough([DB, Key, Value])
                end;
            _ -> meck:passthrough([DB, Key, Value])
        end
    end}]}],
    with_disk_store(StoreID, Mocks, fun() ->
        Name = arweave_storage_sync_record:name(StoreID),
        {ok, PID} = start_supervised_sync_record(StoreID),
        try
            Chunks = [{?DATA_CHUNK_SIZE, unpacked}],
            arweave_storage_ct_util:add_chunks_to_sync_record(Chunks, StoreID),
            ok = arweave_storage:initialize_footprint_record(StoreID),
            ok = ar_test_await:until(footprint_cursor_write_retried, fun() ->
                atomics:get(Attempts, 1) >= 2
            end),
            _ = sys:get_state(Name),
            ?assert(arweave_storage:is_footprint_record_initialized(StoreID)),
            %% One failed completion write followed by one successful retry.
            ?assertEqual(2, atomics:get(Attempts, 1)),
            ?assertEqual(PID, whereis(Name)),
            ?assertEqual(length(Chunks), footprint_record_size(StoreID))
        after
            stop_supervised_sync_record(StoreID)
        end
    end).

footprint_initialization_marked_complete(_Config) ->
    StoreID = test_footprint_mark_complete,
    StateDB = {sync_record, StoreID},
    CursorKey = <<"footprint_record_init_cursor">>,
    Parent = self(),
    Mocks = [{ar_timer, [{apply_after,
        fun(Delay, M, F, A, O) ->
            case A of
                [_, {continue_footprint_record_initialization, _, _} = Step] ->
                    Parent ! {initialization_scheduled, Step},
                    {ok, make_ref()};
                _ -> meck:passthrough([Delay, M, F, A, O])
            end
        end}]}, {ar_kv, []}],
    with_disk_store(StoreID, Mocks, fun() ->
        {ok, PID} = start_supervised_sync_record(StoreID),
        try
            ok = arweave_storage:initialize_footprint_record(StoreID),
            Step = receive {initialization_scheduled, S} -> S end,
            ?assertNot(arweave_storage:is_footprint_record_initialized(StoreID)),
            %% Legacy completion supersedes the pending initialization chain.
            ok = arweave_storage:mark_footprint_record_initialized(StoreID),
            ?assert(arweave_storage:is_footprint_record_initialized(StoreID)),
            ?assertEqual({ok, <<"complete">>}, ar_kv:get(StateDB, CursorKey)),
            %% A stale step must write neither intervals nor its cursor.
            Writes = meck:num_calls(ar_kv, put, [StateDB, '_', '_']),
            gen_server:cast(PID, Step),
            _ = sys:get_state(PID),
            ?assertEqual(Writes, meck:num_calls(
                ar_kv, put, [StateDB, '_', '_']
            ))
        after
            stop_supervised_sync_record(StoreID)
        end
    end).

footprint_initialization_resumes_before_first_step(_Config) ->
    StoreID = test_footprint_start_resume,
    Parent = self(),
    Steps = atomics:new(1, []),
    Mocks = [{ar_timer, [{apply_after,
        fun(Delay, M, F, A, O) ->
            case A of
                [_, {continue_footprint_record_initialization, _, _}] ->
                    case atomics:add_get(Steps, 1, 1) of
                        1 ->
                            %% Lose the first step as if its timer fired while
                            %% the worker was down, before any cursor advance.
                            Parent ! first_footprint_step_scheduled,
                            {ok, make_ref()};
                        _ -> meck:passthrough([Delay, M, F, A, O])
                    end;
                _ -> meck:passthrough([Delay, M, F, A, O])
            end
        end}]}],
    with_disk_store(StoreID, Mocks, fun() ->
        Name = arweave_storage_sync_record:name(StoreID),
        {ok, PID} = start_supervised_sync_record(StoreID),
        try
            Chunks = [{?DATA_CHUNK_SIZE, unpacked}],
            arweave_storage_ct_util:add_chunks_to_sync_record(Chunks, StoreID),
            ?assertEqual(0, atomics:get(Steps, 1)),
            ok = arweave_storage:initialize_footprint_record(StoreID),
            receive first_footprint_step_scheduled -> ok end,
            ?assertEqual(
                {ok, <<"start">>},
                ar_kv:get(
                    {sync_record, StoreID}, <<"footprint_record_init_cursor">>
                )
            ),
            kill(PID),
            ok = ar_test_await:until(footprint_migration_resumed, fun() ->
                PID2 = whereis(Name),
                is_pid(PID2) andalso PID2 =/= PID andalso
                    arweave_storage:is_footprint_record_initialized(StoreID)
            end),
            ?assertEqual(length(Chunks), footprint_record_size(StoreID)),
            %% Completed stores must not schedule another migration on restart.
            StepsBeforeRestart = atomics:get(Steps, 1),
            PID2 = whereis(Name),
            kill(PID2),
            ok = ar_test_await:until(completed_footprint_store_loaded, fun() ->
                PID3 = whereis(Name),
                is_pid(PID3) andalso PID3 =/= PID2
            end),
            _ = sys:get_state(Name),
            ?assertEqual(StepsBeforeRestart, atomics:get(Steps, 1)),
            ?assert(arweave_storage:is_footprint_record_initialized(StoreID))
        after
            stop_supervised_sync_record(StoreID)
        end
    end).

footprint_initialization_resumes_from_persisted_cursor(_Config) ->
    StoreID = test_footprint_cursor_resume,
    Parent = self(),
    Mocks = [{arweave_storage_footprint_record, [{continue_initialization,
        fun(ID, Cursor, EstimateReported, WriteIntervals, State) ->
            Parent ! {footprint_initialization, Cursor},
            meck:passthrough([
                ID, Cursor, EstimateReported, WriteIntervals, State
            ])
        end}]}],
    with_disk_store(StoreID, Mocks, fun() ->
        Name = arweave_storage_sync_record:name(StoreID),
        {ok, PID} = start_supervised_sync_record(StoreID),
        try
            %% Resume at footprint 1, using the unsigned cursor persisted by
            %% earlier versions; footprint 0 was already migrated.
            {_, Cursor} = arweave_lib_footprint:get_footprint_range(
                0, 0
            ),
            Offset =
                arweave_lib_footprint:get_padded_offset_from_footprint_offset(
                    Cursor + 1
                ),
            Chunks = [{?DATA_CHUNK_SIZE, unpacked}, {Offset, unpacked}],
            arweave_storage_ct_util:add_chunks_to_sync_record(Chunks, StoreID),
            ok = arweave_storage:add_footprint(
                ?DATA_CHUNK_SIZE, unpacked, StoreID
            ),
            ok = ar_kv:put(
                {sync_record, StoreID}, <<"footprint_record_init_cursor">>,
                binary:encode_unsigned(Cursor)
            ),
            kill(PID),
            InitializationCursor = receive
                {footprint_initialization, C} -> C
            end,
            ?assertEqual(Cursor, InitializationCursor),
            ok = ar_test_await:until(footprint_cursor_resumed, fun() ->
                PID2 = whereis(Name),
                is_pid(PID2) andalso PID2 =/= PID andalso
                    arweave_storage:is_footprint_record_initialized(StoreID)
            end),
            ?assertEqual(length(Chunks), footprint_record_size(StoreID))
        after
            stop_supervised_sync_record(StoreID)
        end
    end).

%%====================================================================
%% Helpers
%%====================================================================

%% Run Fun with StoreID as an on-disk module covering the first partition.
with_disk_store(StoreID, ExtraMocks, Fun) ->
    arweave_storage_ct_util:with_disk_stores(
        [StoreID], {0, arweave_lib_constants:partition_size()}, ExtraMocks, Fun
    ).

start_supervised_sync_record(StoreID) ->
    Child = #{
        id => StoreID,
        start => {arweave_storage_sync_record, start_link,
            [arweave_storage_sync_record:name(StoreID), StoreID]},
        restart => permanent,
        shutdown => 5000,
        type => worker
    },
    supervisor:start_child(arweave_storage_sync_record_sup, Child).

stop_supervised_sync_record(StoreID) ->
    supervisor:terminate_child(arweave_storage_sync_record_sup, StoreID),
    supervisor:delete_child(arweave_storage_sync_record_sup, StoreID).

footprint_record_size(StoreID) ->
    arweave_lib_intervals:sum(arweave_storage_sync_record:get(
        unpacked, {ar_data_sync, footprint}, StoreID
    )).

receive_scheduled(PID) ->
    receive
        {scheduled, PID, Delay} -> Delay
    after 1000 ->
        erlang:error({no_snapshot_scheduled, PID})
    end.

wal_count(StateDB) ->
    case ar_kv:get(StateDB, <<"wal">>) of
        {ok, V} -> {ok, binary:decode_unsigned(V)};
        Other -> Other
    end.

kill(PID) ->
    unlink(PID),
    MonitorRef = monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', MonitorRef, process, PID, _} -> ok
    end.
