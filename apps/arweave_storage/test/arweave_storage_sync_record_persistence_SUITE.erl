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
    data_sizes_published_on_load
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
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:add(
                2, 0, unpacked, Record, StoreID
            ),
            ?assertEqual(
                [{2, 0}],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{2, 0}],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:add(
                4, 3, any_packing, Record, StoreID
            ),
            ?assertEqual(
                [{2, 0}, {4, 3}],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{2, 0}],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:delete(2, 1, Record, StoreID),
            ?assertEqual(
                [{1, 0}, {4, 3}],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{1, 0}],
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(unpacked, Record, StoreID)
                )
            ),
            ok = arweave_storage_sync_record:cut(1, Record, StoreID),
            ?assertEqual(
                [{1, 0}],
                ar_intervals:to_list(
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
                        {0, arweave_constants:partition_size(), unpacked};
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
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [],
                ar_intervals:to_list(
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
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{6, 5}],
                ar_intervals:to_list(
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
                ar_intervals:to_list(
                    arweave_storage_sync_record:get(
                        any_packing, Record, StoreID
                    )
                )
            ),
            ?assertEqual(
                [{6, 5}],
                ar_intervals:to_list(
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
    with_store_mocks([StoreID], [
        {arweave_storage_deps, [
            %% Capture the delays the stores schedule instead of running them.
            {apply_after, fun(Delay, _M, _F, _A, _O) ->
                Parent ! {scheduled, self(), Delay},
                {ok, make_ref()}
            end}
        ]}
    ], fun() ->
        {ok, PID} = start_record(StoreID),
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
        %% The holder dies without releasing; the next attempt takes over.
        exit(Holder, kill),
        ok = ar_test_await:until(holder_dead, fun() ->
            not is_process_alive(Holder)
        end),
        gen_server:cast(PID, store_state),
        _ = sys:get_state(PID),
        %% WAL count 0 because it has been snapshotted
        ?assertEqual({ok, 0}, wal_count(StateDB)),
        %% The snapshot released the token; the dead holder's row is gone with it.
        ?assertEqual([], ets:lookup(sync_records, snapshot_token)),
        ?assertEqual(Period, receive_scheduled(PID)),
        %% Cast again with nothing added since the last snapshot: the empty WAL
        %% means no snapshot is written and the next attempt is a full period out.
        Writes = meck:num_calls(
            arweave_storage_deps, put_database, [StateDB, <<"sync_records">>, '_']
        ),
        gen_server:cast(PID, store_state),
        _ = sys:get_state(PID),
        ?assertEqual(
            Writes,
            meck:num_calls(
                arweave_storage_deps, put_database,
                [StateDB, <<"sync_records">>, '_']
            )
        ),
        ?assertEqual(Period, receive_scheduled(PID)),
        kill(PID)
    end).

%% @doc A restarted store publishes the data sizes of its snapshot at once
%% rather than after its first periodic snapshot.
data_sizes_published_on_load(_Config) ->
    StoreID = test_sync_record_sizes,
    Record = {ar_data_sync, byte},
    with_store_mocks([StoreID], [], fun() ->
        {ok, PID} = start_record(StoreID),
        ok = arweave_storage_sync_record:add(
            3 * ?DATA_CHUNK_SIZE, 0, unpacked, Record, StoreID
        ),
        %% A graceful stop snapshots the record.
        ok = gen_server:stop(PID),
        ets:delete_all_objects(arweave_storage_data_sizes),
        {ok, PID2} = start_record(StoreID),
        ?assertMatch(
            [{_, unpacked, 0, Size}] when Size == 3 * ?DATA_CHUNK_SIZE,
            arweave_storage_sync_record:get_data_sizes()
        ),
        kill(PID2)
    end).

%%====================================================================
%% Helpers
%%====================================================================

%% Run Fun with the given test stores resolving to on-disk modules under the
%% case's data dir, plus any extra mocks.
with_store_mocks(StoreIDs, ExtraMocks, Fun) ->
    arweave_storage_ct_util:with_mocks(
        [
            {arweave_storage_module, [
                {get_by_id, fun(StoreID) ->
                    case lists:member(StoreID, StoreIDs) of
                        true -> {0, arweave_constants:partition_size(), unpacked};
                        false -> meck:passthrough([StoreID])
                    end
                end},
                {info, fun(StoreID) ->
                    case lists:member(StoreID, StoreIDs) of
                        true ->
                            #store_info{
                                id = StoreID,
                                path = filename:join([
                                    arweave_config:get([data_dir]),
                                    "storage_modules",
                                    atom_to_list(StoreID)
                                ])
                            };
                        false ->
                            meck:passthrough([StoreID])
                    end
                end}
            ]}
            | ExtraMocks
        ],
        Fun
    ).

start_record(StoreID) ->
    arweave_storage_sync_record:start_link(
        arweave_storage_sync_record:name(StoreID), StoreID
    ).

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
