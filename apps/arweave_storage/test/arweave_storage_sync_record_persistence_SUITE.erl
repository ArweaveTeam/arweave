-module(arweave_storage_sync_record_persistence_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [get_after_add, persistence_roundtrip].

init_per_suite(Config) -> arweave_storage_ct_util:init_suite(Config).

end_per_suite(Config) -> arweave_storage_ct_util:end_suite(Config).

init_per_testcase(Name, Config) ->
    Config2 = arweave_storage_ct_util:init_case(Name, Config),
    {ok, _} = arweave_storage:activate(),
    Config2.

end_per_testcase(Name, Config) ->
    arweave_storage_ct_util:stop_record(test_sync_record_get),
    arweave_storage_ct_util:stop_record(test_sync_record_persist),
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

%%====================================================================
%% Helpers
%%====================================================================

kill(PID) ->
    unlink(PID),
    MonitorRef = monitor(process, PID),
    exit(PID, kill),
    receive
        {'DOWN', MonitorRef, process, PID, _} -> ok
    end.
