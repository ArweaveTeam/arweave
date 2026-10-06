-module(arweave_storage_entropy_storage_SUITE).
-test_category([fast]).
-export([all/0, record_chunk_releases_semaphore_on_exception/1, replica_2_9/1]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
all() -> [record_chunk_releases_semaphore_on_exception, replica_2_9].


-record(state, {
                store_id
               }).


%%%===================================================================
%%% Tests.
%%%===================================================================

%% @doc An exception in the record_chunk critical section must still release the
%% per-file semaphore; otherwise the stale ETS entry wedges all future writers.
record_chunk_releases_semaphore_on_exception_test() ->
    Filepath = "test_entropy_semaphore_file",
    case ets:info(ar_entropy_storage) of
        undefined -> ets:new(ar_entropy_storage, [set, public, named_table]);
        _ -> ok
    end,
    ets:delete(ar_entropy_storage, {semaphore, Filepath}),
    meck:new(ar_block, [passthrough]),
    meck:expect(arweave_lib_constants, get_chunk_padded_offset, fun(X) -> X end),
    meck:new(arweave_storage_chunk_storage, [passthrough]),
    meck:expect(arweave_storage_chunk_storage, get_chunk_bucket_start, fun(_) -> 0 end),
    meck:expect(arweave_storage_chunk_storage, locate_chunk_on_disk, fun(_, _) -> {0, Filepath, 0, 0} end),
    meck:new(arweave_storage_sync_record, [passthrough]),
    meck:expect(arweave_storage_sync_record, is_recorded, fun(_, _, _) -> throw(boom) end),
    try
        Threw = try arweave_storage_entropy_storage:record_chunk(1, <<0>>, "store", #{}, {true, <<"addr">>}), false
                catch throw:boom -> true end,
        ?assert(Threw),
        ?assertEqual([], ets:lookup(ar_entropy_storage, {semaphore, Filepath}))
    after
        meck:unload([ar_block, ar_chunk_storage, ar_sync_record])
    end.

record_chunk_releases_semaphore_on_exception(_Config) ->
    record_chunk_releases_semaphore_on_exception_test().



replica_2_9_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_replica_2_9/0}.

replica_2_9(_Config) ->
    replica_2_9_test_().



test_replica_2_9() ->
    case arweave_lib_constants:strict_data_split_threshold() of
        786432 ->
            ok;
        _ ->
            throw(unexpected_strict_data_split_threshold)
    end,

    RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
    Packing = {replica_2_9, RewardAddr},
    StorageModules = [
                      {0, arweave_lib_constants:partition_size(), Packing},
                      {arweave_lib_constants:partition_size(), 2 * arweave_lib_constants:partition_size(), Packing}
                     ],
    arweave_config:with_test_config(fun() ->
                                            ar_test_node:start(#{
                                                                 reward_addr => RewardAddr,
                                                                 [storage_modules] => StorageModules
                                                                }),
                                            StoreID1 = arweave_storage_module:id(lists:nth(1, StorageModules)),
                                            StoreID2 = arweave_storage_module:id(lists:nth(2, StorageModules)),
                                            C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
                                            %% ar_chunk_storage does not allow overwriting a chunk
                                            %% with an unpacked_padded chunk.
                                            ?assertEqual({error, already_stored},
                                                         arweave_storage_chunk_storage:put(?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
                                            ?assertEqual({error, already_stored},
                                                         arweave_storage_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
                                            ?assertEqual({error, already_stored},
                                                         arweave_storage_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
                                            arweave_storage_entropy_storage:assert_get(C1, ?DATA_CHUNK_SIZE, StoreID1),
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
                                            arweave_storage_entropy_storage:assert_get(C1, 2 * ?DATA_CHUNK_SIZE, StoreID1),
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1)),
                                            arweave_storage_entropy_storage:assert_get(C1, 3 * ?DATA_CHUNK_SIZE, StoreID1),

                                            %% Store the new unpacked_padded chunk. Expect it to be enciphered with
                                            %% the its entropy.
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(4 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
                                            {ok, P1, _Entropy} =
                                                ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 4 * ?DATA_CHUNK_SIZE, C1),
                                            arweave_storage_entropy_storage:assert_get(P1, 4 * ?DATA_CHUNK_SIZE, StoreID1),

                                            arweave_storage_entropy_storage:assert_get(not_found, 8 * ?DATA_CHUNK_SIZE, StoreID1),
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(8 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1)),
                                            {ok, P2, _} =
                                                ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 8 * ?DATA_CHUNK_SIZE, C1),
                                            arweave_storage_entropy_storage:assert_get(P2, 8 * ?DATA_CHUNK_SIZE, StoreID1),

                                            %% Store chunks in the second partition.
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(12 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2)),
                                            {ok, P3, Entropy3} =
                                                ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 12 * ?DATA_CHUNK_SIZE, C1),

                                            arweave_storage_entropy_storage:assert_get(P3, 12 * ?DATA_CHUNK_SIZE, StoreID2),
                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(15 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2)),
                                            {ok, P4, Entropy4} =
                                                ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 15 * ?DATA_CHUNK_SIZE, C1),
                                            arweave_storage_entropy_storage:assert_get(P4, 15 * ?DATA_CHUNK_SIZE, StoreID2),
                                            ?assertNotEqual(P3, P4),
                                            ?assertNotEqual(Entropy3, Entropy4),

                                            ?assertEqual({ok, Packing},
                                                         arweave_storage_chunk_storage:put(16 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2)),
                                            {ok, P5, Entropy5} =
                                                ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 16 * ?DATA_CHUNK_SIZE, C1),
                                            arweave_storage_entropy_storage:assert_get(P5, 16 * ?DATA_CHUNK_SIZE, StoreID2),
                                            ?assertNotEqual(Entropy4, Entropy5)
                                    end).