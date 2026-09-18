%%% Node-level integration of prepared entropy, packing and chunk storage.
-module(ar_entropy_storage_tests).
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

replica_2_9_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_replica_2_9/0}.

%% @doc Prepared entropy enciphers new chunks and preserves existing packed data.
test_replica_2_9() ->
    case arweave_constants:strict_data_split_threshold() of
        786432 ->
            ok;
        _ ->
            throw(unexpected_strict_data_split_threshold)
    end,

    RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
    Packing = {replica_2_9, RewardAddr},
    StorageModules = [
        {0, arweave_constants:partition_size(), Packing},
        {
            arweave_constants:partition_size(),
            2 * arweave_constants:partition_size(),
            Packing
        }
    ],
    arweave_config:internal_with_test_config(fun() ->
        ar_test_node:start(#{
            reward_addr => RewardAddr,
            [storage_modules] => StorageModules
        }),
        #store_info{id = StoreID1} = arweave_storage:store_info(
            lists:nth(1, StorageModules)
        ),
        #store_info{id = StoreID2} = arweave_storage:store_info(
            lists:nth(2, StorageModules)
        ),
        C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
        %% ar_chunk_storage does not allow overwriting a chunk
        %% with an unpacked_padded chunk.
        ?assertEqual(
            {error, already_stored},
            arweave_storage:put_chunk(
                ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1
            )
        ),
        ?assertEqual(
            {error, already_stored},
            arweave_storage:put_chunk(
                2 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1
            )
        ),
        ?assertEqual(
            {error, already_stored},
            arweave_storage:put_chunk(
                3 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1
            )
        ),
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(?DATA_CHUNK_SIZE, C1, Packing, StoreID1)
        ),
        ?assertEqual(
            expected_samples(C1, ?DATA_CHUNK_SIZE),
            chunk_samples(?DATA_CHUNK_SIZE, StoreID1)
        ),
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                2 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1
            )
        ),
        ?assertEqual(
            expected_samples(C1, 2 * ?DATA_CHUNK_SIZE),
            chunk_samples(2 * ?DATA_CHUNK_SIZE, StoreID1)
        ),
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                3 * ?DATA_CHUNK_SIZE, C1, Packing, StoreID1
            )
        ),
        ?assertEqual(
            expected_samples(C1, 3 * ?DATA_CHUNK_SIZE),
            chunk_samples(3 * ?DATA_CHUNK_SIZE, StoreID1)
        ),

        %% Store the new unpacked_padded chunk. Expect it to be enciphered with
        %% the its entropy.
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                4 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1
            )
        ),
        {ok, P1, _Entropy} =
            ar_packing_server:pack_replica_2_9_chunk(
                RewardAddr, 4 * ?DATA_CHUNK_SIZE, C1
            ),
        ?assertEqual(
            expected_samples(P1, 4 * ?DATA_CHUNK_SIZE),
            chunk_samples(4 * ?DATA_CHUNK_SIZE, StoreID1)
        ),

        ?assertEqual(
            expected_samples(not_found, 8 * ?DATA_CHUNK_SIZE),
            chunk_samples(8 * ?DATA_CHUNK_SIZE, StoreID1)
        ),
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                8 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID1
            )
        ),
        {ok, P2, _} =
            ar_packing_server:pack_replica_2_9_chunk(
                RewardAddr, 8 * ?DATA_CHUNK_SIZE, C1
            ),
        ?assertEqual(
            expected_samples(P2, 8 * ?DATA_CHUNK_SIZE),
            chunk_samples(8 * ?DATA_CHUNK_SIZE, StoreID1)
        ),

        %% Store chunks in the second partition.
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                12 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2
            )
        ),
        {ok, P3, Entropy3} =
            ar_packing_server:pack_replica_2_9_chunk(
                RewardAddr, 12 * ?DATA_CHUNK_SIZE, C1
            ),

        ?assertEqual(
            expected_samples(P3, 12 * ?DATA_CHUNK_SIZE),
            chunk_samples(12 * ?DATA_CHUNK_SIZE, StoreID2)
        ),
        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                15 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2
            )
        ),
        {ok, P4, Entropy4} =
            ar_packing_server:pack_replica_2_9_chunk(
                RewardAddr, 15 * ?DATA_CHUNK_SIZE, C1
            ),
        ?assertEqual(
            expected_samples(P4, 15 * ?DATA_CHUNK_SIZE),
            chunk_samples(15 * ?DATA_CHUNK_SIZE, StoreID2)
        ),
        ?assertNotEqual(P3, P4),
        ?assertNotEqual(Entropy3, Entropy4),

        ?assertEqual(
            {ok, Packing},
            arweave_storage:put_chunk(
                16 * ?DATA_CHUNK_SIZE, C1, unpacked_padded, StoreID2
            )
        ),
        {ok, P5, Entropy5} =
            ar_packing_server:pack_replica_2_9_chunk(
                RewardAddr, 16 * ?DATA_CHUNK_SIZE, C1
            ),
        ?assertEqual(
            expected_samples(P5, 16 * ?DATA_CHUNK_SIZE),
            chunk_samples(16 * ?DATA_CHUNK_SIZE, StoreID2)
        ),
        ?assertNotEqual(Entropy4, Entropy5)
    end).

expected_samples(not_found, _) ->
    lists:duplicate(9, not_found);
expected_samples(Chunk, Offset) ->
    lists:duplicate(9, {Offset, Chunk}).

chunk_samples(Offset, StoreID) ->
    %% Sample both edges and interior bytes of the same chunk.
    [
        arweave_storage:get_chunk(Byte, StoreID)
     || Byte <- [
            Offset - 1,
            Offset - 2,
            Offset - ?DATA_CHUNK_SIZE,
            Offset - ?DATA_CHUNK_SIZE + 1,
            Offset - ?DATA_CHUNK_SIZE + 2,
            Offset - ?DATA_CHUNK_SIZE div 2,
            Offset - ?DATA_CHUNK_SIZE div 2 + 1,
            Offset - ?DATA_CHUNK_SIZE div 2 - 1,
            Offset - ?DATA_CHUNK_SIZE div 3
        ]
    ].
