-module(ar_data_sync_chunk_cache_leak_test).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

-include("ar.hrl").

%%% Failed unpacking must release shared admission, allowing a valid chunk
%%% offered afterwards to sync even with only one lifecycle reservation.
%%% Corrupt packed bytes preserve peer metadata but fail padding validation.

chunk_cache_leak_on_unpack_error_test_() ->
    {timeout, 600, fun() ->
        Parent = self(),
        ar_test_util:new_mock(ar_peers, [passthrough]),
        ar_test_util:mock_function(
            ar_peers,
            issue_warning,
            fun
                (Peer, chunk, Error) ->
                    Parent ! {unpack_failed, Error},
                    meck:passthrough([Peer, chunk, Error]);
                (Peer, DataType, Error) ->
                    meck:passthrough([Peer, DataType, Error])
            end
        ),
        try
            test_chunk_cache_leak_on_unpack_error()
        after
            ar_test_util:unmock_module(ar_peers)
        end
    end}.

test_chunk_cache_leak_on_unpack_error() ->
    Addr = ar_test_node:generate_address(main),
    PeerAddr = ar_test_node:generate_address(peer1),
    %% Main stores unpacked, so everything fetched from peer1 (packed) goes
    %% through the unpack request path. With the limit of 1 a single leaking
    %% retry stalls the node.
    Wallet = ar_test_data_sync:setup_nodes(#{
        addr => Addr,
        peer_addr => PeerAddr,
        config => #{
            [storage_modules] =>
                [{0, 10 * arweave_constants:partition_size(), unpacked}]
        },
        peer_config => ar_test_node:storage_module_config(PeerAddr, [0])
    }),
    %% One MiB admits one 768 KiB lifecycle reservation.
    ok = arweave_config:set([packing, cache_size], 1),
    %% Fill the weave up to the strict data split threshold so both target
    %% chunks land bucket-padded in peer1's ar_chunk_storage.
    StrictThreshold = arweave_constants:strict_data_split_threshold(),
    ?assertEqual(0, StrictThreshold rem ?DATA_CHUNK_SIZE),
    FillerChunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
            || _ <- lists:seq(1, StrictThreshold div ?DATA_CHUNK_SIZE)],
    mine_block_with_chunks(Wallet, FillerChunks),
    %% Smaller than ?DATA_CHUNK_SIZE: the packed form is zero-padded, so
    %% garbage packed bytes fail unpacking with invalid_padding.
    {CorruptedB, CorruptedTX, CorruptedChunks} =
        mine_block_with_chunks(Wallet, [crypto:strong_rand_bytes(100 * 1024)]),
    {ValidB, ValidTX, ValidChunks} =
        mine_block_with_chunks(Wallet, [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]),
    [{CorruptedEndOffset, CorruptedProof}] =
        ar_test_data_sync:build_proofs(CorruptedB, CorruptedTX, CorruptedChunks),
    [{ValidEndOffset, ValidProof}] =
        ar_test_data_sync:build_proofs(ValidB, ValidTX, ValidChunks),
    %% Push the disk pool threshold past both chunks so peer1 stores posted
    %% proofs packed and main network-syncs the range.
    mine_block_with_chunks(Wallet, [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)]),
    mine_until_below_disk_pool_threshold(ValidEndOffset, 10),
    %% Keep peer1's data out of main's reach while we set the scene.
    ar_test_node:disconnect_from(peer1),
    PeerPacking = ar_test_node:storage_module_packing(PeerAddr, 0),
    post_chunk_to_peer1(CorruptedProof),
    ok = ar_test_await:chunk_recorded(peer1, CorruptedEndOffset,
            #{ packing => PeerPacking }),
    corrupt_stored_chunk(CorruptedEndOffset),
    ar_test_node:connect_to_peer(peer1),
    %% Prove the failure path ran before introducing the valid chunk.
    receive
        {unpack_failed, invalid_padding} -> ok
    after 60_000 -> ?assert(false, "Corrupt chunk never failed unpacking")
    end,
    %% Make the valid chunk available on peer1 and require main to sync it:
    %% the unpack failures must not exhaust the chunk cache budget.
    post_chunk_to_peer1(ValidProof),
    ok = ar_test_await:chunk_recorded(
        peer1,
        ValidEndOffset,
        #{packing => PeerPacking}
    ),
    ok = ar_test_await:until(
        valid_chunk_synced,
        fun() ->
            arweave_storage:is_recorded(
                ValidEndOffset,
                any_packing,
                {ar_data_sync, byte},
                any_store
            ) =/= false
        end,
        60_000
    ),
    %% Stop new fetches before asserting zero outstanding reservations.
    ar_test_node:disconnect_from(peer1),
    ok = ar_test_await:until(
        chunk_cache_drained,
        fun() -> ar_chunk_cache:reserved_size() =:= 0 end
    ),
    {ok, CacheRef} = ar_chunk_cache:reserve(test_store),
    ar_chunk_cache:release(CacheRef).

%% @doc Mine one block on peer1 with a single fixed-data v2 tx carrying Chunks.
mine_block_with_chunks(Wallet, Chunks) ->
    {DataRoot, _DataTree} = ar_merkle:generate_tree(
        ar_tx:sized_chunks_to_sized_chunk_ids(
            ar_tx:chunks_to_size_tagged_chunks(Chunks))),
    {TX, _} = ar_test_data_sync:tx(#{
        wallet => Wallet,
        split_type => {fixed_data, DataRoot, Chunks},
        format => v2,
        reward => fetch,
        tx_anchor_peer => peer1,
        get_fee_peer => peer1 }),
    B = ar_test_node:post_and_mine(#{ miner => peer1, await_on => peer1 }, [TX]),
    ?assertMatch({ok, _}, ar_test_await:node_height(main, B#block.height)),
    {B, TX, Chunks}.

%% @doc Mine empty blocks on peer1 until the disk pool threshold (the weave
%% size a few blocks back) covers Offset on both nodes.
mine_until_below_disk_pool_threshold(_Offset, 0) ->
    ?assert(false, "The disk pool threshold did not reach the target offset.");
mine_until_below_disk_pool_threshold(Offset, RetryCount) ->
    MainThreshold = ar_test_node:remote_call(main, ar_disk_pool, get_threshold, []),
    PeerThreshold = ar_test_node:remote_call(peer1, ar_disk_pool, get_threshold, []),
    case MainThreshold >= Offset andalso PeerThreshold >= Offset of
        true ->
            ok;
        false ->
            Height = ar_test_node:remote_call(peer1, ar_node, get_height, []),
            ar_test_node:mine(peer1),
            ?assertMatch({ok, _}, ar_test_await:node_height(peer1, Height + 1)),
            ?assertMatch({ok, _}, ar_test_await:node_height(main, Height + 1)),
            mine_until_below_disk_pool_threshold(Offset, RetryCount - 1)
    end.

post_chunk_to_peer1(Proof) ->
    ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
        ar_test_node:post_chunk(peer1, ar_serialize:jsonify(Proof))).

%% @doc Overwrite the packed chunk bytes in peer1's chunk storage. Metadata
%% and sync records stay intact, so peer1 keeps serving the chunk.
corrupt_stored_chunk(EndOffset) ->
    PaddedEndOffset = arweave_constants:get_chunk_padded_offset(EndOffset),
    [StorageModule | _] = ar_test_node:remote_call(
        peer1,
        arweave_storage,
        covering_stores,
        [PaddedEndOffset, any_packing]
    ),
    #store_info{id = StoreID} = arweave_storage:store_info(StorageModule),
    Garbage = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    ?assertMatch(
        {ok, _},
        ar_test_node:remote_call(
            peer1,
            arweave_storage,
            internal_write_chunk,
            [PaddedEndOffset, Garbage, StoreID]
        )
    ).
