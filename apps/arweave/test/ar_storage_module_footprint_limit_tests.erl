-module(ar_storage_module_footprint_limit_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

%%% Tests for `storage_modules[].footprint_limit`. Test geometry: 512 KiB
%%% sectors holding two chunks, four sectors per partition, partitions of
%%% 2,000,000 bytes; a footprint_limit of 1 keeps the first chunk of every
%%% sector.

%% ar_device_lock:set_device_lock_metric/3 encodes `complete' as 2.
-define(DEVICE_LOCK_COMPLETE, 2).

entropy_preparation_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_entropy_preparation/0}.

repack_in_place_test_() ->
    {timeout, ?TEST_NODE_TIMEOUT, fun test_repack_in_place/0}.

sync_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        [{ar_fork, height_2_9_6, fun() -> infinity end}],
        fun test_sync/0, 480).

%% Entropy is prepared for the first footprint of every sector and for
%% nothing else, and the preparation completes.
test_entropy_preparation() ->
    Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
    P = ar_block:partition_size(),
    start_main(Addr, [
        {0, P, unpacked},
        #{partition => 1, packing_format => replica_2_9,
            packing_address => Addr, footprint_limit => 1}
    ]),
    StoreID = ar_storage_module:id({P, 2 * P, {replica_2_9, Addr}}),
    %% Partition 1 starts at 2,000,000, so its buckets sit 97,152 bytes into
    %% each sector: the first bucket of sector N ends at 2359296 + N * 524288
    %% and the second at 2621440 + N * 524288.
    assert_entropy(StoreID, [2359296, 2883584, 3407872, 3932160],
        [2621440, 3145728, 3670016]),
    ok = ar_test_await:until(prepare_complete, fun() ->
        device_lock_status(StoreID, prepare) == ?DEVICE_LOCK_COMPLETE
    end).

%% Repacking an unpacked partition in place to replica.2.9 with a limit
%% packs the first-footprint chunks, writes entropy only for them, and
%% reports completion.
test_repack_in_place() ->
    Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
    P = ar_block:partition_size(),
    start_main(Addr, [{0, P, unpacked}]),
    StoreID = ar_storage_module:id({0, P, unpacked}),
    %% The three genesis chunks end at 262144 (sector 0, first),
    %% 524288 (sector 0, second) and 786432 (sector 1, first).
    ok = ar_test_await:chunk_recorded(main, 524288, #{store_id => StoreID}),
    %% force_config lifts the runtime lock; the store survives the restart.
    ok = arweave_config:force_config(#{
        [storage_modules] => [],
        [repack_modules] => [
            #{partition => 0, from_format => unpacked,
                to_format => replica_2_9, to_address => Addr,
                footprint_limit => 1}
        ]
    }),
    ar_test_node:restart(),
    %% The device lock hands the module on as soon as the repack completes,
    %% so wait for the repack's output rather than for its status.
    Packed = #{store_id => StoreID, packing => {replica_2_9, Addr}},
    assert_chunks([262144, 786432], [], Packed),
    assert_entropy(StoreID, [262144, 786432, 1310720, 1835008],
        [524288, 1048576, 1572864]),
    assert_chunks([524288], [], #{store_id => StoreID, packing => unpacked}).

%% A limited unpacked module syncs only the first-footprint chunks from a
%% peer that holds everything, and refuses the others.
test_sync() ->
    Addr = ar_test_node:generate_address(main),
    PeerAddr = ar_test_node:generate_address(peer1),
    P = ar_block:partition_size(),
    %% The main node mines (its packed module) and syncs the data the peer
    %% serves into its limited module. The module is partition 1, past the
    %% genesis data, so the node start does not wait for genesis chunks the
    %% limit excludes.
    Limited = #{partition => 1, packing_format => unpacked,
        footprint_limit => 1},
    Wallet = ar_test_data_sync:setup_nodes(#{
        addr => Addr,
        peer_addr => PeerAddr,
        config => #{[storage_modules] =>
            [Limited | ar_test_node:wide_storage_modules(Addr, [0])]},
        peer_config => #{[storage_modules] => [{0, 3 * P, unpacked}]}
    }),
    StoreID = ar_storage_module:id({P, 2 * P, unpacked}),
    %% Five filler chunks move the weave from 786432 past the start of
    %% partition 1 (2,000,000) to 2097152.
    post_chunks(Wallet, 5, 1),
    %% Six chunks in partition 1, whose buckets sit 97,152 bytes into each
    %% 512 KiB sector: ends 2359296 (sector 0, first), 2621440 (sector 0,
    %% second), 2883584 (sector 1, first), 3145728 (sector 1, second),
    %% 3407872 (sector 2, first) and 3670016 (sector 2, second).
    {TX, Chunks} = post_chunks(Wallet, 6, 2),
    B = ar_node:get_current_block(),
    %% The peer must have indexed the block's data roots before it accepts
    %% the proofs for good rather than into its disk pool.
    ok = ar_test_await:data_roots_available(peer1, B),
    Proofs = ar_test_data_sync:build_proofs(B, TX, Chunks),
    %% 303 means the disk pool kept the chunk as temporary: it is at the
    %% weave tip, above the estimated long-term threshold, until buried.
    lists:foreach(
        fun({_EndOffset, Proof}) ->
            ?assertMatch({ok, {{Status, _}, _, _, _, _}}
                    when Status =:= <<"200">>; Status =:= <<"303">>,
                ar_test_node:post_chunk(peer1, ar_serialize:jsonify(Proof)))
        end,
        Proofs),
    %% Bury the data below the disk pool threshold so it becomes syncable.
    lists:foreach(fun mine/1,
        lists:seq(3, ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 3)),
    Offsets = [EndOffset || {EndOffset, _} <- Proofs],
    ?assertEqual([2359296, 2621440, 2883584, 3145728, 3407872, 3670016],
        lists:sort(Offsets)),
    ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs, infinity),
    assert_chunks([2359296, 2883584, 3407872], [2621440, 3145728, 3670016],
        #{store_id => StoreID}).

%% Start the main node on a fresh genesis paying Addr, with the modules.
start_main(Addr, StorageModules) ->
    [B0] = ar_weave:init([{Addr, ?AR(1000), <<>>}]),
    ar_test_node:start(#{
        b0 => B0,
        addr => Addr,
        [storage_modules] => StorageModules
    }).

%% Post a transaction of Count random chunks to the main node and mine it
%% into the block at Height.
post_chunks(Wallet, Count, Height) ->
    Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
        || _ <- lists:seq(1, Count)],
    #{tx := TX} = ar_test_data_sync:make_fixed_data_tx(Wallet, Chunks),
    ar_test_node:assert_post_tx_to_peer(main, TX),
    mine(Height),
    {TX, Chunks}.

%% Mine a block and wait for the main node to reach Height.
mine(Height) ->
    ar_test_node:mine(),
    {ok, _} = ar_test_await:node_height(main, Height).

%% Entropy is prepared for the buckets ending at Prepared and for none of
%% those ending at NotPrepared.
assert_entropy(StoreID, Prepared, NotPrepared) ->
    assert_each(fun(End) ->
        ar_test_await:entropy_prepared(main, StoreID,
            End - ?DATA_CHUNK_SIZE, End)
    end, Prepared),
    assert_each(fun(End) ->
        ar_test_await:entropy_not_prepared(main, StoreID,
            End - ?DATA_CHUNK_SIZE, End)
    end, NotPrepared).

%% The chunks ending at Recorded are in the store Opts describes and those
%% ending at NotRecorded are not.
assert_chunks(Recorded, NotRecorded, Opts) ->
    assert_each(fun(Offset) ->
        ar_test_await:chunk_recorded(main, Offset, Opts)
    end, Recorded),
    assert_each(fun(Offset) ->
        ar_test_await:chunk_not_recorded(main, Offset, Opts)
    end, NotRecorded).

%% Every await returns ok.
assert_each(Await, Items) ->
    lists:foreach(fun(Item) -> ?assertEqual(ok, Await(Item)) end, Items).

%% The device lock status gauge of the main node for the given store and
%% mode (prepare | repack | sync).
device_lock_status(StoreID, Mode) ->
    Label = ar_test_node:remote_call(main, ar_storage_module, label, [StoreID]),
    ar_test_node:remote_call(main, prometheus_gauge, value,
        [device_lock_status, [Label, Mode]]).
