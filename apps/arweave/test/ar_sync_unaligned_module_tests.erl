-module(ar_sync_unaligned_module_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

-include("ar.hrl").

%%% Network sync into replica.2.9 modules that split a partition between
%%% them. Test geometry: partitions of 2,000,000 bytes; the
%%% three genesis chunks end at 262144, 524288 and 786432, inside
%%% partition 0, below every module here.

split_partition_sync_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        [{arweave_entropy, generate_chunk, fun generate_chunk_spy/2}],
        fun test_split_partition_sync/0, 480).

%% Two modules of the same packing split partition 2 between them at its
%% midpoint: the first ends there, the second starts there. Both sync
%% every chunk of their range from the peer, and neither falls back to
%% generating entropy chunk by chunk, which the prepared module should
%% never need.
test_split_partition_sync() ->
    Addr = ar_test_node:generate_address(main),
    PeerAddr = ar_test_node:generate_address(peer1),
    P = arweave_constants:partition_size(),
    Packing = {replica_2_9, Addr},
    Aligned = {P, 5 * P div 2, Packing},
    Unaligned = {5 * P div 2, 4 * P, Packing},
    %% The main node mines from partition 0, which holds the genesis data
    %% and nothing the two modules under test cover, so their chunks can
    %% only arrive over the network from the peer.
    Wallet = ar_test_data_sync:setup_nodes(#{
        addr => Addr,
        peer_addr => PeerAddr,
        config => #{[storage_modules] => [{0, P, Packing}, Aligned, Unaligned]},
        %% The peer holds the whole weave, with a spare partition past its
        %% end, so the modules under test sit well inside the peer's data.
        peer_config => #{[storage_modules] => [{0, 6 * P, unpacked}]}
    }),
    #store_info{id = AlignedID} = arweave_storage:store_info(Aligned),
    #store_info{id = UnalignedID} = arweave_storage:store_info(Unaligned),
    %% Thirty-seven chunks after the three genesis ones end the weave at
    %% 10,485,760, more than a partition past the second module's end at
    %% 8,000,000, so both modules lie inside the weave with data on every
    %% side and the weave tip plays no part.
    {TX, Chunks} = post_chunks(Wallet, 37, 1),
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
        lists:seq(2, ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 2)),
    ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs, infinity),
    Offsets = [EndOffset || {EndOffset, _} <- Proofs],
    InAligned = [Offset || Offset <- Offsets,
        Offset - ?DATA_CHUNK_SIZE >= P, Offset =< 5 * P div 2],
    InUnaligned = [Offset || Offset <- Offsets,
        Offset - ?DATA_CHUNK_SIZE >= 5 * P div 2, Offset =< 4 * P],
    ?assert(length(InAligned) >= 5),
    ?assert(length(InUnaligned) >= 5),
    lists:foreach(
        fun(Offset) ->
            ?assertEqual(ok, ar_test_await:chunk_recorded(main, Offset,
                #{store_id => AlignedID, packing => Packing}))
        end,
        InAligned),
    lists:foreach(
        fun(Offset) ->
            ?assertEqual(ok, ar_test_await:chunk_recorded(main, Offset,
                #{store_id => UnalignedID, packing => Packing}))
        end,
        InUnaligned),
    ?assertEqual(0, meck:num_calls(arweave_entropy, generate_chunk, '_')).

%%%===================================================================
%%% Helpers.
%%%===================================================================

%% Count on-demand entropy generation without changing its behaviour.
generate_chunk_spy(Offset, RewardAddr) ->
    meck:passthrough([Offset, RewardAddr]).

%% Post a transaction of Count random chunks to the main node and mine it
%% into the block at Height.
post_chunks(Wallet, Count, Height) ->
    Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
        || _ <- lists:seq(1, Count)],
    #{tx := TX} = ar_test_data_sync:make_fixed_data_tx(Wallet, Chunks),
    ar_test_node:assert_post_tx_to_peer(main, TX),
    mine(Height),
    {TX, Chunks}.

%% Mine a block and wait for both nodes to reach Height.
mine(Height) ->
    ar_test_node:mine(),
    {ok, _} = ar_test_await:node_height(main, Height),
    {ok, _} = ar_test_await:node_height(peer1, Height).
