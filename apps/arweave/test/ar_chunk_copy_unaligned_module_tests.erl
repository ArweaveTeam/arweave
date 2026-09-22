-module(ar_chunk_copy_unaligned_module_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

-include("ar.hrl").

%%% Cross-module copy into a module whose range starts and ends
%%% mid-partition and spans several partitions, as a custom-sized module
%%% does. Test geometry: partitions of 2,000,000 bytes; the three genesis
%%% chunks end at 262144, 524288 and 786432, all inside partition 0.

copy_into_module_spanning_partitions_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        [{arweave_entropy, generate_slice, fun generate_slice_spy/3}],
        fun test_copy_into_module_spanning_partitions/0, ?TEST_NODE_TIMEOUT).

%% A replica.2.9 module starting halfway into partition 0 and ending
%% halfway into partition 2, added next to the three full partition
%% modules that already hold the data, receives every chunk of its range
%% from them through the local copy, including the chunks in partitions 1
%% and 2 that lie past its start partition. The sources are packed for the
%% same address, so the copy stores their chunks as they are: it never
%% unpacks one, which would cost an entropy slice per sub-chunk.
test_copy_into_module_spanning_partitions() ->
    Addr = ar_test_node:generate_address(main),
    P = arweave_constants:partition_size(),
    Packing = {replica_2_9, Addr},
    Sources = [{0, P, Packing}, {P, 2 * P, Packing}, {2 * P, 3 * P, Packing}],
    Wallet = ar_test_data_sync:setup_main_node(#{
        addr => Addr,
        [storage_modules] => Sources
    }),
    %% Sixteen chunks after the three genesis ones end the weave at
    %% 4,980,736, halfway into partition 2, so every partition the target
    %% touches holds data.
    Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
        || _ <- lists:seq(1, 16)],
    #{tx := TX} = ar_test_data_sync:make_fixed_data_tx(Wallet, Chunks),
    B = ar_test_node:post_and_mine(#{miner => main, await_on => main}, [TX]),
    Proofs = ar_test_data_sync:build_proofs(B, TX, Chunks),
    %% 303 means the disk pool kept the chunk as temporary: it is at the
    %% weave tip, above the estimated long-term threshold, until buried.
    lists:foreach(
        fun({_EndOffset, Proof}) ->
            ?assertMatch({ok, {{Status, _}, _, _, _, _}}
                    when Status =:= <<"200">>; Status =:= <<"303">>,
                ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof)))
        end,
        Proofs),
    %% Bury the data below the disk pool threshold so the modules take it.
    lists:foreach(
        fun(_) ->
            ar_test_node:post_and_mine(#{miner => main, await_on => main}, [])
        end,
        lists:seq(1, ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 1)),
    Offsets = [EndOffset || {EndOffset, _} <- Proofs],
    lists:foreach(
        fun(EndOffset) ->
            ok = ar_test_await:chunk_recorded(main, EndOffset,
                #{packing => Packing})
        end,
        Offsets),
    %% Once every source holds the chunks the disk pool lets them go. Wait
    %% for that, or the pool would hand the new module unpacked copies at
    %% the restart and the packed sources would never be read, which is the
    %% case a node with old data is in.
    ok = ar_test_await:disk_pool_chunk_count(fun(Count) -> Count == 0 end),
    Target = {P div 2, 5 * P div 2, Packing},
    #store_info{id = TargetID} = arweave_storage:store_info(Target),
    EntropyCallsBeforeCopy =
        meck:num_calls(arweave_entropy, generate_slice, 3),
    ok = arweave_config:internal_force_config(#{
        [storage_modules] => Sources ++ [Target]
    }),
    ar_test_node:restart(),
    Inside = [EndOffset || EndOffset <- Offsets,
        EndOffset - ?DATA_CHUNK_SIZE >= P div 2, EndOffset =< 5 * P div 2],
    ?assert(length(Inside) >= 12),
    lists:foreach(
        fun(EndOffset) ->
            ?assertEqual(ok, ar_test_await:chunk_recorded(main, EndOffset,
                #{store_id => TargetID, packing => Packing}))
        end,
        Inside),
    ?assertEqual(EntropyCallsBeforeCopy,
        meck:num_calls(arweave_entropy, generate_slice, 3)).

%% Count sub-chunk entropy generation without changing its behaviour.
generate_slice_spy(RewardAddr, Offset, SubChunkStart) ->
    meck:passthrough([RewardAddr, Offset, SubChunkStart]).
