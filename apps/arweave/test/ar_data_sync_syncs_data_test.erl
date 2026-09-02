-module(ar_data_sync_syncs_data_test).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

syncs_data_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
            [{ar_fork, height_2_9_6, fun() -> infinity end}],
            fun test_syncs_data/0, 480).

test_syncs_data() ->
    ?LOG_DEBUG([{event, test_syncs_data_start}]),
    Addr = ar_test_node:generate_address(main),
    PeerAddr = ar_test_node:generate_address(peer1),
    Wallet = ar_test_data_sync:setup_nodes(#{
        addr => Addr,
        peer_addr => PeerAddr,
        config => ar_test_node:storage_module_config(Addr, lists:seq(0, 8)),
        peer_config => ar_test_node:storage_module_config(PeerAddr, lists:seq(0, 8))
    }),
    Records = ar_test_data_sync:post_random_blocks(Wallet),
    RecordsWithProofs = lists:flatmap(
            fun({B, TX, Chunks}) -> 
                ar_test_data_sync:get_records_with_proofs(B, TX, Chunks) end, Records),
    lists:foreach(
        fun({_, _, _, {_, Proof}}) ->
            ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
                    ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))),
            ?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
                    ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof)))
        end,
        RecordsWithProofs
    ),
    Proofs = [Proof || {_, _, _, Proof} <- RecordsWithProofs],
    ar_test_data_sync:wait_until_syncs_chunks(Proofs),
    {Height, BI} = ar_node:get_block_index_and_height(),
    DiskPoolThreshold = ar_node:get_partition_upper_bound(Height, BI),
    ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs, DiskPoolThreshold),
    lists:foreach(
        fun({B, #tx{ id = TXID }, Chunks, {_, Proof}}) ->
            TXSize = byte_size(binary:list_to_bin(Chunks)),
            TXOffset = ar_merkle:extract_note(arweave_util:decode(maps:get(tx_path, Proof))),
            AbsoluteTXOffset = B#block.weave_size - B#block.block_size + TXOffset,
            ExpectedOffsetInfo = ar_serialize:jsonify(#{
                    offset => integer_to_binary(AbsoluteTXOffset),
                    size => integer_to_binary(TXSize) }),
            ok = ar_test_await:until(http_tx_offset_matches,
                fun() ->
                    case ar_test_data_sync:get_tx_offset(peer1, TXID) of
                        {ok, {{<<"200">>, _}, _, ExpectedOffsetInfo, _, _}} ->
                            true;
                        _ ->
                            false
                    end
                end
            ),
            ExpectedData = arweave_util:encode(binary:list_to_bin(Chunks)),
            ar_test_node:assert_get_tx_data(main, TXID, ExpectedData),
            case AbsoluteTXOffset > DiskPoolThreshold of
                true ->
                    ok;
                false ->
                    ar_test_node:assert_get_tx_data(peer1, TXID, ExpectedData)
            end
        end,
        RecordsWithProofs
    ). 
