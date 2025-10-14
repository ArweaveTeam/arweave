-module(ar_data_sync_syncs_data_test).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

syncs_data_test_() ->
	{timeout, 240, fun test_syncs_data/0}.

test_syncs_data() ->
	?LOG_DEBUG([{event, test_syncs_data_start}]),
	Wallet = ar_test_data_sync:setup_nodes(),
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
	DiskPoolThreshold = ar_node:get_partition_upper_bound(ar_node:get_block_index()),
	ar_test_data_sync:wait_until_syncs_chunks(peer1, Proofs, DiskPoolThreshold),
	lists:foreach(
		fun({B, #tx{ id = TXID }, Chunks, {_, Proof}}) ->
			TXSize = byte_size(binary:list_to_bin(Chunks)),
			TXOffset = ar_merkle:extract_note(ar_util:decode(maps:get(tx_path, Proof))),
			AbsoluteTXOffset = B#block.weave_size - B#block.block_size + TXOffset,
			ExpectedOffsetInfo = ar_serialize:jsonify(#{
					offset => integer_to_binary(AbsoluteTXOffset),
					size => integer_to_binary(TXSize) }),
			true = ar_util:do_until(
				fun() ->
					case ar_test_data_sync:get_tx_offset(peer1, TXID) of
						{ok, {{<<"200">>, _}, _, ExpectedOffsetInfo, _, _}} ->
							true;
						_ ->
							false
					end
				end,
				100,
				120 * 1000
			),
			ExpectedData = ar_util:encode(binary:list_to_bin(Chunks)),
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
