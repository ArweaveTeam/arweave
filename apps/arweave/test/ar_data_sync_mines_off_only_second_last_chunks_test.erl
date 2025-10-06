-module(ar_data_sync_mines_off_only_second_last_chunks_test).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_config.hrl").

-import(ar_test_node, [test_with_mocked_functions/2]).

mines_off_only_second_last_chunks_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}, mock_reset_frequency()],
			fun test_mines_off_only_second_last_chunks/0).

mock_reset_frequency() ->
	{ar_nonce_limiter, get_reset_frequency, fun() -> 5 end}.

test_mines_off_only_second_last_chunks() ->
	?LOG_DEBUG([{event, test_mines_off_only_second_last_chunks_start}]),
	Wallet = ar_test_data_sync:setup_nodes(),
	%% Submit only the second last chunks (smaller than 256 KiB) of transactions.
	%% Assert the nodes construct correct proofs of access from them.
	lists:foreach(
		fun(Height) ->
			RandomID = crypto:strong_rand_bytes(32),
			Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE div 2),
			ChunkID = ar_tx:generate_chunk_id(Chunk),
			DataSize = (?DATA_CHUNK_SIZE) div 2 + (?DATA_CHUNK_SIZE) div 2 + 3,
			{DataRoot, DataTree} = ar_merkle:generate_tree([{ChunkID, ?DATA_CHUNK_SIZE div 2},
					{RandomID, DataSize}]),
			TX = ar_test_node:sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(main), data_size => DataSize,
					data_root => DataRoot }),
			ar_test_node:post_and_mine(#{ miner => main, await_on => peer1 }, [TX]),
			Offset = 0,
			DataPath = ar_merkle:generate_path(DataRoot, Offset, DataTree),
			Proof = #{ data_root => ar_util:encode(DataRoot),
					data_path => ar_util:encode(DataPath), chunk => ar_util:encode(Chunk),
					offset => integer_to_binary(Offset),
					data_size => integer_to_binary(DataSize) },
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}},
					ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))),
			case Height - ?SEARCH_SPACE_UPPER_BOUND_DEPTH >= 0 of
				true ->
					%% Wait until the new chunks fall below the new upper bound and
					%% remove the original big chunks. The protocol will increase the upper
					%% bound based on the nonce limiter entropy reset, but ar_data_sync waits
					%% for ?SEARCH_SPACE_UPPER_BOUND_DEPTH confirmations before packing the
					%% chunks.
					{ok, Config} = arweave_config:get_env(),
					lists:foreach(
						fun(O) ->
							[ar_chunk_storage:delete(O, ar_storage_module:id(Module))
									|| Module <- Config#config.storage_modules]
						end,
						lists:seq(?DATA_CHUNK_SIZE, ar_block:strict_data_split_threshold(),
								?DATA_CHUNK_SIZE)
					);
				_ ->
					ok
			end
		end,
		lists:seq(1, 6)
	). 