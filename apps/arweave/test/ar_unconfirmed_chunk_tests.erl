-module(ar_unconfirmed_chunk_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

get_unconfirmed_chunk_from_disk_pool_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_from_disk_pool/0}.

get_unconfirmed_chunk_tx_index_fallback_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_tx_index_fallback/0}.

get_unconfirmed_chunk_not_found_test_() ->
	{timeout, 60, fun test_get_unconfirmed_chunk_not_found/0}.

get_unconfirmed_chunk_invalid_input_test_() ->
	{timeout, 60, fun test_get_unconfirmed_chunk_invalid_input/0}.

get_unconfirmed_chunk_not_stored_long_term_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_not_stored_long_term/0}.

get_unconfirmed_chunk_multi_chunk_tx_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_multi_chunk_tx/0}.

get_unconfirmed_chunk_offset_boundary_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_offset_boundary/0}.

get_unconfirmed_chunk_sub_chunk_size_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_sub_chunk_size/0}.

get_unconfirmed_chunk_same_data_different_txs_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_same_data_different_txs/0}.

get_unconfirmed_chunk_negative_offset_test_() ->
	{timeout, 60, fun test_get_unconfirmed_chunk_negative_offset/0}.

get_unconfirmed_chunk_offset_beyond_data_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_offset_beyond_data/0}.

get_unconfirmed_chunk_partial_confirmation_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_partial_confirmation/0}.

get_unconfirmed_chunk_data_path_valid_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_data_path_valid/0}.

get_unconfirmed_chunk_concurrent_requests_test_() ->
	{timeout, 120, fun test_get_unconfirmed_chunk_concurrent_requests/0}.

discover_all_unconfirmed_chunks_test_() ->
	{timeout, 120, fun test_discover_all_unconfirmed_chunks/0}.

%% @doc Chunk is in the disk pool (not yet mined) and served via the ETS cache path.
test_get_unconfirmed_chunk_from_disk_pool() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	?assertEqual(ar_util:encode(hd(Chunks)), maps:get(<<"chunk">>, Response)),
	?assert(byte_size(maps:get(<<"data_path">>, Response)) > 0),
	?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response)),
	?assertEqual(true, maps:get(<<"is_stored_long_term">>, Response)).

%% @doc Chunk was in the disk pool but has been confirmed; served via tx_index fallback.
test_get_unconfirmed_chunk_tx_index_fallback() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
			ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, storage_modules => StorageModules }),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_util:do_until(
			fun() ->
				case ar_test_node:get_unconfirmed_chunk(main, EncodedTXID,
						ChunkEndOffset) of
					{ok, {{<<"200">>, _}, _, _, _, _}} = Result ->
						Result;
					_ ->
						false
				end
			end,
			1000,
			30_000
		),
	Response = jiffy:decode(Body, [return_maps]),
	?assertEqual(ar_util:encode(hd(Chunks)), maps:get(<<"chunk">>, Response)),
	?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response)),
	?assertEqual(true, maps:get(<<"is_stored_long_term">>, Response)).

%% @doc Unknown TXID returns 404.
test_get_unconfirmed_chunk_not_found() ->
	ar_test_data_sync:setup_nodes(),
	RandomTXID = ar_util:encode(crypto:strong_rand_bytes(32)),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, RandomTXID, ?DATA_CHUNK_SIZE)
	).

%% @doc Invalid TXID encoding returns 400; invalid offset returns 400.
test_get_unconfirmed_chunk_invalid_input() ->
	ar_test_data_sync:setup_nodes(),
	ValidTXID = ar_util:encode(crypto:strong_rand_bytes(32)),
	Peer = ar_test_node:peer_ip(main),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/unconfirmed_chunk/not_valid_base64!/" ++
					integer_to_list(?DATA_CHUNK_SIZE)
		})
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/unconfirmed_chunk/" ++ binary_to_list(ValidTXID) ++ "/0"
		})
	),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/unconfirmed_chunk/" ++ binary_to_list(ValidTXID) ++ "/abc"
		})
	).

%% @doc When no storage module covers the vicinity, is_stored_long_term is false.
test_get_unconfirmed_chunk_not_stored_long_term() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 5,
			ar_test_node:get_default_storage_module_packing(Addr, 5)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, storage_modules => StorageModules }),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"303">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	?assertEqual(ar_util:encode(hd(Chunks)), maps:get(<<"chunk">>, Response)),
	?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response)),
	?assertEqual(false, maps:get(<<"is_stored_long_term">>, Response)).

%% @doc Multiple chunks from the same TX can each be retrieved individually.
test_get_unconfirmed_chunk_multi_chunk_tx() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 3)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	DataSize = 3 * ?DATA_CHUNK_SIZE,
	lists:foreach(
		fun({Chunk, N}) ->
			ChunkEndOffset = N * ?DATA_CHUNK_SIZE,
			DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset - 1, DataTree),
			Proof = #{
				data_root => ar_util:encode(DataRoot),
				data_path => ar_util:encode(DataPath),
				chunk => ar_util:encode(Chunk),
				offset => integer_to_binary(ChunkEndOffset - 1),
				data_size => integer_to_binary(DataSize)
			},
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		lists:zip(Chunks, lists:seq(1, 3))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	lists:foreach(
		fun({Chunk, N}) ->
			ChunkEndOffset = N * ?DATA_CHUNK_SIZE,
			{ok, {{<<"200">>, _}, _, Body, _, _}} =
				ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, ChunkEndOffset),
			Response = jiffy:decode(Body, [return_maps]),
			?assertEqual(ar_util:encode(Chunk), maps:get(<<"chunk">>, Response)),
			?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response)),
			?assertEqual(true, maps:get(<<"is_stored_long_term">>, Response))
		end,
		lists:zip(Chunks, lists:seq(1, 3))
	).

%% @doc Querying with an offset that is not the exact chunk end offset returns 404.
test_get_unconfirmed_chunk_offset_boundary() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 3)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	DataSize = 3 * ?DATA_CHUNK_SIZE,
	lists:foreach(
		fun({Chunk, N}) ->
			ChunkEndOffset = N * ?DATA_CHUNK_SIZE,
			DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset - 1, DataTree),
			Proof = #{
				data_root => ar_util:encode(DataRoot),
				data_path => ar_util:encode(DataPath),
				chunk => ar_util:encode(Chunk),
				offset => integer_to_binary(ChunkEndOffset - 1),
				data_size => integer_to_binary(DataSize)
			},
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		lists:zip(Chunks, lists:seq(1, 3))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	%% The exact end offset works.
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, 2 * ?DATA_CHUNK_SIZE)
	),
	%% An offset one byte below the end offset does not match the ETS cache key.
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, 2 * ?DATA_CHUNK_SIZE - 1)
	),
	%% An offset one byte above the end offset does not match either.
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, 2 * ?DATA_CHUNK_SIZE + 1)
	).

%% @doc A TX whose last chunk is smaller than DATA_CHUNK_SIZE can be retrieved.
test_get_unconfirmed_chunk_sub_chunk_size() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	SmallChunkSize = 1000,
	Chunk1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Chunk2 = crypto:strong_rand_bytes(SmallChunkSize),
	Chunks = [Chunk1, Chunk2],
	DataSize = ?DATA_CHUNK_SIZE + SmallChunkSize,
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	%% Post first chunk.
	Chunk1EndOffset = ?DATA_CHUNK_SIZE,
	DataPath1 = ar_merkle:generate_path(DataRoot, Chunk1EndOffset - 1, DataTree),
	Proof1 = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath1),
		chunk => ar_util:encode(Chunk1),
		offset => integer_to_binary(Chunk1EndOffset - 1),
		data_size => integer_to_binary(DataSize)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof1))
	),
	%% Post second (small) chunk.
	Chunk2EndOffset = DataSize,
	DataPath2 = ar_merkle:generate_path(DataRoot, Chunk2EndOffset - 1, DataTree),
	Proof2 = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath2),
		chunk => ar_util:encode(Chunk2),
		offset => integer_to_binary(Chunk2EndOffset - 1),
		data_size => integer_to_binary(DataSize)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof2))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	%% Retrieve the full-size first chunk.
	{ok, {{<<"200">>, _}, _, Body1, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, Chunk1EndOffset),
	Response1 = jiffy:decode(Body1, [return_maps]),
	?assertEqual(ar_util:encode(Chunk1), maps:get(<<"chunk">>, Response1)),
	%% Retrieve the sub-chunk-size second chunk.
	{ok, {{<<"200">>, _}, _, Body2, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, Chunk2EndOffset),
	Response2 = jiffy:decode(Body2, [return_maps]),
	?assertEqual(ar_util:encode(Chunk2), maps:get(<<"chunk">>, Response2)),
	?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response2)).

%% @doc Two TXs with identical data (same DataRoot) are independently retrievable.
test_get_unconfirmed_chunk_same_data_different_txs() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX1, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	{TX2, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	?assertNotEqual(TX1#tx.id, TX2#tx.id),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	%% Both TXIDs should resolve to the same chunk data.
	EncodedTXID1 = ar_util:encode(TX1#tx.id),
	EncodedTXID2 = ar_util:encode(TX2#tx.id),
	{ok, {{<<"200">>, _}, _, Body1, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID1, ChunkEndOffset),
	{ok, {{<<"200">>, _}, _, Body2, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID2, ChunkEndOffset),
	Response1 = jiffy:decode(Body1, [return_maps]),
	Response2 = jiffy:decode(Body2, [return_maps]),
	?assertEqual(
		maps:get(<<"chunk">>, Response1),
		maps:get(<<"chunk">>, Response2)
	),
	?assertEqual(ar_util:encode(hd(Chunks)), maps:get(<<"chunk">>, Response1)).

%% @doc Negative offset returns 400.
test_get_unconfirmed_chunk_negative_offset() ->
	ar_test_data_sync:setup_nodes(),
	ValidTXID = ar_util:encode(crypto:strong_rand_bytes(32)),
	Peer = ar_test_node:peer_ip(main),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/unconfirmed_chunk/" ++ binary_to_list(ValidTXID) ++ "/-1"
		})
	).

%% @doc Offset far beyond the TX data size returns 404.
test_get_unconfirmed_chunk_offset_beyond_data() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, ?DATA_CHUNK_SIZE * 10)
	).

%% @doc Chunk is still retrievable after mining only 1 block (partial confirmation).
test_get_unconfirmed_chunk_partial_confirmation() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	%% Mine one block — chunk is partially confirmed but not yet pruned from disk pool.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_util:do_until(
			fun() ->
				case ar_test_node:get_unconfirmed_chunk(main, EncodedTXID,
						ChunkEndOffset) of
					{ok, {{<<"200">>, _}, _, _, _, _}} = Result ->
						Result;
					_ ->
						false
				end
			end,
			1000,
			30_000
		),
	Response = jiffy:decode(Body, [return_maps]),
	?assertEqual(ar_util:encode(hd(Chunks)), maps:get(<<"chunk">>, Response)),
	?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response)).

%% @doc The data_path returned by the endpoint is a valid merkle proof.
test_get_unconfirmed_chunk_data_path_valid() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	{ok, ReturnedDataPath} = ar_util:safe_decode(maps:get(<<"data_path">>, Response)),
	{ok, ReturnedChunk} = ar_util:safe_decode(maps:get(<<"chunk">>, Response)),
	%% Validate the returned data_path is a valid merkle proof.
	?assertMatch(
		{_, _, _},
		ar_merkle:validate_path(DataRoot, ChunkEndOffset - 1, ?DATA_CHUNK_SIZE,
				ReturnedDataPath)
	),
	%% Verify the chunk ID matches what the merkle proof references.
	{ChunkID, _StartOffset, _EndOffset} =
		ar_merkle:validate_path(DataRoot, ChunkEndOffset - 1, ?DATA_CHUNK_SIZE,
				ReturnedDataPath),
	?assertEqual(ar_tx:generate_chunk_id(ReturnedChunk), ChunkID).

%% @doc Multiple concurrent requests for the same chunk all succeed with identical data.
test_get_unconfirmed_chunk_concurrent_requests() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	{TX, Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX),
	ChunkEndOffset = ?DATA_CHUNK_SIZE,
	DataPath = ar_merkle:generate_path(DataRoot, ChunkEndOffset, DataTree),
	Proof = #{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(DataPath),
		chunk => ar_util:encode(hd(Chunks)),
		offset => integer_to_binary(ChunkEndOffset),
		data_size => integer_to_binary(?DATA_CHUNK_SIZE)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
	),
	EncodedTXID = ar_util:encode(TX#tx.id),
	Self = self(),
	NumRequests = 5,
	lists:foreach(
		fun(_) ->
			spawn(fun() ->
				Result = ar_test_node:get_unconfirmed_chunk(main, EncodedTXID,
						ChunkEndOffset),
				Self ! {chunk_result, Result}
			end)
		end,
		lists:seq(1, NumRequests)
	),
	Results = lists:map(
		fun(_) ->
			receive
				{chunk_result, Result} -> Result
			after 30_000 ->
				error(timeout)
			end
		end,
		lists:seq(1, NumRequests)
	),
	%% All requests should succeed.
	lists:foreach(
		fun(Result) ->
			?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, Result)
		end,
		Results
	),
	%% All responses should return the same chunk data.
	Bodies = lists:map(
		fun({ok, {{<<"200">>, _}, _, Body, _, _}}) -> Body end,
		Results
	),
	[FirstBody | Rest] = Bodies,
	lists:foreach(
		fun(Body) ->
			?assertEqual(FirstBody, Body)
		end,
		Rest
	).

%% @doc A client with zero prior knowledge can discover and retrieve all unconfirmed
%% chunks across multiple transactions using only public API endpoints:
%%   GET /tx/pending -> GET /unconfirmed_tx/{txid} -> GET /unconfirmed_chunk/{txid}/{offset}
test_discover_all_unconfirmed_chunks() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Peer = ar_test_node:peer_ip(main),

	%% --- TX1: 2 full-size chunks ---
	TX1Chunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 2)],
	TX1DataSize = 2 * ?DATA_CHUNK_SIZE,
	{TX1DataRoot, TX1DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(TX1Chunks)
		)
	),
	{TX1, TX1Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, TX1DataRoot, TX1Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	%% POST chunk proofs for TX1.
	lists:foreach(
		fun({Chunk, N}) ->
			ChunkEndOffset = N * ?DATA_CHUNK_SIZE,
			DataPath = ar_merkle:generate_path(TX1DataRoot, ChunkEndOffset - 1, TX1DataTree),
			Proof = #{
				data_root => ar_util:encode(TX1DataRoot),
				data_path => ar_util:encode(DataPath),
				chunk => ar_util:encode(Chunk),
				offset => integer_to_binary(ChunkEndOffset - 1),
				data_size => integer_to_binary(TX1DataSize)
			},
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		lists:zip(TX1Chunks, lists:seq(1, 2))
	),

	%% --- TX2: 1 full chunk + 1 sub-chunk-size chunk ---
	SmallChunkSize = 1000,
	TX2Chunk1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	TX2Chunk2 = crypto:strong_rand_bytes(SmallChunkSize),
	TX2Chunks = [TX2Chunk1, TX2Chunk2],
	TX2DataSize = ?DATA_CHUNK_SIZE + SmallChunkSize,
	{TX2DataRoot, TX2DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(TX2Chunks)
		)
	),
	{TX2, TX2Chunks} = ar_test_data_sync:tx(Wallet, {fixed_data, TX2DataRoot, TX2Chunks}),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	%% POST chunk proofs for TX2.
	TX2ChunkEndOffset1 = ?DATA_CHUNK_SIZE,
	TX2DataPath1 = ar_merkle:generate_path(TX2DataRoot, TX2ChunkEndOffset1 - 1, TX2DataTree),
	TX2Proof1 = #{
		data_root => ar_util:encode(TX2DataRoot),
		data_path => ar_util:encode(TX2DataPath1),
		chunk => ar_util:encode(TX2Chunk1),
		offset => integer_to_binary(TX2ChunkEndOffset1 - 1),
		data_size => integer_to_binary(TX2DataSize)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(TX2Proof1))
	),
	TX2ChunkEndOffset2 = TX2DataSize,
	TX2DataPath2 = ar_merkle:generate_path(TX2DataRoot, TX2ChunkEndOffset2 - 1, TX2DataTree),
	TX2Proof2 = #{
		data_root => ar_util:encode(TX2DataRoot),
		data_path => ar_util:encode(TX2DataPath2),
		chunk => ar_util:encode(TX2Chunk2),
		offset => integer_to_binary(TX2ChunkEndOffset2 - 1),
		data_size => integer_to_binary(TX2DataSize)
	},
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_test_node:post_chunk(main, ar_serialize:jsonify(TX2Proof2))
	),

	%% ============================================================
	%% DISCOVERY PHASE: simulate a client that knows nothing
	%% ============================================================

	%% Step 1: GET /tx/pending -> discover TXIDs in the mempool.
	{ok, {{<<"200">>, _}, _, PendingBody, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/pending"
		}),
	PendingTXIDs = jiffy:decode(PendingBody),
	EncodedTX1ID = ar_util:encode(TX1#tx.id),
	EncodedTX2ID = ar_util:encode(TX2#tx.id),
	?assert(lists:member(EncodedTX1ID, PendingTXIDs)),
	?assert(lists:member(EncodedTX2ID, PendingTXIDs)),

	%% Step 2 & 3: For each discovered TXID, fetch the TX to learn data_size,
	%% then compute chunk offsets and retrieve each chunk.
	lists:foreach(
		fun(DiscoveredTXID) ->
			%% Step 2: GET /unconfirmed_tx/{txid} -> learn data_size and data_root.
			{ok, {{<<"200">>, _}, _, TXBody, _, _}} =
				ar_http:req(#{
					method => get,
					peer => Peer,
					path => "/unconfirmed_tx/" ++ binary_to_list(DiscoveredTXID)
				}),
			TXJson = jiffy:decode(TXBody, [return_maps]),
			DiscoveredDataSize = binary_to_integer(maps:get(<<"data_size">>, TXJson)),
			{ok, DiscoveredDataRoot} = ar_util:safe_decode(
				maps:get(<<"data_root">>, TXJson)
			),

			%% Compute chunk end offsets from data_size alone.
			ChunkEndOffsets = compute_chunk_end_offsets(DiscoveredDataSize),
			?assert(length(ChunkEndOffsets) > 0),

			%% Step 3: GET /unconfirmed_chunk/{txid}/{offset} for each computed offset.
			RetrievedSize = lists:foldl(
				fun(EndOffset, AccSize) ->
					{ok, {{<<"200">>, _}, _, ChunkBody, _, _}} =
						ar_test_node:get_unconfirmed_chunk(main, DiscoveredTXID, EndOffset),
					ChunkResponse = jiffy:decode(ChunkBody, [return_maps]),
					{ok, ReturnedChunk} = ar_util:safe_decode(
						maps:get(<<"chunk">>, ChunkResponse)
					),
					{ok, ReturnedDataPath} = ar_util:safe_decode(
						maps:get(<<"data_path">>, ChunkResponse)
					),
					?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, ChunkResponse)),
					%% Validate the merkle proof.
					?assertMatch(
						{_, _, _},
						ar_merkle:validate_path(DiscoveredDataRoot, EndOffset - 1,
							DiscoveredDataSize, ReturnedDataPath)
					),
					%% Verify chunk ID matches the merkle proof.
					{ChunkID, _, _} = ar_merkle:validate_path(
						DiscoveredDataRoot, EndOffset - 1,
						DiscoveredDataSize, ReturnedDataPath
					),
					?assertEqual(ar_tx:generate_chunk_id(ReturnedChunk), ChunkID),
					AccSize + byte_size(ReturnedChunk)
				end,
				0,
				ChunkEndOffsets
			),
			%% The total retrieved data equals the advertised data_size.
			?assertEqual(DiscoveredDataSize, RetrievedSize)
		end,
		[EncodedTX1ID, EncodedTX2ID]
	).

%% @doc Given a total data size, compute the chunk end offsets a client would use.
%% Chunks are DATA_CHUNK_SIZE bytes each; the last chunk gets the remainder.
compute_chunk_end_offsets(DataSize) ->
	compute_chunk_end_offsets(DataSize, ?DATA_CHUNK_SIZE, []).

compute_chunk_end_offsets(DataSize, _ChunkSize, Acc) when DataSize =< 0 ->
	lists:reverse(Acc);
compute_chunk_end_offsets(DataSize, ChunkSize, Acc) ->
	NextOffset = case Acc of
		[] -> min(ChunkSize, DataSize);
		[Prev | _] -> min(Prev + ChunkSize, DataSize)
	end,
	case NextOffset of
		DataSize ->
			lists:reverse([DataSize | Acc]);
		_ ->
			compute_chunk_end_offsets(DataSize, ChunkSize, [NextOffset | Acc])
	end.
