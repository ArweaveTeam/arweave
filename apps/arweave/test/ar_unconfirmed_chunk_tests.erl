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
