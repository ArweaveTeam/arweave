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
