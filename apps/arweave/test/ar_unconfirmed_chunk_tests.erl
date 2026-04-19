-module(ar_unconfirmed_chunk_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include("ar.hrl").
-include("ar_consensus.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

-define(TIMEOUT, 180).

from_disk_pool_test_() ->
	{timeout, ?TIMEOUT, fun test_from_disk_pool/0}.

tx_index_fallback_test_() ->
	{timeout, ?TIMEOUT, fun test_tx_index_fallback/0}.

not_found_test_() ->
	{timeout, ?TIMEOUT, fun test_not_found/0}.

invalid_input_test_() ->
	{timeout, ?TIMEOUT, fun test_invalid_input/0}.

not_stored_long_term_test_() ->
	{timeout, ?TIMEOUT, fun test_not_stored_long_term/0}.

multi_chunk_tx_test_() ->
	{timeout, ?TIMEOUT, fun test_multi_chunk_tx/0}.

offset_boundary_test_() ->
	{timeout, ?TIMEOUT, fun test_offset_boundary/0}.

sub_chunk_size_test_() ->
	{timeout, ?TIMEOUT, fun test_sub_chunk_size/0}.

same_data_different_txs_test_() ->
	{timeout, ?TIMEOUT, fun test_same_data_different_txs/0}.

same_data_second_tx_after_seed_test_() ->
	{timeout, ?TIMEOUT, fun test_same_data_second_tx_after_seed/0}.

same_data_after_first_tx_confirmed_test_() ->
	{timeout, ?TIMEOUT, fun test_same_data_after_first_tx_confirmed/0}.

same_data_after_disk_pool_cleared_test_() ->
	{timeout, ?TIMEOUT, fun test_same_data_after_disk_pool_cleared/0}.

negative_offset_test_() ->
	{timeout, ?TIMEOUT, fun test_negative_offset/0}.

offset_beyond_data_test_() ->
	{timeout, ?TIMEOUT, fun test_offset_beyond_data/0}.

offset_beyond_tx_size_test_() ->
	{timeout, ?TIMEOUT, fun test_offset_beyond_tx_size/0}.

partial_confirmation_test_() ->
	{timeout, ?TIMEOUT, fun test_partial_confirmation/0}.

data_path_valid_test_() ->
	{timeout, ?TIMEOUT, fun test_data_path_valid/0}.

concurrent_requests_test_() ->
	{timeout, ?TIMEOUT, fun test_concurrent_requests/0}.

discover_all_unconfirmed_chunks_test_() ->
	{timeout, ?TIMEOUT, fun test_discover_all_unconfirmed_chunks/0}.

post_chunk_proofs(Proofs, ExpectedStatus) ->
	lists:foreach(
		fun({_, Proof}) ->
			?assertMatch(
				{ok, {{ExpectedStatus, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		Proofs
	).

post_and_seed_tx(Wallet, InputChunks) ->
	post_and_seed_tx(Wallet, InputChunks, #{}).

post_and_seed_tx(Wallet, InputChunks, Options) ->
	TXOpts = maps:get(tx_opts, Options, #{}),
	ProofOffset = maps:get(proof_offset, Options, inclusive_end),
	ExpectedStatus = maps:get(expected_status, Options, <<"200">>),
	TXMap = case map_size(TXOpts) of
		0 ->
			ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks);
		_ ->
			ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks, TXOpts)
	end,
	#{ tx := TX, data_root := DataRoot, data_tree := DataTree, chunks := Chunks } = TXMap,
	ar_test_node:assert_post_tx_to_peer(main, TX),
	Proofs = ar_test_data_sync:build_proofs(
		DataRoot, DataTree, Chunks, #{ proof_offset => ProofOffset }),
	post_chunk_proofs(Proofs, ExpectedStatus),
	TXMap#{ proofs => Proofs }.

wait_for_unconfirmed_chunk(EncodedTXID, Offset) ->
	wait_for_unconfirmed_chunk(EncodedTXID, Offset, <<"200">>).

wait_for_unconfirmed_chunk(EncodedTXID, Offset, ExpectedStatus) ->
	ar_util:do_until(
		fun() ->
			case ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, Offset) of
				{ok, {{ExpectedStatus, _}, _, _, _, _}} = Result ->
					Result;
				_ ->
					false
			end
		end,
		1000,
		30_000
	),
	Response = ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, Offset),
	?assertMatch({ok, {{ExpectedStatus, _}, _, _, _, _}}, Response),
	Response.

assert_unconfirmed_chunk_response(Response, Proof, IsStoredLongTerm) ->
	?assertEqual(maps:get(chunk, Proof), maps:get(<<"chunk">>, Response)),
	?assertEqual(maps:get(data_path, Proof), maps:get(<<"data_path">>, Response)),
	?assertEqual(<<"unpacked">>, maps:get(<<"packing">>, Response)),
	?assertEqual(IsStoredLongTerm, maps:get(<<"is_stored_long_term">>, Response)).

post_single_chunk_tx(Wallet) ->
	post_single_chunk_tx(Wallet, <<"200">>).

post_single_chunk_tx(Wallet, ExpectedStatus) ->
	#{ tx := TX, data_root := DataRoot, chunks := Chunks,
			proofs := [{ChunkEndOffset, Proof}] } =
		post_and_seed_tx(
			Wallet,
			[crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
			#{
				expected_status => ExpectedStatus,
				proof_offset => end_offset
			}
		),
	#{
		tx => TX,
		data_root => DataRoot,
		chunks => Chunks,
		chunk_end_offset => ChunkEndOffset,
		proof => Proof
	}.

%% @doc Chunk is in the disk pool (not yet mined) and served via the ETS cache path.
test_from_disk_pool() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	#{ tx := TX, chunk_end_offset := ChunkEndOffset, proof := Proof } =
		post_single_chunk_tx(Wallet),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	assert_unconfirmed_chunk_response(Response, Proof, true).

%% @doc Chunk was in the disk pool but has been confirmed; served via tx_index fallback.
test_tx_index_fallback() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
			ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, storage_modules => StorageModules }),
	#{ tx := TX, chunks := Chunks, chunk_end_offset := ChunkEndOffset, proof := Proof } =
		post_single_chunk_tx(Wallet),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	{ok, {TXOffset, _}} = ar_data_sync:get_tx_offset(TX#tx.id),
	AbsoluteEndOffset = TXOffset,
	ar_test_data_sync:wait_until_syncs_chunk(AbsoluteEndOffset, #{
		chunk => maps:get(chunk, Proof),
		data_path => maps:get(data_path, Proof)
	}),
	{ok, {{<<"200">>, _}, _, ChunkBody, _, _}} =
		ar_test_node:get_chunk(main, AbsoluteEndOffset),
	ChunkResponse = jiffy:decode(ChunkBody, [return_maps]),
	?assertEqual(ar_util:encode(hd(Chunks)), maps:get(<<"chunk">>, ChunkResponse)),
	EncodedTXID = ar_util:encode(TX#tx.id),
	%% Force the request down the tx_index path instead of the disk pool ETS cache path.
	ets:delete(ar_disk_pool_chunks_cache, {TX#tx.id, ChunkEndOffset}),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	assert_unconfirmed_chunk_response(Response, Proof, true).

%% @doc Unknown TXID returns 404.
test_not_found() ->
	ar_test_data_sync:setup_nodes(),
	RandomTXID = ar_util:encode(crypto:strong_rand_bytes(32)),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, RandomTXID, ?DATA_CHUNK_SIZE)
	).

%% @doc Invalid TXID encoding returns 400; invalid offset returns 400.
test_invalid_input() ->
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
test_not_stored_long_term() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 5,
			ar_test_node:get_default_storage_module_packing(Addr, 5)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, storage_modules => StorageModules }),
	#{ tx := TX, chunk_end_offset := ChunkEndOffset, proof := Proof } =
		post_single_chunk_tx(Wallet, <<"303">>),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	assert_unconfirmed_chunk_response(Response, Proof, false).

%% @doc Multiple chunks from the same TX can each be retrieved individually.
test_multi_chunk_tx() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	InputChunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 3)],
	#{ tx := TX, proofs := Proofs } = post_and_seed_tx(Wallet, InputChunks),
	EncodedTXID = ar_util:encode(TX#tx.id),
	lists:foreach(
		fun({ChunkEndOffset, Proof}) ->
			{ok, {{<<"200">>, _}, _, Body, _, _}} = wait_for_unconfirmed_chunk(
				EncodedTXID, ChunkEndOffset),
			Response = jiffy:decode(Body, [return_maps]),
			assert_unconfirmed_chunk_response(Response, Proof, true)
		end,
		Proofs
	).

%% @doc Querying with an offset that is not the exact chunk end offset returns 404.
test_offset_boundary() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	InputChunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 3)],
	#{ tx := TX } = post_and_seed_tx(Wallet, InputChunks),
	EncodedTXID = ar_util:encode(TX#tx.id),
	%% The exact end offset works.
	{ok, {{<<"200">>, _}, _, _, _, _}} =
		wait_for_unconfirmed_chunk(EncodedTXID, 2 * ?DATA_CHUNK_SIZE),
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
test_sub_chunk_size() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	SmallChunkSize = 1000,
	Chunk1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Chunk2 = crypto:strong_rand_bytes(SmallChunkSize),
	InputChunks = [Chunk1, Chunk2],
	#{ tx := TX, proofs := Proofs } = post_and_seed_tx(Wallet, InputChunks),
	[{Chunk1EndOffset, Proof1}, {Chunk2EndOffset, Proof2}] = Proofs,
	EncodedTXID = ar_util:encode(TX#tx.id),
	%% Retrieve the full-size first chunk.
	{ok, {{<<"200">>, _}, _, Body1, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, Chunk1EndOffset),
	Response1 = jiffy:decode(Body1, [return_maps]),
	assert_unconfirmed_chunk_response(Response1, Proof1, true),
	%% Retrieve the sub-chunk-size second chunk.
	{ok, {{<<"200">>, _}, _, Body2, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, Chunk2EndOffset),
	Response2 = jiffy:decode(Body2, [return_maps]),
	assert_unconfirmed_chunk_response(Response2, Proof2, true).

%% @doc Two TXs with identical data (same DataRoot) are independently retrievable.
test_same_data_different_txs() ->
	test_same_data_txs(both_before_seed).

%% @doc A later TXID for already-seeded data should also resolve pre-confirmation.
%% Existing duplicate-data coverage posts both TXs before the first seed, so it never
%% exercises the path where a second TXID is added after the chunk is already in disk pool.
test_same_data_second_tx_after_seed() ->
	test_same_data_txs(second_after_seed).

%% @doc A pending duplicate TX should still resolve when the shared data root is already
%% on-chain via an earlier confirmed TX.
test_same_data_after_first_tx_confirmed() ->
	test_same_data_txs(second_after_first_confirmed).

%% @doc Regression test: when a chunk's data root state has been fully cleared from the disk
%% pool (chunk processed, TXID dropped), a new TX with the same data should still be
%% serveable via GET /unconfirmed_chunk after re-seeding. This exercises the
%% chunk_offsets_synced path in check_not_already_synced.
test_same_data_after_disk_pool_cleared() ->
	test_same_data_txs(second_after_disk_pool_cleared).

test_same_data_txs(Mode) ->
	Wallet = ar_test_data_sync:setup_nodes(),
	InputChunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)],
	#{ tx := TX1, data_root := DataRoot, data_tree := DataTree, chunks := Chunks } =
		ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks),
	[{ChunkEndOffset, Proof}] = ar_test_data_sync:build_proofs(
		DataRoot, DataTree, Chunks, #{ proof_offset => end_offset }),
	SeedChunk = fun() ->
		?assertMatch(
			{ok, {{<<"200">>, _}, _, _, _, _}},
			ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
		)
	end,
	TX2 = case Mode of
		both_before_seed ->
			#{ tx := TX2a, data_root := DataRoot2a, chunks := Chunks2a } =
				ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks),
			?assertEqual(DataRoot, DataRoot2a),
			?assertEqual(Chunks, Chunks2a),
			?assertNotEqual(TX1#tx.id, TX2a#tx.id),
			ar_test_node:assert_post_tx_to_peer(main, TX1),
			ar_test_node:assert_post_tx_to_peer(main, TX2a),
			SeedChunk(),
			SeedChunk(),
			TX2a;
		second_after_seed ->
			ar_test_node:assert_post_tx_to_peer(main, TX1),
			SeedChunk(),
			#{ tx := TX2a, data_root := DataRoot2a, chunks := Chunks2a } =
				ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks),
			?assertEqual(DataRoot, DataRoot2a),
			?assertEqual(Chunks, Chunks2a),
			?assertNotEqual(TX1#tx.id, TX2a#tx.id),
			TX2a;
		second_after_first_confirmed ->
			ar_test_node:assert_post_tx_to_peer(main, TX1),
			SeedChunk(),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 1),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 2),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 3),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 4),
			{ok, {TXOffset1, _}} = ar_data_sync:get_tx_offset(TX1#tx.id),
			ar_test_data_sync:wait_until_syncs_chunk(TXOffset1, #{
				chunk => maps:get(chunk, Proof),
				data_path => maps:get(data_path, Proof)
			}),
			#{ tx := TX2a, data_root := DataRoot2a, chunks := Chunks2a } =
				ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks, #{ reward => ?AR(10) }),
			?assertEqual(DataRoot, DataRoot2a),
			?assertEqual(Chunks, Chunks2a),
			?assertNotEqual(TX1#tx.id, TX2a#tx.id),
			TX2a;
		second_after_disk_pool_cleared ->
			DataRootID = ar_data_roots:id(DataRoot, TX1#tx.data_size),
			ar_test_node:assert_post_tx_to_peer(main, TX1),
			SeedChunk(),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 1),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 2),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 3),
			ar_test_node:mine(main),
			assert_wait_until_height(main, 4),
			{ok, {TXOffset1, _}} = ar_data_sync:get_tx_offset(TX1#tx.id),
			ar_test_data_sync:wait_until_syncs_chunk(TXOffset1, #{
				chunk => maps:get(chunk, Proof),
				data_path => maps:get(data_path, Proof)
			}),
			%% Explicitly clear the data root state to simulate the disk pool having
			%% fully processed the chunk. This ensures we exercise the
			%% chunk_offsets_synced path in check_not_already_synced.
			ar_disk_pool:delete_data_root_state(DataRootID),
			?assertEqual(not_found, ar_disk_pool:get_data_root_state(DataRootID)),
			#{ tx := TX2a, data_root := DataRoot2a, chunks := Chunks2a } =
				ar_test_data_sync:make_fixed_data_tx(Wallet, InputChunks, #{ reward => ?AR(10) }),
			?assertEqual(DataRoot, DataRoot2a),
			?assertEqual(Chunks, Chunks2a),
			?assertNotEqual(TX1#tx.id, TX2a#tx.id),
			TX2a
	end,
	EncodedTXID1 = ar_util:encode(TX1#tx.id),
	{ok, {{<<"200">>, _}, _, Body1, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID1, ChunkEndOffset),
	Response1 = jiffy:decode(Body1, [return_maps]),
	assert_unconfirmed_chunk_response(Response1, Proof, true),
	case Mode of
		both_before_seed ->
			ok;
		_ ->
			ar_test_node:assert_post_tx_to_peer(main, TX2),
			SeedChunk()
	end,
	EncodedTXID2 = ar_util:encode(TX2#tx.id),
	{ok, {{<<"200">>, _}, _, Body2, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID2, ChunkEndOffset),
	Response2 = jiffy:decode(Body2, [return_maps]),
	assert_unconfirmed_chunk_response(Response2, Proof, true).

%% @doc Negative offset returns 400.
test_negative_offset() ->
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
test_offset_beyond_data() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	#{ tx := TX } = post_single_chunk_tx(Wallet),
	EncodedTXID = ar_util:encode(TX#tx.id),
	?assertMatch(
		{ok, {{<<"404">>, _}, _, _, _, _}},
		ar_test_node:get_unconfirmed_chunk(main, EncodedTXID, ?DATA_CHUNK_SIZE * 10)
	).

%% @doc GET /unconfirmed_chunk/TXID/Offset where Offset is beyond the end of the
%% TX should return 400. 
test_offset_beyond_tx_size() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [{10 * ?PARTITION_SIZE, 0,
			ar_test_node:get_default_storage_module_packing(Addr, 0)}],
	Wallet = ar_test_data_sync:setup_nodes(
			#{ addr => Addr, storage_modules => StorageModules }),
	%% A single sub-chunk-size TX (size between 20 and 700).
	TXSize = 500,
	TXData = crypto:strong_rand_bytes(TXSize),
	#{ tx := TX, proofs := [{TXEndOffset, _Proof}] } =
		post_and_seed_tx(Wallet, [TXData], #{ proof_offset => end_offset }),
	%% Confirm the TX so the query goes through the tx_index fallback path.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 2),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 3),
	ar_test_node:mine(main),
	assert_wait_until_height(main, 4),
	%% Clear the disk-pool cache entry so the query can't be served from the
	%% ETS cache — forcing it down the tx_index fallback path.
	ets:delete(ar_disk_pool_chunks_cache, {TX#tx.id, TXEndOffset}),
	EncodedTXID = ar_util:encode(TX#tx.id),
	%% Offset 700 is past the TX's size (500) and must be rejected.
	wait_for_unconfirmed_chunk(EncodedTXID, 700, <<"400">>).

%% @doc Chunk is still retrievable after mining only 1 block (partial confirmation).
test_partial_confirmation() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	#{ tx := TX, chunk_end_offset := ChunkEndOffset, proof := Proof } =
		post_single_chunk_tx(Wallet),
	%% Mine one block — chunk is partially confirmed but not yet pruned from disk pool.
	ar_test_node:mine(main),
	assert_wait_until_height(main, 1),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	assert_unconfirmed_chunk_response(Response, Proof, true).

%% @doc The data_path returned by the endpoint is a valid merkle proof.
test_data_path_valid() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	#{ tx := TX, data_root := DataRoot, chunk_end_offset := ChunkEndOffset, proof := Proof } =
		post_single_chunk_tx(Wallet),
	EncodedTXID = ar_util:encode(TX#tx.id),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = wait_for_unconfirmed_chunk(
		EncodedTXID, ChunkEndOffset),
	Response = jiffy:decode(Body, [return_maps]),
	assert_unconfirmed_chunk_response(Response, Proof, true),
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
test_concurrent_requests() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	#{ tx := TX, chunk_end_offset := ChunkEndOffset, proof := Proof } =
		post_single_chunk_tx(Wallet),
	EncodedTXID = ar_util:encode(TX#tx.id),
	NumRequests = 5,
	Results = ar_util:pmap(
		fun(_) ->
			wait_for_unconfirmed_chunk(EncodedTXID, ChunkEndOffset)
		end,
		lists:seq(1, NumRequests),
		30_000
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
	FirstResponse = jiffy:decode(FirstBody, [return_maps]),
	assert_unconfirmed_chunk_response(FirstResponse, Proof, true),
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
	TX1InputChunks = [crypto:strong_rand_bytes(?DATA_CHUNK_SIZE) || _ <- lists:seq(1, 2)],
	#{ tx := TX1 } = post_and_seed_tx(Wallet, TX1InputChunks),

	%% --- TX2: 1 full chunk + 1 sub-chunk-size chunk ---
	SmallChunkSize = 1000,
	TX2Chunk1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	TX2Chunk2 = crypto:strong_rand_bytes(SmallChunkSize),
	TX2InputChunks = [TX2Chunk1, TX2Chunk2],
	#{ tx := TX2 } = post_and_seed_tx(Wallet, TX2InputChunks),

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
					{ok, {{<<"200">>, _}, _, ChunkBody, _, _}} = wait_for_unconfirmed_chunk(
						DiscoveredTXID, EndOffset),
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
