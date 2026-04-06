-module(ar_get_chunk_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").

get_chunk_below_strict_threshold_test_() ->
	ar_test_node:test_with_mocked_functions(
		[strict_data_split_threshold_mock(10 * ?DATA_CHUNK_SIZE)],
		fun test_get_chunk_below_strict_threshold/0,
		120
	).

get_chunk_below_strict_threshold_small_tail_test_() ->
	ar_test_node:test_with_mocked_functions(
		[strict_data_split_threshold_mock(10 * ?DATA_CHUNK_SIZE)],
		fun test_get_chunk_below_strict_threshold_small_tail/0,
		120
	).

get_chunk_above_strict_threshold_test_() ->
	ar_test_node:test_with_mocked_functions(
		[strict_data_split_threshold_mock(?DATA_CHUNK_SIZE)],
		fun test_get_chunk_above_strict_threshold/0,
		180
	).

get_chunk_above_strict_threshold_small_tail_test_() ->
	ar_test_node:test_with_mocked_functions(
		[strict_data_split_threshold_mock(?DATA_CHUNK_SIZE)],
		fun test_get_chunk_above_strict_threshold_small_tail/0,
		180
	).

test_get_chunk_below_strict_threshold() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{AbsoluteEndOffset, Proof} | _] = ar_test_data_sync:build_proofs(B, TX, Chunks),
	post_and_wait_for_chunks([{AbsoluteEndOffset, Proof}]),
	?assert(AbsoluteEndOffset =< ar_block:strict_data_split_threshold()),
	fetch_and_assert_chunk(AbsoluteEndOffset, Proof).

test_get_chunk_below_strict_threshold_small_tail() ->
	SmallChunkSize = 12345,
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(SmallChunkSize)
	],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{AbsoluteEndOffset, Proof} | _] = ar_test_data_sync:build_proofs(B, TX, Chunks),
	post_and_wait_for_chunks([{AbsoluteEndOffset, Proof}]),
	?assert(byte_size(lists:last(Chunks)) < ?DATA_CHUNK_SIZE),
	?assert(AbsoluteEndOffset =< ar_block:strict_data_split_threshold()),
	fetch_and_assert_chunk(AbsoluteEndOffset, Proof).

test_get_chunk_above_strict_threshold() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	Chunks = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{FirstEndOffset, FirstProof}, {SecondEndOffset, SecondProof}] =
		ar_test_data_sync:build_proofs(B, TX, Chunks),
	post_and_wait_for_chunks([{FirstEndOffset, FirstProof}, {SecondEndOffset, SecondProof}]),
	Threshold = ar_block:strict_data_split_threshold(),
	AboveThreshold = [{AbsoluteEndOffset, Proof} || {AbsoluteEndOffset, Proof}
		<- [{FirstEndOffset, FirstProof}, {SecondEndOffset, SecondProof}],
		AbsoluteEndOffset > Threshold],
	?assertMatch([_ | _], AboveThreshold),
	lists:foreach(
		fun({AbsoluteEndOffset, Proof}) ->
			fetch_and_assert_chunk(AbsoluteEndOffset, Proof)
		end,
		AboveThreshold
	).

test_get_chunk_above_strict_threshold_small_tail() ->
	Wallet = ar_test_data_sync:setup_nodes(),
	SmallChunkSize = 12345,
	FirstChunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	LastChunk = crypto:strong_rand_bytes(SmallChunkSize),
	?assert(byte_size(LastChunk) < ?DATA_CHUNK_SIZE),
	Chunks = [FirstChunk, LastChunk],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{FirstEndOffset, FirstProof}, {SecondEndOffset, SecondProof}] =
		ar_test_data_sync:build_proofs(B, TX, Chunks),
	post_and_wait_for_chunks([{FirstEndOffset, FirstProof}, {SecondEndOffset, SecondProof}]),
	Threshold = ar_block:strict_data_split_threshold(),
	?assert(FirstEndOffset > Threshold),
	?assert(SecondEndOffset > Threshold),
	fetch_and_assert_chunk(FirstEndOffset, FirstProof),
	fetch_and_assert_chunk(SecondEndOffset, SecondProof).

fetch_and_assert_chunk(AbsoluteEndOffset, ExpectedProof) ->
	ChunkSize = byte_size(ar_util:decode(maps:get(chunk, ExpectedProof))),
	StartOffset = AbsoluteEndOffset - ChunkSize,
	Offsets = unique_offsets([
		AbsoluteEndOffset,
		AbsoluteEndOffset - 1,
		AbsoluteEndOffset - max(1, ChunkSize div 2)
	], StartOffset),
	fetch_and_assert_chunk(json, Offsets, AbsoluteEndOffset, ExpectedProof),
	fetch_and_assert_chunk(binary, Offsets, AbsoluteEndOffset, ExpectedProof).

fetch_and_assert_chunk(Format, Offsets, AbsoluteEndOffset, ExpectedProof) ->
	[FirstResponse | Rest] = [fetch_chunk_response(Format, Offset) || Offset <- Offsets],
	lists:foreach(
		fun(Response) ->
			?assertEqual(comparable_chunk_response(FirstResponse), comparable_chunk_response(Response))
		end,
		Rest
	),
	assert_chunk_response(FirstResponse, AbsoluteEndOffset, ExpectedProof).

fetch_chunk_response(json, Offset) ->
	{ok, {{<<"200">>, _}, Headers, ProofJSON, _, _}} = ar_test_node:get_chunk(main, Offset),
	{ok, Response} = ar_serialize:json_decode(ProofJSON, [return_maps]),
	#{
		headers => Headers,
		chunk => maps:get(<<"chunk">>, Response),
		data_path => maps:get(<<"data_path">>, Response),
		tx_path => maps:get(<<"tx_path">>, Response),
		absolute_end_offset => maps:get(<<"absolute_end_offset">>, Response),
		chunk_size => maps:get(<<"chunk_size">>, Response),
		packing => maps:get(<<"packing">>, Response)
	};
fetch_chunk_response(binary, Offset) ->
	{ok, {{<<"200">>, _}, Headers, ProofBinary, _, _}} =
		ar_http:req(#{
			method => get,
			peer => ar_test_node:peer_ip(main),
			path => "/chunk2/" ++ integer_to_list(Offset),
			headers => [{<<"x-bucket-based-offset">>, <<"true">>}]
		}),
	{ok, Response} = ar_serialize:binary_to_poa(ProofBinary),
	#{
		headers => Headers,
		chunk => ar_util:encode(maps:get(chunk, Response)),
		data_path => ar_util:encode(maps:get(data_path, Response)),
		tx_path => ar_util:encode(maps:get(tx_path, Response)),
		absolute_end_offset => proplists:get_value(<<"arweave-absolute-end-offset">>, Headers),
		chunk_size => integer_to_binary(byte_size(maps:get(chunk, Response))),
		packing => iolist_to_binary(ar_serialize:encode_packing(maps:get(packing, Response), true))
	}.

assert_chunk_response(Response, AbsoluteEndOffset, ExpectedProof) ->
	assert_absolute_end_offset_header(maps:get(headers, Response), AbsoluteEndOffset),
	?assertEqual(maps:get(chunk, ExpectedProof), maps:get(chunk, Response)),
	?assertEqual(maps:get(data_path, ExpectedProof), maps:get(data_path, Response)),
	?assertEqual(maps:get(tx_path, ExpectedProof), maps:get(tx_path, Response)),
	?assertEqual(integer_to_binary(AbsoluteEndOffset), maps:get(absolute_end_offset, Response)),
	?assertEqual(
		integer_to_binary(byte_size(ar_util:decode(maps:get(chunk, ExpectedProof)))),
		maps:get(chunk_size, Response)
	),
	?assertEqual(
		iolist_to_binary(ar_serialize:encode_packing(unpacked, true)),
		maps:get(packing, Response)
	).

comparable_chunk_response(Response) ->
	maps:remove(headers, Response).

assert_absolute_end_offset_header(Headers, AbsoluteEndOffset) ->
	?assertEqual(
		[{<<"arweave-absolute-end-offset">>, integer_to_binary(AbsoluteEndOffset)}],
		proplists:lookup_all(<<"arweave-absolute-end-offset">>, Headers)
	).

strict_data_split_threshold_mock(Value) ->
	{ar_block, strict_data_split_threshold, fun() -> Value end}.

tx_with_chunks(Wallet, Chunks) ->
	{DataRoot, _} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	ar_test_data_sync:tx(Wallet, {fixed_data, DataRoot, Chunks}).

post_and_wait_for_chunks(Proofs) ->
	lists:foreach(
		fun({_EndOffset, Proof}) ->
			?assertMatch(
				{ok, {{<<"200">>, _}, _, _, _, _}},
				ar_test_node:post_chunk(main, ar_serialize:jsonify(Proof))
			)
		end,
		Proofs
	),
	lists:foreach(
		fun({AbsoluteEndOffset, Proof}) ->
			Expected = #{
				chunk => maps:get(chunk, Proof),
				data_path => maps:get(data_path, Proof),
				tx_path => maps:get(tx_path, Proof)
			},
			ar_test_data_sync:wait_until_syncs_chunk(AbsoluteEndOffset, Expected)
		end,
		Proofs
	).

unique_offsets(Offsets, StartOffset) ->
	lists:usort([Offset || Offset <- Offsets, Offset > StartOffset, Offset >= 0]).
