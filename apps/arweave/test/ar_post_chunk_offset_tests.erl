-module(ar_post_chunk_offset_tests).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

post_chunk_offset_accepts_exact_match_test_() ->
	ar_test_node:test_with_mocked_functions(
		[strict_data_split_threshold_mock(10 * ?DATA_CHUNK_SIZE)],
		fun test_post_chunk_offset_accepts_exact_match/0,
		120
	).

post_chunk_offset_accepts_non_terminal_relative_offset_test_() ->
	ar_test_node:test_with_mocked_functions(
		[strict_data_split_threshold_mock(?DATA_CHUNK_SIZE)],
		fun test_post_chunk_offset_accepts_non_terminal_relative_offset/0,
		120
	).

post_chunk_offset_rejects_unindexed_offset_test_() ->
	{timeout, 120, fun test_post_chunk_offset_rejects_unindexed_offset/0}.

post_chunk_offset_rejects_wrong_chunk_boundary_test_() ->
	{timeout, 120, fun test_post_chunk_offset_rejects_wrong_chunk_boundary/0}.

test_post_chunk_offset_accepts_exact_match() ->
	Wallet = setup_main_node(),
	Chunks = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{AbsoluteEndOffset, Proof} | _] = ar_test_data_sync:build_proofs(B, TX, Chunks),
	assert_post_chunk_offset(AbsoluteEndOffset, Proof),
	assert_chunk_roundtrip(AbsoluteEndOffset, Proof).

test_post_chunk_offset_accepts_non_terminal_relative_offset() ->
	Wallet = setup_main_node(),
	Chunks = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{_FirstEndOffset, _FirstProof}, {SecondEndOffset, SecondProof}] =
		ar_test_data_sync:build_proofs(B, TX, Chunks),
	ProofAtInternalOffset = proof_with_internal_offset(Chunks, 2, SecondProof),
	assert_post_chunk_offset(SecondEndOffset, ProofAtInternalOffset),
	assert_chunk_roundtrip(SecondEndOffset, ProofAtInternalOffset).

test_post_chunk_offset_rejects_unindexed_offset() ->
	Wallet = setup_main_node(),
	Chunks1 = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	Chunks2 = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	{TX1, _} = tx_with_chunks(Wallet, Chunks1),
	B1 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX1]),
	{TX2, _} = tx_with_chunks(Wallet, Chunks2),
	B2 = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX2]),
	[{_AbsoluteEndOffset1, Proof1} | _] = ar_test_data_sync:build_proofs(B1, TX1, Chunks1),
	[{AbsoluteEndOffset2, _Proof2} | _] = ar_test_data_sync:build_proofs(B2, TX2, Chunks2),
	assert_invalid_offset(AbsoluteEndOffset2, Proof1).

test_post_chunk_offset_rejects_wrong_chunk_boundary() ->
	Wallet = setup_main_node(),
	Chunks = [
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		crypto:strong_rand_bytes(?DATA_CHUNK_SIZE)
	],
	{TX, _} = tx_with_chunks(Wallet, Chunks),
	B = ar_test_node:post_and_mine(#{ miner => main, await_on => main }, [TX]),
	[{_FirstEndOffset, FirstProof}, {SecondEndOffset, _SecondProof}] =
		ar_test_data_sync:build_proofs(B, TX, Chunks),
	assert_invalid_offset(SecondEndOffset, FirstProof).

assert_post_chunk_offset(AbsoluteEndOffset, Proof) ->
	RequestOffset = request_offset(AbsoluteEndOffset, Proof),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		post_chunk_at_offset(RequestOffset, Proof)
	).

assert_invalid_offset(AbsoluteEndOffset, Proof) ->
	RequestOffset = request_offset(AbsoluteEndOffset, Proof),
	?assertMatch(
		{ok, {{<<"400">>, _}, _, <<"{\"error\":\"invalid_offset\"}">>, _, _}},
		post_chunk_at_offset(RequestOffset, Proof)
	).

assert_chunk_roundtrip(AbsoluteEndOffset, Proof) ->
	Expected = #{
		chunk => maps:get(chunk, Proof),
		data_path => maps:get(data_path, Proof),
		tx_path => maps:get(tx_path, Proof)
	},
	ar_test_data_sync:wait_until_syncs_chunk(AbsoluteEndOffset, Expected).

post_chunk_at_offset(RequestOffset, Proof) ->
	ar_http:req(#{
		method => post,
		peer => ar_test_node:peer_ip(main),
		path => "/chunk/" ++ integer_to_list(RequestOffset),
		body => ar_serialize:jsonify(Proof)
	}).

request_offset(AbsoluteEndOffset, Proof) ->
	DecodedProof = ar_serialize:json_map_to_poa_map(
		jiffy:decode(ar_serialize:jsonify(Proof), [return_maps])),
	AbsoluteEndOffset - proof_end_offset(DecodedProof) + maps:get(offset, DecodedProof).

proof_end_offset(#{ data_root := DataRoot, data_path := DataPath, data_size := DataSize,
		offset := Offset }) ->
	Base = ar_merkle:validate_path(DataRoot, Offset, DataSize, DataPath, strict_borders_ruleset),
	Strict = ar_merkle:validate_path(DataRoot, Offset, DataSize, DataPath,
			strict_data_split_ruleset),
	Rebase = ar_merkle:validate_path(DataRoot, Offset, DataSize, DataPath,
			offset_rebase_support_ruleset),
	case {Base, Strict, Rebase} of
		{false, false, false} ->
			error(invalid_chunk_proof);
		{_, {_, _, EndOffset}, _} ->
			EndOffset;
		{_, _, {_, _, EndOffset}} ->
			EndOffset;
		{{_, _, EndOffset}, _, _} ->
			EndOffset
	end.

proof_with_internal_offset(Chunks, ChunkIndex, BaseProof) ->
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks)
	),
	{Chunk, ChunkEndOffset} = lists:nth(ChunkIndex, SizeTaggedChunks),
	ChunkStartOffset = ChunkEndOffset - byte_size(Chunk),
	InternalOffset = ChunkStartOffset + byte_size(Chunk) div 2,
	BaseProof#{
		data_root => ar_util:encode(DataRoot),
		data_path => ar_util:encode(ar_merkle:generate_path(DataRoot, InternalOffset, DataTree)),
		chunk => ar_util:encode(Chunk),
		offset => integer_to_binary(InternalOffset),
		data_size => integer_to_binary(byte_size(binary:list_to_bin(Chunks)))
	}.

strict_data_split_threshold_mock(Value) ->
	{ar_block, strict_data_split_threshold, fun() -> Value end}.

setup_main_node() ->
	Wallet = {_, Pub} = ar_wallet:new(),
	WalletAddr = ar_wallet:to_address(Pub),
	[Genesis] = ar_weave:init(
		[{WalletAddr, ?AR(200000), <<>>}],
		ar_retarget:switch_to_linear_diff(2)
	),
	{ok, Config} = arweave_config:get_env(),
	Config2 = Config#config{
		enable = lists:usort([pack_served_chunks | Config#config.enable])
	},
	ar_test_node:start(#{ b0 => Genesis, config => Config2 }),
	Wallet.

tx_with_chunks(Wallet, Chunks) ->
	{DataRoot, _} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)
		)
	),
	ar_test_data_sync:tx(#{
		wallet => Wallet,
		split_type => {fixed_data, DataRoot, Chunks},
		format => v2,
		reward => fetch,
		get_fee_peer => main,
		tx_anchor_peer => main
	}).
