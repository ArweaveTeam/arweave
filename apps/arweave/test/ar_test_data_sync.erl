-module(ar_test_data_sync).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-export([setup_nodes/0, setup_nodes/2, imperfect_split/1, build_proofs/3, build_proofs/5,
            tx/2, tx/3, tx/4, wait_until_syncs_chunk/2, 
            wait_until_syncs_chunks/1, wait_until_syncs_chunks/2, wait_until_syncs_chunks/3,
            get_tx_offset/2, get_tx_data/1,
            post_random_blocks/1, get_records_with_proofs/3, post_proofs/4,
            generate_random_split/1, generate_random_original_split/1,
            generate_random_standard_split/0, generate_random_original_v1_split/0]).


get_records_with_proofs(B, TX, Chunks) ->
	[{B, TX, Chunks, Proof} || Proof <- build_proofs(B, TX, Chunks)].

setup_nodes() ->
	Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	PeerAddr = ar_wallet:to_address(ar_test_node:remote_call(peer1, ar_wallet, new_keyfile, [])),
	setup_nodes(Addr, PeerAddr).

setup_nodes(MainAddr, PeerAddr) ->
	Wallet = {_, Pub} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(200000), <<>>}]),
	{ok, Config} = application:get_env(arweave, config),
	ar_test_node:start(B0, MainAddr, Config),
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	ar_test_node:start_peer(peer1, B0, PeerAddr, PeerConfig),
	ar_test_node:connect_to_peer(peer1),
	Wallet.

tx(Wallet, SplitType) ->
	tx(Wallet, SplitType, v2, fetch).

v1_tx(Wallet) ->
	tx(Wallet, original_split, v1, fetch).

tx(Wallet, SplitType, Format) ->
	tx(Wallet, SplitType, Format, fetch).

tx(Wallet, SplitType, Format, Reward) ->
	case {SplitType, Format} of
		{{fixed_data, DataRoot, Chunks}, v2} ->
			Data = binary:list_to_bin(Chunks),
			Args = #{ data_size => byte_size(Data), data_root => DataRoot,
					last_tx => ar_test_node:get_tx_anchor(main) },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			{ar_test_node:sign_tx(Wallet, Args2), Chunks};
		{{fixed_data, DataRoot, Chunks}, v1} ->
			Data = binary:list_to_bin(Chunks),
			Args = #{ data_size => byte_size(Data), data_root => DataRoot,
					last_tx => ar_test_node:get_tx_anchor(main), data => Data },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			{ar_test_node:sign_v1_tx(Wallet, Args2), Chunks};
		{original_split, v1} ->
			{_, Chunks} = generate_random_original_v1_split(),
			Data = binary:list_to_bin(Chunks),
			Args = #{ data => Data, last_tx => ar_test_node:get_tx_anchor(main) },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			{ar_test_node:sign_v1_tx(Wallet, Args2), Chunks};
		{original_split, v2} ->
			{DataRoot, Chunks} = generate_random_original_split(),
			Data = binary:list_to_bin(Chunks),
			Args = #{ data_size => byte_size(Data), data_root => DataRoot,
					last_tx => ar_test_node:get_tx_anchor(main) },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			{ar_test_node:sign_tx(Wallet, Args2), Chunks};
		{{custom_split, ChunkNumber}, v2} ->
			{DataRoot, Chunks} = generate_random_split(ChunkNumber),
			Args = #{ data_size => byte_size(binary:list_to_bin(Chunks)),
					last_tx => ar_test_node:get_tx_anchor(main), data_root => DataRoot },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			TX = ar_test_node:sign_tx(Wallet, Args2),
			{TX, Chunks};
		{standard_split, v2} ->
			{DataRoot, Chunks} = generate_random_standard_split(),
			Data = binary:list_to_bin(Chunks),
			Args = #{ data_size => byte_size(Data), data_root => DataRoot,
					last_tx => ar_test_node:get_tx_anchor(main) },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			TX = ar_test_node:sign_tx(Wallet, Args2),
			{TX, Chunks};
		{{original_split, ChunkNumber}, v2} ->
			{DataRoot, Chunks} = generate_random_original_split(ChunkNumber),
			Data = binary:list_to_bin(Chunks),
			Args = #{ data_size => byte_size(Data), data_root => DataRoot,
					last_tx => ar_test_node:get_tx_anchor(main) },
			Args2 = case Reward of fetch -> Args; _ -> Args#{ reward => Reward } end,
			TX = ar_test_node:sign_tx(Wallet, Args2),
			{TX, Chunks}
	end.

generate_random_split(ChunkCount) ->
	Chunks = lists:foldl(
		fun(_, Chunks) ->
			RandomSize =
				case rand:uniform(3) of
					1 ->
						?DATA_CHUNK_SIZE;
					_ ->
						OneThird = ?DATA_CHUNK_SIZE div 3,
						OneThird + rand:uniform(?DATA_CHUNK_SIZE - OneThird) - 1
				end,
			Chunk = crypto:strong_rand_bytes(RandomSize),
			[Chunk | Chunks]
		end,
		[],
		lists:seq(1, case ChunkCount of random -> rand:uniform(5); _ -> ChunkCount end)),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(Chunks)),
	{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
	{DataRoot, Chunks}.

generate_random_original_v1_split() ->
	%% Make sure v1 data does not end with a digit, otherwise it's malleable.
	Data = << (crypto:strong_rand_bytes(rand:uniform(1024 * 1024)))/binary, <<"a">>/binary >>,
	original_split(Data).

generate_random_original_split() ->
	Data = << (crypto:strong_rand_bytes(rand:uniform(1024 * 1024)))/binary >>,
	original_split(Data).

generate_random_standard_split() ->
	Data = crypto:strong_rand_bytes(rand:uniform(3 * ?DATA_CHUNK_SIZE)),
	v2_standard_split(Data).

generate_random_original_split(ChunkCount) ->
	RandomSize = (ChunkCount - 1) * ?DATA_CHUNK_SIZE + rand:uniform(?DATA_CHUNK_SIZE),
	Data = crypto:strong_rand_bytes(RandomSize),
	original_split(Data).

%% @doc Split the way v1 transactions are split.
original_split(Data) ->
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, Data),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
		ar_tx:chunks_to_size_tagged_chunks(Chunks)
	),
	{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
	{DataRoot, Chunks}.

%% @doc Split the way v2 transactions are usually split (arweave-js does it
%% this way as of the time this was written).
v2_standard_split(Data) ->
	Chunks = v2_standard_split_get_chunks(Data),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
		ar_tx:chunks_to_size_tagged_chunks(Chunks)
	),
	{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
	{DataRoot, Chunks}.

v2_standard_split_get_chunks(Data) ->
	v2_standard_split_get_chunks(Data, [], 32 * 1024).

v2_standard_split_get_chunks(Chunk, Chunks, _MinSize) when byte_size(Chunk) =< 262144 ->
    lists:reverse([Chunk | Chunks]);
v2_standard_split_get_chunks(<< _:262144/binary, LastChunk/binary >> = Rest, Chunks, MinSize)
		when byte_size(LastChunk) < MinSize ->
    FirstSize = round(math:ceil(byte_size(Rest) / 2)),
    << Chunk1:FirstSize/binary, Chunk2/binary >> = Rest,
    lists:reverse([Chunk2, Chunk1 | Chunks]);
v2_standard_split_get_chunks(<< Chunk:262144/binary, Rest/binary >>, Chunks, MinSize) ->
    v2_standard_split_get_chunks(Rest, [Chunk | Chunks], MinSize).

imperfect_split(Data) ->
	imperfect_split(?DATA_CHUNK_SIZE, Data).

imperfect_split(_ChunkSize, Bin) when byte_size(Bin) == 0 ->
	[];
imperfect_split(ChunkSize, Bin) when byte_size(Bin) < ChunkSize ->
	[Bin];
imperfect_split(ChunkSize, Bin) ->
	<<ChunkBin:ChunkSize/binary, Rest/binary>> = Bin,
	HalfSize = ChunkSize div 2,
	case byte_size(Rest) < HalfSize of
		true ->
			<<ChunkBin2:HalfSize/binary, Rest2/binary>> = Bin,
			%% If Rest is <<>>, both chunks are HalfSize - the chunks are invalid
			%% after the strict data split threshold.
			[ChunkBin2, Rest2];
		false ->
			[ChunkBin | imperfect_split(ChunkSize, Rest)]
	end.

build_proofs(B, TX, Chunks) ->
	build_proofs(TX, Chunks, B#block.txs, B#block.weave_size - B#block.block_size,
			B#block.height).

build_proofs(TX, Chunks, TXs, BlockStartOffset, Height) ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, Height),
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{value, {_, TXOffset}} =
		lists:search(fun({{TXID, _}, _}) -> TXID == TX#tx.id end, SizeTaggedTXs),
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	TXPath = ar_merkle:generate_path(TXRoot, TXOffset - 1, TXTree),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	{DataRoot, DataTree} = ar_merkle:generate_tree(
		ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks)
	),
	DataSize = byte_size(binary:list_to_bin(Chunks)),
	lists:foldl(
		fun
			({<<>>, _}, Proofs) ->
				Proofs;
			({Chunk, ChunkOffset}, Proofs) ->
				TXStartOffset = TXOffset - DataSize,
				AbsoluteChunkEndOffset = BlockStartOffset + TXStartOffset + ChunkOffset,
				Proof = #{
					tx_path => ar_util:encode(TXPath),
					data_root => ar_util:encode(DataRoot),
					data_path =>
						ar_util:encode(
							ar_merkle:generate_path(DataRoot, ChunkOffset - 1, DataTree)
						),
					chunk => ar_util:encode(Chunk),
					offset => integer_to_binary(ChunkOffset - 1),
					data_size => integer_to_binary(DataSize)
				},
				Proofs ++ [{AbsoluteChunkEndOffset, Proof}]
		end,
		[],
		SizeTaggedChunks
	).

get_tx_offset(Node, TXID) ->
	Peer = ar_test_node:peer_ip(Node),
	ar_http:req(#{
		method => get,
		peer => Peer,
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/offset"
	}).

get_tx_data(TXID) ->
  {ok, Config} = application:get_env(arweave, config),
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, Config#config.port},
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
	}).

post_random_blocks(Wallet) ->
	post_blocks(Wallet,
		[
			[v1],
			empty,
			[v2, v1, fixed_data, v2_no_data],
			[v2, v2_standard_split, v1, v2],
			empty,
			[v1, v2, v2, empty_tx, v2_standard_split],
			[v2, v2_no_data, v2_no_data, v1, v2_no_data],
			[empty_tx],
			empty,
			[v2_standard_split, v2_no_data, v2, v1, v2],
			empty,
			[fixed_data, fixed_data],
			empty,
			[fixed_data, fixed_data] % same tx_root as in the block before the previous one
		]
	).

post_blocks(Wallet, BlockMap) ->
	FixedChunks = [crypto:strong_rand_bytes(256 * 1024) || _ <- lists:seq(1, 4)],
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(
			ar_tx:chunks_to_size_tagged_chunks(FixedChunks)),
	{DataRoot, _} = ar_merkle:generate_tree(SizedChunkIDs),
	lists:foldl(
		fun
			({empty, Height}, Acc) ->
				ar_test_node:mine(),
				ar_test_node:assert_wait_until_height(peer1, Height),
				Acc;
			({TXMap, _Height}, Acc) ->
				TXsWithChunks = lists:map(
					fun
						(v1) ->
							{v1_tx(Wallet), v1};
						(v2) ->
							{tx(Wallet, original_split), v2};
						(v2_no_data) -> % same as v2 but its data won't be submitted
							{tx(Wallet, {custom_split, random}), v2_no_data};
						(v2_standard_split) ->
							{tx(Wallet, standard_split), v2_standard_split};
						(empty_tx) ->
							{tx(Wallet, {custom_split, 0}), empty_tx};
						(fixed_data) ->
							{tx(Wallet, {fixed_data, DataRoot, FixedChunks}), fixed_data}
					end,
					TXMap
				),
				B = ar_test_node:post_and_mine(
					#{ miner => main, await_on => main },
					[TX || {{TX, _}, _} <- TXsWithChunks]
				),
				Acc ++ [{B, TX, C} || {{TX, C}, Type} <- lists:sort(TXsWithChunks),
						Type /= v2_no_data, Type /= empty_tx]
		end,
		[],
		lists:zip(BlockMap, lists:seq(1, length(BlockMap)))
	).

post_proofs(Peer, B, TX, Chunks) ->
	Proofs = build_proofs(B, TX, Chunks),
	lists:foreach(
		fun({_, Proof}) ->
			{ok, {{<<"200">>, _}, _, _, _, _}} =
				ar_test_node:post_chunk(Peer, ar_serialize:jsonify(Proof))
		end,
		Proofs
	),
	Proofs.

wait_until_syncs_chunk(Offset, ExpectedProof) ->
	true = ar_util:do_until(
		fun() ->
			case ar_test_node:get_chunk(main, Offset) of
				{ok, {{<<"200">>, _}, _, ProofJSON, _, _}} ->
					Proof = jiffy:decode(ProofJSON, [return_maps]),
					{ok, {{<<"200">>, _}, _, NoChunkProofJSON, _, _}}
						= ar_test_node:get_chunk_proof(main, Offset),
					NoChunkProof = jiffy:decode(NoChunkProofJSON, [return_maps]),
					?assertEqual(maps:get(<<"data_path">>, Proof),
							maps:get(<<"data_path">>, NoChunkProof)),
					?assertEqual(maps:get(<<"tx_path">>, Proof),
							maps:get(<<"tx_path">>, NoChunkProof)),
					maps:fold(
						fun	(_Key, _Value, false) ->
								false;
							(Key, Value, true) ->
								maps:get(atom_to_binary(Key), Proof, not_set) == Value
						end,
						true,
						ExpectedProof
					);
				_ ->
					false
			end
		end,
		100,
		5000
	).

wait_until_syncs_chunks(Proofs) ->
	wait_until_syncs_chunks(main, Proofs, infinity).

wait_until_syncs_chunks(Proofs, UpperBound) ->
	wait_until_syncs_chunks(main, Proofs, UpperBound).

wait_until_syncs_chunks(Node, Proofs, UpperBound) ->
	lists:foreach(
		fun({EndOffset, Proof}) ->
			true = ar_util:do_until(
				fun() ->
					case EndOffset > UpperBound of
						true ->
							true;
						false ->
							case ar_test_node:get_chunk(Node, EndOffset) of
								{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} ->
									FetchedProof = ar_serialize:json_map_to_poa_map(
										jiffy:decode(EncodedProof, [return_maps])
									),
									ExpectedProof = #{
										chunk => ar_util:decode(maps:get(chunk, Proof)),
										tx_path => ar_util:decode(maps:get(tx_path, Proof)),
										data_path => ar_util:decode(maps:get(data_path, Proof))
									},
									compare_proofs(FetchedProof, ExpectedProof, EndOffset);
								_ ->
									false
							end
					end
				end,
				5 * 1000,
				180 * 1000
			)
		end,
		Proofs
	).

compare_proofs(#{ chunk := C, data_path := D, tx_path := T },
		#{ chunk := C, data_path := D, tx_path := T }, _EndOffset) ->
	true;
compare_proofs(_, _, EndOffset) ->
	?debugFmt("Proof mismatch for ~B.", [EndOffset]),
	false.
