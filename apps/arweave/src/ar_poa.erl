-module(ar_poa).

-export([generate/1]).
-export([validate/4]).
-export([validate_data_root/2, validate_data_tree/2, validate_chunk/3]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.

%% @doc Generate a POA for the first option that we can.
generate([B]) when is_record(B, block) ->
	%% Special genesis edge case.
	generate(B);
generate(B) when is_record(B, block) ->
	generate([{B#block.indep_hash, 0}]);
generate([B|_]) when is_record(B, block) -> 
	generate(B);
generate([]) -> unavailable;
generate([{Seed, WeaveSize}|_] = BI) ->
	ar:info([{generating_poa_for_block_after, ar_util:encode(Seed)}]),
	case length(BI) >= ?FORK_2_0 of
		true ->
			generate(
				Seed,
				WeaveSize,
				BI,
				1,
				ar_meta_db:get(max_option_depth)
			);
		false ->
			ar_node_utils:find_recall_block(BI)
	end.

generate(_, _, _, N, N) -> unavailable;
generate(Seed, WeaveSize, BI, Option, Limit) ->
	ChallengeByte = calculate_challenge_byte(Seed, WeaveSize, Option),
	{ChallengeBlock, BlockBase} = find_challenge_block(ChallengeByte, BI),
	ar:info(
		[
			{poa_validation_block_indexes, [ {ar_util:encode(BH), WVSZ} || {BH, WVSZ} <- BI ]},
			{challenge_block, ar_util:encode(ChallengeBlock)}
		]
	),
	case ar_storage:read_block(ChallengeBlock, BI) of
		unavailable ->
			generate(Seed, WeaveSize, BI, Option + 1, Limit);
		B ->
			case B#block.txs of
				[] -> create_poa_from_data(B, no_tx, [], ChallengeByte - BlockBase, Option);
				TXIDs ->
					TXs = ar_storage:read_tx(TXIDs),
					SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
					TXID =
						find_byte_in_size_tagged_list(
							ChallengeByte - BlockBase,
							SizeTaggedTXs
						),
					case ar_storage:read_tx(TXID) of
						unavailable ->
							generate(Seed, WeaveSize, BI, Option + 1, Limit);
						NoTreeTX ->
							create_poa_from_data(B, NoTreeTX, SizeTaggedTXs, ChallengeByte - BlockBase, Option)
					end
			end
	end.

create_poa_from_data(B, no_tx, _, _BlockOffset, Option) ->
	#poa {
		option = Option,
		recall_block = B#block { txs = [], block_index = [], poa = undefined },
		tx_path = <<>>,
		data_path = <<>>,
		chunk = <<>>,
		tx = undefined
	};
create_poa_from_data(NoTreeB, NoTreeTX, SizeTaggedTXs, BlockOffset, Option) ->
	B = ar_block:generate_tx_tree(NoTreeB, SizeTaggedTXs),
	{_TXID, TXEnd} = lists:keyfind(NoTreeTX#tx.id, 1, SizeTaggedTXs),
	TXStart = TXEnd - NoTreeTX#tx.data_size,
	TXOffset = BlockOffset - TXStart,
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, NoTreeTX#tx.data),
	SizedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	Chunk = find_byte_in_size_tagged_list(TXOffset, SizedChunks),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizedChunks),
	TX = ar_tx:generate_chunk_tree(NoTreeTX, SizedChunkIDs),
	ar:info(
		[
			poa_generation,
			{weave_size, B#block.weave_size},
			{block_offset, BlockOffset},
			{tx_offset, BlockOffset},
			{tx_start, TXStart},
			{chunk_size, byte_size(Chunk)},
			{chunk_num, search(Chunk, SizedChunks)},
			{chunk_id, ar_util:encode(ar_tx:generate_chunk_id(Chunk))}
		]
	),
	TXPath =
		ar_merkle:generate_path(
			B#block.tx_root,
			BlockOffset,
			B#block.tx_tree
		),
	DataPath =
		ar_merkle:generate_path(
			TX#tx.data_root,
			TXOffset,
			TX#tx.data_tree
		),
	#poa {
		option = Option,
		recall_block =
			B#block {
				txs = [],
				block_index = [],
				wallet_list = ar_block:hash_wallet_list(B#block.wallet_list),
				poa = undefined,
				tx_tree = []
			},
		tx_path = TXPath,
		tx =
			TX#tx {
				data = <<>>,
				data_tree = []
			},
		data_path = DataPath,
		chunk = Chunk
	}.

search(X, [{X,_}|_]) -> 0;
search(X, [_|R]) -> 1 + search(X, R).

%% @doc Validate a complete proof of access object.
validate(LastHeaderHash, WeaveSize, BI, RawPOA) ->
	POA = RawPOA#poa { recall_block = ar_storage:read_block(RawPOA#poa.recall_block, BI)}, 
	ChallengeByte = calculate_challenge_byte(LastHeaderHash, WeaveSize, POA#poa.option),
	{ChallengeBlock, BlockBase} = find_challenge_block(ChallengeByte, BI),
	validate_recall_block(ChallengeByte - BlockBase, ChallengeBlock, POA).

calculate_challenge_byte(_, 0, _) -> 0;
calculate_challenge_byte(LastHeaderHash, WeaveSize, Option) ->
	binary:decode_unsigned(multihash(LastHeaderHash, Option)) rem WeaveSize.

multihash(X, Remaining) when Remaining =< 0 -> X;
multihash(X, Remaining) ->
	multihash(crypto:hash(?HASH_ALG, X), Remaining - 1).

%% @doc The base of the block is the weave_size tag of the _previous_ block.
%% Traverse the block index until the challenge block is inside the block's bounds.
find_challenge_block(Byte, [{BH, BlockTop}, {_, BlockBase} | _])
	when (Byte >= BlockBase) andalso (Byte < BlockTop) -> {BH, BlockBase};
%% When we are mining the first non-genesis block, the genesis block is the challenge.
find_challenge_block(_Byte, [{BH, _}]) -> {BH, 0};
find_challenge_block(Byte, [_ | R]) ->
	find_challenge_block(Byte, R).

find_byte_in_size_tagged_list(Byte, [{ID, TXEnd} | _])
		when TXEnd >= Byte -> ID;
find_byte_in_size_tagged_list(Byte, [_ | Rest]) ->
	find_byte_in_size_tagged_list(Byte, Rest).

validate_recall_block(BlockOffset, ChallengeBH, POA) ->
	ar:info([
		{poa_validation_rb, ar_util:encode(ar_weave:header_hash(POA#poa.recall_block))},
		{challenge, ar_util:encode(ChallengeBH)}
	]),
	case ar_weave:header_hash(POA#poa.recall_block) of
		ChallengeBH -> validate_tx_path(BlockOffset, POA);
		_ -> false
	end.

%% If we have validated the block and the challenge byte is 0, return true.
validate_tx_path(0, _) -> true;
validate_tx_path(BlockOffset, POA) ->
	Validation =
		ar_merkle:validate_path(
			(POA#poa.recall_block)#block.tx_root,
			BlockOffset,
			POA#poa.tx_path
		),
	case Validation of
		false -> false;
		TXID -> validate_tx(TXID, BlockOffset, POA)
	end.

validate_tx(TXID, BlockOffset, POA) when TXID == (POA#poa.tx)#tx.id ->
	case ar_tx:verify_after_mining(POA#poa.tx) of
		true ->
			validate_data_path(BlockOffset, POA);
		false -> false
	end;
validate_tx(_, _, _) -> false.

validate_data_path(BlockOffset, POA) ->
	%% Calculate TX offsets within the block.
	TXEndOffset = ar_merkle:extract_note(POA#poa.tx_path),
	TXStartOffset = TXEndOffset - (POA#poa.tx)#tx.data_size,
	TXOffset = BlockOffset - TXStartOffset,
	Validation =
		ar_merkle:validate_path(
			(POA#poa.tx)#tx.data_root,
			TXOffset,
			POA#poa.data_path
		),
	ar:info(
		[
			poa_verification,
			{block_header_hash, ar_util:encode((POA#poa.recall_block)#block.header_hash)},
			{tx, ar_util:encode((POA#poa.tx)#tx.id)},
			{tx_start_offset, TXStartOffset},
			{tx_end_offset, TXEndOffset},
			{tx_offset, TXOffset},
			{chunk_id, case Validation of false -> false; _ -> ar_util:encode(Validation) end}
		]
	),
	case Validation of
		false -> false;
		ChunkID ->
			validate_chunk(ChunkID, POA)
	end.

validate_chunk(ChunkID, POA) ->
	ChunkID == ar_tx:generate_chunk_id(POA#poa.chunk).

%% @doc Validate that an untrusted chunk index (probably received from another peer)
%% matches the chunk index hash of a transaction.
validate_data_root(TX, ChunkIndex) ->
	TX2 = ar_tx:generate_data_root(TX#tx { data_tree = ChunkIndex }),
	(TX#tx.data_root == TX2#tx.data_root).

%% @doc Validate that the chunk index against the entire TX data.
validate_data_tree(TX, Data) ->
	TX2 = ar_tx:generate_data_tree(TX#tx { data = Data }),
	TX#tx.data_tree == TX2#tx.data_tree.

%% @doc Validate a single chunk from a chunk index matches.
validate_chunk(TX, ChunkNum, Chunk) ->
	ChunkID = lists:nth(ChunkNum, TX#tx.data_tree),
	ChunkID == ar_tx:generate_chunk_id(TX, Chunk).

validate_chunking_test() ->
	%% Generate our TX data, wallet, and signed and indexed transactions.
	TXData = crypto:strong_rand_bytes(trunc(?DATA_CHUNK_SIZE * 5.5)),
	ChallengeLocation = (?DATA_CHUNK_SIZE * 3) + 10, % Pick a byte in the third chunk
	Chunk =
		binary:part(
			TXData,
			trunc(?DATA_CHUNK_SIZE*3),
			?DATA_CHUNK_SIZE
		),
	{Priv, Pub} = ar_wallet:new(),
	UnsignedTX =
		ar_tx:generate_data_tree(
			#tx {
				format = 2,
				data = TXData,
				data_size = byte_size(TXData),
				reward = ?AR(100)
			}
		),
	SignedTX = ar_tx:sign(UnsignedTX#tx { data = <<>>, data_tree = [] }, Priv, Pub),
	DataPath =
		ar_merkle:generate_path(
			SignedTX#tx.data_root,
			ChallengeLocation,
			UnsignedTX#tx.data_tree
		),
	%% Verify each level of the index and the chunk itself.
	Diff = 1,
	Height = 0,
	Timestamp = os:system_time(seconds),
	?assert(
		ar_tx:verify(
			SignedTX,
			Diff,
			Height,
			[{ar_wallet:to_address(Pub), ?AR(100), <<>>}],
			Timestamp
		)
	),
	RealChunkID = ar_tx:generate_chunk_id(Chunk),
	PathChunkID = ar_merkle:validate_path(SignedTX#tx.data_root, ChallengeLocation, DataPath),
	?assertEqual(RealChunkID, PathChunkID),
	% In this case, we know the chunk is valid as it is generated in the test, but in the real
	% world, this chunk would come from the proof of access received from the network.
	?assertEqual(PathChunkID, ar_tx:generate_chunk_id(Chunk)).
