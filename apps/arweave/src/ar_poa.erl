-module(ar_poa).

-export([generate/1, generate_2_0/2]).
-export([validate/6]).
-export([validate_data_root/2, validate_data_tree/2, validate_chunk/3]).
-export([adjust_diff/2]).
-export([get_recall_block/2, get_recall_tx/2]).

-include("ar.hrl").
-include("perpetual_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.

%% @doc Generate a POA for the first option that we can.
generate([B]) when is_record(B, block) ->
	%% Special genesis edge case.
	generate(B);
generate(B) when is_record(B, block) ->
	generate([{B#block.indep_hash, 0}]);
generate([B | _]) when is_record(B, block) ->
	generate(B);
generate([]) -> #poa{};
generate(BI) ->
	Height = length(BI),
	case Height >= ar_fork:height_2_0() of
		true ->
			generate_2_0(BI, min(ar_meta_db:get(max_option_depth), Height + 1));
		false ->
			ar_node_utils:find_recall_block(BI)
	end.

generate_2_0([], _) -> #poa{};
generate_2_0([{Seed, WeaveSize} | _] = BI, Depth) ->
	generate(
		Seed,
		WeaveSize,
		BI,
		1,
		Depth
	).

generate(_, _, _, N, N) ->
	ar:info([
		{event, no_data_for_poa},
		{tried_options, N - 1}
	]),
	unavailable;
generate(_, 0, _, _, _) ->
	#poa{};
generate(Seed, WeaveSize, BI, Option, Limit) ->
	RecallByte = calculate_challenge_byte(Seed, WeaveSize, Option),
	{RecallBH, BlockBase} = find_challenge_block(RecallByte, BI),
	ar:info(
		[
			{event, generate_poa},
			{challenge_block, ar_util:encode(RecallBH)}
		]
	),
	case ar_storage:read_block(RecallBH, BI) of
		unavailable ->
			generate(Seed, WeaveSize, BI, Option + 1, Limit);
		B ->
			case B#block.txs of
				[] ->
					ar:err([
						{event, empty_poa_challenge_block},
						{hash, ar_util:encode(RecallBH)}
					]),
					error;
				TXIDs ->
					{MissingTXIDs, TXs} = lists:foldl(
						fun(TXID, {AccMissing, Acc}) ->
							case ar_storage:read_tx(TXID) of
								unavailable ->
									{[TXID | AccMissing], Acc};
								TX ->
									{AccMissing, [TX | Acc]}
							end
						end,
						{[], []},
						TXIDs
					),
					case MissingTXIDs of
						[] ->
							SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
							TXHash =
								find_byte_in_size_tagged_list(
									RecallByte - BlockBase,
									SizeTaggedTXs
								),
							{value, TX} = lists:search(fun(T) -> ar_tx:tx_hash(T) == TXHash end, TXs),
							case byte_size(TX#tx.data) == 0 of
								true ->
									generate(Seed, WeaveSize, BI, Option + 1, Limit);
								false ->
									POA = create_poa_from_data(B, TX, SizeTaggedTXs, RecallByte - BlockBase, Option),
									case byte_size(POA#poa.data_path) > ?MAX_PATH_SIZE of
										true ->
											ar:info([
												{event, data_path_size_exceeds_the_limit},
												{block, ar_util:encode(POA#poa.block_indep_hash)},
												{tx, ar_util:encode(POA#poa.tx_id)},
												{limit, ?MAX_PATH_SIZE}
											]),
											generate(Seed, WeaveSize, BI, Option + 1, Limit);
										false ->
											POA
									end
							end
					end
			end
	end.

create_poa_from_data(NoTreeB, NoTreeTX, SizeTaggedTXs, BlockOffset, Option) ->
	B = ar_block:generate_tx_tree(NoTreeB, SizeTaggedTXs),
	{_TXHash, TXEnd} = lists:keyfind(ar_tx:tx_hash(NoTreeTX), 1, SizeTaggedTXs),
	TXStart = TXEnd - NoTreeTX#tx.data_size,
	TXOffset = BlockOffset - TXStart,
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, NoTreeTX#tx.data),
	SizedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	Chunk = find_byte_in_size_tagged_list(TXOffset, SizedChunks),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizedChunks),
	TX = ar_tx:generate_chunk_tree(NoTreeTX, SizedChunkIDs),
	ar:info(
		[
			{event, generated_poa},
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
		block_indep_hash = B#block.indep_hash,
		tx_id = TX#tx.id,
		tx_root = B#block.tx_root,
		tx_path = TXPath,
		data_size = TX#tx.data_size,
		data_root = TX#tx.data_root,
		data_path = DataPath,
		chunk = Chunk
	}.

search(X, [{X, _} | _]) -> 0;
search(X, [_ | R]) -> 1 + search(X, R).

%% @doc Get the recall block header specified in the PoA,
%% from the local storage or from remote peers.
get_recall_block(Peers, POA) ->
	MaybeRecallH = POA#poa.block_indep_hash,
	case MaybeRecallH of
		<<>> ->
			{ok, does_not_exist};
		RecallH ->
			case ar_storage:read_block_shadow(RecallH) of
				unavailable ->
					case ar_http_iface_client:get_block_shadow(Peers, RecallH) of
						unavailable ->
							unavailable;
						{_Peer, B} ->
							{ok, B}
					end;
				B ->
					{ok, B}
			end
	end.

%% @doc Get the recall transaction header specified in the PoA,
%% from the local storage or from remote peers.
get_recall_tx(Peers, POA) ->
	MaybeRecallID = POA#poa.tx_id,
	case MaybeRecallID of
		<<>> ->
			{ok, does_not_exist};
		RecallID ->
			case ar_http_iface_client:get_tx(Peers, RecallID, []) of
				unavailable ->
					unavailable;
				TX ->
					{ok, TX}
			end
	end.

%% @doc Validate a complete proof of access object.
validate(_H, 0, _BI, _POA, does_not_exist, does_not_exist) ->
	%% The weave does not have data yet.
	true;
validate(_H, _WS, BI, #poa{ option = Option }, _RecallB, _RecallTX) when Option > length(BI) + 1 ->
	false;
validate(_H, _WS, _BI, _POA, does_not_exist, _RecallTX) ->
	false;
validate(_H, _WS, _BI, _POA, _RecallB, does_not_exist) ->
	false;
validate(LastIndepHash, WeaveSize, BI, POA, RecallB, RecallTX) ->
	RecallByte = calculate_challenge_byte(LastIndepHash, WeaveSize, POA#poa.option),
	{ExpectedRecallBH, BlockBase} = find_challenge_block(RecallByte, BI),
	validate_recall_block(RecallByte - BlockBase, ExpectedRecallBH, RecallB, RecallTX, POA).

calculate_challenge_byte(_, 0, _) -> 0;
calculate_challenge_byte(LastIndepHash, WeaveSize, Option) ->
	binary:decode_unsigned(multihash(LastIndepHash, Option)) rem WeaveSize.

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

validate_recall_block(BlockOffset, ExpectedRecallBH, RecallB, RecallTX, POA) ->
	case ar_weave:indep_hash_post_fork_2_0(RecallB) of
		ExpectedRecallBH -> validate_tx_root(BlockOffset, RecallB, RecallTX, POA);
		_ -> false
	end.

validate_tx_root(BlockOffset, RecallB, RecallTX, POA) ->
	case RecallB#block.tx_root == POA#poa.tx_root of
		true ->
			validate_tx_path(BlockOffset, RecallTX, POA);
		false ->
			false
	end.

validate_tx_path(BlockOffset, RecallTX, POA) ->
	{Validation, _, _} =
		ar_merkle:validate_path(
			POA#poa.tx_root,
			BlockOffset,
			POA#poa.tx_path
		),
	case Validation of
		false -> false;
		TXHash -> validate_tx(TXHash, BlockOffset, RecallTX, POA)
	end.

validate_tx(TXHash, BlockOffset, RecallTX, POA) ->
	case ar_tx:tx_hash(RecallTX) == TXHash of
		true ->
			validate_data_root(BlockOffset, RecallTX, POA);
		false ->
			false
	end.

validate_data_root(BlockOffset, RecallTX, POA) when RecallTX#tx.data_root == POA#poa.data_root ->
	validate_data_path(BlockOffset, RecallTX, POA);
validate_data_root(_BlockOffset, _RecallTX, _POA) ->
	false.

validate_data_path(BlockOffset, RecallTX, POA) ->
	%% Calculate TX offsets within the block.
	TXEndOffset = ar_merkle:extract_note(POA#poa.tx_path),
	TXStartOffset = TXEndOffset - RecallTX#tx.data_size,
	TXOffset = BlockOffset - TXStartOffset,
	{Validation, _, _} =
		ar_merkle:validate_path(
			POA#poa.data_root,
			TXOffset,
			POA#poa.data_path
		),
	ar:info(
		[
			{event, verify_poa},
			{block_indep_hash, ar_util:encode(POA#poa.block_indep_hash)},
			{tx, ar_util:encode(POA#poa.tx_id)},
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

%% @doc Adjust the difficulty based on the POA option.
adjust_diff(Diff, 1) ->
	Diff;
adjust_diff(Diff, Option) ->
	adjust_diff(ar_difficulty:multiply_diff(Diff, ?ALTERNATIVE_POA_DIFF_MULTIPLIER), Option - 1).
