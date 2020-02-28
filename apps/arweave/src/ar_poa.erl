-module(ar_poa).

-export([generate/1]).
-export([validate/4]).
-export([adjust_diff/2]).

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
	generate([{B#block.indep_hash, B#block.weave_size, B#block.tx_root}]);
generate([B | _]) when is_record(B, block) ->
	generate(B);
generate([]) -> #poa{};
generate(BI) ->
	Height = length(BI),
	case Height >= ar_fork:height_2_0() of
		true ->
			generate(BI, min(ar_meta_db:get(max_option_depth), Height + 1));
		false ->
			ar_node_utils:find_recall_block(BI)
	end.

generate([], _) -> #poa{};
generate([{Seed, WeaveSize, _TXRoot} | _] = BI, Depth) ->
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
	{_TXRoot, BlockBase, RecallBH} = find_challenge_block(RecallByte, BI),
	case ar_storage:read_block(RecallBH, BI) of
		unavailable ->
			generate(Seed, WeaveSize, BI, Option + 1, Limit);
		B ->
			generate(B, RecallByte - BlockBase, Seed, WeaveSize, BI, Option, Limit)
	end.

generate(B, BlockOffset, Seed, WeaveSize, BI, Option, Limit) ->
	case B#block.txs of
		[] ->
			ar:err([
				{event, empty_poa_challenge_block},
				{hash, ar_util:encode(B#block.indep_hash)}
			]),
			error;
		TXIDs ->
			TXs = lists:foldr(
				fun
					(_TXID, unavailable) -> unavailable;
					(TXID, Acc) ->
						case ar_storage:read_tx(TXID) of
							unavailable ->
								unavailable;
							TX ->
								[TX | Acc]
						end
				end,
				[],
				TXIDs
			),
			case TXs of
				unavailable ->
					generate(Seed, WeaveSize, BI, Option + 1, Limit);
				_ ->
					generate(B, TXs, BlockOffset, Seed, WeaveSize, BI, Option, Limit)
			end
	end.

generate(B, TXs, BlockOffset, Seed, WeaveSize, BI, Option, Limit) ->
	TXsWithDataRoot = lists:map(
		fun
			(#tx{ format = 2 } = TX) ->
				TX;
			(TX) ->
				DataRoot = (ar_tx:generate_chunk_tree(TX))#tx.data_root,
				TX#tx{ data_root = DataRoot }
		end,
		TXs
	),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXsWithDataRoot),
	DataRoot = find_byte_in_size_tagged_list(BlockOffset, SizeTaggedTXs),
	{value, TX} = lists:search(fun(TX) -> TX#tx.data_root == DataRoot end, TXsWithDataRoot),
	case byte_size(TX#tx.data) == 0 of
		true ->
			generate(Seed, WeaveSize, BI, Option + 1, Limit);
		false ->
			POA = create_poa_from_data(B, TX, SizeTaggedTXs, BlockOffset, Option),
			case byte_size(POA#poa.data_path) > ?MAX_PATH_SIZE of
				true ->
					ar:info([
						{event, data_path_size_exceeds_the_limit},
						{block, ar_util:encode(B#block.indep_hash)},
						{tx, ar_util:encode(TX#tx.id)},
						{limit, ?MAX_PATH_SIZE}
					]),
					generate(Seed, WeaveSize, BI, Option + 1, Limit);
				false ->
					case byte_size(POA#poa.tx_path) > ?MAX_PATH_SIZE of
						true ->
							ar:info([
								{event, tx_path_size_exceeds_the_limit},
								{block, ar_util:encode(B#block.indep_hash)},
								{tx, ar_util:encode(TX#tx.id)},
								{limit, ?MAX_PATH_SIZE}
							]);
						false ->
							POA
					end
			end
	end.

create_poa_from_data(NoTreeB, NoTreeTX, SizeTaggedTXs, BlockOffset, Option) ->
	B = ar_block:generate_tx_tree(NoTreeB, SizeTaggedTXs),
	{_DataRoot, TXEnd} = lists:keyfind(NoTreeTX#tx.data_root, 1, SizeTaggedTXs),
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
		tx_path = TXPath,
		data_path = DataPath,
		chunk = Chunk
	}.

search(X, [{X, _} | _]) -> 0;
search(X, [_ | R]) -> 1 + search(X, R).

%% @doc Validate a complete proof of access object.
validate(_H, 0, _BI, _POA) ->
	%% The weave does not have data yet.
	true;
validate(_H, _WS, BI, #poa{ option = Option }) when Option > length(BI) + 1 ->
	false;
validate(LastIndepHash, WeaveSize, BI, POA) ->
	RecallByte = calculate_challenge_byte(LastIndepHash, WeaveSize, POA#poa.option),
	{TXRoot, BlockBase, _BH} = find_challenge_block(RecallByte, BI),
	validate_tx_path(RecallByte - BlockBase, TXRoot, POA).

calculate_challenge_byte(_, 0, _) -> 0;
calculate_challenge_byte(LastIndepHash, WeaveSize, Option) ->
	binary:decode_unsigned(multihash(LastIndepHash, Option)) rem WeaveSize.

multihash(X, Remaining) when Remaining =< 0 -> X;
multihash(X, Remaining) ->
	multihash(crypto:hash(?HASH_ALG, X), Remaining - 1).

%% @doc The base of the block is the weave_size tag of the _previous_ block.
%% Traverse the block index until the challenge block is inside the block's bounds.
find_challenge_block(Byte, [{BH, BlockTop, TXRoot}, {_, BlockBase, _} | _])
	when (Byte >= BlockBase) andalso (Byte < BlockTop) -> {TXRoot, BlockBase, BH};
find_challenge_block(Byte, [_ | BI]) ->
	find_challenge_block(Byte, BI).

find_byte_in_size_tagged_list(Byte, [{DataRoot, TXEnd} | _])
		when TXEnd >= Byte -> DataRoot;
find_byte_in_size_tagged_list(Byte, [_ | Rest]) ->
	find_byte_in_size_tagged_list(Byte, Rest).

validate_tx_path(BlockOffset, TXRoot, POA) ->
	Validation =
		ar_merkle:validate_path(
			TXRoot,
			BlockOffset,
			POA#poa.tx_path
		),
	case Validation of
		false -> false;
		{DataRoot, StartOffset, _EndOffset} ->
			TXOffset = BlockOffset - StartOffset,
			validate_data_path(DataRoot, TXOffset, POA)
	end.

validate_data_path(DataRoot, TXOffset, POA) ->
	Validation =
		ar_merkle:validate_path(
			DataRoot,
			TXOffset,
			POA#poa.data_path
		),
	case Validation of
		false -> false;
		{ChunkID, _, _} ->
			validate_chunk(ChunkID, POA)
	end.

validate_chunk(ChunkID, POA) ->
	ChunkID == ar_tx:generate_chunk_id(POA#poa.chunk).

%% @doc Adjust the difficulty based on the POA option.
adjust_diff(Diff, 1) ->
	Diff;
adjust_diff(Diff, Option) ->
	adjust_diff(ar_difficulty:multiply_diff(Diff, ?ALTERNATIVE_POA_DIFF_MULTIPLIER), Option - 1).
