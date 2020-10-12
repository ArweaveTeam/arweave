%%% @doc This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.
-module(ar_poa).

-export([
	generate/1,
	validate/4,
	modify_diff/2
]).

-include("ar.hrl").
-include("perpetual_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MIN_MAX_OPTION_DEPTH, 100).

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
	%% Find locally available data to generate a PoA. Do not go
	%% deeeper than the configured depth - the PoW difficulty increases
	%% exponentially so it does not make sense to go too deep.
	%% There is a hard limit based on the weave height to keep
	%% validation cheap. The minimum maximum depth of ?MIN_MAX_OPTION_DEPTH
	%% is made for small weaves (useful in tests).
	ConfiguredDepth = ar_meta_db:get(max_poa_option_depth) + 1,
	Depth = min(ConfiguredDepth, max(Height + 1, ?MIN_MAX_OPTION_DEPTH + 1)),
	generate(BI, Depth).

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
	{TXRoot, BlockBase, _BlockTop, RecallBH} = find_challenge_block(RecallByte, BI),
	case ar_data_sync:get_chunk(RecallByte + 1) of
		{ok, #{ tx_root := TXRoot, chunk := Chunk, tx_path := TXPath, data_path := DataPath }} ->
			ar:info(
				[
					{event, generated_poa_from_v2_index},
					{weave_size, WeaveSize},
					{recall_byte, RecallByte},
					{option, Option}
				]
			),
			#poa{ option = Option, chunk = Chunk, tx_path = TXPath, data_path = DataPath };
		_ ->	
			case ar_storage:read_block(RecallBH) of
				unavailable ->
					generate(Seed, WeaveSize, BI, Option + 1, Limit);
				B ->
					generate(B, RecallByte - BlockBase, Seed, WeaveSize, BI, TXRoot, Option, Limit)
			end
	end.

generate(B, BlockOffset, Seed, WeaveSize, BI, TXRoot, Option, Limit) ->
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
					generate(B, TXs, BlockOffset, Seed, WeaveSize, BI, TXRoot, Option, Limit)
			end
	end.

generate(B, TXs, BlockOffset, Seed, WeaveSize, BI, TXRoot, Option, Limit) ->
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs),
	{{TXID, DataRoot}, TXEnd} = find_byte_in_size_tagged_list(BlockOffset, SizeTaggedTXs),
	{value, TX} = lists:search(
		fun(#tx{ id = ID }) ->
			ID == TXID
		end,
		TXs
	),
	TXStart = TXEnd - TX#tx.data_size,
	TXData = get_tx_data(TX),
	case byte_size(TXData) > 0 of
		false ->
			generate(Seed, WeaveSize, BI, Option + 1, Limit);
		true ->
			case create_poa_from_data(B, TXRoot, TXStart, TXData, DataRoot, SizeTaggedTXs, BlockOffset, Option) of
				{ok, POA} ->
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
					end;
				{error, invalid_data_root} ->
					ar:warn([
						{event, invalid_data_root},
						{block, ar_util:encode(B#block.indep_hash)},
						{tx, ar_util:encode(TX#tx.id)}
					]),
					generate(Seed, WeaveSize, BI, Option + 1, Limit);
				{error, invalid_root} ->
					ar:warn([
						{event, invalid_transaction_root},
						{block, ar_util:encode(B#block.indep_hash)},
						{tx, ar_util:encode(TX#tx.id)}
					]),
					generate(Seed, WeaveSize, BI, Option + 1, Limit);
				{error, invalid_tx_size} ->
					ar:warn([
						{event, invalid_transaction_size},
						{block, ar_util:encode(B#block.indep_hash)},
						{tx, ar_util:encode(TX#tx.id)}
					]),
					generate(Seed, WeaveSize, BI, Option + 1, Limit)

			end
	end.

get_tx_data(#tx{ format = 1 } = TX) ->
	TX#tx.data;
get_tx_data(#tx{ format = 2 } = TX) ->
	case ar_storage:read_tx_data(TX) of
		{ok, Data} ->
			Data;
		_ ->
			<<>>
	end.

create_poa_from_data(NoTreeB, TXRoot, TXStart, TXData, DataRoot, SizeTaggedTXs, BlockOffset, Option) ->
	SizeTaggedDataRoots = [{Root, Offset} || {{_TXID, Root}, Offset} <- SizeTaggedTXs],
	B = ar_block:generate_tx_tree(NoTreeB, SizeTaggedDataRoots),
	case B#block.tx_root == TXRoot of
		true ->
			create_poa_from_data(B, TXStart, TXData, DataRoot, BlockOffset, Option);
		false ->
			{error, invalid_root}
	end.

create_poa_from_data(B, TXStart, TXData, DataRoot, BlockOffset, Option) ->
	TXOffset = BlockOffset - TXStart,
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, TXData),
	SizedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	case find_byte_in_size_tagged_list(TXOffset, SizedChunks) of
		{error, not_found} ->
			{error, invalid_tx_size};
		{Chunk, _} ->
			SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizedChunks),
			case ar_merkle:generate_tree(SizedChunkIDs) of
				{DataRoot, DataTree} ->
					ar:info(
						[
							{event, generated_poa},
							{weave_size, B#block.weave_size},
							{block_offset, BlockOffset},
							{tx_offset, TXOffset},
							{tx_start, TXStart},
							{data_root, ar_util:encode(DataRoot)},
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
							DataRoot,
							TXOffset,
							DataTree
						),
					{ok, #poa {
						option = Option,
						tx_path = TXPath,
						data_path = DataPath,
						chunk = Chunk
					}};
				{_, _} ->
					{error, invalid_data_root}
			end
	end.

search(X, [{X, _} | _]) -> 0;
search(X, [_ | R]) -> 1 + search(X, R).

%% @doc Validate a complete proof of access object.
validate(_H, 0, _BI, _POA) ->
	%% The weave does not have data yet.
	true;
validate(_H, _WS, BI, #poa{ option = Option })
		when Option > length(BI) andalso Option > ?MIN_MAX_OPTION_DEPTH ->
	false;
validate(LastIndepHash, WeaveSize, BI, POA) ->
	RecallByte = calculate_challenge_byte(LastIndepHash, WeaveSize, POA#poa.option),
	{TXRoot, BlockBase, BlockTop, _BH} = find_challenge_block(RecallByte, BI),
	validate_tx_path(RecallByte - BlockBase, TXRoot, BlockTop - BlockBase, POA).

calculate_challenge_byte(_, 0, _) -> 0;
calculate_challenge_byte(LastIndepHash, WeaveSize, Option) ->
	binary:decode_unsigned(multihash(LastIndepHash, Option)) rem WeaveSize.

multihash(X, Remaining) when Remaining =< 0 -> X;
multihash(X, Remaining) ->
	multihash(crypto:hash(?HASH_ALG, X), Remaining - 1).

%% @doc The base of the block is the weave_size tag of the _previous_ block.
%% Traverse the block index until the challenge block is inside the block's bounds.
find_challenge_block(Byte, [{BH, BlockTop, TXRoot}, {_, BlockBase, _} | _])
	when (Byte >= BlockBase) andalso (Byte < BlockTop) -> {TXRoot, BlockBase, BlockTop, BH};
find_challenge_block(Byte, [_ | BI]) ->
	find_challenge_block(Byte, BI).

find_byte_in_size_tagged_list(Byte, [{Leaf, End} | _])
		when End > Byte -> {Leaf, End};
find_byte_in_size_tagged_list(Byte, [_ | Rest]) ->
	find_byte_in_size_tagged_list(Byte, Rest);
find_byte_in_size_tagged_list(_Byte, []) ->
	{error, not_found}.

validate_tx_path(BlockOffset, TXRoot, BlockEndOffset, POA) ->
	Validation =
		ar_merkle:validate_path(
			TXRoot,
			BlockOffset,
			BlockEndOffset,
			POA#poa.tx_path
		),
	case Validation of
		false -> false;
		{DataRoot, StartOffset, EndOffset} ->
			TXOffset = BlockOffset - StartOffset,
			validate_data_path(DataRoot, TXOffset, EndOffset - StartOffset, POA)
	end.

validate_data_path(DataRoot, TXOffset, EndOffset, POA) ->
	Validation =
		ar_merkle:validate_path(
			DataRoot,
			TXOffset,
			EndOffset,
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
modify_diff(Diff, 1) ->
	Diff;
modify_diff(Diff, Option) ->
	modify_diff(ar_difficulty:multiply_diff(Diff, ?ALTERNATIVE_POA_DIFF_MULTIPLIER), Option - 1).
