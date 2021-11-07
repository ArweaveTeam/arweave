%%% @doc This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.
%%% @end
-module(ar_poa).

-export([validate_pre_fork_2_4/4, validate_pre_fork_2_5/3, validate/4, get_padded_offset/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

-define(MIN_MAX_OPTION_DEPTH, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Validate a proof of access.
validate(RecallByte, BI, SPoA, StrictDataSplitThreshold) ->
	{TXRoot, BlockBase, BlockTop, _BH} = find_challenge_block(RecallByte, BI),
	validate(BlockBase, RecallByte, TXRoot, BlockTop - BlockBase, SPoA,
			StrictDataSplitThreshold).

%% @doc Return the smallest multiple of 256 KiB counting from StrictDataSplitThreshold
%% bigger than or equal to Offset.
get_padded_offset(Offset, StrictDataSplitThreshold) ->
	Diff = Offset - StrictDataSplitThreshold,
	StrictDataSplitThreshold + ((Diff - 1) div (?DATA_CHUNK_SIZE) + 1) * (?DATA_CHUNK_SIZE).

%% @doc Validate a proof of access.
validate_pre_fork_2_5(RecallByte, BI, POA) ->
	{TXRoot, BlockBase, BlockTop, _BH} = find_challenge_block(RecallByte, BI),
	validate_pre_fork_2_5(RecallByte - BlockBase, TXRoot, BlockTop - BlockBase, POA).

%% @doc Validate a proof of access.
validate_pre_fork_2_4(_H, 0, _BI, _POA) ->
	%% The weave does not have data yet.
	true;
validate_pre_fork_2_4(_H, _WS, BI, #poa{ option = Option })
		when Option > length(BI) andalso Option > ?MIN_MAX_OPTION_DEPTH ->
	false;
validate_pre_fork_2_4(LastIndepHash, WeaveSize, BI, POA) ->
	RecallByte = calculate_challenge_byte_pre_fork_2_4(LastIndepHash, WeaveSize, POA#poa.option),
	validate_pre_fork_2_5(RecallByte, BI, POA).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc The base of the block is the weave_size tag of the _previous_ block.
%% Traverse the block index until the challenge block is inside the block's bounds.
%% @end
find_challenge_block(Byte, [{BH, BlockTop, TXRoot}]) when Byte < BlockTop ->
	{TXRoot, 0, BlockTop, BH};
find_challenge_block(Byte, [{BH, BlockTop, TXRoot}, {_, BlockBase, _} | _])
	when (Byte >= BlockBase) andalso (Byte < BlockTop) -> {TXRoot, BlockBase, BlockTop, BH};
find_challenge_block(Byte, [_ | BI]) ->
	find_challenge_block(Byte, BI).

validate(BlockStartOffset, RecallOffset, TXRoot, BlockSize, SPoA, StrictDataSplitThreshold) ->
	#poa{ chunk = Chunk } = SPoA,
	TXPath = SPoA#poa.tx_path,
	RecallBucketOffset =
		case RecallOffset >= StrictDataSplitThreshold of
			true ->
				get_padded_offset(RecallOffset + 1, StrictDataSplitThreshold)
						- (?DATA_CHUNK_SIZE) - BlockStartOffset;
			false ->
				RecallOffset - BlockStartOffset
		end,
	ValidateDataPathFun =
		case BlockStartOffset >= StrictDataSplitThreshold of
			true ->
				fun ar_merkle:validate_path_strict_data_split/4;
			false ->
				fun ar_merkle:validate_path_strict_borders/4
		end,
	case ar_merkle:validate_path(TXRoot, RecallBucketOffset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			TXSize = TXEndOffset - TXStartOffset,
			RecallChunkOffset = RecallBucketOffset - TXStartOffset,
			DataPath = SPoA#poa.data_path,
			case ValidateDataPathFun(DataRoot, RecallChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					ChunkSize = ChunkEndOffset - ChunkStartOffset,
					AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
					case ar_packing_server:unpack(spora_2_5, AbsoluteEndOffset, TXRoot, Chunk,
							ChunkSize) of
						{error, invalid_packed_size} ->
							false;
						{ok, Unpacked} ->
							ChunkID == ar_tx:generate_chunk_id(Unpacked)
					end
			end
	end.

validate_pre_fork_2_5(BlockOffset, TXRoot, BlockEndOffset, POA) ->
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

calculate_challenge_byte_pre_fork_2_4(_, 0, _) -> 0;
calculate_challenge_byte_pre_fork_2_4(LastIndepHash, WeaveSize, Option) ->
	binary:decode_unsigned(multihash(LastIndepHash, Option)) rem WeaveSize.

multihash(X, Remaining) when Remaining =< 0 -> X;
multihash(X, Remaining) ->
	multihash(crypto:hash(?HASH_ALG, X), Remaining - 1).
