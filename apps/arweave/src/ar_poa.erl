%%% @doc This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.
-module(ar_poa).

-export([validate_pre_fork_2_5/4, validate/1, get_padded_offset/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Validate a proof of access.
validate(Args) ->
	{BlockStartOffset, RecallOffset, TXRoot, BlockSize, SPoA, StrictDataSplitThreshold,
			Packing} = Args,
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
					case Packing of
						{spora_2_6, _} ->
							prometheus_counter:inc(validating_packed_2_6_spora);
						spora_2_5 ->
							prometheus_counter:inc(validating_packed_spora)
					end,
					case ar_packing_server:unpack(Packing, AbsoluteEndOffset, TXRoot, Chunk,
							ChunkSize) of
						{error, _} ->
							false;
						{exception, _} ->
							error;
						{ok, Unpacked} ->
							ChunkID == ar_tx:generate_chunk_id(Unpacked)
					end
			end
	end.

%% @doc Return the smallest multiple of 256 KiB counting from StrictDataSplitThreshold
%% bigger than or equal to Offset.
get_padded_offset(Offset, StrictDataSplitThreshold) ->
	Diff = Offset - StrictDataSplitThreshold,
	StrictDataSplitThreshold + ((Diff - 1) div (?DATA_CHUNK_SIZE) + 1) * (?DATA_CHUNK_SIZE).

%% @doc Validate a proof of access.
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

%%%===================================================================
%%% Private functions.
%%%===================================================================

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
