%%% @doc This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.
-module(ar_poa).

-export([get_data_path_validation_ruleset/2, get_data_path_validation_ruleset/3,
		 validate_pre_fork_2_5/4, validate/1, get_padded_offset/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_pricing.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the merkle proof validation ruleset code depending on the block start
%% offset, the threshold where the offset rebases were allowed (and the validation
%% changed in some other ways on top of that). The threshold where the specific
%% requirements were imposed on data splits to make each chunk belong to its own
%% 256 KiB bucket is set to ?STRICT_DATA_SPLIT_THRESHOLD. The code is then passed to
%% ar_merkle:validate_path/5.
get_data_path_validation_ruleset(BlockStartOffset, MerkleRebaseSupportThreshold) ->
	get_data_path_validation_ruleset(BlockStartOffset, MerkleRebaseSupportThreshold,
			?STRICT_DATA_SPLIT_THRESHOLD).

%% @doc Return the merkle proof validation ruleset code depending on the block start
%% offset, the threshold where the offset rebases were allowed (and the validation
%% changed in some other ways on top of that), and the threshold where the specific
%% requirements were imposed on data splits to make each chunk belong to its own
%% 256 KiB bucket. The code is then passed to ar_merkle:validate_path/5.
get_data_path_validation_ruleset(BlockStartOffset, MerkleRebaseSupportThreshold,
		StrictDataSplitThreshold) ->
	case BlockStartOffset >= MerkleRebaseSupportThreshold of
		true ->
			offset_rebase_support_ruleset;
		false ->
			case BlockStartOffset >= StrictDataSplitThreshold of
				true ->
					strict_data_split_ruleset;
				false ->
					strict_borders_ruleset
			end
	end.

get_data_path_validation_ruleset(BlockStartOffset) ->
	get_data_path_validation_ruleset(BlockStartOffset, ?MERKLE_REBASE_SUPPORT_THRESHOLD,
			?STRICT_DATA_SPLIT_THRESHOLD).

%% @doc Validate a proof of access.
validate(Args) ->
	{BlockStartOffset, RecallOffset, TXRoot, BlockSize, SPoA, Packing, SubChunkIndex,
			ExpectedChunkID} = Args,
	#poa{ chunk = Chunk, unpacked_chunk = UnpackedChunk } = SPoA,
	TXPath = SPoA#poa.tx_path,
	RecallBucketOffset = get_recall_bucket_offset(RecallOffset, BlockStartOffset),
	ValidateDataPathRuleset = get_data_path_validation_ruleset(BlockStartOffset),
	case ar_merkle:validate_path(TXRoot, RecallBucketOffset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			TXSize = TXEndOffset - TXStartOffset,
			RecallChunkOffset = RecallBucketOffset - TXStartOffset,
			DataPath = SPoA#poa.data_path,
			case ar_merkle:validate_path(DataRoot, RecallChunkOffset, TXSize, DataPath,
					ValidateDataPathRuleset) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					case ExpectedChunkID of
						not_set ->
							validate2(Packing, {ChunkID, ChunkStartOffset,
									ChunkEndOffset, BlockStartOffset, TXStartOffset,
									TXRoot, Chunk, UnpackedChunk, SubChunkIndex});
						_ ->
							case ChunkID == ExpectedChunkID of
								false ->
									false;
								true ->
									{true, ChunkID}
							end
					end
			end
	end.

get_recall_bucket_offset(RecallOffset, BlockStartOffset) ->
	case RecallOffset >= ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			get_padded_offset(RecallOffset + 1, ?STRICT_DATA_SPLIT_THRESHOLD)
					- (?DATA_CHUNK_SIZE) - BlockStartOffset;
		false ->
			RecallOffset - BlockStartOffset
	end.

validate2({spora_2_6, _} = Packing, Args) ->
	{ChunkID, ChunkStartOffset, ChunkEndOffset, BlockStartOffset, TXStartOffset,
			TXRoot, Chunk, _UnpackedChunk, _SubChunkIndex} = Args,
	ChunkSize = ChunkEndOffset - ChunkStartOffset,
	AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
	prometheus_counter:inc(validating_packed_spora, [ar_packing_server:packing_atom(Packing)]),
	case ar_packing_server:unpack(Packing, AbsoluteEndOffset, TXRoot, Chunk, ChunkSize) of
		{error, _} ->
			false;
		{exception, _} ->
			error;
		{ok, Unpacked} ->
			case ChunkID == ar_tx:generate_chunk_id(Unpacked) of
				false ->
					false;
				true ->
					{true, ChunkID}
			end
	end;
validate2({composite, _, _} = Packing, Args) ->
	{_ChunkID, ChunkStartOffset, ChunkEndOffset, _BlockStartOffset, _TXStartOffset,
			_TXRoot, _Chunk, UnpackedChunk, _SubChunkIndex} = Args,
	ChunkSize = ChunkEndOffset - ChunkStartOffset,
	case ChunkSize > ?DATA_CHUNK_SIZE of
		true ->
			false;
		false ->
			PaddingSize = ?DATA_CHUNK_SIZE - ChunkSize,
			case binary:part(UnpackedChunk, ChunkSize, PaddingSize) of
				<< 0:(PaddingSize * 8) >> ->
					validate3(Packing, Args);
				_ ->
					false
			end
	end.

validate3({composite, _Addr, _PackingDifficulty} = Packing, Args) ->
	{ChunkID, ChunkStartOffset, ChunkEndOffset, BlockStartOffset, TXStartOffset,
			TXRoot, Chunk, UnpackedChunk, SubChunkIndex} = Args,
	AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	SubChunkStartOffset = SubChunkIndex * SubChunkSize,
	%% We always expect the provided unpacked chunks to be padded (if necessary)
	%% to 256 KiB.
	UnpackedSubChunk = binary:part(UnpackedChunk, SubChunkStartOffset, SubChunkSize),
	PackingAtom = ar_packing_server:packing_atom(Packing),
	prometheus_counter:inc(validating_packed_spora, [PackingAtom]),
	case ar_packing_server:unpack_sub_chunk(Packing, AbsoluteEndOffset, TXRoot, Chunk,
			SubChunkStartOffset) of
		{error, _} ->
			false;
		{exception, _} ->
			error;
		{ok, UnpackedSubChunk} ->
			ChunkSize = ChunkEndOffset - ChunkStartOffset,
			UnpackedChunkNoPadding = binary:part(UnpackedChunk, 0, ChunkSize),
			case ChunkID == ar_tx:generate_chunk_id(UnpackedChunkNoPadding) of
				false ->
					false;
				true ->
					{true, ChunkID}
			end;
		{ok, _UnexpectedSubChunk} ->
			false
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
