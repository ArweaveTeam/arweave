%%% @doc This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.
-module(ar_poa).

-export([get_data_path_validation_ruleset/2, get_data_path_validation_ruleset/3,
		 validate_pre_fork_2_5/4, validate/1, validate_paths/4, validate_paths/7,
		 get_padded_offset/1, get_padded_offset/2]).

-include_lib("arweave/include/ar_poa.hrl").
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

	case validate_paths(SPoA, TXRoot, RecallOffset, BlockStartOffset, BlockSize) of
		{false, _} ->
			false;
		{true, ChunkProof} ->
			#chunk_proof{
				chunk_id = ChunkID,
				chunk_start_offset = ChunkStartOffset,
				chunk_end_offset = ChunkEndOffset,
				tx_start_offset = TXStartOffset
			} = ChunkProof,
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
	end.

%% @doc Validate the TXPath and DataPath for a chunk. This will return the ChunkID but won't
%% validate that the ChunkID is correct.
%% 
%% SPoA: the proof of access
%% RecallOffset: the absoluteoffset of the recall byte - 
 validate_paths(#poa{} = SPoA, TXRoot, RecallOffset, BlockStartOffset, BlockSize) ->
	BlockRelativeOffset = get_recall_bucket_offset(RecallOffset, BlockStartOffset),
	ValidateDataPathRuleset = get_data_path_validation_ruleset(BlockStartOffset),

	Proof = #chunk_proof{
		absolute_offset = BlockStartOffset + BlockRelativeOffset,
		tx_root = TXRoot,
		tx_path = SPoA#poa.tx_path,
		data_path = SPoA#poa.data_path,
		block_start_offset = BlockStartOffset,
		block_end_offset = BlockStartOffset + BlockSize,
		validate_data_path_ruleset = ValidateDataPathRuleset
	},
	validate_paths(Proof).

%% @doc Validate the TXPath and DataPath for a chunk. This will return the ChunkID but won't
%% validate that the ChunkID is correct.
validate_paths(TXRoot, TXPath, DataPath, AbsoluteOffset) ->
	{BlockStartOffset, BlockEndOffset, TXRoot} =
		ar_block_index:get_block_bounds(AbsoluteOffset),

	Proof = #chunk_proof{
		absolute_offset = AbsoluteOffset,
		tx_root = TXRoot,
		tx_path = TXPath,
		data_path = DataPath,
		block_start_offset = BlockStartOffset,
		block_end_offset = BlockEndOffset,
		validate_data_path_ruleset = get_data_path_validation_ruleset(BlockStartOffset)
	},
	validate_paths(Proof).

validate_paths(
		TXRoot, TXPath, DataPath, BlockStartOffset, BlockEndOffset, BlockRelativeOffset,
		ValidateDataPathRuleset) ->
	Proof = #chunk_proof{
		absolute_offset = BlockStartOffset + BlockRelativeOffset,
		tx_root = TXRoot,
		tx_path = TXPath,
		data_path = DataPath,
		block_start_offset = BlockStartOffset,
		block_end_offset = BlockEndOffset,
		validate_data_path_ruleset = ValidateDataPathRuleset
	},
	validate_paths(Proof).

validate_paths(Proof) ->
	#chunk_proof{
		absolute_offset = AbsoluteOffset,
		tx_root = TXRoot,
		tx_path = TXPath,
		data_path = DataPath,
		block_start_offset = BlockStartOffset,
		block_end_offset = BlockEndOffset,
		validate_data_path_ruleset = ValidateDataPathRuleset
	} = Proof,

	BlockRelativeOffset = AbsoluteOffset - BlockStartOffset,
	BlockSize = BlockEndOffset - BlockStartOffset,

	case ar_merkle:validate_path(TXRoot, BlockRelativeOffset, BlockSize, TXPath) of
		false ->
			{false, Proof#chunk_proof{ tx_path_is_valid = invalid }};
		{DataRoot, TXStartOffset, TXEndOffset} ->
			Proof2 = Proof#chunk_proof{
				data_root = DataRoot,
				tx_start_offset = TXStartOffset,
				tx_end_offset = TXEndOffset,
				tx_path_is_valid = valid
			},
			TXSize = TXEndOffset - TXStartOffset,
			TXRelativeOffset = BlockRelativeOffset - TXStartOffset,
			case ar_merkle:validate_path(
					DataRoot, TXRelativeOffset, TXSize, DataPath, ValidateDataPathRuleset) of
				false ->
					{false, Proof2#chunk_proof{ data_path_is_valid = invalid }};
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					Proof3 = Proof2#chunk_proof{
						chunk_id = ChunkID,
						chunk_start_offset = ChunkStartOffset,
						chunk_end_offset = ChunkEndOffset,
						data_path_is_valid = valid
					},
					{true, Proof3}
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
validate2(Packing, Args) ->
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

validate3(Packing, Args) ->
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
	case ar_packing_server:unpack_sub_chunk(Packing, AbsoluteEndOffset,
			TXRoot, Chunk, SubChunkStartOffset) of
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

%% @doc Return the smallest multiple of 256 KiB >= Offset
%% counting from ?STRICT_DATA_SPLIT_THRESHOLD.
get_padded_offset(Offset) ->
	get_padded_offset(Offset, ?STRICT_DATA_SPLIT_THRESHOLD).

%% @doc Return the smallest multiple of 256 KiB >= Offset
%% counting from StrictDataSplitThreshold.
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
