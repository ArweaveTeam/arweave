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

%% @doc Validate a proof of access.
validate(Args) ->
	{BlockStartOffset, RecallOffset, TXRoot, BlockSize, SPoA, Packing, ExpectedChunkID} = Args,
	#poa{ chunk = Chunk } = SPoA,
	StrictDataSplitThreshold = ?STRICT_DATA_SPLIT_THRESHOLD,
	MerkleRebaseSupportThreshold = ?MERKLE_REBASE_SUPPORT_THRESHOLD,
	TXPath = SPoA#poa.tx_path,
	RecallBucketOffset =
		case RecallOffset >= StrictDataSplitThreshold of
			true ->
				get_padded_offset(RecallOffset + 1, StrictDataSplitThreshold)
						- (?DATA_CHUNK_SIZE) - BlockStartOffset;
			false ->
				RecallOffset - BlockStartOffset
		end,
	ValidateDataPathRuleset = get_data_path_validation_ruleset(BlockStartOffset,
			MerkleRebaseSupportThreshold, StrictDataSplitThreshold),
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
							ChunkSize = ChunkEndOffset - ChunkStartOffset,
							AbsoluteEndOffset = BlockStartOffset + TXStartOffset
									+ ChunkEndOffset,
							prometheus_counter:inc(
								validating_packed_spora,
								[ar_packing_server:packing_atom(Packing)]),
							case ar_packing_server:unpack(Packing, AbsoluteEndOffset, TXRoot,
									Chunk, ChunkSize) of
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
