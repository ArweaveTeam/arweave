-module(ar_repack_fsm).

-export([crank_state/1]).

-include("ar.hrl").
-include("ar_consensus.hrl").
-include("ar_repack.hrl").

-moduledoc """
	Maintain a finite state machine (FSM) to track the state each chunk passes through as
	it is repacked.

	State Transition Diagram:
	```
	needs_chunk
		|
		+----> entropy_only --------> write_entropy (terminal)
		|
		+----> invalid -------------> entropy_only
		|
		+----> already_repacked ----> ignore (terminal)
		|
		+----> needs_data_path
				|
				+----> invalid
				|
				+----> already_repacked
				|
				+----> needs_repack
						|
						+----> needs_entropy
								|
								+----> needs_encipher
										|
										+----> write_chunk (terminal)
	```

	Start State: needs_chunk
	Terminal States: write_chunk, write_entropy, ignore

	State Descriptions:
	- needs_chunk: Initial state, waiting to read chunk data and metadata
	- entropy_only: Chunk is too small or not found, only entropy will be recorded
	- invalid: Chunk is corrupt or inconsistent, will be invalidated
	- needs_data_path: Chunk not found on disk, checking chunk data db
	- already_repacked: Chunk is already in target format
	- needs_repack: Waiting for chunk to be repacked
	- needs_entropy: Waiting for entropy to be calculated
	- needs_encipher: Waiting for chunk to be enciphered
	- write_chunk: Terminal state, chunk will be written
	- write_entropy: Terminal state, only entropy will be written
	- ignore: Terminal state, no action needed
""".

%% @doc: Repeatedly call next_state until the state no longer changes.
-spec crank_state(#repack_chunk{}) -> #repack_chunk{}.
crank_state(RepackChunk) ->
	crank_state(RepackChunk, next_state(RepackChunk)).

crank_state(RepackChunk, RepackChunk) ->
	%% State did not change, return the final state
	RepackChunk;
crank_state(_OldRepackChunk, NewRepackChunk) ->
	%% State has changed, continue cranking
	crank_state(NewRepackChunk, next_state(NewRepackChunk)).

%% ---------------------------------------------------------------------------
%% State: needs_chunk
%% ---------------------------------------------------------------------------
next_state(
		#repack_chunk{
			state = needs_chunk, chunk = not_set, metadata = not_set
		} = RepackChunk) ->
	RepackChunk;
next_state(
		#repack_chunk{
			state = needs_chunk, chunk = not_found, metadata = not_found
		} = RepackChunk) ->
	%% Chunk is not recorded in any index.
	NextState = entropy_only,
	RepackChunk#repack_chunk{state = NextState};
next_state(#repack_chunk{ state = needs_chunk, metadata = Metadata } = RepackChunk) 
		when Metadata == not_set orelse Metadata == not_found ->
	%% Metadata can not be empty unless chunk is also empty.
	log_error(invalid_repack_fsm_transition, RepackChunk, []),
	RepackChunk#repack_chunk{state = error};
next_state(#repack_chunk{state = needs_chunk} = RepackChunk) ->
	#repack_chunk{
		offsets = Offsets,
		metadata = Metadata,
		chunk = Chunk,
		source_packing = SourcePacking,
		target_packing = TargetPacking
	} = RepackChunk,
	#chunk_metadata{
		chunk_size = ChunkSize
	} = Metadata,
	#chunk_offsets{
		absolute_offset = AbsoluteEndOffset
	} = Offsets,

	IsTooSmall = (
		ChunkSize /= ?DATA_CHUNK_SIZE andalso
		AbsoluteEndOffset =< ?STRICT_DATA_SPLIT_THRESHOLD
	),

	IsStorageSupported = ar_chunk_storage:is_storage_supported(
		AbsoluteEndOffset, ChunkSize, TargetPacking),

	NextState = case {IsTooSmall, SourcePacking, Chunk, IsStorageSupported} of
		{true, _, _, _} -> entropy_only;
		{false, not_found, _, _} ->
			%% This offset exists in some of the chunk indices, the chunk is not recorded
			%% in the sync record. This can happen if there was some corruption at some
			%% point in the past. We'll clean out the bad indices, and then record
			%% the entropy.
			invalid;
		{false, TargetPacking, _, _} -> already_repacked;
		{false, _, not_found, _} ->
			%% Chunk doesn't exist on disk, try chunk data db.
			needs_data_path;
		{false, _, _, false} -> 
			%% We are going to move this chunk to RocksDB after repacking so
			%% we read its DataPath here to pass it later on to store_chunk.
			needs_data_path;
		{false, _, _, true} -> needs_repack;
		_ ->
			log_error(invalid_repack_fsm_transition, RepackChunk, []),
			error
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: invalid
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = invalid} = RepackChunk) ->
	#repack_chunk{
		chunk = Chunk
	} = RepackChunk,

	NextState = case Chunk of
		invalid ->
			%% Chunk is already invalid, ready to write entropy.
			entropy_only;
		_ ->
			%% Offset has not yet been invalidated.
			invalid
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: entropy_only
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = entropy_only} = RepackChunk) ->
	#repack_chunk{
		entropy = Entropy
	} = RepackChunk,

	NextState = case Entropy of
		not_set -> 
			%% Still waiting on entropy.
			entropy_only;
		_ ->
			%% We don't have a record of this chunk anywhere, so we'll record and
			%% index the entropy 
			write_entropy
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: already_repacked
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = already_repacked} = RepackChunk) ->
	#repack_chunk{
		entropy = Entropy
	} = RepackChunk,

	NextState = case Entropy of
		not_set -> 
			%% Still waiting on entropy.
			already_repacked;
		_ ->
			%% Repacked chunk already exists on disk so don't write anything
			%% (neither entropy nor chunk)
			ignore
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_data_path
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{
		state = needs_data_path,
		metadata = #chunk_metadata{data_path = not_set}} = RepackChunk) ->
	%% Still waiting on data path.
	RepackChunk;
next_state(#repack_chunk{state = needs_data_path} = RepackChunk) ->
	#repack_chunk{
		chunk = Chunk,
		metadata = Metadata,
		source_packing = SourcePacking,
		target_packing = TargetPacking
	} = RepackChunk,
	#chunk_metadata{
		data_path = DataPath
	} = Metadata,

	NextState = case {Chunk, DataPath, SourcePacking} of
		{not_found, _, _} -> 
			%% This offset exists in some of the chunk indices and sync records, but there's
			%% no chunk data.. This can happen if there was some corruption at some
			%% point in the past. We'll clean out the bad indices, and then record
			%% the entropy.
			invalid;
		{Chunk, not_found, _} -> 
			%% This offset exists in some of the chunk indices and sync records, but there's
			%% no data_path. This can happen if there was some corruption at some
			%% point in the past. We'll clean out the bad indices, and then record
			%% the entropy.
			invalid;
		{Chunk, DataPath, TargetPacking} -> already_repacked;
		{Chunk, DataPath, SourcePacking} -> needs_repack;
		_ ->
			log_error(invalid_repack_fsm_transition, RepackChunk, []),
			error
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_repack
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_repack} = RepackChunk) ->
	#repack_chunk{
		source_packing = SourcePacking
	} = RepackChunk,

	NextState = case SourcePacking of
		unpacked_padded -> needs_entropy;
		_ ->
			%% Still waiting on repacking.
			needs_repack
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_entropy
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_entropy} = RepackChunk) ->
	#repack_chunk{
		entropy = Entropy
	} = RepackChunk,

	NextState = case Entropy of
		not_set -> 
			%% Still waiting on entropy.
			needs_entropy;
		_ ->
			%% We now have the unpacked_padded chunk and the entropy, proceed
			%% with enciphering and storing the chunk.

			%% sanity checks
			true = RepackChunk#repack_chunk.chunk /= not_found,
			true = RepackChunk#repack_chunk.chunk /= not_set,
			true = RepackChunk#repack_chunk.source_packing == unpacked_padded,
			%% end sanity checks
			needs_encipher
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_encipher
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_encipher} = RepackChunk) ->
	#repack_chunk{
		source_packing = SourcePacking,
		target_packing = TargetPacking
	} = RepackChunk,

	%% sanity checks
	true = RepackChunk#repack_chunk.chunk /= not_found,
	true = RepackChunk#repack_chunk.chunk /= not_set,
	%% end sanity checks
	
	IsRepacked = SourcePacking == TargetPacking,

	NextState = case IsRepacked of
		true -> write_chunk;
		_  -> needs_encipher
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: write_chunk
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = write_chunk} = RepackChunk) ->
	%% write_chunk is a terminal state.
	RepackChunk;
%% ---------------------------------------------------------------------------
%% State: write_entropy
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = write_entropy} = RepackChunk) ->
	%% write_entropy is a terminal state.
	RepackChunk;
%% ---------------------------------------------------------------------------
%% State: ignore
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = ignore} = RepackChunk) ->
	%% ignore is a terminal state.
	RepackChunk;
next_state(RepackChunk) ->
	log_error(invalid_repack_fsm_transition, RepackChunk, []),
	RepackChunk.


log_error(Event, #repack_chunk{} = RepackChunk, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, RepackChunk, ExtraLogs)).

log_debug(Event, #repack_chunk{} = RepackChunk, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, RepackChunk, ExtraLogs)).

format_logs(Event, #repack_chunk{} = RepackChunk, ExtraLogs) ->
	#repack_chunk{
		offsets = Offsets,
		metadata = Metadata,
		state = ChunkState,
		entropy = Entropy,
		source_packing = SourcePacking,
		target_packing = TargetPacking,
		chunk = Chunk
	} = RepackChunk,
	#chunk_offsets{
		absolute_offset = AbsoluteOffset,
		padded_end_offset = PaddedEndOffset,
		bucket_end_offset = BucketEndOffset
	} = Offsets,
	{ChunkSize, DataPath} = case Metadata of
		#chunk_metadata{chunk_size = Size, data_path = Path} -> {Size, Path};
		_ -> Metadata
	end,
	[
		{event, Event},
		{state, ChunkState},
		{bucket_end_offset, BucketEndOffset},
		{absolute_offset, AbsoluteOffset},
		{padded_end_offset, PaddedEndOffset},
		{chunk_size, ChunkSize},
		{source_packing, ar_serialize:encode_packing(SourcePacking, false)},
		{target_packing, ar_serialize:encode_packing(TargetPacking, false)},
		{chunk, atom_or_binary(Chunk)},
		{entropy, atom_or_binary(Entropy)},
		{data_path, atom_or_binary(DataPath)}
		| ExtraLogs
	].


atom_or_binary(Atom) when is_atom(Atom) -> Atom;
atom_or_binary(Bin) when is_binary(Bin) -> binary:part(Bin, {0, min(10, byte_size(Bin))}).

