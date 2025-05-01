-module(ar_repack_fsm).

-export([crank_state/1]).

-include("ar.hrl").
-include("ar_repack.hrl").
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
	Maintain a finite state machine (FSM) to track the state each chunk passes through as
	it is repacked.

	State Transition Diagram:

	needs_chunk
		|
		+----> invalid ----------> entropy_only 
		|
		+----> entropy_only
		|		|
		|		+----> write_entropy (terminal)
		|		|
		|		+----> ignore
		|
		+----> already_repacked -> ignore (terminal)
		|
		+----> needs_data_path --> has_chunk
		|
		+----> has_chunk
				|
				+----> write_chunk (terminal)
				|
				+----> needs_repack ---------------------------> has_chunk
				|
				+----> needs_source_entropy -> needs_decipher -> has_chunk
				|
				+----> needs_target_entropy -> needs_encipher -> has_chunk

	Start State: needs_chunk
	Terminal States: write_chunk, write_entropy, ignore

	State Descriptions:
	- needs_chunk: Initial state, waiting to read chunk data and metadata
	- entropy_only: Chunk is too small or not found, only entropy will be recorded
	- invalid: Chunk is corrupt or inconsistent, will be invalidated
	- already_repacked: Chunk is already in target format
	- needs_data_path: Chunk not found on disk, checking chunk data db
	- has_chunk: Chunk has been read, decide what to do next
	- needs_repack: Repack between non-replica_2_9 formats
	- needs_source_entropy: Waiting for source entropy to be calculated
	- needs_decipher: Waiting for chunk to be deciphered from replica_2_9 to unpacked_padded
	- needs_target_entropy: Waiting for target entropy to be calculated
	- needs_encipher: Waiting for chunk to be enciphered from unpacked_padded to replica_2_9
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
		AbsoluteEndOffset =< ar_block:strict_data_split_threshold()
	),

	IsStorageSupported = ar_chunk_storage:is_storage_supported(
		AbsoluteEndOffset, ChunkSize, TargetPacking),

	NextState = case {IsTooSmall, SourcePacking, Chunk, IsStorageSupported} of
		{true, _, _, _} -> entropy_only;
		{_, not_found, _, _} ->
			%% This offset exists in some of the chunk indices, the chunk is not recorded
			%% in the sync record. This can happen if there was some corruption at some
			%% point in the past. We'll clean out the bad indices, and then record
			%% the entropy.
			invalid;
		{_, TargetPacking, _, _} -> already_repacked;
		{_, _, not_found, _} ->
			%% Chunk doesn't exist on disk, try chunk data db.
			needs_data_path;
		{_, _, _, false} -> 
			%% We are going to move this chunk to RocksDB after repacking so
			%% we read its DataPath here to pass it later on to store_chunk.
			needs_data_path;
		_ -> has_chunk
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
		metadata = Metadata
	} = RepackChunk,
	#chunk_metadata{
		data_path = DataPath
	} = Metadata,

	IsInvalid = (
		Chunk == not_found orelse
		DataPath == not_found
	),

	NextState = case IsInvalid of
		true -> 
			%% This offset exists in some of the chunk indices and sync records, but there's
			%% either no chunk data or no data_path. This can happen if there was some
			%% corruption at some point in the past. We'll clean out the bad indices, and
			%% then record the entropy.
			invalid;
		_ -> has_chunk
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: has_chunk
%% 
%% has_chunk is an intermediate state to avoid duplicating state transition
%% logic across both the needs_chunk and needs_data_path states. Once a chunk
%% and optionally data_path have been read, we'll transition to has_chunk and
%% then from there enter the repack logic.
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = has_chunk} = RepackChunk) ->
	#repack_chunk{
		source_packing = SourcePacking,
		target_packing = TargetPacking
	} = RepackChunk,

	NextState = case {SourcePacking, TargetPacking} of
		_ when SourcePacking == TargetPacking ->
			write_chunk;
		{{replica_2_9, _}, _} ->
			%% Source is replica_2_9, so we need its entropy first before we can unpack it.
			needs_source_entropy;
		{unpacked_padded, {replica_2_9, _}} ->
			%% When source_packing is unpacked_padded it means that the chunk was originally
			%% some other format, but has now been repacked to unpacked_padded and is ready
			%% to be enciphered to the target_packing replica_2_9 format. Before we can do
			%% that we need to wait for the target entropy to be generated.
			needs_target_entropy;
		_ -> 
			%% Source packing is either unpacked or spora_2_6, so the next step is to repack
			%% it. Whether we repack to unpacked_padded or to some other format depends on
			%% the current source and target packing. The logic to determine what to repack
			%% to is handled by ar_repack.erl.
			needs_repack
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
		target_packing = TargetPacking
	} = RepackChunk,

	NextState = case {has_all_entropy(RepackChunk), TargetPacking} of
		{false, _} -> 
			%% Still waiting on entropy.
			entropy_only;
		{true, {replica_2_9, _}} ->
			%% We don't have a record of this chunk anywhere, so we'll record and
			%% index the entropy 
			write_entropy;
		{true, _} ->
			%% We have a record of this chunk, so we'll ignore it.
			ignore
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: already_repacked
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = already_repacked} = RepackChunk) ->
	NextState = case has_all_entropy(RepackChunk) of
		false -> 
			%% Still waiting on entropy.
			already_repacked;
		true ->
			%% Repacked chunk already exists on disk so don't write anything
			%% (neither entropy nor chunk)
			ignore
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_repack
%% 
%% Source chunk will be repacked directly to:
%% - TargetPacking if TargetPacking is not replica_2_9
%% - unpacked_padded if TargetPacking is replica_2_9
%% 
%% Note when TargetPacking is replica_2_9 we need to generate entropy and then
%% encipher the chunk rather than doing a direct repack.
%% 
%% When we detect one of those condition, transition to has_chunk which will
%% determine what state to transition to next.
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_repack} = RepackChunk) ->
	#repack_chunk{
		source_packing = SourcePacking,
		target_packing = TargetPacking
	} = RepackChunk,

	NextState = case {SourcePacking, TargetPacking} of
		{TargetPacking, _} -> has_chunk;
		{unpacked_padded, {replica_2_9, _}} -> has_chunk;
		_ ->
			%% Still waiting on repacking.
			needs_repack
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_source_entropy
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_source_entropy} = RepackChunk) ->
	#repack_chunk{
		source_entropy = SourceEntropy
	} = RepackChunk,

	NextState = case SourceEntropy of
		not_set -> 
			%% Still waiting on entropy.
			needs_source_entropy;
		_ ->
			%% sanity checks
			true = SourceEntropy /= <<>>,
			%% end sanity checks
			needs_decipher
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_target_entropy
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_target_entropy} = RepackChunk) ->
	#repack_chunk{
		target_entropy = TargetEntropy
	} = RepackChunk,

	NextState = case TargetEntropy of
		not_set -> 
			%% Still waiting on entropy.
			needs_target_entropy;
		_ ->
			%% We now have the unpacked_padded chunk and the entropy, proceed
			%% with enciphering and storing the chunk.

			%% sanity checks
			true = TargetEntropy /= <<>>,
			true = RepackChunk#repack_chunk.chunk /= not_found,
			true = RepackChunk#repack_chunk.chunk /= not_set,
			true = RepackChunk#repack_chunk.source_packing == unpacked_padded,
			%% end sanity checks
			needs_encipher
	end,
	RepackChunk#repack_chunk{state = NextState};

%% ---------------------------------------------------------------------------
%% State: needs_decipher
%% ---------------------------------------------------------------------------
next_state(#repack_chunk{state = needs_decipher} = RepackChunk) ->
	#repack_chunk{
		source_packing = SourcePacking
	} = RepackChunk,

	NextState = case SourcePacking of
		unpacked_padded -> has_chunk;
		_ ->
			%% Still waiting on deciphering.
			needs_decipher
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
		true -> has_chunk;
		_  ->
			%% Still waiting on enciphering.
			needs_encipher
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

has_all_entropy(RepackChunk) ->
	#repack_chunk{
		source_entropy = SourceEntropy,
		target_entropy = TargetEntropy
	} = RepackChunk,
	SourceEntropy /= not_set andalso TargetEntropy /= not_set.

log_error(Event, #repack_chunk{} = RepackChunk, ExtraLogs) ->
	?LOG_ERROR(format_logs(Event, RepackChunk, ExtraLogs)).

log_debug(Event, #repack_chunk{} = RepackChunk, ExtraLogs) ->
	?LOG_DEBUG(format_logs(Event, RepackChunk, ExtraLogs)).

format_logs(Event, #repack_chunk{} = RepackChunk, ExtraLogs) ->
	#repack_chunk{
		offsets = Offsets,
		metadata = Metadata,
		state = ChunkState,
		source_entropy = SourceEntropy,
		target_entropy = TargetEntropy,
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
		_ -> {not_set, not_set}
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
		{source_entropy, atom_or_binary(SourceEntropy)},
		{target_entropy, atom_or_binary(TargetEntropy)},
		{data_path, atom_or_binary(DataPath)}
		| ExtraLogs
	].


atom_or_binary(Atom) when is_atom(Atom) -> Atom;
atom_or_binary(Bin) when is_binary(Bin) -> binary:part(Bin, {0, min(10, byte_size(Bin))}).

%%%===================================================================
%%% Tests.
%%%===================================================================

state_transition_test_() ->
	[
		ar_test_node:test_with_mocked_functions([
			{ar_block, strict_data_split_threshold, fun() -> 700_000 end}
		],
		fun test_state_transitions/0, 30)
	].

test_state_transitions() ->
	Addr1 = crypto:strong_rand_bytes(32),
	Addr2 = crypto:strong_rand_bytes(32),
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),	
	Entropy1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Entropy2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),

	%% ---------------------------------------------------------------------------
	%% needs_chunk
	%% ---------------------------------------------------------------------------
	?assertEqual(needs_chunk, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = not_set,
		metadata = not_set
	}))#repack_chunk.state),

	?assertEqual(entropy_only, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = not_found,
		metadata = not_found
	}))#repack_chunk.state),

	?assertEqual(error, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = not_set,
		offsets = #chunk_offsets{}
	}))#repack_chunk.state),

	?assertEqual(entropy_only, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = 100},
		offsets = #chunk_offsets{absolute_offset = 100},
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(invalid, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = not_found,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(already_repacked, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_data_path, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = not_found,
		metadata = #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_data_path, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = 100},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = {replica_2_9, Addr1},
		target_packing = unpacked
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = {replica_2_9, Addr2},
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_chunk,
		chunk = Chunk,
		metadata = #chunk_metadata{chunk_size = ?DATA_CHUNK_SIZE},
		offsets = #chunk_offsets{absolute_offset = 1000000},
		source_packing = {replica_2_9, Addr1},
		target_packing = unpacked
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% needs_data_path
	%% ---------------------------------------------------------------------------
	?assertEqual(needs_data_path, (next_state(#repack_chunk{
		state = needs_data_path,
		metadata = #chunk_metadata{data_path = not_set}
	}))#repack_chunk.state),

	?assertEqual(invalid, (next_state(#repack_chunk{
		state = needs_data_path,
		chunk = not_found,
		metadata = #chunk_metadata{data_path = <<"path">>}
	}))#repack_chunk.state),

	?assertEqual(invalid, (next_state(#repack_chunk{
		state = needs_data_path,
		chunk = Chunk,
		metadata = #chunk_metadata{data_path = not_found}
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_data_path,
		chunk = Chunk,
		metadata = #chunk_metadata{data_path = <<"path">>},
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_data_path,
		chunk = Chunk,
		metadata = #chunk_metadata{data_path = <<"path">>},
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr2}
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_data_path,
		chunk = Chunk,
		metadata = #chunk_metadata{data_path = <<"path">>},
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% has_chunk
	%% ---------------------------------------------------------------------------
	?assertEqual(write_chunk, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_source_entropy, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr2}
	}))#repack_chunk.state),

	?assertEqual(needs_source_entropy, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = {replica_2_9, Addr1},
		target_packing = unpacked
	}))#repack_chunk.state),

	?assertEqual(needs_target_entropy, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = unpacked_padded,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_repack, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_repack, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = unpacked_padded,
		target_packing = unpacked
	}))#repack_chunk.state),

	?assertEqual(needs_repack, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = {spora_2_6, Addr1},
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_repack, (next_state(#repack_chunk{
		state = has_chunk,
		source_packing = {spora_2_6, Addr1},
		target_packing = unpacked
	}))#repack_chunk.state),
	
	%% ---------------------------------------------------------------------------
	%% invalid
	%% ---------------------------------------------------------------------------
	?assertEqual(entropy_only, (next_state(#repack_chunk{
		state = invalid,
		chunk = invalid
	}))#repack_chunk.state),

	?assertEqual(invalid, (next_state(#repack_chunk{
		state = invalid,
		chunk = Chunk
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% entropy_only
	%% ---------------------------------------------------------------------------
	?assertEqual(entropy_only, (next_state(#repack_chunk{
		state = entropy_only
	}))#repack_chunk.state),

	?assertEqual(entropy_only, (next_state(#repack_chunk{
		state = entropy_only,
		target_entropy = Entropy1
	}))#repack_chunk.state),

	?assertEqual(entropy_only, (next_state(#repack_chunk{
		state = entropy_only,
		source_entropy = Entropy1
	}))#repack_chunk.state),

	?assertEqual(write_entropy, (next_state(#repack_chunk{
		state = entropy_only,
		source_entropy = <<>>,
		target_entropy = Entropy1,
		target_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(ignore, (next_state(#repack_chunk{
		state = entropy_only,
		source_entropy = Entropy1,
		target_entropy = <<>>,
		target_packing = unpacked
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% already_repacked
	%% ---------------------------------------------------------------------------
	?assertEqual(already_repacked, (next_state(#repack_chunk{
		state = already_repacked,
		target_entropy = Entropy1
	}))#repack_chunk.state),

	?assertEqual(already_repacked, (next_state(#repack_chunk{
		state = already_repacked,
		source_entropy = Entropy1
	}))#repack_chunk.state),

	?assertEqual(ignore, (next_state(#repack_chunk{
		state = already_repacked,
		source_entropy = <<>>,
		target_entropy = Entropy2
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% needs_repack
	%% ---------------------------------------------------------------------------
	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_repack,
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr1},
		chunk = Chunk
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_repack,
		source_packing = unpacked,
		target_packing = unpacked,
		chunk = Chunk
	}))#repack_chunk.state),

	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_repack,
		source_packing = unpacked_padded,
		target_packing = {replica_2_9, Addr1},
		chunk = Chunk
	}))#repack_chunk.state),

	?assertEqual(needs_repack, (next_state(#repack_chunk{
		state = needs_repack,
		source_packing = unpacked,
		target_packing = {replica_2_9, Addr1},
		chunk = Chunk
	}))#repack_chunk.state),

	?assertEqual(needs_repack, (next_state(#repack_chunk{
		state = needs_repack,
		source_packing = {spora_2_6, Addr1},
		target_packing = unpacked,
		chunk = Chunk
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% needs_source_entropy
	%% ---------------------------------------------------------------------------
	?assertEqual(needs_source_entropy, (next_state(#repack_chunk{
		state = needs_source_entropy,
		chunk = Chunk,
		source_packing = {replica_2_9, Addr1}
	}))#repack_chunk.state),

	?assertEqual(needs_decipher, (next_state(#repack_chunk{
		state = needs_source_entropy,
		chunk = Chunk,
		source_packing = {replica_2_9, Addr1},
		source_entropy = Entropy1
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% needs_target_entropy
	%% ---------------------------------------------------------------------------
	?assertEqual(needs_target_entropy, (next_state(#repack_chunk{
		state = needs_target_entropy,
		target_entropy = not_set,
		chunk = Chunk,
		source_packing = unpacked_padded
	}))#repack_chunk.state),

	?assertEqual(needs_encipher, (next_state(#repack_chunk{
		state = needs_target_entropy,
		target_entropy = Entropy1,
		chunk = Chunk,
		source_packing = unpacked_padded
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% needs_decipher
	%% ---------------------------------------------------------------------------
	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_decipher,
		source_packing = unpacked_padded,
		target_packing = {replica_2_9, Addr1},
		chunk = Chunk
	}))#repack_chunk.state),

	?assertEqual(needs_decipher, (next_state(#repack_chunk{
		state = needs_decipher,
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr2},
		chunk = Chunk
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% needs_encipher
	%% ---------------------------------------------------------------------------
	?assertEqual(has_chunk, (next_state(#repack_chunk{
		state = needs_encipher,
		source_packing = {replica_2_9, Addr1},
		target_packing = {replica_2_9, Addr1},
		chunk = Chunk
	}))#repack_chunk.state),

	?assertEqual(needs_encipher, (next_state(#repack_chunk{
		state = needs_encipher,
		source_packing = unpacked_padded,
		target_packing = {replica_2_9, Addr1},
		chunk = Chunk
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% write_chunk
	%% ---------------------------------------------------------------------------
	?assertEqual(write_chunk, (next_state(#repack_chunk{
		state = write_chunk,
		chunk = Chunk
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% write_entropy
	%% ---------------------------------------------------------------------------
	?assertEqual(write_entropy, (next_state(#repack_chunk{
		state = write_entropy,
		target_entropy = Entropy1
	}))#repack_chunk.state),

	%% ---------------------------------------------------------------------------
	%% ignore
	%% ---------------------------------------------------------------------------
	?assertEqual(ignore, (next_state(#repack_chunk{
		state = ignore,
		chunk = Chunk
	}))#repack_chunk.state),

	ok.

