%%% @doc Read-range worker for `ar_chunk_copy'. One process per task, spawned
%%% via `spawn_monitor'. It reads chunks from the source module's sync record
%%% and posts `{pack_and_store_chunk, ...}' casts to the target `ar_data_sync',
%%% which does the actual storage writes; this worker only reads.
%%%
%%% Exit contract (read by `ar_chunk_copy''s `'DOWN'' handler): `normal' means
%%% the range is fully processed, anything else is a crash and is logged.
%%%
%%% Backpressure (chunk cache full, target disk full, per-batch yield) is
%%% handled by sleeping and re-checking; the worker stays alive across waits
%%% rather than releasing the source, which a 1:1 source/target relationship
%%% wouldn't free for anyone else anyway.
-module(ar_chunk_copy_worker).

-export([run/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%% `pack_and_store_chunk' casts to `ar_data_sync' before the worker yields so
%% other tasks can run. Each cast carries at least one 256 KiB chunk; with
%% multiple sources draining in parallel, unbounded posting can exhaust memory.
-define(READ_RANGE_MESSAGES_PER_BATCH, 40).

%%%===================================================================
%%% Entry point.
%%%===================================================================

%% @doc Entry point for a transient read-range worker. Returns `ok' (exiting
%% the process `normal') when the range is fully processed.
run(Args) ->
	do_run(Args).

%%%===================================================================
%%% Internal.
%%%===================================================================

do_run({Start, End, _, _}) when Start >= End ->
	ok;
do_run({Start, End, _, TargetStoreID} = Args) ->
	case ar_data_sync:is_chunk_cache_full() of
		true ->
			timer:sleep(200),
			do_run(Args);
		false ->
			case ar_data_sync:is_disk_space_sufficient(TargetStoreID) of
				false ->
					timer:sleep(30000),
					do_run(Args);
				true ->
					?LOG_DEBUG([{event, read_range}, {pid, self()},
						{size_mb, (End - Start) / ?MiB}, {args, Args}]),
					read_range(?READ_RANGE_MESSAGES_PER_BATCH, Args)
			end
	end.

read_range(0, Args) ->
	%% Batch yield: let `ar_data_sync''s mailbox drain, then re-check
	%% backpressure via `do_run' before continuing.
	timer:sleep(1000),
	do_run(Args);
read_range(_MessagesRemaining, {Start, End, _, _}) when Start >= End ->
	ok;
read_range(MessagesRemaining,
		{Start, End, OriginStoreID, TargetStoreID} = Args) ->
	case ar_sync_record:is_recorded(Start + 1, ar_data_sync, TargetStoreID) of
		{true, _} ->
			%% Chunk already synced at the target — skip to next gap.
			case ar_sync_record:get_next_unsynced_interval(
					Start, End, ar_data_sync, TargetStoreID) of
				not_found ->
					ok;
				{_, Start2} ->
					read_range(MessagesRemaining,
						{Start2, End, OriginStoreID, TargetStoreID})
			end;
		_ ->
			case ar_sync_record:is_recorded(Start + 1, ar_data_sync,
					OriginStoreID) of
				{true, Packing} ->
					read_and_post_chunk(MessagesRemaining, Packing, Args);
				SyncRecordReply ->
					?LOG_ERROR([{event, cannot_read_requested_range},
						{origin_store_id, OriginStoreID},
						{missing_start_offset, Start + 1},
						{end_offset, End},
						{target_store_id, TargetStoreID},
						{sync_record_reply,
							io_lib:format("~p", [SyncRecordReply])}]),
					ok
			end
	end.

%% @doc Read the chunk covering `Start + 1' in the origin store and hand it to
%% the target store for packing, routing each `read_chunk_with_full_metadata/2'
%% outcome through the range scan.
read_and_post_chunk(MessagesRemaining, Packing,
		{Start, End, OriginStoreID, TargetStoreID}) ->
	PaddedEnd = ar_block:get_chunk_padded_offset(End),
	case ar_data_sync:read_chunk_with_full_metadata(Start + 1, OriginStoreID) of
		no_chunk ->
			%% No chunk at or after `Start + 1' in this prefix; skip ahead.
			Start2 = ar_data_sync:advance_chunks_index_cursor(Start),
			read_range(MessagesRemaining,
				{Start2, End, OriginStoreID, TargetStoreID});
		{error, {data_missing, Metadata, Offsets}} ->
			#chunk_metadata{ chunk_size = ChunkSize } = Metadata,
			#chunk_offsets{ absolute_offset = AbsoluteOffset } = Offsets,
			ar_data_sync:invalidate_bad_data_record(
				AbsoluteOffset, ChunkSize, OriginStoreID,
				read_range_chunk_not_found),
			read_range(MessagesRemaining - 1,
				{Start + ChunkSize, End, OriginStoreID, TargetStoreID});
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_read_chunk},
				{offset, Start + 1},
				{reason, io_lib:format("~p", [Reason])}]),
			ok;
		{ok, _Metadata, #chunk_offsets{ absolute_offset = AbsoluteOffset }, _Chunk}
				when AbsoluteOffset > PaddedEnd ->
			ok;
		{ok, Metadata, Offsets, Chunk} ->
			post_chunk(MessagesRemaining, Packing, Chunk, Metadata, Offsets,
				{Start, End, OriginStoreID, TargetStoreID})
	end.

post_chunk(MessagesRemaining, Packing, Chunk, Metadata, Offsets,
		{Start, End, OriginStoreID, TargetStoreID}) ->
	#chunk_metadata{
		chunk_data_key = ChunkDataKey,
		tx_root = TXRoot,
		tx_path = TXPath,
		data_root = DataRoot,
		data_path = DataPath,
		chunk_size = ChunkSize
	} = Metadata,
	#chunk_offsets{
		absolute_offset = AbsoluteOffset,
		relative_offset = RelativeOffset
	} = Offsets,
	case ar_sync_record:is_recorded(AbsoluteOffset, ar_data_sync,
			OriginStoreID) of
		{true, Packing} ->
			ar_data_sync:increment_chunk_cache_size(),
			UnpackedChunk = case Packing of
				unpacked -> Chunk;
				_ -> none
			end,
			ChunkArgs = {DataRoot, AbsoluteOffset, TXPath, TXRoot, DataPath,
				Packing, RelativeOffset, ChunkSize, Chunk, UnpackedChunk,
				TargetStoreID, ChunkDataKey},
			gen_server:cast(ar_data_sync:name(TargetStoreID),
				{pack_and_store_chunk, ChunkArgs}),
			read_range(MessagesRemaining - 1,
				{Start + ChunkSize, End, OriginStoreID, TargetStoreID});
		{true, _DifferentPacking} ->
			%% Unlucky timing — the chunk should have been repacked
			%% in the meantime.
			read_range(MessagesRemaining,
				{Start, End, OriginStoreID, TargetStoreID});
		Reply ->
			?LOG_ERROR([{event, chunk_record_not_found},
				{absolute_end_offset, AbsoluteOffset},
				{ar_sync_record_reply, io_lib:format("~p", [Reply])}]),
			read_range(MessagesRemaining,
				{Start + ChunkSize, End, OriginStoreID, TargetStoreID})
	end.
