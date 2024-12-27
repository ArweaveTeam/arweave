%%% The blob storage optimized for fast reads.
-module(ar_chunk_storage).

-behaviour(gen_server).

-export([start_link/2, put/2, put/3,
		open_files/1, get/1, get/2, get/5, read_chunk2/5, get_range/2, get_range/3,
		close_file/2, close_files/1, cut/2, delete/1, delete/2, 
		list_files/2, run_defragmentation/0,
		get_storage_module_path/2, get_chunk_storage_path/2, is_prepared/1,
		get_chunk_bucket_start/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
	file_index,
	store_id,
	packing_map = #{},
	repack_cursor = 0,
	prev_repack_cursor = 0,
	target_packing = none,
	repacking_complete = false,
	range_start,
	range_end,
	reward_addr,
	prepare_replica_2_9_cursor,
	is_prepared = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIZE, Offset - ?DATA_CHUNK_SIZE + 1, .., Offset - 1.
put(PaddedOffset, Chunk) ->
	put(PaddedOffset, Chunk, "default").

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIZE, Offset - ?DATA_CHUNK_SIZE + 1, .., Offset - 1.
put(PaddedOffset, Chunk, StoreID) ->
	GenServerID = gen_server_id(StoreID),
	case catch gen_server:call(GenServerID, {put, PaddedOffset, Chunk}, 180_000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Open all the storage files. The subsequent calls to get/1 in the
%% caller process will use the opened file descriptors.
open_files(StoreID) ->
	ets:foldl(
		fun ({{Key, ID}, Filepath}, _) when ID == StoreID ->
				case erlang:get({cfile, {Key, ID}}) of
					undefined ->
						case file:open(Filepath, [read, raw, binary]) of
							{ok, F} ->
								erlang:put({cfile, {Key, ID}}, F);
							_ ->
								ok
						end;
					_ ->
						ok
				end;
			(_, _) ->
				ok
		end,
		ok,
		chunk_storage_file_index
	).

%% @doc Return {AbsoluteEndOffset, Chunk} for the chunk containing the given byte.
get(Byte) ->
	get(Byte, "default").

%% @doc Return {AbsoluteEndOffset, Chunk} for the chunk containing the given byte.
get(Byte, StoreID) ->
	case ar_sync_record:get_interval(Byte + 1, ?MODULE, StoreID) of
		not_found ->
			not_found;
		{_End, IntervalStart} ->
			get(Byte, IntervalStart, StoreID)
	end.

get(Byte, IntervalStart, StoreID) ->
	%% The synced ranges begin at IntervalStart => the chunk
	%% should begin at a multiple of ?DATA_CHUNK_SIZE to the right of IntervalStart.
	ChunkStart = Byte - (Byte - IntervalStart) rem ?DATA_CHUNK_SIZE,
	ChunkFileStart = get_chunk_file_start_by_start_offset(ChunkStart),
	case get(Byte, ChunkStart, ChunkFileStart, StoreID, 1) of
		[] ->
			not_found;
		[{EndOffset, Chunk}] ->
			{EndOffset, Chunk}
	end.

%% @doc Return a list of {AbsoluteEndOffset, Chunk} pairs for the stored chunks
%% inside the given range. The given interval does not have to cover every chunk
%% completely - we return all chunks at the intersection with the range.
get_range(Start, Size) ->
	get_range(Start, Size, "default").

%% @doc Return a list of {AbsoluteEndOffset, Chunk} pairs for the stored chunks
%% inside the given range. The given interval does not have to cover every chunk
%% completely - we return all chunks at the intersection with the range. The
%% very last chunk might be outside of the interval - its start offset is
%% at most Start + Size + ?DATA_CHUNK_SIZE - 1.
get_range(Start, Size, StoreID) ->
	?assert(Size < get_chunk_group_size()),
	case ar_sync_record:get_next_synced_interval(Start, infinity, ?MODULE, StoreID) of
		{_End, IntervalStart} when Start + Size > IntervalStart ->
			Start2 = max(Start, IntervalStart),
			Size2 = Start + Size - Start2,
			ChunkStart = Start2 - (Start2 - IntervalStart) rem ?DATA_CHUNK_SIZE,
			ChunkFileStart = get_chunk_file_start_by_start_offset(ChunkStart),
			End = Start2 + Size2,
			LastChunkStart = (End - 1) - ((End - 1) - IntervalStart) rem ?DATA_CHUNK_SIZE,
			LastChunkFileStart = get_chunk_file_start_by_start_offset(LastChunkStart),
			ChunkCount = (LastChunkStart - ChunkStart) div ?DATA_CHUNK_SIZE + 1,
			case ChunkFileStart /= LastChunkFileStart of
				false ->
					%% All chunks are from the same chunk file.
					get(Start2, ChunkStart, ChunkFileStart, StoreID, ChunkCount);
				true ->
					SizeBeforeBorder = ChunkFileStart + get_chunk_group_size() - ChunkStart,
					ChunkCountBeforeBorder = SizeBeforeBorder div ?DATA_CHUNK_SIZE
							+ case SizeBeforeBorder rem ?DATA_CHUNK_SIZE of 0 -> 0; _ -> 1 end,
					StartAfterBorder = ChunkStart + ChunkCountBeforeBorder * ?DATA_CHUNK_SIZE,
					SizeAfterBorder = Size2 - ChunkCountBeforeBorder * ?DATA_CHUNK_SIZE
							+ (Start2 - ChunkStart),
					get(Start2, ChunkStart, ChunkFileStart, StoreID, ChunkCountBeforeBorder)
						++ get_range(StartAfterBorder, SizeAfterBorder, StoreID)
			end;
		_ ->
			[]
	end.

%% @doc Close the file with the given Key.
close_file(Key, StoreID) ->
	case erlang:erase({cfile, {Key, StoreID}}) of
		undefined ->
			ok;
		F ->
			file:close(F)
	end.

%% @doc Close the files opened by open_files/1.
close_files(StoreID) ->
	close_files(erlang:get_keys(), StoreID).

%% @doc Soft-delete everything above the given end offset.
cut(Offset, StoreID) ->
	ar_sync_record:cut(Offset, ?MODULE, StoreID).

%% @doc Remove the chunk with the given end offset.
delete(Offset) ->
	delete(Offset, "default").

%% @doc Remove the chunk with the given end offset.
delete(PaddedOffset, StoreID) ->
	GenServerID = gen_server_id(StoreID),
	case catch gen_server:call(GenServerID, {delete, PaddedOffset}, 20000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Run defragmentation of chunk files if enabled
run_defragmentation() ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.run_defragmentation of
		false ->
			ok;
		true ->
			ar:console("Defragmentation threshold: ~B bytes.~n",
					   [Config#config.defragmentation_trigger_threshold]),
			DefragModules = modules_to_defrag(Config),
			Sizes = read_chunks_sizes(Config#config.data_dir),
			Files = files_to_defrag(DefragModules,
									Config#config.data_dir,
									Config#config.defragmentation_trigger_threshold,
									Sizes),
			ok = defrag_files(Files),
			ok = update_sizes_file(Files, #{})
	end.

get_storage_module_path(DataDir, "default") ->
	DataDir;
get_storage_module_path(DataDir, StoreID) ->
	filename:join([DataDir, "storage_modules", StoreID]).

get_chunk_storage_path(DataDir, StoreID) ->
	filename:join([get_storage_module_path(DataDir, StoreID), ?CHUNK_DIR]).
%% @doc Return true if the storage is ready to accept chunks.
-spec is_prepared(StoreID :: string()) -> true | false.
is_prepared(StoreID) ->
	GenServerID = gen_server_id(StoreID),
	case catch gen_server:call(GenServerID, is_prepared) of
		{'EXIT', {noproc, {gen_server, call, _}}} ->
			{error, timeout};
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Return the start offset of the bucket containing the given offset.
%% A chunk bucket a 0-based, 256-KiB wide, 256-KiB aligned range that fully contains a chunk.
-spec get_chunk_bucket_start(PaddedEndOffset :: non_neg_integer()) -> non_neg_integer().
get_chunk_bucket_start(PaddedEndOffset) ->
	ar_util:floor_int(max(0, PaddedEndOffset - ?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init({StoreID, RepackInPlacePacking}) ->
	%% Trap exit to avoid corrupting any open files on quit..
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	Dir = get_storage_module_path(DataDir, StoreID),
	ok = filelib:ensure_dir(Dir ++ "/"),
	ok = filelib:ensure_dir(filename:join(Dir, ?CHUNK_DIR) ++ "/"),
	FileIndex = read_file_index(Dir),
	FileIndex2 = maps:map(
		fun(Key, Filepath) ->
			Filepath2 =
				case {StoreID, catch binary_to_integer(Filepath)} of
					{_, {'EXIT', _}} ->
						Filepath;
					{"default", Num} when is_integer(Num) ->
						filename:join([DataDir, ?CHUNK_DIR, Filepath])
				end,
			ets:insert(chunk_storage_file_index, {{Key, StoreID}, Filepath2}),
			Filepath2
		end,
		FileIndex
	),
	warn_custom_chunk_group_size(StoreID),
	State = #state{ file_index = FileIndex2, store_id = StoreID },
	State2 =
		case ar_storage_module:get_packing(StoreID) of
			{replica_2_9, RewardAddr} ->
				{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
				PrepareCursor = {Start, _SubChunkStart} = 
					read_prepare_replica_2_9_cursor(StoreID, {RangeStart + 1, 0}),
				IsPrepared =
					case Start =< RangeEnd of
						true ->
							gen_server:cast(self(), prepare_replica_2_9),
							false;
						false ->
							true
					end,
				State#state{ reward_addr = RewardAddr, range_start = RangeStart,
						range_end = RangeEnd, prepare_replica_2_9_cursor = PrepareCursor,
						is_prepared = IsPrepared };
			_ ->
				State#state{ is_prepared = true }
		end,
	case RepackInPlacePacking of
		none ->
			{ok, State2};
		Packing ->
			%% We use the cursor to speed up the search for the place where
			%% the repacking should start in case the synced intervals are numerous
			%% and we have already repacked a bunch of them.
			Cursor =
				case read_repack_cursor(StoreID, Packing) of
					0 ->
						case remove_repack_cursor(StoreID) of
							ok ->
								0;
							Error ->
								?LOG_ERROR([{event, failed_to_remove_file},
										{error, io_lib:format("~p", [Error])}]),
								timer:sleep(2000),
								erlang:halt()
						end;
					C ->
						C
				end,
			gen_server:cast(self(), {repack, infinity, Packing}),
			{ok, State2#state{ repack_cursor = Cursor, target_packing = Packing }}
	end.

warn_custom_chunk_group_size(StoreID) ->
	case StoreID == "default" andalso get_chunk_group_size() /= ?CHUNK_GROUP_SIZE of
		true ->
			%% This warning applies to all store ids, but we will only print it when loading
			%% the default StoreID to ensure it is only printed once.
			WarningMessage = "WARNING: changing chunk_storage_file_size is not "
				"recommended and may cause errors if different sizes are used for the same "
				"chunk storage files.",
			ar:console(WarningMessage),
			?LOG_WARNING(WarningMessage);
		false ->
			ok
	end.

handle_cast(prepare_replica_2_9, #state{ store_id = StoreID } = State) ->
	case try_acquire_replica_2_9_formatting_lock(StoreID) of
		true ->
			?LOG_DEBUG([{event, acquired_replica_2_9_formatting_lock}, {store_id, StoreID}]),
			gen_server:cast(self(), do_prepare_replica_2_9);
		false ->
			?LOG_DEBUG([{event, failed_to_acquire_replica_2_9_formatting_lock}, {store_id, StoreID}]),
			ar_util:cast_after(2000, self(), prepare_replica_2_9)
	end,
	{noreply, State};

handle_cast(do_prepare_replica_2_9, State) ->
	#state{ reward_addr = RewardAddr, prepare_replica_2_9_cursor = {Start, SubChunkStart},
			range_end = RangeEnd, store_id = StoreID } = State,
	%% Sanity checks:
	PaddedEndOffset = get_chunk_bucket_end(ar_block:get_chunk_padded_offset(Start)),
	PaddedEndOffset = get_chunk_bucket_end(PaddedEndOffset),
	true = (
		max(0, PaddedEndOffset - ?DATA_CHUNK_SIZE) == get_chunk_bucket_start(PaddedEndOffset)
	),
	%% End of sanity checks.

	Partition = ar_replica_2_9:get_entropy_partition(PaddedEndOffset),
	CheckRangeEnd =
		case PaddedEndOffset > RangeEnd of
			true ->
				release_replica_2_9_formatting_lock(StoreID),
				?LOG_INFO([{event, storage_module_replica_2_9_preparation_complete},
						{store_id, StoreID}]),
				ar:console("The storage module ~s is prepared for 2.9 replication.~n",
						[StoreID]),
				true;
			false ->
				false
		end,
	%% For now the SubChunkStart and SubChunkStart2 values will always be 0. The field
	%% is used to make future improvemets easier. e.g. have the cursor increment by
	%% sub-chunk rather than chunk.
	SubChunkStart2 = (SubChunkStart + ?DATA_CHUNK_SIZE) rem ?DATA_CHUNK_SIZE,
	Start2 = PaddedEndOffset + ?DATA_CHUNK_SIZE,
	Cursor2 = {Start2, SubChunkStart2},
	State2 = State#state{ prepare_replica_2_9_cursor = Cursor2 },
	CheckIsRecorded =
		case CheckRangeEnd of
			true ->
				complete;
			false ->
				is_replica_2_9_entropy_sub_chunk_recorded(
					PaddedEndOffset, SubChunkStart, StoreID)
		end,
	StoreEntropy =
		case CheckIsRecorded of
			complete ->
				complete;
			true ->
				is_recorded;
			false ->
				Entropies = generate_entropies(RewardAddr, PaddedEndOffset, SubChunkStart),
				EntropyKeys = generate_entropy_keys(
					RewardAddr, PaddedEndOffset, SubChunkStart),
				SliceIndex = ar_replica_2_9:get_slice_index(PaddedEndOffset),
				%% If we are not at the beginning of the entropy, shift the offset to
				%% the left. store_entropy will traverse the entire 2.9 partition shifting
				%% the offset by sector size. It may happen some sub-chunks will be written
				%% to the neighbouring storage module(s) on the left or on the right
				%% since the 2.9 partition is slightly bigger than the recall partitition
				%% storage modules are commonly set up with.
				PaddedEndOffset2 = shift_replica_2_9_entropy_offset(
					PaddedEndOffset, -SliceIndex),
				store_entropy(Entropies, PaddedEndOffset2, SubChunkStart, Partition,
						EntropyKeys, RewardAddr, 0, 0)
		end,
	?LOG_DEBUG([{event, do_prepare_replica_2_9}, {store_id, StoreID},
			{padded_end_offset, PaddedEndOffset}, {range_end, RangeEnd},
			{partition, Partition}, {start, Start}, {sub_chunk_start, SubChunkStart},
			{check_is_recorded, CheckIsRecorded}, {store_entropy, StoreEntropy}]),
	case StoreEntropy of
		complete ->
			{noreply, State#state{ is_prepared = true }};
		is_recorded ->
			gen_server:cast(self(), do_prepare_replica_2_9),
			{noreply, State2};
		{error, Error} ->
			?LOG_WARNING([{event, failed_to_store_replica_2_9_entropy},
					{cursor, Start},
					{store_id, StoreID},
					{reason, io_lib:format("~p", [Error])}]),
			ar_util:cast_after(500, self(), do_prepare_replica_2_9),
			{noreply, State};
		{ok, SubChunksStored} ->
			?LOG_DEBUG([{event, stored_replica_2_9_entropy},
					{sub_chunks_stored, SubChunksStored},
					{partition, Partition},
					{store_id, StoreID},
					{cursor, Start},
					{padded_end_offset, PaddedEndOffset}]),
			gen_server:cast(self(), do_prepare_replica_2_9),
			case store_prepare_replica_2_9_cursor(Cursor2, StoreID) of
				ok ->
					ok;
				{error, Error} ->
					?LOG_WARNING([{event, failed_to_store_prepare_replica_2_9_cursor},
							{chunk_cursor, Start2},
							{sub_chunk_cursor, SubChunkStart2},
							{store_id, StoreID},
							{reason, io_lib:format("~p", [Error])}])
			end,
			{noreply, State2}
	end;

handle_cast(store_repack_cursor, #state{ repacking_complete = true } = State) ->
	{noreply, State};
handle_cast(store_repack_cursor,
		#state{ repack_cursor = Cursor, prev_repack_cursor = Cursor } = State) ->
	ar_util:cast_after(30000, self(), store_repack_cursor),
	{noreply, State};
handle_cast(store_repack_cursor,
		#state{ repack_cursor = Cursor, store_id = StoreID,
				target_packing = TargetPacking } = State) ->
	ar_util:cast_after(30000, self(), store_repack_cursor),
	ar:console("Repacked up to ~B, scanning further..~n", [Cursor]),
	?LOG_INFO([{event, repacked_partially},
			{storage_module, StoreID}, {cursor, Cursor}]),
	store_repack_cursor(Cursor, StoreID, TargetPacking),
	{noreply, State#state{ prev_repack_cursor = Cursor }};

handle_cast(repacking_complete, State) ->
	{noreply, State#state{ repacking_complete = true }};

handle_cast({repack, RightBound, Packing},
		#state{ store_id = StoreID, repack_cursor = Cursor } = State) ->
	gen_server:cast(self(), store_repack_cursor),
	spawn(fun() -> repack(Cursor, RightBound, Packing, StoreID) end),
	{noreply, State};
handle_cast({repack, Start, End, NextCursor, RightBound, Packing},
		#state{ store_id = StoreID } = State) ->
	spawn(fun() -> repack(Start, End, NextCursor, RightBound, Packing, StoreID) end),
	{noreply, State};

handle_cast({register_packing_ref, Ref, Offset}, #state{ packing_map = Map } = State) ->
	{noreply, State#state{ packing_map = maps:put(Ref, Offset, Map) }};

handle_cast({expire_repack_request, Ref}, #state{ packing_map = Map } = State) ->
	{noreply, State#state{ packing_map = maps:remove(Ref, Map) }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(is_prepared, _From, #state{ is_prepared = IsPrepared } = State) ->
	{reply, IsPrepared, State};

handle_call({put, PaddedEndOffset, Chunk}, _From, State)
		when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	case handle_store_chunk(PaddedEndOffset, Chunk, State) of
		{ok, FileIndex2} ->
			{reply, ok, State#state{ file_index = FileIndex2 }};
		Error ->
			{reply, Error, State}
	end;

handle_call({delete, PaddedEndOffset}, _From, State) ->
	#state{	file_index = FileIndex, store_id = StoreID } = State,
	ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
	Filepath = filepath(ChunkFileStart, FileIndex, StoreID),
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	case ar_sync_record:delete(PaddedEndOffset, StartOffset, ?MODULE, StoreID) of
		ok ->
			case delete_replica_2_9_entropy_record(PaddedEndOffset, StoreID) of
				ok ->
					case delete_chunk(PaddedEndOffset, ChunkFileStart, Filepath) of
						ok ->
							{reply, ok, State};
						Error ->
							{reply, Error, State}
					end;
				Error2 ->
					{reply, Error2, State}
			end;
		Error3 ->
			{reply, Error3, State}
	end;

handle_call(reset, _, #state{ store_id = StoreID, file_index = FileIndex } = State) ->
	maps:map(
		fun(_Key, Filepath) ->
			file:delete(Filepath)
		end,
		FileIndex
	),
	ok = ar_sync_record:cut(0, ?MODULE, StoreID),
	erlang:erase(),
	{reply, ok, State#state{ file_index = #{} }};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_info({chunk, {packed, Ref, ChunkArgs}},
	#state{ packing_map = Map, store_id = StoreID, repack_cursor = PrevCursor } = State) ->
	case maps:get(Ref, Map, not_found) of
		not_found ->
			{noreply, State};
		PaddedEndOffset ->
			State2 = State#state{ packing_map = maps:remove(Ref, Map) },
			{Packing, Chunk, _, _, _} = ChunkArgs,
			case ar_sync_record:delete(PaddedEndOffset, PaddedEndOffset - ?DATA_CHUNK_SIZE,
					ar_data_sync, StoreID) of
				ok ->
					case handle_store_chunk(PaddedEndOffset, Chunk, State) of
						{ok, FileIndex2} ->
							ar_sync_record:add_async(repacked_chunk,
									PaddedEndOffset, PaddedEndOffset - ?DATA_CHUNK_SIZE,
									Packing, ar_data_sync, StoreID),
							{noreply, State2#state{ file_index = FileIndex2,
									repack_cursor = PaddedEndOffset,
									prev_repack_cursor = PrevCursor }};
						Error2 ->
							?LOG_ERROR([{event, failed_to_store_repacked_chunk},
									{storage_module, StoreID},
									{padded_end_offset, PaddedEndOffset},
									{packing, ar_serialize:encode_packing(Packing, true)},
									{error, io_lib:format("~p", [Error2])}]),
							{noreply, State2}
					end;
				Error3 ->
					?LOG_ERROR([{event, failed_to_remove_repacked_chunk_from_sync_record},
							{storage_module, StoreID},
							{padded_end_offset, PaddedEndOffset},
							{packing, ar_serialize:encode_packing(Packing, true)},
							{error, io_lib:format("~p", [Error3])}]),
					{noreply, State2}
			end
	end;

handle_info({Ref, _Reply}, State) when is_reference(Ref) ->
	%% A stale gen_server:call reply.
	{noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, io_lib:format("~p", [Info])}]),
	{noreply, State}.

terminate(_Reason, #state{ repack_cursor = Cursor, store_id = StoreID,
		target_packing = TargetPacking }) ->
	sync_and_close_files(),
	store_repack_cursor(Cursor, StoreID, TargetPacking),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_chunk_group_size() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.chunk_storage_file_size.

read_repack_cursor(StoreID, TargetPacking) ->
	Filepath = get_filepath("repack_in_place_cursor", StoreID),
	case file:read_file(Filepath) of
		{ok, Bin} ->
			case catch binary_to_term(Bin) of
				{Cursor, TargetPacking} when is_integer(Cursor) ->
					Cursor;
				_ ->
					0
			end;
		_ ->
			0
	end.

read_prepare_replica_2_9_cursor(StoreID, Default) ->
	Filepath = get_filepath("prepare_replica_2_9_cursor", StoreID),
	case file:read_file(Filepath) of
		{ok, Bin} ->
			case catch binary_to_term(Bin) of
				{ChunkCursor, SubChunkCursor} = Cursor
						when is_integer(ChunkCursor), is_integer(SubChunkCursor) ->
					Cursor;
				_ ->
					Default
			end;
		_ ->
			Default
	end.

remove_repack_cursor(StoreID) ->
	Filepath = get_filepath("repack_in_place_cursor", StoreID),
	case file:delete(Filepath) of
		ok ->
			ok;
		{error, enoent} ->
			ok;
		Error ->
			Error
	end.

store_repack_cursor(0, _StoreID, _TargetPacking) ->
	ok;
store_repack_cursor(Cursor, StoreID, TargetPacking) ->
	Filepath = get_filepath("repack_in_place_cursor", StoreID),
	file:write_file(Filepath, term_to_binary({Cursor, TargetPacking})).

store_prepare_replica_2_9_cursor(Cursor, StoreID) ->
	Filepath = get_filepath("prepare_replica_2_9_cursor", StoreID),
	file:write_file(Filepath, term_to_binary(Cursor)).

get_filepath(Name, StoreID) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	ChunkDir = get_chunk_storage_path(DataDir, StoreID),
	filename:join([ChunkDir, Name]).

handle_store_chunk(PaddedEndOffset, Chunk, State) ->
	#state{ store_id = StoreID } = State,
	case ar_storage_module:get_packing(StoreID) of
		{replica_2_9, Addr} ->
			handle_store_chunk_replica_2_9(PaddedEndOffset, Chunk, Addr, State);
		_ ->
			handle_store_chunk2(PaddedEndOffset, Chunk, State)
	end.

handle_store_chunk_replica_2_9(PaddedEndOffset, Chunk, RewardAddr, State) ->
	#state{ store_id = StoreID, is_prepared = IsPrepared } = State,
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
	Filepath = filepath(ChunkFileStart, StoreID),
	acquire_replica_2_9_semaphore(Filepath),
	CheckIsStoredAlready =
		ar_sync_record:is_recorded(PaddedEndOffset, ?MODULE, StoreID),
	CheckIsEntropyRecorded =
		case CheckIsStoredAlready of
			true ->
				{error, already_stored};
			false ->
				is_replica_2_9_entropy_recorded(PaddedEndOffset, StoreID)
		end,
	ReadEntropy =
		case CheckIsEntropyRecorded of
			{error, _} = Error ->
				Error;
			false ->
				case IsPrepared of
					false ->
						no_entropy_yet;
					true ->
						missing_entropy
				end;
			true ->
				get(StartOffset, StartOffset, StoreID)
		end,
	case ReadEntropy of
		{error, _} = Error2 ->
			release_replica_2_9_semaphore(Filepath),
			Error2;
		not_found ->
			release_replica_2_9_semaphore(Filepath),
			{error, not_prepared_yet2};
		missing_entropy ->
			Entropy = generate_missing_replica_2_9_entropy(PaddedEndOffset, RewardAddr),
			PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
			Result = handle_store_chunk2(PaddedEndOffset, PackedChunk, State),
			release_replica_2_9_semaphore(Filepath),
			Result;
		no_entropy_yet ->
			Result = handle_store_chunk_no_entropy(PaddedEndOffset, Chunk, State),
			release_replica_2_9_semaphore(Filepath),
			Result;
		{_EndOffset, Entropy} ->
			release_replica_2_9_semaphore(Filepath),
			PackedChunk = ar_packing_server:encipher_replica_2_9_chunk(Chunk, Entropy),
			handle_store_chunk2(PaddedEndOffset, PackedChunk, State)
	end.

handle_store_chunk_no_entropy(PaddedEndOffset, Chunk, State) ->
	#state{ file_index = FileIndex, store_id = StoreID } = State,
	ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
	case store_chunk(ChunkFileStart, PaddedEndOffset, Chunk, FileIndex, StoreID) of
		{ok, Filepath} ->
			prometheus_counter:inc(chunks_without_entropy_stored),
			ID = ar_chunk_storage_replica_2_9_unpacked,
			case ar_sync_record:add(
					PaddedEndOffset, PaddedEndOffset - ?DATA_CHUNK_SIZE, ID, StoreID) of
				ok ->
					ets:insert(chunk_storage_file_index,
							{{ChunkFileStart, StoreID}, Filepath}),
					{error, stored_without_entropy};
				Error ->
					Error
			end;
		Error2 ->
			Error2
	end.

handle_store_chunk2(PaddedEndOffset, Chunk, State) ->
	#state{ file_index = FileIndex, store_id = StoreID } = State,
	ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
	case store_chunk(ChunkFileStart, PaddedEndOffset, Chunk, FileIndex, StoreID) of
		{ok, Filepath} ->
			prometheus_counter:inc(chunks_stored),
			case ar_sync_record:add(
					PaddedEndOffset, PaddedEndOffset - ?DATA_CHUNK_SIZE, ?MODULE, StoreID) of
				ok ->
					ets:insert(chunk_storage_file_index, {{ChunkFileStart, StoreID}, Filepath}),
					{ok, maps:put(ChunkFileStart, Filepath, FileIndex)};
				Error ->
					Error
			end;
		Error2 ->
			Error2
	end.

get_chunk_file_start(EndOffset) ->
	StartOffset = EndOffset - ?DATA_CHUNK_SIZE,
	get_chunk_file_start_by_start_offset(StartOffset).

get_chunk_file_start_by_start_offset(StartOffset) ->
	ar_util:floor_int(StartOffset, get_chunk_group_size()).

get_chunk_bucket_end(PaddedEndOffset) ->
	get_chunk_bucket_start(PaddedEndOffset) + ?DATA_CHUNK_SIZE.

%% @doc Return true if the given sub-chunk bucket contains the 2.9 entropy.
is_replica_2_9_entropy_sub_chunk_recorded(
		PaddedEndOffset, SubChunkBucketStartOffset, StoreID) ->
	ID = ar_chunk_storage_replica_2_9_entropy,
	ChunkBucketStart = get_chunk_bucket_start(PaddedEndOffset),
	SubChunkBucketStart = ChunkBucketStart + SubChunkBucketStartOffset,
	ar_sync_record:is_recorded(SubChunkBucketStart + 1, ID, StoreID).

%% @doc Return true if the 2.9 entropy for every sub-chunk of the chunk with the
%% given offset (> start offset, =< end offset) is recorded.
%% We check every sub-chunk because the entropy is written on the sub-chunk level.
is_replica_2_9_entropy_recorded(PaddedEndOffset, StoreID) ->
	ChunkBucketStart = get_chunk_bucket_start(PaddedEndOffset),
	is_replica_2_9_entropy_recorded2(ChunkBucketStart,
			ChunkBucketStart + ?DATA_CHUNK_SIZE, StoreID).

is_replica_2_9_entropy_recorded2(Cursor, BucketEnd, _StoreID)
		when Cursor >= BucketEnd ->
	true;
is_replica_2_9_entropy_recorded2(Cursor, BucketEnd, StoreID) ->
	ID = ar_chunk_storage_replica_2_9_entropy,
	case ar_sync_record:is_recorded(Cursor + 1, ID, StoreID) of
		false ->
			false;
		true ->
			SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
			is_replica_2_9_entropy_recorded2(Cursor + SubChunkSize, BucketEnd, StoreID)
	end.

update_replica_2_9_entropy_record(PaddedEndOffset, StoreID) ->
	ID = ar_chunk_storage_replica_2_9_entropy,
	BucketEnd = get_chunk_bucket_end(PaddedEndOffset),
	BucketStart = get_chunk_bucket_start(PaddedEndOffset),
	ar_sync_record:add_async(store_replica_2_9_entropy, BucketEnd, BucketStart, ID, StoreID).

delete_replica_2_9_entropy_record(PaddedEndOffset, StoreID) ->
	ID = ar_chunk_storage_replica_2_9_entropy,
	BucketStart = get_chunk_bucket_start(PaddedEndOffset),
	ar_sync_record:delete(BucketStart + ?DATA_CHUNK_SIZE, BucketStart, ID, StoreID).

generate_missing_replica_2_9_entropy(PaddedEndOffset, RewardAddr) ->
	Entropies = generate_entropies(RewardAddr, PaddedEndOffset, 0),
	EntropyIndex = ar_replica_2_9:get_slice_index(PaddedEndOffset),
	take_combined_entropy_by_index(Entropies, EntropyIndex).

generate_entropies(_RewardAddr, _PaddedEndOffset, SubChunkStart)
		when SubChunkStart == ?DATA_CHUNK_SIZE ->
	[];
generate_entropies(RewardAddr, PaddedEndOffset, SubChunkStart) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	[ar_packing_server:get_replica_2_9_entropy(RewardAddr, PaddedEndOffset, SubChunkStart)
			| generate_entropies(RewardAddr, PaddedEndOffset, SubChunkStart + SubChunkSize)].

generate_entropy_keys(_RewardAddr, _Offset, SubChunkStart)
		when SubChunkStart == ?DATA_CHUNK_SIZE ->
	[];
generate_entropy_keys(RewardAddr, Offset, SubChunkStart) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	[ar_replica_2_9:get_entropy_key(RewardAddr, Offset, SubChunkStart)
			| generate_entropy_keys(RewardAddr, Offset, SubChunkStart + SubChunkSize)].

store_entropy(
		Entropies, PaddedEndOffset, SubChunkStartOffset,
		Partition, Keys, RewardAddr, N, WaitN) ->
	case take_combined_entropy(Entropies) of
		{<<>>, []} ->
			wait_store_entropy_processes(WaitN),
			{ok, N};
		{EntropyPart, Rest} ->
			true = ar_replica_2_9:get_entropy_partition(PaddedEndOffset) == Partition,
			sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr,
					SubChunkStartOffset, Keys),
			FindModule =
				case ar_storage_module:get_strict(
						PaddedEndOffset, {replica_2_9, RewardAddr}) of
					not_found ->
						not_found;
					{ok, StoreID, _StorageModule} ->
						{ok, StoreID}
				end,
			case FindModule of
				not_found ->
					PaddedEndOffset2 = shift_replica_2_9_entropy_offset(PaddedEndOffset, 1),
					store_entropy(Rest, PaddedEndOffset2, SubChunkStartOffset, Partition,
							Keys, RewardAddr, N, WaitN);
				{ok, StoreID2} ->
					From = self(),
					spawn_link(fun() ->
						StartTime = erlang:monotonic_time(),

						store_entropy2(EntropyPart, PaddedEndOffset, StoreID2),
						
						EndTime = erlang:monotonic_time(),
						ElapsedTime = erlang:convert_time_unit(
							EndTime-StartTime, native, microsecond),
						%% bytes per second
						WriteRate = case ElapsedTime > 0 of 
							true -> 1000000 * byte_size(EntropyPart) div ElapsedTime; 
							false -> 0
						end,
						prometheus_gauge:set(replica_2_9_entropy_store_rate,
								[StoreID2], WriteRate),
						From ! {store_entropy_sub_chunk_written, WaitN + 1}
						end),
					PaddedEndOffset2 = shift_replica_2_9_entropy_offset(PaddedEndOffset, 1),
					store_entropy(Rest, PaddedEndOffset2, SubChunkStartOffset, Partition,
							Keys, RewardAddr, N + length(Keys), WaitN + 1)
			end
	end.

take_combined_entropy(Entropies) ->
	take_combined_entropy(Entropies, [], []).

take_combined_entropy([], Acc, RestAcc) ->
	{iolist_to_binary(Acc), lists:reverse(RestAcc)};
take_combined_entropy([<<>> | Entropies], _Acc, _RestAcc) ->
	true = lists:all(fun(Entropy) -> Entropy == <<>> end, Entropies),
	{<<>>, []};
take_combined_entropy([
		<< EntropyPart:(?COMPOSITE_PACKING_SUB_CHUNK_SIZE)/binary, Rest/binary >>
		| Entropies], Acc, RestAcc) ->
	take_combined_entropy(Entropies, [Acc | [EntropyPart]], [Rest | RestAcc]).

take_combined_entropy_by_index(Entropies, Index) ->
	take_combined_entropy_by_index(Entropies, Index, []).

take_combined_entropy_by_index([], _Index, Acc) ->
	iolist_to_binary(Acc);
take_combined_entropy_by_index([Entropy | Entropies], Index, Acc) ->
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	take_combined_entropy_by_index(Entropies, Index,
			[Acc | [binary:part(Entropy, Index * SubChunkSize, SubChunkSize)]]).

sanity_check_replica_2_9_entropy_keys(
		_PaddedEndOffset, _RewardAddr, _SubChunkStartOffset, []) ->
	ok;
sanity_check_replica_2_9_entropy_keys(
		PaddedEndOffset, RewardAddr, SubChunkStartOffset, [Key | Keys]) ->
	Key = ar_replica_2_9:get_entropy_key(RewardAddr, PaddedEndOffset, SubChunkStartOffset),
	SubChunkSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr,
			SubChunkStartOffset + SubChunkSize, Keys).

wait_store_entropy_processes(0) ->
	ok;
wait_store_entropy_processes(N) ->
	receive {store_entropy_sub_chunk_written, N} ->
		wait_store_entropy_processes(N - 1)
	end.

shift_replica_2_9_entropy_offset(Offset, SectorCount) ->
	SectorSize = ar_replica_2_9:get_sector_size(),
	get_chunk_bucket_end(ar_block:get_chunk_padded_offset(Offset + SectorSize * SectorCount)).

store_entropy2(EntropyPart, PaddedEndOffset, StoreID) ->
	ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
	Filepath = filepath(ChunkFileStart, StoreID),
	Size = byte_size(EntropyPart),
	{Position, _ChunkOffset} = get_position_and_relative_chunk_offset(
			ChunkFileStart, PaddedEndOffset),
	%% The entropy for the first sub-chunk of the chunk.
	%% The zero-offset does not have a real meaning, it is set
	%% to make sure we pass offset validation on read.
	Bin = << (get_special_zero_offset()):?OFFSET_BIT_SIZE, EntropyPart/binary >>,
	%% We allow generating and filling it the 2.9 entropy and storing unpacked chunks (to
	%% be enciphered later) asynchronously. Whatever comes first, is stored.
	%% If the other counterpart is stored already, we read it, encipher and store the
	%% packed chunk.
	acquire_replica_2_9_semaphore(Filepath),
	ID = ar_chunk_storage_replica_2_9_unpacked,
	IsUnpackedChunkRecorded = ar_sync_record:is_recorded(PaddedEndOffset, ID, StoreID),
	PrepareBin =
		case get_handle_by_filepath(Filepath) of
			{error, _} = Error ->
				Error;
			F ->
				case IsUnpackedChunkRecorded of
					false ->
						{no_unpacked_chunk, Bin, F};
					true ->
						{file:pread(F, Position, Size), F}
				end
		end,
	PrepareBin2 =
		case PrepareBin of
			{error, _} = Error2 ->
				Error2;
			{{error, _} = Error2, F2} ->
				{Error2, F2};
			{no_unpacked_chunk, Bin3, F2} ->
				{Bin3, F2, false};
			{{ok, Bin3}, F2} ->
				{ar_packing_server:encipher_replica_2_9_chunk(iolist_to_binary(Bin3), Bin),
						F2, true}
		end,
	?LOG_DEBUG([{event, store_entropy2}, {padded_end_offset, PaddedEndOffset},
			{position, Position}]),
	WriteResult =
		case PrepareBin2 of
			{error, _} = Error3 ->
				Error3;
			{{error, _}, F3} = Error3 ->
				{Error3, F3};
			{Bin4, F3, IsComplete} ->
				{file:pwrite(F3, Position, Bin4), F3, IsComplete}
		end,
	case WriteResult of
		{error, Reason} = Error4 ->
			?LOG_ERROR([{event, failed_to_store_replica_2_9_sub_chunk_entropy},
					{file, Filepath}, {position, Position},
					{reason, io_lib:format("~p", [Reason])}]),
			release_replica_2_9_semaphore(Filepath),
			Error4;
		{{error, Reason}, F4} = Error4 ->
			file:close(F4),
			?LOG_ERROR([{event, failed_to_store_replica_2_9_sub_chunk_entropy},
					{file, Filepath}, {position, Position},
					{reason, io_lib:format("~p", [Reason])}]),
			release_replica_2_9_semaphore(Filepath),
			Error4;
		{ok, F4, IsComplete2} ->
			file:close(F4),
			ets:insert(chunk_storage_file_index,
					{{ChunkFileStart, StoreID}, Filepath}),
			case update_replica_2_9_entropy_record(PaddedEndOffset, StoreID) of
				ok ->
					prometheus_counter:inc(replica_2_9_entropy_stored,
									[StoreID], byte_size(Bin)),
					release_replica_2_9_semaphore(Filepath),
					case IsComplete2 of
						false ->
							ok;
						true ->
							StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
							ar_sync_record:add_async(ar_chunk_storage_store_entropy_with_chunk,
									PaddedEndOffset, StartOffset, ?MODULE, StoreID),
							ar_sync_record:add_async(ar_data_sync_store_entropy_with_chunk,
									PaddedEndOffset, StartOffset, ar_data_sync, StoreID)
					end;
				Error5 ->
					release_replica_2_9_semaphore(Filepath),
					Error5
			end
	end.

acquire_replica_2_9_semaphore(Filepath) ->
	case ets:insert_new(ar_chunk_storage, {{replica_2_9_semaphore, Filepath}}) of
		false ->
			timer:sleep(20),
			acquire_replica_2_9_semaphore(Filepath);
		true ->
			ok
	end.

release_replica_2_9_semaphore(Filepath) ->
	ets:delete(ar_chunk_storage, {replica_2_9_semaphore, Filepath}).

store_chunk(ChunkFileStart, PaddedOffset, Chunk, FileIndex, StoreID) ->
	Filepath = filepath(ChunkFileStart, FileIndex, StoreID),
	store_chunk2(ChunkFileStart, PaddedOffset, Chunk, Filepath).

filepath(ChunkFileStart, FileIndex, StoreID) ->
	case maps:get(ChunkFileStart, FileIndex, not_found) of
		not_found ->
			filepath(ChunkFileStart, StoreID);
		Filepath ->
			Filepath
	end.

filepath(ChunkFileStart, StoreID) ->
	get_filepath(integer_to_binary(ChunkFileStart), StoreID).

get_handle_by_filepath(Filepath) ->
	case erlang:get({write_handle, Filepath}) of
		undefined ->
			case file:open(Filepath, [read, write, raw]) of
				{error, Reason} = Error ->
					?LOG_ERROR([
						{event, failed_to_open_chunk_file},
						{file, Filepath},
						{reason, io_lib:format("~p", [Reason])}
					]),
					Error;
				{ok, F} ->
					erlang:put({write_handle, Filepath}, F),
					F
			end;
		F ->
			F
	end.

store_chunk2(ChunkFileStart, PaddedOffset, Chunk, Filepath) ->
	case get_handle_by_filepath(Filepath) of
		{error, _} = Error ->
			Error;
		F ->
			store_chunk3(ChunkFileStart, PaddedOffset, Chunk, Filepath, F)
	end.

store_chunk3(ChunkFileStart, PaddedOffset, Chunk, Filepath, F) ->
	{Position, ChunkOffset} = get_position_and_relative_chunk_offset(ChunkFileStart,
			PaddedOffset),
	ChunkOffsetBinary =
		case ChunkOffset of
			0 ->
				ZeroOffset = get_special_zero_offset(),
				%% Represent 0 as the largest possible offset plus one,
				%% to distinguish zero offset from not yet written data.
				<< ZeroOffset:?OFFSET_BIT_SIZE >>;
			_ ->
				<< ChunkOffset:?OFFSET_BIT_SIZE >>
		end,
	Result = file:pwrite(F, Position, [ChunkOffsetBinary | Chunk]),
	case Result of
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_write_chunk},
				{padded_offset, PaddedOffset},
				{file, Filepath},
				{position, Position},
				{reason, io_lib:format("~p", [Reason])}
			]),
			Error;
		ok ->
			{ok, Filepath}
	end.

get_special_zero_offset() ->
	?DATA_CHUNK_SIZE.

get_position_and_relative_chunk_offset(ChunkFileStart, Offset) ->
	BucketPickOffset = Offset - ?DATA_CHUNK_SIZE,
	get_position_and_relative_chunk_offset_by_start_offset(ChunkFileStart, BucketPickOffset).

get_position_and_relative_chunk_offset_by_start_offset(ChunkFileStart, BucketPickOffset) ->
	BucketStart = ar_util:floor_int(BucketPickOffset, ?DATA_CHUNK_SIZE),
	ChunkOffset = BucketPickOffset - BucketStart,
	RelativeOffset = BucketStart - ChunkFileStart,
	Position = RelativeOffset + ?OFFSET_SIZE * (RelativeOffset div ?DATA_CHUNK_SIZE),
	{Position, ChunkOffset}.

delete_chunk(PaddedOffset, ChunkFileStart, Filepath) ->
	case file:open(Filepath, [read, write, raw]) of
		{ok, F} ->
			{Position, _ChunkOffset} =
					get_position_and_relative_chunk_offset(ChunkFileStart, PaddedOffset),
			ZeroChunk =
				case erlang:get(zero_chunk) of
					undefined ->
						OffsetBytes = << 0:?OFFSET_BIT_SIZE >>,
						ZeroBytes = << <<0>> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE) >>,
						Chunk = << OffsetBytes/binary, ZeroBytes/binary >>,
						%% Cache the zero chunk in the process memory, constructing
						%% it is expensive.
						erlang:put(zero_chunk, Chunk),
						Chunk;
					Chunk ->
						Chunk
				end,
			acquire_replica_2_9_semaphore(Filepath),
			Result = file:pwrite(F, Position, ZeroChunk),
			release_replica_2_9_semaphore(Filepath),
			Result;
		{error, enoent} ->
			ok;
		Error ->
			Error
	end.

get(Byte, Start, ChunkFileStart, StoreID, ChunkCount) ->
	case erlang:get({cfile, {ChunkFileStart, StoreID}}) of
		undefined ->
			case ets:lookup(chunk_storage_file_index, {ChunkFileStart, StoreID}) of
				[] ->
					[];
				[{_, Filepath}] ->
					read_chunk(Byte, Start, ChunkFileStart, Filepath, ChunkCount)
			end;
		File ->
			read_chunk2(Byte, Start, ChunkFileStart, File, ChunkCount)
	end.

read_chunk(Byte, Start, ChunkFileStart, Filepath, ChunkCount) ->
	case file:open(Filepath, [read, raw, binary]) of
		{error, enoent} ->
			[];
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_open_chunk_file},
				{byte, Byte},
				{reason, io_lib:format("~p", [Reason])}
			]),
			[];
		{ok, File} ->
			Result = read_chunk2(Byte, Start, ChunkFileStart, File, ChunkCount),
			file:close(File),
			Result
	end.

read_chunk2(Byte, Start, ChunkFileStart, File, ChunkCount) ->
	{Position, _ChunkOffset} =
			get_position_and_relative_chunk_offset_by_start_offset(ChunkFileStart, Start),
	BucketStart = ar_util:floor_int(Start, ?DATA_CHUNK_SIZE),
	read_chunk3(Byte, Position, BucketStart, File, ChunkCount).

read_chunk3(Byte, Position, BucketStart, File, ChunkCount) ->
	case file:pread(File, Position, (?DATA_CHUNK_SIZE + ?OFFSET_SIZE) * ChunkCount) of
		{ok, << ChunkOffset:?OFFSET_BIT_SIZE, _Chunk/binary >> = Bin} ->
			case is_offset_valid(Byte, BucketStart, ChunkOffset) of
				true ->
					extract_end_offset_chunk_pairs(Bin, BucketStart, 1);
				false ->
					[]
			end;
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_read_chunk},
				{byte, Byte},
				{position, Position},
				{reason, io_lib:format("~p", [Reason])}
			]),
			[];
		eof ->
			[]
	end.

extract_end_offset_chunk_pairs(
		<< 0:?OFFSET_BIT_SIZE, _ZeroChunk:?DATA_CHUNK_SIZE/binary, Rest/binary >>,
		BucketStart,
		Shift
 ) ->
	extract_end_offset_chunk_pairs(Rest, BucketStart, Shift + 1);
extract_end_offset_chunk_pairs(
		<< ChunkOffset:?OFFSET_BIT_SIZE, Chunk:?DATA_CHUNK_SIZE/binary, Rest/binary >>,
		BucketStart,
		Shift
 ) ->
	ChunkOffsetLimit = ?DATA_CHUNK_SIZE,
	EndOffset =
		BucketStart
		+ (ChunkOffset rem ChunkOffsetLimit)
		+ (?DATA_CHUNK_SIZE * Shift),
	[{EndOffset, Chunk}
			| extract_end_offset_chunk_pairs(Rest, BucketStart, Shift + 1)];
extract_end_offset_chunk_pairs(<<>>, _BucketStart, _Shift) ->
	[].

is_offset_valid(_Byte, _BucketStart, 0) ->
	%% 0 is interpreted as "data has not been written yet".
	false;
is_offset_valid(Byte, BucketStart, ChunkOffset) ->
	Delta = Byte - (BucketStart + ChunkOffset rem ?DATA_CHUNK_SIZE),
	Delta >= 0 andalso Delta < ?DATA_CHUNK_SIZE.

close_files([{cfile, {_, StoreID} = Key} | Keys], StoreID) ->
	file:close(erlang:get({cfile, Key})),
	close_files(Keys, StoreID);
close_files([_ | Keys], StoreID) ->
	close_files(Keys, StoreID);
close_files([], _StoreID) ->
	ok.

read_file_index(Dir) ->
	ChunkDir = filename:join(Dir, ?CHUNK_DIR),
	{ok, Filenames} = file:list_dir(ChunkDir),
	lists:foldl(
		fun(Filename, Acc) ->
			case catch list_to_integer(Filename) of
				Key when is_integer(Key) ->
					maps:put(Key, filename:join(ChunkDir, Filename), Acc);
				_ ->
					Acc
			end
		end,
		#{},
		Filenames
	).

sync_and_close_files() ->
	sync_and_close_files(erlang:get_keys()).

sync_and_close_files([{write_handle, _} = Key | Keys]) ->
	F = erlang:get(Key),
	ok = file:sync(F),
	file:close(F),
	sync_and_close_files(Keys);
sync_and_close_files([_ | Keys]) ->
	sync_and_close_files(Keys);
sync_and_close_files([]) ->
	ok.

list_files(DataDir, StoreID) ->
	Dir = get_storage_module_path(DataDir, StoreID),
	ok = filelib:ensure_dir(Dir ++ "/"),
	ok = filelib:ensure_dir(filename:join(Dir, ?CHUNK_DIR) ++ "/"),
	StorageIndex = read_file_index(Dir),
	maps:values(StorageIndex).

files_to_defrag(StorageModules, DataDir, ByteSizeThreshold, Sizes) ->
	AllFiles = lists:flatmap(
		fun(StorageModule) ->
			list_files(DataDir, ar_storage_module:id(StorageModule))
		end, StorageModules),
	lists:filter(
		fun(Filepath) ->
			case file:read_file_info(Filepath) of
				{ok, #file_info{ size = Size }} ->
					LastSize = maps:get(Filepath, Sizes, 1),
					Growth = (Size - LastSize) / LastSize,
					Size >= ByteSizeThreshold andalso Growth > 0.1;
				{error, Reason} ->
					?LOG_ERROR([
						{event, failed_to_read_chunk_file_info},
						{file, Filepath},
						{reason, io_lib:format("~p", [Reason])}
					]),
					false
			end
		end, AllFiles).

defrag_files([]) ->
	ok;
defrag_files([Filepath | Rest]) ->
	?LOG_DEBUG([{event, defragmenting_file}, {file, Filepath}]),
	ar:console("Defragmenting ~s...~n", [Filepath]),
	TmpFilepath = Filepath ++ ".tmp",
	DefragCmd = io_lib:format("rsync --sparse --quiet ~ts ~ts", [Filepath, TmpFilepath]),
	MoveDefragCmd = io_lib:format("mv ~ts ~ts", [TmpFilepath, Filepath]),
	%% We expect nothing to be returned on successful calls.
	[] = os:cmd(DefragCmd),
	[] = os:cmd(MoveDefragCmd),
	ar:console("Defragmented ~s...~n", [Filepath]),
	defrag_files(Rest).

update_sizes_file([], Sizes) ->
	{ok, Config} = application:get_env(arweave, config),
	SizesFile = filename:join(Config#config.data_dir, "chunks_sizes"),
	case file:open(SizesFile, [write, raw]) of
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_open_chunk_sizes_file},
				{file, SizesFile},
				{reason, io_lib:format("~p", [Reason])}
			]),
			error;
		{ok, F} ->
			SizesBinary = erlang:term_to_binary(Sizes),
			ok = file:write(F, SizesBinary),
			file:close(F)
	end;
update_sizes_file([Filepath | Rest], Sizes) ->
	case file:read_file_info(Filepath) of
		{ok, #file_info{ size = Size }} ->
			update_sizes_file(Rest, Sizes#{ Filepath => Size });
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_read_chunk_file_info},
				{file, Filepath},
				{reason, io_lib:format("~p", [Reason])}
			]),
			error
	end.

read_chunks_sizes(DataDir) ->
	SizesFile = filename:join(DataDir, "chunks_sizes"),
	case file:read_file(SizesFile) of
		{ok, Content} ->
			erlang:binary_to_term(Content);
		{error, enoent} ->
			#{};
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_read_chunk_sizes_file},
				{file, SizesFile},
				{reason, io_lib:format("~p", [Reason])}
			]),
			error
	end.

modules_to_defrag(#config{defragmentation_modules = [_ | _] = Modules}) -> Modules;
modules_to_defrag(#config{storage_modules = Modules}) -> Modules.

chunk_offset_list_to_map(ChunkOffsets) ->
	chunk_offset_list_to_map(ChunkOffsets, infinity, 0, #{}).

repack(Cursor, RightBound, Packing, StoreID) ->
	case ar_sync_record:get_next_synced_interval(Cursor, RightBound, ?MODULE, StoreID) of
		not_found ->
			ar:console("~n~nRepacking of ~s is complete! "
					"We suggest you stop the node, rename "
					"the storage module folder to reflect the new packing, and start the "
					"node with the new storage module.~n", [StoreID]),
			?LOG_INFO([{event, repacking_complete},
					{storage_module, StoreID},
					{target_packing, ar_serialize:encode_packing(Packing, true)}]),
			Server = gen_server_id(StoreID),
			gen_server:cast(Server, repacking_complete),
			ok;
		{End, Start} ->
			Start2 = max(Cursor, Start),
			case ar_sync_record:get_next_synced_interval(Start2, End, Packing, ar_data_sync,
					StoreID) of
				not_found ->
					repack(Start2, End, End, RightBound, Packing, StoreID);
				{End3, Start3} when Start3 > Start2 ->
					repack(Start2, Start3, End3, RightBound, Packing, StoreID);
				{End3, _Start3} ->
					repack(End3, RightBound, Packing, StoreID)
			end
	end.

repack(Start, End, NextCursor, RightBound, Packing, StoreID) when Start >= End ->
	repack(NextCursor, RightBound, Packing, StoreID);
repack(Start, End, NextCursor, RightBound, RequiredPacking, StoreID) ->
	{ok, Config} = application:get_env(arweave, config),
	RepackIntervalSize = ?DATA_CHUNK_SIZE * Config#config.repack_batch_size,
	Server = gen_server_id(StoreID),
	CheckPackingBuffer =
		case ar_packing_server:is_buffer_full() of
			true ->
				ar_util:cast_after(200, Server,
						{repack, Start, End, NextCursor, RightBound, RequiredPacking}),
				continue;
			false ->
				ok
		end,
	ReadRange =
		case CheckPackingBuffer of
			continue ->
				continue;
			ok ->
				case catch get_range(Start, RepackIntervalSize, StoreID) of
					[] ->
						Start2 = Start + RepackIntervalSize,
						gen_server:cast(Server, {repack, Start2, End, NextCursor, RightBound,
								RequiredPacking}),
						continue;
					{'EXIT', _Exc} ->
						?LOG_ERROR([{event, failed_to_read_chunk_range},
								{storage_module, StoreID},
								{start, Start},
								{size, RepackIntervalSize},
								{store_id, StoreID}]),
						Start2 = Start + RepackIntervalSize,
						gen_server:cast(Server, {repack, Start2, End, NextCursor, RightBound,
								RequiredPacking}),
						continue;
					Range ->
						{ok, Range}
				end
		end,
	ReadMetadataRange =
		case ReadRange of
			continue ->
				continue;
			{ok, Range2} ->
				{Min, Max, Map} = chunk_offset_list_to_map(Range2),
				case ar_data_sync:get_chunk_metadata_range(Min, min(Max, End), StoreID) of
					{ok, MetadataMap} ->
						{ok, Map, MetadataMap};
					{error, Error} ->
						?LOG_ERROR([{event, failed_to_read_chunk_metadata_range},
								{storage_module, StoreID},
								{error, io_lib:format("~p", [Error])},
								{left, Min},
								{right, Max}]),
						Start3 = Start + RepackIntervalSize,
						gen_server:cast(Server, {repack, Start3, End, NextCursor, RightBound,
								RequiredPacking}),
						continue
				end
		end,
	case ReadMetadataRange of
		continue ->
			ok;
		{ok, Map2, MetadataMap2} ->
			Start4 = Start + RepackIntervalSize,
			gen_server:cast(Server, {repack, Start4, End, NextCursor, RightBound,
					RequiredPacking}),
			maps:fold(
				fun	(AbsoluteOffset, {_, _TXRoot, _, _, _, ChunkSize}, ok)
							when ChunkSize /= ?DATA_CHUNK_SIZE,
									AbsoluteOffset =< ?STRICT_DATA_SPLIT_THRESHOLD ->
						ok;
					(AbsoluteOffset, {_, TXRoot, _, _, _, ChunkSize}, ok) ->
						PaddedOffset = ar_block:get_chunk_padded_offset(AbsoluteOffset),
						case ar_sync_record:is_recorded(PaddedOffset, ar_data_sync, StoreID) of
							{true, RequiredPacking} ->
								?LOG_WARNING([{event,
											repacking_process_chunk_already_repacked},
										{storage_module, StoreID},
										{packing,
											ar_serialize:encode_packing(RequiredPacking,true)},
										{offset, AbsoluteOffset}]),
								ok;
							{true, Packing} ->
								case maps:get(PaddedOffset, Map2, not_found) of
									not_found ->
										?LOG_WARNING([{event,
												chunk_not_found_in_chunk_storage},
											{storage_module, StoreID},
											{offset, PaddedOffset}]),
										ok;
									Chunk ->
										Ref = make_ref(),
										gen_server:cast(Server,
												{register_packing_ref, Ref, PaddedOffset}),
										ar_util:cast_after(300000, Server,
												{expire_repack_request, Ref}),
										ar_packing_server:request_repack(Ref, whereis(Server),
												{RequiredPacking, Packing, Chunk,
													AbsoluteOffset, TXRoot, ChunkSize}),
										ok
								end;
							true ->
								?LOG_WARNING([{event, no_packing_information_for_the_chunk},
										{storage_module, StoreID},
										{offset, PaddedOffset}]),
								ok;
							false ->
								?LOG_WARNING([{event, chunk_not_found_in_sync_record},
										{storage_module, StoreID},
										{offset, PaddedOffset}]),
								ok
						end
				end,
				ok,
				MetadataMap2
			)
	end.

chunk_offset_list_to_map([], Min, Max, Map) ->
	{Min, Max, Map};
chunk_offset_list_to_map([{Offset, Chunk} | ChunkOffsets], Min, Max, Map) ->
	chunk_offset_list_to_map(ChunkOffsets, min(Min, Offset), max(Max, Offset),
			maps:put(Offset, Chunk, Map)).

-ifdef(DEBUG).
	try_acquire_replica_2_9_formatting_lock(_StoreID) ->
		true.
-else.
try_acquire_replica_2_9_formatting_lock(StoreID) ->
	case ets:insert_new(ar_chunk_storage, {update_replica_2_9_lock}) of
		true ->
			Count = get_replica_2_9_acquired_locks_count(),
			{ok, Config} = application:get_env(arweave, config),
			MaxWorkers = Config#config.replica_2_9_workers,
			case Count + 1 > MaxWorkers of
				true ->
					ets:delete(ar_chunk_storage, update_replica_2_9_lock),
					false;
				false ->
					ets:update_counter(ar_chunk_storage, replica_2_9_acquired_locks_count,
							1, {replica_2_9_acquired_locks_count, 0}),
					ets:delete(ar_chunk_storage, update_replica_2_9_lock),
					true
			end;
		false ->
			try_acquire_replica_2_9_formatting_lock(StoreID)
	end.
-endif.

get_replica_2_9_acquired_locks_count() ->
	case ets:lookup(ar_chunk_storage, replica_2_9_acquired_locks_count) of
		[] ->
			0;
		[{_, Count}] ->
			Count
	end.

release_replica_2_9_formatting_lock(StoreID) ->
	case ets:insert_new(ar_chunk_storage, {update_replica_2_9_lock}) of
		true ->
			Count = get_replica_2_9_acquired_locks_count(),
			case Count of
				0 ->
					ok;
				_ ->
					ets:update_counter(ar_chunk_storage, replica_2_9_acquired_locks_count,
							-1, {replica_2_9_acquired_locks_count, 0})
			end,
			ets:delete(ar_chunk_storage, update_replica_2_9_lock);
		false ->
			release_replica_2_9_formatting_lock(StoreID)
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

replica_2_9_test_() ->
	{timeout, 300, fun test_replica_2_9/0}.

test_replica_2_9() ->
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	StorageModules = [
			{?PARTITION_SIZE, 0, {replica_2_9, RewardAddr}},
			{?PARTITION_SIZE, 1, {replica_2_9, RewardAddr}}
	],
	{ok, Config} = application:get_env(arweave, config),
	try
		ar_test_node:start(#{ reward_addr => RewardAddr, storage_modules => StorageModules }),
		StoreID1 = ar_storage_module:id(lists:nth(1, StorageModules)),
		StoreID2 = ar_storage_module:id(lists:nth(2, StorageModules)),
		C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
		%% The replica_2_9 storage does not support updates and three chunks are written
		%% into the first partition when the test node is launched.
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(?DATA_CHUNK_SIZE, C1, StoreID1)),
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, StoreID1)),
		?assertEqual({error, already_stored},
				ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C1, StoreID1)),

		%% Store the new chunk.
		?assertEqual(ok, ar_chunk_storage:put(4 * ?DATA_CHUNK_SIZE, C1, StoreID1)),
		{ok, P1, _Entropy} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 4 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P1, 4 * ?DATA_CHUNK_SIZE, StoreID1),

		assert_get(not_found, 8 * ?DATA_CHUNK_SIZE, StoreID1),
		?assertEqual(ok, ar_chunk_storage:put(8 * ?DATA_CHUNK_SIZE, C1, StoreID1)),
		{ok, P2, _} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 8 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P2, 8 * ?DATA_CHUNK_SIZE, StoreID1),

		%% Store chunks in the second partition.
		?assertEqual(ok, ar_chunk_storage:put(12 * ?DATA_CHUNK_SIZE, C1, StoreID2)),
		{ok, P3, Entropy3} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 12 * ?DATA_CHUNK_SIZE, C1),

		assert_get(P3, 12 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertEqual(ok, ar_chunk_storage:put(15 * ?DATA_CHUNK_SIZE, C1, StoreID2)),
		{ok, P4, Entropy4} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 15 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P4, 15 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertNotEqual(P3, P4),
		?assertNotEqual(Entropy3, Entropy4),

		?assertEqual(ok, ar_chunk_storage:put(16 * ?DATA_CHUNK_SIZE, C1, StoreID2)),
		{ok, P5, Entropy5} =
				ar_packing_server:pack_replica_2_9_chunk(RewardAddr, 16 * ?DATA_CHUNK_SIZE, C1),
		assert_get(P5, 16 * ?DATA_CHUNK_SIZE, StoreID2),
		?assertNotEqual(Entropy4, Entropy5)
	after
		ok = application:set_env(arweave, config, Config)
	end.

well_aligned_test_() ->
	{timeout, 300, fun test_well_aligned/0}.

test_well_aligned() ->
	clear("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ok = ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(?DATA_CHUNK_SIZE, C2),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	?assertEqual([{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
			ar_chunk_storage:get_range(0, 2 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
			ar_chunk_storage:get_range(1, 2 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
			ar_chunk_storage:get_range(1, 2 * ?DATA_CHUNK_SIZE - 1)),
	?assertEqual([{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
			ar_chunk_storage:get_range(0, 3 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
			ar_chunk_storage:get_range(0, ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C3),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C2),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(C2, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE),
	ar_chunk_storage:delete(?DATA_CHUNK_SIZE),
	assert_get(not_found, ?DATA_CHUNK_SIZE),
	?assertEqual([], ar_chunk_storage:get_range(0, ?DATA_CHUNK_SIZE)),
	assert_get(C2, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE),
	?assertEqual([{2 * ?DATA_CHUNK_SIZE, C2}, {3 * ?DATA_CHUNK_SIZE, C3}],
			ar_chunk_storage:get_range(0, 4 * ?DATA_CHUNK_SIZE)),
	?assertEqual([], ar_chunk_storage:get_range(7 * ?DATA_CHUNK_SIZE, 13 * ?DATA_CHUNK_SIZE)).

not_aligned_test_() ->
	{timeout, 300, fun test_not_aligned/0}.

test_not_aligned() ->
	clear("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE + 7, C1),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE + 7),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE + 7, C1),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE + 7),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE + 7)),
	?assertEqual(not_found, ar_chunk_storage:get(?DATA_CHUNK_SIZE + 7 - 1)),
	?assertEqual(not_found, ar_chunk_storage:get(?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(?DATA_CHUNK_SIZE - 1)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(1)),
	ar_chunk_storage:put(?DATA_CHUNK_SIZE + 3, C2),
	assert_get(C2, ?DATA_CHUNK_SIZE + 3),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(1)),
	?assertEqual(not_found, ar_chunk_storage:get(2)),
	ar_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE + 7),
	assert_get(C2, ?DATA_CHUNK_SIZE + 3),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE + 7, C3),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE + 7, C1),
	assert_get(C1, 3 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2, C2),
	assert_get(C2, 4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2),
	?assertEqual(
		not_found,
		ar_chunk_storage:get(4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2)
	),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 7)),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 8)),
	ar_chunk_storage:put(5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2),
	assert_get(C2, 5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:delete(4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2),
	assert_get(not_found, 4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2),
	assert_get(C2, 5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1),
	assert_get(C1, 3 * ?DATA_CHUNK_SIZE + 7),
	?assertEqual([{3 * ?DATA_CHUNK_SIZE + 7, C1}],
			ar_chunk_storage:get_range(2 * ?DATA_CHUNK_SIZE + 7, 2 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{3 * ?DATA_CHUNK_SIZE + 7, C1}],
			ar_chunk_storage:get_range(2 * ?DATA_CHUNK_SIZE + 6, 2 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{3 * ?DATA_CHUNK_SIZE + 7, C1},
			{5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}],
			%% The end offset of the second chunk is bigger than Start + Size but
			%% it is included because Start + Size is bigger than the start offset
			%% of the bucket where the last chunk is placed.
			ar_chunk_storage:get_range(2 * ?DATA_CHUNK_SIZE + 7, 2 * ?DATA_CHUNK_SIZE + 1)),
	?assertEqual([{3 * ?DATA_CHUNK_SIZE + 7, C1},
			{5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}],
			ar_chunk_storage:get_range(2 * ?DATA_CHUNK_SIZE + 7, 3 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{3 * ?DATA_CHUNK_SIZE + 7, C1},
			{5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}],
			ar_chunk_storage:get_range(2 * ?DATA_CHUNK_SIZE + 7 - 1, 3 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{3 * ?DATA_CHUNK_SIZE + 7, C1},
			{5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}],
			ar_chunk_storage:get_range(2 * ?DATA_CHUNK_SIZE, 4 * ?DATA_CHUNK_SIZE)).

cross_file_aligned_test_() ->
	{timeout, 300, fun test_cross_file_aligned/0}.

test_cross_file_aligned() ->
	clear("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(get_chunk_group_size(), C1),
	assert_get(C1, get_chunk_group_size()),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size())),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() - ?DATA_CHUNK_SIZE - 1)),
	ar_chunk_storage:put(get_chunk_group_size() + ?DATA_CHUNK_SIZE, C2),
	assert_get(C2, get_chunk_group_size() + ?DATA_CHUNK_SIZE),
	assert_get(C1, get_chunk_group_size()),
	?assertEqual([{get_chunk_group_size(), C1}, {get_chunk_group_size() + ?DATA_CHUNK_SIZE, C2}],
			ar_chunk_storage:get_range(get_chunk_group_size() - ?DATA_CHUNK_SIZE,
					2 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{get_chunk_group_size(), C1}, {get_chunk_group_size() + ?DATA_CHUNK_SIZE, C2}],
			ar_chunk_storage:get_range(get_chunk_group_size() - 2 * ?DATA_CHUNK_SIZE - 1,
					4 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() - ?DATA_CHUNK_SIZE - 1)),
	ar_chunk_storage:delete(get_chunk_group_size()),
	assert_get(not_found, get_chunk_group_size()),
	assert_get(C2, get_chunk_group_size() + ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(get_chunk_group_size(), C2),
	assert_get(C2, get_chunk_group_size()).

cross_file_not_aligned_test_() ->
	{timeout, 300, fun test_cross_file_not_aligned/0}.

test_cross_file_not_aligned() ->
	clear("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(get_chunk_group_size() + 1, C1),
	assert_get(C1, get_chunk_group_size() + 1),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() - ?DATA_CHUNK_SIZE)),
	ar_chunk_storage:put(2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2, C2),
	assert_get(C2, 2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	ar_chunk_storage:put(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2, C3),
	assert_get(C2, 2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	assert_get(C3, 2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2),
	?assertEqual([{2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2, C3},
			{2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2, C2}],
			ar_chunk_storage:get_range(2 * get_chunk_group_size()
					- ?DATA_CHUNK_SIZE div 2 - ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE * 2)),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	?assertEqual(
		not_found,
		ar_chunk_storage:get(get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2 - 1)
	),
	ar_chunk_storage:delete(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2),
	assert_get(not_found, 2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2),
	assert_get(C2, 2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	assert_get(C1, get_chunk_group_size() + 1),
	ar_chunk_storage:delete(get_chunk_group_size() + 1),
	assert_get(not_found, get_chunk_group_size() + 1),
	assert_get(not_found, 2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2),
	assert_get(C2, 2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	ar_chunk_storage:delete(2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	assert_get(not_found, 2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	ar_chunk_storage:delete(get_chunk_group_size() + 1),
	ar_chunk_storage:delete(100 * get_chunk_group_size() + 1),
	ar_chunk_storage:put(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2, C1),
	assert_get(C1, 2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2),
	?assertEqual(not_found,
			ar_chunk_storage:get(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2)).

gen_server_id(StoreID) ->
	list_to_atom("ar_chunk_storage_" ++ ar_storage_module:label_by_id(StoreID)).

clear(StoreID) ->
	ok = gen_server:call(gen_server_id(StoreID), reset).

assert_get(Expected, Offset) ->
	assert_get(Expected, Offset, "default").

assert_get(Expected, Offset, StoreID) ->
	ExpectedResult =
		case Expected of
			not_found ->
				not_found;
			_ ->
				{Offset, Expected}
		end,
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - 1, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - 2, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 1, StoreID)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 2, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 + 1, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 - 1, StoreID)),
	?assertEqual(ExpectedResult,
			ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 3, StoreID)).

defrag_command_test() ->
	RandomID = crypto:strong_rand_bytes(16),
	Filepath = "test_defrag_" ++ binary_to_list(ar_util:encode(RandomID)),
	{ok, F} = file:open(Filepath, [binary, write]),
	{O1, C1} = {236, crypto:strong_rand_bytes(262144)},
	{O2, C2} = {262144, crypto:strong_rand_bytes(262144)},
	{O3, C3} = {262143, crypto:strong_rand_bytes(262144)},
	file:pwrite(F, 1, <<"a">>),
	file:pwrite(F, 1000, <<"b">>),
	file:pwrite(F, 1000000, <<"cde">>),
	file:pwrite(F, 10000001, << O1:24, C1/binary, O2:24, C2/binary >>),
	file:pwrite(F, 30000001, << O3:24, C3/binary >>),
	file:close(F),
	defrag_files([Filepath]),
	{ok, F2} = file:open(Filepath, [binary, read]),
	?assertEqual({ok, <<0>>}, file:pread(F2, 0, 1)),
	?assertEqual({ok, <<"a">>}, file:pread(F2, 1, 1)),
	?assertEqual({ok, <<0>>}, file:pread(F2, 2, 1)),
	?assertEqual({ok, <<"b">>}, file:pread(F2, 1000, 1)),
	?assertEqual({ok, <<"c">>}, file:pread(F2, 1000000, 1)),
	?assertEqual({ok, <<"cde">>}, file:pread(F2, 1000000, 3)),
	?assertEqual({ok, C1}, file:pread(F2, 10000001 + 3, 262144)),
	?assertMatch({ok, << O1:24, _/binary >>}, file:pread(F2, 10000001, 10)),
	?assertMatch({ok, << O1:24, C1:262144/binary, O2:24, C2:262144/binary,
			0:((262144 + 3) * 2 * 8) >>}, file:pread(F2, 10000001, (262144 + 3) * 4)),
	?assertMatch({ok, << O3:24, C3:262144/binary >>},
			file:pread(F2, 30000001, 262144 + 3 + 100)). % End of file => +100 is ignored.
