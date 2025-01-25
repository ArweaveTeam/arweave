%% The blob storage optimized for fast reads.
-module(ar_chunk_storage).

-behaviour(gen_server).

-export([start_link/2, name/1, register_workers/0, is_storage_supported/3, put/3, put/4,
		open_files/1, get/1, get/2, get/3, get/5, locate_chunk_on_disk/2,
		get_range/2, get_range/3, cut/2, delete/1, delete/2, set_entropy_context/2,
		get_filepath/2, get_handle_by_filepath/1, close_file/2, close_files/1, 
		list_files/2, run_defragmentation/0,
		get_storage_module_path/2, get_chunk_storage_path/2,
		get_chunk_bucket_start/1, get_chunk_bucket_end/1,
		sync_record_id/1, store_chunk/6, write_chunk/4, record_chunk/7]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("../include/ar.hrl").
-include("../include/ar_sup.hrl").
-include("../include/ar_config.hrl").
-include("../include/ar_consensus.hrl").
-include("../include/ar_chunk_storage.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
	file_index,
	store_id,
	store_id_label,
	packing_labels = #{},
	packing_map = #{},
	repack_cursor = 0,
	target_packing,
	repack_status = undefined,
	entropy_context = none, %% some data we need pass to ar_entropy_storage
	range_start,
	range_end
}).

-ifdef(AR_TEST).
-define(DEVICE_LOCK_WAIT, 100).
-else.
-define(DEVICE_LOCK_WAIT, 5_000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link(Name, {StoreID, RepackInPlacePacking}) ->
	gen_server:start_link({local, Name}, ?MODULE, {StoreID, RepackInPlacePacking}, []).

%% @doc Return the name of the server serving the given StoreID.
name(StoreID) ->
	list_to_atom("ar_chunk_storage_" ++ ar_storage_module:label_by_id(StoreID)).

register_workers() ->
	{ok, Config} = application:get_env(arweave, config),
	ConfiguredWorkers = lists:map(
		fun(StorageModule) ->
			StoreID = ar_storage_module:id(StorageModule),

			ChunkStorageName = ar_chunk_storage:name(StoreID),
			?CHILD_WITH_ARGS(ar_chunk_storage, worker,
				ChunkStorageName, [ChunkStorageName, {StoreID, none}])
		end,
		Config#config.storage_modules
	),
	
	DefaultChunkStorageWorker = ?CHILD_WITH_ARGS(ar_chunk_storage, worker,
		ar_chunk_storage_default, [ar_chunk_storage_default, {"default", none}]),

	RepackInPlaceWorkers = lists:map(
		fun({StorageModule, Packing}) ->
			StoreID = ar_storage_module:id(StorageModule),
			%% Note: the config validation will prevent a StoreID from being used in both
			%% `storage_modules` and `repack_in_place_storage_modules`, so there's
			%% no risk of a `Name` clash with the workers spawned above.
			ChunkStorageName = ar_chunk_storage:name(StoreID),
			?CHILD_WITH_ARGS(ar_chunk_storage, worker,
				ChunkStorageName, [ChunkStorageName, {StoreID, Packing}])
		end,
		Config#config.repack_in_place_storage_modules
	),

	ConfiguredWorkers ++ RepackInPlaceWorkers ++ [DefaultChunkStorageWorker].

%% @doc Return true if we can accept the chunk for storage.
%% 256 KiB chunks are stored in the blob storage optimized for read speed.
%% Unpacked chunks smaller than 256 KiB cannot be stored here currently,
%% because the module does not keep track of the chunk sizes - all chunks
%% are assumed to be 256 KiB.
-spec is_storage_supported(
		Offset :: non_neg_integer(),
		ChunkSize :: non_neg_integer(),
		Packing :: term()
) -> true | false.

is_storage_supported(Offset, ChunkSize, Packing) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			%% All chunks above ?STRICT_DATA_SPLIT_THRESHOLD are placed in 256 KiB buckets
			%% so technically can be stored in ar_chunk_storage. However, to avoid
			%% managing padding in ar_chunk_storage for unpacked chunks smaller than 256 KiB
			%% (we do not need fast random access to unpacked chunks after
			%% ?STRICT_DATA_SPLIT_THRESHOLD anyways), we put them to RocksDB.
			Packing /= unpacked orelse ChunkSize == (?DATA_CHUNK_SIZE);
		false ->
			ChunkSize == (?DATA_CHUNK_SIZE)
	end.

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIZE, Offset - ?DATA_CHUNK_SIZE + 1, .., Offset - 1.
put(PaddedOffset, Chunk, StoreID) ->
	Packing = ar_storage_module:get_packing(StoreID),
	put(PaddedOffset, Chunk, Packing, StoreID).

put(PaddedOffset, Chunk, Packing, StoreID) ->
	GenServerID = name(StoreID),
	case catch gen_server:call(GenServerID, {put, PaddedOffset, Chunk, Packing}, 180_000) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			?LOG_ERROR([{event, gen_server_timeout_putting_chunk},
				{padded_offset, PaddedOffset},
				{store_id, StoreID}
			]),
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
	case ar_sync_record:get_interval(Byte + 1, ar_chunk_storage, StoreID) of
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

locate_chunk_on_disk(PaddedEndOffset, StoreID) ->
	locate_chunk_on_disk(PaddedEndOffset, StoreID, #{}).

locate_chunk_on_disk(PaddedEndOffset, StoreID, FileIndex) ->
	ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
	Filepath = filepath(ChunkFileStart, FileIndex, StoreID),
	{Position, ChunkOffset} =
        get_position_and_relative_chunk_offset(ChunkFileStart, PaddedEndOffset),
	{ChunkFileStart, Filepath, Position, ChunkOffset}.

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
	case ar_sync_record:get_next_synced_interval(Start, infinity, ar_chunk_storage, StoreID) of
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
	ar_sync_record:cut(Offset, ar_chunk_storage, StoreID).

%% @doc Remove the chunk with the given end offset.
delete(Offset) ->
	delete(Offset, "default").

%% @doc Remove the chunk with the given end offset.
delete(PaddedOffset, StoreID) ->
	GenServerID = name(StoreID),
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

%% @doc Return the start and end offset of the bucket containing the given offset.
%% A chunk bucket a 0-based, 256-KiB wide, 256-KiB aligned range that fully contains a chunk.
-spec get_chunk_bucket_start(PaddedEndOffset :: non_neg_integer()) -> non_neg_integer().
get_chunk_bucket_start(PaddedEndOffset) ->
	ar_util:floor_int(max(0, PaddedEndOffset - ?DATA_CHUNK_SIZE), ?DATA_CHUNK_SIZE).

-spec get_chunk_bucket_end(Offset :: non_neg_integer()) -> non_neg_integer().
get_chunk_bucket_end(EndOffset) ->
	PaddedEndOffset = ar_block:get_chunk_padded_offset(EndOffset),
	get_chunk_bucket_start(PaddedEndOffset) + ?DATA_CHUNK_SIZE.

set_entropy_context(StoreID, EntropyContext) ->
	gen_server:cast(name(StoreID), {set_entropy_context, EntropyContext}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init({"default" = StoreID, _}) ->
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
			Filepath2 = filename:join([DataDir, ?CHUNK_DIR, Filepath]),
			ets:insert(chunk_storage_file_index, {{Key, StoreID}, Filepath2}),
			Filepath2
		end,
		FileIndex
	),
	warn_custom_chunk_group_size(StoreID),
	StoreIDLabel = ar_storage_module:label_by_id(StoreID),
	{ok, #state{
		file_index = FileIndex2, store_id = StoreID, store_id_label = StoreIDLabel }};
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
			ets:insert(chunk_storage_file_index, {{Key, StoreID}, Filepath}),
			Filepath
		end,
		FileIndex
	),
	warn_custom_chunk_group_size(StoreID),
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	StoreIDLabel = ar_storage_module:label_by_id(StoreID),

	State = #state{
		file_index = FileIndex2,
		store_id = StoreID,
		range_start = RangeStart,
		range_end = RangeEnd,
		store_id_label = StoreIDLabel
	},
	
	State2 = case RepackInPlacePacking of
		none ->
			State#state{
				repack_cursor = none,
				repack_status = off,
				target_packing = ar_storage_module:get_packing(StoreID)
			};
		Packing ->
			RepackCursor = ar_repack:read_cursor(StoreID, Packing, RangeStart),
			gen_server:cast(self(), {repack, Packing}),
			?LOG_INFO([{event, starting_repack_in_place},
					{tags, [repack_in_place]},
					{cursor, RepackCursor},
					{store_id, StoreID},
					{target_packing, ar_serialize:encode_packing(Packing, true)}]),
			State#state{ 
				repack_cursor = RepackCursor, 
				target_packing = Packing,
				repack_status = paused 
			}
	end,

	EntropyContext = ar_entropy_gen:initialize_context(StoreID, State2#state.target_packing),
	State3 = State2#state{ entropy_context = EntropyContext },

	{ok, State3}.

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

handle_cast(store_repack_cursor, #state{ repack_status = complete } = State) ->
	{noreply, State};
handle_cast(store_repack_cursor,
		#state{ repack_cursor = Cursor, store_id = StoreID,
				target_packing = TargetPacking } = State) ->
	ar_repack:store_cursor(Cursor, StoreID, TargetPacking),
	ar_entropy_gen:set_repack_cursor(StoreID, Cursor),
	{noreply, State};

handle_cast(repacking_complete, State) ->
	#state{ store_id = StoreID } = State,
	ar_device_lock:release_lock(repack, StoreID),
	{noreply, State#state{ repack_status = complete }};

handle_cast({repack, Packing},
		#state{ store_id = StoreID, repack_cursor = Cursor,
				range_start = RangeStart, range_end = RangeEnd } = State) ->
	NewStatus = ar_device_lock:acquire_lock(repack, StoreID, State#state.repack_status),
	State2 = State#state{ repack_status = NewStatus },
	case NewStatus of
		active ->
			spawn(fun() ->
				ar_repack:repack(Cursor, RangeStart, RangeEnd, Packing, StoreID) end);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(), {repack, Packing});
		_ ->
			ok
	end,
	{noreply, State2};

handle_cast({repack, Cursor, RangeStart, RangeEnd, Packing}, State) ->
	#state{ store_id = StoreID } = State,
	gen_server:cast(self(), store_repack_cursor),
	NewStatus = ar_device_lock:acquire_lock(repack, StoreID, State#state.repack_status),
	State2 = State#state{ repack_status = NewStatus, repack_cursor = Cursor },
	case NewStatus of
		active ->
			spawn(fun() ->
				ar_repack:repack(Cursor, RangeStart, RangeEnd, Packing, StoreID) end);
		paused ->
			ar_util:cast_after(?DEVICE_LOCK_WAIT, self(),
				{repack, Cursor, RangeStart, RangeEnd, Packing});
		_ ->
			ok
	end,
	{noreply, State2};

handle_cast({register_packing_ref, Ref, Args}, #state{ packing_map = Map } = State) ->
	{noreply, State#state{ packing_map = maps:put(Ref, Args, Map) }};

handle_cast({expire_repack_request, Ref}, #state{ packing_map = Map } = State) ->
	{noreply, State#state{ packing_map = maps:remove(Ref, Map) }};

handle_cast({set_entropy_context, EntropyContext}, State) ->
	{noreply, State#state{ entropy_context = EntropyContext }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call({put, PaddedEndOffset, Chunk, Packing}, _From, State)
		when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	#state{ store_id = StoreID, store_id_label = StoreIDLabel,
		entropy_context = EntropyContext, file_index = FileIndex } = State,

	{PackingLabel, State2} = get_packing_label(Packing, State),
	Result = store_chunk(
		PaddedEndOffset, Chunk, Packing, StoreID,
		StoreIDLabel, PackingLabel, FileIndex, EntropyContext),
	case Result of
		{ok, FileIndex2, NewPacking} ->
			{reply, {ok, NewPacking}, State2#state{ file_index = FileIndex2 }};
		Error ->
			{reply, Error, State2}
	end;

handle_call({delete, PaddedEndOffset}, _From, State) ->
	#state{	store_id = StoreID } = State,
	StartOffset = PaddedEndOffset - ?DATA_CHUNK_SIZE,
	case ar_sync_record:delete(PaddedEndOffset, StartOffset, ar_chunk_storage, StoreID) of
		ok ->
			case ar_entropy_storage:delete_record(PaddedEndOffset, StoreID) of
				ok ->
					case delete_chunk(PaddedEndOffset, StoreID) of
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
	ok = ar_sync_record:cut(0, ar_chunk_storage, StoreID),
	erlang:erase(),
	{reply, ok, State#state{ file_index = #{} }};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

%% @doc This is only called during repack_in_place. Called when a chunk has been repacked
%% from the old format to the new format and is ready to be stored in the chunk storage.
handle_info({chunk, {packed, Ref, ChunkArgs}},
	#state{ packing_map = Map } = State) ->
	case maps:get(Ref, Map, not_found) of
		not_found ->
			{Packing, _, Offset, _, ChunkSize} = ChunkArgs,
			?LOG_WARNING([{event, chunk_repack_request_not_found}, 
					{offset, Offset}, {chunk_size, ChunkSize},
					{packing, ar_serialize:encode_packing(Packing, true)}]),
			{noreply, State};
		Args ->
			State2 = State#state{ packing_map = maps:remove(Ref, Map) },
			#state{ store_id = StoreID, entropy_context = EntropyContext, 
				file_index = FileIndex } = State2,
			case ar_repack:chunk_repacked(
					ChunkArgs, Args, StoreID, FileIndex, EntropyContext) of
				{ok, FileIndex2} ->
					{noreply, State2#state{ file_index = FileIndex2 }};
				Error ->
					?LOG_ERROR([{event, failed_to_repack_chunk}, {error, Error}]),
					{noreply, State2}
			end
	end;

handle_info({Ref, _Reply}, State) when is_reference(Ref) ->
	?LOG_ERROR([{event, stale_gen_server_call_reply}, {ref, Ref}, {reply, _Reply}]),
	%% A stale gen_server:call reply.
	{noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
	{noreply, State};

handle_info({entropy_generated, _Ref, {error, Reason}}, State) ->
	?LOG_ERROR([{event, failed_to_generate_replica_2_9_entropy_and_timeout},
			{error, Reason}]),
	{noreply, State};
handle_info({entropy_generated, _Ref, _Entropy}, State) ->
	?LOG_WARNING([{event, entropy_generation_timed_out}]),
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, io_lib:format("~p", [Info])}]),
	{noreply, State}.

terminate(_Reason, #state{ repack_cursor = Cursor, store_id = StoreID,
		target_packing = TargetPacking }) ->
	sync_and_close_files(),
	ar_repack:store_cursor(Cursor, StoreID, TargetPacking),
	ar_entropy_gen:set_repack_cursor(StoreID, Cursor),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
get_chunk_group_size() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.chunk_storage_file_size.

get_filepath(Name, StoreID) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	ChunkDir = get_chunk_storage_path(DataDir, StoreID),
	filename:join([ChunkDir, Name]).

store_chunk(PaddedEndOffset, Chunk, Packing, StoreID, FileIndex, PrepareContext) ->
	StoreIDLabel = ar_storage_module:label_by_id(StoreID),
	PackingLabel = ar_storage_module:packing_label(Packing),
	store_chunk(PaddedEndOffset, Chunk, Packing, StoreID, 
		StoreIDLabel, PackingLabel, FileIndex, PrepareContext).	

store_chunk(
		PaddedEndOffset, Chunk, Packing, StoreID, StoreIDLabel,
		PackingLabel, FileIndex, PrepareContext) ->
	case ar_entropy_gen:is_entropy_packing(Packing) of
		true ->
			ar_entropy_storage:record_chunk(
				PaddedEndOffset, Chunk, StoreID,
				StoreIDLabel, PackingLabel, FileIndex, PrepareContext);
		false ->
			record_chunk(
				PaddedEndOffset, Chunk, Packing, StoreID, 
				StoreIDLabel, PackingLabel, FileIndex)
	end.

record_chunk(
		PaddedEndOffset, Chunk, Packing, StoreID, StoreIDLabel, PackingLabel, FileIndex) ->
	case write_chunk(PaddedEndOffset, Chunk, FileIndex, StoreID) of
		{ok, Filepath} ->
			prometheus_counter:inc(chunks_stored, [PackingLabel, StoreIDLabel]),
			case ar_sync_record:add(
					PaddedEndOffset, PaddedEndOffset - ?DATA_CHUNK_SIZE,
					sync_record_id(Packing), StoreID) of
				ok ->
					ChunkFileStart = get_chunk_file_start(PaddedEndOffset),
					ets:insert(chunk_storage_file_index,
						{{ChunkFileStart, StoreID}, Filepath}),
					{ok, maps:put(ChunkFileStart, Filepath, FileIndex), Packing};
				Error ->
					Error
			end;
		Error2 ->
			Error2
	end.

sync_record_id(unpacked_padded) ->
	%% Entropy indexing changed between 2.9.0 and 2.9.1. So we'll use a new
	%% sync_record id (ar_chunk_storage_replica_2_9_1_unpacked) going forward.
	%% The old id (ar_chunk_storage_replica_2_9_unpacked) should not be used.
	ar_chunk_storage_replica_2_9_1_unpacked;
sync_record_id(_Packing) ->
	ar_chunk_storage.

get_chunk_file_start(EndOffset) ->
	StartOffset = EndOffset - ?DATA_CHUNK_SIZE,
	get_chunk_file_start_by_start_offset(StartOffset).

get_chunk_file_start_by_start_offset(StartOffset) ->
	ar_util:floor_int(StartOffset, get_chunk_group_size()).

write_chunk(PaddedOffset, Chunk, FileIndex, StoreID) ->
	{_ChunkFileStart, Filepath, Position, ChunkOffset} =
		locate_chunk_on_disk(PaddedOffset, StoreID, FileIndex),
	case get_handle_by_filepath(Filepath) of
		{error, _} = Error ->
			Error;
		F ->
			write_chunk2(PaddedOffset, ChunkOffset, Chunk, Filepath, F, Position)
	end.

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

write_chunk2(_PaddedOffset, ChunkOffset, Chunk, Filepath, F, Position) ->
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
		{error, _Reason} = Error ->
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

delete_chunk(PaddedOffset, StoreID) ->
	{_ChunkFileStart, Filepath, Position, _ChunkOffset} =
		locate_chunk_on_disk(PaddedOffset, StoreID),
	case file:open(Filepath, [read, write, raw]) of
		{ok, F} ->
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
			ar_entropy_storage:acquire_semaphore(Filepath),
			Result = file:pwrite(F, Position, ZeroChunk),
			ar_entropy_storage:release_semaphore(Filepath),
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
	[];
extract_end_offset_chunk_pairs(<< ChunkOffset:?OFFSET_BIT_SIZE, Chunk/binary >>,
		BucketStart, Shift) ->
	?LOG_ERROR([{event, unexpected_chunk_data}, {chunk_offset, ChunkOffset},
			{bucket_start, BucketStart}, {shift, Shift}, {chunk_size, byte_size(Chunk)}]),
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

get_packing_label(Packing, State) ->
	case maps:get(Packing, State#state.packing_labels, not_found) of
		not_found ->
			Label = ar_storage_module:packing_label(Packing),
			Map = maps:put(Packing, Label, State#state.packing_labels),
			{Label, State#state{ packing_labels = Map }};
		Label ->
			{Label, State}
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

chunk_bucket_test() ->
	?assertEqual(786432, ?STRICT_DATA_SPLIT_THRESHOLD),

	%% get_chunk_bucket_end pads the provided offset
	%% get_chunk_bucket_start does not padd the provided offset

	%% At and before the STRICT_DATA_SPLIT_THRESHOLD, offsets are not padded.
	?assertEqual(262144, get_chunk_bucket_end(0)),
	?assertEqual(0, get_chunk_bucket_start(0)),

	?assertEqual(262144, get_chunk_bucket_end(1)),
	?assertEqual(0, get_chunk_bucket_start(1)),

	?assertEqual(262144, get_chunk_bucket_end(?DATA_CHUNK_SIZE - 1)),
	?assertEqual(0, get_chunk_bucket_start(?DATA_CHUNK_SIZE - 1)),

	?assertEqual(262144, get_chunk_bucket_end(?DATA_CHUNK_SIZE)),
	?assertEqual(0, get_chunk_bucket_start(?DATA_CHUNK_SIZE)),

	?assertEqual(262144, get_chunk_bucket_end(?DATA_CHUNK_SIZE + 1)),
	?assertEqual(0, get_chunk_bucket_start(?DATA_CHUNK_SIZE + 1)),

	?assertEqual(524288, get_chunk_bucket_end(2 * ?DATA_CHUNK_SIZE)),
	?assertEqual(262144, get_chunk_bucket_start(2 * ?DATA_CHUNK_SIZE)),

	?assertEqual(524288, get_chunk_bucket_end(2 * ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(262144, get_chunk_bucket_start(2 * ?DATA_CHUNK_SIZE + 1)),

	?assertEqual(524288, get_chunk_bucket_end(3 * ?DATA_CHUNK_SIZE - 1)),
	?assertEqual(262144, get_chunk_bucket_start(3 * ?DATA_CHUNK_SIZE - 1)),

	?assertEqual(786432, get_chunk_bucket_end(3 * ?DATA_CHUNK_SIZE)),
	?assertEqual(524288, get_chunk_bucket_start(3 * ?DATA_CHUNK_SIZE)),

	%% After the STRICT_DATA_SPLIT_THRESHOLD, offsets are padded.
	?assertEqual(1048576, get_chunk_bucket_end(3 * ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(524288, get_chunk_bucket_start(3 * ?DATA_CHUNK_SIZE + 1)),

	?assertEqual(1048576, get_chunk_bucket_end(4 * ?DATA_CHUNK_SIZE - 1)),
	?assertEqual(524288, get_chunk_bucket_start(4 * ?DATA_CHUNK_SIZE - 1)),

	?assertEqual(1048576, get_chunk_bucket_end(4 * ?DATA_CHUNK_SIZE)),
	?assertEqual(786432, get_chunk_bucket_start(4 * ?DATA_CHUNK_SIZE)),

	?assertEqual(1310720, get_chunk_bucket_end(4 * ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(786432, get_chunk_bucket_start(4 * ?DATA_CHUNK_SIZE + 1)),

	?assertEqual(1310720, get_chunk_bucket_end(5 * ?DATA_CHUNK_SIZE - 1)),
	?assertEqual(786432, get_chunk_bucket_start(5 * ?DATA_CHUNK_SIZE - 1)),

	?assertEqual(1310720, get_chunk_bucket_end(5 * ?DATA_CHUNK_SIZE)),
	?assertEqual(1048576, get_chunk_bucket_start(5 * ?DATA_CHUNK_SIZE)),

	?assertEqual(1572864, get_chunk_bucket_end(5 * ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(1048576, get_chunk_bucket_start(5 * ?DATA_CHUNK_SIZE + 1)),

	?assertEqual(1572864, get_chunk_bucket_end(6 * ?DATA_CHUNK_SIZE - 1)),
	?assertEqual(1048576, get_chunk_bucket_start(6 * ?DATA_CHUNK_SIZE - 1)),

	?assertEqual(1572864, get_chunk_bucket_end(6 * ?DATA_CHUNK_SIZE)),
	?assertEqual(1310720, get_chunk_bucket_start(6 * ?DATA_CHUNK_SIZE)),

	?assertEqual(1835008, get_chunk_bucket_end(6 * ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(1310720, get_chunk_bucket_start(6 * ?DATA_CHUNK_SIZE + 1)),
	
	?assertEqual(1835008, get_chunk_bucket_end(7 * ?DATA_CHUNK_SIZE)),
	?assertEqual(1572864, get_chunk_bucket_start(7 * ?DATA_CHUNK_SIZE)),

	?assertEqual(2097152, get_chunk_bucket_end(8 * ?DATA_CHUNK_SIZE)),
	?assertEqual(1835008, get_chunk_bucket_start(8 * ?DATA_CHUNK_SIZE)),

	?assertEqual(2359296, get_chunk_bucket_end(9 * ?DATA_CHUNK_SIZE)),
	?assertEqual(2097152, get_chunk_bucket_start(9 * ?DATA_CHUNK_SIZE)),

	?assertEqual(2621440, get_chunk_bucket_end(10 * ?DATA_CHUNK_SIZE)),
	?assertEqual(2359296, get_chunk_bucket_start(10 * ?DATA_CHUNK_SIZE)).
	
well_aligned_test_() ->
	{timeout, 20, fun test_well_aligned/0}.

test_well_aligned() ->
	clear("default"),
	Packing = ar_storage_module:get_packing("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	{ok, unpacked} = ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, Packing, "default"),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(?DATA_CHUNK_SIZE, C2, Packing, "default"),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1, Packing, "default"),
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
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C3, Packing, "default"),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C2, Packing, "default"),
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
	{timeout, 20, fun test_not_aligned/0}.

test_not_aligned() ->
	clear("default"),
	Packing = ar_storage_module:get_packing("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE + 7, C1, Packing, "default"),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE + 7),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE + 7, C1, Packing, "default"),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE + 7),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE + 7)),
	?assertEqual(not_found, ar_chunk_storage:get(?DATA_CHUNK_SIZE + 7 - 1)),
	?assertEqual(not_found, ar_chunk_storage:get(?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(?DATA_CHUNK_SIZE - 1)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(1)),
	ar_chunk_storage:put(?DATA_CHUNK_SIZE + 3, C2, Packing, "default"),
	assert_get(C2, ?DATA_CHUNK_SIZE + 3),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(1)),
	?assertEqual(not_found, ar_chunk_storage:get(2)),
	ar_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE + 7),
	assert_get(C2, ?DATA_CHUNK_SIZE + 3),
	assert_get(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE + 7, C3, Packing, "default"),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE + 7, C1, Packing, "default"),
	assert_get(C1, 3 * ?DATA_CHUNK_SIZE + 7),
	ar_chunk_storage:put(4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2, C2, Packing, "default"),
	assert_get(C2, 4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2),
	?assertEqual(
		not_found,
		ar_chunk_storage:get(4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2)
	),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 7)),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 8)),
	ar_chunk_storage:put(5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2, Packing, "default"),
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
	{timeout, 20, fun test_cross_file_aligned/0}.

test_cross_file_aligned() ->
	clear("default"),
	Packing = ar_storage_module:get_packing("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(get_chunk_group_size(), C1, Packing, "default"),
	assert_get(C1, get_chunk_group_size()),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size())),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() - ?DATA_CHUNK_SIZE - 1)),
	ar_chunk_storage:put(get_chunk_group_size() + ?DATA_CHUNK_SIZE, C2, Packing, "default"),
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
	ar_chunk_storage:put(get_chunk_group_size(), C2, Packing, "default"),
	assert_get(C2, get_chunk_group_size()).

cross_file_not_aligned_test_() ->
	{timeout, 20, fun test_cross_file_not_aligned/0}.

test_cross_file_not_aligned() ->
	clear("default"),
	Packing = ar_storage_module:get_packing("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(get_chunk_group_size() + 1, C1, Packing, "default"),
	assert_get(C1, get_chunk_group_size() + 1),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() - ?DATA_CHUNK_SIZE)),
	ar_chunk_storage:put(2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2, C2, Packing, "default"),
	assert_get(C2, 2 * get_chunk_group_size() + ?DATA_CHUNK_SIZE div 2),
	?assertEqual(not_found, ar_chunk_storage:get(get_chunk_group_size() + 1)),
	ar_chunk_storage:put(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2, C3, Packing, "default"),
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
	ar_chunk_storage:put(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2, C1, Packing, "default"),
	assert_get(C1, 2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2),
	?assertEqual(not_found,
			ar_chunk_storage:get(2 * get_chunk_group_size() - ?DATA_CHUNK_SIZE div 2)).

clear(StoreID) ->
	ok = gen_server:call(name(StoreID), reset).

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
