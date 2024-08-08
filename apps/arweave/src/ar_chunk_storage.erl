%%% The blob storage optimized for fast reads.
-module(ar_chunk_storage).

-behaviour(gen_server).

-export([start_link/2, encode_packing/1, put/2, put/3,
		open_files/1, get/1, get/2, get/5, read_chunk2/5, get_range/2, get_range/3,
		close_file/2, close_files/1, cut/2, delete/1, delete/2, 
		list_files/2, run_defragmentation/0]).

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
	repacking_complete = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

encode_packing({spora_2_6, Addr}) ->
	"spora_2_6_" ++ binary_to_list(ar_util:encode(Addr));
encode_packing({composite, Addr, PackingDifficulty}) ->
	"composite_" ++ binary_to_list(ar_util:encode(Addr)) ++ ":"
			++ integer_to_list(PackingDifficulty);
encode_packing(spora_2_5) ->
	"spora_2_5";
encode_packing(unpacked) ->
	"unpacked".

%% @doc Start the server.
start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIIZE, Offset - ?DATA_CHUNK_SIIZE + 1, .., Offset - 1.
put(Offset, Chunk) ->
	put(Offset, Chunk, "default").

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIIZE, Offset - ?DATA_CHUNK_SIIZE + 1, .., Offset - 1.
put(Offset, Chunk, StoreID) ->
	GenServerID = list_to_atom("ar_chunk_storage_" ++ ar_storage_module:label_by_id(StoreID)),
	case catch gen_server:call(GenServerID, {put, Offset, Chunk}) of
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
			Start = Byte - (Byte - IntervalStart) rem ?DATA_CHUNK_SIZE,
			LeftBorder = ar_util:floor_int(Start, get_chunk_group_size()),
			case get(Byte, Start, LeftBorder, StoreID, 1) of
				[] ->
					not_found;
				[{EndOffset, Chunk}] ->
					{EndOffset, Chunk}
			end
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
	case ar_sync_record:get_next_synced_interval(Start, infinity, ?MODULE, StoreID) of
		{_End, IntervalStart} when Start + Size > IntervalStart ->
			Start2 = max(Start, IntervalStart),
			Size2 = Start + Size - Start2,
			BucketStart = Start2 - (Start2 - IntervalStart) rem ?DATA_CHUNK_SIZE,
			LeftBorder = ar_util:floor_int(BucketStart, get_chunk_group_size()),
			End = Start2 + Size2,
			LastBucketStart = (End - 1) - ((End - 1)- IntervalStart) rem ?DATA_CHUNK_SIZE,
			case LastBucketStart >= LeftBorder + get_chunk_group_size() of
				false ->
					ChunkCount = (LastBucketStart - BucketStart) div ?DATA_CHUNK_SIZE + 1,
					get(Start2, BucketStart, LeftBorder, StoreID, ChunkCount);
				true ->
					SizeBeforeBorder = LeftBorder + get_chunk_group_size() - BucketStart,
					ChunkCountBeforeBorder = SizeBeforeBorder div ?DATA_CHUNK_SIZE
							+ case SizeBeforeBorder rem ?DATA_CHUNK_SIZE of 0 -> 0; _ -> 1 end,
					StartAfterBorder = BucketStart + ChunkCountBeforeBorder * ?DATA_CHUNK_SIZE,
					SizeAfterBorder = Size2 - ChunkCountBeforeBorder * ?DATA_CHUNK_SIZE
							+ (Start2 - BucketStart),
					get(Start2, BucketStart, LeftBorder, StoreID, ChunkCountBeforeBorder)
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
delete(Offset, StoreID) ->
	GenServerID = list_to_atom("ar_chunk_storage_" ++ ar_storage_module:label_by_id(StoreID)),
	case catch gen_server:call(GenServerID, {delete, Offset}, 20000) of
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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init({StoreID, RepackInPlacePacking}) ->
	%% Trap exit to avoid corrupting any open files on quit..
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	Dir =
		case StoreID of
			"default" ->
				DataDir;
			_ ->
				filename:join([DataDir, "storage_modules", StoreID])
		end,
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
	case RepackInPlacePacking of
		none ->
			{ok, #state{ file_index = FileIndex2, store_id = StoreID }};
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
			{ok, #state{ file_index = FileIndex2, store_id = StoreID,
					repack_cursor = Cursor, target_packing = Packing }}
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

handle_call({put, Offset, Chunk}, _From, State) when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	#state{ file_index = FileIndex, store_id = StoreID } = State,
	case handle_store_chunk(Offset, Chunk, FileIndex, StoreID) of
		{ok, FileIndex2} ->
			{reply, ok, State#state{ file_index = FileIndex2 }};
		Error ->
			{reply, Error, State}
	end;

handle_call({delete, Offset}, _From, State) ->
	#state{	file_index = FileIndex, store_id = StoreID } = State,
	Key = get_key(Offset),
	Filepath = filepath(Key, FileIndex, StoreID),
	case ar_sync_record:delete(Offset, Offset - ?DATA_CHUNK_SIZE, ?MODULE, StoreID) of
		ok ->
			case delete_chunk(Offset, Key, Filepath) of
				ok ->
					{reply, ok, State};
				Error2 ->
					{reply, Error2, State}
			end;
		Error ->
			{reply, Error, State}
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
	#state{ packing_map = Map, store_id = StoreID, file_index = FileIndex,
				repack_cursor = PrevCursor } = State) ->
	case maps:get(Ref, Map, not_found) of
		not_found ->
			{noreply, State};
		Offset ->
			State2 = State#state{ packing_map = maps:remove(Ref, Map) },
			{Packing, Chunk, _, _, _} = ChunkArgs,
			case ar_sync_record:delete(Offset, Offset - ?DATA_CHUNK_SIZE,
					ar_data_sync, StoreID) of
				ok ->
					case handle_store_chunk(Offset, Chunk, FileIndex, StoreID) of
						{ok, FileIndex2} ->
							case ar_sync_record:add(Offset, Offset - ?DATA_CHUNK_SIZE,
									Packing, ar_data_sync, StoreID) of
								ok ->
									?LOG_DEBUG([{event, repacked_chunk},
											{storage_module, StoreID},
											{offset, Offset},
											{packing, encode_packing(Packing)}]);
								Error ->
									?LOG_ERROR([{event, failed_to_record_repacked_chunk},
											{storage_module, StoreID},
											{offset, Offset},
											{packing, encode_packing(Packing)},
											{error, io_lib:format("~p", [Error])}])
							end,
							{noreply, State2#state{ file_index = FileIndex2,
									repack_cursor = Offset, prev_repack_cursor = PrevCursor }};
						Error2 ->
							?LOG_ERROR([{event, failed_to_store_repacked_chunk},
									{storage_module, StoreID},
									{offset, Offset},
									{packing, encode_packing(Packing)},
									{error, io_lib:format("~p", [Error2])}]),
							{noreply, State2}
					end;
				Error3 ->
					?LOG_ERROR([{event, failed_to_remove_repacked_chunk_from_sync_record},
							{storage_module, StoreID},
							{offset, Offset},
							{packing, encode_packing(Packing)},
							{error, io_lib:format("~p", [Error3])}]),
					{noreply, State2}
			end
	end;

handle_info({Ref, _Reply}, State) when is_reference(Ref) ->
	%% A stale gen_server:call reply.
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

get_filepath(Name, StoreID) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	case StoreID of
		"default" ->
			filename:join([DataDir, ?CHUNK_DIR, Name]);
		_ ->
			filename:join([DataDir, "storage_modules", StoreID, ?CHUNK_DIR, Name])
	end.

handle_store_chunk(Offset, Chunk, FileIndex, StoreID) ->
	Key = get_key(Offset),
	case store_chunk(Key, Offset, Chunk, FileIndex, StoreID) of
		{ok, Filepath} ->
			case ar_sync_record:add(Offset, Offset - ?DATA_CHUNK_SIZE, ?MODULE, StoreID) of
				ok ->
					ets:insert(chunk_storage_file_index, {{Key, StoreID}, Filepath}),
					{ok, maps:put(Key, Filepath, FileIndex)};
				Error ->
					Error
			end;
		Error2 ->
			Error2
	end.

get_key(Offset) ->
	StartOffset = Offset - ?DATA_CHUNK_SIZE,
	ar_util:floor_int(StartOffset, get_chunk_group_size()).

store_chunk(Key, Offset, Chunk, FileIndex, StoreID) ->
	Filepath = filepath(Key, FileIndex, StoreID),
	store_chunk(Key, Offset, Chunk, Filepath).

filepath(Key, FileIndex, StoreID) ->
	case maps:get(Key, FileIndex, not_found) of
		not_found ->
			get_filepath(integer_to_binary(Key), StoreID);
		Filepath ->
			Filepath
	end.

store_chunk(Key, Offset, Chunk, Filepath) ->
	case erlang:get({write_handle, Filepath}) of
		undefined ->
			case file:open(Filepath, [read, write, raw]) of
				{error, Reason} = Error ->
					?LOG_ERROR([
						{event, failed_to_open_chunk_file},
						{offset, Offset},
						{file, Filepath},
						{reason, io_lib:format("~p", [Reason])}
					]),
					Error;
				{ok, F} ->
					erlang:put({write_handle, Filepath}, F),
					store_chunk2(Key, Offset, Chunk, Filepath, F)
			end;
		F ->
			store_chunk2(Key, Offset, Chunk, Filepath, F)
	end.

store_chunk2(Key, Offset, Chunk, Filepath, F) ->
	StartOffset = Offset - ?DATA_CHUNK_SIZE,
	LeftChunkBorder = ar_util:floor_int(StartOffset, ?DATA_CHUNK_SIZE),
	ChunkOffset = StartOffset - LeftChunkBorder,
	RelativeOffset = LeftChunkBorder - Key,
	Position = RelativeOffset + ?OFFSET_SIZE * (RelativeOffset div ?DATA_CHUNK_SIZE),
	ChunkOffsetBinary =
		case ChunkOffset of
			0 ->
				%% Represent 0 as ?DATA_CHUNK_SIZE, to distinguish
				%% zero offset from not yet written data.
				<< (?DATA_CHUNK_SIZE):?OFFSET_BIT_SIZE >>;
			_ ->
				<< ChunkOffset:?OFFSET_BIT_SIZE >>
		end,
	case file:pwrite(F, Position, [ChunkOffsetBinary | Chunk]) of
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_write_chunk},
				{offset, Offset},
				{file, Filepath},
				{position, Position},
				{reason, io_lib:format("~p", [Reason])}
			]),
			Error;
		ok ->
			prometheus_counter:inc(chunks_stored),
			{ok, Filepath}
	end.

delete_chunk(Offset, Key, Filepath) ->
	case file:open(Filepath, [read, write, raw]) of
		{ok, F} ->
			StartOffset = Offset - ?DATA_CHUNK_SIZE,
			LeftChunkBorder = ar_util:floor_int(StartOffset, ?DATA_CHUNK_SIZE),
			RelativeOffset = LeftChunkBorder - Key,
			Position = RelativeOffset + ?OFFSET_SIZE * (RelativeOffset div ?DATA_CHUNK_SIZE),
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
			file:pwrite(F, Position, ZeroChunk);
		{error, enoent} ->
			ok;
		Error ->
			Error
	end.

get(Byte, Start, Key, StoreID, ChunkCount) ->
	case erlang:get({cfile, {Key, StoreID}}) of
		undefined ->
			case ets:lookup(chunk_storage_file_index, {Key, StoreID}) of
				[] ->
					[];
				[{_, Filepath}] ->
					read_chunk(Byte, Start, Key, Filepath, ChunkCount)
			end;
		File ->
			read_chunk2(Byte, Start, Key, File, ChunkCount)
	end.

read_chunk(Byte, Start, Key, Filepath, ChunkCount) ->
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
			Result = read_chunk2(Byte, Start, Key, File, ChunkCount),
			file:close(File),
			Result
	end.

read_chunk2(Byte, Start, Key, File, ChunkCount) ->
	LeftChunkBorder = ar_util:floor_int(Start, ?DATA_CHUNK_SIZE),
	RelativeOffset = LeftChunkBorder - Key,
	Position = RelativeOffset + ?OFFSET_SIZE * RelativeOffset div ?DATA_CHUNK_SIZE,
	read_chunk3(Byte, Position, LeftChunkBorder, File, ChunkCount).

read_chunk3(Byte, Position, LeftChunkBorder, File, ChunkCount) ->
	case file:pread(File, Position, (?DATA_CHUNK_SIZE + ?OFFSET_SIZE) * ChunkCount) of
		{ok, << ChunkOffset:?OFFSET_BIT_SIZE, _Chunk/binary >> = Bin} ->
			case is_offset_valid(Byte, LeftChunkBorder, ChunkOffset) of
				true ->
					split_binary(Bin, LeftChunkBorder, 1);
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

split_binary(<< 0:?OFFSET_BIT_SIZE, _ZeroChunk:?DATA_CHUNK_SIZE/binary, Rest/binary >>,
		LeftChunkBorder, N) ->
	split_binary(Rest, LeftChunkBorder, N + 1);
split_binary(<< ChunkOffset:?OFFSET_BIT_SIZE, Chunk:?DATA_CHUNK_SIZE/binary, Rest/binary >>,
		LeftChunkBorder, N) ->
	EndOffset = LeftChunkBorder + (ChunkOffset rem ?DATA_CHUNK_SIZE) + (?DATA_CHUNK_SIZE * N),
	[{EndOffset, Chunk} | split_binary(Rest, LeftChunkBorder, N + 1)];
split_binary(<<>>, _LeftChunkBorder, _N) ->
	[].

is_offset_valid(_Byte, _LeftChunkBorder, 0) ->
	%% 0 is interpreted as "data has not been written yet".
	false;
is_offset_valid(Byte, LeftChunkBorder, ChunkOffset) ->
	Delta = Byte - (LeftChunkBorder + ChunkOffset rem ?DATA_CHUNK_SIZE),
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
	Dir =
		case StoreID of
			"default" ->
				DataDir;
			_ ->
				filename:join([DataDir, "storage_modules", StoreID])
		end,
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
			ar:console("~n~nRepacking is complete! We suggest you stop the node, rename "
					"the storage module folder to reflect the new packing, and start the "
					"node with the new storage module.~n", []),
			?LOG_INFO([{event, repacking_complete},
					{storage_module, StoreID},
					{target_packing, encode_packing(Packing)}]),
			Server = list_to_atom("ar_chunk_storage_"
					++ ar_storage_module:label_by_id(StoreID)),
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
	Server = list_to_atom("ar_chunk_storage_" ++ ar_storage_module:label_by_id(StoreID)),
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
						PaddedOffset = ar_data_sync:get_chunk_padded_offset(AbsoluteOffset),
						case ar_sync_record:is_recorded(PaddedOffset, ar_data_sync, StoreID) of
							{true, RequiredPacking} ->
								?LOG_WARNING([{event,
											repacking_process_chunk_already_repacked},
										{storage_module, StoreID},
										{packing, encode_packing(RequiredPacking)},
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

%%%===================================================================
%%% Tests.
%%%===================================================================

well_aligned_test_() ->
	{timeout, 20, fun test_well_aligned/0}.

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
	{timeout, 20, fun test_not_aligned/0}.

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
	{timeout, 20, fun test_cross_file_aligned/0}.

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
	{timeout, 20, fun test_cross_file_not_aligned/0}.

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

clear(StoreID) ->
	GenServerID = list_to_atom("ar_chunk_storage_" ++ ar_storage_module:label_by_id(StoreID)),
	ok = gen_server:call(GenServerID, reset).

assert_get(Expected, Offset) ->
	ExpectedResult =
		case Expected of
			not_found ->
				not_found;
			_ ->
				{Offset, Expected}
		end,
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - 1)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - 2)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 2)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 + 1)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 - 1)),
	?assertEqual(ExpectedResult, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 3)).

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
