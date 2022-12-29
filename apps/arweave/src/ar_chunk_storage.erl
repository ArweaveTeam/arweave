%%% The blob storage optimized for fast reads.
-module(ar_chunk_storage).

-behaviour(gen_server).

-export([start_link/2, put/2, put/3, open_files/1, get/1, get/2, get_range/2, get_range/3,
		close_file/2, close_files/1, cut/2, delete/1, delete/2, run_defragmentation/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
	file_index,
	store_id
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

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
	GenServerID = list_to_atom("ar_chunk_storage_" ++ StoreID),
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
				case erlang:get({cfile, Key}) of
					undefined ->
						case file:open(Filepath, [read, raw, binary]) of
							{ok, F} ->
								erlang:put({cfile, Key}, F);
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
			LeftBorder = Start - Start rem ?CHUNK_GROUP_SIZE,
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
			LeftBorder = BucketStart - BucketStart rem ?CHUNK_GROUP_SIZE,
			End = Start2 + Size2,
			LastBucketStart = (End - 1) - ((End - 1)- IntervalStart) rem ?DATA_CHUNK_SIZE,
			case LastBucketStart >= LeftBorder + ?CHUNK_GROUP_SIZE of
				false ->
					ChunkCount = (LastBucketStart - BucketStart) div ?DATA_CHUNK_SIZE + 1,
					get(Start2, BucketStart, LeftBorder, StoreID, ChunkCount);
				true ->
					SizeBeforeBorder = LeftBorder + ?CHUNK_GROUP_SIZE - BucketStart,
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
	GenServerID = list_to_atom("ar_chunk_storage_" ++ StoreID),
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
			ar:console("Defragmentation threshold ~B bytes~n",
					   [Config#config.defragmentation_trigger_threshold]),
			Files = files_to_defrag(Config#config.storage_modules,
									Config#config.data_dir,
									Config#config.defragmentation_trigger_threshold),
			defrag_files(Files)
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(StoreID) ->
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
	{ok, #state{ file_index = FileIndex2, store_id = StoreID }}.

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_call({put, Offset, Chunk}, _From, State) when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	#state{ file_index = FileIndex, store_id = StoreID } = State,
	Key = get_key(Offset),
	{Reply, FileIndex2} =
		case store_chunk(Key, Offset, Chunk, FileIndex, StoreID) of
			{ok, Filepath} ->
				case ar_sync_record:add(Offset, Offset - ?DATA_CHUNK_SIZE, ?MODULE, StoreID) of
					ok ->
						ets:insert(chunk_storage_file_index, {{Key, StoreID}, Filepath}),
						{ok, maps:put(Key, Filepath, FileIndex)};
					Error ->
						{Error, FileIndex}
				end;
			Error2 ->
				{Error2, FileIndex}
		end,
	{reply, Reply, State#state{ file_index = FileIndex2 }};

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
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_info({Ref, _Reply}, State) when is_reference(Ref) ->
	%% A stale gen_server:call reply.
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, io_lib:format("~p", [Info])}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	sync_and_close_files(),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_key(Offset) ->
	StartOffset = Offset - ?DATA_CHUNK_SIZE,
	StartOffset - StartOffset rem ?CHUNK_GROUP_SIZE.

store_chunk(Key, Offset, Chunk, FileIndex, StoreID) ->
	Filepath = filepath(Key, FileIndex, StoreID),
	store_chunk(Key, Offset, Chunk, Filepath).

filepath(Key, FileIndex, StoreID) ->
	case maps:get(Key, FileIndex, not_found) of
		not_found ->
			{ok, Config} = application:get_env(arweave, config),
			DataDir = Config#config.data_dir,
			Name = integer_to_binary(Key),
			case StoreID of
				"default" ->
					filename:join([DataDir, ?CHUNK_DIR, Name]);
				_ ->
					filename:join([DataDir, "storage_modules", StoreID, ?CHUNK_DIR, Name])
			end;
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
	LeftChunkBorder = StartOffset - StartOffset rem ?DATA_CHUNK_SIZE,
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
			{ok, Filepath}
	end.

delete_chunk(Offset, Key, Filepath) ->
	case file:open(Filepath, [read, write, raw]) of
		{ok, F} ->
			StartOffset = Offset - ?DATA_CHUNK_SIZE,
			LeftChunkBorder = StartOffset - StartOffset rem ?DATA_CHUNK_SIZE,
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
	LeftChunkBorder = Start - Start rem ?DATA_CHUNK_SIZE,
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
	 Diff = Byte - (LeftChunkBorder + ChunkOffset rem ?DATA_CHUNK_SIZE),
	 Diff >= 0 andalso Diff < ?DATA_CHUNK_SIZE.

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

files_to_defrag(StorageModules, DataDir, ByteSizeThreshold) ->
	%% TODO: This gets all files that are at least of ByteSizeThreshold,
	%% 		 but it is missing a filter based on file size growth so it is not
	%% 	     included if it hasn't grown beyond a threshold
	AllFiles = lists:flatmap(
		fun (StorageModule) ->
			StorageId = ar_storage_module:id(StorageModule),
			Dir =
				case StorageId of
					"default" ->
						DataDir;
					_ ->
						filename:join([DataDir, "storage_modules", StorageId])
				end,
			StorageIndex = read_file_index(Dir),
			maps:values(StorageIndex)
		end, StorageModules),
	lists:filter(
		fun (Filepath) ->
			case file:read_file_info(Filepath) of
				{ok, #file_info{size = Size}} ->
					Size >= ByteSizeThreshold;
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
	TmpFilepath = Filepath ++ ".tmp",
	DefragCmd = io_lib:format("rsync --sparse --quiet ~ts ~ts", [Filepath, TmpFilepath]),
	MoveDefragCmd = io_lib:format("mv ~ts ~ts", [TmpFilepath, Filepath]),
	%% We expect nothing to be returned on successful calls
	[] = os:cmd(DefragCmd),
	[] = os:cmd(MoveDefragCmd),
	defrag_files(Rest).

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
	ar_chunk_storage:put(?CHUNK_GROUP_SIZE, C1),
	assert_get(C1, ?CHUNK_GROUP_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE + 1)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE - 1)),
	ar_chunk_storage:put(?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE, C2),
	assert_get(C2, ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE),
	assert_get(C1, ?CHUNK_GROUP_SIZE),
	?assertEqual([{?CHUNK_GROUP_SIZE, C1}, {?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE, C2}],
			ar_chunk_storage:get_range(?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE,
					2 * ?DATA_CHUNK_SIZE)),
	?assertEqual([{?CHUNK_GROUP_SIZE, C1}, {?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE, C2}],
			ar_chunk_storage:get_range(?CHUNK_GROUP_SIZE - 2 * ?DATA_CHUNK_SIZE - 1,
					4 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(0)),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE - 1)),
	ar_chunk_storage:delete(?CHUNK_GROUP_SIZE),
	assert_get(not_found, ?CHUNK_GROUP_SIZE),
	assert_get(C2, ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(?CHUNK_GROUP_SIZE, C2),
	assert_get(C2, ?CHUNK_GROUP_SIZE).

cross_file_not_aligned_test_() ->
	{timeout, 20, fun test_cross_file_not_aligned/0}.

test_cross_file_not_aligned() ->
	clear("default"),
	C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(?CHUNK_GROUP_SIZE + 1, C1),
	assert_get(C1, ?CHUNK_GROUP_SIZE + 1),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE + 1)),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE)),
	ar_chunk_storage:put(2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2, C2),
	assert_get(C2, 2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE + 1)),
	ar_chunk_storage:put(2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2, C3),
	assert_get(C2, 2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2),
	assert_get(C3, 2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2),
	?assertEqual([{2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2, C3},
			{2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2, C2}],
			ar_chunk_storage:get_range(2 * ?CHUNK_GROUP_SIZE
					- ?DATA_CHUNK_SIZE div 2 - ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE * 2)),
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE + 1)),
	?assertEqual(
		not_found,
		ar_chunk_storage:get(?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2 - 1)
	),
	ar_chunk_storage:delete(2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2),
	assert_get(not_found, 2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2),
	assert_get(C2, 2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2),
	assert_get(C1, ?CHUNK_GROUP_SIZE + 1),
	ar_chunk_storage:delete(?CHUNK_GROUP_SIZE + 1),
	assert_get(not_found, ?CHUNK_GROUP_SIZE + 1),
	assert_get(not_found, 2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2),
	assert_get(C2, 2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2),
	ar_chunk_storage:delete(2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2),
	assert_get(not_found, 2 * ?CHUNK_GROUP_SIZE + ?DATA_CHUNK_SIZE div 2),
	ar_chunk_storage:delete(?CHUNK_GROUP_SIZE + 1),
	ar_chunk_storage:delete(100 * ?CHUNK_GROUP_SIZE + 1),
	ar_chunk_storage:put(2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2, C1),
	assert_get(C1, 2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2),
	?assertEqual(not_found,
			ar_chunk_storage:get(2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2)).

clear(StoreID) ->
	GenServerID = list_to_atom("ar_chunk_storage_" ++ StoreID),
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
