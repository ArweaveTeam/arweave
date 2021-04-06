%%% The blob storage optimized for fast reads.
-module(ar_chunk_storage).

-behaviour(gen_server).

-export([
	start_link/0,
	put/2,
	open_files/0,
	get/1,
	has_chunk/1,
	close_files/0,
	cut/1,
	delete/1,
	repair_chunk/2
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

-include_lib("eunit/include/eunit.hrl").

-record(state, {
	file_index
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIIZE, Offset - ?DATA_CHUNK_SIIZE + 1, .., Offset - 1.
put(Offset, Chunk) ->
	case catch gen_server:call(?MODULE, {put, Offset, Chunk}) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%% @doc Open all the storage files. The subsequent calls to get/1 in the
%% caller process will use the opened file descriptors.
open_files() ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	ets:foldl(
		fun({Key, Filename}, _) ->
			case erlang:get({cfile, Key}) of
				undefined ->
					Filepath = filename:join([DataDir, ?CHUNK_DIR, Filename]),
					case file:open(Filepath, [read, raw, binary]) of
						{ok, F} ->
							erlang:put({cfile, Key}, F);
						_ ->
							ok
					end;
				_ ->
					ok
			end
		end,
		ok,
		chunk_storage_file_index
	).

%% @doc Return {absolute end offset, chunk} for the chunk containing the given byte.
get(Byte) ->
	case ar_sync_record:get_interval(Byte + 1, ?MODULE) of
		not_found ->
			not_found;
		{_End, IntervalStart} ->
			Start = Byte - (Byte - IntervalStart) rem ?DATA_CHUNK_SIZE,
			LeftBorder = Start - Start rem ?CHUNK_GROUP_SIZE,
			get(Byte, Start, LeftBorder)
	end.

%% @doc Return true if the storage contains the given byte.
has_chunk(Byte) ->
	ar_sync_record:is_recorded(Byte + 1, ?MODULE).

%% @doc Close the files opened by open_files/1.
close_files() ->
	close_files(erlang:get_keys()).

%% @doc Soft-delete everything above the given end offset.
cut(Offset) ->
	gen_server:cast(?MODULE, {cut, Offset}).

%% @doc Remove the chunk with the given end offset.
delete(Offset) ->
	gen_server:call(?MODULE, {delete, Offset}, 10000).

%% @doc Recover from the bugs in 2.1 where either recently stored chunks would
%% not be recorded in the sync record on shutdown because the server was not set
%% up to trap exit signals or recent chunks would be removed from the storage
%% after a chain reorg but stay recorded as synced.
repair_chunk(Offset, DataPath) ->
	gen_server:call(?MODULE, {repair_chunk, Offset, DataPath}, infinity).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	ok = filelib:ensure_dir(filename:join(DataDir, ?CHUNK_DIR) ++ "/"),
	FileIndex = read_state(),
	maps:map(
		fun(Key, Filename) ->
			ets:insert(chunk_storage_file_index, {Key, Filename})
		end,
		FileIndex
	),
	{ok, _} =
		timer:apply_interval(
			?STORE_CHUNK_STORAGE_STATE_FREQUENCY_MS,
			gen_server,
			cast,
			[?MODULE, store_state]
		),
	{ok, #state{ file_index = FileIndex }}.

handle_cast(store_state, State) ->
	store_state(State),
	{noreply, State};

handle_cast({cut, Offset}, State) ->
	ok = ar_sync_record:cut(Offset, ?MODULE),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_call({put, Offset, Chunk}, _From, State) when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	#state{ file_index = FileIndex } = State,
	Key = get_key(Offset),
	{Reply, FileIndex2} =
		case store_chunk(Key, Offset, Chunk, FileIndex) of
			{ok, Filename} ->
				ok = ar_sync_record:add(Offset, Offset - ?DATA_CHUNK_SIZE, ?MODULE),
				ets:insert(chunk_storage_file_index, {Key, Filename}),
				{ok, maps:put(Key, Filename, FileIndex)};
			Error ->
				{Error, FileIndex}
		end,
	{reply, Reply, State#state{ file_index = FileIndex2 }};

handle_call({delete, Offset}, _From, State) ->
	#state{	file_index = FileIndex } = State,
	Key = get_key(Offset),
	Filename = filename(Key, FileIndex),
	ok = ar_sync_record:delete(Offset, Offset - ?DATA_CHUNK_SIZE, ?MODULE),
	case delete_chunk(Offset, Key, Filename) of
		ok ->
			{reply, ok, State};
		Error ->
			{reply, Error, State}
	end;

handle_call({repair_chunk, Offset, DataPath}, _From, State) ->
	Start = Offset - ?DATA_CHUNK_SIZE,
	LeftBorder = Start - Start rem ?CHUNK_GROUP_SIZE,
	case get(Offset - 1, Start, LeftBorder) of
		not_found ->
			{reply, {ok, removed}, State};
		{Offset, Chunk} ->
			case binary:match(DataPath, crypto:hash(sha256, Chunk)) of
				nomatch ->
					{reply, {ok, removed}, State};
				_ ->
					ok = ar_sync_record:add(Offset, Offset - ?DATA_CHUNK_SIZE, ?MODULE),
					{reply, {ok, synced}, State}
			end;
		_ ->
			{reply, {ok, removed}, State}
	end;

handle_call(reset, _, #state{ file_index = FileIndex }) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	DataDir = "data_test_master",
	ChunkDir = filename:join(DataDir, ?CHUNK_DIR),
	maps:map(
		fun(_Key, Filename) ->
			file:delete(filename:join(ChunkDir, Filename))
		end,
		FileIndex
	),
	ok = ar_sync_record:cut(0, ?MODULE),
	erlang:erase(),
	{reply, ok, #state{ file_index = #{} }};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, io_lib:format("~p", [Info])}]),
	{noreply, State}.

terminate(_Reason, State) ->
	sync_and_close_files(),
	store_state(State),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_key(Offset) ->
	StartOffset = Offset - ?DATA_CHUNK_SIZE,
	StartOffset - StartOffset rem ?CHUNK_GROUP_SIZE.

store_chunk(Key, Offset, Chunk, FileIndex) ->
	Filename = filename(Key, FileIndex),
	{ok, Config} = application:get_env(arweave, config),
	Dir = filename:join(Config#config.data_dir, ?CHUNK_DIR),
	Filepath = filename:join(Dir, Filename),
	store_chunk(Key, Offset, Chunk, Filename, Filepath).

filename(Key, FileIndex) ->
	case maps:get(Key, FileIndex, not_found) of
		not_found ->
			integer_to_binary(Key);
		Filename ->
			Filename
	end.

store_chunk(Key, Offset, Chunk, Filename, Filepath) ->
	case erlang:get({write_handle, Filename}) of
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
					erlang:put({write_handle, Filename}, F),
					store_chunk2(Key, Offset, Chunk, Filename, Filepath, F)
			end;
		F ->
			store_chunk2(Key, Offset, Chunk, Filename, Filepath, F)
	end.

store_chunk2(Key, Offset, Chunk, Filename, Filepath, F) ->
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
			{ok, Filename}
	end.

delete_chunk(Offset, Key, Filename) ->
	{ok, Config} = application:get_env(arweave, config),
	Dir = filename:join(Config#config.data_dir, ?CHUNK_DIR),
	Filepath = filename:join(Dir, Filename),
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

get(Byte, Start, Key) ->
	case erlang:get({cfile, Key}) of
		undefined ->
			case ets:lookup(chunk_storage_file_index, Key) of
				[] ->
					not_found;
				[{_, Filename}] ->
					read_chunk(Byte, Start, Key, Filename)
			end;
		File ->
			read_chunk2(Byte, Start, Key, File)
	end.

read_chunk(Byte, Start, Key, Filename) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	Filepath = filename:join([DataDir, ?CHUNK_DIR, Filename]),
	case file:open(Filepath, [read, raw, binary]) of
		{error, enoent} ->
			not_found;
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_open_chunk_file},
				{byte, Byte},
				{reason, io_lib:format("~p", [Reason])}
			]),
			not_found;
		{ok, File} ->
			Result = read_chunk2(Byte, Start, Key, File),
			file:close(File),
			Result
	end.

read_chunk2(Byte, Start, Key, File) ->
	LeftChunkBorder = Start - Start rem ?DATA_CHUNK_SIZE,
	RelativeOffset = LeftChunkBorder - Key,
	Position = RelativeOffset + ?OFFSET_SIZE * RelativeOffset div ?DATA_CHUNK_SIZE,
	read_chunk3(Byte, Position, LeftChunkBorder, File).

read_chunk3(Byte, Position, LeftChunkBorder, File) ->
	case file:pread(File, Position, ?DATA_CHUNK_SIZE + ?OFFSET_SIZE) of
		{ok, << ChunkOffset:?OFFSET_BIT_SIZE, Chunk/binary >>} ->
			case is_offset_valid(Byte, LeftChunkBorder, ChunkOffset) of
				true ->
					EndOffset =
						LeftChunkBorder + (ChunkOffset rem ?DATA_CHUNK_SIZE) + ?DATA_CHUNK_SIZE,
					{EndOffset, Chunk};
				false ->
					not_found
			end;
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_read_chunk},
				{byte, Byte},
				{position, Position},
				{reason, io_lib:format("~p", [Reason])}
			]),
			not_found;
		eof ->
			not_found
	end.

is_offset_valid(_Byte, _LeftChunkBorder, 0) ->
	%% 0 is interpreted as "data has not been written yet".
	false;
is_offset_valid(Byte, LeftChunkBorder, ChunkOffset) ->
	 Diff = Byte - (LeftChunkBorder + ChunkOffset rem ?DATA_CHUNK_SIZE),
	 Diff >= 0 andalso Diff < ?DATA_CHUNK_SIZE.

close_files([{cfile, Key} | Keys]) ->
	file:close(erlang:get({cfile, Key})),
	close_files(Keys);
close_files([_ | Keys]) ->
	close_files(Keys);
close_files([]) ->
	ok.

read_state() ->
	case ar_storage:read_term(chunk_storage_index) of
		{ok, {SyncRecord, FileIndex}} ->
			ok = ar_sync_record:set(SyncRecord, ?MODULE),
			FileIndex;
		{ok, FileIndex} ->
			FileIndex;
		not_found ->
			#{}
	end.

store_state(#state{ file_index = FileIndex }) ->
	case ar_storage:write_term(chunk_storage_index, FileIndex) of
		{error, Reason} ->
			?LOG_ERROR([
				{event, chunk_storage_failed_to_persist_state},
				{reason, io_lib:format("~p", [Reason])}
			]);
		ok ->
			ok
	end.

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

%%%===================================================================
%%% Tests.
%%%===================================================================

well_aligned_test_() ->
	{timeout, 20, fun test_well_aligned/0}.

test_well_aligned() ->
	clear(),
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
	assert_get(C2, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE).

not_aligned_test_() ->
	{timeout, 20, fun test_not_aligned/0}.

test_not_aligned() ->
	clear(),
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
	assert_get(C1, 3 * ?DATA_CHUNK_SIZE + 7).

cross_file_aligned_test_() ->
	{timeout, 20, fun test_cross_file_aligned/0}.

test_cross_file_aligned() ->
	clear(),
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
	clear(),
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
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE div 2)).

clear() ->
	ok = gen_server:call(?MODULE, reset).

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
