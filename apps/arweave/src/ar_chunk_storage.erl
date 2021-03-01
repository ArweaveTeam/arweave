%%% The blob storage optimized for fast reads.
-module(ar_chunk_storage).

-behaviour(gen_server).

-export([
	start_link/0,
	put/2, put/3,
	open_files/0,
	get/1,
	has_chunk/1,
	close_files/0,
	cut/1
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Store the chunk under the given end offset,
%% bytes Offset - ?DATA_CHUNK_SIIZE, Offset - ?DATA_CHUNK_SIIZE + 1, .., Offset - 1.
put(Offset, Chunk) ->
	put(Offset, Chunk, infinity).

put(Offset, Chunk, Timeout) ->
	gen_server:call(?MODULE, {put, Offset, Chunk}, Timeout).

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

%% @doc Return the chunk containing the given byte.
get(Byte) ->
	case ar_ets_intervals:get_interval_with_byte(chunk_storage_sync_record, Byte + 1) of
		not_found ->
			not_found;
		{_End, IntervalStart} ->
			Start = Byte - (Byte - IntervalStart) rem ?DATA_CHUNK_SIZE,
			LeftBorder = Start - Start rem ?CHUNK_GROUP_SIZE,
			get(Byte, Start, LeftBorder)
	end.

%% @doc Return true if the storage contains the given byte.
has_chunk(Byte) ->
	ar_ets_intervals:is_inside(chunk_storage_sync_record, Byte + 1).

%% @doc Close the files opened by open_files/1.
close_files() ->
	close_files(erlang:get_keys()).

%% @doc Soft-delete everything above the given end offset.
cut(Offset) ->
	gen_server:cast(?MODULE, {cut, Offset}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	ok = filelib:ensure_dir(filename:join(DataDir, ?CHUNK_DIR) ++ "/"),
	{SyncRecord, FileIndex} = read_state(),
	ar_ets_intervals:init_from_gb_set(chunk_storage_sync_record, SyncRecord),
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
	{ok, #state{ sync_record = SyncRecord, file_index = FileIndex }}.

handle_cast(store_state, State) ->
	store_state(State),
	{noreply, State};

handle_cast({cut, Offset}, State) ->
	#state{ sync_record = SyncRecord } = State,
	ar_ets_intervals:cut(chunk_storage_sync_record, Offset),
	{noreply, State#state{ sync_record = ar_intervals:cut(SyncRecord, Offset) }};

handle_cast(reset, #state{ file_index = FileIndex }) ->
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
	ets:delete_all_objects(chunk_storage_sync_record),
	{noreply, #state{ sync_record = ar_intervals:new(), file_index = #{} }}.

handle_call({put, Offset, Chunk}, _From, State) when byte_size(Chunk) == ?DATA_CHUNK_SIZE ->
	#state{ sync_record = SyncRecord, file_index = FileIndex } = State,
	Key = get_key(Offset),
	{Reply, SyncRecord2, FileIndex2} =
		case store_chunk(Key, Offset, Chunk, FileIndex) of
			{ok, Filename} ->
				ar_ets_intervals:add(
					chunk_storage_sync_record,
					Offset,
					Offset - ?DATA_CHUNK_SIZE
				),
				ets:insert(chunk_storage_file_index, {Key, Filename}),
				{ok, ar_intervals:add(SyncRecord, Offset, Offset - ?DATA_CHUNK_SIZE),
					maps:put(Key, Filename, FileIndex)};
			Error ->
				{Error, SyncRecord, FileIndex}
		end,
	{reply, Reply, State#state{ sync_record = SyncRecord2, file_index = FileIndex2 }}.

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {info, io_lib:format("~p", [Info])}]),
	{noreply, State}.

terminate(_Reason, State) ->
	store_state(State),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_key(Offset) ->
	StartOffset = Offset - ?DATA_CHUNK_SIZE,
	StartOffset - StartOffset rem ?CHUNK_GROUP_SIZE.

store_chunk(Key, Offset, Chunk, FileIndex) ->
	Filename =
		case maps:get(Key, FileIndex, not_found) of
			not_found ->
				filename(Key);
			F ->
				F
		end,
	{ok, Config} = application:get_env(arweave, config),
	Dir = filename:join(Config#config.data_dir, ?CHUNK_DIR),
	Filepath = filename:join(Dir, Filename),
	store_chunk(Key, Offset, Chunk, Filename, Filepath).

filename(Key) ->
	integer_to_binary(Key).

store_chunk(Key, Offset, Chunk, Filename, Filepath) ->
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
			store_chunk2(Key, Offset, Chunk, Filename, Filepath, F)
	end.

store_chunk2(Key, Offset, Chunk, Filename, Filepath, F) ->
	StartOffset = Offset - ?DATA_CHUNK_SIZE,
	LeftChunkBorder = StartOffset - StartOffset rem ?DATA_CHUNK_SIZE,
	ChunkOffset = StartOffset - LeftChunkBorder,
	RelativeOffset = LeftChunkBorder - Key,
	Position = RelativeOffset + ?OFFSET_SIZE * (RelativeOffset div ?DATA_CHUNK_SIZE),
	case file:position(F, {bof, Position}) of
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_position_file_cursor},
				{offset, Offset},
				{file, Filepath},
				{position, Position},
				{reason, io_lib:format("~p", [Reason])}
			]),
			file:close(F),
			Error;
		{ok, Position} ->
			store_chunk3(Offset, Chunk, Filename, Filepath, F, Position, ChunkOffset);
		{ok, WrongPosition} ->
			?LOG_ERROR([
				{event, failed_to_position_file_cursor},
				{offset, Offset},
				{file, Filepath},
				{position, Position},
				{got_position, WrongPosition}
			]),
			file:close(F),
			{error, failed_to_position_file_cursor}
	end.

store_chunk3(Offset, Chunk, Filename, Filepath, F, Position, ChunkOffset) ->
	ChunkOffsetBinary =
		case ChunkOffset of
			0 ->
				%% Represent 0 as ?DATA_CHUNK_SIZE, to distinguish
				%% zero offset from not yet written data.
				<< (?DATA_CHUNK_SIZE):?OFFSET_BIT_SIZE >>;
			_ ->
				<< ChunkOffset:?OFFSET_BIT_SIZE >>
		end,
	case file:write(F, [ChunkOffsetBinary | Chunk]) of
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_write_chunk},
				{offset, Offset},
				{file, Filepath},
				{position, Position},
				{reason, io_lib:format("~p", [Reason])}
			]),
			file:close(F),
			Error;
		ok ->
			store_chunk4(Offset, Filename, Filepath, F)
	end.

store_chunk4(Offset, Filename, Filepath, F) ->
	case file:sync(F) of
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_fsync_chunk},
				{offset, Offset},
				{file, Filepath},
				{reason, io_lib:format("~p", [Reason])}
			]),
			file:close(F),
			Error;
		ok ->
			file:close(F),
			{ok, Filename}
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
					Chunk;
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
			{SyncRecord, FileIndex};
		not_found ->
			{ar_intervals:new(), #{}}
	end.

store_state(#state{ sync_record = SyncRecord, file_index = FileIndex }) ->
	case ar_storage:write_term(chunk_storage_index, {SyncRecord, FileIndex}) of
		{error, Reason} ->
			?LOG_ERROR([
				{event, chunk_storage_failed_to_persist_state},
				{reason, io_lib:format("~p", [Reason])}
			]);
		ok ->
			ok
	end.

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
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C1),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(2 * ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:put(?DATA_CHUNK_SIZE, C2),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	ar_chunk_storage:put(3 * ?DATA_CHUNK_SIZE, C3),
	assert_get(C2, ?DATA_CHUNK_SIZE),
	assert_get(C1, 2 * ?DATA_CHUNK_SIZE),
	assert_get(C3, 3 * ?DATA_CHUNK_SIZE),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE)),
	?assertEqual(not_found, ar_chunk_storage:get(3 * ?DATA_CHUNK_SIZE + 1)),
	ar_chunk_storage:put(2 * ?DATA_CHUNK_SIZE, C2),
	assert_get(C2, ?DATA_CHUNK_SIZE),
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
	assert_get(C2, 5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1).

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
	?assertEqual(not_found, ar_chunk_storage:get(?CHUNK_GROUP_SIZE - ?DATA_CHUNK_SIZE - 1)).

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
	).

clear() ->
	gen_server:cast(?MODULE, reset).

assert_get(Expected, Offset) ->
	?assertEqual(Expected, ar_chunk_storage:get(Offset - 1)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - 2)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 1)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE + 2)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 + 1)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 2 - 1)),
	?assertEqual(Expected, ar_chunk_storage:get(Offset - ?DATA_CHUNK_SIZE div 3)).
