-module(ar_kv).

-behaviour(gen_server).

-export([start_link/0, open/2, open/3, open/4, put/3, get/2, get_next_by_prefix/4, get_next/2,
		get_prev/2, get_range/2, get_range/3, delete/2, delete_range/3, count/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WITH_DB(Name, Callback), with_db(Name, ?FUNCTION_NAME, Callback)).
-define(WITH_ITERATOR(Name, IteratorOptions, Callback), with_iterator(Name, ?FUNCTION_NAME, IteratorOptions, Callback)).

-define(DEFAULT_ROCKSDB_DATABASE_OPTIONS, #{
	create_if_missing => true,
	create_missing_column_families => true,

	%% enable atomic_flush for dbs that utilize column families
	atomic_flush => true,

	%% these are default values, but they must not be overriden;
	%% otherwise the syncWAL will not work.
	allow_mmap_reads => false,
	allow_mmap_writes => false
}).

-record(db, {
	%% name may be undefined in short intervals before opening the database,
	%% or reopening the database (which implies close and open operations).
	%% It may happen in case of opening the database with column families.
	%% NB: records with undefined db_handle must not be stored in the ETS table.
	name :: term() | undefined,
	filepath :: file:filename_all(),
	db_options :: rocksdb:db_options(),
	%% db_handle may be undefined in short intervals before opening the database,
	%% or reopening the database (which implies close and open operations).
	%% NB: records with undefined db_handle must not be stored in the ETS table.
	db_handle :: rocksdb:db_handle() | undefined,

	%% column families only fields, must be set to undefined for plain databases.
	cf_names = undefined :: [term()],
	cf_descriptors = undefined :: [rocksdb:cf_descriptor()],
	cf_handle = undefined :: rocksdb:cf_handle()
}).

-record(state, {}).



%%%===================================================================
%%% Public interface.
%%%===================================================================



start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).



%% @doc Open a key-value store located at the given filesystem path relative to
%% the data directory and identified by the given Name.
open(DataDirRelativePath, Name) ->
	open(DataDirRelativePath, [], Name).



%% @doc Open a key-value store with the given options located at the given filesystem path
%% relative to the data directory and identified by the given Name.
open(DataDirRelativePath, UserOptions, Name) ->
	gen_server:call(?MODULE, {open, {DataDirRelativePath, UserOptions, Name}}, infinity).



%% @doc Open a key-value store with the column families located at the given filesystem path
%% relative to the data directory and identified by the given Name.
open(DataDirRelativePath, CfDescriptors, UserOptions, CfNames) ->
	gen_server:call(
		?MODULE, {open, {DataDirRelativePath, CfDescriptors, UserOptions, CfNames}}, infinity
	).



%% @doc Store the given value under the given key.
put(Name, Key, Value) ->
	?WITH_DB(Name, fun
		(#db{db_handle = Db, cf_handle = undefined}) ->
			rocksdb:put(Db, Key, Value, []);
		(#db{db_handle = Db, cf_handle = Cf}) ->
			rocksdb:put(Db, Cf, Key, Value, [])
	end).



%% @doc Return the value stored under the given key.
get(Name, Key) ->
	?WITH_DB(Name, fun
		(#db{db_handle = Db, cf_handle = undefined}) ->
			rocksdb:get(Db, Key, []);
		(#db{db_handle = Db, cf_handle = Cf}) ->
			rocksdb:get(Db, Cf, Key, [])
	end).



%% @doc Return the key ({ok, Key, Value}) equal to or bigger than OffsetBinary with
%% either the matching PrefixBitSize first bits or PrefixBitSize first bits bigger by one.
get_next_by_prefix(Name, PrefixBitSize, KeyBitSize, OffsetBinary) ->
	?WITH_ITERATOR(Name, [{prefix_same_as_start, true}], fun
		(Iterator) -> get_next_by_prefix2(Iterator, PrefixBitSize, KeyBitSize, OffsetBinary)
	end).



get_next_by_prefix2(Iterator, PrefixBitSize, KeyBitSize, OffsetBinary) ->
	case rocksdb:iterator_move(Iterator, {seek, OffsetBinary}) of
		{error, invalid_iterator} ->
			%% There is no bigger or equal key sharing the prefix.
			%% Query one more time with prefix + 1.
			SuffixBitSize = KeyBitSize - PrefixBitSize,
			<< Prefix:PrefixBitSize, _:SuffixBitSize >> = OffsetBinary,
			NextPrefixSmallestBytes = << (Prefix + 1):PrefixBitSize, 0:SuffixBitSize >>,
			rocksdb:iterator_move(Iterator, {seek, NextPrefixSmallestBytes});
		Reply ->
			Reply
	end.



%% @doc Return {ok, Key, Value} where Key is the smallest Key equal to or bigger than Cursor
%% or none.
get_next(Name, Cursor) ->
	?WITH_ITERATOR(Name, [{total_order_seek, true}], fun
		(Iterator) -> get_next2(Iterator, Cursor)
	end).



get_next2(Iterator, Cursor) ->
	case rocksdb:iterator_move(Iterator, Cursor) of
		{error, invalid_iterator} -> none;
		Reply -> Reply
	end.



%% @doc Return {ok, Key, Value} where Key is the largest Key equal to or smaller than Cursor
%% or none.
get_prev(Name, Cursor) ->
	?WITH_ITERATOR(Name, [{total_order_seek, true}], fun
		(Iterator) -> get_prev2(Iterator, Cursor)
	end).



get_prev2(Iterator, Cursor) ->
	case rocksdb:iterator_move(Iterator, {seek_for_prev, Cursor}) of
		{error, invalid_iterator} -> none;
		Reply -> Reply
	end.



%% @doc Return a Key => Value map with all keys equal to or larger than Start.
get_range(Name, Start) ->
	get_range2(Name, {Start, undefined}).



%% @doc Return a Key => Value map with all keys equal to or larger than Start and
%% equal to or smaller than End.
get_range(Name, Start, End) ->
	get_range2(Name, {Start, End}).



get_range2(Name, {StartOffsetBinary, MaybeEndOffsetBinary}) ->
	?WITH_ITERATOR(Name, [{total_order_seek, true}], fun
		(Iterator) -> get_range3(Iterator, {StartOffsetBinary, MaybeEndOffsetBinary})
	end).



get_range3(Iterator, {StartOffsetBinary, MaybeEndOffsetBinary}) ->
	case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
		{ok, Key, _Value} when is_binary(MaybeEndOffsetBinary), Key > MaybeEndOffsetBinary ->
			{ok, #{}};
		{ok, Key, Value} ->
			get_range4(Iterator, #{ Key => Value }, MaybeEndOffsetBinary);
		{error, invalid_iterator} ->
			{ok, #{}};
		{error, Reason} ->
			{error, Reason}
	end.



get_range4(Iterator, Map, MaybeEndOffsetBinary) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, _Value} when is_binary(MaybeEndOffsetBinary), Key > MaybeEndOffsetBinary ->
			{ok, Map};
		{ok, Key, Value} ->
			get_range4(Iterator, Map#{ Key => Value }, MaybeEndOffsetBinary);
		{error, invalid_iterator} ->
			{ok, Map};
		{error, Reason} ->
			{error, Reason}
	end.



%% @doc Remove the given key.
delete(Name, Key) ->
	?WITH_DB(Name, fun
		(#db{db_handle = Db, cf_handle = undefined}) -> rocksdb:delete(Db, Key, []);
		(#db{db_handle = Db, cf_handle = Cf}) -> rocksdb:delete(Db, Cf, Key, [])
	end).



%% @doc Remove the keys equal to or larger than Start and smaller than End.
delete_range(Name, StartOffsetBinary, EndOffsetBinary) ->
	?WITH_DB(Name, fun
		(#db{db_handle = Db, cf_handle = undefined}) -> rocksdb:delete_range(Db, StartOffsetBinary, EndOffsetBinary, []);
		(#db{db_handle = Db, cf_handle = Cf}) -> rocksdb:delete_range(Db, Cf, StartOffsetBinary, EndOffsetBinary, [])
	end).



%% @doc Return the number of keys in the table.
count(Name) ->
	?WITH_DB(Name, fun
		(#db{db_handle = Db, cf_handle = undefined}) -> rocksdb:count(Db);
		(#db{db_handle = Db, cf_handle = Cf}) -> rocksdb:count(Db, Cf)
	end).



%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================



init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.



handle_call({open, {DataDirRelativePath, UserOptions, Name}}, _From, State) ->
	DbRec0 = new_dbrec(Name, DataDirRelativePath, UserOptions),
	case ets:lookup(?MODULE, DbRec0#db.name) of
		[] ->
			case open(DbRec0) of
				ok -> {reply, ok, State};
				{error, Reason} -> {reply, {error, Reason}, State}
			end;
		[#db{filepath = Filepath, db_options = DbOptions}]
		when DbRec0#db.filepath == Filepath, DbRec0#db.db_options == DbOptions ->
			{reply, ok, State};
		[#db{filepath = Filepath, db_options = Options}] ->
			{reply, {error, {already_open, Filepath, Options}}, State}
	end;

handle_call({open, {DataDirRelativePath, CfDescriptors, UserOptions, CfNames}}, _From, State) ->
	DbRec0 = new_dbrec(CfNames, CfDescriptors, DataDirRelativePath, UserOptions),
	case ets:lookup(?MODULE, hd(CfNames)) of
		[] ->
			?LOG_INFO([{event, skipping_repair_for_cf_database}]),
			case open(DbRec0) of
				ok -> {reply, ok, State};
				{error, Reason} -> {reply, {error, Reason}, State}
			end;
		[#db{filepath = Filepath, db_options = DbOptions, cf_descriptors = CfDescriptors, cf_names = CfNames}]
		when
		DbRec0#db.filepath == Filepath, DbRec0#db.db_options == DbOptions,
		DbRec0#db.cf_descriptors == CfDescriptors, DbRec0#db.cf_names == CfNames ->
			{reply, ok, State};
		[#db{filepath = Filepath1, db_options = Options1}] ->
			{reply, {error, {already_open, Filepath1, Options1}}, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.



handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.



handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.



terminate(_Reason, _State) ->
	ets:foldl(
		fun(#db{db_handle = Db} = DbRec0, Closed) ->
				case sets:is_element(Db, Closed) of
					true ->
						Closed;
					false ->
						_ = flush(DbRec0),
						_ = sync_wal(DbRec0),
						_ = close(DbRec0),
						sets:add_element(Db, Closed)
				end
		end,
		sets:new(),
		?MODULE
	).



%%%===================================================================
%%% Private functions.
%%%===================================================================



%% @doc Create a new plain database record.
new_dbrec(Name, DataDirRelativePath, UserOptions) ->
	Filepath = filename:join(get_data_dir(), DataDirRelativePath),
	LogDir = filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, filename:basename(Filepath)]),
	ok = filelib:ensure_dir(Filepath ++ "/"),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	DefaultOptionsMap = (?DEFAULT_ROCKSDB_DATABASE_OPTIONS)#{db_log_dir => LogDir},
	DbOptions = maps:to_list(maps:merge(maps:from_list(UserOptions), DefaultOptionsMap)),
	#db{name = Name, filepath = Filepath, db_options = DbOptions}.



%% @doc  Create a new 'column-family' database record.
new_dbrec(CfNames, CfDescriptors, DataDirRelativePath, UserOptions) ->
	Filepath = filename:join(get_data_dir(), DataDirRelativePath),
	LogDir = filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, filename:basename(Filepath)]),
	ok = filelib:ensure_dir(Filepath ++ "/"),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	DefaultOptionsMap = (?DEFAULT_ROCKSDB_DATABASE_OPTIONS)#{db_log_dir => LogDir},
	DbOptions = maps:to_list(maps:merge(maps:from_list(UserOptions), DefaultOptionsMap)),
	#db{
		name = hd(CfNames), filepath = Filepath,
		db_options = DbOptions,
		cf_descriptors = CfDescriptors, cf_names = CfNames
	}.



%% @doc Attempt to open the database.
%% Both plain and 'column-family' databases are attempted.
%% When opening the plain database, the record will have `name` set to the given
%% name parameter.
%% When opening 'column-family' database, the record will have a column name; several
%% database records will be inserted during the process.
open(#db{db_handle = undefined, cf_descriptors = undefined, filepath = Filepath, db_options = DbOptions} = DbRec0) ->
	case rocksdb:open(Filepath, DbOptions) of
		{ok, Db} ->
			?LOG_INFO([{event, db_operation}, {op, open}, {name, io_lib:format("~p", [DbRec0#db.name])}]),
			DbRec1 = DbRec0#db{db_handle = Db},
			true = ets:insert(?MODULE, DbRec1),
			ok;
		{error, OpenError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, open},
				{name, io_lib:format("~p", [DbRec0#db.name])},
				{reason, io_lib:format("~p", [OpenError])}]),
			{error, failed}
	end;

open(#db{
	db_handle = undefined, cf_descriptors = CfDescriptors, cf_names = CfNames,
	filepath = Filepath, db_options = DbOptions
} = DbRec0) ->
	case rocksdb:open(Filepath, DbOptions, CfDescriptors) of
		{ok, Db, Cfs} ->
			lists:foreach(
				fun({Cf, CfName}) ->
					?LOG_INFO([{event, db_operation}, {op, open}, {name, io_lib:format("~p", [CfName])}]),
					DbRec1 = DbRec0#db{name = CfName, db_handle = Db, cf_handle = Cf},
					true = ets:insert(?MODULE, DbRec1)
				end,
				lists:zip(Cfs, CfNames)
			),
			ok;
		{error, OpenError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, open},
				{name, io_lib:format("~p", [DbRec0#db.name])},
				{reason, io_lib:format("~p", [OpenError])}]),
			{error, failed}
	end;

open(#db{} = DbRec0) ->
	?LOG_ERROR([
		{event, db_operation_failed}, {op, open}, {error, already_open},
		{name, io_lib:format("~p", [DbRec0#db.name])}
	]).



%% Attempt to close the database.
%% This function WILL NOT perform any actions regarding persistence: it is up to
%% the user to ensure that both flush/1 and sync_wal/1 functions are called prior
%% calling this function.
%% Database must be open at the moment of calling the function.
close(#db{db_handle = undefined}) -> {error, closed};

close(#db{db_handle = Db, name = Name}) ->
	try
		case rocksdb:close(Db) of
			ok ->
				?LOG_INFO([{event, db_operation}, {op, close}, {name, io_lib:format("~p", [Name])}]);
			{error, CloseError} ->
				?LOG_ERROR([
					{event, db_operation_failed}, {op, close}, {name, io_lib:format("~p", [Name])},
					{error, io_lib:format("~p", [CloseError])}
				])
		end
	catch
		Exc ->
			?LOG_ERROR([
				{event, ar_kv_failed}, {op, close}, {name, io_lib:format("~p", [Name])},
				{reason, io_lib:format("~p", [Exc])}
			])
	end.



%% @doc Attempt to flush the database: persist the memtables contents on disk.
%% Database must be open at the moment of calling the function.
flush(#db{name = Name, db_handle = undefined}) ->
	?LOG_ERROR([{event, db_operation_failed}, {op, flush}, {error, closed}, {name, io_lib:format("~p", [Name])}]),
	{error, closed};

flush(#db{name = Name, db_handle = Db}) ->
	case rocksdb:flush(Db, [{wait, true}, {allow_write_stall, false}]) of
		{error, FlushError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, flush},
				{name, io_lib:format("~p", [Name])},
				{reason, io_lib:format("~p", [FlushError])}]),
			{error, failed};
		_ ->
			?LOG_INFO([{event, db_operation}, {op, flush}, {name, io_lib:format("~p", [Name])}]),
			ok
	end.



%% @doc Attempt to sync Write Ahead Log (WAL): persist WAL contents on disk.
%% Database must be open at the moment of calling the function.
sync_wal(#db{name = Name, db_handle = undefined}) ->
	?LOG_ERROR([{event, db_operation_failed}, {op, sync_wal}, {error, closed}, {name, io_lib:format("~p", [Name])}]),
	{error, closed};

sync_wal(#db{name = Name, db_handle = Db}) ->
	case rocksdb:sync_wal(Db) of
		{error, SyncError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, sync_wal},
				{name, io_lib:format("~p", [Name])},
				{reason, io_lib:format("~p", [SyncError])}]),
			{error, failed};
		_ ->
			?LOG_INFO([{event, db_operation}, {op, sync_wal}, {name, io_lib:format("~p", [Name])}]),
			ok
	end.



%% @doc Apply callback if it is possible to obtain the iterator for the database.
%% The callback will get an iterator as an argument.
with_iterator(Name, Op, IteratorOptions, Callback) ->
	with_db(Name, Op, fun
		(#db{db_handle = Db, cf_handle = undefined}) ->
			case rocksdb:iterator(Db, IteratorOptions) of
				{ok, Iterator} -> apply(Callback, [Iterator]);
				{error, IteratorError} -> {error, IteratorError}
			end;
		(#db{db_handle = Db, cf_handle = Cf}) ->
			case rocksdb:iterator(Db, Cf, IteratorOptions) of
				{ok, Iterator} -> apply(Callback, [Iterator]);
				{error, IteratorError} -> {error, IteratorError}
			end
	end).



%% @doc Apply callback if the database is available.
%% The callback will get the database record (#db{}) as an argument.
with_db(Name, Op, Callback) ->
	try
		case ets:lookup(?MODULE, Name) of
			[] ->
				{error, db_not_found};
			[DbRec0] ->
				apply(Callback, [DbRec0])
		end
	catch
		Exc ->
			?LOG_ERROR([{event, db_operation_failed}, {op, Op},
				{name, io_lib:format("~p", [Name])},
				{reason, io_lib:format("~p", [Exc])}]),
			{error, failed}
	end.



get_data_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.data_dir.



get_base_log_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.log_dir.



%%%===================================================================
%%% Tests.
%%%===================================================================



rocksdb_iterator_test_() ->
	{timeout, 300, fun test_rocksdb_iterator/0}.



test_rocksdb_iterator() ->
	destroy("test_db"),
	%% Configure the DB similarly to how it used to be configured before the tested change.
	Opts = [
		{prefix_extractor, {capped_prefix_transform, 28}},
		{optimize_filters_for_hits, true},
		{max_open_files, 1000000}
	],
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts}, {"test", Opts}], [], [default, test]),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts}, {"test", Opts}], [], [default, test]),
	SmallerPrefix = crypto:strong_rand_bytes(29),
	<< O1:232 >> = SmallerPrefix,
	BiggerPrefix = << (O1 + 1):232 >>,
	Suffixes =
		sets:to_list(sets:from_list([crypto:strong_rand_bytes(3) || _ <- lists:seq(1, 20)])),
	{Suffixes1, Suffixes2} = lists:split(10, Suffixes),
	lists:foreach(
		fun(Suffix) ->
			ok = ar_kv:put(
				test,
				<< SmallerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(40 * 1024 * 1024)
			),
			ok = ar_kv:put(
				test,
				<< BiggerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(40 * 1024 * 1024)
			)
		end,
		Suffixes1
	),
	test_close(test),
	%% Reopen with the new configuration.
	Opts2 = [
		{block_based_table_options, [
			{cache_index_and_filter_blocks, true},
			{bloom_filter_policy, 10}
		]},
		{prefix_extractor, {capped_prefix_transform, 29}},
		{optimize_filters_for_hits, true},
		{max_open_files, 1000000},
		{write_buffer_size, 256 * 1024 * 1024},
		{target_file_size_base, 256 * 1024 * 1024},
		{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
	],
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts2}, {"test", Opts2}], [], [default, test]),
	%% Store new data enough for new SST files to be created.
	lists:foreach(
		fun(Suffix) ->
			ok = ar_kv:put(
				test,
				<< SmallerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(40 * 1024 * 1024)
			),
			ok = ar_kv:put(
				test,
				<< BiggerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(50 * 1024 * 1024)
			)
		end,
		Suffixes2
	),
	assert_iteration(test, SmallerPrefix, BiggerPrefix, Suffixes),
	%% Close the database to make sure the new data is flushed.
	test_close(test),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts2}, {"test", Opts2}], [], [default1, test1]),
	assert_iteration(test1, SmallerPrefix, BiggerPrefix, Suffixes),
	test_close(test1),
	destroy("test_db").



delete_range_test_() ->
	{timeout, 300, fun test_delete_range/0}.



test_delete_range() ->
	destroy("test_db"),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"), test_db),
	ok = ar_kv:put(test_db, << 0:256 >>, << 0:256 >>),
	ok = ar_kv:put(test_db, << 1:256 >>, << 1:256 >>),
	ok = ar_kv:put(test_db, << 2:256 >>, << 2:256 >>),
	ok = ar_kv:put(test_db, << 3:256 >>, << 3:256 >>),
	ok = ar_kv:put(test_db, << 4:256 >>, << 4:256 >>),
	?assertEqual({ok, << 1:256 >>}, ar_kv:get(test_db, << 1:256 >>)),

	%% Base case
	?assertEqual(ok, ar_kv:delete_range(test_db, << 1:256 >>, << 2:256 >>)),
	?assertEqual({ok, << 0:256 >>}, ar_kv:get(test_db, << 0:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 1:256 >>)),
	?assertEqual({ok, << 2:256 >>}, ar_kv:get(test_db, << 2:256 >>)),

	%% Missing start and missing end
	?assertEqual(ok, ar_kv:delete_range(test_db, << 1:256 >>, << 5:256 >>)),
	?assertEqual({ok, << 0:256 >>}, ar_kv:get(test_db, << 0:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 1:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 2:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 3:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 4:256 >>)),

	%% Empty range
	?assertEqual(ok, ar_kv:delete_range(test_db, << 1:256 >>, << 1:256 >>)),
	?assertEqual({ok, << 0:256 >>}, ar_kv:get(test_db, << 0:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 1:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 2:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 3:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 4:256 >>)),

	%% Reversed range
	?assertMatch({error, _}, ar_kv:delete_range(test_db, << 1:256 >>, << 0:256 >>)),
	?assertEqual({ok, << 0:256 >>}, ar_kv:get(test_db, << 0:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 1:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 2:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 3:256 >>)),
	?assertEqual(not_found, ar_kv:get(test_db, << 4:256 >>)),

	destroy("test_db").



assert_iteration(Name, SmallerPrefix, BiggerPrefix, Suffixes) ->
	SortedSuffixes = lists:sort(Suffixes),
	SmallestKey = << SmallerPrefix/binary, (lists:nth(1, SortedSuffixes))/binary >>,
	NextSmallestKey = << SmallerPrefix/binary, (lists:nth(2, SortedSuffixes))/binary >>,
	<< SmallestOffset:256 >> = SmallestKey,
	%% Assert forwards and backwards iteration within the same prefix works.
	?assertMatch({ok, SmallestKey, _}, ar_kv:get_next_by_prefix(Name, 232, 256, SmallestKey)),
	?assertMatch({ok, SmallestKey, _}, ar_kv:get_prev(Name, SmallestKey)),
	?assertMatch({ok, NextSmallestKey, _},
			ar_kv:get_next_by_prefix(Name, 232, 256, << (SmallestOffset + 1):256 >>)),
	<< NextSmallestOffset:256 >> = NextSmallestKey,
	?assertMatch({ok, SmallestKey, _},
			ar_kv:get_prev(Name, << (NextSmallestOffset - 1):256 >>)),
	%% Assert forwards and backwards iteration across different prefixes works.
	SmallerPrefixBiggestKey = << SmallerPrefix/binary, (lists:last(SortedSuffixes))/binary >>,
	BiggerPrefixSmallestKey = << BiggerPrefix/binary, (lists:nth(1, SortedSuffixes))/binary >>,
	<< SmallerPrefixBiggestOffset:256 >> = SmallerPrefixBiggestKey,
	?assertMatch({ok, BiggerPrefixSmallestKey, _},
			ar_kv:get_next_by_prefix(Name, 232, 256,
			<< (SmallerPrefixBiggestOffset + 1):256 >>)),
	<< BiggerPrefixSmallestOffset:256 >> = BiggerPrefixSmallestKey,
	?assertMatch({ok, SmallerPrefixBiggestKey, _},
			ar_kv:get_prev(Name, << (BiggerPrefixSmallestOffset - 1):256 >>)),
	BiggerPrefixNextSmallestKey =
		<< BiggerPrefix/binary, (lists:nth(2, SortedSuffixes))/binary >>,
	{ok, Map} = ar_kv:get_range(Name, SmallerPrefixBiggestKey, BiggerPrefixNextSmallestKey),
	?assertEqual(3, map_size(Map)),
	?assert(maps:is_key(SmallerPrefixBiggestKey, Map)),
	?assert(maps:is_key(BiggerPrefixNextSmallestKey, Map)),
	?assert(maps:is_key(BiggerPrefixSmallestKey, Map)),
	ar_kv:delete_range(Name, SmallerPrefixBiggestKey, BiggerPrefixNextSmallestKey),
	?assertEqual(not_found, ar_kv:get(Name, SmallerPrefixBiggestKey)),
	?assertEqual(not_found, ar_kv:get(Name, BiggerPrefixSmallestKey)),
	lists:foreach(
		fun(Suffix) ->
			?assertMatch({ok, _}, ar_kv:get(Name, << BiggerPrefix/binary, Suffix/binary >>))
		end,
		lists:sublist(lists:reverse(SortedSuffixes), length(SortedSuffixes) - 1)
	),
	lists:foreach(
		fun(Suffix) ->
			?assertMatch({ok, _},
					ar_kv:get(Name, << SmallerPrefix/binary, Suffix/binary >>))
		end,
		lists:sublist(SortedSuffixes, length(SortedSuffixes) - 1)
	),
	ar_kv:put(Name, SmallerPrefixBiggestKey, crypto:strong_rand_bytes(50 * 1024)),
	ar_kv:put(Name, BiggerPrefixNextSmallestKey, crypto:strong_rand_bytes(50 * 1024)),
	ar_kv:put(Name, BiggerPrefixSmallestKey, crypto:strong_rand_bytes(50 * 1024)).



destroy(Name) ->
	RocksDBDir = filename:join(get_data_dir(), ?ROCKS_DB_DIR),
	Filename = filename:join(RocksDBDir, Name),
	case filelib:is_dir(Filename) of
		true ->
			rocksdb:destroy(Filename, []);
		false ->
			ok
	end.



test_close(Name) ->
	[{_, {DB, _}}] = ets:lookup(?MODULE, Name),
	ok = rocksdb:close(DB),
	case ets:lookup(?MODULE, {config, Name}) of
		[{_, {_, _, _, CfNames}}] ->
			[ets:delete(?MODULE, {config, Name2}) || Name2 <- CfNames],
			[ets:delete(?MODULE, Name2) || Name2 <- CfNames];
		_ ->
			ets:delete(?MODULE, {config, Name}),
			ets:delete(?MODULE, Name)
	end.
