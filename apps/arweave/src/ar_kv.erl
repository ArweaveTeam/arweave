-module(ar_kv).

-behaviour(gen_server).

-export([start_link/0, open/2, open/3, open/4, put/3, get/2, get_next_by_prefix/4, get_next/2,
		get_prev/2, get_range/2, get_range/3, delete/2, delete_range/3, count/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

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
	gen_server:call(?MODULE, {open, DataDirRelativePath, UserOptions, Name}, infinity).

%% @doc Open a key-value store with the column families located at the given filesystem path
%% relative to the data directory and identified by the given Name.
open(DataDirRelativePath, CFDescriptors, UserOptions, Names) ->
	gen_server:call(?MODULE, {open, DataDirRelativePath, CFDescriptors, UserOptions, Names},
			infinity).

%% @doc Store the given value under the given key.
put(Name, Key, Value) ->
	put(Name, Key, Value, 1).

%% @doc Return the value stored under the given key.
get(Name, Key) ->
	get(Name, Key, 1).

%% @doc Return the key ({ok, Key, Value}) equal to or bigger than OffsetBinary with
%% either the matching PrefixBitSize first bits or PrefixBitSize first bits bigger by one.
get_next_by_prefix(Name, PrefixBitSize, KeyBitSize, OffsetBinary) ->
	get_next_by_prefix(Name, PrefixBitSize, KeyBitSize, OffsetBinary, 1).

%% @doc Return {ok, Key, Value} where Key is the smallest Key equal to or bigger than Cursor
%% or none.
get_next(Name, Cursor) ->
	get_next(Name, Cursor, 1).

%% @doc Return {ok, Key, Value} where Key is the largest Key equal to or smaller than Cursor
%% or none.
get_prev(Name, Cursor) ->
	get_prev(Name, Cursor, 1).

%% @doc Return a Key => Value map with all keys equal to or larger than Start.
get_range(Name, Start) ->
	get_range2(Name, Start, 1).

%% @doc Return a Key => Value map with all keys equal to or larger than Start and
%% equal to or smaller than End.
get_range(Name, Start, End) ->
	get_range2(Name, Start, End, 1).

%% @doc Remove the given key.
delete(Name, Key) ->
	delete(Name, Key, 1).

%% @doc Remove the keys equal to or larger than Start and smaller than End.
delete_range(Name, Start, End) ->
	delete_range(Name, Start, End, 1).

%% @doc Return the number of keys in the table.
count(Name) ->
	[{_, DB}] = ets:lookup(?MODULE, Name),
	case catch rocksdb:count(DB) of
		{'EXIT', Exc} ->
			{error, Exc};
		Reply ->
			Reply
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

handle_call({open, DataDirRelativePath, UserOptions, Name}, _From, State) ->
	Filepath = filename:join(get_data_dir(), DataDirRelativePath),
	ok = filelib:ensure_dir(Filepath ++ "/"),
	LogDir = filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, filename:basename(Filepath)]),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	DefaultOptions = [{create_if_missing, true}, {db_log_dir, LogDir}],
	Options = DefaultOptions ++ UserOptions,
	case ets:lookup(?MODULE, {config, Name}) of
		[] ->
			may_be_repair(Filepath),
			case rocksdb:open(Filepath, Options) of
				{ok, DB} ->
					ets:insert(?MODULE, {{config, Name}, {Filepath, Options}}),
					ets:insert(?MODULE, {Name, DB}),
					{reply, ok, State};
				Error ->
					{reply, Error, State}
			end;
		[{_, {Filepath, Options}}] ->
			{reply, ok, State}
	end;

handle_call({open, DataDirRelativePath, CFDescriptors, UserOptions, Names}, _From, State) ->
	Filepath = filename:join(get_data_dir(), DataDirRelativePath),
	LogDir = filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, filename:basename(Filepath)]),
	ok = filelib:ensure_dir(Filepath ++ "/"),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	DefaultOptions = [{create_if_missing, true}, {create_missing_column_families, true},
			{db_log_dir, LogDir}],
	Options = DefaultOptions ++ UserOptions,
	case ets:lookup(?MODULE, {config, hd(Names)}) of
		[] ->
			may_be_repair(Filepath),
			case rocksdb:open(Filepath, Options, CFDescriptors) of
				{ok, DB, CFs} ->
					lists:foreach(
						fun({CF, Name}) ->
							ets:insert(?MODULE, {{config, Name},
									{Filepath, Options, CFDescriptors, Names}}),
							ets:insert(?MODULE, {Name, {DB, CF}})
						end,
						lists:zip(CFs, Names)
					),
					{reply, ok, State};
				Error ->
					{reply, Error, State}
			end;
		[{_, {Filepath, Options, CFDescriptors, Names}}] ->
			{reply, ok, State}
	end;

handle_call({reconnect, Name, Ref}, _From, State) ->
	case ets:lookup(?MODULE, Name) of
		[{_, Ref}] ->
			case Ref of
				{DB, _CF} ->
					case catch rocksdb:close(DB) of
						ok ->
							reconnect2(Name, State);
						{'EXIT', Exc} ->
							{reply, {error, Exc}, State};
						Error ->
							{reply, Error, State}
					end;
				DB ->
					case catch rocksdb:close(DB) of
						ok ->
							reconnect3(Name, State);
						{'EXIT', Exc} ->
							{reply, {error, Exc}, State};
						Error ->
							{reply, Error, State}
					end
			end;
		[{_, Ref2}] ->
			{reply, {ok, Ref2}, State}
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
		fun	({{config, _}, _}, Closed) ->
				Closed;
			({Name, Ref}, Closed) ->
				DB = case Ref of {_, _} -> element(1, Ref); _ -> Ref end,
				case sets:is_element(DB, Closed) of
					true ->
						Closed;
					false ->
						case catch rocksdb:close(DB) of
							ok ->
								ok;
							{'EXIT', Exc} ->
								?LOG_ERROR([{event, ar_kv_failed}, {op, close},
										{name, io_lib:format("~p", [Name])},
										{exception, io_lib:format("~p", [Exc])}]);
							Error ->
								?LOG_ERROR([{event, ar_kv_failed}, {op, close},
										{error, io_lib:format("~p", [Error])}])
						end,
						sets:add_element(DB, Closed)
				end
		end,
		sets:new(),
		?MODULE
	).

%%%===================================================================
%%% Private functions.
%%%===================================================================

may_be_repair(Filepath) ->
	{ok, Config} = application:get_env(arweave, config),
	Enabled = lists:member(repair_rocksdb, Config#config.enable)
			orelse lists:member(filename:absname(Filepath), Config#config.repair_rocksdb),
	case Enabled of
		true ->
			ar:console("Repairing ~s.~n", [Filepath]),
			Reply = rocksdb:repair(Filepath, []),
			ar:console("Result: ~p.~n", [Reply]);
		_ ->
			ok
	end.

put(Name, Key, Value, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch put2(Ref, Key, Value) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, put}, {key, ar_util:encode(Key)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							put(Name, Key, Value, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

put2({DB, CF}, Key, Value) ->
	rocksdb:put(DB, CF, Key, Value, []);
put2(DB, Key, Value) ->
	rocksdb:put(DB, Key, Value, []).

reconnect(Name, Ref) ->
	gen_server:call(?MODULE, {reconnect, Name, Ref}, infinity).

reconnect2(Name, State) ->
	[{_, {Filepath, Options, CFDescriptors, Names}}] = ets:lookup(?MODULE, {config, Name}),
	case rocksdb:open(Filepath, Options, CFDescriptors) of
		{ok, DB2, CFs} ->
			lists:foreach(
				fun({CF2, Name2}) ->
					ets:insert(?MODULE, {Name2, {DB2, CF2}})
				end,
				lists:zip(CFs, Names)
			),
			[{_, Ref2}] = ets:lookup(?MODULE, Name),
			{reply, {ok, Ref2}, State};
		Error ->
			{reply, Error, State}
	end.

reconnect3(Name, State) ->
	[{_, {Filepath, Options}}] = ets:lookup(?MODULE, {config, Name}),
	case rocksdb:open(Filepath, Options) of
		{ok, DB2} ->
			ets:insert(?MODULE, {Name, DB2}),
			{reply, {ok, DB2}, State};
		Error ->
			{reply, Error, State}
	end.

get(Name, Key, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch get2(Ref, Key) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, get}, {key, ar_util:encode(Key)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							get(Name, Key, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

get2({DB, CF}, Key) ->
	rocksdb:get(DB, CF, Key, []);
get2(DB, Key) ->
	rocksdb:get(DB, Key, []).

get_next_by_prefix(Name, PrefixBitSize, KeyBitSize, OffsetBinary, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch get_next_by_prefix2(Ref, PrefixBitSize, KeyBitSize, OffsetBinary) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, get_next_by_prefix},
							{key, ar_util:encode(OffsetBinary)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							get_next_by_prefix(Name, PrefixBitSize, KeyBitSize, OffsetBinary,
									RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

get_next_by_prefix2({DB, CF}, PrefixBitSize, KeyBitSize, OffsetBinary) ->
	case catch rocksdb:iterator(DB, CF, [{prefix_same_as_start, true}]) of
		{'EXIT', _} ->
			<< Offset: 256 >> = OffsetBinary,
			?LOG_WARNING([{event, failed_to_instantiate_rocksdb_iterator},
					{offset, Offset}]),
			%% Presumably, happens when there is a lot of writing activity in the vicinity
			%% of the offset.
			{error, temporary_error};
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, OffsetBinary}) of
				{error, invalid_iterator} ->
					%% There is no bigger or equal key sharing the prefix.
					%% Query one more time with prefix + 1.
					SuffixBitSize = KeyBitSize - PrefixBitSize,
					<< Prefix:PrefixBitSize, _:SuffixBitSize >> = OffsetBinary,
					NextPrefixSmallestBytes = << (Prefix + 1):PrefixBitSize,
							0:SuffixBitSize >>,
					rocksdb:iterator_move(Iterator, {seek, NextPrefixSmallestBytes});
				Reply ->
					Reply
			end;
		Error ->
			Error
	end.

get_next(Name, Cursor, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch get_next2(Ref, Cursor) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, get_next},
							{key, ar_util:encode(Cursor)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							get_next(Name, Cursor, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

get_next2({DB, CF}, Cursor) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, Cursor) of
				{error, invalid_iterator} ->
					none;
				Reply ->
					Reply
			end;
		Error ->
			Error
	end;
get_next2(DB, Cursor) ->
	case rocksdb:iterator(DB, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, Cursor) of
				{error, invalid_iterator} ->
					none;
				Reply ->
					Reply
			end;
		Error ->
			Error
	end.

get_prev(Name, Cursor, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch get_prev2(Ref, Cursor) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, get_prev},
							{key, ar_util:encode(Cursor)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							get_prev(Name, Cursor, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

get_prev2({DB, CF}, OffsetBinary) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek_for_prev, OffsetBinary}) of
				{error, invalid_iterator} ->
					none;
				Reply ->
					Reply
			end;
		Error ->
			Error
	end;
get_prev2(DB, OffsetBinary) ->
	case rocksdb:iterator(DB, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek_for_prev, OffsetBinary}) of
				{error, invalid_iterator} ->
					none;
				Reply ->
					Reply
			end;
		Error ->
			Error
	end.

get_range2(Name, Start, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch get_range3(Ref, Start) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, get_range},
							{start_key, ar_util:encode(Start)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							get_range2(Name, Start, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

get_range3({DB, CF}, StartOffsetBinary) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, Value} ->
					get_range4(Iterator, #{ Key => Value });
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end;
get_range3(DB, StartOffsetBinary) ->
	case rocksdb:iterator(DB, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, Value} ->
					get_range4(Iterator, #{ Key => Value });
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end.

get_range2(Name, Start, End, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch get_range3(Ref, Start, End) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, get_range},
							{start_key, ar_util:encode(Start)},
							{end_key, ar_util:encode(End)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							get_range2(Name, Start, End, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

get_range3({DB, CF}, StartOffsetBinary, EndOffsetBinary) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, _Value} when Key > EndOffsetBinary ->
					{ok, #{}};
				{ok, Key, Value} ->
					get_range4(Iterator, #{ Key => Value }, EndOffsetBinary);
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end;
get_range3(DB, StartOffsetBinary, EndOffsetBinary) ->
	case rocksdb:iterator(DB, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, _Value} when Key > EndOffsetBinary ->
					{ok, #{}};
				{ok, Key, Value} ->
					get_range4(Iterator, #{ Key => Value }, EndOffsetBinary);
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end.

delete(Name, Key, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch delete2(Ref, Key) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, delete},
							{key, ar_util:encode(Key)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							delete(Name, Key, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

delete2({DB, CF}, Key) ->
	rocksdb:delete(DB, CF, Key, []);
delete2(DB, Key) ->
	rocksdb:delete(DB, Key, []).

delete_range(Name, Start, End, RetryCount) ->
	[{_, Ref}] = ets:lookup(?MODULE, Name),
	case catch delete_range2(Ref, Start, End) of
		{'EXIT', Exc} ->
			case RetryCount of
				0 ->
					?LOG_ERROR([{event, ar_kv_error}, {op, delete_range},
							{start_key, ar_util:encode(Start)},
							{end_key, ar_util:encode(End)},
							{exception, io_lib:format("~p", [Exc])}]),
					{error, Exc};
				_ ->
					case reconnect(Name, Ref) of
						{ok, _Ref2} ->
							delete_range(Name, Start, End, RetryCount - 1);
						Error ->
							Error
					end
			end;
		Reply ->
			Reply
	end.

delete_range2({DB, CF}, StartKey, EndKey) ->
	rocksdb:delete_range(DB, CF, StartKey, EndKey, []);
delete_range2(DB, StartKey, EndKey) ->
	rocksdb:delete_range(DB, StartKey, EndKey, []).

get_data_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.data_dir.

get_base_log_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.log_dir.

get_range4(Iterator, Map) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, Value} ->
			get_range4(Iterator, Map#{ Key => Value });
		{error, invalid_iterator} ->
			{ok, Map};
		{error, Reason} ->
			{error, Reason}
	end.

get_range4(Iterator, Map, EndOffsetBinary) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, _Value} when Key > EndOffsetBinary ->
			{ok, Map};
		{ok, Key, Value} ->
			get_range4(Iterator, Map#{ Key => Value }, EndOffsetBinary);
		{error, invalid_iterator} ->
			{ok, Map};
		{error, Reason} ->
			{error, Reason}
	end.

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
	close(test),
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
	close(test),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts2}, {"test", Opts2}], [], [default1, test1]),
	assert_iteration(test1, SmallerPrefix, BiggerPrefix, Suffixes),
	close(test1),
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

close(Name) ->
	[{_, {DB, _}}] = ets:lookup(?MODULE, Name),
	ok = rocksdb:close(DB),
	case ets:lookup(?MODULE, {config, Name}) of
		[{_, {_, _, _, Names}}] ->
			[ets:delete(?MODULE, {config, Name2}) || Name2 <- Names],
			[ets:delete(?MODULE, Name2) || Name2 <- Names];
		_ ->
			ets:delete(?MODULE, {config, Name}),
			ets:delete(?MODULE, Name)
	end.
