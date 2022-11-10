-module(ar_kv).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, open_without_column_families/2, close/1, put/3, get/2,
		get_next_by_prefix/4, get_next/2, cyclic_iterator_move/2, get_prev/2, get_range/2,
		get_range/3, delete/2, delete_range/3, destroy/1, count/1]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

open(DataDirRelativePath) ->
	open_without_column_families(DataDirRelativePath, []).

open(DataDirRelativePath, CFDescriptors) ->
	Filepath = filename:join(get_data_dir(), DataDirRelativePath),
	LogDir = filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, filename:basename(Filepath)]),
	ok = filelib:ensure_dir(Filepath ++ "/"),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	Opts = [
		{create_if_missing, true},
		{create_missing_column_families, true},
		{db_log_dir, LogDir}
	],
	may_be_repair(Filepath),
	case rocksdb:open(Filepath, Opts, CFDescriptors) of
		{ok, DB, CFs} ->
			{ok, DB, CFs};
		Error ->
			Error
	end.

open_without_column_families(DataDirRelativePath, Opts) ->
	Filepath = filename:join(get_data_dir(), DataDirRelativePath),
	ok = filelib:ensure_dir(Filepath ++ "/"),
	LogDir = filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, filename:basename(Filepath)]),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	may_be_repair(Filepath),
	rocksdb:open(Filepath, [{create_if_missing, true}, {db_log_dir, LogDir}] ++ Opts).

may_be_repair(Filepath) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(repair_rocksdb, Config#config.enable) of
		true ->
			ar:console("Repairing ~s.~n", [Filepath]),
			Reply = rocksdb:repair(Filepath, []),
			ar:console("Result: ~p.~n", [Reply]);
		_ ->
			ok
	end.

close(DB) ->
	rocksdb:close(DB).

put({DB, CF}, Key, Value) ->
	rocksdb:put(DB, CF, Key, Value, []);
put(DB, Key, Value) ->
	rocksdb:put(DB, Key, Value, []).

get({DB, CF}, Key) ->
	rocksdb:get(DB, CF, Key, []);
get(DB, Key) ->
	rocksdb:get(DB, Key, []).

%% @doc Return the key ({ok, Key, Value}) equal to or bigger than OffsetBinary with
%% either the matching PrefixBitSize first bits or PrefixBitSize first bits bigger by one.
get_next_by_prefix({DB, CF}, PrefixBitSize, KeyBitSize, OffsetBinary) ->
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
					NextPrefixSmallestBytes = << (Prefix + 1):PrefixBitSize, 0:SuffixBitSize >>,
					rocksdb:iterator_move(Iterator, {seek, NextPrefixSmallestBytes});
				Reply ->
					Reply
			end;
		Error ->
			Error
	end.

get_next({DB, CF}, Cursor) ->
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
get_next(DB, Cursor) ->
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

cyclic_iterator_move({DB, CF}, Cursor) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, Cursor) of
				{error, invalid_iterator} ->
					case Cursor of
						first ->
							none;
						_ ->
							cyclic_iterator_move({DB, CF}, first)
					end;
				{ok, Key, Value} ->
					case rocksdb:iterator_move(Iterator, next) of
						{ok, NextKey, _Value} ->
							{ok, Key, Value, {seek, NextKey}};
						{error, invalid_iterator} ->
							{ok, Key, Value, first};
						Error ->
							Error
					end;
				Error ->
					Error
			end;
		Error ->
			Error
	end.

get_prev({DB, CF}, OffsetBinary) ->
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
get_prev(DB, OffsetBinary) ->
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

get_range({DB, CF}, StartOffsetBinary) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, Value} ->
					get_range2(Iterator, #{ Key => Value });
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end.

get_range({DB, CF}, StartOffsetBinary, EndOffsetBinary) ->
	case rocksdb:iterator(DB, CF, [{total_order_seek, true}]) of
		{ok, Iterator} ->
			case rocksdb:iterator_move(Iterator, {seek, StartOffsetBinary}) of
				{ok, Key, _Value} when Key > EndOffsetBinary ->
					{ok, #{}};
				{ok, Key, Value} ->
					get_range2(Iterator, #{ Key => Value }, EndOffsetBinary);
				{error, invalid_iterator} ->
					{ok, #{}};
				{error, Reason} ->
					{error, Reason}
			end;
		Error ->
			Error
	end.

delete({DB, CF}, Key) ->
	rocksdb:delete(DB, CF, Key, []);
delete(DB, Key) ->
	rocksdb:delete(DB, Key, []).

delete_range({DB, CF}, StartKey, EndKey) ->
	rocksdb:delete_range(DB, CF, StartKey, EndKey, []);
delete_range(DB, StartKey, EndKey) ->
	rocksdb:delete_range(DB, StartKey, EndKey, []).

destroy(Name) ->
	RocksDBDir = filename:join(get_data_dir(), ?ROCKS_DB_DIR),
	Filename = filename:join(RocksDBDir, Name),
	case filelib:is_dir(Filename) of
		true ->
			rocksdb:destroy(Filename, []);
		false ->
			ok
	end.

count(DB) ->
	rocksdb:count(DB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_data_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.data_dir.

get_base_log_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.log_dir.

get_range2(Iterator, Map) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, Value} ->
			get_range2(Iterator, Map#{ Key => Value });
		{error, invalid_iterator} ->
			{ok, Map};
		{error, Reason} ->
			{error, Reason}
	end.

get_range2(Iterator, Map, EndOffsetBinary) ->
	case rocksdb:iterator_move(Iterator, next) of
		{ok, Key, _Value} when Key > EndOffsetBinary ->
			{ok, Map};
		{ok, Key, Value} ->
			get_range2(Iterator, Map#{ Key => Value }, EndOffsetBinary);
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
	{ok, DB0, [_DefaultCF0, CF0]} = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts}, {"test", Opts}]),
	SmallerPrefix = crypto:strong_rand_bytes(29),
	<< O1:232 >> = SmallerPrefix,
	BiggerPrefix = << (O1 + 1):232 >>,
	Suffixes =
		sets:to_list(sets:from_list([crypto:strong_rand_bytes(3) || _ <- lists:seq(1, 20)])),
	{Suffixes1, Suffixes2} = lists:split(10, Suffixes),
	lists:foreach(
		fun(Suffix) ->
			ok = ar_kv:put(
				{DB0, CF0},
				<< SmallerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(40 * 1024 * 1024)
			),
			ok = ar_kv:put(
				{DB0, CF0},
				<< BiggerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(40 * 1024 * 1024)
			)
		end,
		Suffixes1
	),
	ar_kv:close(DB0),
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
	{ok, DB, [_DefaultCF, CF]} = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts2}, {"test", Opts2}]),
	%% Store new data enough for new SST files to be created.
	lists:foreach(
		fun(Suffix) ->
			ok = ar_kv:put(
				{DB, CF},
				<< SmallerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(40 * 1024 * 1024)
			),
			ok = ar_kv:put(
				{DB, CF},
				<< BiggerPrefix/binary, Suffix/binary >>,
				crypto:strong_rand_bytes(50 * 1024 * 1024)
			)
		end,
		Suffixes2
	),
	assert_iteration(DB, CF, SmallerPrefix, BiggerPrefix, Suffixes),
	%% Close the database to make sure the new data is flushed.
	ar_kv:close(DB),
	{ok, DB1, [_DefaultCF1, CF1]} = ar_kv:open(filename:join(?ROCKS_DB_DIR, "test_db"),
			[{"default", Opts2}, {"test", Opts2}]),
	assert_iteration(DB1, CF1, SmallerPrefix, BiggerPrefix, Suffixes),
	destroy("test_db").

assert_iteration(DB, CF, SmallerPrefix, BiggerPrefix, Suffixes) ->
	SortedSuffixes = lists:sort(Suffixes),
	SmallestKey = << SmallerPrefix/binary, (lists:nth(1, SortedSuffixes))/binary >>,
	NextSmallestKey = << SmallerPrefix/binary, (lists:nth(2, SortedSuffixes))/binary >>,
	%% Make sure we can iterate from the beginning.
	{ok, K1, _, Cursor} = ar_kv:cyclic_iterator_move({DB, CF}, first),
	?assertEqual(SmallestKey, K1),
	?assertEqual({seek, NextSmallestKey}, Cursor),
	<< SmallestOffset:256 >> = SmallestKey,
	%% Assert forwards and backwards iteration within the same prefix works.
	?assertMatch({ok, SmallestKey, _}, ar_kv:get_next_by_prefix({DB, CF}, 232, 256, SmallestKey)),
	?assertMatch({ok, SmallestKey, _}, ar_kv:get_prev({DB, CF}, SmallestKey)),
	?assertMatch(
		{ok, NextSmallestKey, _},
		ar_kv:get_next_by_prefix({DB, CF}, 232, 256, << (SmallestOffset + 1):256 >>)
	),
	<< NextSmallestOffset:256 >> = NextSmallestKey,
	?assertMatch(
		{ok, SmallestKey, _},
		ar_kv:get_prev({DB, CF}, << (NextSmallestOffset - 1):256 >>)
	),
	%% Assert forwards and backwards iteration across different prefixes works.
	SmallerPrefixBiggestKey = << SmallerPrefix/binary, (lists:last(SortedSuffixes))/binary >>,
	BiggerPrefixSmallestKey = << BiggerPrefix/binary, (lists:nth(1, SortedSuffixes))/binary >>,
	<< SmallerPrefixBiggestOffset:256 >> = SmallerPrefixBiggestKey,
	?assertMatch(
		{ok, BiggerPrefixSmallestKey, _},
		ar_kv:get_next_by_prefix({DB, CF}, 232, 256, << (SmallerPrefixBiggestOffset + 1):256 >>)
	),
	<< BiggerPrefixSmallestOffset:256 >> = BiggerPrefixSmallestKey,
	?assertMatch(
		{ok, SmallerPrefixBiggestKey, _},
		ar_kv:get_prev({DB, CF}, << (BiggerPrefixSmallestOffset - 1):256 >>)
	),
	BiggerPrefixNextSmallestKey =
		<< BiggerPrefix/binary, (lists:nth(2, SortedSuffixes))/binary >>,
	{ok, Map} =
		ar_kv:get_range({DB, CF}, SmallerPrefixBiggestKey, BiggerPrefixNextSmallestKey),
	?assertEqual(3, map_size(Map)),
	?assert(maps:is_key(SmallerPrefixBiggestKey, Map)),
	?assert(maps:is_key(BiggerPrefixNextSmallestKey, Map)),
	?assert(maps:is_key(BiggerPrefixSmallestKey, Map)),
	ar_kv:delete_range({DB, CF}, SmallerPrefixBiggestKey, BiggerPrefixNextSmallestKey),
	?assertEqual(not_found, ar_kv:get({DB, CF}, SmallerPrefixBiggestKey)),
	?assertEqual(not_found, ar_kv:get({DB, CF}, BiggerPrefixSmallestKey)),
	lists:foreach(
		fun(Suffix) ->
			?assertMatch({ok, _}, ar_kv:get({DB, CF}, << BiggerPrefix/binary, Suffix/binary >>))
		end,
		lists:sublist(lists:reverse(SortedSuffixes), length(SortedSuffixes) - 1)
	),
	lists:foreach(
		fun(Suffix) ->
			?assertMatch({ok, _}, ar_kv:get({DB, CF}, << SmallerPrefix/binary, Suffix/binary >>))
		end,
		lists:sublist(SortedSuffixes, length(SortedSuffixes) - 1)
	),
	ar_kv:put({DB, CF}, SmallerPrefixBiggestKey, crypto:strong_rand_bytes(50 * 1024)),
	ar_kv:put({DB, CF}, BiggerPrefixNextSmallestKey, crypto:strong_rand_bytes(50 * 1024)),
	ar_kv:put({DB, CF}, BiggerPrefixSmallestKey, crypto:strong_rand_bytes(50 * 1024)).
