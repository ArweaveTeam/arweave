-module(ar_data_roots).

-export([
	open_index_db/3,
	open_index_db/4,
	column_family/1,
	keys_column_family/1,
	legacy_db/1,
	keys_db/1,
	add_block_data_roots/3,
	store_block/5,
	get_entry/2,
	remove_range/3,
	iterator/3,
	next/1,
	reset/1,
	id/1,
	id/2,
	index_key/3,
	get_block/1,
	validate_data_roots/4,
	are_synced/2,
	are_synced/4,
	is_synced/2,
	repair/3
]).
-export_type([data_root_entry/0, data_root_entries/0]).

-include("ar.hrl").
-include("ar_data_sync.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-type data_root_entry() :: {DataRoot :: binary(), TXSize :: non_neg_integer(),
		TXStartOffset :: non_neg_integer(), TXPath :: binary()}.
-type data_root_entries() :: [data_root_entry()].
%%%===================================================================
%%% Public: DB configuration
%%%===================================================================
open_index_db(Dir, StoreID, BloomFilterOpts) ->
	open_index_db(Dir, "ar_data_sync_data_root_index_db", StoreID, BloomFilterOpts).

open_index_db(Dir, DBName, StoreID, BloomFilterOpts) ->
	ar_kv:open(#{
		path => filename:join(Dir, DBName),
		name => index_db(StoreID),
		options => [
			{max_open_files, 100}, {max_background_compactions, 8},
			{write_buffer_size, 256 * ?MiB}, % 256 MiB per memtable.
			{target_file_size_base, 256 * ?MiB}, % 256 MiB per SST file.
			%% 10 files in L1 to make L1 == L0 as recommended by the
			%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
			{max_bytes_for_level_base, 10 * 256 * ?MiB}
		] ++ BloomFilterOpts
	}).

column_family(Opts) ->
	{"data_root_index", Opts}.

keys_column_family(Opts) ->
	{"data_root_offset_index", Opts}.

legacy_db(StoreID) ->
	{data_root_index_old, StoreID}.

%% A reference to the on-disk key-value storage mapping
%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootIDs}.
%% Each key in DataRootIDs is a << DataRoot/binary, TXSize:256 >> binary.
%% Used to remove orphaned entries from DataRootIndex.
keys_db(StoreID) ->
	{data_root_offset_index, StoreID}.

%%%===================================================================
%%% Public: Block data root entries
%%%===================================================================
add_block_data_roots([], _BlockStart, _StoreID) ->
	{ok, sets:new()};
add_block_data_roots(SizeTaggedTXs, BlockStart, StoreID) ->
	{TXRoot, BlockSize, DataRootEntries} =
		build_block_data_root_entries(BlockStart, SizeTaggedTXs),
	case BlockSize > 0 of
		true ->
			store_block(BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID);
		false ->
			{ok, sets:new()}
	end.

%% @doc Build `{TXRoot, BlockSize, DataRootEntries}` for the given size-tagged txs.
build_block_data_root_entries(BlockStart, SizeTaggedTXs) ->
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, DataRootEntries} = lists:foldl(
		fun ({_, Offset}, {Offset, _} = Acc) ->
				Acc;
			({{padding, _}, Offset}, {_, DataRootEntriesAcc}) ->
				{Offset, DataRootEntriesAcc};
			({{_, DataRoot}, Offset}, {_, DataRootEntriesAcc}) when byte_size(DataRoot) < 32 ->
				{Offset, DataRootEntriesAcc};
			({{_, DataRoot}, TXEndOffset}, {PrevOffset, DataRootEntriesAcc}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = BlockStart + PrevOffset,
				TXSize = TXEndOffset - PrevOffset,
				{TXEndOffset,
						[{DataRoot, TXSize, TXOffset, TXPath} | DataRootEntriesAcc]}
		end,
		{0, []},
		SizeTaggedTXs
	),
	{TXRoot, BlockSize, DataRootEntries}.

%% @doc Store all data roots for a given block.
%% DataRootEntries is a list of `{DataRoot, TXSize, TXStartOffset, TXPath}` tuples.
store_block(BlockStart, BlockSize, TXRoot, DataRootEntries, StoreID) ->
	% Update index_db
	lists:foreach(
		fun({DataRoot, TXSize, TXStartOffset, TXPath}) ->
			ok = ar_kv:put(index_db(StoreID),
				index_key(DataRoot, TXSize, TXStartOffset), TXPath)
		end,
		DataRootEntries
	),
	% Update keys_db
	DataRootIDs = sets:from_list([
		id(DataRoot, TXSize)
		|| {DataRoot, TXSize, _TXStart, _TXPath} <- DataRootEntries
	]),
	ok = ar_kv:put(keys_db(StoreID),
			<< BlockStart:?OFFSET_KEY_BITSIZE >>,
			term_to_binary({TXRoot, BlockSize, DataRootIDs})),
	{ok, DataRootIDs}.

%% @doc Get the data roots for the block containing the given block offset.
%% Return only entries corresponding to non-empty transactions.
%% Return the complete list of entries in the order they appear in the data root index,
%% which corresponds to sorted #tx records in the block.
%% Return {ok, {TXRoot, BlockSize, DataRootEntries}}
%% or {error, Reason}.
get_block(Offset) ->
	case Offset >= ar_disk_pool:get_threshold() of
		true ->
			{error, not_found};
		false ->
			{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(Offset),
			true = Offset >= BlockStart andalso Offset < BlockEnd,
			case get_keys(BlockStart, ?DEFAULT_MODULE) of
				not_found ->
					{error, not_found};
				{ok, Bin} ->
					{TXRoot2, BlockSize, DataRootIDs} = binary_to_term(Bin),
					true = TXRoot2 == TXRoot,
					DataRootEntriesLists = sets:fold(
						fun(<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Acc) ->
							%% List of lists is intended. We'll keep one result list per key and
							%% flatten once after the fold.
							[get_all_in_range(DataRoot, TXSize, BlockStart, BlockEnd,
									?DEFAULT_MODULE) | Acc]
						end,
						[],
						DataRootIDs
					),
					DataRootEntries = lists:append(DataRootEntriesLists),
					SortedDataRootEntries = lists:sort(
						fun({_DataRoot1, _TXSize1, TXStart1, _TXPath1},
								{_DataRoot2, _TXSize2, TXStart2, _TXPath2}) ->
							TXStart1 < TXStart2
						end,
						DataRootEntries
					),
					{ok, {TXRoot, BlockSize, SortedDataRootEntries}}
			end
	end.

%% @doc Validate the given data roots against the local block index.
%% Also recompute the TXRoot from entries and verify Merkle paths.
validate_data_roots(TXRoot, BlockSize, DataRootEntries, Offset) ->
	{BlockStart, BlockEnd, ExpectedTXRoot} = ar_block_index:get_block_bounds(Offset),
	maybe
		ok ?=
			case Offset >= BlockStart andalso Offset < BlockEnd of
				true ->
					ok;
				false ->
					{error, invalid_block_bounds}
			end,
		ok ?=
			case BlockSize == BlockEnd - BlockStart of
				true ->
					ok;
				false ->
					{error, invalid_block_size}
			end,
		ok ?=
			case TXRoot == ExpectedTXRoot of
				true ->
					ok;
				false ->
					{error, invalid_tx_root}
			end,
		ok ?= verify_data_root_entries(DataRootEntries, TXRoot, BlockStart, BlockSize, 0, 0),
		{ok, {TXRoot, BlockSize, DataRootEntries}}
	end.

%%%===================================================================
%%% Public: Entry lookup and iteration
%%%===================================================================
%% @doc Get the entry containing the given data root id.
get_entry(DataRootID, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
	% Since the index_db keys include the TX start offset, we have will stub in "a" (which is
	% guaranteed to be alphanumerically greater than any TX offset) and then query the
	% previous key.
	DataRootIndexKey = << DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			<<"a">>/binary >>,
	prev_entry(DataRootIndexKey, DataRoot, TXSize, StoreID).

iterator(DataRootID, TXStartOffset, StoreID) ->
	{DataRootID, TXStartOffset, TXStartOffset, StoreID, 1}.

next(Args) ->
	{ok, Config} = arweave_config:get_env(),
	next(Args, Config#config.max_duplicate_data_roots).

next({_, _, _, _, Count}, Limit) when Count > Limit ->
	none;
next({_, 0, _, _, _}, _Limit) ->
	none;
next(Args, _Limit) ->
	{DataRootID, TXStartOffset, LatestTXStartOffset, StoreID, Count} = Args,
	case prev_entry(DataRootID, TXStartOffset, StoreID) of
		not_found ->
			none;
		{ok, {_DataRoot, _TXSize, TXStartOffset2, _TXPath} = Entry} ->
			{ok, Entry,
					{DataRootID, TXStartOffset2, LatestTXStartOffset, StoreID,
							Count + 1}};
		{error, _} = Error ->
			Error
	end.

reset({DataRootID, _, TXStartOffset, StoreID, _}) ->
	{DataRootID, TXStartOffset, TXStartOffset, StoreID, 1}.

id(Iterator) ->
	element(1, Iterator).

%%%===================================================================
%%% Public: Sync status and maintenance
%%%===================================================================
remove_range(Start, End, StoreID) ->
	{ok, RemovedDataRoots} =
		case ar_kv:get_range(keys_db(StoreID), << Start:?OFFSET_KEY_BITSIZE >>,
				<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
			{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
				{ok, sets:new()};
			{ok, Map} ->
				maps:fold(
					fun
						(_, _Value, {error, _} = Error) ->
							Error;
						(_, Value, {ok, RemovedDataRoots}) ->
							{_TXRoot, _BlockSize, DataRootIDs} = binary_to_term(Value, [safe]),
							sets:fold(
								fun (_Key, {error, _} = Error) ->
										Error;
									(<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
											{ok, Removed}) ->
										case remove(DataRootID, Start, End, StoreID) of
											removed ->
												{ok, sets:add_element(DataRootID, Removed)};
											ok ->
												{ok, Removed};
											Error ->
												Error
										end;
									(_, Acc) ->
										Acc
								end,
								{ok, RemovedDataRoots},
								DataRootIDs
							)
					end,
					{ok, sets:new()},
					Map
				);
			Error ->
				Error
		end,
	ok = ar_kv:delete_range(keys_db(StoreID),
		<< Start:?OFFSET_KEY_BITSIZE >>, << End:?OFFSET_KEY_BITSIZE >>),
	{ok, RemovedDataRoots}.

%% @doc Return true if the data roots for the given block range are synced, false otherwise.
are_synced(B, StoreID) ->
	BlockEnd = B#block.weave_size,
	BlockStart = BlockEnd - B#block.block_size,
	are_synced(BlockStart, BlockEnd, B#block.tx_root, StoreID).
are_synced(BlockStart, BlockEnd, TXRoot, StoreID) ->
	case get_keys(BlockStart, StoreID) of
		not_found ->
			false;
		{ok, Bin} ->
			{TXRoot2, BlockSize, _DataRootIDs} = binary_to_term(Bin),
			TXRoot2 == TXRoot andalso BlockSize == BlockEnd - BlockStart
	end.

%% @doc Returns true if the given data root id is in the data root index.
is_synced(DataRootID, StoreID) ->
	case get_entry(DataRootID, StoreID) of
		{ok, _} ->
			true;
		_ ->
			false
	end.

%%%===================================================================
%%% Public: Repair
%%%===================================================================
repair(BI, StoreID, RemoveTXRangeFun) ->
	MigrationDB = ar_data_sync:migration_db(StoreID),
	case ar_kv:get(MigrationDB, <<"repair_data_root_offset_index">>) of
		not_found ->
			?LOG_INFO([{event, starting_data_root_offset_index_scan}]),
			ReverseBI = lists:reverse(BI),
			ResyncBlocks = repair(ReverseBI, <<>>, 0, [], StoreID, RemoveTXRangeFun),
			ok = ar_kv:put(MigrationDB, <<"repair_data_root_offset_index">>, <<>>),
			?LOG_INFO([{event, data_root_offset_index_scan_complete}]),
			{ok, ResyncBlocks};
		_ ->
			ok
	end.

%%%===================================================================
%%% Private: DB internals
%%%===================================================================
%% @doc Maintains a record of the data roots that have been synced and mapping of data roots
%% to TX start offsets and paths.
index_db(StoreID) ->
	{data_root_index, StoreID}.

get_keys(BlockStart, StoreID) ->
	ar_kv:get(keys_db(StoreID), << BlockStart:?OFFSET_KEY_BITSIZE >>).

id(DataRoot, TXSize) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>.

index_key(DataRoot, TXSize, TXStartOffset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(TXStartOffset, 8))/binary >>.

parse_index_key(DataRootIndexKey) ->
	<< DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
		TXStartOffsetSize:8, TXStartOffset:(TXStartOffsetSize * 8) >> = DataRootIndexKey,
	{DataRoot, TXSize, TXStartOffset}.

%%%===================================================================
%%% Private: Entry lookup and range operations
%%%===================================================================
prev_entry(DataRootID, TXStartOffset, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
	DataRootIndexKey = index_key(DataRoot, TXSize, TXStartOffset - 1),
	prev_entry(DataRootIndexKey, StoreID).

prev_entry(DataRootIndexKey, StoreID) ->
	{DataRoot, TXSize, _TXStartOffset} = parse_index_key(DataRootIndexKey),
	prev_entry(DataRootIndexKey, DataRoot, TXSize, StoreID).

prev_entry(DataRootIndexKey, DataRoot, TXSize, StoreID) ->
	case ar_kv:get_prev(index_db(StoreID), DataRootIndexKey) of
		none ->
			not_found;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				OffsetSize:8, TXStartOffset:(OffsetSize * 8) >>, TXPath} ->
			{ok, {DataRoot, TXSize, TXStartOffset, TXPath}};
		{ok, _, _} ->
			not_found;
		{error, _} = Error ->
			Error
	end.

remove(DataRootID, Start, End, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootID,
	StartIndexKey = index_key(DataRoot, TXSize, Start),
	EndIndexKey = index_key(DataRoot, TXSize, End),
	case ar_kv:delete_range(index_db(StoreID), StartIndexKey, EndIndexKey) of
		ok ->
			case prev_entry(StartIndexKey, StoreID) of
				{ok, _} ->
					ok;
				not_found ->
					removed;
				{error, _} = Error ->
					Error
			end;
		Error ->
			Error
	end.

%% @doc Get all the matching data root tuples matching DataRoom in the provided range
get_all_in_range(_DataRoot, _TXSize, Start, Cursor, _StoreID) when Cursor =< Start ->
	[];
get_all_in_range(DataRoot, TXSize, Start, Cursor, StoreID) ->
	StartIndexKey = index_key(DataRoot, TXSize, Start),
	EndIndexKey = index_key(DataRoot, TXSize, Cursor - 1),
	case ar_kv:get_range(index_db(StoreID), StartIndexKey, EndIndexKey) of
		{ok, Range} ->
			DataRootEntries = maps:fold(
				fun(DataRootIndexKey, TXPath, Acc) ->
					case parse_index_key(DataRootIndexKey) of
						{DataRoot, TXSize, TXStart} ->
							[{DataRoot, TXSize, TXStart, TXPath} | Acc];
						_ ->
							Acc
					end
				end,
				[],
				Range
			),
			lists:sort(
				fun({_DataRoot1, _TXSize1, TXStart1, _TXPath1},
						{_DataRoot2, _TXSize2, TXStart2, _TXPath2}) ->
					TXStart1 > TXStart2
				end,
				DataRootEntries
			);
		_ ->
			[]
	end.

%%%===================================================================
%%% Private: Validation
%%%===================================================================
get_padded_size(TXSize, BlockStart) ->
	case BlockStart >= ar_block:strict_data_split_threshold() of
		true ->
			ar_poa:get_padded_offset(TXSize, 0);
		false ->
			TXSize
	end.

verify_data_root_entries([], _TXRoot, _BlockStart, BlockSize, Total, Total)
		when Total == BlockSize ->
	ok;
verify_data_root_entries([], _TXRoot, _BlockStart, _BlockSize, _TXStartOffset, _Total) ->
	{error, invalid_total_tx_size};
verify_data_root_entries([{_DataRoot, 0, _TXStartOffset, _TXPath} | _], _TXRoot, _BlockStart,
		_BlockSize, _ExpectedTXStartOffset, _Total) ->
	{error, invalid_zero_tx_size};
verify_data_root_entries([{DataRoot, TXSize, TXStartOffset, TXPath} | DataRootEntries], TXRoot,
		BlockStart, BlockSize, ExpectedTXStartOffset, Total) ->
	TXEndOffset = TXStartOffset + TXSize - BlockStart,
	case TXEndOffset >= 0 of
		false ->
			{error, invalid_entry_merkle_label};
		true ->
			case ar_merkle:validate_path(TXRoot, TXEndOffset - 1, BlockSize, TXPath) of
				false ->
					{error, invalid_tx_path};
				{DataRoot, ExpectedTXStartOffset, TXEndOffset} ->
					PaddedTXSize = get_padded_size(TXSize, BlockStart),
					PaddedEndOffset = get_padded_size(TXEndOffset, BlockStart),
					verify_data_root_entries(
						DataRootEntries,
						TXRoot,
						BlockStart,
						BlockSize,
						PaddedEndOffset,
						Total + PaddedTXSize
					);
				_ ->
					{error, invalid_tx_path}
			end
	end.

%%%===================================================================
%%% Private: Repair
%%%===================================================================
repair(BI, Cursor, Height, ResyncBlocks, StoreID, RemoveTXRangeFun) ->
	case ar_kv:get_next(keys_db(StoreID), Cursor) of
		none ->
			ResyncBlocks;
		{ok, Key, Value} ->
			<< BlockStart:?OFFSET_KEY_BITSIZE >> = Key,
			{TXRoot, BlockSize, _DataRootIDs} = binary_to_term(Value, [safe]),
			BlockEnd = BlockStart + BlockSize,
			case shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks, BI) of
				{ok, {Height2, BI2}} ->
					Cursor2 = << (BlockStart + 1):?OFFSET_KEY_BITSIZE >>,
					repair(BI2, Cursor2, Height2, ResyncBlocks, StoreID, RemoveTXRangeFun);
				{bad_key, []} ->
					ResyncBlocks;
				{bad_key, ResyncBlocks2} ->
					?LOG_INFO([{event, removing_data_root_index_range},
							{range_start, BlockStart}, {range_end, BlockEnd}]),
					ok = RemoveTXRangeFun(BlockStart, BlockEnd),
					{ok, _} = remove_range(BlockStart, BlockEnd, StoreID),
					repair(BI, Cursor, Height, ResyncBlocks2, StoreID, RemoveTXRangeFun)
			end
	end.

shift_block_index(_TXRoot, _BlockStart, _BlockEnd, _Height, ResyncBlocks, []) ->
	{bad_key, ResyncBlocks};
shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks,
		[{_H, WeaveSize, _TXRoot} | BI]) when BlockEnd > WeaveSize ->
	ResyncBlocks2 =
		case BlockStart < WeaveSize of
			true -> [Height | ResyncBlocks];
			false -> ResyncBlocks
		end,
	shift_block_index(TXRoot, BlockStart, BlockEnd, Height + 1, ResyncBlocks2, BI);
shift_block_index(TXRoot, _BlockStart, WeaveSize, Height, _ResyncBlocks,
		[{_H, WeaveSize, TXRoot} | BI]) ->
	{ok, {Height + 1, BI}};
shift_block_index(_TXRoot, _BlockStart, _WeaveSize, Height, ResyncBlocks, _BI) ->
	{bad_key, [Height | ResyncBlocks]}.

%%%===================================================================
%%% Tests.
%%%===================================================================

get_all_in_range_returns_multiple_matches_for_same_pair_test() ->
	with_test_index_db(
		fun(StoreID) ->
			DataRoot = << 1:256 >>,
			TXSize = 100,
			ok = put_test_tx(StoreID, DataRoot, TXSize, 10, <<"path-10">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize, 20, <<"path-20">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize, 30, <<"path-30">>),
			?assertEqual(
				[
					{DataRoot, TXSize, 30, <<"path-30">>},
					{DataRoot, TXSize, 20, <<"path-20">>},
					{DataRoot, TXSize, 10, <<"path-10">>}
				],
				get_all_in_range(DataRoot, TXSize, 10, 40, StoreID)
			)
		end
	).

get_all_in_range_excludes_matches_outside_start_and_cursor_test() ->
	with_test_index_db(
		fun(StoreID) ->
			DataRoot = << 2:256 >>,
			TXSize = 100,
			ok = put_test_tx(StoreID, DataRoot, TXSize, 5, <<"path-5">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize, 10, <<"path-10">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize, 20, <<"path-20">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize, 30, <<"path-30">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize, 40, <<"path-40">>),
			?assertEqual(
				[
					{DataRoot, TXSize, 30, <<"path-30">>},
					{DataRoot, TXSize, 20, <<"path-20">>}
				],
				get_all_in_range(DataRoot, TXSize, 15, 35, StoreID)
			)
		end
	).

get_all_in_range_ignores_other_data_root_and_tx_size_test() ->
	with_test_index_db(
		fun(StoreID) ->
			DataRoot = << 3:256 >>,
			OtherDataRoot = << 4:256 >>,
			TXSize = 100,
			ok = put_test_tx(StoreID, DataRoot, TXSize, 10, <<"path-10">>),
			ok = put_test_tx(StoreID, DataRoot, TXSize + 1, 20, <<"wrong-size">>),
			ok = put_test_tx(StoreID, OtherDataRoot, TXSize, 30, <<"wrong-root">>),
			?assertEqual(
				[{DataRoot, TXSize, 10, <<"path-10">>}],
				get_all_in_range(DataRoot, TXSize, 0, 40, StoreID)
			)
		end
	).

validate_data_roots_accepts_valid_entries_test() ->
	BlockStart = 0,
	Offset = 1,
	{TXRoot, BlockSize, DataRootEntries} = make_valid_data_root_entries(BlockStart, [10, 20]),
	with_mocked_block_bounds(
		BlockStart,
		BlockStart + BlockSize,
		TXRoot,
		fun() ->
			?assertEqual(
				{ok, {TXRoot, BlockSize, DataRootEntries}},
				validate_data_roots(TXRoot, BlockSize, DataRootEntries, Offset)
			)
		end
	).

validate_data_roots_rejects_invalid_tx_path_test() ->
	BlockStart = 0,
	Offset = 1,
	{TXRoot, BlockSize, [{DataRoot, TXSize, TXStartOffset, TXPath} | Rest]} =
		make_valid_data_root_entries(BlockStart, [10, 20]),
	InvalidEntries = [{DataRoot, TXSize, TXStartOffset, << TXPath/binary, 0 >>} | Rest],
	with_mocked_block_bounds(
		BlockStart,
		BlockStart + BlockSize,
		TXRoot,
		fun() ->
			?assertEqual(
				{error, invalid_tx_path},
				validate_data_roots(TXRoot, BlockSize, InvalidEntries, Offset)
			)
		end
	).

validate_data_roots_rejects_invalid_zero_tx_size_test() ->
	BlockStart = 0,
	Offset = 1,
	{TXRoot, BlockSize, [{DataRoot, _TXSize, TXStartOffset, TXPath} | Rest]} =
		make_valid_data_root_entries(BlockStart, [10, 20]),
	InvalidEntries = [{DataRoot, 0, TXStartOffset, TXPath} | Rest],
	with_mocked_block_bounds(
		BlockStart,
		BlockStart + BlockSize,
		TXRoot,
		fun() ->
			?assertEqual(
				{error, invalid_zero_tx_size},
				validate_data_roots(TXRoot, BlockSize, InvalidEntries, Offset)
			)
		end
	).

validate_data_roots_rejects_invalid_total_tx_size_test() ->
	BlockStart = 0,
	Offset = 1,
	{TXRoot, BlockSize, [Entry | _Rest]} =
		make_valid_data_root_entries(BlockStart, [10, 20]),
	InvalidEntries = [Entry],
	with_mocked_block_bounds(
		BlockStart,
		BlockStart + BlockSize,
		TXRoot,
		fun() ->
			?assertEqual(
				{error, invalid_total_tx_size},
				validate_data_roots(TXRoot, BlockSize, InvalidEntries, Offset)
			)
		end
	).

validate_data_roots_rejects_invalid_tx_root_test() ->
	BlockStart = 0,
	Offset = 1,
	{TXRoot, BlockSize, DataRootEntries} = make_valid_data_root_entries(BlockStart, [10, 20]),
	InvalidTXRoot = << 999:256 >>,
	with_mocked_block_bounds(
		BlockStart,
		BlockStart + BlockSize,
		TXRoot,
		fun() ->
			?assertEqual(
				{error, invalid_tx_root},
				validate_data_roots(InvalidTXRoot, BlockSize, DataRootEntries, Offset)
			)
		end
	).

with_test_index_db(Fun) ->
	ensure_test_kv_started(),
	Unique = integer_to_list(erlang:unique_integer([positive])),
	DBName = "ar_data_roots_test_" ++ Unique,
	StoreID = {ar_data_roots_test, Unique},
	ok = ar_kv:test_destroy(DBName),
	ok = open_index_db(ar_kv:test_db_path(), DBName, StoreID, []),
	try
		Fun(StoreID)
	after
		_ = ar_kv:test_close(index_db(StoreID)),
		ok = ar_kv:test_destroy(DBName)
	end.

ensure_test_kv_started() ->
	case ar_kv_sup:start_link() of
		{ok, _} ->
			ok;
		{error, {already_started, _}} ->
			ok
	end.

put_test_tx(StoreID, DataRoot, TXSize, TXStartOffset, TXPath) ->
	ar_kv:put(index_db(StoreID), index_key(DataRoot, TXSize, TXStartOffset), TXPath).

with_mocked_block_bounds(BlockStart, BlockEnd, TXRoot, Fun) ->
	meck:new(ar_block_index, [passthrough]),
	meck:expect(
		ar_block_index,
		get_block_bounds,
		fun(_) -> {BlockStart, BlockEnd, TXRoot} end
	),
	try
		Fun()
	after
		ok = meck:unload(ar_block_index)
	end.

make_valid_data_root_entries(BlockStart, TXSizes) ->
	SizeTaggedTXs =
		lists:mapfoldl(
			fun(TXSize, TXEndOffset) ->
				DataRoot = << TXEndOffset:256 >>,
				NextTXEndOffset = TXEndOffset + TXSize,
				{{{dummy, DataRoot}, NextTXEndOffset}, NextTXEndOffset}
			end,
			0,
			TXSizes
		),
	{SizeTaggedTXs2, _} = SizeTaggedTXs,
	{TXRoot, BlockSize, DataRootEntriesReversed} =
		build_block_data_root_entries(BlockStart, SizeTaggedTXs2),
	DataRootEntries = lists:reverse(DataRootEntriesReversed),
	{TXRoot, BlockSize, DataRootEntries}.
