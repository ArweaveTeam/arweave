-module(ar_data_roots).

-export([
	open_index_db/3,
	open_index_db/4,
	legacy_column_family/1,
	keys_column_family/1,
	legacy_db/1,
	keys_db/1,
	build_block_data_root_entries/2,
	store_block/5,
	get_entry/2,
	remove_range/3,
	iterator/3,
	next/1,
	reset/1,
	key/1,
	get_block/1,
	validate_data_roots/4,
	are_synced/2,
	are_synced/4,
	is_synced/2,
	repair/3,
	start_legacy_data_root_index_migration/1,
	continue_legacy_data_root_index_migration/3,
	is_migration_complete/0
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

keys_column_family(Opts) ->
	{"data_root_offset_index", Opts}.

legacy_column_family(Opts) ->
	{"data_root_index", Opts}.

%% A reference to the on-disk key-value storage mapping
%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootKeys}.
%% Each key in DataRootKeys is a << DataRoot/binary, TXSize:256 >> binary.
%% Used to remove orphaned entries from DataRootIndex.
keys_db(StoreID) ->
	{data_root_offset_index, StoreID}.

legacy_db(StoreID) ->
	{data_root_index_old, StoreID}.

%%%===================================================================
%%% Public: Block data root entries
%%%===================================================================
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
				key(DataRoot, TXSize, TXStartOffset), TXPath)
		end,
		DataRootEntries
	),
	% Update keys_db
	DataRootKeys = sets:from_list([
		<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>
		|| {DataRoot, TXSize, _TXStart, _TXPath} <- DataRootEntries
	]),
	ok = ar_kv:put(keys_db(StoreID),
			<< BlockStart:?OFFSET_KEY_BITSIZE >>,
			term_to_binary({TXRoot, BlockSize, DataRootKeys})),
	{ok, DataRootKeys}.

%% @doc Get the data roots for the block containing the given block offset.
%% Return only entries corresponding to non-empty transactions.
%% Return the complete list of entries in the order they appear in the data root index,
%% which corresponds to sorted #tx records in the block.
%% Return {ok, {TXRoot, BlockSize, DataRootEntries}}
%% or {error, Reason}.
get_block(Offset) ->
	case Offset >= ar_data_sync:get_disk_pool_threshold() of
		true ->
			{error, not_found};
		false ->
			{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(Offset),
			true = Offset >= BlockStart andalso Offset < BlockEnd,
			case get_keys(BlockStart, ?DEFAULT_MODULE) of
				not_found ->
					{error, not_found};
				{ok, Bin} ->
					{TXRoot2, BlockSize, DataRootKeys} = binary_to_term(Bin),
					true = TXRoot2 == TXRoot,
					DataRootEntriesLists = sets:fold(
						fun(<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Acc) ->
							%% List of lists is intended. We'll keep one result list per key and
							%% flatten once after the fold.
							[get_all_in_range(DataRoot, TXSize, BlockStart, BlockEnd,
									?DEFAULT_MODULE) | Acc]
						end,
						[],
						DataRootKeys
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
%% @doc Get the entry containing the given data root key.
get_entry(DataRootKey, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	% Since the index_db keys include the TX start offset, we have will stub in "a" (which is
	% guaranteed to be alphanumerically greater than any TX offset) and then query the
	% previous key.
	Key = << DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			<<"a">>/binary >>,
	prev_entry(Key, DataRoot, TXSize, StoreID).

iterator(DataRootKey, TXStartOffset, StoreID) ->
	{DataRootKey, TXStartOffset, TXStartOffset, StoreID, 1}.

next(Args) ->
	{ok, Config} = arweave_config:get_env(),
	next(Args, Config#config.max_duplicate_data_roots).

next({_, _, _, _, Count}, Limit) when Count > Limit ->
	none;
next({_, 0, _, _, _}, _Limit) ->
	none;
next(Args, _Limit) ->
	{DataRootKey, TXStartOffset, LatestTXStartOffset, StoreID, Count} = Args,
	case prev_entry(DataRootKey, TXStartOffset, StoreID) of
		not_found ->
			none;
		{ok, {_DataRoot, _TXSize, TXStartOffset2, _TXPath} = Entry} ->
			{ok, Entry,
					{DataRootKey, TXStartOffset2, LatestTXStartOffset, StoreID,
							Count + 1}};
		{error, _} = Error ->
			Error
	end.

reset({DataRootKey, _, TXStartOffset, StoreID, _}) ->
	{DataRootKey, TXStartOffset, TXStartOffset, StoreID, 1}.

key(Iterator) ->
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
							{_TXRoot, _BlockSize, DataRootKeys} = binary_to_term(Value, [safe]),
							sets:fold(
								fun (_Key, {error, _} = Error) ->
										Error;
									(<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = Key,
											{ok, Removed}) ->
										case remove(Key, Start, End, StoreID) of
											removed ->
												{ok, sets:add_element(Key, Removed)};
											ok ->
												{ok, Removed};
											Error ->
												Error
										end;
									(_, Acc) ->
										Acc
								end,
								{ok, RemovedDataRoots},
								DataRootKeys
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
			{TXRoot2, BlockSize, _DataRootKeys} = binary_to_term(Bin),
			true = TXRoot2 == TXRoot,
			true = BlockSize == BlockEnd - BlockStart,
			true
	end.

%% @doc Returns true if the given data root key is in the data root index.
is_synced(DataRootKey, StoreID) ->
	case get_entry(DataRootKey, StoreID) of
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
%%% Public: Legacy data root index migration
%%%===================================================================
start_legacy_data_root_index_migration(StoreID) ->
	case start_migration(StoreID) of
		complete ->
			set_migration_complete(),
			ok;
		{continue, Cursor, N} ->
			migrate_batch(Cursor, N, StoreID)
	end.

continue_legacy_data_root_index_migration(Cursor, N, StoreID) ->
	case migrate_batch(Cursor, N, StoreID) of
		{continue, Cursor2, N2} ->
			?LOG_DEBUG([{event, moving_data_root_index}, {moved_keys, N}]),
			ok = ar_kv:put(ar_data_sync:migration_db(StoreID),
				<<"move_data_root_index">>, Cursor2),
			ar_data_sync:continue_legacy_data_root_index_migration(Cursor2, N2, StoreID);
		complete ->
			ok = ar_kv:put(ar_data_sync:migration_db(StoreID),
				<<"move_data_root_index">>, <<"complete">>),
			set_migration_complete(),
			ok
	end.

is_migration_complete() ->
	ets:member(ar_data_sync_state, move_data_root_index_migration_complete).

%%%===================================================================
%%% Private: DB internals
%%%===================================================================
%% @doc Maintains a record of the data roots that have been synced and mapping of data roots
%% to TX start offsets and paths.
index_db(StoreID) ->
	{data_root_index, StoreID}.

get_keys(BlockStart, StoreID) ->
	ar_kv:get(keys_db(StoreID), << BlockStart:?OFFSET_KEY_BITSIZE >>).

key(DataRoot, TXSize, TXStartOffset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(TXStartOffset, 8))/binary >>.

parse_key(Key) ->
	<< DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
		TXStartOffsetSize:8, TXStartOffset:(TXStartOffsetSize * 8) >> = Key,
	{DataRoot, TXSize, TXStartOffset}.

%%%===================================================================
%%% Private: Entry lookup and range operations
%%%===================================================================
prev_entry(DataRootKey, TXStartOffset, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = key(DataRoot, TXSize, TXStartOffset - 1),
	prev_entry(Key, StoreID).

prev_entry(Key, StoreID) ->
	{DataRoot, TXSize, _TXStartOffset} = parse_key(Key),
	prev_entry(Key, DataRoot, TXSize, StoreID).

prev_entry(Key, DataRoot, TXSize, StoreID) ->
	case ar_kv:get_prev(index_db(StoreID), Key) of
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

remove(DataRootKey, Start, End, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = key(DataRoot, TXSize, Start),
	EndKey = key(DataRoot, TXSize, End),
	case ar_kv:delete_range(index_db(StoreID), StartKey, EndKey) of
		ok ->
			case prev_entry(StartKey, StoreID) of
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
	StartKey = key(DataRoot, TXSize, Start),
	EndKey = key(DataRoot, TXSize, Cursor - 1),
	case ar_kv:get_range(index_db(StoreID), StartKey, EndKey) of
		{ok, Range} ->
			DataRootEntries = maps:fold(
				fun(Key, TXPath, Acc) ->
					case parse_key(Key) of
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
			{TXRoot, BlockSize, _DataRootKeys} = binary_to_term(Value, [safe]),
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
%%% Private: Legacy data root index migration
%%%===================================================================
start_migration(StoreID) ->
	case ar_kv:get(ar_data_sync:migration_db(StoreID), <<"move_data_root_index">>) of
		{ok, <<"complete">>} ->
			complete;
		{ok, Cursor} ->
			{continue, Cursor, 1};
		not_found ->
			case ar_kv:get_next(legacy_db(StoreID), last) of
				none ->
					complete;
				{ok, Key, _} ->
					{continue, Key, 1}
			end
	end.

migrate_batch(Cursor, N, StoreID) ->
	case N rem 50000 of
		0 ->
			{continue, Cursor, N + 1};
		_ ->
			case ar_kv:get_prev(legacy_db(StoreID), Cursor) of
				none ->
					complete;
				{ok, << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Value} ->
					M = binary_to_term(Value, [safe]),
					migrate_entries(DataRoot, TXSize, legacy_iterator(M), index_db(StoreID)),
					PrevKey = << DataRoot:32/binary, (TXSize - 1):?OFFSET_KEY_BITSIZE >>,
					migrate_batch(PrevKey, N + 1, StoreID);
				{ok, Key, _} ->
					%% The empty data root key (from transactions without data) was
					%% unnecessarily recorded in the index.
					PrevKey = binary:part(Key, 0, byte_size(Key) - 1),
					migrate_batch(PrevKey, N + 1, StoreID)
			end
	end.

migrate_entries(DataRoot, TXSize, Iterator, DB) ->
	case legacy_next(Iterator, infinity) of
		none ->
			ok;
		{{Offset, _TXRoot, TXPath}, Iterator2} ->
			Key = key(DataRoot, TXSize, Offset),
			ok = ar_kv:put(DB, Key, TXPath),
			migrate_entries(DataRoot, TXSize, Iterator2, DB)
	end.

legacy_iterator(TXRootMap) ->
	{maps:fold(
		fun(TXRoot, Map, Acc) ->
			maps:fold(
				fun(Offset, TXPath, Acc2) ->
					gb_sets:insert({Offset, TXRoot, TXPath}, Acc2)
				end,
				Acc,
				Map
			)
		end,
		gb_sets:new(),
		TXRootMap
	), 0}.

legacy_next({_Index, Count}, Limit) when Count >= Limit ->
	none;
legacy_next({Index, Count}, _Limit) ->
	case gb_sets:is_empty(Index) of
		true ->
			none;
		false ->
			{Element, Index2} = gb_sets:take_largest(Index),
			{Element, {Index2, Count + 1}}
	end.

set_migration_complete() ->
	ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}).

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
	ar_kv:put(index_db(StoreID), key(DataRoot, TXSize, TXStartOffset), TXPath).

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
