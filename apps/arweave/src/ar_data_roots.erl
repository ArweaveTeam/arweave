-module(ar_data_roots).

-export([
	open_index_db/3,
	old_column_family/1,
	offset_column_family/1,
	old_db/1,
	offset_db/1,
	update/5,
	get_offset/2,
	get_offset_entry/2,
	put_offset_entry/5,
	get_next_offset_entry/2,
	get_prev_offset/3,
	remove_range/3,
	iterator_v2/3,
	next_v2/2,
	reset/1,
	get_key/1,
	get_for_offset/1,
	are_synced/3,
	start_old_data_root_index_migration/1,
	continue_old_data_root_index_migration/3,
	is_migration_complete/0
]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

open_index_db(Dir, StoreID, BloomFilterOpts) ->
	ar_kv:open(#{
		path => filename:join(Dir, "ar_data_sync_data_root_index_db"),
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

index_db(StoreID) ->
	{data_root_index, StoreID}.

%% A reference to the on-disk key-value storage mapping
%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootIndexKeySet}.
%% Each key in DataRootIndexKeySet is a << DataRoot/binary, TXSize:256 >> binary.
%% Used to remove orphaned entries from DataRootIndex.
offset_db(StoreID) ->
	{data_root_offset_index, StoreID}.

old_db(StoreID) ->
	{data_root_index_old, StoreID}.

offset_column_family(Opts) ->
	{"data_root_offset_index", Opts}.

old_column_family(Opts) ->
	{"data_root_index", Opts}.

key_v2(DataRoot, TXSize, Offset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(Offset, 8))/binary >>.

update(DataRoot, TXSize, AbsoluteTXStartOffset, TXPath, StoreID) ->
	ar_kv:put(index_db(StoreID),
			key_v2(DataRoot, TXSize, AbsoluteTXStartOffset), TXPath).

get_offset(DataRootKey, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	DataRootIndex = index_db(StoreID),
	case ar_kv:get_prev(DataRootIndex, << DataRoot:32/binary,
			(ar_serialize:encode_int(TXSize, 8))/binary, <<"a">>/binary >>) of
		none ->
			not_found;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				OffsetSize:8, Offset:(OffsetSize * 8) >>, TXPath} ->
			{ok, {Offset, TXPath}};
		{ok, _, _} ->
			not_found;
		{error, _} = Error ->
			Error
	end.

get_offset_entry(BlockStart, StoreID) ->
	ar_kv:get(offset_db(StoreID), << BlockStart:?OFFSET_KEY_BITSIZE >>).

put_offset_entry(BlockStart, TXRoot, BlockSize, DataRootIndexKeySet, StoreID) ->
	ar_kv:put(offset_db(StoreID),
			<< BlockStart:?OFFSET_KEY_BITSIZE >>,
			term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})).

get_next_offset_entry(Cursor, StoreID) ->
	ar_kv:get_next(offset_db(StoreID), Cursor).

get_prev_offset(DataRootKey, TXStartOffset, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = key_v2(DataRoot, TXSize, TXStartOffset - 1),
	ar_kv:get_prev(index_db(StoreID), Key).

remove_range(Start, End, StoreID) ->
	{ok, RemovedDataRoots} =
		case ar_kv:get_range(offset_db(StoreID), << Start:?OFFSET_KEY_BITSIZE >>,
				<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
			{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
				{ok, sets:new()};
			{ok, Map} ->
				maps:fold(
					fun
						(_, _Value, {error, _} = Error) ->
							Error;
						(_, Value, {ok, RemovedDataRoots}) ->
							{_TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value, [safe]),
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
								DataRootIndexKeySet
							)
					end,
					{ok, sets:new()},
					Map
				);
			Error ->
				Error
		end,
	ok = ar_kv:delete_range(offset_db(StoreID),
		<< Start:?OFFSET_KEY_BITSIZE >>, << End:?OFFSET_KEY_BITSIZE >>),
	{ok, RemovedDataRoots}.

remove(DataRootKey, Start, End, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = key_v2(DataRoot, TXSize, Start),
	EndKey = key_v2(DataRoot, TXSize, End),
	case ar_kv:delete_range(index_db(StoreID), StartKey, EndKey) of
		ok ->
			case ar_kv:get_prev(index_db(StoreID), StartKey) of
				{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
						_Rest/binary >>, _} ->
					ok;
				{ok, _, _} ->
					removed;
				none ->
					removed;
				{error, _} = Error ->
					Error
			end;
		Error ->
			Error
	end.

iterator_v2(DataRootKey, TXStartOffset, StoreID) ->
	{DataRootKey, TXStartOffset, TXStartOffset, StoreID, 1}.

next_v2({_, _, _, _, Count}, Limit) when Count > Limit ->
	none;
next_v2({_, 0, _, _, _}, _Limit) ->
	none;
next_v2(Args, _Limit) ->
	{DataRootKey, TXStartOffset, LatestTXStartOffset, StoreID, Count} = Args,
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = key_v2(DataRoot, TXSize, TXStartOffset - 1),
	case ar_kv:get_prev(index_db(StoreID), Key) of
		none ->
			none;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				TXStartOffset2Size:8, TXStartOffset2:(TXStartOffset2Size * 8) >>, TXPath} ->
			{ok, TXRoot} = ar_merkle:extract_root(TXPath),
			{{TXStartOffset2, TXRoot, TXPath},
					{DataRootKey, TXStartOffset2, LatestTXStartOffset, StoreID,
							Count + 1}};
		{ok, _, _} ->
			none
	end.

reset({DataRootKey, _, TXStartOffset, StoreID, _}) ->
	{DataRootKey, TXStartOffset, TXStartOffset, StoreID, 1}.

get_key(Iterator) ->
	element(1, Iterator).

iterator(TXRootMap) ->
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

next({_Index, Count}, Limit) when Count >= Limit ->
	none;
next({Index, Count}, _Limit) ->
	case gb_sets:is_empty(Index) of
		true ->
			none;
		false ->
			{Element, Index2} = gb_sets:take_largest(Index),
			{Element, {Index2, Count + 1}}
	end.

get_for_offset(Offset) ->
	case Offset >= ar_data_sync:get_disk_pool_threshold() of
		true ->
			{error, not_found};
		false ->
			{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(Offset),
			true = Offset >= BlockStart andalso Offset < BlockEnd,
			StoreID = ?DEFAULT_MODULE,
			case get_offset_entry(BlockStart, StoreID) of
				not_found ->
					{error, not_found};
				{ok, Bin} ->
					{TXRoot2, BlockSize, DataRootIndexKeySet} = binary_to_term(Bin),
					true = TXRoot2 == TXRoot,
					{ok, {TXRoot, BlockSize, lists:sort(
						fun({_DataRoot1, _TXSize1, TXStart1, _TXPath1},
								{_DataRoot2, _TXSize2, TXStart2, _TXPath2}) ->
							TXStart1 < TXStart2
						end,
						sets:fold(
							fun(<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Acc) ->
								read_entries(DataRoot, TXSize, BlockStart, BlockEnd, StoreID, Acc)
							end,
							[],
							DataRootIndexKeySet
						))}}
			end
	end.

are_synced(BlockStart, BlockEnd, TXRoot) ->
	case get_offset_entry(BlockStart, ?DEFAULT_MODULE) of
		not_found ->
			false;
		{ok, Bin} ->
			{TXRoot2, BlockSize, _DataRootIndexKeySet} = binary_to_term(Bin),
			true = TXRoot2 == TXRoot,
			true = BlockSize == BlockEnd - BlockStart,
			true
	end.

read_entries(_DataRoot, _TXSize, _BlockStart, 0, _StoreID, Acc) ->
	Acc;
read_entries(DataRoot, TXSize, BlockStart, Cursor, StoreID, Acc) ->
	Key = key_v2(DataRoot, TXSize, Cursor - 1),
	case ar_kv:get_prev(index_db(StoreID), Key) of
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				TXStartSize:8, TXStart:(TXStartSize * 8) >>, TXPath} when TXStart >= BlockStart ->
			[{DataRoot, TXSize, TXStart, TXPath}
				| read_entries(DataRoot, TXSize, BlockStart, TXStart, StoreID, Acc)];
		{ok, _, _} ->
			Acc;
		none ->
			Acc
	end.

%%%===================================================================
%%% Old data root index migration.
%%%===================================================================
start_old_data_root_index_migration(StoreID) ->
	case start_migration(StoreID) of
		complete ->
			set_migration_complete(),
			ok;
		{continue, Cursor, N} ->
			migrate_batch(Cursor, N, StoreID)
	end.

continue_old_data_root_index_migration(Cursor, N, StoreID) ->
	case migrate_batch(Cursor, N, StoreID) of
		{continue, Cursor2, N2} ->
			?LOG_DEBUG([{event, moving_data_root_index}, {moved_keys, N}]),
			ok = ar_kv:put(ar_data_sync:migration_db(StoreID),
				<<"move_data_root_index">>, Cursor2),
			ar_data_sync:continue_old_data_root_index_migration(Cursor2, N2, StoreID);
		complete ->
			ok = ar_kv:put(ar_data_sync:migration_db(StoreID),
				<<"move_data_root_index">>, <<"complete">>),
			set_migration_complete(),
			ok
	end.

start_migration(StoreID) ->
	MigrationsIndex = ar_data_sync:migration_db(StoreID),
	DataRootIndexOld = old_db(StoreID),
	case ar_kv:get(MigrationsIndex, <<"move_data_root_index">>) of
		{ok, <<"complete">>} ->
			complete;
		{ok, Cursor} ->
			{continue, Cursor, 1};
		not_found ->
			case ar_kv:get_next(DataRootIndexOld, last) of
				none ->
					complete;
				{ok, Key, _} ->
					{continue, Key, 1}
			end
	end.

migrate_batch(Cursor, N, StoreID) ->
	Old = old_db(StoreID),
	New = index_db(StoreID),
	case N rem 50000 of
		0 ->
			{continue, Cursor, N + 1};
		_ ->
			case ar_kv:get_prev(Old, Cursor) of
				none ->
					complete;
				{ok, << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Value} ->
					M = binary_to_term(Value, [safe]),
					migrate_entries(DataRoot, TXSize, iterator(M), New),
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
	case next(Iterator, infinity) of
		none ->
			ok;
		{{Offset, _TXRoot, TXPath}, Iterator2} ->
			Key = key_v2(DataRoot, TXSize, Offset),
			ok = ar_kv:put(DB, Key, TXPath),
			migrate_entries(DataRoot, TXSize, Iterator2, DB)
	end.

set_migration_complete() ->
	ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}).

is_migration_complete() ->
	ets:member(ar_data_sync_state, move_data_root_index_migration_complete).
