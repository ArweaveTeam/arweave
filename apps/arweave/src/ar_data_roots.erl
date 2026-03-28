-module(ar_data_roots).

-export([
	open_index_db/3,
	old_column_family/1,
	keys_column_family/1,
	index_db/1,
	old_db/1,
	keys_db/1,
	store_block/5,
	get_tx/2,
	get_prev_tx/3,
	remove_range/3,
	iterator_v2/3,
	next_v2/2,
	reset/1,
	get_key/1,
	get_for_offset/1,
	are_synced/2,
	are_synced/4,
	is_synced/2,
	repair/3,
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

%% @doc Maintains a record of the data roots that have been synced and mapping of data roots
%% to TX start offsets and paths.
index_db(StoreID) ->
	{data_root_index, StoreID}.

%% A reference to the on-disk key-value storage mapping
%% AbsoluteBlockStartOffset => {TXRoot, BlockSize, DataRootKeys}.
%% Each key in DataRootKeys is a << DataRoot/binary, TXSize:256 >> binary.
%% Used to remove orphaned entries from DataRootIndex.
keys_db(StoreID) ->
	{data_root_offset_index, StoreID}.

old_db(StoreID) ->
	{data_root_index_old, StoreID}.

keys_column_family(Opts) ->
	{"data_root_offset_index", Opts}.

old_column_family(Opts) ->
	{"data_root_index", Opts}.

key_v2(DataRoot, TXSize, TXStartOffset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(TXStartOffset, 8))/binary >>.

parse_key_v2(Key) ->
	<< DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
		TXStartOffsetSize:8, TXStartOffset:(TXStartOffsetSize * 8) >> = Key,
	{DataRoot, TXSize, TXStartOffset}.

%% @doc Store all data roots for a given block. 
%% DataRoots is a list of {DataRoot, TXSize, TXStartOffset, TXPath} tuples.
store_block(BlockStart, BlockSize, TXRoot, DataRootTuples, StoreID) ->
	% Update index_db
	lists:foreach(
		fun({DataRoot, TXSize, TXStartOffset, TXPath}) ->
			ok = ar_kv:put(index_db(StoreID),
				key_v2(DataRoot, TXSize, TXStartOffset), TXPath)
		end,
		DataRootTuples
	),
	% Update keys_db
	DataRootKeys = sets:from_list([
		<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>
		|| {DataRoot, TXSize, _TXStart, _TXPath} <- DataRootTuples
	]),
	ok = ar_kv:put(keys_db(StoreID),
			<< BlockStart:?OFFSET_KEY_BITSIZE >>,
			term_to_binary({TXRoot, BlockSize, DataRootKeys})),
	{ok, DataRootKeys}.

%% @doc Get the start offset of the TX containing the given data root.
get_tx(DataRootKey, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	% Since the index_db keys include the TX start offset, we have will stub in "a" (which is
	% guaranteed to be alphanumerically greater than any TX offset) and then query the 
	% previous key.
	Key = << DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary, <<"a">>/binary >>,
	get_prev_tx(Key, DataRoot, TXSize, StoreID).

get_prev_tx(DataRootKey, TXStartOffset, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = key_v2(DataRoot, TXSize, TXStartOffset - 1),
	get_prev_tx(Key, StoreID).

get_prev_tx(Key, StoreID) ->
	{DataRoot, TXSize, _TXStartOffset} = parse_key_v2(Key),
	get_prev_tx(Key, DataRoot, TXSize, StoreID).

get_prev_tx(Key, DataRoot, TXSize, StoreID) ->
	case ar_kv:get_prev(index_db(StoreID), Key) of
		none ->
			not_found;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				OffsetSize:8, TXStartOffset:(OffsetSize * 8) >>, TXPath} ->
			{ok, {TXStartOffset, TXPath}};
		{ok, _, _} ->
			not_found;
		{error, _} = Error ->
			Error
	end.

get_keys(BlockStart, StoreID) ->
	ar_kv:get(keys_db(StoreID), << BlockStart:?OFFSET_KEY_BITSIZE >>).

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

remove(DataRootKey, Start, End, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = key_v2(DataRoot, TXSize, Start),
	EndKey = key_v2(DataRoot, TXSize, End),
	case ar_kv:delete_range(index_db(StoreID), StartKey, EndKey) of
		ok ->
			case get_prev_tx(StartKey, StoreID) of
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

iterator_v2(DataRootKey, TXStartOffset, StoreID) ->
	{DataRootKey, TXStartOffset, TXStartOffset, StoreID, 1}.

next_v2({_, _, _, _, Count}, Limit) when Count > Limit ->
	none;
next_v2({_, 0, _, _, _}, _Limit) ->
	none;
next_v2(Args, _Limit) ->
	{DataRootKey, TXStartOffset, LatestTXStartOffset, StoreID, Count} = Args,
	case get_prev_tx(DataRootKey, TXStartOffset, StoreID) of
		not_found ->
			none;
		{ok, {TXStartOffset2, TXPath}} ->
			{ok, TXRoot} = ar_merkle:extract_root(TXPath),
			{{TXStartOffset2, TXRoot, TXPath},
					{DataRootKey, TXStartOffset2, LatestTXStartOffset, StoreID,
							Count + 1}};
		_ ->
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

%% @doc Get data roots for a given offset (>= BlockStartOffset, < BlockEndOffset) from local indices.
%% Return only entries corresponding to non-empty transactions.
%% Return the complete list of entries in the order they appear in the data root index,
%% which corresponds to sorted #tx records in the block.
%% Return {ok, {TXRoot, BlockSize, [{DataRoot, TXSize, TXStartOffset, TXPath}, ...]}}
%% or {error, Reason}.
get_for_offset(Offset) ->
	case Offset >= ar_data_sync:get_disk_pool_threshold() of
		true ->
			{error, not_found};
		false ->
			{BlockStart, BlockEnd, TXRoot} = ar_block_index:get_block_bounds(Offset),
			true = Offset >= BlockStart andalso Offset < BlockEnd,
			StoreID = ?DEFAULT_MODULE,
			case get_keys(BlockStart, StoreID) of
				not_found ->
					{error, not_found};
				{ok, Bin} ->
					{TXRoot2, BlockSize, DataRootKeys} = binary_to_term(Bin),
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
							DataRootKeys
						))}}
			end
	end.

%% @doc Return true if the data roots for the given block range are synced, false otherwise.
%% Assert the given BlockEnd and TXRoot match the stored values.
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
			true = TXRoot2 == TXRoot,`
			true = BlockSize == BlockEnd - BlockStart,
			true
	end.

%% @doc Returns true if the given data root key is in the data root index.
is_synced(DataRootKey, StoreID) ->
	case get_tx(DataRootKey, StoreID) of
		{ok, _} ->
			true;
		_ ->
			false
	end.

read_entries(_DataRoot, _TXSize, _BlockStart, 0, _StoreID, Acc) ->
	Acc;
read_entries(DataRoot, TXSize, BlockStart, Cursor, StoreID, Acc) ->
	Key = key_v2(DataRoot, TXSize, Cursor - 1),
	case get_prev_tx(Key, StoreID) of
		{ok, {TXStart, TXPath}} when TXStart >= BlockStart ->
			[{DataRoot, TXSize, TXStart, TXPath}
				| read_entries(DataRoot, TXSize, BlockStart, TXStart, StoreID, Acc)];
		{ok, _, _} ->
			Acc;
		none ->
			Acc
	end.

%%%===================================================================
%%% Repair.
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
	case ar_kv:get(ar_data_sync:migration_db(StoreID), <<"move_data_root_index">>) of
		{ok, <<"complete">>} ->
			complete;
		{ok, Cursor} ->
			{continue, Cursor, 1};
		not_found ->
			case ar_kv:get_next(old_db(StoreID), last) of
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
			case ar_kv:get_prev(old_db(StoreID), Cursor) of
				none ->
					complete;
				{ok, << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Value} ->
					M = binary_to_term(Value, [safe]),
					migrate_entries(DataRoot, TXSize, iterator(M), index_db(StoreID)),
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
