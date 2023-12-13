-module(ar_doctor_merge).

-export([main/1, help/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

main(Args) ->
	merge(Args).

help() ->
	ar:console("data-doctor merge data_dir storage_module src_directories~n").

merge(Args) when length(Args) < 3 ->
	false;
merge(Args) ->
	[DataDir, StorageModuleConfig | SrcDirs ] = Args,

	StorageModule = ar_config:parse_storage_module(StorageModuleConfig),
	StoreID = ar_storage_module:id(StorageModule),

	ok = merge(DataDir, StorageModule, StoreID, SrcDirs),
	true.

merge(_DataDir, _StorageModule, _StoreID, []) ->
	ok;
merge(DataDir, StorageModule, StoreID, [SrcDir | SrcDirs]) ->

	DstDir = filename:join([DataDir, "storage_modules", StoreID]),
	ar:console("~n~nMerge data from ~p into ~p~n~n", [SrcDir, DstDir]),

	move_chunk_storage(SrcDir, DstDir),

	copy_db("ar_data_sync_db", SrcDir, DstDir),
	copy_db("ar_data_sync_chunk_db", SrcDir, DstDir),
	copy_db("ar_data_sync_disk_pool_chunks_index_db", SrcDir, DstDir),
	copy_db("ar_data_sync_data_root_index_db", SrcDir, DstDir),
	copy_sync_records(SrcDir, DstDir),

	merge(DataDir, StorageModule, StoreID, SrcDirs).

move_chunk_storage(SrcDir, DstDir) ->
	MkDir = io_lib:format("mkdir -p ~s/chunk_storage ~s/rocksdb~n", [DstDir, DstDir]),
	Mv = io_lib:format("mv ~s/chunk_storage/* ~s/chunk_storage~n", [SrcDir, DstDir]),
	ar:console(MkDir),
	os:cmd(MkDir),
	ar:console(Mv),
	os:cmd(Mv).

% Function to copy all key/value pairs from one DB to another
copy_db(DB, SrcDir, DstDir) ->
	ar:console("~nCopying DB ~p~n", [DB]),
	SrcPath = filename:join([SrcDir, "rocksdb", DB]),
	DstPath = filename:join([DstDir, "rocksdb", DB]),
    % List all column families in the source database
    {ok, ColumnFamilies} = rocksdb:list_column_families(SrcPath, [{create_if_missing, false}]),

	CFDescriptors = lists:foldl(
		fun(CF, Acc) ->
			[{CF, []} | Acc]
		end,
		[],
		ColumnFamilies
	),
	
    % Open Source Database with all column families
    {ok, SrcDB, SrcCFs} = rocksdb:open(SrcPath, [{create_if_missing, false}], CFDescriptors),

    % Open Destination Database with all column families, creating them if necessary
    {ok, DstDB, DstCFs} = rocksdb:open(DstPath,
		[{create_if_missing, true}, {create_missing_column_families, true}], CFDescriptors),

    % Iterate and copy for each column family
    lists:zipwith(
		fun({SrcCF, DstCF}, ColumnFamily) -> 
			ar:console("Copying family ~p~n", [ColumnFamily]),
			copy_column_family(SrcDB, DstDB, SrcCF, DstCF) 
		end,
		lists:zip(SrcCFs, DstCFs), ColumnFamilies),

    % Close databases
    rocksdb:close(SrcDB),
    rocksdb:close(DstDB).

% Function to copy a specific column family
copy_column_family(SrcDB, DstDB, SrcCF, DstCF) ->
    % Create an Iterator for this column family in Source Database
    {ok, Itr} = rocksdb:iterator(SrcDB, SrcCF, []),
	copy_from_iterator(Itr, rocksdb:iterator_move(Itr, first), DstDB, DstCF),
    rocksdb:iterator_close(Itr).

% Helper function to copy key/value pairs from iterator to destination DB
copy_from_iterator(Itr, Res, DstDB, DstCF) ->
	case Res of
		{ok, Key, Value} ->
            ok = rocksdb:put(DstDB, DstCF, Key, Value, []),
            copy_from_iterator(Itr, rocksdb:iterator_move(Itr, next), DstDB, DstCF);
        {error, invalid_iterator} ->
            % End of iteration
            ok
    end.

copy_sync_records(SrcDir, DstDir) ->
	ar:console("Copying sync records~n", []),
	SrcPath = filename:join([SrcDir, "rocksdb", "ar_sync_record_db"]),
	DstPath = filename:join([DstDir, "rocksdb", "ar_sync_record_db"]),
	{ok, SrcDB} = rocksdb:open(SrcPath, [{create_if_missing, false}]),
	{ok, DstDB} = rocksdb:open(DstPath, [{create_if_missing, true}]),
	SrcSyncRecords = get_sync_records(SrcDB),
	DstSyncRecords = get_sync_records(DstDB),
	Union = merge_sync_records(SrcSyncRecords, DstSyncRecords),
	put_sync_records(DstDB, Union),
	rocksdb:close(SrcDB),
	rocksdb:close(DstDB).

get_sync_records(DB) ->
	Record = rocksdb:get(DB, <<"sync_records">>, []),
	case Record of
		{ok, Bin} ->
			binary_to_term(Bin);
		_ ->
			{#{}, #{}}
	end.

put_sync_records(DB, Intervals) ->
	rocksdb:put(DB, <<"sync_records">>, term_to_binary(Intervals), []).

merge_sync_records(
		{SrcSyncRecordByID, SrcSyncRecordByIDType}, {DstSyncRecordByID, DstSyncRecordByIDType}) ->
	UnionSyncRecordByID = maps:merge_with(
		fun(_Key, Src, Dst) -> 
			ar_intervals:union(Src, Dst)
		end,
		SrcSyncRecordByID, DstSyncRecordByID),
	UnionRecordByIDType = maps:merge_with(
		fun(_Key, Src, Dst) -> 
			ar_intervals:union(Src, Dst)
		end,
		SrcSyncRecordByIDType, DstSyncRecordByIDType),
	{UnionSyncRecordByID, UnionRecordByIDType}.