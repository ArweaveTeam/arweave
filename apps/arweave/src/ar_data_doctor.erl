-module(ar_data_doctor).

-export([main/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

main([]) ->
	help(),
	erlang:halt(1);
main(Args) ->
	Command = hd(Args),
	Success = case Command of
		"merge" ->
			merge(tl(Args));
		_ ->
			false
	end,
	case Success of
		true ->
			erlang:halt(0);
		_ ->
			help(),
			erlang:halt(1)
	end.

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

%%
%% XXX: the following functions are not used, because it was faster to merge the sync records
%% than re-register all chunks. However a future version of this script could add a data
%% repacking or validation step for which reading every chunk using the code below could help.
%%

register_chunk_storage(DataDir, StorageModule, StoreID) ->
	Config = #config{data_dir = DataDir, storage_modules = [StorageModule]},
	application:set_env(arweave, config, Config),
	ar_kv_sup:start_link(),
	ar_sync_record_sup:start_link(),

	ChunkFiles = lists:sort(ar_chunk_storage:list_files(DataDir, StoreID)),
	Count = register_chunk_files(StorageModule, StoreID, ChunkFiles, 0),
	ar:console("Registered ~B chunks~n", [Count]).

register_chunk_files(_StorageModule, _StoreID, [], Count) ->
	Count;
register_chunk_files(StorageModule, StoreID, [ChunkFile | ChunkFiles], Count) ->
	ar:console("ChunkFile: ~p~n", [ChunkFile]),
	[Filename] = filename:split(filename:basename(ChunkFile)),
    Offset = list_to_integer(Filename),
	PaddedOffset = ar_data_sync:get_chunk_padded_offset(Offset),
	{ok, File} = file:open(ChunkFile, [read, raw, binary]),
	Count2 = register_chunk_file(
		File, PaddedOffset, Offset + ?CHUNK_GROUP_SIZE, StorageModule, StoreID, Count),
	file:close(File),

	register_chunk_files(StorageModule, StoreID, ChunkFiles, Count2).

register_chunk_file(_File, Offset, MaxOffset, _StorageModule, _StoreID, Count) 
		when Offset >= MaxOffset ->
	Count;
register_chunk_file(File, Offset, MaxOffset, {_, _, Packing} = StorageModule, StoreID, Count) ->
	NumChunks = 400,
	IntervalStart = Offset,
	Start = Offset - (Offset - IntervalStart) rem ?DATA_CHUNK_SIZE,
	LeftBorder = ar_util:floor_int(Start, ?CHUNK_GROUP_SIZE),
	
	{LastOffset, Count2} =
		case ar_chunk_storage:read_chunk2(Offset, Start, LeftBorder, File, NumChunks) of
			[] ->
				ar:console("No chunks~n"),
				{Offset + (NumChunks * ?DATA_CHUNK_SIZE), Count};
			Chunks ->
				ar:console("~B Chunks~n", [length(Chunks)]),
				register_chunks(Chunks, undefined, Count, Packing, StoreID)
		end,

	register_chunk_file(File, LastOffset, MaxOffset, StorageModule, StoreID, Count2).

register_chunks([], LastOffset, Count, _Packing, _StoreID) ->
	{LastOffset, Count};
register_chunks([{Offset, _Chunk} | Chunks], _, Count, Packing, StoreID) ->
	% ar:console("add: ~B, ~B~n", [Offset - ?DATA_CHUNK_SIZE, Offset]),
	ar_sync_record:add(Offset, Offset - ?DATA_CHUNK_SIZE, Packing, ar_data_sync, StoreID),
	register_chunks(Chunks, Offset, Count+1, Packing, StoreID).
