-module(ar_doctor_bench).

-export([main/1, help/0]).

-include_lib("kernel/include/file.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-define(NUM_ITERATIONS, 5).
-define(NUM_FILES, 15).

main(Args) ->
	bench_read(Args).

help() ->
	ar:console("data-doctor bench data_dir storage_module [storage_module] [...]~n").

bench_read(Args) when length(Args) < 2 ->
	false;
bench_read(Args) ->
	[DataDir | StorageModuleConfigs] = Args,

	StorageModules = parse_storage_modules(StorageModuleConfigs, []),
	Config = #config{data_dir = DataDir, storage_modules = StorageModules},
	application:set_env(arweave, config, Config),

	ar_kv_sup:start_link(),
	ar_sync_record_sup:start_link(),
	ar_chunk_storage_sup:start_link(),

	ar:console("~n~nStarting disk read benchmark. It may take a few minutes to complete.~n"),

	Results = ar_util:pmap(
		fun(StorageModule) ->
			read_storage_module(DataDir, StorageModule)
		end,
		StorageModules
	),

	lists:foreach(
		fun({StoreID, SumChunks, SumElapsedTime}) ->
			ReadRate = (SumChunks * 1000 div 4) div SumElapsedTime,
			ar:console("~s read ~B chunks in ~B ms (~B MiB/s)~n", [StoreID, SumChunks, SumElapsedTime, ReadRate])
		end,
		Results),
	
	true.

parse_storage_modules([], StorageModules) ->
	StorageModules;
parse_storage_modules([StorageModuleConfig | StorageModuleConfigs], StorageModules) ->
	StorageModule = ar_config:parse_storage_module(StorageModuleConfig),
	parse_storage_modules(StorageModuleConfigs, StorageModules ++ [StorageModule]).
	
read_storage_module(DataDir, StorageModule) ->
	StoreID = ar_storage_module:id(StorageModule),
	{StartOffset, EndOffset} = ar_storage_module:get_range(StoreID),	

	random_read(StoreID, StartOffset, EndOffset).

	% random_chunk_pread(DataDir, StoreID),
	% random_dev_pread(DataDir, StoreID),
	% dd_chunk_files_read(DataDir, StoreID),
	% dd_chunk_file_read(DataDir, StoreID),
	% dd_devs_read(DataDir, StoreID),
	% dd_dev_read(DataDir, StoreID),

random_read(StoreID, StartOffset, EndOffset) ->
	random_read(StoreID, StartOffset, EndOffset, ?NUM_ITERATIONS, 0, 0).
random_read(StoreID, _StartOffset, _EndOffset, 0, SumChunks, SumElapsedTime) ->
	{StoreID, SumChunks, SumElapsedTime};
random_read(StoreID, StartOffset, EndOffset, Count, SumChunks, SumElapsedTime) ->
	StartTime = erlang:monotonic_time(),
	Chunks = read(StoreID, StartOffset, EndOffset, ?RECALL_RANGE_SIZE, ?NUM_FILES),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	random_read(StoreID, StartOffset, EndOffset, Count - 1, SumChunks + Chunks, SumElapsedTime + ElapsedTime).
	
read(StoreID, StartOffset, EndOffset, Size, NumReads) ->
	read(StoreID, StartOffset, EndOffset, Size, 0, NumReads).

read(_StoreID, _StartOffset, _EndOffset, _Size, NumChunks, 0) ->
	NumChunks;
read(StoreID, StartOffset, EndOffset, Size, NumChunks, NumReads) ->
	Offset = rand:uniform(EndOffset - Size - StartOffset + 1) + StartOffset,
	Chunks = ar_chunk_storage:get_range(Offset, Size, StoreID),
	read(StoreID, StartOffset, EndOffset, Size, NumChunks + length(Chunks), NumReads - 1).
	
%% XXX: the following functions are not used, but may be useful in the future to benchmark
%% different read strategies. They can be deleted when they are no longer useful.

random_chunk_pread(DataDir, StoreID) ->
	random_chunk_pread(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
random_chunk_pread(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*Random* chunk pread ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
random_chunk_pread(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Files = open_files(DataDir, StoreID),
	StartTime = erlang:monotonic_time(),
	Bytes = pread(Files, ?RECALL_RANGE_SIZE, 0),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	random_chunk_pread(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).

random_dev_pread(DataDir, StoreID) ->
	random_dev_pread(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
random_dev_pread(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*Random* device pread ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
random_dev_pread(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Filepath = hd(ar_chunk_storage:list_files(DataDir, StoreID)),
	Device = get_mounted_device(Filepath),
	{ok, File} = file:open(Device, [read, raw, binary]),
	Files = [{Device, File, ?PARTITION_SIZE} || _ <- lists:seq(1, ?NUM_FILES)],
	StartTime = erlang:monotonic_time(),
	Bytes = pread(Files, ?RECALL_RANGE_SIZE, 0),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	random_dev_pread(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).

dd_chunk_files_read(DataDir, StoreID) ->
	dd_chunk_files_read(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
dd_chunk_files_read(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*dd* multi chunk files read ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
dd_chunk_files_read(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Files = open_files(DataDir, StoreID),
	StartTime = erlang:monotonic_time(),
	Bytes = dd_files(Files, ?RECALL_RANGE_SIZE, 0),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	dd_chunk_files_read(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).

dd_chunk_file_read(DataDir, StoreID) ->
	dd_chunk_file_read(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
dd_chunk_file_read(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*dd* single chunk file read ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
dd_chunk_file_read(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Files = open_files(DataDir, StoreID),
	{Filepath, _File, FileSize} = hd(Files),
	StartTime = erlang:monotonic_time(),
	dd(Filepath, FileSize, ?RECALL_RANGE_SIZE, ?NUM_FILES),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	Bytes = ?RECALL_RANGE_SIZE * ?NUM_FILES,
	dd_chunk_file_read(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).

dd_dev_file_read(DataDir, StoreID) ->
	dd_dev_file_read(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
dd_dev_file_read(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*dd* multi dev file read ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
dd_dev_file_read(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Filepath = "/opt/prod/data/storage_modules/storage_module_19_cLGt682uYLJCl47QsRHfdTzMhSPTHPsUnUOzuvTm1HQ/dd.10GB",
	StartTime = erlang:monotonic_time(),
	dd(Filepath, 10*?GiB, ?RECALL_RANGE_SIZE, ?NUM_FILES),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	Bytes = ?RECALL_RANGE_SIZE * ?NUM_FILES,
	dd_dev_file_read(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).

dd_devs_read(DataDir, StoreID) ->
	dd_devs_read(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
dd_devs_read(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*dd* multi devs read ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
dd_devs_read(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Filepath = hd(ar_chunk_storage:list_files(DataDir, StoreID)),
	Device = get_mounted_device(Filepath),
	Devices = [{Device, not_set, ?PARTITION_SIZE} || _ <- lists:seq(1, ?NUM_FILES)],
	StartTime = erlang:monotonic_time(),
	Bytes = dd_files(Devices, ?RECALL_RANGE_SIZE, 0),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	dd_devs_read(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).

dd_dev_read(DataDir, StoreID) ->
	dd_dev_read(DataDir, StoreID, ?NUM_ITERATIONS, 0, 0).
dd_dev_read(_DataDir, _StoreID, 0, SumBytes, SumElapsedTime) ->
	ReadRate = (SumBytes * 1000 div ?MiB) div SumElapsedTime,
	ar:console("*dd* single dev read ~B MiB in ~B ms (~B MiB/s)~n", [SumBytes div ?MiB, SumElapsedTime, ReadRate]);
dd_dev_read(DataDir, StoreID, Count, SumBytes, SumElapsedTime) ->
	Filepath = hd(ar_chunk_storage:list_files(DataDir, StoreID)),
	Device = get_mounted_device(Filepath),
	StartTime = erlang:monotonic_time(),
	dd(Device, ?PARTITION_SIZE, ?RECALL_RANGE_SIZE, ?NUM_FILES),
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),
	Bytes = ?RECALL_RANGE_SIZE * ?NUM_FILES,
	dd_dev_read(DataDir, StoreID, Count - 1, SumBytes + Bytes, SumElapsedTime + ElapsedTime).
	
get_mounted_device(FilePath) ->
	Cmd = "df " ++ FilePath ++ " | awk 'NR==2 {print $1}'",
	Device = os:cmd(Cmd),
	string:trim(Device, both, "\n").
	
open_files(DataDir, StoreID) ->
	AllFilepaths = ar_chunk_storage:list_files(DataDir, StoreID),
	Filepaths = lists:sublist(ar_util:shuffle_list(AllFilepaths), ?NUM_FILES),
	lists:foldl(
		fun(Filepath, Acc) ->
			{ok, FileInfo} = file:read_file_info(Filepath),
			{ok, File} = file:open(Filepath, [read, raw, binary]),
			[{Filepath, File, FileInfo#file_info.size} | Acc]
		end,
		[], Filepaths).

pread([], _Size, NumBytes) ->
	NumBytes;
pread([{Filepath, File, FileSize} | Files], Size, NumBytes) ->
	Position = max(0, rand:uniform(FileSize - Size)),
 	% ar:console("pread: ~p ~B ~B ~B ~B~n", [Filepath, FileSize, Position, Size, NumBytes]),
	{ok, Bin} = file:pread(File, Position, Size),
	pread(Files, Size, NumBytes + byte_size(Bin)).

dd_files([], _Size, NumBytes) ->
	NumBytes;
dd_files([{Filepath, _File, FileSize} | Files], Size, NumBytes) ->
	dd(Filepath, FileSize, Size, 1),
	dd_files(Files, Size, NumBytes + Size).

dd(Filepath, FileSize, Size, Count) ->
	BlockSize = ?RECALL_RANGE_SIZE,
	Bytes = Size * Count,
	Blocks = Bytes div BlockSize,
	MaxOffset = max(1, FileSize - Bytes),
	Position = rand:uniform(MaxOffset) div BlockSize,
	Command = io_lib:format("dd iflag=direct if=~s skip=~B of=/dev/null bs=~B count=~B", [Filepath, Position, BlockSize, Blocks]),
	% ar:console("~s~n", [Command]),
	os:cmd(Command).

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
