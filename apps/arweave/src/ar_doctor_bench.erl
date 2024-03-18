-module(ar_doctor_bench).

-export([main/1, help/0]).

-include_lib("kernel/include/file.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-define(NUM_ITERATIONS, 5).
-define(NUM_FILES, 15).
-define(OUTPUT_FILENAME, "<storage_module>.benchmark.csv").
-define(FILE_FORMAT, "timestamp,bytes_read,elapsed_time_ms,throughput_bps").

main(Args) ->
	bench_read(Args).

help() ->
	ar:console("data-doctor bench <duration> <data_dir> <storage_module> [<storage_module> ...]~n"),
	ar:console("  duration: How long, in seconds, to run the benchmark for.~n"), 
	ar:console("  data_dir: Full path to your data_dir.~n"), 
	ar:console("  storage_module: List of storage modules in same format used for Arweave ~n"),
	ar:console("                  configuration (e.g. 0,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI).~n"), 
	ar:console("                  It's recommended that you specify all configured storage_modules ~n"),
	ar:console("                  in order to benchmark the overall system performance including  ~n"),
	ar:console("                  any data busses that are shared across disks.~n"), 
	ar:console("~n"), 
	ar:console("Example:~n"), 
	ar:console("data-doctor bench 60 /mnt/arweave-data 0,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI \\~n"),
	ar:console("    1,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI \\~n"),
	ar:console("    2,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI \\~n"),
	ar:console("    3,En2eqsVJARnTVOSh723PBXAKGmKgrGSjQ2YIGwE_ZRI~n"),
	ar:console("~n"), 
	ar:console("Note: During the run data will be logged to ~p in the format:~n", [?OUTPUT_FILENAME]),
	ar:console("      '~s'~n", [?FILE_FORMAT]).

bench_read(Args) when length(Args) < 3 ->
	false;
bench_read(Args) ->
	[DurationString, DataDir | StorageModuleConfigs] = Args,
	Duration = list_to_integer(DurationString),

	StorageModules = parse_storage_modules(StorageModuleConfigs, []),
	Config = #config{data_dir = DataDir, storage_modules = StorageModules},
	application:set_env(arweave, config, Config),

	ar_kv_sup:start_link(),
	ar_sync_record_sup:start_link(),
	ar_chunk_storage_sup:start_link(),

	ar:console("~n~nDisk read benchmark will run for ~B seconds.~n", [Duration]),
	ar:console("Data will be logged continuously to ~p in the format:~n", [?OUTPUT_FILENAME]),
	ar:console("'~s'~n~n", [?FILE_FORMAT]),

	StopTime = erlang:monotonic_time() + erlang:convert_time_unit(Duration, second, native),

	Results = ar_util:pmap(
		fun(StorageModule) ->
			read_storage_module(DataDir, StorageModule, StopTime)
		end,
		StorageModules
	),

	lists:foreach(
		fun({StoreID, SumChunks, SumElapsedTime}) ->
			ReadRate = (SumChunks * 1000 div 4) div SumElapsedTime,
			ar:console("~s read ~B chunks in ~B ms (~B MiB/s)~n", [StoreID, SumChunks, SumElapsedTime, ReadRate])
		end,
		Results),

	ar:console("~n"),
	
	true.

parse_storage_modules([], StorageModules) ->
	StorageModules;
parse_storage_modules([StorageModuleConfig | StorageModuleConfigs], StorageModules) ->
	StorageModule = ar_config:parse_storage_module(StorageModuleConfig),
	parse_storage_modules(StorageModuleConfigs, StorageModules ++ [StorageModule]).
	
read_storage_module(DataDir, StorageModule, StopTime) ->
	StoreID = ar_storage_module:id(StorageModule),
	{StartOffset, EndOffset} = ar_storage_module:get_range(StoreID),	

	OutputFileName = string:replace(?OUTPUT_FILENAME, "<storage_module>", StoreID),

	random_read(StoreID, StartOffset, EndOffset, StopTime, OutputFileName).

	% random_chunk_pread(DataDir, StoreID),
	% random_dev_pread(DataDir, StoreID),
	% dd_chunk_files_read(DataDir, StoreID),
	% dd_chunk_file_read(DataDir, StoreID),
	% dd_devs_read(DataDir, StoreID),
	% dd_dev_read(DataDir, StoreID),

random_read(StoreID, StartOffset, EndOffset, StopTime, OutputFileName) ->
	random_read(StoreID, StartOffset, EndOffset, StopTime, OutputFileName, 0, 0).
random_read(StoreID, StartOffset, EndOffset, StopTime, OutputFileName, SumChunks, SumElapsedTime) ->
	StartTime = erlang:monotonic_time(),
	case StartTime < StopTime of
		true ->
			Chunks = read(StoreID, StartOffset, EndOffset, ?RECALL_RANGE_SIZE, ?NUM_FILES),
			EndTime = erlang:monotonic_time(),
			ElapsedTime = erlang:convert_time_unit(EndTime - StartTime, native, millisecond),

			%% timestamp,bytes_read,elapsed_time_ms,throughput_bps
			Timestamp = os:system_time(second),
			BytesRead = Chunks * ?DATA_CHUNK_SIZE,
			Line = io_lib:format("~B,~B,~B,~B~n", [
				Timestamp, BytesRead, ElapsedTime, BytesRead * 1000 div ElapsedTime]),
			file:write_file(OutputFileName, Line, [append]),
			random_read(StoreID, StartOffset, EndOffset, StopTime, OutputFileName,
				SumChunks + Chunks, SumElapsedTime + ElapsedTime);
		false ->
			{StoreID, SumChunks, SumElapsedTime}
	end.
	
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
