-module(ar_bench_2_9).

-export([show_help/0, run_benchmark_from_cli/1, run_benchmark/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("kernel/include/file.hrl").

-define(TB, 1_000_000_000_000).
-define(FOOTPRINTS_PER_ITERATION, 1).
-define(ENTROPY_FOOTPRINT_SIZE, (?REPLICA_2_9_ENTROPY_SIZE * ?COMPOSITE_PACKING_SUB_CHUNK_COUNT)).

%%%===================================================================
%%% CLI and Entry Points
%%%===================================================================

run_benchmark_from_cli(Args) ->
	Config = parse_cli_args(Args),
	validate_config(Config),
	run_benchmark(Config).

parse_cli_args(Args) ->
	Threads = list_to_integer(get_flag_value(Args, "threads",
		integer_to_list(erlang:system_info(dirty_cpu_schedulers_online)))),
	Samples = list_to_integer(get_flag_value(Args, "samples", "20")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	RatedSpeedMB = list_to_integer(get_flag_value(Args, "rated_speed", "250")),
	ReadLoadThreads = list_to_integer(get_flag_value(Args, "read_load", "2")),
	ReadFileGB = list_to_integer(get_flag_value(Args, "read_file_gb", "4")),
	Dir = get_flag_value(Args, "dir", undefined),
	{Dir, Threads, Samples, LargePages, RatedSpeedMB, ReadLoadThreads, ReadFileGB}.

validate_config({Dir, _Threads, _Samples, _LargePages, _RatedSpeedMB, _ReadLoadThreads, _ReadFileGB}) ->
	case Dir of
		undefined ->
			io:format("~nNo directory specified - will benchmark entropy generation only.~n"),
			io:format("For disk I/O benchmark, specify: dir /path/to/storage~n~n");
		_ ->
			case filelib:ensure_dir(filename:join(Dir, "dummy")) of
				ok -> ok;
				{error, Reason} ->
					io:format("Error: Could not ensure directory ~p exists: ~p~n", [Dir, Reason]),
					erlang:halt(1)
			end
	end.

get_flag_value([], _, DefaultValue) ->
	DefaultValue;
get_flag_value([Flag, Value | _Tail], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
	Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
	get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark-2.9 [options]~n~n"),
	io:format("Options:~n"),
	io:format("  threads       Number of threads. Default: number of CPU cores.~n"),
	io:format("  samples       Number of samples to average. Default: 20.~n"),
	io:format("  large_pages   Use large pages for RandomX (0=off, 1=on). Default: 1.~n"),
	io:format("  rated_speed   Expected disk write speed in MB/s. Benchmark will exclude samples\n"),
	io:format("                that are too fast as they are likely cached. Default: 250.~n"),
	io:format("  read_load     Background read threads (simulates other disk activity). Default: 2.~n"),
	io:format("  read_file_gb  Size of read load file in GB (larger = less caching). Default: 4.~n"),
	io:format("  dir           Directory to write to (optional).~n~n"),
	io:format("Examples:~n"),
	io:format("  benchmark-2.9 threads 8 dir /mnt/storage1~n"),
	io:format("  benchmark-2.9 rated_speed 246 dir /tmp/bench~n~n"),
	io:format("For more information, see the Benchmarking section at docs.arweave.org~n~n"),
	init:stop(1).

%%%===================================================================
%%% Main Benchmark Orchestration
%%%===================================================================

run_benchmark({Dir, Threads, TargetSamples, LargePages, RatedSpeedMB, ReadLoadThreads, ReadFileGB}) ->
	configure_randomx(LargePages),
	
	print_header(Threads, TargetSamples, RatedSpeedMB, ReadLoadThreads, ReadFileGB, Dir),
	
	ar:console("~nInitializing...~n"),
	RandomXState = init_randomx_state(Threads),
	RewardAddress = crypto:strong_rand_bytes(32),
	
	ChunkDir = init_chunk_dir(Dir),
	ReadPids = start_read_load(ReadLoadThreads, ReadFileGB, Dir),
	
	print_cache_fill_start(ChunkDir),
	MinDiskMs = calculate_min_disk_ms(RatedSpeedMB),
	{AllResults, ValidResults} = collect_samples(
		TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddress, ChunkDir),
	
	stop_read_load(ReadPids),
	close_file_handles(),
	
	print_results(AllResults, ValidResults, ChunkDir).

configure_randomx(LargePages) ->
	case LargePages of
		1 -> arweave_config:set_env(#config{disable = [], enable = [randomx_large_pages]});
		0 -> arweave_config:set_env(#config{disable = [randomx_large_pages], enable = []})
	end.

calculate_mib_per_iteration() ->
	BytesPerIteration = ?FOOTPRINTS_PER_ITERATION * ?ENTROPY_FOOTPRINT_SIZE,
	BytesPerIteration div ?MiB.

calculate_min_disk_ms(RatedSpeedMB) ->
	%% Convert MB/s (decimal, marketing) to MiB/s (binary)
	%% 1 MiB = 1.048576 MB, so MiB/s = MB/s / 1.048576
	RatedSpeedMiB = RatedSpeedMB / 1.048576,
	%% Add 10% margin - writes up to 10% faster than rated still count as valid
	EffectiveSpeed = RatedSpeedMiB * 1.1,
	%% Calculate minimum expected time for a "real" disk write
	calculate_mib_per_iteration() / EffectiveSpeed * 1000.

init_chunk_dir(undefined) ->
	undefined;
init_chunk_dir(Dir) ->
	ChunkDir = filename:join(Dir, "benchmark_chunk_storage"),
	filelib:ensure_dir(filename:join(ChunkDir, "dummy")),
	clear_dir(ChunkDir),
	ChunkDir.

init_randomx_state(Threads) ->
	try
		ar_mine_randomx:init_fast(rxsquared, ?RANDOMX_PACKING_KEY, Threads)
	catch
		error:{badmatch, {error, Reason}} ->
			ar:console("~nError: Failed to initialize RandomX: ~p~n", [Reason]),
			ar:console("~nTry running with large_pages 0 if large pages are not supported.~n~n"),
			erlang:halt(1),
			undefined
	end.

collect_samples(TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddress, ChunkDir) ->
	collect_samples_loop(
		0, TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddress, ChunkDir, [], []).

collect_samples_loop(_Iteration, TargetSamples, _MinDiskMs, _Threads, _RandomXState, 
		_RewardAddr, _ChunkDir, AllResults, ValidResults) 
		when length(ValidResults) >= TargetSamples ->
	ar:console("~n"),
	{lists:reverse(AllResults), lists:reverse(ValidResults)};
collect_samples_loop(Iteration, TargetSamples, MinDiskMs, Threads, RandomXState, 
		RewardAddr, ChunkDir, AllResults, ValidResults) ->
	Result = run_iteration(Iteration, Threads, RandomXState, RewardAddr, ChunkDir),
	NewValidResults = process_sample(Result, MinDiskMs, ChunkDir, ValidResults),
	collect_samples_loop(
		Iteration + 1, TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddr, ChunkDir, 
		[Result | AllResults], NewValidResults).

process_sample({EntropyMs, DiskMs} = Result, MinDiskMs, ChunkDir, ValidResults) ->
	case ChunkDir of
		undefined ->
			%% CPU-only: all samples count
			print_cpu_sample(length(ValidResults) + 1, EntropyMs),
			[Result | ValidResults];
		_ ->
			case DiskMs >= MinDiskMs of
				true ->
					print_valid_sample(length(ValidResults) + 1, EntropyMs, DiskMs),
					[Result | ValidResults];
				false ->
					print_cached_sample(),
					ValidResults
			end
	end.

%%%===================================================================
%%% Iteration Execution
%%%===================================================================

run_iteration(Iteration, _Threads, RandomXState, RewardAddr, ChunkDir) ->
	{EntropyMs, Entropies} = time_entropy_generation(Iteration, RandomXState, RewardAddr),
	DiskMs = time_disk_write(Iteration, ChunkDir, Entropies),
	{EntropyMs, DiskMs}.

time_entropy_generation(Iteration, RandomXState, RewardAddr) ->
	StartTime = erlang:monotonic_time(microsecond),
	Entropies = generate_all_footprints(Iteration, RandomXState, RewardAddr),
	EndTime = erlang:monotonic_time(microsecond),
	{(EndTime - StartTime) / 1000, Entropies}.

time_disk_write(_Iteration, undefined, _Entropies) ->
	0;
time_disk_write(Iteration, ChunkDir, Entropies) ->
	BaseOffset = Iteration * ?FOOTPRINTS_PER_ITERATION * ?DATA_CHUNK_SIZE,
	StartTime = erlang:monotonic_time(microsecond),
	write_all_entropies(ChunkDir, Entropies, BaseOffset),
	EndTime = erlang:monotonic_time(microsecond),
	(EndTime - StartTime) / 1000.

%%%===================================================================
%%% Entropy Generation
%%%===================================================================

generate_all_footprints(Iteration, RandomXState, RewardAddr) ->
	FootprintIds = lists:seq(0, ?FOOTPRINTS_PER_ITERATION - 1),
	ar_util:pmap(
		fun(FootprintId) ->
			UniqueId = Iteration * ?FOOTPRINTS_PER_ITERATION + FootprintId,
			generate_footprint(RandomXState, RewardAddr, UniqueId)
		end,
		FootprintIds, infinity).

generate_footprint(RandomXState, RewardAddr, UniqueId) ->
	SubChunkIndices = lists:seq(0, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT - 1),
	ar_util:pmap(
		fun(SubChunkIndex) ->
			AbsoluteOffset = (UniqueId + 1) * ?DATA_CHUNK_SIZE,
			SubChunkOffset = SubChunkIndex * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
			Key = ar_replica_2_9:get_entropy_key(RewardAddr, AbsoluteOffset, SubChunkOffset),
			ar_mine_randomx:randomx_generate_replica_2_9_entropy(RandomXState, Key)
		end,
		SubChunkIndices, infinity).

%%%===================================================================
%%% Disk I/O - Chunk Storage
%%%===================================================================

write_all_entropies(_ChunkDir, [], _BaseOffset) ->
	ok;
write_all_entropies(ChunkDir, [Footprint | Rest], BaseOffset) ->
	Offsets = ar_entropy_gen:entropy_offsets(BaseOffset + ?DATA_CHUNK_SIZE, ?PARTITION_SIZE),
	ar_entropy_gen:map_entropies(
		Footprint, Offsets, 0, [], <<>>,
		fun write_chunk_callback/5, [ChunkDir], ok),
	write_all_entropies(ChunkDir, Rest, BaseOffset + ?DATA_CHUNK_SIZE).

write_chunk_callback(ChunkEntropy, BucketEndOffset, _RewardAddr, ChunkDir, ok) ->
	write_chunk(ChunkDir, BucketEndOffset, ChunkEntropy),
	ok.

write_chunk(ChunkDir, PaddedEndOffset, Chunk) ->
	ChunkFileStart = ar_chunk_storage:get_chunk_file_start(PaddedEndOffset),
	{Position, ChunkOffset} = ar_chunk_storage:get_position_and_relative_chunk_offset(
		ChunkFileStart, PaddedEndOffset),
	Filepath = filename:join(ChunkDir, integer_to_list(ChunkFileStart)),
	FH = get_file_handle(Filepath),
	ok = file:pwrite(FH, Position, [<< ChunkOffset:?OFFSET_BIT_SIZE >> | Chunk]).

get_file_handle(Filepath) ->
	case erlang:get({write_handle, Filepath}) of
		undefined ->
			{ok, FH} = file:open(Filepath, [read, write, raw]),
			erlang:put({write_handle, Filepath}, FH),
			FH;
		FH ->
			FH
	end.

close_file_handles() ->
	lists:foreach(
		fun({write_handle, _} = Key) ->
			file:close(erlang:get(Key)),
			erlang:erase(Key);
		   (_) ->
			ok
		end,
		erlang:get_keys()).

clear_dir(Dir) ->
	case file:list_dir(Dir) of
		{ok, Files} ->
			lists:foreach(fun(File) -> file:delete(filename:join(Dir, File)) end, Files);
		{error, enoent} ->
			ok
	end.

%%%===================================================================
%%% Disk I/O - Read Load Simulation
%%%===================================================================

start_read_load(0, _ReadFileGB, _Dir) ->
	[];
start_read_load(_N, _ReadFileGB, undefined) ->
	[];
start_read_load(NumThreads, ReadFileSizeGB, Dir) ->
	ReadFile = create_read_load_file(Dir, ReadFileSizeGB),
	spawn_read_load_threads(NumThreads, ReadFile).

create_read_load_file(Dir, SizeGB) ->
	ReadFile = filename:join(Dir, "benchmark_read_load.bin"),
	SizeMB = SizeGB * 1024,
	file:delete(ReadFile),
	ar:console("Creating read load file...~n"),
	{ok, FH} = file:open(ReadFile, [write, raw, binary]),
	lists:foreach(
		fun(_) -> file:write(FH, crypto:strong_rand_bytes(1024 * 1024)) end,
		lists:seq(1, SizeMB)),
	file:close(FH),
	ReadFile.

spawn_read_load_threads(NumThreads, ReadFile) ->
	[spawn_link(fun() -> read_load_loop(ReadFile) end) || _ <- lists:seq(1, NumThreads)].

read_load_loop(ReadFile) ->
	{ok, FH} = file:open(ReadFile, [read, raw, binary, {read_ahead, 0}]),
	{ok, FileInfo} = file:read_file_info(ReadFile),
	FileSize = FileInfo#file_info.size,
	read_load_loop(FH, FileSize).

read_load_loop(FH, FileSize) ->
	%% Random read of 4-64KB (typical RocksDB read sizes)
	ReadSize = 4096 + rand:uniform(60 * 1024),
	MaxOffset = max(0, FileSize - ReadSize),
	Offset = rand:uniform(MaxOffset + 1) - 1,
	file:pread(FH, Offset, ReadSize),
	read_load_loop(FH, FileSize).

stop_read_load(Pids) ->
	lists:foreach(fun(Pid) -> exit(Pid, kill) end, Pids).

%%%===================================================================
%%% Output - Progress and Results
%%%===================================================================

print_header(Threads, TargetSamples, RatedSpeedMB, ReadLoadThreads, ReadFileGB, Dir) ->
	ar:console("~n=== Replica 2.9 Preparation Benchmark ===~n"),
	ar:console("See docs.arweave.org for more information.~n~n"),
	ar:console("Configuration:~n"),
	ar:console("  Threads:            ~p~n", [Threads]),
	ar:console("  Samples:            ~p~n", [TargetSamples]),
	ar:console("  Data per iteration: ~p MiB~n", [calculate_mib_per_iteration()]),
	ar:console("  Rated disk speed:   ~p MB/s~n", [RatedSpeedMB]),
	ar:console("  Read load threads:  ~p~n", [ReadLoadThreads]),
	ar:console("  Read file size:     ~p GB~n", [ReadFileGB]),
	case Dir of
		undefined -> ar:console("  Directory:          (none - CPU benchmark only)~n");
		_ -> ar:console("  Directory:          ~p~n", [Dir])
	end.

print_cache_fill_start(undefined) ->
	ok;
print_cache_fill_start(_ChunkDir) ->
	ar:console("~nFilling write cache", []).

print_cpu_sample(SampleNum, EntropyMs) ->
	EntropyRate = calculate_mib_per_iteration() / (EntropyMs / 1000),
	ar:console("~nSample ~p: Entropy ~.1f MiB/s", [SampleNum, EntropyRate]).

print_valid_sample(SampleNum, EntropyMs, DiskMs) ->
	case SampleNum of
		1 -> ar:console("~n~nRunning benchmark:");
		_ -> ok
	end,
	MiBPerIteration = calculate_mib_per_iteration(),
	EntropyRate = MiBPerIteration / (EntropyMs / 1000),
	DiskRate = MiBPerIteration / (DiskMs / 1000),
	ar:console("~nSample ~p: Entropy ~.1f MiB/s, Disk ~.1f MiB/s", [SampleNum, EntropyRate, DiskRate]).

print_cached_sample() ->
	ar:console(".", []).

print_results(AllResults, ValidResults, ChunkDir) ->
	MiBPerIteration = calculate_mib_per_iteration(),
	{AllEntropyTimes, _} = lists:unzip(AllResults),
	{ValidEntropyTimes, ValidDiskTimes} = lists:unzip(ValidResults),
	
	TotalIterations = length(AllResults),
	ValidCount = length(ValidResults),
	CachedCount = TotalIterations - ValidCount,
	
	AvgEntropyMs = lists:sum(AllEntropyTimes) / TotalIterations,
	AvgDiskMs = safe_average(ValidDiskTimes),
	AvgValidEntropyMs = case ValidCount > 0 of
		true -> lists:sum(ValidEntropyTimes) / ValidCount;
		false -> AvgEntropyMs
	end,
	
	EntropyRate = MiBPerIteration / (AvgValidEntropyMs / 1000),
	
	ar:console("~n=== Results ===~n~n"),
	ar:console("Data per iteration:   ~p MiB (~p chunks)~n", [MiBPerIteration, MiBPerIteration * 4]),
	ar:console("Total iterations:     ~p (~p excluded, ~p samples)~n", [TotalIterations, CachedCount, ValidCount]),
	ar:console("~n"),
	ar:console("Entropy generation:   ~.2f MiB/s~n", [EntropyRate]),
	
	case ChunkDir of
		undefined ->
			ar:console("~nNo disk I/O measured.~n"),
			print_extrapolation(EntropyRate);
		_ ->
			DiskRate = MiBPerIteration / (AvgDiskMs / 1000),
			ar:console("Disk write:           ~.2f MiB/s~n", [DiskRate]),
			{EffectiveRate, Bottleneck} = case EntropyRate < DiskRate of
				true -> {EntropyRate, "CPU (entropy generation)"};
				false -> {DiskRate, "Disk I/O"}
			end,
			ar:console("~nBottleneck:           ~s~n", [Bottleneck]),
			ar:console("Effective rate:       ~.2f MiB/s~n", [EffectiveRate]),
			print_extrapolation(EffectiveRate)
	end,
	ar:console("~n").

print_extrapolation(EffectiveRate) ->
	PartitionSizeTB = ?PARTITION_SIZE / ?TB,
	TotalSeconds = ?PARTITION_SIZE / (EffectiveRate * ?MiB),
	ar:console("~nEstimated time to prepare ~.1f TB partition: ~s~n", 
		[PartitionSizeTB, format_duration(TotalSeconds)]).

%%%===================================================================
%%% Utilities
%%%===================================================================

safe_average([]) ->
	0;
safe_average(List) ->
	lists:sum(List) / length(List).

format_duration(Seconds) when Seconds < 60 ->
	io_lib:format("~.1f seconds", [Seconds]);
format_duration(Seconds) when Seconds < 3600 ->
	io_lib:format("~.1f minutes", [Seconds / 60]);
format_duration(Seconds) when Seconds < 86400 ->
	io_lib:format("~.1f hours", [Seconds / 3600]);
format_duration(Seconds) ->
	Days = Seconds / 86400,
	io_lib:format("~.1f days (~p hours)", [Days, trunc(Days * 24)]).
