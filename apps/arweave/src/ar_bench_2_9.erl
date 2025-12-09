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
	
	%% Phase 1: Entropy Preparation
	print_cache_fill_start(ChunkDir),
	MinDiskMs = calculate_min_disk_ms(RatedSpeedMB),
	{AllEntropyResults, ValidEntropyResults} = collect_entropy_samples(
		TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddress, ChunkDir),
	
	print_entropy_results(AllEntropyResults, ValidEntropyResults, ChunkDir, Threads),
	
	%% Phase 2: Packing (only if disk I/O enabled)
	case ChunkDir of
		undefined ->
			ok;
		_ ->
			%% Sync and drop page cache so reads hit disk
			sync_and_drop_cache(),
			close_file_handles(),
			PackingResults = collect_packing_samples(
				TargetSamples, Threads, RandomXState, ChunkDir),
			print_packing_results(PackingResults, Threads)
	end,
	
	stop_read_load(ReadPids),
	close_file_handles(),
	ar:console("~n").

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

collect_entropy_samples(TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddress, ChunkDir) ->
	collect_entropy_samples_loop(
		0, TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddress, ChunkDir, [], []).

collect_entropy_samples_loop(_Iteration, TargetSamples, _MinDiskMs, _Threads, _RandomXState, 
		_RewardAddr, _ChunkDir, AllResults, ValidResults) 
		when length(ValidResults) >= TargetSamples ->
	ar:console("~n"),
	{lists:reverse(AllResults), lists:reverse(ValidResults)};
collect_entropy_samples_loop(Iteration, TargetSamples, MinDiskMs, Threads, RandomXState, 
		RewardAddr, ChunkDir, AllResults, ValidResults) ->
	Result = run_entropy_iteration(Iteration, Threads, RandomXState, RewardAddr, ChunkDir),
	NewValidResults = process_entropy_sample(Result, MinDiskMs, ChunkDir, ValidResults),
	collect_entropy_samples_loop(
		Iteration + 1, TargetSamples, MinDiskMs, Threads, RandomXState, RewardAddr, ChunkDir, 
		[Result | AllResults], NewValidResults).

process_entropy_sample({EntropyMs, DiskMs} = Result, MinDiskMs, ChunkDir, ValidResults) ->
	case ChunkDir of
		undefined ->
			%% CPU-only: all samples count
			print_entropy_cpu_sample(length(ValidResults) + 1, EntropyMs),
			[Result | ValidResults];
		_ ->
			case DiskMs >= MinDiskMs of
				true ->
					print_entropy_valid_sample(length(ValidResults) + 1, EntropyMs, DiskMs),
					[Result | ValidResults];
				false ->
					print_cached_sample(),
					ValidResults
			end
	end.

%%%===================================================================
%%% Phase 1: Entropy Preparation - Iteration Execution
%%%===================================================================

run_entropy_iteration(Iteration, _Threads, RandomXState, RewardAddr, ChunkDir) ->
	{EntropyMs, Entropies} = time_entropy_generation(Iteration, RandomXState, RewardAddr),
	DiskMs = time_entropy_disk_write(Iteration, ChunkDir, Entropies),
	{EntropyMs, DiskMs}.

time_entropy_generation(Iteration, RandomXState, RewardAddr) ->
	StartTime = erlang:monotonic_time(microsecond),
	Entropies = generate_all_footprints(Iteration, RandomXState, RewardAddr),
	EndTime = erlang:monotonic_time(microsecond),
	{(EndTime - StartTime) / 1000, Entropies}.

time_entropy_disk_write(_Iteration, undefined, _Entropies) ->
	0;
time_entropy_disk_write(Iteration, ChunkDir, Entropies) ->
	BaseOffset = Iteration * ?FOOTPRINTS_PER_ITERATION * ?DATA_CHUNK_SIZE,
	StartTime = erlang:monotonic_time(microsecond),
	write_all_entropies(ChunkDir, Entropies, BaseOffset),
	EndTime = erlang:monotonic_time(microsecond),
	(EndTime - StartTime) / 1000.

%%%===================================================================
%%% Phase 2: Packing - Sample Collection
%%%===================================================================

collect_packing_samples(TargetSamples, Threads, RandomXState, ChunkDir) ->
	ar:console("~n=== Phase 2: Packing Benchmark ===~n"),
	ar:console("~nRunning packing benchmark:"),
	%% Generate a new reward address for unpacking entropy
	UnpackRewardAddr = crypto:strong_rand_bytes(32),
	collect_packing_samples_loop(0, TargetSamples, Threads, RandomXState, UnpackRewardAddr, ChunkDir, []).

collect_packing_samples_loop(SampleNum, TargetSamples, _Threads, _RandomXState, _UnpackRewardAddr, _ChunkDir, Results) 
		when SampleNum >= TargetSamples ->
	ar:console("~n"),
	lists:reverse(Results);
collect_packing_samples_loop(SampleNum, TargetSamples, Threads, RandomXState, UnpackRewardAddr, ChunkDir, Results) ->
	Result = run_packing_iteration(SampleNum, Threads, RandomXState, UnpackRewardAddr, ChunkDir),
	print_packing_sample(SampleNum + 1, Result, Threads),
	collect_packing_samples_loop(SampleNum + 1, TargetSamples, Threads, RandomXState, UnpackRewardAddr, ChunkDir, [Result | Results]).

run_packing_iteration(Iteration, _Threads, RandomXState, UnpackRewardAddr, ChunkDir) ->
	BaseOffset = Iteration * ?FOOTPRINTS_PER_ITERATION * ?DATA_CHUNK_SIZE,
	
	%% Step 1: Generate unpack entropy (parallelized across threads)
	{UnpackEntropyMs, UnpackEntropies} = time_entropy_generation(Iteration, RandomXState, UnpackRewardAddr),
	
	%% Step 2-5: Walk through entropy using map_entropies (same pattern as phase 1)
	{DecipherMs, ReadMs, EncipherMs, WriteMs} = pack_all_chunks(ChunkDir, UnpackEntropies, BaseOffset),
	
	{UnpackEntropyMs, DecipherMs, ReadMs, EncipherMs, WriteMs}.

pack_all_chunks(_ChunkDir, [], _BaseOffset) ->
	{0, 0, 0, 0};
pack_all_chunks(ChunkDir, [Footprint | Rest], BaseOffset) ->
	Offsets = ar_entropy_gen:entropy_offsets(BaseOffset + ?DATA_CHUNK_SIZE, ?PARTITION_SIZE),
	{D1, R1, E1, W1} = ar_entropy_gen:map_entropies(
		Footprint, Offsets, 0, [], <<>>,
		fun pack_chunk_callback/5, [ChunkDir], {0, 0, 0, 0}),
	{D2, R2, E2, W2} = pack_all_chunks(ChunkDir, Rest, BaseOffset + ?DATA_CHUNK_SIZE),
	{D1 + D2, R1 + R2, E1 + E2, W1 + W2}.

pack_chunk_callback(UnpackEntropy, BucketEndOffset, _RewardAddr, ChunkDir, {DAcc, RAcc, EAcc, WAcc}) ->
	{DecipherMs, ReadMs, EncipherMs, WriteMs} = pack_single_chunk(ChunkDir, BucketEndOffset, UnpackEntropy),
	{DAcc + DecipherMs, RAcc + ReadMs, EAcc + EncipherMs, WAcc + WriteMs}.

pack_single_chunk(ChunkDir, PaddedEndOffset, UnpackEntropy) ->
	%% Step 2: Generate random packed chunk and decipher it
	RandomPackedChunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	DecipherStart = erlang:monotonic_time(microsecond),
	UnpackedChunk = ar_packing_server:exor_replica_2_9_chunk(RandomPackedChunk, UnpackEntropy),
	DecipherEnd = erlang:monotonic_time(microsecond),
	DecipherMs = (DecipherEnd - DecipherStart) / 1000,
	
	%% Step 3: Read pack entropy from disk
	ChunkFileStart = ar_chunk_storage:get_chunk_file_start(PaddedEndOffset),
	{Position, _ChunkOffset} = ar_chunk_storage:get_position_and_relative_chunk_offset(
		ChunkFileStart, PaddedEndOffset),
	Filepath = filename:join(ChunkDir, integer_to_list(ChunkFileStart)),
	FH = get_file_handle(Filepath),
	
	ReadStart = erlang:monotonic_time(microsecond),
	{ok, EntropyWithHeader} = file:pread(FH, Position, ?OFFSET_BIT_SIZE div 8 + ?DATA_CHUNK_SIZE),
	<< _StoredChunkOffset:?OFFSET_BIT_SIZE, PackEntropy/binary >> = EntropyWithHeader,
	ReadEnd = erlang:monotonic_time(microsecond),
	ReadMs = (ReadEnd - ReadStart) / 1000,
	
	%% Step 4: Encipher the unpacked chunk with pack entropy
	EncipherStart = erlang:monotonic_time(microsecond),
	PackedChunk = ar_packing_server:exor_replica_2_9_chunk(UnpackedChunk, PackEntropy),
	EncipherEnd = erlang:monotonic_time(microsecond),
	EncipherMs = (EncipherEnd - EncipherStart) / 1000,
	
	%% Step 5: Write the packed chunk
	WriteStart = erlang:monotonic_time(microsecond),
	ok = file:pwrite(FH, Position + (?OFFSET_BIT_SIZE div 8), PackedChunk),
	WriteEnd = erlang:monotonic_time(microsecond),
	WriteMs = (WriteEnd - WriteStart) / 1000,
	
	{DecipherMs, ReadMs, EncipherMs, WriteMs}.

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
			{ok, FH} = file:open(Filepath, [read, write, raw, binary]),
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

sync_and_drop_cache() ->
	ar:console("~nSyncing and dropping page cache", []),
	lists:foreach(
		fun({write_handle, _} = Key) ->
			FH = erlang:get(Key),
			%% Sync to disk
			file:sync(FH),
			%% Get file size for advise
			case file:position(FH, eof) of
				{ok, Size} ->
					%% Tell kernel we don't need these pages cached
					file:advise(FH, 0, Size, dont_need);
				_ ->
					ok
			end,
			ar:console(".", []);
		   (_) ->
			ok
		end,
		erlang:get_keys()),
	ar:console("~n", []).

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
	ar:console("~n=== Phase 1: Entropy Preparation ===~n"),
	ar:console("~nFilling write cache", []).

print_entropy_cpu_sample(SampleNum, EntropyMs) ->
	EntropyRate = calculate_mib_per_iteration() / (EntropyMs / 1000),
	ar:console("~nSample ~p: Entropy: ~p MiB/s", [SampleNum, round(EntropyRate)]).

print_entropy_valid_sample(SampleNum, EntropyMs, DiskMs) ->
	case SampleNum of
		1 -> ar:console("~n~nRunning entropy benchmark:");
		_ -> ok
	end,
	MiBPerIteration = calculate_mib_per_iteration(),
	EntropyRate = MiBPerIteration / (EntropyMs / 1000),
	DiskRate = MiBPerIteration / (DiskMs / 1000),
	ar:console("~nSample ~p: Entropy: ~p MiB/s, Write: ~p MiB/s", [SampleNum, round(EntropyRate), round(DiskRate)]).

print_cached_sample() ->
	ar:console(".", []).

print_entropy_results(AllResults, ValidResults, ChunkDir, Threads) ->
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
	
	ar:console("~n--- Entropy Preparation Results ---~n~n"),
	ar:console("Data per iteration:   ~p MiB (~p chunks)~n", [MiBPerIteration, MiBPerIteration * 4]),
	ar:console("Total iterations:     ~p (~p excluded, ~p samples)~n", [TotalIterations, CachedCount, ValidCount]),
	ar:console("~n"),
	ar:console("Entropy generation:   ~.2f MiB/s (~p threads)~n", [EntropyRate, Threads]),
	
	case ChunkDir of
		undefined ->
			ar:console("~nNo disk I/O measured.~n"),
			print_preparation_extrapolation(EntropyRate);
		_ ->
			DiskRate = MiBPerIteration / (AvgDiskMs / 1000),
			ar:console("Disk write:           ~.2f MiB/s~n", [DiskRate]),
			{EffectiveRate, Bottleneck} = case EntropyRate < DiskRate of
				true -> {EntropyRate, "CPU (entropy generation)"};
				false -> {DiskRate, "Disk Write"}
			end,
			ar:console("~nBottleneck:           ~s~n", [Bottleneck]),
			ar:console("Effective rate:       ~.2f MiB/s~n", [EffectiveRate]),
			print_preparation_extrapolation(EffectiveRate)
	end.

print_preparation_extrapolation(EffectiveRate) ->
	PartitionSizeTB = ?PARTITION_SIZE / ?TB,
	TotalSeconds = ?PARTITION_SIZE / (EffectiveRate * ?MiB),
	ar:console("~nEstimated preparation time for ~.1f TB partition: ~s~n", 
		[PartitionSizeTB, format_duration(TotalSeconds)]).

print_packing_sample(SampleNum, {UnpackEntropyMs, DecipherMs, ReadMs, EncipherMs, WriteMs}, _Threads) ->
	MiBPerIteration = calculate_mib_per_iteration(),
	UnpackEntropyRate = MiBPerIteration / (UnpackEntropyMs / 1000),
	DecipherRate = MiBPerIteration / (DecipherMs / 1000),
	ReadRate = MiBPerIteration / (ReadMs / 1000),
	EncipherRate = MiBPerIteration / (EncipherMs / 1000),
	WriteRate = MiBPerIteration / (WriteMs / 1000),
	ar:console("~nSample ~p: Entropy: ~p MiB/s, Decipher: ~p MiB/s, Read: ~p MiB/s, Encipher: ~p MiB/s, Write: ~p MiB/s", 
		[SampleNum, round(UnpackEntropyRate), round(DecipherRate), round(ReadRate), 
		 round(EncipherRate), round(WriteRate)]).

print_packing_results(Results, Threads) ->
	MiBPerIteration = calculate_mib_per_iteration(),
	SampleCount = length(Results),
	
	{UnpackEntropyTimes, DecipherTimes, ReadTimes, EncipherTimes, WriteTimes} = lists:foldl(
		fun({UE, D, R, E, W}, {UEAcc, DAcc, RAcc, EAcc, WAcc}) -> 
			{UEAcc + UE, DAcc + D, RAcc + R, EAcc + E, WAcc + W} 
		end,
		{0, 0, 0, 0, 0},
		Results),
	
	AvgUnpackEntropyMs = UnpackEntropyTimes / SampleCount,
	AvgDecipherMs = DecipherTimes / SampleCount,
	AvgReadMs = ReadTimes / SampleCount,
	AvgEncipherMs = EncipherTimes / SampleCount,
	AvgWriteMs = WriteTimes / SampleCount,
	
	UnpackEntropyRate = MiBPerIteration / (AvgUnpackEntropyMs / 1000),
	DecipherRate = MiBPerIteration / (AvgDecipherMs / 1000),
	ReadRate = MiBPerIteration / (AvgReadMs / 1000),
	EncipherRate = MiBPerIteration / (AvgEncipherMs / 1000),
	WriteRate = MiBPerIteration / (AvgWriteMs / 1000),
	
	%% Identify bottleneck (slowest operation = lowest rate)
	{EffectiveRate, Bottleneck} = find_bottleneck([
		{UnpackEntropyRate, io_lib:format("CPU (unpack entropy, ~p threads)", [Threads])},
		{DecipherRate, "CPU (decipher)"},
		{ReadRate, "Disk read"},
		{EncipherRate, "CPU (encipher)"},
		{WriteRate, "Disk write"}
	]),
	
	ar:console("~n--- Packing Results ---~n~n"),
	ar:console("Samples:              ~p~n", [SampleCount]),
	ar:console("~n"),
	ar:console("Unpack entropy:       ~.2f MiB/s (~p threads)~n", [UnpackEntropyRate, Threads]),
	ar:console("Decipher:             ~.2f MiB/s~n", [DecipherRate]),
	ar:console("Read:                 ~.2f MiB/s~n", [ReadRate]),
	ar:console("Encipher:             ~.2f MiB/s~n", [EncipherRate]),
	ar:console("Write:                ~.2f MiB/s~n", [WriteRate]),
	ar:console("~nBottleneck:           ~s~n", [Bottleneck]),
	ar:console("Effective rate:       ~.2f MiB/s~n", [EffectiveRate]),
	print_packing_extrapolation(EffectiveRate).

find_bottleneck([{Rate, Name} | Rest]) ->
	find_bottleneck(Rest, Rate, Name).

find_bottleneck([], MinRate, MinName) ->
	{MinRate, MinName};
find_bottleneck([{Rate, Name} | Rest], MinRate, MinName) ->
	case Rate < MinRate of
		true -> find_bottleneck(Rest, Rate, Name);
		false -> find_bottleneck(Rest, MinRate, MinName)
	end.

print_packing_extrapolation(EffectiveRate) ->
	PartitionSizeTB = ?PARTITION_SIZE / ?TB,
	TotalSeconds = ?PARTITION_SIZE / (EffectiveRate * ?MiB),
	ar:console("Estimated packing time for ~.1f TB partition: ~s~n", 
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
