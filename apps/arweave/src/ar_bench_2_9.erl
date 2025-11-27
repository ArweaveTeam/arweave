-module(ar_bench_2_9).

-export([show_help/0, run_benchmark_from_cli/1, run_benchmark/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(TB, 1_000_000_000_000).
-define(SLICES_PER_ENTROPY, (?REPLICA_2_9_ENTROPY_SIZE div ?COMPOSITE_PACKING_SUB_CHUNK_SIZE)).
-define(MIN_MIB_PER_ITERATION, (?COMPOSITE_PACKING_SUB_CHUNK_COUNT * ?REPLICA_2_9_ENTROPY_SIZE div ?MiB)).

run_benchmark_from_cli(Args) ->
	Threads = list_to_integer(get_flag_value(Args, "threads",
		integer_to_list(erlang:system_info(dirty_cpu_schedulers_online)))),
	DataMiB = list_to_integer(get_flag_value(Args, "mib", "5120")),
	Iterations = list_to_integer(get_flag_value(Args, "iterations", "10")),
	PartitionSizeTB = list_to_float(get_flag_value(Args, "partition_tb", "3.6")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	Dirs = collect_dirs(Args),

	lists:foreach(fun(Dir) ->
		case filelib:ensure_dir(filename:join(Dir, "dummy")) of
			ok -> ok;
			{error, Reason} ->
				io:format("Error: Could not ensure directory ~p exists: ~p~n", [Dir, Reason]),
				erlang:halt(1)
		end
	end, Dirs),

	MiBPerIteration = DataMiB div Iterations,
	case MiBPerIteration < ?MIN_MIB_PER_ITERATION of
		true ->
			io:format("~nError: mib/iterations must be at least ~p MiB.~n",
				[?MIN_MIB_PER_ITERATION]),
			io:format("With mib=~p and iterations=~p, you get ~p MiB per iteration.~n~n",
				[DataMiB, Iterations, MiBPerIteration]),
			erlang:halt(1);
		false ->
			ok
	end,

	case Dirs of
		[] ->
			io:format("~nNo directory specified - will benchmark entropy generation only.~n"),
			io:format("For disk I/O benchmark, specify: dir /path/to/storage~n~n");
		_ ->
			ok
	end,

	run_benchmark({Dirs, Threads, DataMiB, Iterations, PartitionSizeTB, LargePages}).

collect_dirs([]) ->
	[];
collect_dirs(["dir", Dir | Tail]) ->
	[Dir | collect_dirs(Tail)];
collect_dirs([_ | Tail]) ->
	collect_dirs(Tail).

get_flag_value([], _, DefaultValue) ->
	DefaultValue;
get_flag_value([Flag, Value | _Tail], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
	Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
	get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark-2.9 [options] [dir path1 ...]~n~n"),
	io:format("Options:~n"),
	io:format("  threads       Number of threads. Default: number of CPU cores.~n"),
	io:format("  mib           Total data and disk space in MiB. Default: 2560.~n"),
	io:format("  iterations    Iterations for averaging. Default: 10.~n"),
	io:format("  partition_tb  Partition size in TB for extrapolation. Default: 3.6.~n"),
	io:format("  large_pages   Use large pages for RandomX (0=off, 1=on). Default: 1.~n"),
	io:format("  dir           Directory to write to.~n~n"),
	io:format("Constraint: mib/iterations >= ~p MiB~n~n", [?MIN_MIB_PER_ITERATION]),
	io:format("Examples:~n"),
	io:format("  benchmark-2.9 threads 8 dir /mnt/storage1~n"),
	io:format("  benchmark-2.9 large_pages 0 mib 5120 iterations 20 dir /tmp/bench~n~n"),
	init:stop(1).

run_benchmark({Dirs, Threads, DataMiB, Iterations, PartitionSizeTB, LargePages}) ->
	case LargePages of
		1 ->
			arweave_config:set_env(#config{disable = [], enable = [randomx_large_pages]});
		0 ->
			arweave_config:set_env(#config{disable = [randomx_large_pages], enable = []})
	end,

	MiBPerIteration = DataMiB div Iterations,

	ar:console("~n=== Replica 2.9 Entropy Benchmark ===~n~n"),
	ar:console("Configuration:~n"),
	ar:console("  Threads:            ~p~n", [Threads]),
	ar:console("  Total data:         ~p MiB~n", [DataMiB]),
	ar:console("  Iterations:         ~p~n", [Iterations]),
	ar:console("  Data per iteration: ~p MiB~n", [MiBPerIteration]),
	ar:console("  Partition size:     ~.1f TB~n", [PartitionSizeTB]),
	ar:console("  Large pages:        ~p~n", [LargePages]),

	case Dirs of
		[] ->
			ar:console("  Disk space:         0 MiB (no disk I/O)~n");
		_ ->
			ar:console("  Disk space:         ~p MiB~n", [DataMiB]),
			ar:console("  Directory:          ~p~n", [hd(Dirs)])
	end,

	ar:console("~nInitializing RandomX state...~n"),
	RandomXState = init_randomx_state(Threads),
	RewardAddress = crypto:strong_rand_bytes(32),

	EntropySetsPerIteration = MiBPerIteration * ?MiB div
		(?COMPOSITE_PACKING_SUB_CHUNK_COUNT * ?REPLICA_2_9_ENTROPY_SIZE),
	ChunksPerIteration = EntropySetsPerIteration * ?SLICES_PER_ENTROPY,
	BytesPerIteration = ChunksPerIteration * ?DATA_CHUNK_SIZE,

	ar:console("~nRunning ~p iterations, ~p entropy sets per iteration...~n",
		[Iterations, EntropySetsPerIteration]),

	FilePath = case Dirs of
		[] -> undefined;
		[Dir | _] -> filename:join(Dir, "benchmark_chunks.bin")
	end,

	Results = run_iterations(0, Iterations, Threads, RandomXState, RewardAddress,
		FilePath, BytesPerIteration, EntropySetsPerIteration, []),

	{EntropyTimes, DiskTimes} = lists:unzip(Results),
	AvgEntropyMs = lists:sum(EntropyTimes) / Iterations,
	AvgDiskMs = lists:sum(DiskTimes) / Iterations,

	ActualMiBPerIteration = BytesPerIteration / ?MiB,
	EntropyRate = ActualMiBPerIteration / (AvgEntropyMs / 1000),

	ar:console("~n=== Results ===~n~n"),
	ar:console("Data per iteration:   ~.1f MiB (~p chunks)~n", 
		[ActualMiBPerIteration, ChunksPerIteration]),
	ar:console("~n"),
	ar:console("Entropy generation:   ~.2f MiB/s~n", [EntropyRate]),

	case FilePath of
		undefined ->
			ar:console("~nNo disk I/O measured. Real packing may be limited by disk I/O.~n"),
			extrapolate_results(EntropyRate, EntropyRate, BytesPerIteration, PartitionSizeTB);
		_ ->
			DiskRate = ActualMiBPerIteration / (AvgDiskMs / 1000),
			ar:console("Disk I/O:             ~.2f MiB/s~n", [DiskRate]),

			{EffectiveRate, Bottleneck} = case EntropyRate < DiskRate of
				true -> {EntropyRate, "CPU (entropy generation)"};
				false -> {DiskRate, "Disk I/O"}
			end,

			ar:console("~nBottleneck:           ~s~n", [Bottleneck]),
			ar:console("Effective pack rate:  ~.2f MiB/s~n", [EffectiveRate]),

			extrapolate_results(EffectiveRate, EntropyRate, BytesPerIteration, PartitionSizeTB)
	end,

	ar:console("~n").

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

run_iterations(Iteration, MaxIterations, _Threads, _RandomXState, _RewardAddr,
		_FilePath, _BytesPerIteration, _EntropySets, Acc)
		when Iteration >= MaxIterations ->
	lists:reverse(Acc);
run_iterations(Iteration, MaxIterations, Threads, RandomXState, RewardAddr,
		FilePath, BytesPerIteration, EntropySets, Acc) ->
	{EntropyMs, DiskMs} = run_single_iteration(
		Iteration, Threads, RandomXState, RewardAddr, FilePath, 
		BytesPerIteration, EntropySets),
	run_iterations(Iteration + 1, MaxIterations, Threads, RandomXState, RewardAddr,
		FilePath, BytesPerIteration, EntropySets, [{EntropyMs, DiskMs} | Acc]).

run_single_iteration(Iteration, _Threads, RandomXState, RewardAddr, FilePath,
		BytesPerIteration, EntropySets) ->
	EntropyStart = erlang:monotonic_time(microsecond),
	
	SetIds = lists:seq(0, EntropySets - 1),
	AllEntropies = ar_util:pmap(
		fun(SetId) ->
			UniqueId = Iteration * EntropySets + SetId,
			generate_entropy_set(RandomXState, RewardAddr, UniqueId)
		end,
		SetIds, infinity),

	EntropyEnd = erlang:monotonic_time(microsecond),
	EntropyMs = (EntropyEnd - EntropyStart) / 1000,

	DiskMs = case FilePath of
		undefined ->
			0;
		_ ->
			BaseOffset = Iteration * BytesPerIteration,
			DiskStart = erlang:monotonic_time(microsecond),
			write_all_chunks(FilePath, AllEntropies, BaseOffset),
			DiskEnd = erlang:monotonic_time(microsecond),
			(DiskEnd - DiskStart) / 1000
	end,

	{EntropyMs, DiskMs}.

generate_entropy_set(RandomXState, RewardAddr, UniqueId) ->
	SubChunkIndices = lists:seq(0, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT - 1),
	ar_util:pmap(
		fun(SubChunkIndex) ->
			AbsoluteOffset = (UniqueId + 1) * ?DATA_CHUNK_SIZE,
			SubChunkOffset = SubChunkIndex * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
			Key = ar_replica_2_9:get_entropy_key(RewardAddr, AbsoluteOffset, SubChunkOffset),
			ar_mine_randomx:randomx_generate_replica_2_9_entropy(RandomXState, Key)
		end,
		SubChunkIndices, infinity).

write_all_chunks(FilePath, EntropySets, BaseOffset) ->
	{ok, FH} = file:open(FilePath, [write, read, binary, raw]),
	write_entropy_sets(FH, EntropySets, BaseOffset, 0),
	ok = file:sync(FH),
	file:close(FH).

write_entropy_sets(_FH, [], _BaseOffset, _SetIdx) ->
	ok;
write_entropy_sets(FH, [Entropies | Rest], BaseOffset, SetIdx) ->
	write_chunks_from_set(FH, Entropies, BaseOffset, SetIdx, 0),
	write_entropy_sets(FH, Rest, BaseOffset, SetIdx + 1).

write_chunks_from_set(_FH, _Entropies, _BaseOffset, _SetIdx, SliceIdx)
		when SliceIdx >= ?SLICES_PER_ENTROPY ->
	ok;
write_chunks_from_set(FH, Entropies, BaseOffset, SetIdx, SliceIdx) ->
	Chunk = assemble_chunk(Entropies, SliceIdx),
	ChunkIndex = SetIdx * ?SLICES_PER_ENTROPY + SliceIdx,
	Position = BaseOffset + ChunkIndex * ?DATA_CHUNK_SIZE,
	ok = file:pwrite(FH, Position, Chunk),
	write_chunks_from_set(FH, Entropies, BaseOffset, SetIdx, SliceIdx + 1).

assemble_chunk(Entropies, SliceIdx) ->
	SliceSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	SliceOffset = SliceIdx * SliceSize,
	iolist_to_binary(lists:map(
		fun(Entropy) ->
			binary:part(Entropy, SliceOffset, SliceSize)
		end,
		Entropies)).

extrapolate_results(EffectiveRate, EntropyRate, TestedBytes, PartitionSizeTB) ->
	PartitionBytes = trunc(PartitionSizeTB * ?TB),

	ar:console("~n--- Extrapolation to ~.1f TB partition ---~n", [PartitionSizeTB]),

	case TestedBytes < PartitionBytes of
		true ->
			ExtrapolationFactor = PartitionBytes / TestedBytes,
			ar:console("Extrapolation factor: ~.1f MiB -> ~.1f TB = ~.1fx~n",
				[TestedBytes / ?MiB, PartitionSizeTB, ExtrapolationFactor]),
			EntropyTimeSeconds = PartitionBytes / (EntropyRate * ?MiB),
			DiskTimeSeconds = PartitionBytes / (EffectiveRate * ?MiB),

			TotalSeconds = max(EntropyTimeSeconds, DiskTimeSeconds),

			ar:console("~n"),
			ar:console("Estimated time to prepare ~.1f TB partition: ~s~n",
				[PartitionSizeTB, format_duration(TotalSeconds)]);
		false ->
			TotalSeconds = PartitionBytes / (EffectiveRate * ?MiB),
			ar:console("Time to prepare ~.1f TB: ~s~n",
				[PartitionSizeTB, format_duration(TotalSeconds)])
	end,

	ar:console("~nNote: In production, syncing and network download may be bottlenecks.~n").

format_duration(Seconds) when Seconds < 60 ->
	io_lib:format("~.1f seconds", [Seconds]);
format_duration(Seconds) when Seconds < 3600 ->
	Minutes = Seconds / 60,
	io_lib:format("~.1f minutes", [Minutes]);
format_duration(Seconds) when Seconds < 86400 ->
	Hours = Seconds / 3600,
	io_lib:format("~.1f hours", [Hours]);
format_duration(Seconds) ->
	Days = Seconds / 86400,
	io_lib:format("~.1f days (~.0f hours)", [Days, Days * 24]).
