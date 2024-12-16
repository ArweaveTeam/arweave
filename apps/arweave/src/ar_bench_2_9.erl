-module(ar_bench_2_9).

-export([show_help/0, run_benchmark_from_cli/1, run_benchmark/1]).

-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").

run_benchmark_from_cli(Args) ->
	Threads = list_to_integer(get_flag_value(Args, "threads", "1")),
	DataMiB = list_to_integer(get_flag_value(Args, "mib", "1024")),

	Format= case get_flag_value(Args, "format", "replica_2_9") of
		"replica_2_9" -> replica_2_9;
		"replica_2_9f" -> replica_2_9f;
		"replica_2_9fll" -> replica_2_9fll;
		"composite.1" -> {composite, 1};
		"composite.10" -> {composite, 10};
		"spora_2_6" -> spora_2_6;
		_ -> show_help()
	end,

	% Collect all directory values
	Dirs = collect_dirs(Args),

	% Ensure each directory exists
	lists:foreach(fun(Dir) ->
		case filelib:ensure_dir(filename:join(Dir, "dummy")) of
			ok -> ok;
			{error, Reason} ->
				io:format("Error: Could not ensure directory ~p exists. Reason: ~p~n", [Dir, Reason]),
				show_help(),
				erlang:halt(1)
		end
	end, Dirs),
	run_benchmark({Format, Dirs, Threads, DataMiB}).

collect_dirs([]) ->
	[];
collect_dirs(["dir", Dir | Tail]) ->
	[Dir | collect_dirs(Tail)];
collect_dirs([_ | Tail]) ->
	collect_dirs(Tail).

get_flag_value([], _, DefaultValue) ->
	DefaultValue;
get_flag_value([Flag, Value | Tail], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
	Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
	get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark-2.9 [format replica_2_9|replica_2_9f|replica_2_9fll|composite.1|composite.10|spora_2_6] [threads N] [mib N] [dir path1 dir path2 dir path3 ...]~n~n"),

	io:format("format: format to pack. replica_2_9, replica_2_9f, replica_2_9fll, composite.1, composite.10, or spora_2_6. Default: replica_2_9.~n"),
	io:format("threads: number of threads to run. Default: 1.~n"),
	io:format("mib: total amount of data to pack in MiB. Default: 1024.~n"),
	io:format("     Will be divided evenly between threads, so the final number may be~n"),
	io:format("     lower than specified to ensure balanced threads.~n"),
	io:format("dir: directories to pack data to. If left off, benchmark will just simulate~n"),
	io:format("     entropy generation without writing to disk.~n~n"),
	erlang:halt().

run_benchmark({Format, Dirs, Threads, DataMiB}) ->
	application:set_env(arweave, config, #config{ 
		disable = [], enable  = [randomx_large_pages] }),

	ThreadDirPairs = assign_threads_to_dirs(Threads, Dirs),

	Context = prepare_context(Format, Threads, DataMiB),
		
	PackedData = get_total_data(Format, Threads, Context),

	Iterations = get_iterations(Format, Threads, Context),
	
	case Dirs of
		[] ->
			ar:console("~n~nRunning ~p threads (~p iterations per thread) to pack ~.1f MiB of data~n", [Threads, Iterations, PackedData]);
		_ ->
			ar:console("~n~nRunning ~p threads (~p iterations per thread) to pack and write ~.1f MiB of data~n", [Threads, Iterations, PackedData]),
			ar:console("Directories: ~p~n", [Dirs])
	end,
	Times = ar_util:pmap(
		fun({Thread, Dir}) ->
			Start = erlang:monotonic_time(millisecond),
			pack_chunks(Format, Thread, Dir, Context, Iterations),
			End = erlang:monotonic_time(millisecond),
			End - Start
		end,
		ThreadDirPairs),
	ar:console("~n~nTimes: ~p~n", [Times]),
	Average = lists:sum(Times) / length(Times),
	ar:console("~nAverage: ~p ms~n", [Average]),
	
	Rate = PackedData / Average * 1000,
	ar:console("~nTime ~p ms, ~.1f MiB, ~.2f MiB/s ~n~n", [Average, PackedData, Rate]).

assign_threads_to_dirs(Threads, []) ->
	[ {Thread, undefined} || Thread <- lists:seq(1, Threads) ];
assign_threads_to_dirs(Threads, Dirs) ->
	ThreadList = lists:seq(1, Threads),
	DirCount = length(Dirs),
	% Cycle through directories if there are more threads than directories
	CycledDirs = lists:map(
		fun(Index) -> lists:nth((Index rem DirCount) + 1, Dirs) end, ThreadList),
	lists:zip(ThreadList, CycledDirs).

prepare_context(replica_2_9, Threads, DataMiB) ->
	Address = crypto:strong_rand_bytes(32),
	SubChunk = crypto:strong_rand_bytes(?COMPOSITE_PACKING_SUB_CHUNK_SIZE),
	SubChunkIndex = rand:uniform(32768),
	Offset = rand:uniform(1024 * 1024 * 1024),
	Key = ar_block:get_replica_2_9_entropy_key(Address, Offset, SubChunkIndex),
	DataPerThread = DataMiB * ?MiB div Threads,
	EntropyPerThread = DataPerThread div 
		(?REPLICA_2_9_ENTROPY_SUB_CHUNK_COUNT * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE),
	RandomXState = ar_mine_randomx:init_fast2(
		rxsquared, ?RANDOMX_PACKING_KEY, 1, 1, 
		erlang:system_info(dirty_cpu_schedulers_online)),
	{RandomXState, SubChunk, Key, EntropyPerThread};
prepare_context(replica_2_9f, Threads, DataMiB)->
	prepare_context(replica_2_9, Threads, DataMiB);
prepare_context(replica_2_9fll, Threads, DataMiB)->
	prepare_context(replica_2_9, Threads, DataMiB);
prepare_context(spora_2_6, Threads, DataMiB) ->
	Root = crypto:strong_rand_bytes(32),
	Address = crypto:strong_rand_bytes(32),
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Offset = rand:uniform(1024 * 1024 * 1024),
	{spora_2_6, Key} = ar_packing_server:chunk_key({spora_2_6, Address}, Offset, Root),
	{rx512, RandomXState} = ar_mine_randomx:init_fast2(
		rx512, ?RANDOMX_PACKING_KEY, 1, 1, 
		erlang:system_info(dirty_cpu_schedulers_online)),
	ChunksPerThread = DataMiB * ?MiB div Threads div ?DATA_CHUNK_SIZE,
	{RandomXState, Chunk, Key, ChunksPerThread};
prepare_context({composite, Difficulty}, Threads, DataMiB) ->
	Root = crypto:strong_rand_bytes(32),
	Address = crypto:strong_rand_bytes(32),
	Chunk = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
	Offset = rand:uniform(1024 * 1024 * 1024),
	{composite, Key} = ar_packing_server:chunk_key({composite, Address, Difficulty}, Offset, Root),
	{rx4096, RandomXState} = ar_mine_randomx:init_fast2(
		rx4096, ?RANDOMX_PACKING_KEY, 1, 1, 
		erlang:system_info(dirty_cpu_schedulers_online)),
	ChunksPerThread = DataMiB * ?MiB div Threads div ?DATA_CHUNK_SIZE,
	{RandomXState, Chunk, Key, ChunksPerThread}.

get_total_data(replica_2_9, Threads, {_,_, _, EntropyPerThread}) ->
	Threads * EntropyPerThread * 
	?REPLICA_2_9_ENTROPY_SUB_CHUNK_COUNT * ?COMPOSITE_PACKING_SUB_CHUNK_SIZE / ?MiB;
get_total_data(replica_2_9f, Threads, Context) ->
	get_total_data(replica_2_9, Threads, Context);
get_total_data(replica_2_9fll, Threads, Context) ->
	get_total_data(replica_2_9, Threads, Context);
get_total_data(spora_2_6, Threads, {_, _, _, ChunksPerThread}) ->
	Threads * ChunksPerThread * ?DATA_CHUNK_SIZE / ?MiB;
get_total_data({composite, _}, Threads, {_, _, _, ChunksPerThread}) ->
	Threads * ChunksPerThread * ?DATA_CHUNK_SIZE / ?MiB.
get_iterations(replica_2_9, _Threads, {_,_, _, EntropyPerThread}) ->
	EntropyPerThread;
get_iterations(replica_2_9f, _Threads, {_,_, _, EntropyPerThread}) ->
	EntropyPerThread;
get_iterations(replica_2_9fll, _Threads, {_,_, _, EntropyPerThread}) ->
	EntropyPerThread;
get_iterations(spora_2_6, _Threads, {_, _, _, ChunksPerThread}) ->
	ChunksPerThread;
get_iterations({composite, _}, _Threads, {_, _, _, ChunksPerThread}) ->
	ChunksPerThread.

pack_chunks(_Format, _Thread, _Dir, _Context, 0) ->
	ok;
pack_chunks(replica_2_9, Thread, Dir, Context, Count) ->
	{RandomXState, SubChunk, Key, _EntropyPerThread} = Context,
	Entropy = ar_mine_randomx:randomx_generate_replica_2_9_entropy(RandomXState, Key),
	PackedSubChunks = pack_sub_chunks(SubChunk, Entropy, 0, RandomXState, []),
	case Dir of
		undefined ->
			ok;
		_ ->
			
			Filename = io_lib:format("t~p_e~p.bin", [Thread, Count]),
			Path = filename:join(Dir, Filename),
			file:write_file(Path, PackedSubChunks)
	end,
	pack_chunks(replica_2_9, Thread, Dir, Context, Count-1);
pack_chunks(replica_2_9f, _Thread, _Dir, _Context, 0) ->
	ok;
pack_chunks(replica_2_9f, Thread, Dir, Context, Count) ->
	{RandomXState, SubChunk, Key, _EntropyPerThread} = Context,

	%% This is where we call the new fused NIF:
	%% Suppose the new NIF returns {ok, EntropyBin} or something similar.
	{ok, Entropy} = ar_rxsquared_nif:rsp_fused_entropy_nif(
		element(2, RandomXState),
		?REPLICA_2_9_ENTROPY_SUB_CHUNK_COUNT,
		?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
		?REPLICA_2_9_RANDOMX_LANE_COUNT,
		?REPLICA_2_9_RANDOMX_DEPTH,
		1,  %% jitEnabled,
		1,  %% largePagesEnabled
		1,  %% hardwareAESEnabled
		?REPLICA_2_9_RANDOMX_ROUND_COUNT,
		Key
	),

	%% Then we can reuse pack_sub_chunks as before
	PackedSubChunks = pack_sub_chunks(SubChunk, Entropy, 0, RandomXState, []),
	case Dir of
		undefined ->
			ok;
		_ ->
			Filename = io_lib:format("t~p_e~p.bin", [Thread, Count]),
			Path = filename:join(Dir, Filename),
			file:write_file(Path, PackedSubChunks)
	end,
	pack_chunks(replica_2_9f, Thread, Dir, Context, Count - 1);
pack_chunks(replica_2_9fll, _Thread, _Dir, _Context, 0) ->
	ok;
pack_chunks(replica_2_9fll, Thread, Dir, Context, Count) ->
	{RandomXState, SubChunk, Key, _EntropyPerThread} = Context,

	%% This is where we call the new fused NIF:
	%% Suppose the new NIF returns {ok, EntropyBin} or something similar.
	{ok, Entropy} = ar_rxsquared_nif:rsp_fused_entropy_low_latency_nif(
		element(2, RandomXState),
		?REPLICA_2_9_ENTROPY_SUB_CHUNK_COUNT,
		?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
		?REPLICA_2_9_RANDOMX_LANE_COUNT,
		?REPLICA_2_9_RANDOMX_DEPTH,
		1,  %% jitEnabled,
		1,  %% largePagesEnabled
		1,  %% hardwareAESEnabled
		?REPLICA_2_9_RANDOMX_ROUND_COUNT,
		Key
	),

	%% Then we can reuse pack_sub_chunks as before
	PackedSubChunks = pack_sub_chunks(SubChunk, Entropy, 0, RandomXState, []),
	case Dir of
		undefined ->
			ok;
		_ ->
			Filename = io_lib:format("t~p_e~p.bin", [Thread, Count]),
			Path = filename:join(Dir, Filename),
			file:write_file(Path, PackedSubChunks)
	end,
	pack_chunks(replica_2_9fll, Thread, Dir, Context, Count - 1);

pack_chunks(spora_2_6, Thread, Dir, Context, Count) ->
	{RandomXState, Chunk, Key, _ChunksPerThread} = Context,
	{ok, PackedChunk} = ar_rx512_nif:rx512_encrypt_chunk_nif(
		RandomXState, Key, Chunk, ?RANDOMX_PACKING_ROUNDS_2_6,
		1, 1, 1),
	case Dir of
		undefined ->
			ok;
		_ ->
			Filename = io_lib:format("t~p_e~p.bin", [Thread, Count]),
			Path = filename:join(Dir, Filename),
			file:write_file(Path, PackedChunk)
	end,
	pack_chunks(spora_2_6, Thread, Dir, Context, Count-1);
pack_chunks({composite, Difficulty}, Thread, Dir, Context, Count) ->
	{RandomXState, Chunk, Key, _ChunksPerThread} = Context,
	{ok, PackedChunk} = ar_rx4096_nif:rx4096_encrypt_composite_chunk_nif(
		RandomXState, Key, Chunk,
		1, 1, 1,
		?COMPOSITE_PACKING_ROUND_COUNT,
		Difficulty,
		?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
	case Dir of
		undefined ->
			ok;
		_ ->
			Filename = io_lib:format("t~p_e~p.bin", [Thread, Count]),
			Path = filename:join(Dir, Filename),
			file:write_file(Path, PackedChunk)
	end,
	pack_chunks({composite, Difficulty}, Thread, Dir, Context, Count-1).

pack_sub_chunks(_SubChunk, _Entropy, Index, _State, PackedSubChunks)
		when Index == ?REPLICA_2_9_ENTROPY_SUB_CHUNK_COUNT ->
	PackedSubChunks;
pack_sub_chunks(SubChunk, Entropy, Index, State, PackedSubChunks) ->
	{ok, PackedSubChunk} = ar_mine_randomx:randomx_encrypt_replica_2_9_sub_chunk(
			{State, Entropy, SubChunk, Index}),
	pack_sub_chunks(
		SubChunk, Entropy, Index + 1, State, 
		[PackedSubChunk | PackedSubChunks]).
