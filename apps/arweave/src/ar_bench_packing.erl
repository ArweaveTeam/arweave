-module(ar_bench_packing).

-export([run_benchmark_from_cli/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("kernel/include/file.hrl").

-record(test_config, {
	test,
	num_workers,
	total_megabytes,
	jit,
	large_pages,
	hardware_aes,
	packing_difficulty,
	rounds,
	root,
	src_address,
	dst_address,
	randomx_state,
	input_file,
	output_file
}).

-define(VALID_TESTS, #{
	baseline_pack => {false, fun baseline_pack_chunks/4},
	baseline_repack => {true, fun baseline_repack_chunks/4},
	nif_repack => {true, fun nif_repack_chunks/4},
	nif_repack_legacy_to_composite => {true, fun nif_repack_legacy_to_composite_chunks/4},
	nif_repack_composite_to_composite => {true, fun nif_repack_composite_to_composite_chunks/4},
	baseline_pack_composite => {false, fun baseline_pack_composite_chunks/4}
}).

run_benchmark_from_cli(Args) ->
	Test = list_to_atom(get_flag_value(Args, "test", "baseline_pack")),
	JIT = list_to_integer(get_flag_value(Args, "jit", "1")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	HardwareAES = list_to_integer(get_flag_value(Args, "hw_aes", "1")),
	VDF = list_to_integer(get_flag_value(Args, "vdf", "0")),
	PackingDifficulty = list_to_integer(get_flag_value(Args, "pdiff", "1")),
	Rounds = list_to_integer(get_flag_value(Args, "rounds",
		integer_to_list(?COMPOSITE_PACKING_ROUND_COUNT))),
	run_benchmark(Test, JIT, LargePages, HardwareAES, VDF, PackingDifficulty, Rounds).

get_flag_value([], _, DefaultValue) ->
	DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
	Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
	get_flag_value(Tail, TargetFlag, DefaultValue).

show_help() ->
	io:format("~nUsage: benchmark-packing [options]~n"),
	io:format("Options:~n"),
	io:format("  test <test> (default: baseline_pack)~n"),
	io:format("  mb <megabytes> (default: 16)~n"),
	io:format("  jit <0|1> (default: 1)~n"),
	io:format("  large_pages <0|1> (default: 1)~n"),
	io:format("  hw_aes <0|1> (default: 1)~n"),
	io:format("  vdf <0|1> (default: 0)~n"),
	io:format("  pdiff <number> (default: 1)~n"),
	io:format("  rounds <number> (default: 10)~n"),
	lists:foreach(fun(Test) -> io:format("  ~p~n", [Test]) end, maps:keys(?VALID_TESTS)),
	erlang:halt().

run_benchmark(Test, JIT, LargePages, HardwareAES, VDF, PackingDifficulty, Rounds) ->
	timer:sleep(3000),
	ets:new(offsets, [set, named_table, public]),
	EncodedRoot = <<"OIgTTxuEPklMR47Ho8VWnNr1Uh6TNjzxwIs38yuqBK0">>,
	Root = ar_util:decode(EncodedRoot),
	EncodedSrcAddress = <<"mvK6e65dcD6XNYDHUVxMa7-d6wVP535Ummtvb8OCUtQ">>,
	SrcAddress = ar_util:decode(EncodedSrcAddress),
	EncodedDstAddress = <<"ymvkTAt6DVo0LaV3SH4TPLvzCmn5TIqvCcv1pHWt2Zs">>,
	DstAddress = ar_util:decode(EncodedDstAddress),

	NumWorkers = erlang:system_info(dirty_cpu_schedulers_online),

	TotalMegaBytes = (1024 div NumWorkers) * NumWorkers,

	Config = #test_config{
		test = Test,
		num_workers = NumWorkers,
		total_megabytes = TotalMegaBytes,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		packing_difficulty = PackingDifficulty,
		rounds = Rounds,
		root = Root,
		src_address = SrcAddress,
		dst_address = DstAddress
	},

	io:format("~nBenchmark settings:~n"),
	io:format("~12s: ~p~n", ["Test", Test]),
	io:format("~12s: ~p~n", ["Data (MB)", TotalMegaBytes]),
	io:format("~12s: ~p~n", ["Cores Used", NumWorkers]),
	io:format("~12s: ~p~n", ["JIT", JIT]),
	io:format("~12s: ~p~n", ["Large Pages", LargePages]),
	io:format("~12s: ~p~n", ["HW AES", HardwareAES]),
	io:format("~12s: ~p~n", ["VDF", VDF]),
	io:format("~nBenchmark settings (composite only):~n"),
	io:format("~12s: ~p~n", ["pdiff", PackingDifficulty]),
	io:format("~12s: ~p~n", ["rounds", Rounds]),
	io:format("~n"),

	generate_input(Config),

	start_vdf(VDF),

	case lists:member(Test, maps:keys(?VALID_TESTS)) of
		true ->
			run_dirty_benchmark(Config);
		false ->
			show_help()
	end,

	Init = ar_bench_timer:get_total({init}) / 1000000,
	Total = ar_bench_timer:get_total({wall}) / 1000000,

	File = open_file("benchmark.results.csv", [append]),

	%% Write the CSV string to the file
	Output = io_lib:format("~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p~n", [
		erlang:system_time() div 1000000000,
		Test, TotalMegaBytes, JIT, LargePages, HardwareAES, VDF,
		PackingDifficulty, Rounds,
		Init, Total]),
	
	file:write(File, Output),
	file:close(File),

	Label = case Test of
		baseline_pack ->
			"Chunks Packed";
		_ ->
			"Chunks Repacked"
	end,

	Chunks = (TotalMegaBytes * ?MiB) div ?DATA_CHUNK_SIZE,
	TimePerChunk = (Total / Chunks) * 1000,
	TimePerChunkPerCore = TimePerChunk * NumWorkers,
	ChunksPerSecond = Chunks / Total,
	ChunksPerSecondPerCore = ChunksPerSecond / NumWorkers,

	io:format("~nBenchmark results:~n"),
	io:format("~28s: ~p~n", [Label, Chunks]),
	io:format("~28s: ~.2f~n", ["Total Time (s)", Total]),
	io:format("~28s: ~.2f~n", ["Time Per Chunk (ms)", TimePerChunk]),
	io:format("~28s: ~.2f~n", ["Time Per Chunk Per Core (ms)", TimePerChunkPerCore]),
	io:format("~28s: ~p~n", ["Chunks Per Second", floor(ChunksPerSecond)]),
	io:format("~28s: ~p~n", ["Chunks Per Second Per Core", floor(ChunksPerSecondPerCore)]).

%% --------------------------------------------------------------------------------------------
%% Write Input files
%% --------------------------------------------------------------------------------------------
is_repack_test(Test) ->
	{IsRepackTest, _} = maps:get(Test, ?VALID_TESTS),
	IsRepackTest.

output_filename(Config) ->
	#test_config{
		test = Test,
		total_megabytes = TotalMegaBytes,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES
	} = Config,
	Permutation = {TotalMegaBytes, JIT, LargePages, HardwareAES},
	%% convert the Permutation tuple to a list of strings so that we can join them with a dot
	StringList = lists:map(fun(E) -> integer_to_list(E) end, tuple_to_list(Permutation)),
	io_lib:format("benchmark.output.~s.~p", [string:join(StringList, "."), Test]).
unpacked_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.unpacked", [TotalMegaBytes]).
packed_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.packed", [TotalMegaBytes]).

generate_input(Config) ->
	#test_config{ total_megabytes = TotalMegaBytes } = Config,
	TotalBytes = TotalMegaBytes * ?MiB,

	UnpackedFilename = unpacked_filename(TotalMegaBytes),
	case file:read_file_info(UnpackedFilename) of
		{ok, FileInfo1} ->
			if
				FileInfo1#file_info.size == TotalBytes ->
					ok;
				true ->
					file:delete(UnpackedFilename),
					write_random_data(UnpackedFilename, TotalBytes)
			end;
		{error, _} ->
			write_random_data(UnpackedFilename, TotalBytes)
	end,

	PackedFilename = packed_filename(TotalMegaBytes),
	case file:read_file_info(PackedFilename) of
		{ok, FileInfo2} ->
			%% If the file already exists and is the correct size, we don't need to do anything
			if
				FileInfo2#file_info.size == TotalBytes ->
					ok;
				true ->
					file:delete(PackedFilename),
					write_packed_data(Config, UnpackedFilename, PackedFilename)
			end;
		{error, _} ->
			write_packed_data(Config, UnpackedFilename, PackedFilename)
	end.

write_random_data(UnpackedFilename, TotalBytes) ->
	io:format("Generating input file: ~s~n", [UnpackedFilename]),
	File = open_file(UnpackedFilename, [write, binary, raw]),
	write_chunks(File, TotalBytes),
	file:close(File).
write_chunks(File, TotalBytes) ->
	ChunkSize = 1024*1024, % 1MB
	RemainingBytes = TotalBytes,
	write_chunks_loop(File, RemainingBytes, ChunkSize).
write_chunks_loop(_File, 0, _) ->
	ok;
write_chunks_loop(File, RemainingBytes, ChunkSize) ->
	BytesToWrite = min(RemainingBytes, ChunkSize),
	Data = crypto:strong_rand_bytes(BytesToWrite),
	file:write(File, Data),
	write_chunks_loop(File, RemainingBytes - BytesToWrite, ChunkSize).

write_packed_data(Config, UnpackedFilename, PackedFilename) ->
	#test_config{
		num_workers = NumWorkers,
		jit = JIT,
		large_pages = LargePages
	} = Config,
	io:format("Generating input file: ~s~n", [PackedFilename]),
	{ok, RandomXState} = ar_bench_timer:record({init},
		fun ar_mine_randomx:init_randomx_nif/5,
			[?RANDOMX_PACKING_KEY, ?RANDOMX_HASHING_MODE_FAST, JIT, LargePages, NumWorkers]),

	UnpackedFileHandle = open_file(UnpackedFilename, [read, binary]),
	PackedFileHandle = open_file(PackedFilename, [write, binary]),

	dirty_test(Config#test_config{
		test = baseline_pack,
		randomx_state = RandomXState,
		input_file = UnpackedFileHandle,
		output_file = PackedFileHandle
	}),
	
	file:close(PackedFileHandle),
	file:close(UnpackedFileHandle).

%% --------------------------------------------------------------------------------------------
%% VDF Background Task
%% --------------------------------------------------------------------------------------------
start_vdf(0) ->
	ok;
start_vdf(1) ->
	io:format("~nStarting VDF...~n~n"),
	Input = crypto:strong_rand_bytes(32),
	spawn(fun() -> vdf_worker(Input) end).

vdf_worker(Input) ->
	ar_vdf:compute2(1, Input, ?VDF_DIFFICULTY),
	vdf_worker(Input).

%% --------------------------------------------------------------------------------------------
%% Test Runners
%% --------------------------------------------------------------------------------------------

run_dirty_benchmark(Config) ->
	#test_config{
		test = Test,
		num_workers = NumWorkers,
		jit = JIT,
		large_pages = LargePages,
		total_megabytes = TotalMegaBytes
	} = Config,
	
	Config2 = case is_repack_test(Test) of
		true ->
			Config#test_config{
				input_file = open_file(packed_filename(TotalMegaBytes), [read, binary]),
				output_file = open_file(output_filename(Config), [write, binary])
			};
		false ->
			Config#test_config{
				input_file = open_file(unpacked_filename(TotalMegaBytes), [read, binary]),
				output_file = open_file(output_filename(Config), [write, binary])
			}
	end,

	{ok, RandomXState} = ar_bench_timer:record({init},
		fun ar_mine_randomx:init_randomx_nif/5,
			[?RANDOMX_PACKING_KEY, ?RANDOMX_HASHING_MODE_FAST, JIT, LargePages, NumWorkers]),

	run_dirty_test(Config2#test_config{randomx_state = RandomXState}).

run_dirty_test(Config) ->
	#test_config{
		input_file = InputFileHandle,
		output_file = OutputFileHandle
	} = Config,

	io:format("packing..."),
	ar_bench_timer:record({wall}, fun dirty_test/1, [Config]),

	file:close(InputFileHandle),
	file:close(OutputFileHandle).

%% For now this just encrypts each chunk without adding the offset hash
dirty_test(Config) ->
	#test_config{
		test = Test,
		total_megabytes = TotalMegaBytes,
		num_workers = NumWorkers
	} = Config,
	TotalBytes = TotalMegaBytes * ?MiB,
	%% Spin up NumWorkers threads each responsible for a fraction of the file
	WorkerSize = TotalBytes div NumWorkers,
	{_, WorkerFun} = maps:get(Test, ?VALID_TESTS),
	Workers = [spawn_monitor(
		fun() -> dirty_worker(
			N,
			Config, 
			WorkerFun,
			WorkerSize * (N - 1),
			WorkerSize
		) end) || N <- lists:seq(1, NumWorkers)],
	%% Wait for all workers to finish
	[
		receive
			{'DOWN', Ref, process, Pid, _Result} -> erlang:demonitor(Ref), ok
		after 
			60000 -> timeout
		end || {Pid, Ref} <- Workers
	],
	io:format("~n").

dirty_worker(WorkerID, Config, WorkerFun, Offset, Size) ->
	ar_bench_timer:record({total, WorkerID}, WorkerFun, [
			WorkerID,
			Config,
			Offset,
			Size
		]),
	exit(normal).

%% --------------------------------------------------------------------------------------------
%% Baseline Packing Test
%% --------------------------------------------------------------------------------------------
baseline_pack_chunks(_WorkerID, _Config, _Offset, Size) when Size =< 0 ->
	ok;
baseline_pack_chunks(WorkerID, Config, Offset, Size) ->
	#test_config{
		randomx_state = RandomXState,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		input_file = UnpackedFileHandle,
		output_file = PackedFileHandle,
		root = Root,
		dst_address = DstAddress
	} = Config,
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, Key} = ar_packing_server:chunk_key({spora_2_6, DstAddress}, Offset, Root),
	ReadResult = file:pread(UnpackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
		{ok, UnpackedChunk} ->
			{ok, PackedChunk} = ar_mine_randomx:randomx_encrypt_chunk_nif(
				RandomXState, Key, UnpackedChunk, ?RANDOMX_PACKING_ROUNDS_2_6,
				JIT, LargePages, HardwareAES),
			file:pwrite(PackedFileHandle, Offset, PackedChunk),
			(Size - ChunkSize);
		eof ->
			0;
		{error, Reason} ->
			io:format("Error reading file: ~p~n", [Reason]),
			0
	end,
	baseline_pack_chunks(WorkerID, Config, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% Baseline Packing 2.8 Test
%% --------------------------------------------------------------------------------------------
% TODO diff other than 1

baseline_pack_composite_chunks(_WorkerID, _Config, _Offset, Size) when Size =< 0 ->
	ok;
baseline_pack_composite_chunks(WorkerID, Config, Offset, Size) ->
	#test_config{
		randomx_state = RandomXState,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		input_file = UnpackedFileHandle,
		output_file = PackedFileHandle,
		root = Root,
		dst_address = DstAddress,
		packing_difficulty = PackingDifficulty,
		rounds = Rounds
	} = Config,
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{composite, Key} = ar_packing_server:chunk_key({composite, DstAddress, PackingDifficulty}, Offset, Root),
	ReadResult = file:pread(UnpackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
		{ok, UnpackedChunk} ->
			{ok, PackedChunk} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
				RandomXState, Key, UnpackedChunk,
				JIT, LargePages, HardwareAES,
				Rounds, PackingDifficulty, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
			file:pwrite(PackedFileHandle, Offset, PackedChunk),
			(Size - ChunkSize);
		eof ->
			0;
		{error, Reason} ->
			io:format("Error reading file: ~p~n", [Reason]),
			0
	end,
	baseline_pack_composite_chunks(WorkerID, Config, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% Baseline Repacking Test
%% --------------------------------------------------------------------------------------------
baseline_repack_chunks(_WorkerID, _Config, _Offset, Size) when Size =< 0 ->
	ok;
baseline_repack_chunks(WorkerID, Config, Offset, Size) ->
	#test_config{
		randomx_state = RandomXState,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		input_file = PackedFileHandle,
		output_file = RepackedFileHandle,
		root = Root,
		src_address = SrcAddress,
		dst_address = DstAddress
	} = Config,
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, UnpackKey} = ar_packing_server:chunk_key({spora_2_6, SrcAddress}, Offset, Root),
	{spora_2_6, PackKey} = ar_packing_server:chunk_key({spora_2_6, DstAddress}, Offset, Root),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
		{ok, PackedChunk} ->
			{ok, UnpackedChunk} = ar_mine_randomx:randomx_decrypt_chunk_nif(
				RandomXState, UnpackKey, PackedChunk, ChunkSize, ?RANDOMX_PACKING_ROUNDS_2_6,
				JIT, LargePages, HardwareAES),
			{ok, RepackedChunk} =ar_mine_randomx:randomx_encrypt_chunk_nif(
				RandomXState, PackKey, UnpackedChunk, ?RANDOMX_PACKING_ROUNDS_2_6,
				JIT, LargePages, HardwareAES),	
			file:pwrite(RepackedFileHandle, Offset, RepackedChunk),
			(Size - ChunkSize);
		eof ->
			0;
		{error, Reason} ->
			io:format("Error reading file: ~p~n", [Reason]),
			0
	end,
	baseline_repack_chunks(WorkerID, Config, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% NIF Repacking Test
%% --------------------------------------------------------------------------------------------
nif_repack_chunks(_WorkerID, _Config, _Offset, Size) when Size =< 0 ->
	ok;
nif_repack_chunks(WorkerID, Config, Offset, Size) ->
	#test_config{
		randomx_state = RandomXState,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		input_file = PackedFileHandle,
		output_file = RepackedFileHandle,
		root = Root,
		src_address = SrcAddress,
		dst_address = DstAddress
	} = Config,
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, UnpackKey} = ar_packing_server:chunk_key({spora_2_6, SrcAddress}, Offset, Root),
	{spora_2_6, PackKey} = ar_packing_server:chunk_key({spora_2_6, DstAddress}, Offset, Root),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
		{ok, PackedChunk} ->
			{ok, RepackedChunk, _} = ar_mine_randomx:randomx_reencrypt_chunk_nif(
				RandomXState, UnpackKey, PackKey, PackedChunk, ChunkSize,
				?RANDOMX_PACKING_ROUNDS_2_6, ?RANDOMX_PACKING_ROUNDS_2_6,
				JIT, LargePages, HardwareAES),
			file:pwrite(RepackedFileHandle, Offset, RepackedChunk),
			(Size - ChunkSize);
		eof ->
			0;
		{error, Reason} ->
			io:format("Error reading file: ~p~n", [Reason]),
			0
	end,
	nif_repack_chunks(WorkerID, Config, Offset+ChunkSize, RemainingSize).

nif_repack_legacy_to_composite_chunks(_WorkerID, _Config, _Offset, Size) when Size =< 0 ->
	ok;
nif_repack_legacy_to_composite_chunks(WorkerID, Config, Offset, Size) ->
	#test_config{
		randomx_state = RandomXState,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		input_file = PackedFileHandle,
		output_file = RepackedFileHandle,
		root = Root,
		src_address = SrcAddress,
		dst_address = DstAddress,
		packing_difficulty = PackingDifficulty,
		rounds = Rounds
	} = Config,
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, UnpackKey} = ar_packing_server:chunk_key({spora_2_6, SrcAddress}, Offset, Root),
	{composite, PackKey} = ar_packing_server:chunk_key({composite, DstAddress, PackingDifficulty}, Offset, Root),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
		{ok, PackedChunk} ->
			{ok, RepackedChunk, _} = ar_mine_randomx:randomx_reencrypt_legacy_to_composite_chunk_nif(
				RandomXState, UnpackKey, PackKey, PackedChunk,
				JIT, LargePages, HardwareAES,
				?RANDOMX_PACKING_ROUNDS_2_6, Rounds, PackingDifficulty,
				?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
			file:pwrite(RepackedFileHandle, Offset, RepackedChunk),
			(Size - ChunkSize);
		eof ->
			0;
		{error, Reason} ->
			io:format("Error reading file: ~p~n", [Reason]),
			0
	end,
	nif_repack_legacy_to_composite_chunks(WorkerID, Config, Offset+ChunkSize, RemainingSize).

nif_repack_composite_to_composite_chunks(_WorkerID, _Config, _Offset, Size) when Size =< 0 ->
	ok;
nif_repack_composite_to_composite_chunks(WorkerID, Config, Offset, Size) ->
	#test_config{
		randomx_state = RandomXState,
		jit = JIT,
		large_pages = LargePages,
		hardware_aes = HardwareAES,
		input_file = PackedFileHandle,
		output_file = RepackedFileHandle,
		root = Root,
		src_address = SrcAddress,
		dst_address = DstAddress,
		packing_difficulty = PackingDifficulty,
		rounds = Rounds
	} = Config,
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{composite, UnpackKey} = ar_packing_server:chunk_key({composite, SrcAddress, PackingDifficulty}, Offset, Root),
	{composite, PackKey} = ar_packing_server:chunk_key({composite, DstAddress, PackingDifficulty}, Offset, Root),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
		{ok, PackedChunk} ->
			{ok, RepackedChunk, _} = ar_mine_randomx:randomx_reencrypt_composite_to_composite_chunk_nif(
				RandomXState, UnpackKey, PackKey, PackedChunk, JIT, LargePages, HardwareAES,
				Rounds, Rounds, PackingDifficulty, PackingDifficulty,
				?COMPOSITE_PACKING_SUB_CHUNK_COUNT, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT),
			
			file:pwrite(RepackedFileHandle, Offset, RepackedChunk),
			(Size - ChunkSize);
		eof ->
			0;
		{error, Reason} ->
			io:format("Error reading file: ~p~n", [Reason]),
			0
	end,
	nif_repack_composite_to_composite_chunks(WorkerID, Config, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% Helpers
%% --------------------------------------------------------------------------------------------
open_file(Filename, Options) ->
	case file:open(Filename, Options) of
		{ok, File} ->
			File;
		{error, Reason} ->
			io:format("Error opening ~s: ~p~n", [Filename, Reason]),
			show_help()
	end.
