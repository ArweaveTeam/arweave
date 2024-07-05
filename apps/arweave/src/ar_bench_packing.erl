-module(ar_bench_packing).

-export([run_benchmark_from_cli/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("kernel/include/file.hrl").

run_benchmark_from_cli(Args) ->
	Test = list_to_atom(get_flag_value(Args, "test", "baseline_pack")),
	JIT = list_to_integer(get_flag_value(Args, "jit", "1")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	HardwareAES = list_to_integer(get_flag_value(Args, "hw_aes", "1")),
	VDF = list_to_integer(get_flag_value(Args, "vdf", "0")),
	Iterations = list_to_integer(get_flag_value(Args, "iterations", "1")),
	Rounds = list_to_integer(get_flag_value(Args, "rounds", "8")),
	run_benchmark(Test, JIT, LargePages, HardwareAES, VDF, Iterations, Rounds).

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
	io:format("  iterations <number> (default: 1)~n"),
	io:format("  rounds <number> (default: 8)~n"),
	io:format("Valid tests:~n"),
	io:format("  baseline_pack~n"),
	io:format("  baseline_repack~n"),
	io:format("  nif_repack~n"),
	io:format("  baseline_pack_composite~n"),
	erlang:halt().

run_benchmark(Test, JIT, LargePages, HardwareAES, VDF, Iterations, Rounds) ->
	timer:sleep(3000),
	ets:new(offsets, [set, named_table, public]),
	EncodedRoot = <<"OIgTTxuEPklMR47Ho8VWnNr1Uh6TNjzxwIs38yuqBK0">>,
	Root = ar_util:decode(EncodedRoot),
	EncodedRewardAddress = <<"mvK6e65dcD6XNYDHUVxMa7-d6wVP535Ummtvb8OCUtQ">>,
	RewardAddress = ar_util:decode(EncodedRewardAddress),

	NumWorkers = erlang:system_info(dirty_cpu_schedulers_online),

	TotalMegaBytes = (1024 div NumWorkers) * NumWorkers,

	io:format("~nBenchmark settings:~n"),
	io:format("~12s: ~p~n", ["Test", Test]),
	io:format("~12s: ~p~n", ["Data (MB)", TotalMegaBytes]),
	io:format("~12s: ~p~n", ["Cores Used", NumWorkers]),
	io:format("~12s: ~p~n", ["JIT", JIT]),
	io:format("~12s: ~p~n", ["Large Pages", LargePages]),
	io:format("~12s: ~p~n", ["HW AES", HardwareAES]),
	io:format("~12s: ~p~n", ["VDF", VDF]),
	io:format("~nBenchmark settings (composite only):~n"),
	io:format("~12s: ~p~n", ["iterations", Iterations]),
	io:format("~12s: ~p~n", ["rounds", Rounds]),
	io:format("~n"),

	Permutation = {TotalMegaBytes, JIT, LargePages, HardwareAES},
	generate_input(Permutation, NumWorkers,  Root, RewardAddress),

	start_vdf(VDF),

	case Test of
		baseline_pack ->
			run_dirty_benchmark(baseline_pack, Permutation, NumWorkers, Root, RewardAddress, Iterations, Rounds);
		baseline_repack ->
			run_dirty_benchmark(baseline_repack, Permutation, NumWorkers, Root, RewardAddress, Iterations, Rounds);
		nif_repack ->
			run_dirty_benchmark(nif_repack, Permutation, NumWorkers, Root, RewardAddress, Iterations, Rounds);
		baseline_pack_composite ->
			run_dirty_benchmark(baseline_pack_composite, Permutation, NumWorkers, Root, RewardAddress, Iterations, Rounds);
		_ ->
			show_help()
	end,

	Init = ar_bench_timer:get_total({init}) / 1000000,
	Total = ar_bench_timer:get_total({wall}) / 1000000,

	File = open_file("benchmark.results.csv", [append]),

	%% Write the CSV string to the file
	Output = io_lib:format("~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p~n", [
		erlang:system_time() div 1000000000,
		Test, TotalMegaBytes, JIT, LargePages, HardwareAES, VDF,
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
output_filename(Test, Permutation) ->
	%% convert the Permutation tuple to a list of strings so that we can join them with a dot
	StringList = lists:map(fun(E) -> integer_to_list(E) end, tuple_to_list(Permutation)),
	io_lib:format("benchmark.output.~s.~p", [string:join(StringList, "."), Test]).
unpacked_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.unpacked", [TotalMegaBytes]).
packed_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.packed", [TotalMegaBytes]).

generate_input({TotalMegaBytes, _, _, _} = Permutation, NumWorkers, Root, RewardAddress) ->
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
					write_packed_data(Permutation, UnpackedFilename, PackedFilename,
						TotalMegaBytes, NumWorkers, Root, RewardAddress)
			end;
		{error, _} ->
			write_packed_data(Permutation, UnpackedFilename, PackedFilename,
						TotalMegaBytes, NumWorkers, Root, RewardAddress)
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

write_packed_data({_, JIT, LargePages, _}, UnpackedFilename, PackedFilename,
		TotalMegaBytes, NumWorkers, Root, RewardAddress) ->
	io:format("Generating input file: ~s~n", [PackedFilename]),
	{ok, RandomXState} = ar_bench_timer:record({init},
		fun ar_mine_randomx:init_fast_nif/4, [?RANDOMX_PACKING_KEY, JIT, LargePages, NumWorkers]),

	UnpackedFileHandle = open_file(UnpackedFilename, [read, binary]),
	PackedFileHandle = open_file(PackedFilename, [write, binary]),
	dirty_test({TotalMegaBytes, 1, 1, 1},
		fun baseline_pack_chunks/5,
		{RandomXState, UnpackedFileHandle, PackedFileHandle, Root, RewardAddress},
		NumWorkers),
	
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

run_dirty_benchmark(Test, {_, JIT, LargePages, _} = Permutation, NumWorkers, Root, RewardAddress, Iterations, Rounds) ->
	{ok, RandomXState} = ar_bench_timer:record({init},
		fun ar_mine_randomx:init_fast_nif/4, [?RANDOMX_PACKING_KEY, JIT, LargePages, NumWorkers]),

	run_dirty_test(Test, Permutation, RandomXState, Root, RewardAddress, NumWorkers, Iterations, Rounds).

run_dirty_test(baseline_pack, Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, Iterations, Rounds) ->
	run_dirty_pack_test(baseline_pack, Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, Iterations, Rounds);
run_dirty_test(baseline_repack, {TotalMegaBytes, _, _, _} = Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, _Iterations, _Rounds) ->
	run_dirty_repack_test(
		packed_filename(TotalMegaBytes),
		output_filename(baseline_repack, Permutation),
		fun baseline_repack_chunks/5,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers);
run_dirty_test(nif_repack, {TotalMegaBytes, _, _, _} = Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, _Iterations, _Rounds) ->
	run_dirty_repack_test(
		packed_filename(TotalMegaBytes),
		output_filename(nif_repack, Permutation),
		fun nif_repack_chunks/5,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers);
run_dirty_test(baseline_pack_composite, Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, Iterations, Rounds) ->
	run_dirty_pack_test(baseline_pack_composite, Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, Iterations, Rounds).

run_dirty_pack_test(baseline_pack, {TotalMegaBytes, _, _, _} = Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, _Iterations, _Rounds) ->
	UnpackedFilename = unpacked_filename(TotalMegaBytes),
	PackedFilename = output_filename(baseline_pack, Permutation),
	UnpackedFileHandle = open_file(UnpackedFilename, [read, binary]),
	PackedFileHandle = open_file(PackedFilename, [write, binary]),

	io:format("packing..."),
	Args = {RandomXState, UnpackedFileHandle, PackedFileHandle, Root, RewardAddress},
	ar_bench_timer:record({wall}, fun dirty_test/4, [
		Permutation,
		fun baseline_pack_chunks/5,
		Args,
		NumWorkers
	]),

	file:close(UnpackedFileHandle),
	file:close(PackedFileHandle);

run_dirty_pack_test(baseline_pack_composite, {TotalMegaBytes, _, _, _} = Permutation,
		RandomXState, Root, RewardAddress, NumWorkers, Iterations, Rounds) ->
	UnpackedFilename = unpacked_filename(TotalMegaBytes),
	PackedFilename = output_filename(baseline_pack, Permutation),
	UnpackedFileHandle = open_file(UnpackedFilename, [read, binary]),
	PackedFileHandle = open_file(PackedFilename, [write, binary]),

	io:format("packing..."),
	Args = {RandomXState, UnpackedFileHandle, PackedFileHandle, Root, RewardAddress, Iterations, Rounds},
	ar_bench_timer:record({wall}, fun dirty_test/4, [
		Permutation,
		fun baseline_pack_composite_chunks/5,
		Args,
		NumWorkers
	]),

	file:close(UnpackedFileHandle),
	file:close(PackedFileHandle).

run_dirty_repack_test(
		InputFileName, OutputFileName, WorkerFun,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	InputFileHandle = open_file(InputFileName, [read, binary]),
	OutputFileHandle = open_file(OutputFileName, [write, binary]),

	io:format("repacking..."),
	Args = {
		RandomXState,
		InputFileHandle, OutputFileHandle,
		Root, RewardAddress
	},
	ar_bench_timer:record({wall}, fun dirty_test/4, [
		Permutation,
		WorkerFun,
		Args,
		NumWorkers
	]),

	file:close(InputFileHandle),
	file:close(OutputFileHandle).

%% For now this just encrypts each chunk without adding the offset hash
dirty_test({TotalMegaBytes, _, _, _} = Permutation, WorkerFun, Args, NumWorkers) ->
	TotalBytes = TotalMegaBytes * ?MiB,
	%% Spin up NumWorkers threads each responsible for a fraction of the file
	WorkerSize = TotalBytes div NumWorkers,
	Workers = [spawn_monitor(
		fun() -> dirty_worker(
			N,
			Permutation, 
			WorkerFun,
			Args,
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

dirty_worker(WorkerID, Permutation, WorkerFun, Args, Offset, Size) ->
	ar_bench_timer:record({total, WorkerID}, WorkerFun, [
			WorkerID,
			Permutation,
			Args,
			Offset,
			Size
		]),
    exit(normal).

%% --------------------------------------------------------------------------------------------
%% Baseline Packing Test
%% --------------------------------------------------------------------------------------------
baseline_pack_chunks(_WorkerID, _Permutation, _Args, _Offset, Size) when Size =< 0 ->
	ok;
baseline_pack_chunks(WorkerID,
		{
			_, JIT, LargePages, HardwareAES
		} = Permutation,
		{
			RandomXState, UnpackedFileHandle, PackedFileHandle, 
			Root, RewardAddress
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, Key} = ar_packing_server:chunk_key({spora_2_6, RewardAddress}, Offset, Root),
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
	baseline_pack_chunks(WorkerID, Permutation, Args, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% Baseline Packing 2.8 Test
%% --------------------------------------------------------------------------------------------
% TODO merge with Packing Test with 1 extra parameter
% TODO diff other than 1

baseline_pack_composite_chunks(_WorkerID, _Permutation, _Args, _Offset, Size) when Size =< 0 ->
	ok;
baseline_pack_composite_chunks(WorkerID,
		{
			_, JIT, LargePages, HardwareAES
		} = Permutation,
		{
			RandomXState, UnpackedFileHandle, PackedFileHandle, 
			Root, RewardAddress, Iterations, Rounds
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{composite, Key} = ar_packing_server:chunk_key({composite, RewardAddress, Iterations}, Offset, Root),
	ReadResult = file:pread(UnpackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
        {ok, UnpackedChunk} ->
			{ok, PackedChunk} = ar_mine_randomx:randomx_encrypt_composite_chunk_nif(
				RandomXState, Key, UnpackedChunk,
				JIT, LargePages, HardwareAES,
				Rounds, Iterations, 32),
			file:pwrite(PackedFileHandle, Offset, PackedChunk),
			(Size - ChunkSize);
        eof ->
            0;
        {error, Reason} ->
            io:format("Error reading file: ~p~n", [Reason]),
			0
    end,
	baseline_pack_composite_chunks(WorkerID, Permutation, Args, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% Baseline Repacking Test
%% --------------------------------------------------------------------------------------------
baseline_repack_chunks(_WorkerID, _Permutation, _Args, _Offset, Size) when Size =< 0 ->
	ok;
baseline_repack_chunks(WorkerID,
		{
			_, JIT, LargePages, HardwareAES
		} = Permutation,
		{
			RandomXState, PackedFileHandle, RepackedFileHandle,
			Root, RewardAddress
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, Key} = ar_packing_server:chunk_key({spora_2_6, RewardAddress}, Offset, Root),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
        {ok, PackedChunk} ->
			{ok, UnpackedChunk} = ar_mine_randomx:randomx_decrypt_chunk_nif(
				RandomXState, Key, PackedChunk, ChunkSize, ?RANDOMX_PACKING_ROUNDS_2_6,
				JIT, LargePages, HardwareAES),
			{ok, RepackedChunk} =ar_mine_randomx:randomx_encrypt_chunk_nif(
				RandomXState, Key, UnpackedChunk, ?RANDOMX_PACKING_ROUNDS_2_6,
				JIT, LargePages, HardwareAES),	
			file:pwrite(RepackedFileHandle, Offset, RepackedChunk),
			(Size - ChunkSize);
        eof ->
            0;
        {error, Reason} ->
            io:format("Error reading file: ~p~n", [Reason]),
			0
    end,
	baseline_repack_chunks(WorkerID, Permutation, Args, Offset+ChunkSize, RemainingSize).

%% --------------------------------------------------------------------------------------------
%% NIF Repacking Test
%% --------------------------------------------------------------------------------------------
nif_repack_chunks(_WorkerID, _Permutation, _Args, _Offset, Size) when Size =< 0 ->
	ok;
nif_repack_chunks(WorkerID,
		{
			_, JIT, LargePages, HardwareAES
		} = Permutation,
		{
			RandomXState, PackedFileHandle, RepackedFileHandle,
			Root, RewardAddress
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{spora_2_6, Key} = ar_packing_server:chunk_key({spora_2_6, RewardAddress}, Offset, Root),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
        {ok, PackedChunk} -> % TODO
			{ok, RepackedChunk} = ar_mine_randomx:randomx_reencrypt_chunk_nif(
				RandomXState, Key, Key, PackedChunk, ChunkSize,
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
	nif_repack_chunks(WorkerID, Permutation, Args, Offset+ChunkSize, RemainingSize).

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
