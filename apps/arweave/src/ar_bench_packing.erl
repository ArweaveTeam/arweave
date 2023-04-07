-module(ar_bench_packing).

-export([run_benchmark_from_cli/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_vdf.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

run_benchmark_from_cli(Args) ->
	Test = list_to_atom(get_flag_value(Args, "test", "baseline_pack")),
	TotalMegaBytes = list_to_integer(get_flag_value(Args, "mb", "16")),
	JIT = list_to_integer(get_flag_value(Args, "jit", "1")),
	LargePages = list_to_integer(get_flag_value(Args, "large_pages", "1")),
	HardwareAES = list_to_integer(get_flag_value(Args, "hw_aes", "1")),
	VDF = list_to_integer(get_flag_value(Args, "vdf", "0")),
	run_benchmark(Test, TotalMegaBytes, JIT, LargePages, HardwareAES, VDF).

get_flag_value([], _, DefaultValue) ->
    DefaultValue;
get_flag_value([Flag | [Value | _Tail]], TargetFlag, _DefaultValue) when Flag == TargetFlag ->
    Value;
get_flag_value([_ | Tail], TargetFlag, DefaultValue) ->
    get_flag_value(Tail, TargetFlag, DefaultValue).

run_benchmark(Test, TotalMegaBytes, JIT, LargePages, HardwareAES, VDF) ->
	timer:sleep(3000),
	ets:new(offsets, [set, named_table, public]),
	EncodedRoot = <<"OIgTTxuEPklMR47Ho8VWnNr1Uh6TNjzxwIs38yuqBK0">>,
	Root = ar_util:decode(EncodedRoot),
	EncodedRewardAddress = <<"mvK6e65dcD6XNYDHUVxMa7-d6wVP535Ummtvb8OCUtQ">>,
	RewardAddress = ar_util:decode(EncodedRewardAddress),

	io:format("~nBenchmark settings:~n"),
	io:format("~12s: ~p~n", ["Test", Test]),
	io:format("~12s: ~p~n", ["Data (MB)", TotalMegaBytes]),
	io:format("~12s: ~p~n", ["JIT", JIT]),
	io:format("~12s: ~p~n", ["Large Pages", LargePages]),
	io:format("~12s: ~p~n", ["HW AES", HardwareAES]),
	io:format("~12s: ~p~n", ["VDF", VDF]),

	start_vdf(VDF),

	Permutation = {TotalMegaBytes, JIT, LargePages, HardwareAES},

	case Test of
		input ->
			generate_input(TotalMegaBytes, Root, RewardAddress);
		baseline_pack ->
			run_dirty_benchmark(baseline_pack, Permutation, Root, RewardAddress);
		baseline_repack_2_5 ->
			run_dirty_benchmark(baseline_repack_2_5, Permutation, Root, RewardAddress);
		baseline_repack_2_6 ->
			run_dirty_benchmark(baseline_repack_2_6, Permutation, Root, RewardAddress);
		nif_repack_2_5 ->
			run_dirty_benchmark(nif_repack_2_5, Permutation, Root, RewardAddress);
		nif_repack_2_6 ->
			run_dirty_benchmark(nif_repack_2_6, Permutation, Root, RewardAddress);
		_ ->
			?LOG_ERROR(
				"Unknown test: ~p. Valid values: input, baseline_pack, " ++
				"baseline_repack_2_5, baseline_repack_2_6, " ++
				"nif_repack_2_5, nif_repack_2_6", [Test])
	end,
	Init = ar_bench_timer:get_total({init}) / 1000000,
	Total = ar_bench_timer:get_total({wall}) / 1000000,

	{ok, File} = file:open("benchmark.results.csv", [append]),

	%% Write the CSV string to the file
	Output = io_lib:format("~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p, ~p~n", [
		erlang:system_time() div 1000000000,
		Test, TotalMegaBytes, JIT, LargePages, HardwareAES, VDF,
		Init, Total]),
	
	file:write(File, Output),
	io:format("~n"),
	io:format(Output),

	%% Close the file
	file:close(File).

%% --------------------------------------------------------------------------------------------
%% Write Input files
%% --------------------------------------------------------------------------------------------
output_filename(Test, Permutation) ->
	%% convert the Permutation tuple to a list of strings so that we can join them with a dot
	StringList = lists:map(fun(E) -> integer_to_list(E) end, tuple_to_list(Permutation)),
	io_lib:format("benchmark.output.~s.~p", [string:join(StringList, "."), Test]).
unpacked_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.unpacked", [TotalMegaBytes]).
spora_2_5_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.spora_2_5", [TotalMegaBytes]).
spora_2_6_filename(TotalMegaBytes) ->
	io_lib:format("benchmark.input.~p.spora_2_6", [TotalMegaBytes]).

generate_input(TotalMegaBytes, Root, RewardAddress) ->
	TotalBytes = TotalMegaBytes * 1024 * 1024,
	io:format("~ngenerating input files...~n~n"),

	UnpackedFilename = unpacked_filename(TotalMegaBytes),
	io:format("~s~n", [UnpackedFilename]),
	ar_bench_timer:record({wall},
		fun write_random_data/2, [UnpackedFilename, TotalBytes]),
	{ok, UnpackedFileHandle} = file:open(UnpackedFilename, [read, binary]),

	NumWorkers = erlang:system_info(dirty_cpu_schedulers_online),
	{ok, RandomXState} = ar_bench_timer:record({init}, fun ar_mine_randomx:init_fast_nif/4, [?RANDOMX_PACKING_KEY, 1, 1, NumWorkers]),

	Spora25Filename = spora_2_5_filename(TotalMegaBytes),
	io:format("~s", [Spora25Filename]),
	
	{ok, Spora25FileHandle} = file:open(Spora25Filename, [write, binary]),
	ar_bench_timer:record({wall},
		fun dirty_test/4, [
			{TotalMegaBytes, 1, 1, 1},
			fun baseline_pack_chunks/5,
			{RandomXState, UnpackedFileHandle, Spora25FileHandle, Root, RewardAddress, spora_2_5},
			NumWorkers
		]),
	file:close(Spora25FileHandle),

	Spora26Filename = spora_2_6_filename(TotalMegaBytes),
	io:format("~s", [Spora26Filename]),
	{ok, Spora26FileHandle} = file:open(Spora26Filename, [write, binary]),
	ar_bench_timer:record({wall},
			fun dirty_test/4, [
				{TotalMegaBytes, 1, 1, 1},
				fun baseline_pack_chunks/5,
				{RandomXState, UnpackedFileHandle, Spora26FileHandle, Root, RewardAddress, spora_2_6},
				NumWorkers
			]),
	file:close(Spora26FileHandle),

	file:close(UnpackedFileHandle),
	io:format("~ndone~n").

write_random_data(UnpackedFilename, TotalBytes) ->
    {ok, File} = file:open(UnpackedFilename, [write, binary, raw]),
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

run_dirty_benchmark(Test, {_, JIT, LargePages, _} = Permutation, Root, RewardAddress) ->
	io:format("~ninit~n"),
	NumWorkers = erlang:system_info(dirty_cpu_schedulers_online),
	{ok, RandomXState} = ar_bench_timer:record({init}, fun ar_mine_randomx:init_fast_nif/4, [?RANDOMX_PACKING_KEY, JIT, LargePages, NumWorkers]),

	run_dirty_test(Test, Permutation, RandomXState, Root, RewardAddress, NumWorkers).

run_dirty_test(baseline_pack, Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	run_dirty_pack_test(baseline_pack, Permutation, RandomXState, Root, RewardAddress, NumWorkers);
run_dirty_test(baseline_repack_2_5, {TotalMegaBytes, _, _, _} = Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	run_dirty_repack_test(
		spora_2_5_filename(TotalMegaBytes),
		output_filename(baseline_repack_2_5, Permutation),
		spora_2_5, spora_2_6, fun baseline_repack_chunks/5,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers);
run_dirty_test(baseline_repack_2_6, {TotalMegaBytes, _, _, _} = Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	run_dirty_repack_test(
		spora_2_6_filename(TotalMegaBytes),
		output_filename(baseline_repack_2_6, Permutation),
		spora_2_6, spora_2_6, fun baseline_repack_chunks/5,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers);
run_dirty_test(nif_repack_2_5, {TotalMegaBytes, _, _, _} = Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	run_dirty_repack_test(
		spora_2_5_filename(TotalMegaBytes),
		output_filename(nif_repack_2_5, Permutation),
		spora_2_5, spora_2_6, fun nif_repack_chunks/5,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers);
run_dirty_test(nif_repack_2_6, {TotalMegaBytes, _, _, _} = Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	run_dirty_repack_test(
		spora_2_6_filename(TotalMegaBytes),
		output_filename(nif_repack_2_6, Permutation),
		spora_2_6, spora_2_6, fun nif_repack_chunks/5,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers).

run_dirty_pack_test(baseline_pack, {TotalMegaBytes, _, _, _} = Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	UnpackedFilename = unpacked_filename(TotalMegaBytes),
	PackedFilename = output_filename(baseline_pack, Permutation),
	{ok, UnpackedFileHandle} = file:open(UnpackedFilename, [read, binary]),
	{ok, PackedFileHandle} = file:open(PackedFilename, [write, binary]),

	io:format("packing"),
	Args = {RandomXState, UnpackedFileHandle, PackedFileHandle, Root, RewardAddress, spora_2_6},
	ar_bench_timer:record({wall}, fun dirty_test/4, [
		Permutation,
		fun baseline_pack_chunks/5,
		Args,
		NumWorkers
	]),

	file:close(UnpackedFileHandle),
	file:close(PackedFileHandle).

run_dirty_repack_test(
		InputFileName, OutputFileName, InputPackingType, OutputPackingType, WorkerFun,
		Permutation, RandomXState, Root, RewardAddress, NumWorkers) ->
	{ok, InputFileHandle} = file:open(InputFileName, [read, binary]),
	{ok, OutputFileHandle} = file:open(OutputFileName, [write, binary]),

	io:format("repacking"),
	Args = {
		RandomXState,
		InputFileHandle, OutputFileHandle,
		Root, RewardAddress,
		InputPackingType, OutputPackingType
	},
	ar_bench_timer:record({wall}, fun dirty_test/4, [
		Permutation,
		WorkerFun,
		Args,
		NumWorkers
	]),

	io:format("~12s: ~8.2f~n", ["init", ar_bench_timer:get_total({init}) / 1000000]),
	io:format("~12s: ~8.2f~n", ["wall", ar_bench_timer:get_total({wall}) / 1000000]),
	file:close(InputFileHandle),
	file:close(OutputFileHandle).

%% For now this just encrypts each chunk without adding the offset hash
dirty_test({TotalMegaBytes, _, _, _} = Permutation, WorkerFun, Args, NumWorkers) ->
	TotalBytes = TotalMegaBytes * 1024 * 1024,
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
	process_flag(trap_exit, true),
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
			Root, RewardAddress, PackingType
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{Key, PackingRounds} = get_packing_args(PackingType, Offset, Root, RewardAddress),
	ReadResult = file:pread(UnpackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
        {ok, UnpackedChunk} ->
			{ok, PackedChunk} = ar_mine_randomx:randomx_encrypt_chunk_nif(RandomXState, Key, UnpackedChunk, PackingRounds, JIT, LargePages, HardwareAES),
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
			Root, RewardAddress, UnpackingType, RepackingType
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{UnpackKey, UnpackingRounds} = get_packing_args(UnpackingType, Offset, Root, RewardAddress),
	{RepackKey, RepackingRounds} = get_packing_args(RepackingType, Offset, Root, RewardAddress),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
        {ok, PackedChunk} ->
			{ok, UnpackedChunk} = ar_mine_randomx:randomx_decrypt_chunk_nif(RandomXState, UnpackKey, PackedChunk, ChunkSize, UnpackingRounds, JIT, LargePages, HardwareAES),
			{ok, RepackedChunk} =ar_mine_randomx:randomx_encrypt_chunk_nif(RandomXState, RepackKey, UnpackedChunk, RepackingRounds, JIT, LargePages, HardwareAES),	
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
			Root, RewardAddress, UnpackingType, RepackingType
		} = Args,
		Offset, Size) ->
	ChunkSize = min(Size, ?DATA_CHUNK_SIZE),
	{UnpackKey, UnpackingRounds} = get_packing_args(UnpackingType, Offset, Root, RewardAddress),
	{RepackKey, RepackingRounds} = get_packing_args(RepackingType, Offset, Root, RewardAddress),
	ReadResult = file:pread(PackedFileHandle, Offset, ChunkSize),
	RemainingSize = case ReadResult of
        {ok, PackedChunk} ->
			{ok, RepackedChunk} = ar_mine_randomx:randomx_reencrypt_chunk_nif(RandomXState, UnpackKey, RepackKey, PackedChunk, ChunkSize, UnpackingRounds, RepackingRounds, JIT, LargePages, HardwareAES),
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
get_packing_args(spora_2_5, Offset, Root, _RewardAddress) ->
	{
		ar_packing_server:chunk_key_2_5(Offset, Root),
		?RANDOMX_PACKING_ROUNDS
	};
get_packing_args(spora_2_6, Offset, Root, RewardAddress) ->
	{
		ar_packing_server:chunk_key_2_6(Offset, Root, RewardAddress),
		?RANDOMX_PACKING_ROUNDS_2_6
	}.

%% --------------------------------------------------------------------------------------------
%% Pipelined
%% --------------------------------------------------------------------------------------------

% run_pipelined_benchmark({TotalBytes, JIT, LargePages, HardwareAES}, UnpackedFilename, PackedFilename, Key) ->
% 	io:format("~npipelined init~n"),
% 	ar_bench_timer:record({pipelined, init}, fun ar_packing_pipeline:init/3, [JIT, LargePages, HardwareAES]),

% 	{ok, UnpackedFileHandle} = file:open(UnpackedFilename, [read, binary]),
% 	{ok, PackedFileHandle} = file:open(PackedFilename, [write, binary]),

% 	Root = crypto:strong_rand_bytes(32),
% 	RewardAddress = crypto:strong_rand_bytes(32),

% 	ar_bench_timer:record({pipelined, wall}, fun run_pipelined_test/5, [UnpackedFileHandle, PackedFileHandle, RewardAddress, Root, TotalBytes]),

% 	file:close(UnpackedFileHandle),
% 	file:close(PackedFileHandle).

% run_pipelined_test(UnpackedFileHandle, PackedFileHandle, RewardAddress, Root, TotalBytes) ->
% 	% spawn(fun() -> pack_pipelined_chunks(UnpackedFileHandle, RewardAddress, Root, 0, TotalBytes) end),
% 	pack_pipelined_chunks(UnpackedFileHandle, RewardAddress, Root, 0, TotalBytes),
% 	io:format("~npulling"),
% 	write_pipelined_chunks(PackedFileHandle, TotalBytes div ?DATA_CHUNK_SIZE),
% 	io:format("~n").

% pack_pipelined_chunks(_UnpackedFileHandle, _RewardAddress, _Root, _Offset, Size) when Size =< 0 ->
% 	ok;
% pack_pipelined_chunks(UnpackedFileHandle, RewardAddress, Root, Offset, Size) ->
% 	ReadResult = ar_bench_timer:record({pipelined, read, Offset}, fun file:pread/3, [UnpackedFileHandle, Offset, min(Size, ?DATA_CHUNK_SIZE)]),
% 	RemainingSize = case ReadResult of
%         {ok, UnpackedChunk} ->
% 			TaskID = ar_packing_pipeline:pack_push({spora_2_6, RewardAddress}, Offset, Root, UnpackedChunk),
% 			ar_bench_timer:start({pipelined, pack, Offset}),
% 			ets:insert(offsets, {TaskID, Offset}),
% 			(Size - ?DATA_CHUNK_SIZE);
%         eof ->
%             0;
%         {error, Reason} ->
%             io:format("Error reading file: ~p~n", [Reason]),
% 			0
%     end,
% 	pack_pipelined_chunks(UnpackedFileHandle, RewardAddress, Root, Offset+?DATA_CHUNK_SIZE, RemainingSize).

% write_pipelined_chunks(_PackedFileHandle, ChunksRemaining) when ChunksRemaining =< 0 ->
% 	ok;
% write_pipelined_chunks(PackedFileHandle, ChunksRemaining) ->
% 	case ar_packing_pipeline:task_pull() of
% 		{ok, TaskID, Chunk} ->
% 			[{_, Offset}] = ets:lookup(offsets, TaskID),
% 			io:format("."),
% 			ar_bench_timer:stop({pipelined, pack, Offset}),
% 			ar_bench_timer:record({pipelined, write, Offset}, fun file:pwrite/3, [PackedFileHandle, Offset, Chunk]),
% 			write_pipelined_chunks(PackedFileHandle, ChunksRemaining-1);
% 		Error ->
% 			timer:sleep(100),
% 			write_pipelined_chunks(PackedFileHandle, ChunksRemaining)
% 	end.