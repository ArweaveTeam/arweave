-module(ar_mining_io_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

weave_size() ->
	ar_util:ceil_int(trunc(5.5 * ?PARTITION_SIZE), ?DATA_CHUNK_SIZE).

recall_chunk(WhichChunk, Chunk, Nonce, Candidate) ->
	ets:insert(?MODULE, {WhichChunk, Nonce, Chunk, Candidate}).

setup_all() ->
	[B0] = ar_weave:init([], 1, weave_size()),
	ar_test_node:start(B0),
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_mining_server, recall_chunk, fun recall_chunk/4}
	]),
	Functions = Setup(),
	{Cleanup, Functions}.

cleanup_all({Cleanup, Functions}) ->
	Cleanup(Functions).

setup_one() ->
	ets:new(?MODULE, [named_table, duplicate_bag, public]).

cleanup_one(_) ->
	ets:delete(?MODULE).

read_recall_range_test_() ->
	{setup, fun setup_all/0, fun cleanup_all/1,
		{foreach, fun setup_one/0, fun cleanup_one/1,
		[
			{timeout, 180, fun test_read_recall_range/0},
			{timeout, 180, fun test_io_threads/0},
			{timeout, 30, fun test_partitions/0},
			{timeout, 180, fun test_mining_session/0}
		]}
    }.

test_read_recall_range() ->
	Candidate = default_candidate(),
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, 0)),
	wait_for_io(2),
	[Chunk1, Chunk2] = get_recall_chunks(),
	assert_recall_chunks([{chunk1, 0, Chunk1, Candidate}, {chunk1, 1, Chunk2, Candidate}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, ?DATA_CHUNK_SIZE div 2)),
	wait_for_io(2),
	assert_recall_chunks([{chunk1, 0, Chunk1, Candidate}, {chunk1, 1, Chunk2, Candidate}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, ?DATA_CHUNK_SIZE)),
	wait_for_io(2),
	[Chunk2, Chunk3] = get_recall_chunks(),
	assert_recall_chunks([{chunk1, 0, Chunk2, Candidate}, {chunk1, 1, Chunk3, Candidate}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk2, Candidate,
		?PARTITION_SIZE - ?DATA_CHUNK_SIZE)),
	wait_for_io(2),
	[Chunk4, Chunk5] = get_recall_chunks(),
	assert_recall_chunks([{chunk2, 0, Chunk4, Candidate}, {chunk2, 1, Chunk5, Candidate}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk2, Candidate, ?PARTITION_SIZE)),
	wait_for_io(2),
	[Chunk5, Chunk6] = get_recall_chunks(),
	assert_recall_chunks([{chunk2, 0, Chunk5, Candidate}, {chunk2, 1, Chunk6, Candidate}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate,
		weave_size() - ?DATA_CHUNK_SIZE)),
	wait_for_io(2),
	[Chunk7, _Chunk8] = get_recall_chunks(),
	assert_recall_chunks([{chunk1, 0, Chunk7, Candidate}, {skipped, 1, undefined, Candidate}]),

	?assertEqual(false, ar_mining_io:read_recall_range(chunk1, Candidate, weave_size())).

test_io_threads() ->
	Candidate = default_candidate(),

	%% default configuration has 9 storage modules, even though the weave size only covers the
	%% first 3.
	?assertEqual(9, ar_mining_io:get_thread_count()),

	%% Assert that ar_mining_io uses multiple threads when reading from different partitions.
	%% We do this indirectly by comparing the time to read repeatedly from one partition vs.
	%% the time to read from multiple partitions.
	Iterations = 3000,

    SingleThreadStart = os:system_time(microsecond),
    lists:foreach(
		fun(_) ->
			?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, 0))
		end,
		lists:seq(1, Iterations)),
	wait_for_io(2*Iterations),
    SingleThreadTime = os:system_time(microsecond) - SingleThreadStart,
	ets:delete_all_objects(?MODULE),

	MultiThreadStart = os:system_time(microsecond),
    lists:foreach(
		fun(I) ->
			Offset = (I * 2 * ?DATA_CHUNK_SIZE) rem weave_size(),
			?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, Offset))
		end,
		lists:seq(1, Iterations)),
	wait_for_io(2*Iterations),
    MultiThreadTime = os:system_time(microsecond) - MultiThreadStart,
	ets:delete_all_objects(?MODULE),
	?assert(SingleThreadTime > 1.5 * MultiThreadTime,
		lists:flatten(io_lib:format(
			"Multi-thread time (~p) not 1.5x faster than single-thread time (~p)",
			[MultiThreadTime, SingleThreadTime]))).	

test_partitions() ->
	Candidate = default_candidate(),
	MiningAddress = Candidate#mining_candidate.mining_address,
	Packing = {spora_2_6, MiningAddress},

	?assertEqual([
			{0, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 0, Packing})}],
		ar_mining_io:get_partitions(0)),

	?assertEqual([
			{0, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 0, Packing})}],
		ar_mining_io:get_partitions(?PARTITION_SIZE)),

	?assertEqual([
			{0, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 0, Packing})},
			{1, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 1, Packing})},
			{2, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 2, Packing})}],
		ar_mining_io:get_partitions(trunc(2.5 * ?PARTITION_SIZE))),

	?assertEqual([
			{0, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 0, Packing})},
			{1, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 1, Packing})},
			{2, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 2, Packing})},
			{3, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 3, Packing})},
			{4, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 4, Packing})},
			{5, MiningAddress, ar_storage_module:id({?PARTITION_SIZE, 5, Packing})}],
		ar_mining_io:get_partitions(weave_size())).

test_mining_session() ->
	Candidate = default_candidate(),
	SessionRef = make_ref(),

	%% mining session: not set, candidate session: not set
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, 0)),
	wait_for_io(2),
	[Chunk1, Chunk2] = get_recall_chunks(),
	assert_recall_chunks([{chunk1, 0, Chunk1, Candidate}, {chunk1, 1, Chunk2, Candidate}]),

	%% mining session: not set, candidate session: set
	Candidate2 = Candidate#mining_candidate{ session_ref = SessionRef },
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate2, 0)),
	assert_no_io(),

	ar_mining_io:reset(SessionRef),

	%% mining session: set, candidate session: set
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate2, 0)),
	wait_for_io(2),
	[Chunk1, Chunk2] = get_recall_chunks(),
	assert_recall_chunks([{chunk1, 0, Chunk1, Candidate2}, {chunk1, 1, Chunk2, Candidate2}]),

	%% mining session: set, candidate session: not set
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate, 0)),
	wait_for_io(2),
	[Chunk1, Chunk2] = get_recall_chunks(),
	assert_recall_chunks([{chunk1, 0, Chunk1, Candidate}, {chunk1, 1, Chunk2, Candidate}]),

	%% mining session: set, candidate session: set different
	Candidate3 = Candidate#mining_candidate{ session_ref = make_ref() },
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, Candidate3, 0)),
	assert_no_io().

default_candidate() ->
	{ok, Config} = application:get_env(arweave, config),
	MiningAddr = Config#config.mining_addr,
	#mining_candidate{
		mining_address = MiningAddr
	}.

wait_for_io(NumChunks) ->
	Result = ar_util:do_until(
		fun() ->
			NumChunks == length(ets:tab2list(?MODULE))
		end,
		100,
		60000),
	?assertEqual(true, Result, "Timeout while waiting to read chunks").

assert_no_io() ->
	Result = ar_util:do_until(
		fun() ->
			length(ets:tab2list(?MODULE)) > 0
		end,
		100,
		5000),
	?assertEqual({error, timeout}, Result, "Unexpectedly read chunks").

get_recall_chunks() ->
	lists:map(fun({_, _, Chunk, _}) -> Chunk end, lists:sort(ets:tab2list(?MODULE))).
	
assert_recall_chunks(ExpectedChunks) ->
	?assertEqual(lists:sort(ExpectedChunks), lists:sort(ets:tab2list(?MODULE))),
	ets:delete_all_objects(?MODULE).