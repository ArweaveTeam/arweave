-module(ar_mining_io_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WEAVE_SIZE, trunc(2.5 * ?PARTITION_SIZE)).

chunks_read(_Worker, WhichChunk, Candidate, RangeStart, ChunkOffsets) ->
	ets:insert(?MODULE, {WhichChunk, Candidate, RangeStart, ChunkOffsets}).

setup_all() ->
	[B0] = ar_weave:init([], 1, ?WEAVE_SIZE),
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	{ok, Config} = application:get_env(arweave, config),
	StorageModules = lists:flatten(
		[[{?PARTITION_SIZE, N, {spora_2_6, RewardAddr}}] || N <- lists:seq(0, 8)]),
	ar_test_node:start(B0, RewardAddr, Config, StorageModules),
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_mining_worker, chunks_read, fun chunks_read/5}
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
			{timeout, 30, fun test_read_recall_range/0},
			{timeout, 30, fun test_partitions/0}
		]}
    }.

test_read_recall_range() ->
	Candidate = default_candidate(),
	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, self(), Candidate, 0)),
	wait_for_io(1),
	[Chunk1, Chunk2] = get_recall_chunks(),
	assert_chunks_read([{chunk1, Candidate, 0, [
		{?DATA_CHUNK_SIZE, Chunk1},
		{?DATA_CHUNK_SIZE*2, Chunk2}]}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, self(), Candidate, ?DATA_CHUNK_SIZE div 2)),
	wait_for_io(1),
	[Chunk1, Chunk2, Chunk3] = get_recall_chunks(),
	assert_chunks_read([{chunk1, Candidate, ?DATA_CHUNK_SIZE div 2, [
		{?DATA_CHUNK_SIZE, Chunk1},
		{?DATA_CHUNK_SIZE*2, Chunk2},
		{?DATA_CHUNK_SIZE*3, Chunk3}]}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, self(), Candidate, ?DATA_CHUNK_SIZE)),
	wait_for_io(1),
	[Chunk2, Chunk3] = get_recall_chunks(),
	assert_chunks_read([{chunk1, Candidate, ?DATA_CHUNK_SIZE, [
		{?DATA_CHUNK_SIZE*2, Chunk2},
		{?DATA_CHUNK_SIZE*3, Chunk3}]}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk2, self(), Candidate,
		?PARTITION_SIZE - ?DATA_CHUNK_SIZE)),
	wait_for_io(1),
	[Chunk4, Chunk5] = get_recall_chunks(),
	assert_chunks_read([{chunk2, Candidate, ?PARTITION_SIZE - ?DATA_CHUNK_SIZE, [
		{?PARTITION_SIZE, Chunk4},
		{?PARTITION_SIZE + ?DATA_CHUNK_SIZE, Chunk5}]}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk2, self(), Candidate, ?PARTITION_SIZE)),
	wait_for_io(1),
	[Chunk5, Chunk6] = get_recall_chunks(),
	assert_chunks_read([{chunk2, Candidate, ?PARTITION_SIZE, [
		{?PARTITION_SIZE + ?DATA_CHUNK_SIZE, Chunk5},
		{?PARTITION_SIZE + (2*?DATA_CHUNK_SIZE), Chunk6}]}]),

	?assertEqual(true, ar_mining_io:read_recall_range(chunk1, self(), Candidate,
		?WEAVE_SIZE - ?DATA_CHUNK_SIZE)),
	wait_for_io(1),
	[Chunk7] = get_recall_chunks(),
	assert_chunks_read([{chunk1, Candidate, ?WEAVE_SIZE - ?DATA_CHUNK_SIZE, [
		{?WEAVE_SIZE, Chunk7}]}]),

	?assertEqual(false, ar_mining_io:read_recall_range(chunk1, self(), Candidate, ?WEAVE_SIZE)).

test_partitions() ->
	Candidate = default_candidate(),
	MiningAddress = Candidate#mining_candidate.mining_address,

	ar_mining_io:set_largest_seen_upper_bound(0),
	?assertEqual([], ar_mining_io:get_partitions()),

	ar_mining_io:set_largest_seen_upper_bound(?PARTITION_SIZE),
	?assertEqual([], ar_mining_io:get_partitions(0)),
	?assertEqual([
			{0, MiningAddress, 0}],
		ar_mining_io:get_partitions()),

	ar_mining_io:set_largest_seen_upper_bound(trunc(2.5 * ?PARTITION_SIZE)),
	?assertEqual([
			{0, MiningAddress, 0}],
		ar_mining_io:get_partitions(?PARTITION_SIZE)),
	?assertEqual([
			{0, MiningAddress, 0},
			{1, MiningAddress, 0}],
		ar_mining_io:get_partitions()),

	ar_mining_io:set_largest_seen_upper_bound(trunc(5 * ?PARTITION_SIZE)),
	?assertEqual([
			{0, MiningAddress, 0},
			{1, MiningAddress, 0}],
		ar_mining_io:get_partitions(trunc(2.5 * ?PARTITION_SIZE))),
	?assertEqual([
			{0, MiningAddress, 0},
			{1, MiningAddress, 0},
			{2, MiningAddress, 0},
			{3, MiningAddress, 0},
			{4, MiningAddress, 0}],
		ar_mining_io:get_partitions()),
	?assertEqual([
			{0, MiningAddress, 0},
			{1, MiningAddress, 0},
			{2, MiningAddress, 0},
			{3, MiningAddress, 0},
			{4, MiningAddress, 0}],
		ar_mining_io:get_partitions(trunc(5 * ?PARTITION_SIZE))).

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

get_recall_chunks() ->
	case ets:tab2list(?MODULE) of
		[] -> [];
		[{_WhichChunk, _Candidate, _RangeStart, ChunkOffsets}] -> 
			lists:map(
				fun({_Offset, Chunk}) -> 
					Chunk
				end,
				ChunkOffsets
			)
	end.

assert_chunks_read(ExpectedChunks) ->
	?assertEqual(ExpectedChunks, ets:tab2list(?MODULE)),
	ets:delete_all_objects(?MODULE).
