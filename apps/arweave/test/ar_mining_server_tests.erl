-module(ar_mining_server_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(WEAVE_SIZE, (3 * ?PARTITION_SIZE)).
%% RECALL_RANGE_1 and SYNCED_RECALL_RANGE_2 must be different partitions so that different io
%% threads are used. 
%% ?RECALL_RANGE_1 is set so 1 chunk is synced and one is missing.
-define(RECALL_RANGE_1, (3*?PARTITION_SIZE-?DATA_CHUNK_SIZE)).
-define(SYNCED_RECALL_RANGE_2, ?PARTITION_SIZE).
-define(UNSYNCED_RECALL_RANGE_2, 0).


setup_all() ->
	[B0] = ar_weave:init([], ar_test_node:get_difficulty_for_invalid_hash(), ?WEAVE_SIZE),
	RewardAddr = ar_wallet:to_address(ar_wallet:new_keyfile()),
	{ok, Config} = application:get_env(arweave, config),
	%% We'll use partition 0 for any unsynced ranges.
	StorageModules = [
		{?PARTITION_SIZE, 1, {spora_2_6, RewardAddr}},
		{?PARTITION_SIZE, 2, {spora_2_6, RewardAddr}}
	],
	ar_test_node:start(B0, RewardAddr, Config, StorageModules).

cleanup_all(_) ->
	ok.

setup_one() ->
	ets:new(?MODULE, [set, public, named_table]).

cleanup_one(_) ->
	ets:delete(?MODULE).

chunk_cache_size_test_() ->
	{setup, fun setup_all/0, fun cleanup_all/1,
		{foreach, fun setup_one/0, fun cleanup_one/1,
		[
			{timeout, 30, fun test_h2_solution_chunk1_first/0},
			{timeout, 30, fun test_h2_solution_chunk2_first/0},
			{timeout, 30, fun test_h1_solution_h2_synced_chunk1_first/0},
			{timeout, 30, fun test_h1_solution_h2_synced_chunk2_first/0},
			{timeout, 30, fun test_h1_solution_h2_unsynced/0},
			{timeout, 30, fun test_no_solution_then_h2_solution/0},
			{timeout, 30, fun test_no_solution_then_h1_solution_h2_synced/0},
			{timeout, 30, fun test_no_solution_then_h1_solution_h2_unsynced/0}
		]}
    }.

test_h2_solution_chunk1_first() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:invalid_solution()],
		[ar_test_node:valid_solution()],
		[?SYNCED_RECALL_RANGE_2],
		[chunk1]
	).

test_h2_solution_chunk2_first() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:invalid_solution()],
		[ar_test_node:valid_solution()],
		[?SYNCED_RECALL_RANGE_2],
		[chunk2]
	).

test_h1_solution_h2_synced_chunk1_first() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:valid_solution()],
		[ar_test_node:invalid_solution()],
		[?SYNCED_RECALL_RANGE_2],
		[chunk1]
	).

test_h1_solution_h2_synced_chunk2_first() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:valid_solution()],
		[ar_test_node:invalid_solution()],
		[?SYNCED_RECALL_RANGE_2],
		[chunk2]
	).

test_h1_solution_h2_unsynced() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:valid_solution()],
		[],
		[?UNSYNCED_RECALL_RANGE_2],
		[chunk1]
	).

test_no_solution_then_h2_solution() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:invalid_solution()],
		[ar_test_node:invalid_solution(), ar_test_node:invalid_solution(),
			ar_test_node:valid_solution()],
		[?SYNCED_RECALL_RANGE_2],
		[chunk1]
	).

test_no_solution_then_h1_solution_h2_synced() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:invalid_solution(), ar_test_node:invalid_solution(),
			ar_test_node:valid_solution()],
		[ar_test_node:invalid_solution()],
		[?SYNCED_RECALL_RANGE_2],
		[chunk1]
	).

test_no_solution_then_h1_solution_h2_unsynced() ->
	do_test_chunk_cache_size_with_mocks(
		[ar_test_node:invalid_solution(), ar_test_node:invalid_solution(),
			ar_test_node:valid_solution()],
		[],
		[?UNSYNCED_RECALL_RANGE_2],
		[chunk1]
	).

do_test_chunk_cache_size_with_mocks(H1s, H2s, RecallRange2s, FirstChunks) ->
	Height = ar_node:get_height() + 1,
	ets:insert(?MODULE, {compute_h1, 0}),
	ets:insert(?MODULE, {compute_h2, 0}),
	ets:insert(?MODULE, {get_recall_range, 0}),
	ets:insert(?MODULE, {get_range, 0}),
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{
			ar_retarget, is_retarget_height,
			fun (_Height) ->
				false
			end
		},
		{
			ar_block, compute_h1,
			fun (_H0, _Nonce, _Chunk) ->
				Count = increment_mock_counter(compute_h1),
				Solution = get_mock_value(Count, H1s),
				{Solution, Solution}
			end
		},
		{
			ar_block, compute_h2,
			fun (_H0, _Nonce, _Chunk) ->
				Count = increment_mock_counter(compute_h2),
				Solution = get_mock_value(Count, H2s),
				{Solution, Solution}
			end
		},
		{
			ar_block, get_recall_range,
			fun (_H0, _PartitionNumber, _PartitionUpperBound) ->
				Count = increment_mock_counter(get_recall_range),
				RecallRange2 = get_mock_value(Count, RecallRange2s),
				{?RECALL_RANGE_1, RecallRange2}
			end
		},
		{
			ar_chunk_storage, get_range,
			fun (RangeStart, Size, StoreID) ->
				Count = increment_mock_counter(get_range),
				FirstChunk = get_mock_value(Count, FirstChunks),
				case FirstChunk == chunk1 andalso RangeStart /= ?RECALL_RANGE_1 of
					true ->
						timer:sleep(100);
					_ -> ok
				end,
				case FirstChunk == chunk2 andalso RangeStart == ?RECALL_RANGE_1 of
					true ->
						timer:sleep(100);
					_ -> ok
				end,
				meck:passthrough([RangeStart, Size, StoreID])
			end
		}
	]),
	Functions = Setup(),

	try
		ar_test_node:mine(),
		ar_test_node:wait_until_height(Height),
		%% wait until the mining has stopped
		?assert(ar_util:do_until(fun() -> get_chunk_cache_size() == 0 end, 200, 10000))
	after
		Cleanup(Functions)
	end.

get_chunk_cache_size() ->
	Pattern = {{chunk_cache_size, '$1'}, '_'}, % '$1' matches any PartitionNumber
    Entries = ets:match(?MODULE, Pattern),
    lists:foldl(
        fun(PartitionNumber, Acc) ->
			case ets:lookup(ar_mining_server, {chunk_cache_size, PartitionNumber}) of
				[] ->
					Acc;
				[{_, Size}] ->
					Acc + Size
			end
		end,
		0,
        Entries
    ).

get_mock_value(Index, Values) when Index < length(Values) ->
    lists:nth(Index, Values);
    
get_mock_value(_, Values) ->
    lists:last(Values).

increment_mock_counter(Mock) ->
	ets:update_counter(?MODULE, Mock, {2, 1}),
	[{_, Count}] = ets:lookup(?MODULE, Mock),
	Count.