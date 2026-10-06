-module(arweave_entropy_generation).
-export([generate/4]).
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_entropy/include/arweave_entropy_deps.hrl").
-define(ENTROPY_GENERATION_STATS_WINDOW_MS, 1000 * 60 * 30).

generate(RewardAddr, BucketEndOffset, SubChunkStartOffset) ->
    generate(RewardAddr, BucketEndOffset, SubChunkStartOffset, true).

generate(RewardAddr, BucketEndOffset, SubChunkStartOffset, false) ->
    Key = arweave_lib_replica_2_9:get_entropy_key(RewardAddr, BucketEndOffset, SubChunkStartOffset),
    do_generate_entropy(RewardAddr, Key);
generate(RewardAddr, BucketEndOffset, SubChunkStartOffset, true) ->
    Key = arweave_lib_replica_2_9:get_entropy_key(RewardAddr, BucketEndOffset, SubChunkStartOffset),
    Partition = (BucketEndOffset div arweave_lib_constants:partition_size()),

    entropy_generation_lock(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset),
    case arweave_entropy_cache:get(Key) of
        {ok, Entropy} ->
            ?DEP(metrics):counter_inc(replica_2_9_entropy_stats, [Partition, cache_hit]),
            entropy_generation_release(Key),
            Entropy;
        not_found ->
            ?DEP(metrics):counter_inc(replica_2_9_entropy_stats, [Partition, cache_miss]),
            Entropy = do_generate_entropy(RewardAddr, Key),
            update_entropy_generation_stats(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset),
            EntropyCacheSizeMb = ?DEP(config):get([packing, entropy, cache_size]),
            MaxSize = EntropyCacheSizeMb * ?MiB,
            arweave_entropy_cache:clean_up_space(?REPLICA_2_9_ENTROPY_SIZE, MaxSize),
            arweave_entropy_cache:put(Key, Entropy, ?REPLICA_2_9_ENTROPY_SIZE),
            entropy_generation_release(Key),
            Entropy
    end.


do_generate_entropy(RewardAddr, Key) ->
    PackingState = ?DEP(packing):get_packing_state(),
    RandomXState = get_randomx_state_by_packing({replica_2_9, RewardAddr}, PackingState),
    Entropy = ?DEP(randomx):randomx_generate(RandomXState, Key),
    %% Primarily needed for testing where the entropy generated exceeds the entropy
    %% needed for tests.
    binary_part(Entropy, 0, ?REPLICA_2_9_ENTROPY_SIZE).


get_randomx_state_by_packing({replica_2_9, _}, {_, _, RandomXState}) ->
    RandomXState;
get_randomx_state_by_packing({spora_2_6, _}, {RandomXState, _, _}) ->
    RandomXState;
get_randomx_state_by_packing(spora_2_5, {RandomXState, _, _}) ->
    RandomXState.


entropy_generation_lock(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset) ->
    case ets:insert_new(ar_packing_server, {{entropy_generation_lock, Key}}) of
        true ->
            ok;
        false ->
            timer:sleep(100),
            entropy_generation_lock(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset)
    end.


entropy_generation_release(Key) ->
    ets:delete(ar_packing_server, {entropy_generation_lock, Key}).


update_entropy_generation_stats(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset) ->
    Tab = entropy_generation_stats,
    Time = erlang:monotonic_time(millisecond),
    ets:update_counter(Tab, Key, {2, 1}, {Key, 0, Time}),
    ?DEP(metrics):counter_inc(replica_2_9_entropy_generated, ?REPLICA_2_9_ENTROPY_SIZE),
    maybe_report_redundant_entropy_generation(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset),
    remove_outdated_entropy_generation_stats().


maybe_report_redundant_entropy_generation(Key, RewardAddr, BucketEndOffset, SubChunkStartOffset) ->
    Tab = entropy_generation_stats,
    Now = erlang:monotonic_time(millisecond),
    [{_, Count, Time}] = ets:lookup(Tab, Key),
    case Count > 1 of
        true ->
            Partition = (BucketEndOffset div arweave_lib_constants:partition_size()),
            ?DEP(metrics):counter_inc(replica_2_9_entropy_stats, [Partition, redundant]),
            ?LOG_DEBUG([{event, possibly_redundant_entropy_generation},
                        {reward_addr, arweave_lib_util:encode(RewardAddr)},
                        {key, arweave_lib_util:encode(Key)},
                        {bucket_end_offset, BucketEndOffset},
                        {sub_chunk_start_offset, SubChunkStartOffset},
                        {count, Count},
                        {seconds_since_first_generation, (Now - Time) / 1_000},
                        {avg_per_second, Count / ((Now - Time) / 1_000)}]);
        false ->
            ok
    end.


remove_outdated_entropy_generation_stats() ->
    Tab = entropy_generation_stats,
    Cursor = ets:first(Tab),
    Now = erlang:monotonic_time(millisecond),
    case ets:lookup(Tab, Cursor) of
        [{_, _, Time}] when Time < Now - ?ENTROPY_GENERATION_STATS_WINDOW_MS ->
            ets:delete(Tab, Cursor),
            remove_outdated_entropy_generation_stats();
        _ ->
            ok
    end.
