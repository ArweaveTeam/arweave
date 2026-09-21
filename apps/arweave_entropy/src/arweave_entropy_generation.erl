%%% Entropy generation, cache reuse and footprint slicing.
%%%
%%% A whole 8 MiB entropy stays inside this module and the footprint
%%% preparation path. Everything that packs or unpacks a chunk asks for the
%%% 8 KiB slice it needs through generate_slice/3, so no long-lived process
%%% ever holds references to cached entropies; a heap that references a few
%%% of them at once raises the VM's binary garbage collection threshold and
%%% then carries hundreds of MiB of evicted entropies between collections.
-module(arweave_entropy_generation).
-export([
    generate/4,
    generate_slice/3,
    generate_chunk/2,
    generate_entropies/2, generate_entropies/3,
    generate_entropy_keys/2,
    entropy_offsets/2,
    map_entropies/8
]).
-include_lib("arweave_constants/include/arweave_constants.hrl").
-include_lib("kernel/include/logger.hrl").
%% Retain generation history for thirty minutes to measure repeated work.
-define(ENTROPY_GENERATION_STATS_WINDOW_MS, 1000 * 60 * 30).

%% @doc Return the chunk's slice of the cached 2.9 entropy as a fresh binary.
generate_slice(RewardAddr, BucketEndOffset, SubChunkStartOffset) ->
    Entropy = generate(RewardAddr, BucketEndOffset, SubChunkStartOffset, true),
    Index = arweave_entropy_deps:get_slice_index(BucketEndOffset),
    binary:copy(
        binary:part(Entropy, Index * ?SUB_CHUNK_SIZE, ?SUB_CHUNK_SIZE)
    ).

%% @doc Generate the 2.9 entropy, reusing the cache when CacheEntropy is true.
generate(RewardAddr, BucketEndOffset, SubChunkStartOffset, false) ->
    Key = arweave_entropy_deps:get_entropy_key(
        RewardAddr, BucketEndOffset, SubChunkStartOffset
    ),
    arweave_entropy_deps:generate_entropy(RewardAddr, Key);
generate(RewardAddr, BucketEndOffset, SubChunkStartOffset, true) ->
    Key = arweave_entropy_deps:get_entropy_key(
        RewardAddr, BucketEndOffset, SubChunkStartOffset
    ),
    Partition = (BucketEndOffset div arweave_constants:partition_size()),

    entropy_generation_lock(Key),
    try
        case arweave_entropy_cache:get(Key) of
            {ok, Entropy} ->
                arweave_metrics:counter_inc(replica_2_9_entropy_stats, [
                    Partition, cache_hit
                ]),
                Entropy;
            not_found ->
                arweave_metrics:counter_inc(replica_2_9_entropy_stats, [
                    Partition, cache_miss
                ]),
                Entropy = arweave_entropy_deps:generate_entropy(
                    RewardAddr, Key
                ),
                update_entropy_generation_stats(
                    Key, RewardAddr, BucketEndOffset, SubChunkStartOffset
                ),
                EntropyCacheSizeMb = arweave_config:get([
                    packing, entropy, cache_size
                ]),
                MaxSize = EntropyCacheSizeMb * ?MiB,
                arweave_entropy_cache:put_with_limit(
                    Key, Entropy, ?REPLICA_2_9_ENTROPY_SIZE, MaxSize
                ),
                Entropy
        end
    after
        entropy_generation_release(Key)
    end.

%% @doc Combine the cached entropy slices of every sub-chunk of one chunk.
generate_chunk(PaddedEndOffset, RewardAddr) ->
    Slices = request_per_sub_chunk(
        fun(Ref, SubChunkStart) ->
            arweave_entropy_deps:request_slice(
                Ref, self(), {RewardAddr, PaddedEndOffset, SubChunkStart}
            )
        end
    ),
    case Slices of
        {error, Reason} ->
            {error, Reason};
        _ ->
            iolist_to_binary(Slices)
    end.

%% @doc Return a list of all BucketEndOffsets covered by the entropy needed to encipher
%% the chunk at the given offset. The list returned may include offsets that occur before
%% the provided offset. This is expected if Offset does not refer to a sector 0 chunk.
entropy_offsets(Offset, ModuleEnd) ->
    BucketEndOffset = arweave_storage:get_chunk_bucket_end(Offset),
    BucketEndOffset2 = reset_entropy_offset(BucketEndOffset),
    Partition = arweave_entropy_deps:get_entropy_partition(BucketEndOffset),
    {_, EntropyPartitionEnd} = arweave_entropy_deps:get_entropy_partition_range(
        Partition
    ),
    End = min(EntropyPartitionEnd, ModuleEnd),
    entropy_offsets2(BucketEndOffset2, End).

entropy_offsets2(BucketEndOffset, PaddedPartitionEnd) when
    BucketEndOffset > PaddedPartitionEnd
->
    [];
entropy_offsets2(BucketEndOffset, PaddedPartitionEnd) ->
    NextOffset = shift_entropy_offset(BucketEndOffset, 1),
    [BucketEndOffset | entropy_offsets2(NextOffset, PaddedPartitionEnd)].

%% @doc If we are not at the beginning of the entropy, shift the offset to
%% the left. store_entropy_footprint will traverse the entire 2.9 partition shifting
%% the offset by sector size.
reset_entropy_offset(BucketEndOffset) ->
    %% Sanity checks
    BucketEndOffset = arweave_storage:get_chunk_bucket_end(BucketEndOffset),
    %% End sanity checks
    SliceIndex = arweave_entropy_deps:get_slice_index(BucketEndOffset),
    shift_entropy_offset(BucketEndOffset, -SliceIndex).

shift_entropy_offset(Offset, SectorCount) ->
    SectorSize = arweave_constants:get_replica_2_9_entropy_sector_size(),
    arweave_storage:get_chunk_bucket_end(Offset + SectorSize * SectorCount).

%% @doc Generate one entropy per sub-chunk for the chunk's entire footprint.
generate_entropies(RewardAddr, BucketEndOffset) ->
    generate_entropies(RewardAddr, BucketEndOffset, true).

generate_entropies(RewardAddr, BucketEndOffset, CacheEntropy) ->
    prometheus_histogram:observe_duration(
        replica_2_9_entropy_duration_milliseconds,
        [],
        fun() ->
            do_generate_entropies(RewardAddr, BucketEndOffset, CacheEntropy)
        end
    ).

%% @doc Fold over a footprint, combining entropy slices one chunk at a time.
map_entropies(
    _Entropies,
    [],
    _RangeStart,
    _Keys,
    _RewardAddr,
    _Fun,
    _Args,
    Acc
) ->
    %% The amount of entropy generated per partition is slightly more than the amount needed.
    %% So at the end of a partition we will have finished processing chunks, but still have
    %% some entropy left. In this case we stop the recursion early and wait for the writes
    %% to complete.
    Acc;
map_entropies(
    Entropies,
    [BucketEndOffset | EntropyOffsets],
    RangeStart,
    Keys,
    RewardAddr,
    Fun,
    Args,
    Acc
) ->
    case take_and_combine_entropy_slices(Entropies) of
        {ChunkEntropy, Rest} ->
            %% Sanity checks
            sanity_check_replica_2_9_entropy_keys(
                BucketEndOffset, RewardAddr, Keys
            ),
            %% End sanity checks

            Acc2 =
                case BucketEndOffset > RangeStart of
                    true ->
                        erlang:apply(
                            Fun,
                            [ChunkEntropy, BucketEndOffset, RewardAddr] ++ Args ++
                                [Acc]
                        );
                    false ->
                        %% Don't write entropy before the start of the range.
                        Acc
                end,

            %% Jump to the next sector covered by this entropy.
            map_entropies(
                Rest,
                EntropyOffsets,
                RangeStart,
                Keys,
                RewardAddr,
                Fun,
                Args,
                Acc2
            )
    end.

do_generate_entropies(RewardAddr, BucketEndOffset, CacheEntropy) ->
    request_per_sub_chunk(
        fun(Ref, SubChunkStart) ->
            arweave_entropy_deps:request_generation(
                Ref,
                self(),
                {RewardAddr, BucketEndOffset, SubChunkStart, CacheEntropy}
            )
        end
    ).

%% @doc Send one request per sub-chunk to the packing workers and collect
%% the replies in sub-chunk order.
request_per_sub_chunk(Request) ->
    SubChunkSize = ?SUB_CHUNK_SIZE,
    Refs =
        lists:map(
            fun(SubChunkStart) ->
                Ref = make_ref(),
                Request(Ref, SubChunkStart),
                Ref
            end,
            lists:seq(0, ?DATA_CHUNK_SIZE - SubChunkSize, SubChunkSize)
        ),
    Replies = collect_entropies(Refs, []),
    case Replies of
        {error, _Reason} ->
            flush_entropy_messages();
        _ ->
            ok
    end,
    Replies.

%% @doc Take the first slice of each entropy and combine into a single binary. This binary
%% can be used to encipher a single chunk.
take_and_combine_entropy_slices(Entropies) ->
    true = ?SUB_CHUNK_COUNT == length(Entropies),
    take_and_combine_entropy_slices(Entropies, [], []).

take_and_combine_entropy_slices([], Acc, RestAcc) ->
    {iolist_to_binary(Acc), lists:reverse(RestAcc)};
take_and_combine_entropy_slices([<<>> | Entropies], _Acc, _RestAcc) ->
    true = lists:all(fun(Entropy) -> Entropy == <<>> end, Entropies),
    {<<>>, []};
take_and_combine_entropy_slices(
    [
        <<EntropySlice:?SUB_CHUNK_SIZE/binary, Rest/binary>>
        | Entropies
    ],
    Acc,
    RestAcc
) ->
    take_and_combine_entropy_slices(Entropies, [Acc, EntropySlice], [
        Rest | RestAcc
    ]).

sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, Keys) ->
    sanity_check_replica_2_9_entropy_keys(PaddedEndOffset, RewardAddr, 0, Keys).

sanity_check_replica_2_9_entropy_keys(
    _PaddedEndOffset, _RewardAddr, _SubChunkStartOffset, []
) ->
    ok;
sanity_check_replica_2_9_entropy_keys(
    PaddedEndOffset, RewardAddr, SubChunkStartOffset, [Key | Keys]
) ->
    Key = arweave_entropy_deps:get_entropy_key(
        RewardAddr, PaddedEndOffset, SubChunkStartOffset
    ),
    SubChunkSize = ?SUB_CHUNK_SIZE,
    sanity_check_replica_2_9_entropy_keys(
        PaddedEndOffset,
        RewardAddr,
        SubChunkStartOffset + SubChunkSize,
        Keys
    ).

generate_entropy_keys(RewardAddr, Offset) ->
    generate_entropy_keys(RewardAddr, Offset, 0).

generate_entropy_keys(_RewardAddr, _Offset, SubChunkStart) when
    SubChunkStart == ?DATA_CHUNK_SIZE
->
    [];
generate_entropy_keys(RewardAddr, Offset, SubChunkStart) ->
    SubChunkSize = ?SUB_CHUNK_SIZE,
    [
        arweave_entropy_deps:get_entropy_key(RewardAddr, Offset, SubChunkStart)
        | generate_entropy_keys(
            RewardAddr, Offset, SubChunkStart + SubChunkSize
        )
    ].

collect_entropies([], Acc) ->
    lists:reverse(Acc);
collect_entropies([Ref | Rest], Acc) ->
    receive
        {entropy_generated, Ref, Entropy} ->
            collect_entropies(Rest, [Entropy | Acc])
    after 600_000 ->
        ?LOG_ERROR([{event, entropy_generation_timeout}, {ref, Ref}]),
        {error, timeout}
    end.

flush_entropy_messages() ->
    ?LOG_INFO([{event, flush_entropy_messages}]),
    receive
        {entropy_generated, _, _} ->
            flush_entropy_messages()
    after 0 ->
        ok
    end.

entropy_generation_lock(Key) ->
    case ets:insert_new(arweave_entropy_generation, {Key, self()}) of
        true ->
            ok;
        false ->
            wait_for_generation(Key),
            entropy_generation_lock(Key)
    end.

entropy_generation_release(Key) ->
    ets:delete_object(arweave_entropy_generation, {Key, self()}).

%% @doc Recover an abandoned lock without deleting a replacement owner's lock.
wait_for_generation(Key) ->
    case ets:lookup(arweave_entropy_generation, Key) of
        [] ->
            ok;
        [{Key, Owner}] ->
            Ref = monitor(process, Owner),
            try
                receive
                    {'DOWN', Ref, process, Owner, _Reason} ->
                        ets:delete_object(
                            arweave_entropy_generation, {Key, Owner}
                        )
                after 100 ->
                    ok
                end
            after
                demonitor(Ref, [flush])
            end
    end.

update_entropy_generation_stats(
    Key, RewardAddr, BucketEndOffset, SubChunkStartOffset
) ->
    Tab = entropy_generation_stats,
    Time = erlang:monotonic_time(millisecond),
    ets:update_counter(Tab, Key, {2, 1}, {Key, 0, Time}),
    arweave_metrics:counter_inc(
        replica_2_9_entropy_generated, ?REPLICA_2_9_ENTROPY_SIZE
    ),
    maybe_report_redundant_entropy_generation(
        Key, RewardAddr, BucketEndOffset, SubChunkStartOffset
    ),
    remove_outdated_entropy_generation_stats().

maybe_report_redundant_entropy_generation(
    Key, RewardAddr, BucketEndOffset, SubChunkStartOffset
) ->
    Tab = entropy_generation_stats,
    Now = erlang:monotonic_time(millisecond),
    [{_, Count, Time}] = ets:lookup(Tab, Key),
    case Count > 1 of
        true ->
            Partition =
                (BucketEndOffset div arweave_constants:partition_size()),
            arweave_metrics:counter_inc(replica_2_9_entropy_stats, [
                Partition, redundant
            ]),
            ?LOG_DEBUG([
                {event, possibly_redundant_entropy_generation},
                {reward_addr, arweave_util:encode(RewardAddr)},
                {key, arweave_util:encode(Key)},
                {bucket_end_offset, BucketEndOffset},
                {sub_chunk_start_offset, SubChunkStartOffset},
                {count, Count},
                {seconds_since_first_generation, (Now - Time) / 1_000},
                {avg_per_second, Count / ((Now - Time) / 1_000)}
            ]);
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
