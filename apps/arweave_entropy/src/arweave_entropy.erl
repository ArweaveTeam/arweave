%%% Public entropy API; persistence belongs to arweave_storage.
-module(arweave_entropy).
-behaviour(application).
-export([start/2, stop/1, child_spec/0]).
-export([
    generate/3, generate/4,
    generate_chunk/2,
    generate_entropies/2, generate_entropies/4,
    generate_entropy_keys/2,
    entropy_offsets/2,
    map_entropies/8
]).

-ifdef(AR_TEST).
-export([internal_get_cached/1, internal_cache/4, internal_clear_cache/0]).
-endif.

start(_Type, _Args) -> arweave_entropy_sup:start_link().
stop(_State) -> ok.
child_spec() -> arweave_entropy_lifecycle:child_spec().

generate(RewardAddr, Offset, SubChunkStart) ->
    arweave_entropy_generation:generate(RewardAddr, Offset, SubChunkStart).

generate(RewardAddr, Offset, SubChunkStart, Cache) ->
    arweave_entropy_generation:generate(
        RewardAddr, Offset, SubChunkStart, Cache
    ).

generate_chunk(Offset, RewardAddr) ->
    arweave_entropy_generation:generate_chunk(Offset, RewardAddr).

generate_entropies(RewardAddr, Offset) ->
    arweave_entropy_generation:generate_entropies(RewardAddr, Offset).

generate_entropies(StoreID, RewardAddr, Offset, ReplyTo) ->
    arweave_entropy_preparation:generate_entropies(
        StoreID, RewardAddr, Offset, ReplyTo
    ).

generate_entropy_keys(RewardAddr, Offset) ->
    arweave_entropy_generation:generate_entropy_keys(RewardAddr, Offset).

entropy_offsets(Offset, End) ->
    arweave_entropy_generation:entropy_offsets(Offset, End).

map_entropies(Entropies, Offsets, Start, Keys, RewardAddr, Fun, Args, Acc) ->
    arweave_entropy_generation:map_entropies(
        Entropies, Offsets, Start, Keys, RewardAddr, Fun, Args, Acc
    ).

-ifdef(AR_TEST).

%% @doc Read a simulated entry using the real entropy cache.
internal_get_cached(Key) ->
    arweave_entropy_cache:get(Key).

%% @doc Cache a simulated value with its modeled size and capacity in bytes.
internal_cache(Key, Value, Size, MaxSize) ->
    arweave_entropy_cache:put_with_limit(Key, Value, Size, MaxSize).

%% @doc Clear cached entries between tests without bypassing eviction accounting.
internal_clear_cache() ->
    arweave_entropy_cache:clean_up_space(0, 0).

-endif.
